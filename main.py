from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from starlette.middleware.cors import CORSMiddleware
from supabase import create_client
import uvicorn
import httpx
import hashlib
import json
import time
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")

app = FastAPI()
db  = create_client(SUPABASE_URL, SUPABASE_KEY)

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=".*",
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    allow_credentials=True,
)


@app.get("/")
def root():
    return RedirectResponse(url="/static/index.html")

app.mount("/static", StaticFiles(directory="static"), name="static")


# ── Helpers ──────────────────────────────────────────────────────
def sha256(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode()).hexdigest()

def extrair_utms(data: dict) -> dict:
    return {
        "utm_source":   data.get("utm_source"),
        "utm_medium":   data.get("utm_medium"),
        "utm_campaign": data.get("utm_campaign"),
        "utm_content":  data.get("utm_content"),
        "utm_term":     data.get("utm_term"),
    }

def get_meta_config():
    """Lê pixel_id e token salvos no Supabase."""
    try:
        result = db.table("configuracoes").select("chave,valor").in_("chave", ["meta_pixel_id", "meta_token"]).execute()
        cfg = {r["chave"]: r["valor"] for r in (result.data or [])}
        return cfg.get("meta_pixel_id"), cfg.get("meta_token")
    except Exception as e:
        print(f"[CFG] Erro ao ler config: {e}")
        return None, None

def salvar_log_conversao(plataforma: str, event_name: str, status: str, code: int, response: str,
                          email: str = None, phone: str = None, value: float = None,
                          telegram_user_id: str = None, canal_nome: str = None, direcao: str = "enviado"):
    try:
        db.table("conversion_logs").insert({
            "plataforma":       plataforma,
            "event_name":       event_name,
            "status":           status,
            "response_code":    code,
            "response_body":    (response or "")[:500],
            "email":            email,
            "phone":            phone,
            "value":            value,
            "telegram_user_id": telegram_user_id,
            "canal_nome":       canal_nome,
            "direcao":          direcao,
        }).execute()
    except Exception as e:
        print(f"[LOG ERRO] {e}")


_geo_cache = {}

async def _geo_lookup(ip: str) -> dict:
    """Reverse geo do IP (cidade, estado, país). Cache em memória."""
    if not ip:
        return {}
    if ip in _geo_cache:
        return _geo_cache[ip]
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"http://ip-api.com/json/{ip}", params={"fields": "status,city,regionName,countryCode"})
            d = r.json()
            if d.get("status") == "success":
                geo = {
                    "city":    (d.get("city") or "").lower().strip(),
                    "state":   (d.get("regionName") or "").lower().strip(),
                    "country": (d.get("countryCode") or "").lower().strip(),
                }
                _geo_cache[ip] = geo
                return geo
    except Exception as e:
        print(f"[GEO ERRO] {e}")
    _geo_cache[ip] = {}
    return {}


async def enviar_meta(event_name: str, email: str = None, phone: str = None, value: float = None,
                      first_name: str = None, last_name: str = None,
                      telegram_user_id: str = None, canal_nome: str = None,
                      fbc: str = None, fbp: str = None, client_ip: str = None,
                      user_agent: str = None, external_id: str = None,
                      event_source_url: str = None, action_source: str = "website"):
    pixel_id, token = get_meta_config()
    if not pixel_id or not token:
        print(f"[META ✗] Pixel ID ou Token não configurado")
        salvar_log_conversao("meta", event_name, "erro", 0, "Pixel/Token não configurado",
                             email, phone, value, telegram_user_id, canal_nome)
        return

    # Se faltar fbc/fbp/IP/UA, tenta puxar do snapshot da última entrada (atribuição Telegram)
    snap = {}
    if not (fbc and fbp and client_ip and user_agent):
        try:
            raw = _get_cfg("_ultima_atribuicao")
            if raw:
                snap = json.loads(raw)
                # Snapshot só vale se for recente (últimas 24h)
                if int(time.time()) - int(snap.get("ts", 0)) > 86400:
                    snap = {}
        except Exception:
            snap = {}
    fbc        = fbc        or snap.get("fbc")
    fbp        = fbp        or snap.get("fbp")
    client_ip  = client_ip  or snap.get("client_ip")
    user_agent = user_agent or snap.get("user_agent")
    external_id = external_id or snap.get("external_id") or telegram_user_id
    event_source_url = event_source_url or snap.get("page_url")

    # Geolocalização do IP (city/state/country) — boost match quality
    geo = await _geo_lookup(client_ip) if client_ip else {}

    user_data = {}
    if email:       user_data["em"] = [sha256(email)]
    if phone:       user_data["ph"] = [sha256(phone)]
    if first_name:  user_data["fn"] = [sha256(first_name)]
    if last_name:   user_data["ln"] = [sha256(last_name)]
    if external_id: user_data["external_id"] = [sha256(external_id)]
    if geo.get("city"):    user_data["ct"]      = [sha256(geo["city"])]
    if geo.get("state"):   user_data["st"]      = [sha256(geo["state"])]
    if geo.get("country"): user_data["country"] = [sha256(geo["country"])]
    if fbc:         user_data["fbc"] = fbc
    if fbp:         user_data["fbp"] = fbp
    if client_ip:   user_data["client_ip_address"] = client_ip
    if user_agent:  user_data["client_user_agent"] = user_agent

    # custom_data: UTMs + value + tracking interno
    custom_data = {}
    for k in ("utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"):
        v = snap.get(k)
        if v: custom_data[k] = v
    if value is not None:
        custom_data["currency"] = "BRL"
        custom_data["value"]    = value
    if canal_nome:
        custom_data["canal"] = canal_nome
    if telegram_user_id:
        custom_data["telegram_user_id"] = str(telegram_user_id)

    evento = {
        "event_name":    event_name,
        "event_time":    int(time.time()),
        "event_id":      f"{event_name}_{external_id or telegram_user_id or int(time.time()*1000)}",
        "action_source": action_source,
        "user_data":     user_data,
    }
    if event_source_url:
        evento["event_source_url"] = event_source_url
    if custom_data:
        evento["custom_data"] = custom_data

    url = f"https://graph.facebook.com/v19.0/{pixel_id}/events"

    async with httpx.AsyncClient() as client:
        resp = await client.post(url, params={"access_token": token}, json={"data": [evento]})

    status = "sucesso" if resp.status_code == 200 else "erro"
    salvar_log_conversao("meta", event_name, status, resp.status_code, resp.text,
                         email, phone, value, telegram_user_id, canal_nome)

    if resp.status_code == 200:
        print(f"[META ✓] {event_name} | fbc={'✓' if fbc else '✗'} fbp={'✓' if fbp else '✗'} ip={'✓' if client_ip else '✗'} ua={'✓' if user_agent else '✗'}")
    else:
        print(f"[META ✗] {resp.status_code} — {resp.text}")


def get_kwai_config():
    """Lê kwai_pixel_id e kwai_token salvos no Supabase."""
    try:
        result = db.table("configuracoes").select("chave,valor").in_("chave", ["kwai_pixel_id", "kwai_token"]).execute()
        cfg = {r["chave"]: r["valor"] for r in (result.data or [])}
        return cfg.get("kwai_pixel_id"), cfg.get("kwai_token")
    except Exception as e:
        print(f"[CFG KWAI] Erro ao ler config: {e}")
        return None, None

def get_tiktok_config():
    """Lê tiktok_pixel_code e tiktok_token salvos no Supabase."""
    try:
        result = db.table("configuracoes").select("chave,valor").in_("chave", ["tiktok_pixel_code", "tiktok_token"]).execute()
        cfg = {r["chave"]: r["valor"] for r in (result.data or [])}
        return cfg.get("tiktok_pixel_code"), cfg.get("tiktok_token")
    except Exception as e:
        print(f"[CFG TIKTOK] Erro ao ler config: {e}")
        return None, None

async def enviar_kwai(event_name: str, email: str = None, phone: str = None, value: float = None, telegram_user_id: str = None, canal_nome: str = None):
    pixel_id, token = get_kwai_config()
    if not pixel_id or not token:
        print(f"[KWAI ✗] Pixel ID ou Token não configurado")
        salvar_log_conversao("kwai", event_name, "erro", 0, "Pixel/Token não configurado",
                             email, phone, value, telegram_user_id, canal_nome)
        return

    user_info = {}
    if email:
        user_info["email"] = sha256(email)
    if phone:
        user_info["phone"] = sha256(phone)

    event = {
        "event_type": event_name,
        "event_time": int(time.time() * 1000),
        "user_info": user_info,
    }
    if value is not None:
        event["custom_info"] = {"value": value, "currency": "BRL"}
    else:
        event["custom_info"] = {}

    body = {
        "click_id": "",
        "events": [event],
        "pixel_id": pixel_id,
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://open.kwai.com/api/openapi/v1/conversion/event/batch",
            headers={"access-token": token},
            json=body,
        )

    status = "sucesso" if resp.status_code == 200 else "erro"
    salvar_log_conversao("kwai", event_name, status, resp.status_code, resp.text,
                         email, phone, value, telegram_user_id, canal_nome)

    if resp.status_code == 200:
        print(f"[KWAI ✓] Evento '{event_name}' enviado")
    else:
        print(f"[KWAI ✗] {resp.status_code} — {resp.text}")

async def enviar_tiktok(event_name: str, email: str = None, phone: str = None, value: float = None, telegram_user_id: str = None, canal_nome: str = None):
    pixel_code, token = get_tiktok_config()
    if not pixel_code or not token:
        print(f"[TIKTOK ✗] Pixel Code ou Token não configurado")
        salvar_log_conversao("tiktok", event_name, "erro", 0, "Pixel/Token não configurado",
                             email, phone, value, telegram_user_id, canal_nome)
        return

    user = {}
    if email:
        user["email"] = sha256(email)
    if phone:
        user["phone_number"] = sha256(phone)

    body = {
        "pixel_code": pixel_code,
        "event": event_name,
        "timestamp": str(int(time.time())),
        "context": {"user": user},
    }
    if value is not None:
        body["properties"] = {"currency": "BRL", "value": str(value)}
    else:
        body["properties"] = {}

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://business-api.tiktok.com/open_api/v1.3/event/track/",
            headers={"Access-Token": token},
            json=body,
        )

    status = "sucesso" if resp.status_code == 200 else "erro"
    salvar_log_conversao("tiktok", event_name, status, resp.status_code, resp.text,
                         email, phone, value, telegram_user_id, canal_nome)

    if resp.status_code == 200:
        print(f"[TIKTOK ✓] Evento '{event_name}' enviado")
    else:
        print(f"[TIKTOK ✗] {resp.status_code} — {resp.text}")


# ── Config ───────────────────────────────────────────────────────
@app.get("/config/meta")
def ler_config_meta():
    pixel_id, token = get_meta_config()
    return {
        "pixel_id": pixel_id or "",
        "token": token or "",
        "configurado": bool(pixel_id and token),
    }

@app.post("/config/meta")
async def salvar_config_meta(request: Request):
    data = await request.json()
    pixel_id = data.get("pixel_id", "").strip()
    token = data.get("token", "").strip()

    if not pixel_id or not token:
        raise HTTPException(status_code=400, detail="pixel_id e token são obrigatórios")

    try:
        for chave, valor in [("meta_pixel_id", pixel_id), ("meta_token", token)]:
            existing = db.table("configuracoes").select("chave").eq("chave", chave).execute()
            if existing.data:
                db.table("configuracoes").update({"valor": valor}).eq("chave", chave).execute()
            else:
                db.table("configuracoes").insert({"chave": chave, "valor": valor}).execute()
    except Exception as e:
        print(f"[CONFIG ERRO] {e}")
        raise HTTPException(status_code=500, detail=str(e))

    print(f"[CONFIG] Meta Pixel atualizado: {pixel_id}")
    return {"status": "ok"}

@app.get("/config/kwai")
def ler_config_kwai():
    pixel_id, token = get_kwai_config()
    return {
        "pixel_id": pixel_id or "",
        "token": token or "",
        "configurado": bool(pixel_id and token),
    }

@app.post("/config/kwai")
async def salvar_config_kwai(request: Request):
    data = await request.json()
    pixel_id = data.get("pixel_id", "").strip()
    token = data.get("token", "").strip()

    if not pixel_id or not token:
        raise HTTPException(status_code=400, detail="pixel_id e token são obrigatórios")

    try:
        for chave, valor in [("kwai_pixel_id", pixel_id), ("kwai_token", token)]:
            existing = db.table("configuracoes").select("chave").eq("chave", chave).execute()
            if existing.data:
                db.table("configuracoes").update({"valor": valor}).eq("chave", chave).execute()
            else:
                db.table("configuracoes").insert({"chave": chave, "valor": valor}).execute()
    except Exception as e:
        print(f"[CONFIG ERRO] {e}")
        raise HTTPException(status_code=500, detail=str(e))

    print(f"[CONFIG] Kwai Pixel atualizado: {pixel_id}")
    return {"status": "ok"}

@app.get("/config/tiktok")
def ler_config_tiktok():
    pixel_code, token = get_tiktok_config()
    return {
        "pixel_code": pixel_code or "",
        "token": token or "",
        "configurado": bool(pixel_code and token),
    }

@app.post("/config/tiktok")
async def salvar_config_tiktok(request: Request):
    data = await request.json()
    pixel_code = data.get("pixel_code", "").strip()
    token = data.get("token", "").strip()

    if not pixel_code or not token:
        raise HTTPException(status_code=400, detail="pixel_code e token são obrigatórios")

    try:
        for chave, valor in [("tiktok_pixel_code", pixel_code), ("tiktok_token", token)]:
            existing = db.table("configuracoes").select("chave").eq("chave", chave).execute()
            if existing.data:
                db.table("configuracoes").update({"valor": valor}).eq("chave", chave).execute()
            else:
                db.table("configuracoes").insert({"chave": chave, "valor": valor}).execute()
    except Exception as e:
        print(f"[CONFIG ERRO] {e}")
        raise HTTPException(status_code=500, detail=str(e))

    print(f"[CONFIG] TikTok Pixel atualizado: {pixel_code}")
    return {"status": "ok"}


# ── Meta Ads (Marketing API) ──────────────────────────────────────
def _set_cfg(chave: str, valor: str):
    existing = db.table("configuracoes").select("chave").eq("chave", chave).execute()
    if existing.data:
        db.table("configuracoes").update({"valor": valor}).eq("chave", chave).execute()
    else:
        db.table("configuracoes").insert({"chave": chave, "valor": valor}).execute()

def _get_cfg(chave: str) -> str:
    r = db.table("configuracoes").select("valor").eq("chave", chave).execute()
    return r.data[0]["valor"] if r.data else ""


# ── Análise com IA (Claude) ──────────────────────────────────────
@app.post("/ia/analisar")
async def ia_analisar(request: Request):
    body = await request.json()
    pergunta = (body.get("pergunta") or "").strip()
    if not pergunta:
        raise HTTPException(status_code=400, detail="pergunta é obrigatória")

    api_key = _get_cfg("anthropic_api_key") or os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=400, detail="ANTHROPIC_API_KEY não configurada (Configurações → IA)")

    try:
        import anthropic
    except ImportError:
        raise HTTPException(status_code=500, detail="Pacote anthropic não instalado")

    # Coleta dados resumidos do app
    try:
        tz_offset = int(_get_cfg("timezone_offset") or "-3")
    except Exception:
        tz_offset = -3
    tz_str = f"{tz_offset:+03d}:00"

    # Período: últimos 30 dias (configurável via body)
    data_inicio = body.get("data_inicio")
    data_fim = body.get("data_fim")
    import datetime
    if not data_fim:
        data_fim = datetime.date.today().isoformat()
    if not data_inicio:
        data_inicio = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()

    contexto = {"periodo": {"inicio": data_inicio, "fim": data_fim, "timezone_offset": tz_offset}}

    try:
        # Cadastros
        r = (db.table("cadastros").select("id,utm_source,utm_campaign,utm_medium,created_at", count="exact")
             .gte("created_at", data_inicio + "T00:00:00" + tz_str)
             .lte("created_at", data_fim + "T23:59:59" + tz_str).execute())
        contexto["cadastros_total"] = r.count
        from collections import Counter
        contexto["cadastros_por_source"] = dict(Counter(x.get("utm_source") or "Direto" for x in (r.data or [])))
        contexto["cadastros_por_campanha"] = dict(Counter(x.get("utm_campaign") or "Direto" for x in (r.data or [])))

        # Depósitos
        r2 = (db.table("depositos").select("id,email,valor,utm_source,created_at")
              .gte("created_at", data_inicio + "T00:00:00" + tz_str)
              .lte("created_at", data_fim + "T23:59:59" + tz_str).execute())
        deps = r2.data or []
        contexto["depositos_total"] = len(deps)
        contexto["receita_total"] = round(sum(float(d.get("valor") or 0) for d in deps), 2)
        # FTD
        seen = set(); ftd = 0; ftd_valor = 0
        for d in sorted(deps, key=lambda x: x.get("created_at", "")):
            email = d.get("email", "")
            if email and email not in seen:
                seen.add(email); ftd += 1; ftd_valor += float(d.get("valor") or 0)
        contexto["ftd_count"] = ftd
        contexto["ftd_valor"] = round(ftd_valor, 2)
        contexto["redep_count"] = len(deps) - ftd
        contexto["redep_valor"] = round(contexto["receita_total"] - ftd_valor, 2)

        # Telegram members
        r3 = (db.table("telegram_members").select("event", count="exact")
              .gte("created_at", data_inicio + "T00:00:00" + tz_str)
              .lte("created_at", data_fim + "T23:59:59" + tz_str).execute())
        contexto["telegram_joins"]  = sum(1 for x in (r3.data or []) if x.get("event") == "join")
        contexto["telegram_leaves"] = sum(1 for x in (r3.data or []) if x.get("event") == "leave")

        # PageViews
        r4 = (db.table("tracker_pageviews").select("id", count="exact")
              .gte("created_at", data_inicio + "T00:00:00" + tz_str)
              .lte("created_at", data_fim + "T23:59:59" + tz_str).execute())
        contexto["pageviews"] = r4.count

        # Conversões enviadas
        r5 = (db.table("conversion_logs").select("plataforma,event_name,status", count="exact")
              .gte("created_at", data_inicio + "T00:00:00" + tz_str)
              .lte("created_at", data_fim + "T23:59:59" + tz_str).execute())
        cl_data = r5.data or []
        contexto["conversoes_enviadas"] = {
            "total": len(cl_data),
            "sucesso": sum(1 for x in cl_data if x.get("status") == "sucesso"),
            "erro":    sum(1 for x in cl_data if x.get("status") == "erro"),
            "por_plataforma": dict(Counter(x.get("plataforma") for x in cl_data)),
            "por_evento":     dict(Counter(x.get("event_name") for x in cl_data)),
        }

        # Meta Ads (se conectado)
        meta_token = _get_cfg("metaads_access_token")
        if meta_token and body.get("metaads_account_id"):
            account_id = body["metaads_account_id"]
            async with httpx.AsyncClient(timeout=20) as client:
                ri = await client.get(
                    f"https://graph.facebook.com/v19.0/{account_id}/insights",
                    params={
                        "access_token": meta_token,
                        "fields": "spend,impressions,clicks,ctr,cpc,cpm,reach",
                        "level": "account",
                        "time_range": json.dumps({"since": data_inicio, "until": data_fim}),
                    },
                )
                d = ri.json().get("data", [])
                if d:
                    ins = d[0]
                    contexto["meta_ads"] = {
                        "spend":       float(ins.get("spend", 0)),
                        "impressions": int(ins.get("impressions", 0)),
                        "clicks":      int(ins.get("clicks", 0)),
                        "ctr":         float(ins.get("ctr", 0)),
                        "cpc":         float(ins.get("cpc", 0)),
                        "cpm":         float(ins.get("cpm", 0)),
                        "reach":       int(ins.get("reach", 0)),
                    }
    except Exception as e:
        contexto["erro_coleta"] = str(e)

    # Chama Claude
    try:
        client = anthropic.Anthropic(api_key=api_key)
        prompt = f"""Você é um analista de marketing digital especializado em tráfego pago e conversão. Analise os dados abaixo e responda à pergunta do usuário de forma direta, prática e com insights acionáveis.

DADOS DO PERÍODO ({contexto['periodo']['inicio']} até {contexto['periodo']['fim']}):
{json.dumps(contexto, ensure_ascii=False, indent=2)}

PERGUNTA DO USUÁRIO:
{pergunta}

Responda em português, formato Markdown. Se houver problemas óbvios (ex: ROAS negativo, CPL alto, conversão baixa), aponte. Se faltar dado importante, diga. Seja conciso — máximo 5 parágrafos curtos."""

        msg = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        resposta = msg.content[0].text
        return {"resposta": resposta, "contexto": contexto}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao chamar IA: {e}")


@app.post("/config/ia")
async def salvar_config_ia(request: Request):
    body = await request.json()
    if "anthropic_api_key" in body:
        _set_cfg("anthropic_api_key", body["anthropic_api_key"])
    return {"status": "ok"}


@app.get("/config/ia")
def ler_config_ia():
    key = _get_cfg("anthropic_api_key")
    return {"configurada": bool(key), "preview": (key[:15] + "..." + key[-5:]) if key else ""}


@app.get("/config/geral")
def get_config_geral():
    return {
        "timezone_offset": _get_cfg("timezone_offset") or "-3",
    }

@app.post("/config/geral")
async def salvar_config_geral(request: Request):
    data = await request.json()
    if "timezone_offset" in data:
        _set_cfg("timezone_offset", str(data["timezone_offset"]))
    return {"status": "ok"}


@app.get("/config/metaads")
def get_metaads_config(request: Request):
    redirect_uri = str(request.base_url).rstrip("/").replace("http://", "https://") + "/metaads/callback"
    contas_raw = _get_cfg("metaads_contas") or "[]"
    try:
        contas = json.loads(contas_raw)
    except Exception:
        contas = []
    return {
        "app_id":       _get_cfg("metaads_app_id"),
        "app_secret":   bool(_get_cfg("metaads_app_secret")),
        "access_token": bool(_get_cfg("metaads_access_token")),
        "contas":       contas,
        "redirect_uri": redirect_uri,
    }


@app.post("/config/metaads")
async def salvar_metaads_credenciais(request: Request):
    data = await request.json()
    app_id = (data.get("app_id") or "").strip()
    app_secret = (data.get("app_secret") or "").strip()
    if not app_id or not app_secret:
        raise HTTPException(status_code=400, detail="app_id e app_secret obrigatórios")
    try:
        _set_cfg("metaads_app_id", app_id)
        _set_cfg("metaads_app_secret", app_secret)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "ok"}


@app.get("/metaads/connect")
async def metaads_connect(request: Request):
    """Redireciona pro OAuth do Facebook."""
    app_id = _get_cfg("metaads_app_id")
    if not app_id:
        raise HTTPException(status_code=400, detail="App ID não configurado")
    redirect_uri = str(request.base_url).rstrip("/").replace("http://", "https://") + "/metaads/callback"
    scopes = "ads_read,ads_management,business_management"
    url = (
        f"https://www.facebook.com/v19.0/dialog/oauth"
        f"?client_id={app_id}"
        f"&redirect_uri={redirect_uri}"
        f"&scope={scopes}"
        f"&response_type=code"
    )
    return RedirectResponse(url=url)


@app.get("/metaads/callback")
async def metaads_callback(request: Request, code: str = None, error: str = None):
    """Recebe o code do OAuth, troca por access_token de longa duração e busca contas."""
    if error or not code:
        return RedirectResponse(url="/static/dashboard.html?metaads_erro=" + (error or "sem-codigo"))

    app_id = _get_cfg("metaads_app_id")
    app_secret = _get_cfg("metaads_app_secret")
    redirect_uri = str(request.base_url).rstrip("/").replace("http://", "https://") + "/metaads/callback"

    async with httpx.AsyncClient(timeout=20) as client:
        # 1) Trocar code por short-lived token
        r = await client.get(
            "https://graph.facebook.com/v19.0/oauth/access_token",
            params={
                "client_id": app_id,
                "client_secret": app_secret,
                "redirect_uri": redirect_uri,
                "code": code,
            },
        )
        d = r.json()
        if "access_token" not in d:
            return RedirectResponse(url=f"/static/dashboard.html?metaads_erro={d.get('error',{}).get('message','token-erro')}")
        short_token = d["access_token"]

        # 2) Trocar por long-lived token (60 dias)
        r2 = await client.get(
            "https://graph.facebook.com/v19.0/oauth/access_token",
            params={
                "grant_type":      "fb_exchange_token",
                "client_id":       app_id,
                "client_secret":   app_secret,
                "fb_exchange_token": short_token,
            },
        )
        d2 = r2.json()
        long_token = d2.get("access_token", short_token)

        # 3) Buscar contas de anúncio
        r3 = await client.get(
            "https://graph.facebook.com/v19.0/me/adaccounts",
            params={"access_token": long_token, "fields": "id,name,account_status,currency,business_name"},
        )
        d3 = r3.json()
        contas = d3.get("data", []) or []

    _set_cfg("metaads_access_token", long_token)
    _set_cfg("metaads_contas", json.dumps(contas, ensure_ascii=False))

    return RedirectResponse(url="/static/dashboard.html?metaads_ok=1")


@app.delete("/config/metaads")
def desconectar_metaads():
    try:
        for chave in ["metaads_access_token", "metaads_contas"]:
            db.table("configuracoes").delete().eq("chave", chave).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "ok"}


@app.get("/metaads/campaigns")
async def metaads_campaigns(account_id: str, since: str = None, until: str = None):
    """Lista campanhas + insights em paralelo (rápido)."""
    token = _get_cfg("metaads_access_token")
    if not token:
        raise HTTPException(status_code=400, detail="Não conectado")

    insights_params = {
        "access_token": token,
        "fields": "campaign_id,spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,actions",
        "level": "campaign",
        "limit": 500,
    }
    if since and until:
        insights_params["time_range"] = json.dumps({"since": since, "until": until})
    else:
        insights_params["date_preset"] = "last_30d"

    async with httpx.AsyncClient(timeout=45) as client:
        # Em paralelo: lista campanhas + insights agregados de todas
        camps_task = client.get(
            f"https://graph.facebook.com/v19.0/{account_id}/campaigns",
            params={
                "access_token": token,
                "fields": "id,name,status,effective_status,objective,daily_budget,lifetime_budget,start_time,stop_time",
                "limit": 500,
            },
        )
        ins_task = client.get(
            f"https://graph.facebook.com/v19.0/{account_id}/insights",
            params=insights_params,
        )
        r_camps, r_ins = await asyncio.gather(camps_task, ins_task)

    camps = r_camps.json().get("data", []) or []
    ins_data = r_ins.json().get("data", []) or []
    ins_por_campanha = {x.get("campaign_id"): x for x in ins_data}

    for c in camps:
        c["insights"] = ins_por_campanha.get(c["id"], {})

    return {"campaigns": camps}


@app.post("/metaads/campaigns/{campaign_id}/toggle")
async def metaads_toggle_campaign(campaign_id: str, request: Request):
    """Alterna o status de uma campanha (ACTIVE / PAUSED)."""
    token = _get_cfg("metaads_access_token")
    if not token:
        raise HTTPException(status_code=400, detail="Não conectado")
    body = await request.json()
    novo_status = body.get("status", "").upper()
    if novo_status not in ("ACTIVE", "PAUSED"):
        raise HTTPException(status_code=400, detail="status deve ser ACTIVE ou PAUSED")
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            f"https://graph.facebook.com/v19.0/{campaign_id}",
            data={"access_token": token, "status": novo_status},
        )
        d = r.json()
    if r.status_code != 200 or not d.get("success", True):
        raise HTTPException(status_code=400, detail=d.get("error", {}).get("message", "Erro ao alterar status"))
    return {"ok": True, "status": novo_status}


@app.get("/metaads/insights")
async def metaads_insights(account_id: str, since: str = None, until: str = None):
    """Retorna insights de gasto de uma conta."""
    token = _get_cfg("metaads_access_token")
    if not token:
        raise HTTPException(status_code=400, detail="Não conectado")
    params = {
        "access_token": token,
        "fields": "spend,impressions,clicks,reach,ctr,cpc,cpm,actions,date_start,date_stop",
        "level":  "account",
    }
    if since and until:
        params["time_range"] = json.dumps({"since": since, "until": until})
    else:
        params["date_preset"] = "last_30d"
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"https://graph.facebook.com/v19.0/{account_id}/insights", params=params)
        return r.json()


# ── Endpoints ────────────────────────────────────────────────────
@app.post("/cadastro")
async def cadastro(request: Request):
    data = await request.json()
    print(f"[CADASTRO PAYLOAD] {data}")

    inner = data.get("data") or data
    utm   = inner.get("utm") or {}

    registro = {
        "nome":     (inner.get("firstName","") + " " + inner.get("lastName","")).strip() or inner.get("nome"),
        "email":    inner.get("email")    or data.get("email"),
        "telefone": inner.get("phone")    or inner.get("telefone") or data.get("telefone"),
        "utm_source":   utm.get("source")   or data.get("utm_source"),
        "utm_medium":   utm.get("medium")   or data.get("utm_medium"),
        "utm_campaign": utm.get("campaign") or data.get("utm_campaign"),
        "utm_content":  utm.get("content")  or data.get("utm_content"),
        "utm_term":     utm.get("term")     or data.get("utm_term"),
    }

    result = db.table("cadastros").insert(registro).execute()
    if not result.data:
        raise HTTPException(status_code=500, detail="Erro ao salvar cadastro")

    await enviar_meta(
        "track_cadastro",
        email=registro["email"],
        phone=registro["telefone"],
        first_name=inner.get("firstName"),
        last_name=inner.get("lastName"),
    )
    await enviar_kwai("Registration", email=registro["email"], phone=registro["telefone"])
    await enviar_tiktok("CompleteRegistration", email=registro["email"], phone=registro["telefone"])

    print(f"[CADASTRO] {registro['email']} salvo")
    return {"status": "ok", "id": result.data[0]["id"]}


@app.post("/deposito")
async def deposito(request: Request):
    data = await request.json()
    print(f"[DEPOSITO PAYLOAD] {data}")

    registro = {
        "email": data.get("email"),
        "valor": data.get("valor"),
        **extrair_utms(data),
    }

    result = db.table("depositos").insert(registro).execute()
    if not result.data:
        raise HTTPException(status_code=500, detail="Erro ao salvar deposito")

    await enviar_meta("track_deposito", email=registro["email"], value=registro["valor"])
    await enviar_kwai("Purchase", email=registro["email"], value=registro["valor"])
    await enviar_tiktok("PlaceAnOrder", email=registro["email"], value=registro["valor"])

    print(f"[DEPOSITO] {registro['email']} - R$ {registro['valor']}")
    return {"status": "ok", "id": result.data[0]["id"]}


# ── Telegram ─────────────────────────────────────────────────────
def _get_telegram_secret_token() -> str:
    """Gera (se não existir) e retorna o secret_token do webhook Telegram."""
    secret = _get_cfg("telegram_webhook_secret")
    if not secret:
        import secrets as _secrets
        secret = _secrets.token_urlsafe(32)
        _set_cfg("telegram_webhook_secret", secret)
    return secret


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    # Validação: só aceita POSTs do Telegram com o secret_token correto
    secret_esperado = _get_cfg("telegram_webhook_secret")
    if secret_esperado:
        secret_recebido = request.headers.get("x-telegram-bot-api-secret-token", "")
        if secret_recebido != secret_esperado:
            print(f"[WEBHOOK BLOQUEADO] secret incorreto/ausente — IP={_client_ip(request)}")
            raise HTTPException(status_code=401, detail="invalid secret token")
    update = await request.json()

    # ── Solicitação de entrada (chat_join_request) ──
    join_req = update.get("chat_join_request")
    if join_req:
        chat = join_req.get("chat", {})
        from_user = join_req.get("from", {})
        canal_tg_id = chat.get("id")
        user_id_req = from_user.get("id")
        # user_chat_id: janela de 5min pra DM mesmo sem /start
        user_chat_id_req = join_req.get("user_chat_id") or user_id_req
        canal_nome_req = chat.get("title", "")
        first_name_req = from_user.get("first_name", "")

        # Busca config de auto-aprovação pelo canal_id interno
        try:
            canal = db.table("telegram_canais").select("id").eq("telegram_id", str(canal_tg_id)).execute()
            canal_id_interno = canal.data[0]["id"] if canal.data else None
        except Exception:
            canal_id_interno = None

        # Log: evento recebido
        salvar_log_conversao(
            "telegram", "JoinRequest", "sucesso", 200, f"user={user_id_req} canal={canal_nome_req}",
            telegram_user_id=str(user_id_req), canal_nome=canal_nome_req, direcao="recebido",
        )

        if canal_id_interno is not None:
            bot_token_local = TELEGRAM_BOT_TOKEN
            try:
                r2 = db.table("configuracoes").select("valor").eq("chave","telegram_bot_token").execute()
                if r2.data: bot_token_local = r2.data[0]["valor"]
            except Exception:
                pass

            # ── Boas-vindas: enviar DM ANTES de aprovar (janela 5min do user_chat_id) ──
            bv = _bv_cfg(canal_id_interno)
            if bv.get("ativo") and bot_token_local and user_chat_id_req:
                msg_renderizada = _render_msg(bv.get("mensagem", ""), from_user)
                # Renderiza placeholders também nos textos de botões (URL fica intacta)
                botoes_render = []
                for b in (bv.get("botoes") or []):
                    botoes_render.append({
                        "texto": _render_msg(b.get("texto", ""), from_user),
                        "url":   b.get("url", ""),
                    })
                asyncio.create_task(_enviar_dm_boas_vindas(
                    bot_token_local, user_chat_id_req, msg_renderizada,
                    bv.get("parse_mode", "HTML"), canal_nome_req, str(user_id_req),
                    tipo=bv.get("tipo", "texto"),
                    midia_url=bv.get("midia_url", ""),
                    botoes=botoes_render,
                    canal_id=canal_id_interno, evento="welcome",
                    user_obj=from_user,
                ))
                print(f"[BOAS-VINDAS] disparado pra {first_name_req} (chat {user_chat_id_req}) tipo={bv.get('tipo','texto')}")

            cfg = _aprov_cfg(canal_id_interno)
            if cfg.get("auto_aprovar"):
                # Dispara aprovação em background (com delay configurado)
                asyncio.create_task(_aprovar_join_request(
                    bot_token_local, canal_tg_id, user_id_req, cfg.get("delay_seg", 0),
                    canal_nome_req,
                ))
                print(f"[JOIN REQ] {first_name_req} (id {user_id_req}) — auto-aprovação em {cfg.get('delay_seg',0)}s")
            else:
                print(f"[JOIN REQ] {first_name_req} (id {user_id_req}) — aguardando aprovação manual")
        return {"ok": True}

    # ── Bot foi adicionado/removido como admin de um canal ──
    my_chat_member = update.get("my_chat_member")
    if my_chat_member:
        chat       = my_chat_member.get("chat", {})
        new_status = my_chat_member.get("new_chat_member", {}).get("status", "")
        old_status = my_chat_member.get("old_chat_member", {}).get("status", "")

        chat_type = chat.get("type", "")
        if chat_type in ("channel", "supergroup"):
            chat_id    = chat.get("id")
            chat_title = chat.get("title", "")
            chat_uname = ("@" + chat.get("username")) if chat.get("username") else ""

            bot_added = new_status in ("administrator", "member") and old_status in ("left", "kicked", "")
            bot_removed = new_status in ("left", "kicked") and old_status in ("administrator", "member")

            if bot_added:
                try:
                    existing = db.table("telegram_canais").select("id").eq("telegram_id", str(chat_id)).execute()
                    if not existing.data:
                        db.table("telegram_canais").insert({
                            "nome": chat_title,
                            "username": chat_uname,
                            "telegram_id": str(chat_id),
                            "link": "",
                        }).execute()
                        print(f"[CANAL AUTO] Canal detectado e salvo: {chat_title} ({chat_uname}) id={chat_id}")
                    else:
                        db.table("telegram_canais").update({"nome": chat_title, "username": chat_uname}).eq("telegram_id", str(chat_id)).execute()
                        print(f"[CANAL AUTO] Canal atualizado: {chat_title}")
                except Exception as e:
                    print(f"[CANAL AUTO ERRO] {e}")

            elif bot_removed:
                try:
                    db.table("telegram_canais").delete().eq("telegram_id", str(chat_id)).execute()
                    print(f"[CANAL AUTO] Canal removido: {chat_title}")
                except Exception as e:
                    print(f"[CANAL AUTO ERRO] {e}")

        return {"ok": True}

    # ── Post novo no canal (auto-boost) ──
    channel_post = update.get("channel_post")
    if channel_post:
        try:
            chat = channel_post.get("chat", {})
            chat_id_raw = chat.get("id")
            chat_id = str(chat_id_raw) if chat_id_raw is not None else ""
            msg_id = channel_post.get("message_id")
            chat_title = chat.get("title", "")
            chat_uname = chat.get("username")
            cfg_rows = (db.table("booster_auto").select("*")
                        .eq("canal_telegram_id", chat_id)
                        .eq("ativo", True).execute().data) or []
            if cfg_rows and msg_id:
                cfg = cfg_rows[0]
                # Constrói link
                if chat_uname:
                    link = f"https://t.me/{chat_uname}/{msg_id}"
                else:
                    id_for_link = chat_id.replace("-100", "") if chat_id.startswith("-100") else chat_id
                    link = f"https://t.me/c/{id_for_link}/{msg_id}"

                async def _agendar_campanha_auto():
                    aguardar = int(cfg.get("aguardar_min_antes") or 0)
                    if aguardar > 0:
                        await asyncio.sleep(aguardar * 60)
                    try:
                        db.table("booster_campanhas").insert({
                            "nome":             f"🔄 Auto: {chat_title} #{msg_id}",
                            "canal_link":       link,
                            "msg_id":           msg_id,
                            "qtd_views":        cfg.get("qtd_views") or 0,
                            "qtd_reacoes":      cfg.get("qtd_reacoes") or 0,
                            "reacoes_emojis":   cfg.get("reacoes_emojis") or ["👍"],
                            "delay_min_seg":    cfg.get("delay_min_seg") or 2,
                            "delay_max_seg":    cfg.get("delay_max_seg") or 30,
                            "janela_min":       cfg.get("janela_min") or 30,
                            "status":           "pendente",
                        }).execute()
                        print(f"[BOOSTER AUTO] campanha criada pra {chat_title} #{msg_id}")
                    except Exception as e:
                        print(f"[BOOSTER AUTO ERRO insert] {e}")

                asyncio.create_task(_agendar_campanha_auto())
        except Exception as e:
            print(f"[BOOSTER AUTO ERRO] {e}")
        return {"ok": True}

    # ── Mensagem privada recebida (resposta do usuário no DM do bot) ──
    msg_in = update.get("message")
    if msg_in and msg_in.get("chat", {}).get("type") == "private":
        from_u = msg_in.get("from", {})
        # Detecta tipo + conteúdo
        tipo_in = "texto"
        midia_in = ""
        if msg_in.get("photo"):
            tipo_in = "foto"
        elif msg_in.get("video"):
            tipo_in = "video"
        elif msg_in.get("animation"):
            tipo_in = "animacao"
        elif msg_in.get("voice") or msg_in.get("audio"):
            tipo_in = "audio"
        elif msg_in.get("document"):
            tipo_in = "documento"
        conteudo_in = msg_in.get("text") or msg_in.get("caption") or ""
        # Não persiste comandos como /start (apenas eventos relevantes pra inbox)
        if conteudo_in.strip() != "/start":
            salvar_msg_bot(
                direcao="in", user=from_u,
                evento="user_reply", tipo=tipo_in,
                conteudo=conteudo_in, midia_url=midia_in,
                status="recebida",
            )
            print(f"[INBOX IN] @{from_u.get('username') or from_u.get('id')}: {conteudo_in[:60]}")
        return {"ok": True}

    # ── Usuário entrou/saiu do canal ──
    chat_member = update.get("chat_member")
    if not chat_member:
        return {"ok": True}

    chat_evento = chat_member.get("chat", {})
    canal_nome = chat_evento.get("title", "")

    user        = chat_member.get("new_chat_member", {}).get("user", {})
    new_status  = chat_member.get("new_chat_member", {}).get("status", "")
    old_status  = chat_member.get("old_chat_member", {}).get("status", "")

    user_id    = user.get("id")
    username   = user.get("username")
    first_name = user.get("first_name", "")
    last_name  = user.get("last_name", "")

    joined = old_status in ("left", "kicked") and new_status == "member"
    left   = old_status == "member" and new_status in ("left", "kicked")

    if not joined and not left:
        return {"ok": True}

    event = "join" if joined else "leave"
    event_name_meta = "JoinChannel" if joined else "LeaveChannel"

    registro = {
        "user_id":    user_id,
        "username":   username,
        "first_name": first_name,
        "last_name":  last_name,
        "event":      event,
    }

    try:
        db.table("telegram_members").insert(registro).execute()
    except Exception as e:
        print(f"[TELEGRAM ERRO] {e}")

    # Para JoinChannel/LeaveChannel: usa snapshot da última /tracker/entrada
    # action_source=website (igual ao funil do Pixel JS, melhor atribuição)
    await enviar_meta(event_name_meta, first_name=first_name, last_name=last_name,
                      telegram_user_id=str(user_id) if user_id else None, canal_nome=canal_nome,
                      action_source="website")

    # ── Mensagem de saída: tenta DM (só funciona se user deu /start no bot) ──
    if left and user_id:
        try:
            canal_row = db.table("telegram_canais").select("id").eq("telegram_id", str(chat_evento.get("id"))).execute()
            canal_id_interno = canal_row.data[0]["id"] if canal_row.data else None
        except Exception:
            canal_id_interno = None
        if canal_id_interno is not None:
            saida = _bv_saida_cfg(canal_id_interno)
            if saida.get("ativo"):
                bot_token_local = TELEGRAM_BOT_TOKEN
                try:
                    r2 = db.table("configuracoes").select("valor").eq("chave","telegram_bot_token").execute()
                    if r2.data: bot_token_local = r2.data[0]["valor"]
                except Exception:
                    pass
                if bot_token_local:
                    msg_renderizada = _render_msg(saida.get("mensagem", ""), user)
                    botoes_render = [
                        {"texto": _render_msg(b.get("texto",""), user), "url": b.get("url","")}
                        for b in (saida.get("botoes") or [])
                    ]
                    asyncio.create_task(_enviar_dm_boas_vindas(
                        bot_token_local, user_id, msg_renderizada,
                        saida.get("parse_mode", "HTML"), canal_nome, str(user_id),
                        tipo=saida.get("tipo", "texto"),
                        midia_url=saida.get("midia_url", ""),
                        botoes=botoes_render,
                        canal_id=canal_id_interno, evento="leave",
                        user_obj=user,
                    ))
                    print(f"[SAIDA DM] disparado pra {first_name} (user {user_id})")

    print(f"[TELEGRAM] {event.upper()} — @{username or user_id} ({first_name} {last_name})")
    return {"ok": True}


@app.get("/config/telegram")
def ler_config_telegram():
    try:
        result = db.table("configuracoes").select("chave,valor").in_("chave", ["telegram_bot_token", "telegram_bot_username"]).execute()
        cfg = {r["chave"]: r["valor"] for r in (result.data or [])}
        token = cfg.get("telegram_bot_token") or TELEGRAM_BOT_TOKEN
        username = cfg.get("telegram_bot_username", "")
        canais_res = db.table("telegram_canais").select("*").execute()
        canais = canais_res.data or []
        return {"token": token or "", "username": username, "configurado": bool(token), "canais": canais}
    except Exception:
        return {"token": "", "username": "", "configurado": False, "canais": []}

@app.post("/config/telegram")
async def salvar_config_telegram(request: Request):
    global TELEGRAM_BOT_TOKEN
    data = await request.json()
    token = data.get("token", "").strip()
    username = data.get("username", "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="token é obrigatório")
    try:
        for chave, valor in [("telegram_bot_token", token), ("telegram_bot_username", username)]:
            existing = db.table("configuracoes").select("chave").eq("chave", chave).execute()
            if existing.data:
                db.table("configuracoes").update({"valor": valor}).eq("chave", chave).execute()
            else:
                db.table("configuracoes").insert({"chave": chave, "valor": valor}).execute()
        TELEGRAM_BOT_TOKEN = token
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Configurar webhook AUTOMATICAMENTE para capturar my_chat_member quando o bot for adicionado
    try:
        webhook_url = str(request.base_url).rstrip("/").replace("http://", "https://") + "/telegram/webhook"
        secret_token = _get_telegram_secret_token()
        async with httpx.AsyncClient(timeout=15) as client:
            wh_resp = await client.post(
                f"https://api.telegram.org/bot{token}/setWebhook",
                json={"url": webhook_url, "allowed_updates": ["chat_member", "my_chat_member", "chat_join_request", "message", "channel_post"],
                      "secret_token": secret_token}
            )
            print(f"[CONFIG] Webhook setup: {wh_resp.json()}")
    except Exception as e:
        print(f"[CONFIG] Webhook setup falhou: {e}")

    print(f"[CONFIG] Telegram Bot atualizado: {username}")
    return {"status": "ok"}

@app.delete("/config/telegram")
async def remover_config_telegram():
    global TELEGRAM_BOT_TOKEN
    try:
        for chave in ["telegram_bot_token", "telegram_bot_username"]:
            db.table("configuracoes").delete().eq("chave", chave).execute()
        TELEGRAM_BOT_TOKEN = ""
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "ok"}

@app.post("/config/telegram/canal")
async def adicionar_canal_telegram(request: Request):
    data = await request.json()
    nome = data.get("nome", "").strip()
    username = data.get("username", "").strip()
    link = data.get("link", "").strip()
    if not nome:
        raise HTTPException(status_code=400, detail="nome é obrigatório")
    try:
        result = db.table("telegram_canais").insert({"nome": nome, "username": username, "link": link}).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "ok", "id": result.data[0]["id"]}

def _client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else ""

def _client_ua(request: Request) -> str:
    return request.headers.get("user-agent", "")[:500]

@app.post("/tracker/pageview")
async def tracker_pageview(request: Request):
    try:
        data = await request.json()
    except Exception:
        return {"ok": True}
    canal_id = data.get("channel_id")
    page_url = data.get("page_url", "")
    utms = {k: data.get(k) for k in ["utm_source","utm_medium","utm_campaign","utm_content","utm_term"] if data.get(k)}
    try:
        db.table("tracker_pageviews").insert({
            "canal_id": canal_id,
            "page_url": page_url,
            **utms
        }).execute()
    except Exception as e:
        print(f"[PAGEVIEW ERRO] {e}")
    return {"ok": True}

@app.post("/tracker/entrada")
async def tracker_entrada(request: Request):
    try:
        data = await request.json()
    except Exception:
        return {"ok": True}
    canal_id = data.get("channel_id")
    page_url = data.get("page_url", "")
    utms = {k: data.get(k) for k in ["utm_source","utm_medium","utm_campaign","utm_content","utm_term"] if data.get(k)}

    # Dados de atribuição Meta + identificadores
    fbc         = data.get("fbc") or ""
    fbp         = data.get("fbp") or ""
    external_id = data.get("external_id") or ""
    referrer    = data.get("referrer") or ""
    client_ip   = _client_ip(request)
    user_agent  = data.get("user_agent") or _client_ua(request)

    # Snapshot mais recente para uso quando JoinChannel disparar
    try:
        _set_cfg("_ultima_atribuicao", json.dumps({
            "fbc": fbc, "fbp": fbp, "external_id": external_id,
            "client_ip": client_ip, "user_agent": user_agent,
            "page_url": page_url, "referrer": referrer,
            "canal_id": canal_id,
            "ts": int(time.time()),
            **utms,
        }))
    except Exception as e:
        print(f"[ENTRADA SNAP ERRO] {e}")

    try:
        registro = {"canal_id": canal_id, "page_url": page_url, **utms}
        # Tenta salvar campos extras (vão falhar silenciosamente se a coluna não existir)
        for k, v in [("fbc", fbc), ("fbp", fbp), ("external_id", external_id),
                     ("client_ip", client_ip), ("user_agent", user_agent), ("referrer", referrer)]:
            if v: registro[k] = v
        db.table("tracker_entradas").insert(registro).execute()
    except Exception as e:
        # Fallback se faltar coluna nova: só insere campos antigos
        try:
            db.table("tracker_entradas").insert({"canal_id": canal_id, "page_url": page_url, **utms}).execute()
        except Exception as e2:
            print(f"[ENTRADA ERRO] {e2}")
        print(f"[ENTRADA EXTRA ERRO] {e}")
    return {"ok": True}

@app.get("/leads")
def get_leads():
    try:
        # 1. Cadastros como fonte principal
        cads = db.table("cadastros").select("*").execute().data or []
        cads.sort(key=lambda x: x.get("created_at") or "", reverse=True)

        # 2. Telegram members: status mais recente por primeiro nome
        all_events = db.table("telegram_members").select("*").execute().data or []
        all_events.sort(key=lambda x: x.get("created_at") or "")
        first_join_by_name = {}   # primeiro_nome -> created_at do primeiro join
        latest_by_name = {}       # primeiro_nome -> evento mais recente
        for ev in all_events:
            first = (ev.get("first_name") or "").lower().strip()
            if not first: continue
            if ev.get("event") == "join" and first not in first_join_by_name:
                first_join_by_name[first] = ev.get("created_at")
            latest_by_name[first] = ev

        # 3. Depósitos indexados por email
        deps = db.table("depositos").select("*").execute().data or []
        deps.sort(key=lambda x: x.get("created_at") or "")
        dep_by_email = {}
        for d in deps:
            email = d.get("email", "")
            if not email: continue
            if email not in dep_by_email:
                dep_by_email[email] = {"ftd": d.get("created_at"), "count": 0, "ltv": 0.0}
            dep_by_email[email]["count"] += 1
            dep_by_email[email]["ltv"] += float(d.get("valor") or 0)

        # 4. Canal principal
        canais = db.table("telegram_canais").select("nome").execute().data or []
        canal_nome = canais[0]["nome"] if canais else "—"

        # 5. Monta lista com cadastros + enriquece com Telegram
        leads = []
        for cad in cads:
            email = cad.get("email", "")
            dep = dep_by_email.get(email, {})
            parts = (cad.get("nome") or "").split()
            first_key = parts[0].lower().strip() if parts else ""
            tg = latest_by_name.get(first_key)
            entrada = first_join_by_name.get(first_key) if tg else None
            saiu_em = tg.get("created_at") if tg and tg.get("event") == "leave" else None
            status = tg.get("event") if tg else None
            leads.append({
                "user_id": tg.get("user_id") if tg else None,
                "username": tg.get("username") or "" if tg else "",
                "first_name": parts[0] if parts else "",
                "last_name": " ".join(parts[1:]) if len(parts) > 1 else "",
                "nome": cad.get("nome") or "",
                "email": email,
                "telefone": cad.get("telefone") or "",
                "canal": canal_nome,
                "utm_source": cad.get("utm_source") or "",
                "utm_medium": cad.get("utm_medium") or "",
                "utm_campaign": cad.get("utm_campaign") or "",
                "page_url": cad.get("page_url") or cad.get("utm_content") or "",
                "entrada": entrada,
                "registro": cad.get("created_at"),
                "ftd": dep.get("ftd"),
                "depositos": dep.get("count", 0),
                "ltv": dep.get("ltv", 0.0),
                "status": status,
                "saiu_em": saiu_em,
            })

        return {"leads": leads}
    except Exception as e:
        print(f"[LEADS ERRO] {e}")
        return {"leads": []}

@app.get("/telegram/members-status")
def telegram_members_status():
    """Retorna o status atual (join/leave mais recente) por user_id."""
    try:
        r = db.table("telegram_members").select("user_id,first_name,last_name,username,event,created_at").execute()
        rows = sorted(r.data or [], key=lambda x: x.get("created_at") or "", reverse=True)
        # Pega o evento mais recente por user_id
        by_user = {}
        for row in rows:
            uid = row.get("user_id")
            if not uid:
                continue
            if uid not in by_user:
                by_user[uid] = row
        return {"members": list(by_user.values())}
    except Exception as e:
        return {"members": []}

# ── Solicitações de entrada (Telegram chat_join_request) ──────────
def _aprov_cfg(canal_id: int) -> dict:
    """Config de auto-aprovação por canal (salva em configuracoes)."""
    raw = _get_cfg(f"aprov_{canal_id}")
    if raw:
        try: return json.loads(raw)
        except Exception: pass
    return {"auto_aprovar": False, "delay_seg": 0}


def _bv_cfg(canal_id: int) -> dict:
    """Config de boas-vindas por canal."""
    raw = _get_cfg(f"bv_{canal_id}")
    if raw:
        try:
            cfg = json.loads(raw)
            # Defaults pra retrocompat
            cfg.setdefault("tipo", "texto")
            cfg.setdefault("midia_url", "")
            cfg.setdefault("botoes", [])
            return cfg
        except Exception:
            pass
    return {
        "ativo": False,
        "mensagem": "Olá {primeiro_nome}! 👋\n\nSeja muito bem-vindo(a)! Em instantes você será aprovado no canal.",
        "parse_mode": "HTML",
        "tipo": "texto",       # texto | foto | video | animacao
        "midia_url": "",
        "botoes": [],          # [{"texto": "...", "url": "..."}, ...]
    }


def _bv_saida_cfg(canal_id: int) -> dict:
    """Config de mensagem de saída por canal."""
    raw = _get_cfg(f"bvsaida_{canal_id}")
    if raw:
        try:
            cfg = json.loads(raw)
            cfg.setdefault("tipo", "texto")
            cfg.setdefault("midia_url", "")
            cfg.setdefault("botoes", [])
            return cfg
        except Exception:
            pass
    return {
        "ativo": False,
        "mensagem": "Olá {primeiro_nome}, vimos que você saiu do canal. 😢\n\nSe foi por engano ou se quiser voltar, é só clicar abaixo.",
        "parse_mode": "HTML",
        "tipo": "texto",
        "midia_url": "",
        "botoes": [],
    }


def salvar_msg_bot(direcao: str, user: dict, canal_id: int = None, canal_nome: str = "",
                    evento: str = "", tipo: str = "texto", conteudo: str = "",
                    midia_url: str = "", botoes: list = None,
                    status: str = "", response_code: int = 0, response_body: str = ""):
    """Persiste uma mensagem trocada com o usuário (entrada ou saída) na tabela bot_messages."""
    try:
        db.table("bot_messages").insert({
            "user_id":       str(user.get("id") or user.get("user_id") or ""),
            "username":      user.get("username"),
            "first_name":    user.get("first_name"),
            "last_name":     user.get("last_name"),
            "canal_id":      canal_id,
            "canal_nome":    canal_nome,
            "direcao":       direcao,
            "evento":        evento,
            "tipo":          tipo or "texto",
            "conteudo":      conteudo or "",
            "midia_url":     midia_url or "",
            "botoes":        botoes or [],
            "status":        status,
            "response_code": response_code,
            "response_body": (response_body or "")[:500],
        }).execute()
    except Exception as e:
        print(f"[BOT_MSG ERRO] {e}")


def _build_inline_keyboard(botoes: list) -> dict:
    """Converte lista [{texto,url}, ...] em reply_markup do Telegram."""
    if not botoes:
        return None
    rows = []
    for b in botoes:
        texto = (b.get("texto") or "").strip()
        url = (b.get("url") or "").strip()
        if not texto or not url:
            continue
        rows.append([{"text": texto, "url": url}])
    if not rows:
        return None
    return {"inline_keyboard": rows}


def _render_msg(template: str, user: dict) -> str:
    """Substitui placeholders {nome}, {primeiro_nome}, {username} na mensagem."""
    primeiro = user.get("first_name", "") or ""
    ultimo   = user.get("last_name", "") or ""
    nome_completo = (primeiro + " " + ultimo).strip() or "amigo(a)"
    username = user.get("username", "") or ""
    return (template
            .replace("{nome}",          nome_completo)
            .replace("{primeiro_nome}", primeiro or "amigo(a)")
            .replace("{username}",      ("@" + username) if username else ""))


async def _enviar_dm_boas_vindas(bot_token: str, user_chat_id, mensagem: str, parse_mode: str = "HTML",
                                  canal_nome: str = "", user_id_log: str = "",
                                  tipo: str = "texto", midia_url: str = "", botoes: list = None,
                                  canal_id: int = None, evento: str = "welcome",
                                  user_obj: dict = None):
    """Envia DM de boas-vindas via user_chat_id (janela de 5min do join_request).

    Suporta texto, foto, vídeo, animação (GIF) e botões inline com URL.
    Persiste a mensagem na tabela bot_messages pra alimentar o Inbox.
    """
    tipo = (tipo or "texto").lower()
    midia_url = (midia_url or "").strip()
    reply_markup = _build_inline_keyboard(botoes or [])

    # Determina endpoint + payload baseado no tipo
    if tipo == "foto" and midia_url:
        endpoint = "sendPhoto"
        payload = {"chat_id": user_chat_id, "photo": midia_url, "caption": mensagem}
    elif tipo == "video" and midia_url:
        endpoint = "sendVideo"
        payload = {"chat_id": user_chat_id, "video": midia_url, "caption": mensagem,
                   "supports_streaming": True}
    elif tipo == "animacao" and midia_url:
        endpoint = "sendAnimation"
        payload = {"chat_id": user_chat_id, "animation": midia_url, "caption": mensagem}
    else:
        endpoint = "sendMessage"
        payload = {"chat_id": user_chat_id, "text": mensagem, "disable_web_page_preview": True}

    if parse_mode in ("HTML", "Markdown", "MarkdownV2"):
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(
                f"https://api.telegram.org/bot{bot_token}/{endpoint}",
                json=payload,
            )
            d = r.json()
        ok = bool(d.get("ok"))
        status = "sucesso" if ok else "erro"
        salvar_log_conversao(
            "telegram", "WelcomeDM", status, r.status_code, json.dumps(d, ensure_ascii=False)[:500],
            telegram_user_id=str(user_id_log), canal_nome=canal_nome, direcao="enviado",
        )
        # Persiste mensagem no Inbox
        salvar_msg_bot(
            direcao="out",
            user=(user_obj or {"id": user_id_log}),
            canal_id=canal_id, canal_nome=canal_nome, evento=evento,
            tipo=tipo, conteudo=mensagem, midia_url=midia_url, botoes=botoes or [],
            status=status, response_code=r.status_code,
            response_body=json.dumps(d, ensure_ascii=False)[:500],
        )
        print(f"[BOAS-VINDAS {endpoint}] user={user_id_log} ok={ok} resp={d}")
        return ok
    except Exception as e:
        salvar_log_conversao(
            "telegram", "WelcomeDM", "erro", 0, str(e)[:500],
            telegram_user_id=str(user_id_log), canal_nome=canal_nome, direcao="enviado",
        )
        salvar_msg_bot(
            direcao="out",
            user=(user_obj or {"id": user_id_log}),
            canal_id=canal_id, canal_nome=canal_nome, evento=evento,
            tipo=tipo, conteudo=mensagem, midia_url=midia_url, botoes=botoes or [],
            status="erro", response_code=0, response_body=str(e)[:500],
        )
        print(f"[BOAS-VINDAS DM ERRO] {e}")
        return False


@app.get("/canais/{canal_id}/aprovacao")
def get_aprovacao_canal(canal_id: int):
    return _aprov_cfg(canal_id)


@app.post("/canais/{canal_id}/aprovacao")
async def salvar_aprovacao_canal(canal_id: int, request: Request):
    body = await request.json()
    cfg = {
        "auto_aprovar": bool(body.get("auto_aprovar")),
        "delay_seg":    int(body.get("delay_seg", 0)),
    }
    _set_cfg(f"aprov_{canal_id}", json.dumps(cfg))
    return {"ok": True, **cfg}


@app.get("/canais/{canal_id}/boas-vindas")
def get_boas_vindas(canal_id: int):
    return _bv_cfg(canal_id)


@app.post("/canais/{canal_id}/boas-vindas")
async def salvar_boas_vindas(canal_id: int, request: Request):
    body = await request.json()
    # Sanitiza botões
    botoes_in = body.get("botoes") or []
    botoes = []
    for b in botoes_in:
        t = (b.get("texto") or "").strip()
        u = (b.get("url") or "").strip()
        if t and u:
            botoes.append({"texto": t[:64], "url": u[:512]})
    tipo = (body.get("tipo") or "texto").lower()
    if tipo not in ("texto", "foto", "video", "animacao"):
        tipo = "texto"
    cfg = {
        "ativo":      bool(body.get("ativo")),
        "mensagem":   (body.get("mensagem") or "").strip(),
        "parse_mode": body.get("parse_mode") or "HTML",
        "tipo":       tipo,
        "midia_url":  (body.get("midia_url") or "").strip(),
        "botoes":     botoes,
    }
    _set_cfg(f"bv_{canal_id}", json.dumps(cfg, ensure_ascii=False))
    return {"ok": True, **cfg}


@app.post("/canais/{canal_id}/boas-vindas/testar")
async def testar_boas_vindas(canal_id: int, request: Request):
    """Envia uma DM de teste pra um user_id específico (deve ter dado /start no bot antes)."""
    body = await request.json()
    user_id = body.get("user_id")
    if not user_id:
        raise HTTPException(status_code=400, detail="user_id é obrigatório")
    bv = _bv_cfg(canal_id)
    bot_token = TELEGRAM_BOT_TOKEN
    try:
        r = db.table("configuracoes").select("valor").eq("chave","telegram_bot_token").execute()
        if r.data: bot_token = r.data[0]["valor"]
    except Exception:
        pass
    if not bot_token:
        raise HTTPException(status_code=400, detail="Bot não configurado")
    canal = db.table("telegram_canais").select("nome").eq("id", canal_id).execute()
    canal_nome = canal.data[0]["nome"] if canal.data else ""
    fake_user = {"first_name": "Teste", "username": "teste", "last_name": "", "id": user_id}
    msg = _render_msg(bv.get("mensagem", ""), fake_user)
    botoes_render = [
        {"texto": _render_msg(b.get("texto",""), fake_user), "url": b.get("url","")}
        for b in (bv.get("botoes") or [])
    ]
    ok = await _enviar_dm_boas_vindas(
        bot_token, user_id, msg, bv.get("parse_mode","HTML"), canal_nome, str(user_id),
        tipo=bv.get("tipo","texto"), midia_url=bv.get("midia_url",""), botoes=botoes_render,
        canal_id=canal_id, evento="welcome_test", user_obj=fake_user,
    )
    return {"ok": ok}


@app.get("/canais/{canal_id}/saida")
def get_saida(canal_id: int):
    return _bv_saida_cfg(canal_id)


@app.post("/canais/{canal_id}/saida")
async def salvar_saida(canal_id: int, request: Request):
    body = await request.json()
    botoes_in = body.get("botoes") or []
    botoes = []
    for b in botoes_in:
        t = (b.get("texto") or "").strip()
        u = (b.get("url") or "").strip()
        if t and u:
            botoes.append({"texto": t[:64], "url": u[:512]})
    tipo = (body.get("tipo") or "texto").lower()
    if tipo not in ("texto", "foto", "video", "animacao"):
        tipo = "texto"
    cfg = {
        "ativo":      bool(body.get("ativo")),
        "mensagem":   (body.get("mensagem") or "").strip(),
        "parse_mode": body.get("parse_mode") or "HTML",
        "tipo":       tipo,
        "midia_url":  (body.get("midia_url") or "").strip(),
        "botoes":     botoes,
    }
    _set_cfg(f"bvsaida_{canal_id}", json.dumps(cfg, ensure_ascii=False))
    return {"ok": True, **cfg}


@app.post("/canais/{canal_id}/saida/testar")
async def testar_saida(canal_id: int, request: Request):
    """Envia DM de saída de teste pra um user_id (precisa /start prévio no bot)."""
    body = await request.json()
    user_id = body.get("user_id")
    if not user_id:
        raise HTTPException(status_code=400, detail="user_id é obrigatório")
    saida = _bv_saida_cfg(canal_id)
    bot_token = TELEGRAM_BOT_TOKEN
    try:
        r = db.table("configuracoes").select("valor").eq("chave","telegram_bot_token").execute()
        if r.data: bot_token = r.data[0]["valor"]
    except Exception:
        pass
    if not bot_token:
        raise HTTPException(status_code=400, detail="Bot não configurado")
    canal = db.table("telegram_canais").select("nome").eq("id", canal_id).execute()
    canal_nome = canal.data[0]["nome"] if canal.data else ""
    fake_user = {"first_name": "Teste", "username": "teste", "last_name": "", "id": user_id}
    msg = _render_msg(saida.get("mensagem", ""), fake_user)
    botoes_render = [
        {"texto": _render_msg(b.get("texto",""), fake_user), "url": b.get("url","")}
        for b in (saida.get("botoes") or [])
    ]
    ok = await _enviar_dm_boas_vindas(
        bot_token, user_id, msg, saida.get("parse_mode","HTML"), canal_nome, str(user_id),
        tipo=saida.get("tipo","texto"), midia_url=saida.get("midia_url",""), botoes=botoes_render,
        canal_id=canal_id, evento="leave_test", user_obj=fake_user,
    )
    return {"ok": ok}


@app.post("/canais/{canal_id}/gerar-link")
async def gerar_link_solicitacao(canal_id: int, request: Request):
    """Cria um novo invite link com creates_join_request=true."""
    body = await request.json() if request.headers.get("content-length") else {}
    bot_token = TELEGRAM_BOT_TOKEN
    try:
        r = db.table("configuracoes").select("valor").eq("chave","telegram_bot_token").execute()
        if r.data: bot_token = r.data[0]["valor"]
    except Exception:
        pass
    if not bot_token:
        raise HTTPException(status_code=400, detail="Bot não configurado")
    canal = db.table("telegram_canais").select("*").eq("id", canal_id).execute()
    if not canal.data:
        raise HTTPException(status_code=404, detail="Canal não encontrado")
    tg_id = canal.data[0].get("telegram_id")
    if not tg_id:
        raise HTTPException(status_code=400, detail="Canal sem telegram_id (precisa estar admin lá)")

    nome = body.get("nome", "trackfy_solicitacao")
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"https://api.telegram.org/bot{bot_token}/createChatInviteLink",
            json={"chat_id": tg_id, "name": nome[:32], "creates_join_request": True},
        )
    d = resp.json()
    if not d.get("ok"):
        raise HTTPException(status_code=400, detail=d.get("description","erro Telegram"))
    invite = d["result"]["invite_link"]
    # Atualiza no banco
    try:
        db.table("telegram_canais").update({"link": invite}).eq("id", canal_id).execute()
    except Exception as e:
        print(f"[GERAR LINK] erro update: {e}")
    return {"ok": True, "invite_link": invite}


async def _aprovar_join_request(bot_token: str, chat_id, user_id, delay_seg: int = 0, canal_nome: str = ""):
    """Aprova um pedido de entrada após delay opcional."""
    if delay_seg > 0:
        await asyncio.sleep(delay_seg)
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                f"https://api.telegram.org/bot{bot_token}/approveChatJoinRequest",
                json={"chat_id": chat_id, "user_id": user_id},
            )
            d = r.json()
            ok = bool(d.get("ok"))
            salvar_log_conversao(
                "telegram", "JoinApproved", "sucesso" if ok else "erro",
                r.status_code, json.dumps(d, ensure_ascii=False)[:500],
                telegram_user_id=str(user_id), canal_nome=canal_nome, direcao="enviado",
            )
            print(f"[APROVAR JOIN] chat={chat_id} user={user_id} → {d}")
    except Exception as e:
        salvar_log_conversao(
            "telegram", "JoinApproved", "erro", 0, str(e)[:500],
            telegram_user_id=str(user_id), canal_nome=canal_nome, direcao="enviado",
        )
        print(f"[APROVAR JOIN ERRO] {e}")


# ── Inbox: histórico de mensagens trocadas com usuários ──────────
@app.get("/inbox/diag")
def inbox_diag():
    """Diagnóstico: verifica se a tabela existe e quantos registros tem."""
    try:
        r = db.table("bot_messages").select("id", count="exact").limit(1).execute()
        total = getattr(r, "count", None) or 0
        return {"ok": True, "tabela_existe": True, "total_registros": total}
    except Exception as e:
        return {"ok": False, "tabela_existe": False, "erro": str(e)}


@app.get("/inbox/threads")
def inbox_threads(canal_id: int = None, limit: int = 100):
    """Lista threads (1 por usuário) com a última mensagem e contagem total."""
    try:
        q = db.table("bot_messages").select("*").order("created_at", desc=True).limit(2000)
        if canal_id:
            q = q.eq("canal_id", canal_id)
        rows = q.execute().data or []
        threads = {}
        for r in rows:
            uid = r.get("user_id") or ""
            if not uid:
                continue
            if uid not in threads:
                threads[uid] = {
                    "user_id":     uid,
                    "username":    r.get("username"),
                    "first_name":  r.get("first_name"),
                    "last_name":   r.get("last_name"),
                    "canal_id":    r.get("canal_id"),
                    "canal_nome":  r.get("canal_nome"),
                    "ultima_msg":  r.get("conteudo") or "",
                    "ultima_dir":  r.get("direcao"),
                    "ultima_em":   r.get("created_at"),
                    "ultimo_tipo": r.get("tipo"),
                    "total":       1,
                    "tem_resposta": (r.get("direcao") == "in"),
                }
            else:
                threads[uid]["total"] += 1
                if r.get("direcao") == "in":
                    threads[uid]["tem_resposta"] = True
        lista = sorted(threads.values(), key=lambda x: x.get("ultima_em") or "", reverse=True)
        return {"threads": lista[:limit]}
    except Exception as e:
        print(f"[INBOX threads ERRO] {e}")
        return {"threads": []}


@app.get("/inbox/thread/{user_id}")
def inbox_thread(user_id: str, limit: int = 200):
    """Histórico completo de mensagens com um usuário (ordem cronológica)."""
    try:
        rows = (db.table("bot_messages").select("*")
                .eq("user_id", str(user_id))
                .order("created_at", desc=False)
                .limit(limit).execute().data) or []
        return {"messages": rows}
    except Exception as e:
        print(f"[INBOX thread ERRO] {e}")
        return {"messages": []}


@app.post("/inbox/reply")
async def inbox_reply(request: Request):
    """Envia resposta manual do admin pra um usuário (precisa /start prévio)."""
    body = await request.json()
    user_id = body.get("user_id")
    texto = (body.get("texto") or "").strip()
    if not user_id or not texto:
        raise HTTPException(status_code=400, detail="user_id e texto são obrigatórios")
    bot_token = TELEGRAM_BOT_TOKEN
    try:
        r = db.table("configuracoes").select("valor").eq("chave","telegram_bot_token").execute()
        if r.data: bot_token = r.data[0]["valor"]
    except Exception:
        pass
    if not bot_token:
        raise HTTPException(status_code=400, detail="Bot não configurado")
    last = (db.table("bot_messages").select("*").eq("user_id", str(user_id))
            .order("created_at", desc=True).limit(1).execute().data) or []
    user_info = {
        "id": user_id,
        "first_name": (last[0].get("first_name") if last else "") or "",
        "username":   (last[0].get("username") if last else "") or "",
        "last_name":  (last[0].get("last_name") if last else "") or "",
    }
    canal_id_use = last[0].get("canal_id") if last else None
    canal_nome_use = last[0].get("canal_nome") if last else ""
    ok = await _enviar_dm_boas_vindas(
        bot_token, user_id, texto, "HTML", canal_nome_use, str(user_id),
        tipo="texto", midia_url="", botoes=[],
        canal_id=canal_id_use, evento="manual", user_obj=user_info,
    )
    return {"ok": ok}


@app.get("/conversion-logs")
def conversion_logs(
    plataforma: str = None,
    event_name: str = None,
    status: str = None,
    canal_nome: str = None,
    telegram_user_id: str = None,
    direcao: str = "enviado",
    data_inicio: str = None,
    data_fim: str = None,
    limit: int = 200,
):
    """Lista logs de conversão com filtros."""
    try:
        q = db.table("conversion_logs").select("*").order("created_at", desc=True).limit(limit)
        if plataforma:    q = q.eq("plataforma", plataforma)
        if event_name:    q = q.eq("event_name", event_name)
        if status:        q = q.eq("status", status)
        if canal_nome:    q = q.eq("canal_nome", canal_nome)
        if telegram_user_id: q = q.ilike("telegram_user_id", f"%{telegram_user_id}%")
        if direcao:       q = q.eq("direcao", direcao)
        try:
            tz_offset = int(_get_cfg("timezone_offset") or "-3")
        except Exception:
            tz_offset = -3
        tz_str = f"{tz_offset:+03d}:00"
        if data_inicio:   q = q.gte("created_at", data_inicio + "T00:00:00" + tz_str)
        if data_fim:      q = q.lte("created_at", data_fim + "T23:59:59" + tz_str)
        r = q.execute()
        return {"logs": r.data or [], "total": len(r.data or [])}
    except Exception as e:
        print(f"[LOGS ERRO] {e}")
        return {"logs": [], "total": 0, "erro": str(e)}


@app.get("/telegram/members-historico")
def telegram_members_historico():
    """Retorna todos os eventos de entrada/saída com data, separados."""
    try:
        r = db.table("telegram_members").select("*").order("created_at", desc=True).execute()
        rows = r.data or []
        entradas = [x for x in rows if x.get("event") == "join"]
        saidas   = [x for x in rows if x.get("event") == "leave"]
        return {"entradas": entradas, "saidas": saidas, "total_entradas": len(entradas), "total_saidas": len(saidas)}
    except Exception as e:
        print(f"[MEMBROS HIST ERRO] {e}")
        return {"entradas": [], "saidas": [], "total_entradas": 0, "total_saidas": 0}

@app.get("/tracker/stats")
async def tracker_stats(canal_id: str = None, data_inicio: str = None, data_fim: str = None):
    """Retorna métricas do dashboard para o canal e período selecionados."""
    try:
        # Helper to apply date filters com timezone (offset em horas)
        try:
            tz_offset = int(_get_cfg("timezone_offset") or "-3")
        except Exception:
            tz_offset = -3

        def aplicar_datas(q, inicio, fim):
            if inicio:
                # Converte: início do dia local → UTC. Ex: 00:00 BR (UTC-3) = 03:00 UTC
                tz_str = f"{tz_offset:+03d}:00"
                q = q.gte("created_at", inicio + "T00:00:00" + tz_str)
            if fim:
                tz_str = f"{tz_offset:+03d}:00"
                q = q.lte("created_at", fim + "T23:59:59" + tz_str)
            return q

        # PageViews
        q_pv = db.table("tracker_pageviews").select("id,created_at", count="exact")
        if canal_id:
            q_pv = q_pv.eq("canal_id", canal_id)
        q_pv = aplicar_datas(q_pv, data_inicio, data_fim)
        r_pv = q_pv.execute()
        pageviews = r_pv.count or 0

        # Cliques no link t.me (tracker.js)
        q_en = db.table("tracker_entradas").select("id,created_at", count="exact")
        if canal_id:
            q_en = q_en.eq("canal_id", canal_id)
        q_en = aplicar_datas(q_en, data_inicio, data_fim)
        r_en = q_en.execute()
        cliques = r_en.count or 0

        # Entradas = joins no canal Telegram (telegram_members event=join)
        q_jo = db.table("telegram_members").select("user_id,created_at", count="exact").eq("event", "join")
        q_jo = aplicar_datas(q_jo, data_inicio, data_fim)
        r_jo = q_jo.execute()
        joins = r_jo.count or 0
        entradas = joins

        # Saídas (telegram_members event=leave)
        q_sa = db.table("telegram_members").select("user_id,created_at", count="exact").eq("event", "leave")
        q_sa = aplicar_datas(q_sa, data_inicio, data_fim)
        r_sa = q_sa.execute()
        saidas = r_sa.count or 0

        # Tempo médio Entrada → Saída (mesmo user_id)
        import datetime as dt
        tm_entrada_saida = None
        try:
            joins_data = r_jo.data or []
            saidas_data = r_sa.data or []
            # Mapeia user_id → join mais recente
            join_por_user = {}
            for row in joins_data:
                uid = row.get("user_id")
                ts = row.get("created_at","")
                if uid and ts:
                    if uid not in join_por_user or ts > join_por_user[uid]:
                        join_por_user[uid] = ts
            diffs = []
            for row in saidas_data:
                uid = row.get("user_id")
                ts_leave = row.get("created_at","")
                if uid and ts_leave and uid in join_por_user:
                    ts_join = join_por_user[uid]
                    if ts_leave > ts_join:
                        t1 = dt.datetime.fromisoformat(ts_join.replace("Z","+00:00"))
                        t2 = dt.datetime.fromisoformat(ts_leave.replace("Z","+00:00"))
                        diffs.append((t2 - t1).total_seconds())
            if diffs:
                avg_sec = sum(diffs) / len(diffs)
                if avg_sec < 3600:
                    tm_entrada_saida = f"{int(avg_sec//60)} min"
                elif avg_sec < 86400:
                    tm_entrada_saida = f"{int(avg_sec//3600)} h"
                else:
                    tm_entrada_saida = f"{int(avg_sec//86400)} dias"
        except Exception as ex:
            print(f"[TM ERRO] {ex}")

        # Registros (cadastros)
        q_ca = db.table("cadastros").select("id,email,created_at", count="exact")
        q_ca = aplicar_datas(q_ca, data_inicio, data_fim)
        r_ca = q_ca.execute()
        registros = r_ca.count or 0

        # Depósitos para FTD e redep
        q_dep = db.table("depositos").select("id,email,valor,created_at")
        q_dep = aplicar_datas(q_dep, data_inicio, data_fim)
        r_dep = q_dep.execute()
        deps = r_dep.data or []
        sorted_deps = sorted(deps, key=lambda x: x.get("created_at",""))
        seen = set()
        ftd_count = 0
        redep_count = 0
        ftd_valor = 0.0
        redep_valor = 0.0
        for d in sorted_deps:
            email = d.get("email","")
            valor = float(d.get("valor") or 0)
            if email not in seen:
                seen.add(email)
                ftd_count += 1
                ftd_valor += valor
            else:
                redep_count += 1
                redep_valor += valor

        # Rates
        ctr = round(cliques / pageviews * 100, 2) if pageviews > 0 else 0
        conv_telegram = round(joins / cliques * 100, 2) if cliques > 0 else 0
        conv_pagina = round(registros / pageviews * 100, 2) if pageviews > 0 else 0
        retencao = round((joins - saidas) / joins * 100, 2) if joins > 0 else 0

        # Evolução temporal — agrupa por hora se range for 1 dia, senão por dia
        from collections import defaultdict
        import datetime
        pv_data = r_pv.data or []
        jo_data = r_jo.data or []
        sa_data = r_sa.data or []

        hoje = datetime.date.today()
        if data_inicio:
            try:
                d_ini = datetime.date.fromisoformat(data_inicio)
            except Exception:
                d_ini = hoje.replace(day=1)
        else:
            d_ini = hoje.replace(day=1)

        if data_fim:
            try:
                d_fim = datetime.date.fromisoformat(data_fim)
            except Exception:
                d_fim = hoje
        else:
            if d_ini.month == 12:
                d_fim = datetime.date(d_ini.year + 1, 1, 1) - datetime.timedelta(days=1)
            else:
                d_fim = datetime.date(d_ini.year, d_ini.month + 1, 1) - datetime.timedelta(days=1)

        agrupar_por_hora = (d_ini == d_fim)

        if agrupar_por_hora:
            # Granularidade por hora (range = 1 dia)
            pv_por = defaultdict(int)
            jo_por = defaultdict(int)
            sa_por = defaultdict(int)
            for row in pv_data:
                ca = (row.get("created_at") or "")[:13]  # YYYY-MM-DDTHH
                if ca: pv_por[ca] += 1
            for row in jo_data:
                ca = (row.get("created_at") or "")[:13]
                if ca: jo_por[ca] += 1
            for row in sa_data:
                ca = (row.get("created_at") or "")[:13]
                if ca: sa_por[ca] += 1

            evolucao = []
            for h in range(24):
                key = f"{d_ini.isoformat()}T{h:02d}"
                evolucao.append({
                    "data": f"{h:02d}:00",
                    "pageviews": pv_por[key],
                    "entradas": jo_por[key],
                    "saidas": sa_por[key],
                })
        else:
            # Granularidade por dia
            pv_por_dia = defaultdict(int)
            jo_por_dia = defaultdict(int)
            sa_por_dia = defaultdict(int)
            for row in pv_data:
                dia = (row.get("created_at") or "")[:10]
                if dia: pv_por_dia[dia] += 1
            for row in jo_data:
                dia = (row.get("created_at") or "")[:10]
                if dia: jo_por_dia[dia] += 1
            for row in sa_data:
                dia = (row.get("created_at") or "")[:10]
                if dia: sa_por_dia[dia] += 1

            if (d_fim - d_ini).days > 365:
                d_ini = d_fim - datetime.timedelta(days=365)

            dias = []
            cur = d_ini
            while cur <= d_fim:
                dias.append(cur.isoformat())
                cur += datetime.timedelta(days=1)
            evolucao = [{"data": d, "pageviews": pv_por_dia[d], "entradas": jo_por_dia[d], "saidas": sa_por_dia[d]} for d in dias]

        return {
            "pageviews": pageviews,
            "entradas": entradas,
            "saidas": saidas,
            "cliques": cliques,
            "registros": registros,
            "ftd": ftd_count,
            "ftd_valor": ftd_valor,
            "redep": redep_count,
            "redep_valor": redep_valor,
            "ctr": ctr,
            "conv_telegram": conv_telegram,
            "conv_pagina": conv_pagina,
            "retencao": retencao,
            "tm_entrada_saida": tm_entrada_saida,
            "evolucao": evolucao,
        }
    except Exception as e:
        print(f"[STATS ERRO] {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/config/telegram/canal/{canal_id}")
async def remover_canal_telegram(canal_id: int):
    try:
        db.table("telegram_canais").delete().eq("id", canal_id).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "ok"}

@app.get("/telegram/verificar-canal")
async def verificar_canal(username: str):
    """Verifica canal via API do Telegram e salva automaticamente."""
    bot_token = TELEGRAM_BOT_TOKEN
    try:
        result = db.table("configuracoes").select("valor").eq("chave", "telegram_bot_token").execute()
        if result.data:
            bot_token = result.data[0]["valor"]
    except Exception:
        pass

    if not bot_token:
        raise HTTPException(status_code=400, detail="Bot não configurado")

    chat_id = username if username.startswith("@") else "@" + username

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"https://api.telegram.org/bot{bot_token}/getChat",
            params={"chat_id": chat_id}
        )

    data = resp.json()
    if not data.get("ok"):
        raise HTTPException(status_code=400, detail=data.get("description", "Canal não encontrado"))

    chat = data["result"]
    nome = chat.get("title", username)
    uname = "@" + chat.get("username", "") if chat.get("username") else username
    tg_id = chat.get("id")
    invite = chat.get("invite_link", "")

    # Salva ou atualiza no banco
    try:
        existing = db.table("telegram_canais").select("id").eq("username", uname).execute()
        if existing.data:
            db.table("telegram_canais").update({"nome": nome, "link": invite, "telegram_id": str(tg_id)}).eq("username", uname).execute()
            canal_id = existing.data[0]["id"]
        else:
            ins = db.table("telegram_canais").insert({"nome": nome, "username": uname, "link": invite, "telegram_id": str(tg_id)}).execute()
            canal_id = ins.data[0]["id"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    print(f"[CANAL] Conectado: {nome} ({uname}) id={tg_id}")
    return {"ok": True, "id": canal_id, "nome": nome, "username": uname, "link": invite, "telegram_id": tg_id}


@app.get("/telegram/detectar-canais")
async def detectar_canais(request: Request):
    """Pausa webhook, busca updates com my_chat_member, reativa webhook."""
    bot_token = TELEGRAM_BOT_TOKEN
    try:
        result = db.table("configuracoes").select("valor").eq("chave", "telegram_bot_token").execute()
        if result.data:
            bot_token = result.data[0]["valor"]
    except Exception:
        pass

    if not bot_token:
        raise HTTPException(status_code=400, detail="Bot não configurado")

    webhook_url = str(request.base_url).rstrip("/").replace("http://", "https://") + "/telegram/webhook"

    async with httpx.AsyncClient(timeout=15) as client:
        # 1. Deletar webhook temporariamente (sem drop_pending para preservar updates)
        await client.post(f"https://api.telegram.org/bot{bot_token}/deleteWebhook")

        # 2. Buscar TODOS os updates pendentes (não só my_chat_member)
        #    com allowed_updates explícito incluindo my_chat_member e chat_member
        resp = await client.get(
            f"https://api.telegram.org/bot{bot_token}/getUpdates",
            params={"allowed_updates": '["my_chat_member","chat_member","message"]', "limit": 100, "timeout": 5}
        )
        data = resp.json()
        print(f"[DETECTAR] getUpdates retornou {len(data.get('result', []))} updates")

        # 3. Reativar webhook imediatamente
        await client.post(
            f"https://api.telegram.org/bot{bot_token}/setWebhook",
            json={"url": webhook_url, "allowed_updates": ["chat_member", "my_chat_member", "chat_join_request", "message", "channel_post"],
                  "secret_token": _get_telegram_secret_token()}
        )

    if not data.get("ok"):
        raise HTTPException(status_code=400, detail=data.get("description", "Erro ao buscar updates"))

    salvos = 0
    chats_vistos = {}  # chat_id -> chat info

    # Extrair chats de qualquer tipo de update
    for update in data.get("result", []):
        chat = None
        # my_chat_member: bot adicionado/removido
        mcm = update.get("my_chat_member")
        if mcm:
            new_status = mcm.get("new_chat_member", {}).get("status", "")
            if new_status in ("administrator", "member"):
                chat = mcm.get("chat", {})
        # mensagens em canal/grupo
        if not chat:
            for key in ("channel_post", "message", "edited_channel_post", "edited_message"):
                msg = update.get(key)
                if msg:
                    chat = msg.get("chat", {})
                    break

        if not chat:
            continue
        if chat.get("type") not in ("channel", "supergroup", "group"):
            continue

        chat_id = chat.get("id")
        if not chat_id or chat_id in chats_vistos:
            continue
        chats_vistos[chat_id] = chat

    # Salvar todos os chats descobertos
    for chat_id, chat in chats_vistos.items():
        chat_title = chat.get("title", "")
        chat_uname = ("@" + chat.get("username")) if chat.get("username") else ""

        try:
            existing = db.table("telegram_canais").select("id").eq("telegram_id", str(chat_id)).execute()
            if not existing.data:
                db.table("telegram_canais").insert({
                    "nome": chat_title,
                    "username": chat_uname,
                    "telegram_id": str(chat_id),
                    "link": "",
                }).execute()
                salvos += 1
                print(f"[DETECTAR] Canal salvo: {chat_title} id={chat_id}")
        except Exception as e:
            print(f"[DETECTAR ERRO] {e}")

    # Retornar todos os canais já cadastrados (inclusive os salvos pelo webhook)
    canais_total = []
    try:
        canais_res = db.table("telegram_canais").select("*").execute()
        canais_total = canais_res.data or []
    except Exception:
        pass

    return {"ok": True, "salvos": salvos, "canais": canais_total, "total": len(canais_total)}


@app.get("/telegram/setup")
async def telegram_setup(request: Request):
    """Registra o webhook do bot no Telegram."""
    # tenta pegar token do Supabase primeiro
    bot_token = TELEGRAM_BOT_TOKEN
    try:
        result = db.table("configuracoes").select("valor").eq("chave", "telegram_bot_token").execute()
        if result.data:
            bot_token = result.data[0]["valor"]
    except Exception:
        pass

    if not bot_token:
        raise HTTPException(status_code=400, detail="TELEGRAM_BOT_TOKEN não configurado")

    webhook_url = str(request.base_url).rstrip("/").replace("http://", "https://") + "/telegram/webhook"

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"https://api.telegram.org/bot{bot_token}/setWebhook",
            json={"url": webhook_url, "allowed_updates": ["chat_member", "my_chat_member", "chat_join_request", "message", "channel_post"],
                  "secret_token": _get_telegram_secret_token()},
        )

    result = resp.json()
    print(f"[TELEGRAM SETUP] {result}")
    return result


# ═══════════════════════════════════════════════════════════════════
# ─── BOOSTER (userbots, views, reações) ────────────────────────────
# ═══════════════════════════════════════════════════════════════════
import base64

def _booster_fernet():
    """Inicializa Fernet com chave derivada de SUPABASE_KEY (estável entre restarts)."""
    try:
        from cryptography.fernet import Fernet
        seed = os.environ.get("BOOSTER_ENC_KEY") or os.environ.get("SUPABASE_KEY", "")
        key = base64.urlsafe_b64encode(hashlib.sha256(seed.encode()).digest())
        return Fernet(key)
    except Exception as e:
        print(f"[BOOSTER fernet ERRO] {e}")
        return None

def _enc(s: str) -> str:
    if not s: return ""
    f = _booster_fernet()
    if not f: return s
    try: return f.encrypt(s.encode()).decode()
    except Exception: return s

def _dec(s: str) -> str:
    if not s: return ""
    f = _booster_fernet()
    if not f: return s
    try: return f.decrypt(s.encode()).decode()
    except Exception: return ""


# ── Config global (api_id / api_hash compartilhado entre as contas) ──
@app.get("/booster/config")
def booster_get_config():
    return {
        "api_id":   _get_cfg("booster_api_id") or "",
        "api_hash": bool(_get_cfg("booster_api_hash")),
    }


@app.post("/booster/config")
async def booster_set_config(request: Request):
    body = await request.json()
    if "api_id" in body:
        _set_cfg("booster_api_id", str(body.get("api_id") or "").strip())
    if "api_hash" in body and body.get("api_hash"):
        _set_cfg("booster_api_hash", _enc(str(body["api_hash"]).strip()))
    return {"ok": True}


def _booster_api_creds():
    api_id = _get_cfg("booster_api_id")
    api_hash_enc = _get_cfg("booster_api_hash")
    api_hash = _dec(api_hash_enc) if api_hash_enc else ""
    return api_id, api_hash


# ── Contas (CRUD básico) ─────────────────────────────────────────────
@app.get("/booster/contas")
def booster_listar_contas():
    try:
        rows = (db.table("booster_contas")
                .select("id,phone,username,first_name,user_id_tg,proxy,has_2fa,status,"
                        "cooldown_ate,views_hoje,reacoes_hoje,total_views,total_reacoes,"
                        "ultima_acao,banido_em,erro_msg,criado_em")
                .order("criado_em", desc=True).execute().data) or []
        return {"contas": rows}
    except Exception as e:
        return {"contas": [], "erro": str(e)}


@app.post("/booster/contas/upload-session")
async def booster_upload_session(request: Request):
    """Recebe um .session (Telethon) base64 e converte pra StringSession."""
    body = await request.json()
    session_b64 = (body.get("session_b64") or "").strip()
    proxy = (body.get("proxy") or "").strip()
    if not session_b64:
        raise HTTPException(status_code=400, detail="session_b64 obrigatório")
    api_id, api_hash = _booster_api_creds()
    if not api_id or not api_hash:
        raise HTTPException(status_code=400, detail="Configure api_id e api_hash em Booster → Config primeiro")

    try:
        from telethon import TelegramClient
        from telethon.sessions import SQLiteSession, StringSession
        import tempfile

        # Salva o .session temporariamente em disco pra abrir
        raw = base64.b64decode(session_b64)
        with tempfile.NamedTemporaryFile(suffix=".session", delete=False) as tf:
            tf.write(raw)
            tmp_path = tf.name

        # Abre como SQLiteSession e converte pra StringSession
        sql_sess = SQLiteSession(tmp_path[:-len(".session")])
        client = TelegramClient(sql_sess, int(api_id), api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            os.unlink(tmp_path)
            raise HTTPException(status_code=400, detail="Sessão não autorizada (.session vencida ou inválida)")
        me = await client.get_me()
        # Converte pra StringSession in-memory
        str_sess = StringSession.save(client.session)
        await client.disconnect()
        os.unlink(tmp_path)

        # Insere no banco
        new_row = {
            "phone":          getattr(me, "phone", None) and ("+" + me.phone),
            "username":       getattr(me, "username", None),
            "first_name":     getattr(me, "first_name", None),
            "user_id_tg":     str(getattr(me, "id", "")),
            "session_string": _enc(str_sess),
            "proxy":          proxy or None,
            "status":         "ativa",
        }
        ins = db.table("booster_contas").insert(new_row).execute()
        return {"ok": True, "conta": {k: new_row[k] for k in ("phone","username","first_name","user_id_tg")}}
    except HTTPException:
        raise
    except Exception as e:
        print(f"[BOOSTER upload-session ERRO] {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/booster/contas/{conta_id}")
def booster_deletar_conta(conta_id: int):
    try:
        db.table("booster_contas").delete().eq("id", conta_id).execute()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Login via SMS (cadastro de novo número direto pelo painel) ────────
# Cache em memória com TelegramClient ainda conectado, expira em 10min.
_login_pending: dict = {}  # login_id -> {client, phone, criado_em}

def _gen_login_id() -> str:
    import secrets
    return secrets.token_urlsafe(12)

async def _cleanup_logins_antigos():
    """Remove logins pendentes com mais de 10min."""
    agora = time.time()
    expirados = [lid for lid, d in _login_pending.items() if agora - d["criado_em"] > 600]
    for lid in expirados:
        try:
            cli = _login_pending[lid].get("client")
            if cli: await cli.disconnect()
        except Exception: pass
        _login_pending.pop(lid, None)


@app.post("/booster/login/iniciar")
async def booster_login_iniciar(request: Request):
    """Step 1: recebe phone + proxy, conecta Telethon, manda SMS, retorna login_id."""
    await _cleanup_logins_antigos()
    body = await request.json()
    phone = (body.get("phone") or "").strip()
    proxy_str = (body.get("proxy") or "").strip()
    if not phone:
        raise HTTPException(status_code=400, detail="phone é obrigatório")
    api_id, api_hash = _booster_api_creds()
    if not api_id or not api_hash:
        raise HTTPException(status_code=400, detail="Configure api_id e api_hash em Booster → Config primeiro")
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        # Parse proxy se fornecido
        proxy = None
        if proxy_str:
            try:
                # formato esperado: socks5://user:pass@host:porta
                from urllib.parse import urlparse
                p = urlparse(proxy_str)
                proxy_type = p.scheme  # socks5, http, etc
                user = p.username; pw = p.password
                host = p.hostname; port = p.port
                proxy = (proxy_type, host, port, True, user, pw) if user else (proxy_type, host, port)
            except Exception:
                pass
        client = TelegramClient(StringSession(), int(api_id), api_hash, proxy=proxy)
        await client.connect()
        sent = await client.send_code_request(phone)
        login_id = _gen_login_id()
        _login_pending[login_id] = {
            "client": client,
            "phone": phone,
            "phone_code_hash": sent.phone_code_hash,
            "proxy": proxy_str,
            "criado_em": time.time(),
        }
        return {"ok": True, "login_id": login_id}
    except Exception as e:
        print(f"[BOOSTER login iniciar ERRO] {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/booster/login/confirmar")
async def booster_login_confirmar(request: Request):
    """Step 2: recebe código de 5 dígitos. Se conta tem 2FA, retorna needs_password=true."""
    body = await request.json()
    login_id = body.get("login_id")
    codigo = (body.get("codigo") or "").strip()
    auto_2fa = bool(body.get("auto_2fa"))
    if not login_id or not codigo:
        raise HTTPException(status_code=400, detail="login_id e codigo obrigatórios")
    sess = _login_pending.get(login_id)
    if not sess:
        raise HTTPException(status_code=400, detail="login_id inválido ou expirado (refaça)")
    client = sess["client"]
    phone = sess["phone"]
    try:
        from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PhoneCodeExpiredError
        try:
            await client.sign_in(phone=phone, code=codigo, phone_code_hash=sess["phone_code_hash"])
        except SessionPasswordNeededError:
            return {"ok": True, "needs_password": True}
        except PhoneCodeInvalidError:
            raise HTTPException(status_code=400, detail="Código inválido")
        except PhoneCodeExpiredError:
            raise HTTPException(status_code=400, detail="Código expirou. Reenvie um novo.")
        # Se chegou aqui, autenticou sem 2FA
        return await _finalizar_login(login_id, auto_2fa=auto_2fa)
    except HTTPException:
        raise
    except Exception as e:
        print(f"[BOOSTER login confirmar ERRO] {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/booster/login/confirmar-2fa")
async def booster_login_2fa(request: Request):
    """Step 3 (opcional): senha 2FA."""
    body = await request.json()
    login_id = body.get("login_id")
    senha = body.get("senha") or ""
    auto_2fa = bool(body.get("auto_2fa"))
    if not login_id or not senha:
        raise HTTPException(status_code=400, detail="login_id e senha obrigatórios")
    sess = _login_pending.get(login_id)
    if not sess:
        raise HTTPException(status_code=400, detail="login_id inválido ou expirado")
    client = sess["client"]
    try:
        from telethon.errors import PasswordHashInvalidError
        try:
            await client.sign_in(password=senha)
        except PasswordHashInvalidError:
            raise HTTPException(status_code=400, detail="Senha 2FA incorreta")
        return await _finalizar_login(login_id, auto_2fa=auto_2fa, senha_2fa_existente=senha)
    except HTTPException:
        raise
    except Exception as e:
        print(f"[BOOSTER login 2FA ERRO] {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/booster/login/cancelar")
async def booster_login_cancelar(request: Request):
    body = await request.json()
    login_id = body.get("login_id")
    sess = _login_pending.pop(login_id, None)
    if sess:
        try: await sess["client"].disconnect()
        except Exception: pass
    return {"ok": True}


async def _finalizar_login(login_id: str, auto_2fa: bool = False, senha_2fa_existente: str = ""):
    """Pega o cliente autenticado, salva conta no banco, opcionalmente ativa 2FA, libera cache."""
    from telethon.sessions import StringSession
    sess = _login_pending.get(login_id)
    if not sess:
        raise HTTPException(status_code=400, detail="login_id inválido")
    client = sess["client"]
    phone = sess["phone"]
    proxy_str = sess.get("proxy", "")

    me = await client.get_me()
    senha_2fa_nova = ""
    has_2fa = bool(senha_2fa_existente)

    # Se pediu auto-2FA E ainda não tem 2FA, gera senha forte e configura
    if auto_2fa and not has_2fa:
        try:
            import secrets, string
            senha_2fa_nova = "".join(secrets.choice(string.ascii_letters + string.digits + "!@#$%") for _ in range(16))
            await client.edit_2fa(new_password=senha_2fa_nova, hint="Apollo Booster")
            has_2fa = True
            print(f"[BOOSTER 2FA] ativada pra {phone}")
        except Exception as e:
            print(f"[BOOSTER 2FA ERRO] {e}")
            senha_2fa_nova = ""

    str_sess = StringSession.save(client.session)
    await client.disconnect()
    _login_pending.pop(login_id, None)

    new_row = {
        "phone":          phone,
        "username":       getattr(me, "username", None),
        "first_name":     getattr(me, "first_name", None),
        "user_id_tg":     str(getattr(me, "id", "")),
        "session_string": _enc(str_sess),
        "proxy":          proxy_str or None,
        "has_2fa":        has_2fa,
        "senha_2fa_enc":  _enc(senha_2fa_nova or senha_2fa_existente) if (senha_2fa_nova or senha_2fa_existente) else None,
        "status":         "ativa",
    }
    db.table("booster_contas").insert(new_row).execute()
    return {
        "ok": True,
        "conta": {
            "phone":      phone,
            "username":   new_row["username"],
            "first_name": new_row["first_name"],
            "user_id_tg": new_row["user_id_tg"],
            "has_2fa":    has_2fa,
        },
        "senha_2fa_gerada": senha_2fa_nova or "",  # só retorna se foi gerada agora (mostra 1x na UI)
    }


@app.post("/booster/contas/{conta_id}/testar")
async def booster_testar_conta(conta_id: int):
    """Conecta a conta e chama get_me pra validar que está ativa."""
    try:
        row = db.table("booster_contas").select("*").eq("id", conta_id).execute()
        if not row.data:
            raise HTTPException(status_code=404, detail="Conta não encontrada")
        conta = row.data[0]
        api_id, api_hash = _booster_api_creds()
        if not api_id or not api_hash:
            raise HTTPException(status_code=400, detail="Config booster ausente")
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        sess = _dec(conta.get("session_string") or "")
        client = TelegramClient(StringSession(sess), int(api_id), api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            db.table("booster_contas").update({"status": "banida", "erro_msg": "session expirada"}).eq("id", conta_id).execute()
            await client.disconnect()
            return {"ok": False, "motivo": "session expirada"}
        me = await client.get_me()
        await client.disconnect()
        db.table("booster_contas").update({
            "status": "ativa", "erro_msg": None,
            "username": getattr(me, "username", None),
            "first_name": getattr(me, "first_name", None),
        }).eq("id", conta_id).execute()
        return {"ok": True, "first_name": getattr(me, "first_name", ""), "username": getattr(me, "username", "")}
    except HTTPException:
        raise
    except Exception as e:
        try:
            db.table("booster_contas").update({"status": "erro", "erro_msg": str(e)[:200]}).eq("id", conta_id).execute()
        except Exception: pass
        return {"ok": False, "motivo": str(e)}


# ── Campanhas: parser de URL + helpers Telethon ─────────────────────
import re, random
from datetime import datetime, timezone

def _parse_post_url(url: str):
    """Extrai (peer, msg_id) de uma URL do Telegram. Aceita t.me/canal/123, t.me/c/12345/123, @canal/123."""
    if not url: return (None, None)
    url = url.strip()
    # t.me/c/123456789/123 (canal privado)
    m = re.match(r"https?://t\.me/c/(\d+)/(\d+)", url)
    if m:
        return (int("-100" + m.group(1)), int(m.group(2)))
    # t.me/username/123
    m = re.match(r"https?://t\.me/([^/]+)/(\d+)", url)
    if m:
        return (m.group(1), int(m.group(2)))
    # @username/123 ou username/123
    m = re.match(r"@?([^/\s]+)/(\d+)", url)
    if m:
        return (m.group(1), int(m.group(2)))
    return (None, None)


def _parse_proxy(s: str):
    if not s: return None
    try:
        from urllib.parse import urlparse
        p = urlparse(s.strip())
        if not p.scheme or not p.hostname: return None
        if p.username:
            return (p.scheme, p.hostname, p.port, True, p.username, p.password)
        return (p.scheme, p.hostname, p.port)
    except Exception: return None


async def _conectar_conta(conta: dict):
    """Cria TelegramClient autenticado a partir do registro do banco."""
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    api_id, api_hash = _booster_api_creds()
    if not api_id or not api_hash:
        raise Exception("api_id/api_hash não configurados")
    sess = _dec(conta.get("session_string") or "")
    if not sess:
        raise Exception("session_string vazia/inválida")
    proxy = _parse_proxy(conta.get("proxy") or "")
    client = TelegramClient(StringSession(sess), int(api_id), api_hash, proxy=proxy)
    await client.connect()
    if not await client.is_user_authorized():
        raise Exception("session expirada")
    return client


async def _executar_view(conta: dict, peer, msg_id: int):
    """Executa uma view no post. Retorna (ok, erro_msg)."""
    from telethon.errors import FloodWaitError, UserDeactivatedBanError, AuthKeyUnregisteredError, ChannelPrivateError
    client = None
    try:
        client = await _conectar_conta(conta)
        entity = await client.get_entity(peer)
        # GetMessages incrementa contador de views automaticamente
        await client.get_messages(entity, ids=[msg_id])
        return (True, None)
    except FloodWaitError as e:
        return (False, f"flood {e.seconds}s")
    except (UserDeactivatedBanError, AuthKeyUnregisteredError):
        return (False, "banida")
    except ChannelPrivateError:
        return (False, "canal privado/sem acesso")
    except Exception as e:
        return (False, str(e)[:200])
    finally:
        if client:
            try: await client.disconnect()
            except Exception: pass


async def _executar_reacao(conta: dict, peer, msg_id: int, emoji: str):
    """Adiciona reação ao post. Retorna (ok, erro_msg)."""
    from telethon.errors import FloodWaitError, UserDeactivatedBanError, AuthKeyUnregisteredError, ChannelPrivateError, ReactionInvalidError
    from telethon.tl.functions.messages import SendReactionRequest
    from telethon.tl.types import ReactionEmoji
    client = None
    try:
        client = await _conectar_conta(conta)
        entity = await client.get_entity(peer)
        await client(SendReactionRequest(
            peer=entity, msg_id=msg_id,
            reaction=[ReactionEmoji(emoticon=emoji)],
        ))
        return (True, None)
    except FloodWaitError as e:
        return (False, f"flood {e.seconds}s")
    except (UserDeactivatedBanError, AuthKeyUnregisteredError):
        return (False, "banida")
    except ChannelPrivateError:
        return (False, "canal privado/sem acesso")
    except ReactionInvalidError:
        return (False, f"emoji '{emoji}' inválido pra esse canal")
    except Exception as e:
        return (False, str(e)[:200])
    finally:
        if client:
            try: await client.disconnect()
            except Exception: pass


# ── Worker (executa campanhas em background) ──────────────────────────
_campanhas_tasks: dict = {}  # camp_id -> set(asyncio.Task)
_worker_iniciado = False


async def _registrar_acao(camp_id: int, conta_id: int, tipo: str, emoji: str, ok: bool, erro: str):
    """Persiste uma ação executada e atualiza contadores."""
    try:
        status = "sucesso" if ok else ("flood" if erro and "flood" in erro else ("banida" if erro == "banida" else "erro"))
        db.table("booster_acoes").insert({
            "campanha_id": camp_id, "conta_id": conta_id,
            "tipo": tipo, "emoji": emoji,
            "status": status, "erro_msg": erro,
        }).execute()
        # Marca conta como banida se for o caso
        if status == "banida":
            db.table("booster_contas").update({"status": "banida", "banido_em": datetime.now(timezone.utc).isoformat(), "erro_msg": erro}).eq("id", conta_id).execute()
        elif status == "flood" and erro:
            mat = re.search(r"flood (\d+)", erro)
            if mat:
                segs = int(mat.group(1))
                from datetime import timedelta
                ate = datetime.now(timezone.utc) + timedelta(seconds=segs)
                db.table("booster_contas").update({"status": "cooldown", "cooldown_ate": ate.isoformat()}).eq("id", conta_id).execute()
        # Incrementa contadores se sucesso
        if ok:
            r = db.table("booster_contas").select("views_hoje,reacoes_hoje,total_views,total_reacoes").eq("id", conta_id).execute()
            if r.data:
                d = r.data[0]
                upd = {"ultima_acao": datetime.now(timezone.utc).isoformat()}
                if tipo == "view":
                    upd["views_hoje"]   = (d.get("views_hoje") or 0) + 1
                    upd["total_views"]  = (d.get("total_views") or 0) + 1
                else:
                    upd["reacoes_hoje"]  = (d.get("reacoes_hoje") or 0) + 1
                    upd["total_reacoes"] = (d.get("total_reacoes") or 0) + 1
                db.table("booster_contas").update(upd).eq("id", conta_id).execute()
            r2 = db.table("booster_campanhas").select("views_entregues,reacoes_entregues").eq("id", camp_id).execute()
            if r2.data:
                d2 = r2.data[0]
                col = "views_entregues" if tipo == "view" else "reacoes_entregues"
                db.table("booster_campanhas").update({col: (d2.get(col) or 0) + 1}).eq("id", camp_id).execute()
    except Exception as e:
        print(f"[BOOSTER registrar_acao ERRO] {e}")


async def _executar_acao_agendada(camp_id: int, conta: dict, acao: dict, peer, msg_id: int):
    """Espera o delay programado e executa a ação."""
    try:
        await asyncio.sleep(acao["delay"])
        if acao["tipo"] == "view":
            ok, erro = await _executar_view(conta, peer, msg_id)
            await _registrar_acao(camp_id, conta["id"], "view", "", ok, erro)
        else:
            ok, erro = await _executar_reacao(conta, peer, msg_id, acao["emoji"])
            await _registrar_acao(camp_id, conta["id"], "reacao", acao["emoji"], ok, erro)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"[BOOSTER acao agendada ERRO] {e}")


async def _executar_campanha(camp: dict):
    """Distribui as ações da campanha pelas contas elegíveis e roda em paralelo."""
    camp_id = camp["id"]
    janela_seg = (camp.get("janela_min") or 30) * 60
    delay_min = camp.get("delay_min_seg") or 1
    delay_max = camp.get("delay_max_seg") or 30

    peer, msg_id = _parse_post_url(camp.get("canal_link") or "")
    if not msg_id:
        db.table("booster_campanhas").update({"status": "erro", "erro_msg": "URL do post inválida"}).eq("id", camp_id).execute()
        return

    contas = (db.table("booster_contas").select("*").eq("status","ativa").execute().data) or []
    if not contas:
        db.table("booster_campanhas").update({"status": "erro", "erro_msg": "nenhuma conta ativa"}).eq("id", camp_id).execute()
        return

    qtd_views = camp.get("qtd_views") or 0
    qtd_reacoes = camp.get("qtd_reacoes") or 0
    emojis = camp.get("reacoes_emojis") or ["👍"]

    # Cria lista de ações
    acoes = []
    for _ in range(qtd_views):
        acoes.append({"tipo": "view"})
    for _ in range(qtd_reacoes):
        acoes.append({"tipo": "reacao", "emoji": random.choice(emojis)})
    random.shuffle(acoes)

    # Distribui no tempo (cumulativo)
    tempo = 0.0
    for a in acoes:
        d = random.uniform(delay_min, delay_max)
        tempo += d
        if tempo > janela_seg:
            tempo = random.uniform(0, janela_seg)
        a["delay"] = tempo

    # Spawn tasks
    db.table("booster_campanhas").update({"status": "rodando", "iniciado_em": datetime.now(timezone.utc).isoformat()}).eq("id", camp_id).execute()
    tasks = set()
    _campanhas_tasks[camp_id] = tasks
    for a in acoes:
        conta = random.choice(contas)
        t = asyncio.create_task(_executar_acao_agendada(camp_id, conta, a, peer, msg_id))
        tasks.add(t)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    # Marca concluída se ainda estiver rodando
    r = db.table("booster_campanhas").select("status").eq("id", camp_id).execute()
    if r.data and r.data[0]["status"] == "rodando":
        db.table("booster_campanhas").update({"status": "concluida", "finalizado_em": datetime.now(timezone.utc).isoformat()}).eq("id", camp_id).execute()
    _campanhas_tasks.pop(camp_id, None)


async def _booster_worker_loop():
    """Loop infinito: pega campanhas pendentes e dispara execução."""
    while True:
        try:
            r = db.table("booster_campanhas").select("*").eq("status", "pendente").execute()
            for camp in (r.data or []):
                if camp["id"] not in _campanhas_tasks:
                    asyncio.create_task(_executar_campanha(camp))
        except Exception as e:
            print(f"[BOOSTER WORKER] erro: {e}")
        await asyncio.sleep(5)


@app.on_event("startup")
async def _startup_booster():
    global _worker_iniciado
    if not _worker_iniciado:
        asyncio.create_task(_booster_worker_loop())
        _worker_iniciado = True
        print("[BOOSTER] worker iniciado")


# ── Endpoints de campanhas ──────────────────────────────────────────
@app.get("/booster/campanhas")
def booster_listar_campanhas(limit: int = 100):
    try:
        rows = (db.table("booster_campanhas").select("*").order("criado_em", desc=True).limit(limit).execute().data) or []
        return {"campanhas": rows}
    except Exception as e:
        return {"campanhas": [], "erro": str(e)}


@app.post("/booster/campanhas")
async def booster_criar_campanha(request: Request):
    body = await request.json()
    canal_link = (body.get("canal_link") or "").strip()
    if not canal_link:
        raise HTTPException(status_code=400, detail="canal_link obrigatório (link completo do post)")
    peer, msg_id = _parse_post_url(canal_link)
    if not msg_id:
        raise HTTPException(status_code=400, detail="URL inválida. Use o link completo do post (ex: https://t.me/canal/123)")
    qtd_views   = max(0, int(body.get("qtd_views")   or 0))
    qtd_reacoes = max(0, int(body.get("qtd_reacoes") or 0))
    if qtd_views == 0 and qtd_reacoes == 0:
        raise HTTPException(status_code=400, detail="Defina pelo menos qtd_views ou qtd_reacoes")
    new_row = {
        "nome":             body.get("nome") or f"Campanha {datetime.now().strftime('%d/%m %H:%M')}",
        "canal_link":       canal_link,
        "msg_id":           msg_id,
        "qtd_views":        qtd_views,
        "qtd_reacoes":      qtd_reacoes,
        "reacoes_emojis":   body.get("reacoes_emojis") or ["👍","❤️","🔥"],
        "delay_min_seg":    max(1, int(body.get("delay_min_seg") or 2)),
        "delay_max_seg":    max(2, int(body.get("delay_max_seg") or 30)),
        "janela_min":       max(1, int(body.get("janela_min") or 30)),
        "status":           "pendente" if body.get("iniciar_agora", True) else "pausada",
    }
    ins = db.table("booster_campanhas").insert(new_row).execute()
    return {"ok": True, "campanha": (ins.data[0] if ins.data else new_row)}


@app.post("/booster/campanhas/canal")
async def booster_criar_campanha_canal(request: Request):
    """Pega os últimos N posts de um canal via Telethon e cria 1 campanha pra cada."""
    body = await request.json()
    canal_link_raw = (body.get("canal_link") or "").strip()
    qtd_posts = max(1, int(body.get("qtd_posts") or 5))
    qtd_views = max(0, int(body.get("qtd_views") or 0))
    qtd_reacoes = max(0, int(body.get("qtd_reacoes") or 0))
    if qtd_views == 0 and qtd_reacoes == 0:
        raise HTTPException(status_code=400, detail="Defina pelo menos qtd_views ou qtd_reacoes")
    if not canal_link_raw:
        raise HTTPException(status_code=400, detail="canal_link obrigatório")

    # Normaliza pra @username ou id numérico
    canal_target = canal_link_raw
    m = re.match(r"https?://t\.me/c/(\d+)", canal_link_raw)
    if m:
        canal_target = int("-100" + m.group(1))
    else:
        m = re.match(r"https?://t\.me/([^/?\s]+)", canal_link_raw)
        if m:
            canal_target = m.group(1)
            if not canal_target.startswith("@"):
                canal_target = "@" + canal_target
        elif canal_link_raw.startswith("@"):
            canal_target = canal_link_raw
        elif not canal_link_raw.lstrip("-").isdigit():
            canal_target = "@" + canal_link_raw

    contas = (db.table("booster_contas").select("*").eq("status", "ativa").limit(1).execute().data) or []
    if not contas:
        raise HTTPException(status_code=400, detail="Precisa pelo menos 1 conta ativa pra buscar os posts")
    api_id, api_hash = _booster_api_creds()
    if not api_id or not api_hash:
        raise HTTPException(status_code=400, detail="Configure api_id/api_hash em Booster → Config")

    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.errors import ChannelPrivateError, UsernameNotOccupiedError, UsernameInvalidError

    conta = contas[0]
    sess = _dec(conta.get("session_string") or "")
    proxy = _parse_proxy(conta.get("proxy") or "")
    client = TelegramClient(StringSession(sess), int(api_id), api_hash, proxy=proxy)

    posts = []
    canal_username = None
    canal_title = ""
    canal_id_tg = None
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise HTTPException(status_code=400, detail="Conta userbot não autorizada (sessão expirada)")
        entity = await client.get_entity(canal_target)
        msgs = await client.get_messages(entity, limit=qtd_posts)
        canal_username = getattr(entity, "username", None)
        canal_title = getattr(entity, "title", None) or canal_username or ""
        canal_id_tg = entity.id
        for msg in msgs:
            if msg and getattr(msg, "id", None):
                posts.append({"msg_id": msg.id, "text": (getattr(msg, "text", "") or "")[:60]})
    except (ChannelPrivateError, UsernameNotOccupiedError, UsernameInvalidError):
        raise HTTPException(status_code=400, detail="Canal não encontrado ou privado. Se for privado, a conta userbot precisa ser membro.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)[:300])
    finally:
        try: await client.disconnect()
        except Exception: pass

    if not posts:
        raise HTTPException(status_code=400, detail="Nenhum post encontrado no canal")

    criadas = []
    for p in posts:
        if canal_username:
            link = f"https://t.me/{canal_username}/{p['msg_id']}"
        else:
            id_simple = str(canal_id_tg).replace("-100", "")
            link = f"https://t.me/c/{id_simple}/{p['msg_id']}"
        new_row = {
            "nome":             f"📌 {canal_title} #{p['msg_id']}",
            "canal_link":       link,
            "msg_id":           p["msg_id"],
            "qtd_views":        qtd_views,
            "qtd_reacoes":      qtd_reacoes,
            "reacoes_emojis":   body.get("reacoes_emojis") or ["👍","❤️","🔥"],
            "delay_min_seg":    max(1, int(body.get("delay_min_seg") or 2)),
            "delay_max_seg":    max(2, int(body.get("delay_max_seg") or 30)),
            "janela_min":       max(1, int(body.get("janela_min") or 30)),
            "status":           "pendente",
        }
        ins = db.table("booster_campanhas").insert(new_row).execute()
        criadas.append({"msg_id": p["msg_id"], "id": (ins.data[0]["id"] if ins.data else None), "preview": p["text"]})

    return {"ok": True, "canal": canal_title, "qtd_criadas": len(criadas), "criadas": criadas}


@app.post("/booster/campanhas/{camp_id}/iniciar")
def booster_iniciar_campanha(camp_id: int):
    db.table("booster_campanhas").update({"status": "pendente"}).eq("id", camp_id).execute()
    return {"ok": True}


@app.post("/booster/campanhas/{camp_id}/cancelar")
async def booster_cancelar_campanha(camp_id: int):
    tasks = _campanhas_tasks.get(camp_id, set())
    for t in list(tasks):
        try: t.cancel()
        except Exception: pass
    _campanhas_tasks.pop(camp_id, None)
    db.table("booster_campanhas").update({"status": "cancelada", "finalizado_em": datetime.now(timezone.utc).isoformat()}).eq("id", camp_id).execute()
    return {"ok": True}


@app.delete("/booster/campanhas/{camp_id}")
async def booster_deletar_campanha(camp_id: int):
    tasks = _campanhas_tasks.get(camp_id, set())
    for t in list(tasks):
        try: t.cancel()
        except Exception: pass
    _campanhas_tasks.pop(camp_id, None)
    db.table("booster_campanhas").delete().eq("id", camp_id).execute()
    return {"ok": True}


@app.get("/booster/campanhas/{camp_id}/acoes")
def booster_acoes_campanha(camp_id: int, limit: int = 200):
    try:
        rows = (db.table("booster_acoes").select("*,booster_contas(phone,first_name,username)")
                .eq("campanha_id", camp_id)
                .order("executado_em", desc=True).limit(limit).execute().data) or []
        return {"acoes": rows}
    except Exception as e:
        return {"acoes": [], "erro": str(e)}


# ── Cadastro manual de canal via link (precisa do bot admin) ─────────
@app.post("/booster/canal/adicionar-via-link")
async def booster_adicionar_canal_via_link(request: Request):
    """Resolve um canal via link/username, valida que o bot tem acesso, salva em telegram_canais."""
    body = await request.json()
    link = (body.get("link") or "").strip()
    if not link:
        raise HTTPException(status_code=400, detail="link obrigatório")
    # Normaliza pra @username ou -100... id
    chat_id_param = None
    m = re.match(r"https?://t\.me/c/(\d+)", link)
    if m:
        chat_id_param = "-100" + m.group(1)
    else:
        m = re.match(r"https?://t\.me/(\+[\w-]+)", link)  # link de invite (joinchat)
        if m:
            raise HTTPException(status_code=400, detail="Link de convite (joinchat) não funciona aqui. Use o link público @canal ou t.me/canal")
        m = re.match(r"https?://t\.me/([^/?\s]+)", link)
        if m:
            chat_id_param = "@" + m.group(1)
        elif link.startswith("@"):
            chat_id_param = link
        else:
            chat_id_param = "@" + link
    bot_token = TELEGRAM_BOT_TOKEN
    try:
        r = db.table("configuracoes").select("valor").eq("chave","telegram_bot_token").execute()
        if r.data: bot_token = r.data[0]["valor"]
    except Exception:
        pass
    if not bot_token:
        raise HTTPException(status_code=400, detail="Bot não configurado em Pixels → Telegram")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"https://api.telegram.org/bot{bot_token}/getChat",
                params={"chat_id": chat_id_param},
            )
        d = resp.json()
        if not d.get("ok"):
            desc = d.get("description") or "erro"
            if "chat not found" in desc.lower():
                raise HTTPException(status_code=400, detail="Canal não encontrado. O bot precisa ser admin (ou membro) do canal — adicione e tente de novo.")
            raise HTTPException(status_code=400, detail=desc)
        result = d["result"]
        chat_id = result.get("id")
        title = result.get("title") or result.get("username", "")
        uname = result.get("username")
        chat_type = result.get("type")
        if chat_type not in ("channel", "supergroup", "group"):
            raise HTTPException(status_code=400, detail=f"Tipo de chat '{chat_type}' não suportado (precisa ser canal ou grupo)")
        # Insere/atualiza em telegram_canais
        existing = db.table("telegram_canais").select("id").eq("telegram_id", str(chat_id)).execute()
        if existing.data:
            db.table("telegram_canais").update({
                "nome": title, "username": ("@"+uname) if uname else "", "link": link,
            }).eq("id", existing.data[0]["id"]).execute()
            canal_id = existing.data[0]["id"]
            criado = False
        else:
            ins = db.table("telegram_canais").insert({
                "nome": title, "username": ("@"+uname) if uname else "",
                "telegram_id": str(chat_id), "link": link,
            }).execute()
            canal_id = ins.data[0]["id"] if ins.data else None
            criado = True
        return {"ok": True, "canal_id": canal_id, "nome": title, "username": uname, "telegram_id": str(chat_id), "criado": criado}
    except HTTPException:
        raise
    except Exception as e:
        print(f"[BOOSTER add canal ERRO] {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Auto-Boost: configuração por canal pra todos os posts novos ──────
@app.get("/booster/auto")
def booster_auto_listar():
    """Lista canais cadastrados + config de auto-boost (se houver)."""
    try:
        canais = (db.table("telegram_canais").select("id,nome,username,telegram_id").execute().data) or []
        autos = (db.table("booster_auto").select("*").execute().data) or []
        by_tgid = {a.get("canal_telegram_id"): a for a in autos}
        merged = []
        for c in canais:
            tgid = str(c.get("telegram_id") or "")
            cfg = by_tgid.get(tgid)
            merged.append({
                "canal_id":          c.get("id"),
                "nome":              c.get("nome"),
                "username":          c.get("username"),
                "telegram_id":       tgid,
                "tem_config":        bool(cfg),
                "ativo":             bool(cfg and cfg.get("ativo")),
                "qtd_views":         (cfg or {}).get("qtd_views") or 0,
                "qtd_reacoes":       (cfg or {}).get("qtd_reacoes") or 0,
                "reacoes_emojis":    (cfg or {}).get("reacoes_emojis") or ["👍","❤️","🔥"],
                "delay_min_seg":     (cfg or {}).get("delay_min_seg") or 2,
                "delay_max_seg":     (cfg or {}).get("delay_max_seg") or 30,
                "janela_min":        (cfg or {}).get("janela_min") or 30,
                "aguardar_min_antes":(cfg or {}).get("aguardar_min_antes") or 0,
                "auto_id":           (cfg or {}).get("id"),
            })
        return {"canais": merged}
    except Exception as e:
        return {"canais": [], "erro": str(e)}


@app.post("/booster/auto")
async def booster_auto_salvar(request: Request):
    """Salva (insert/update) config de auto-boost pra um canal."""
    body = await request.json()
    canal_id = body.get("canal_id")
    if not canal_id:
        raise HTTPException(status_code=400, detail="canal_id obrigatório")
    canal = db.table("telegram_canais").select("*").eq("id", canal_id).execute()
    if not canal.data:
        raise HTTPException(status_code=404, detail="Canal não encontrado")
    canal_tgid = str(canal.data[0].get("telegram_id") or "")
    if not canal_tgid:
        raise HTTPException(status_code=400, detail="Canal sem telegram_id (bot precisa estar admin lá)")

    payload = {
        "canal_id":           canal_id,
        "canal_telegram_id":  canal_tgid,
        "canal_nome":         canal.data[0].get("nome"),
        "ativo":              bool(body.get("ativo")),
        "qtd_views":          max(0, int(body.get("qtd_views") or 0)),
        "qtd_reacoes":        max(0, int(body.get("qtd_reacoes") or 0)),
        "reacoes_emojis":     body.get("reacoes_emojis") or ["👍","❤️","🔥"],
        "delay_min_seg":      max(1, int(body.get("delay_min_seg") or 2)),
        "delay_max_seg":      max(2, int(body.get("delay_max_seg") or 30)),
        "janela_min":         max(1, int(body.get("janela_min") or 30)),
        "aguardar_min_antes": max(0, int(body.get("aguardar_min_antes") or 0)),
        "atualizado_em":      datetime.now(timezone.utc).isoformat(),
    }
    existing = db.table("booster_auto").select("id").eq("canal_telegram_id", canal_tgid).execute()
    if existing.data:
        db.table("booster_auto").update(payload).eq("id", existing.data[0]["id"]).execute()
    else:
        db.table("booster_auto").insert(payload).execute()
    return {"ok": True}


@app.delete("/booster/auto/{auto_id}")
def booster_auto_deletar(auto_id: int):
    db.table("booster_auto").delete().eq("id", auto_id).execute()
    return {"ok": True}


@app.post("/booster/webhook/atualizar")
async def booster_atualizar_webhook(request: Request):
    """Força reconfiguração do webhook com channel_post incluído (pra Auto-Boost)."""
    bot_token = TELEGRAM_BOT_TOKEN
    try:
        r = db.table("configuracoes").select("valor").eq("chave","telegram_bot_token").execute()
        if r.data: bot_token = r.data[0]["valor"]
    except Exception: pass
    if not bot_token:
        raise HTTPException(status_code=400, detail="bot_token não configurado em Pixels → Telegram")
    webhook_url = str(request.base_url).rstrip("/").replace("http://","https://") + "/telegram/webhook"
    secret = _get_telegram_secret_token()
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"https://api.telegram.org/bot{bot_token}/setWebhook",
                json={"url": webhook_url,
                      "allowed_updates": ["chat_member","my_chat_member","chat_join_request","message","channel_post"],
                      "secret_token": secret},
            )
        d = resp.json()
        if not d.get("ok"):
            raise HTTPException(status_code=400, detail=d.get("description","erro"))
        # Verifica
        async with httpx.AsyncClient(timeout=10) as client:
            info = await client.get(f"https://api.telegram.org/bot{bot_token}/getWebhookInfo")
        info_d = info.json()
        return {"ok": True, "webhook": info_d.get("result", {})}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/booster/auto/diag")
async def booster_auto_diag():
    """Diagnóstico completo do Auto-Boost: webhook, configs, contas, últimas campanhas auto."""
    out = {}
    # 1. WebhookInfo
    bot_token = TELEGRAM_BOT_TOKEN
    try:
        r = db.table("configuracoes").select("valor").eq("chave","telegram_bot_token").execute()
        if r.data: bot_token = r.data[0]["valor"]
    except Exception: pass
    if bot_token:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"https://api.telegram.org/bot{bot_token}/getWebhookInfo")
            d = resp.json()
            if d.get("ok"):
                info = d["result"]
                out["webhook"] = {
                    "url": info.get("url",""),
                    "allowed_updates": info.get("allowed_updates", []),
                    "tem_channel_post": "channel_post" in (info.get("allowed_updates") or []),
                    "pending_update_count": info.get("pending_update_count", 0),
                    "last_error_date": info.get("last_error_date"),
                    "last_error_message": info.get("last_error_message"),
                }
        except Exception as e:
            out["webhook"] = {"erro": str(e)}
    else:
        out["webhook"] = {"erro": "bot_token não configurado"}

    # 2. Configs ativas
    try:
        cfgs = (db.table("booster_auto").select("*").eq("ativo", True).execute().data) or []
        out["auto_configs_ativas"] = [
            {"canal_nome": c.get("canal_nome"), "canal_telegram_id": c.get("canal_telegram_id"),
             "qtd_views": c.get("qtd_views"), "qtd_reacoes": c.get("qtd_reacoes")}
            for c in cfgs
        ]
    except Exception as e:
        out["auto_configs_ativas"] = {"erro": str(e)}

    # 3. Contas ativas
    try:
        contas = db.table("booster_contas").select("status,phone,first_name").execute().data or []
        out["contas"] = {
            "total": len(contas),
            "ativas": sum(1 for c in contas if c.get("status") == "ativa"),
            "banidas": sum(1 for c in contas if c.get("status") == "banida"),
            "cooldown": sum(1 for c in contas if c.get("status") == "cooldown"),
        }
    except Exception as e:
        out["contas"] = {"erro": str(e)}

    # 4. Últimas 5 campanhas (foca em auto)
    try:
        camps = (db.table("booster_campanhas").select("id,nome,canal_link,status,views_entregues,reacoes_entregues,qtd_views,qtd_reacoes,erro_msg,criado_em")
                 .order("criado_em", desc=True).limit(10).execute().data) or []
        out["ultimas_campanhas"] = camps
    except Exception as e:
        out["ultimas_campanhas"] = {"erro": str(e)}

    # 5. Worker
    out["worker_iniciado"] = _worker_iniciado
    out["campanhas_em_execucao"] = list(_campanhas_tasks.keys())

    return out


@app.get("/booster/status-resumo")
def booster_status_resumo():
    """Resumo geral pra aba Status."""
    try:
        contas = db.table("booster_contas").select("status").execute().data or []
        camps  = db.table("booster_campanhas").select("status,views_entregues,reacoes_entregues").execute().data or []
        from collections import Counter
        contas_por_status = Counter(c.get("status") or "?" for c in contas)
        camps_por_status  = Counter(c.get("status") or "?" for c in camps)
        return {
            "contas": dict(contas_por_status),
            "campanhas": dict(camps_por_status),
            "totais": {
                "views_entregues":   sum((c.get("views_entregues")   or 0) for c in camps),
                "reacoes_entregues": sum((c.get("reacoes_entregues") or 0) for c in camps),
            },
        }
    except Exception as e:
        return {"erro": str(e)}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
