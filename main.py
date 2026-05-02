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

    user_data = {}
    if email:       user_data["em"] = [sha256(email)]
    if phone:       user_data["ph"] = [sha256(phone)]
    if first_name:  user_data["fn"] = [sha256(first_name)]
    if last_name:   user_data["ln"] = [sha256(last_name)]
    if external_id: user_data["external_id"] = [sha256(external_id)]
    if fbc:         user_data["fbc"] = fbc
    if fbp:         user_data["fbp"] = fbp
    if client_ip:   user_data["client_ip_address"] = client_ip
    if user_agent:  user_data["client_user_agent"] = user_agent

    evento = {
        "event_name":    event_name,
        "event_time":    int(time.time()),
        "event_id":      f"{event_name}_{external_id or telegram_user_id or int(time.time()*1000)}",
        "action_source": action_source,
        "user_data":     user_data,
    }
    if event_source_url:
        evento["event_source_url"] = event_source_url
    if value is not None:
        evento["custom_data"] = {"currency": "BRL", "value": value}

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

    # Para JoinChannel/LeaveChannel: a chamada vem do Telegram (não do browser do user),
    # então fbc/fbp/IP/UA serão preenchidos do snapshot da última /tracker/entrada
    await enviar_meta(event_name_meta, first_name=first_name, last_name=last_name,
                      telegram_user_id=str(user_id) if user_id else None, canal_nome=canal_nome,
                      action_source="system_generated")

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
                json={"url": webhook_url, "allowed_updates": ["chat_member", "my_chat_member"],
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
            json={"url": webhook_url, "allowed_updates": ["chat_member", "my_chat_member"],
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
            json={"url": webhook_url, "allowed_updates": ["chat_member", "my_chat_member"],
                  "secret_token": _get_telegram_secret_token()},
        )

    result = resp.json()
    print(f"[TELEGRAM SETUP] {result}")
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
