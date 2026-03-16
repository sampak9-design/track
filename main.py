from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from starlette.middleware.cors import CORSMiddleware
from supabase import create_client
import uvicorn
import httpx
import hashlib
import time
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
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
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

async def enviar_meta(event_name: str, email: str = None, phone: str = None, value: float = None, first_name: str = None, last_name: str = None):
    pixel_id, token = get_meta_config()
    if not pixel_id or not token:
        print(f"[META ✗] Pixel ID ou Token não configurado")
        return

    user_data = {}
    if email:
        user_data["em"] = [sha256(email)]
    if phone:
        user_data["ph"] = [sha256(phone)]
    if first_name:
        user_data["fn"] = [sha256(first_name)]
    if last_name:
        user_data["ln"] = [sha256(last_name)]

    evento = {
        "event_name":    event_name,
        "event_time":    int(time.time()),
        "action_source": "website",
        "user_data":     user_data,
    }
    if value is not None:
        evento["custom_data"] = {"currency": "BRL", "value": value}

    url = f"https://graph.facebook.com/v19.0/{pixel_id}/events"

    async with httpx.AsyncClient() as client:
        resp = await client.post(url, params={"access_token": token}, json={"data": [evento]})

    if resp.status_code == 200:
        print(f"[META ✓] Evento '{event_name}' enviado")
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

async def enviar_kwai(event_name: str, email: str = None, phone: str = None, value: float = None):
    pixel_id, token = get_kwai_config()
    if not pixel_id or not token:
        print(f"[KWAI ✗] Pixel ID ou Token não configurado")
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

    if resp.status_code == 200:
        print(f"[KWAI ✓] Evento '{event_name}' enviado")
    else:
        print(f"[KWAI ✗] {resp.status_code} — {resp.text}")

async def enviar_tiktok(event_name: str, email: str = None, phone: str = None, value: float = None):
    pixel_code, token = get_tiktok_config()
    if not pixel_code or not token:
        print(f"[TIKTOK ✗] Pixel Code ou Token não configurado")
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
@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
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

    await enviar_meta(event_name_meta, first_name=first_name, last_name=last_name)

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
    try:
        db.table("tracker_entradas").insert({
            "canal_id": canal_id,
            "page_url": page_url,
            **utms
        }).execute()
    except Exception as e:
        print(f"[ENTRADA ERRO] {e}")
    return {"ok": True}

@app.get("/leads")
def get_leads():
    try:
        # Todos os eventos do canal
        all_events = db.table("telegram_members").select("*").order("created_at", ascending=True).execute().data or []

        # Por user_id: primeira entrada (join) e status atual
        first_join = {}
        latest = {}
        for ev in all_events:
            uid = ev.get("user_id")
            if not uid: continue
            if ev.get("event") == "join" and uid not in first_join:
                first_join[uid] = ev.get("created_at")
            latest[uid] = ev  # última linha = status atual

        # Cadastros indexados por primeiro nome
        cads = db.table("cadastros").select("*").execute().data or []
        cad_by_name = {}
        for c in cads:
            key = (c.get("nome") or "").split()[0].lower().strip()
            if key: cad_by_name[key] = c

        # Depósitos indexados por email
        deps = db.table("depositos").select("*").order("created_at", ascending=True).execute().data or []
        dep_by_email = {}
        for d in deps:
            email = d.get("email", "")
            if not email: continue
            if email not in dep_by_email:
                dep_by_email[email] = {"ftd": d.get("created_at"), "count": 0, "ltv": 0.0}
            dep_by_email[email]["count"] += 1
            dep_by_email[email]["ltv"] += float(d.get("valor") or 0)

        # Canal principal
        canais = db.table("telegram_canais").select("nome").execute().data or []
        canal_nome = canais[0]["nome"] if canais else "—"

        leads = []
        for uid, user in latest.items():
            first = (user.get("first_name") or "").lower().strip()
            cad = cad_by_name.get(first)
            email = (cad or {}).get("email", "")
            dep = dep_by_email.get(email, {})
            entrada = first_join.get(uid)
            saiu_em = user.get("created_at") if user.get("event") == "leave" else None
            leads.append({
                "user_id": uid,
                "username": user.get("username") or "",
                "first_name": user.get("first_name") or "",
                "last_name": user.get("last_name") or "",
                "canal": canal_nome,
                "utm_source": (cad or {}).get("utm_source") or "",
                "page_url": (cad or {}).get("utm_content") or "",
                "entrada": entrada,
                "registro": (cad or {}).get("created_at"),
                "ftd": dep.get("ftd"),
                "depositos": dep.get("count", 0),
                "ltv": dep.get("ltv", 0.0),
                "status": user.get("event", "join"),
                "saiu_em": saiu_em,
            })
        leads.sort(key=lambda x: x.get("entrada") or "", reverse=True)
        return {"leads": leads}
    except Exception as e:
        print(f"[LEADS ERRO] {e}")
        return {"leads": []}

@app.get("/telegram/members-status")
def telegram_members_status():
    """Retorna o status atual (join/leave mais recente) por user_id."""
    try:
        r = db.table("telegram_members").select("user_id,first_name,last_name,username,event,created_at").order("created_at", ascending=False).execute()
        rows = r.data or []
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

@app.get("/tracker/stats")
async def tracker_stats(canal_id: str = None, data_inicio: str = None, data_fim: str = None):
    """Retorna métricas do dashboard para o canal e período selecionados."""
    try:
        # Helper to apply date filters
        def aplicar_datas(q, inicio, fim):
            if inicio:
                q = q.gte("created_at", inicio + "T00:00:00")
            if fim:
                q = q.lte("created_at", fim + "T23:59:59")
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

        # Evolução diária (últimos 14 dias de pageviews, joins e saidas)
        from collections import defaultdict
        import datetime
        pv_data = r_pv.data or []
        jo_data = r_jo.data or []
        sa_data = r_sa.data or []
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
        hoje = datetime.date.today()
        dias = [(hoje - datetime.timedelta(days=i)).isoformat() for i in range(13, -1, -1)]
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

    async with httpx.AsyncClient() as client:
        # 1. Deletar webhook temporariamente
        await client.post(f"https://api.telegram.org/bot{bot_token}/deleteWebhook")

        # 2. Buscar updates pendentes
        resp = await client.get(
            f"https://api.telegram.org/bot{bot_token}/getUpdates",
            params={"allowed_updates": ["my_chat_member"], "limit": 100}
        )
        data = resp.json()

        # 3. Reativar webhook imediatamente
        await client.post(
            f"https://api.telegram.org/bot{bot_token}/setWebhook",
            json={"url": webhook_url, "allowed_updates": ["chat_member", "my_chat_member"]}
        )

    if not data.get("ok"):
        raise HTTPException(status_code=400, detail=data.get("description", "Erro ao buscar updates"))

    salvos = 0
    for update in data.get("result", []):
        mcm = update.get("my_chat_member")
        if not mcm:
            continue
        chat = mcm.get("chat", {})
        new_status = mcm.get("new_chat_member", {}).get("status", "")
        if chat.get("type") not in ("channel", "supergroup"):
            continue
        if new_status not in ("administrator", "member"):
            continue

        chat_id    = chat.get("id")
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

    return {"ok": True, "salvos": salvos}


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
            json={"url": webhook_url, "allowed_updates": ["chat_member", "my_chat_member"]},
        )

    result = resp.json()
    print(f"[TELEGRAM SETUP] {result}")
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
