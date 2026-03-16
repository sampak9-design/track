from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
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

    print(f"[DEPOSITO] {registro['email']} - R$ {registro['valor']}")
    return {"status": "ok", "id": result.data[0]["id"]}


# ── Telegram ─────────────────────────────────────────────────────
@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    update = await request.json()

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

    # Determinar evento
    joined  = old_status in ("left", "kicked") and new_status == "member"
    left    = old_status == "member" and new_status in ("left", "kicked")

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


@app.get("/telegram/setup")
async def telegram_setup(request: Request):
    """Registra o webhook do bot no Telegram."""
    if not TELEGRAM_BOT_TOKEN:
        raise HTTPException(status_code=400, detail="TELEGRAM_BOT_TOKEN não configurado")

    webhook_url = str(request.base_url).rstrip("/") + "/telegram/webhook"

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook",
            json={"url": webhook_url, "allowed_updates": ["chat_member"]},
        )

    result = resp.json()
    print(f"[TELEGRAM SETUP] {result}")
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
