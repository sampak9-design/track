"""
Backfill multi-tenant: atribui TODOS os dados atuais ao projeto principal
e seta o projeto_id nos logins existentes.

Rodar UMA vez, DEPOIS da Parte 1 do migration_multitenant.sql:
    python3 backfill_multitenant.py
"""
import os
import uuid
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
db = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

MAIN = "1398a8e4-d176-49ea-ac3f-cb8b89e78ae2"
# Dono do projeto principal (herda todos os dados atuais). Os demais logins
# sem projeto ganham um projeto próprio ZERADO.
MAIN_EMAIL = "masantos102514@gmail.com"

# 1. Carimba todas as linhas antigas (projeto_id nulo) com o projeto principal
tabelas = ["cadastros", "depositos", "configuracoes", "tracker_entradas",
           "tracker_pageviews", "conversion_logs", "telegram_members"]
for t in tabelas:
    try:
        r = db.table(t).update({"projeto_id": MAIN}).is_("projeto_id", "null").execute()
        print(f"[{t}] {len(r.data or [])} linhas atribuídas ao projeto principal")
    except Exception as e:
        print(f"[{t}] ERRO: {e}")

# 2. Seta projeto_id no app_metadata dos usuários que ainda não têm
try:
    page = db.auth.admin.list_users()
    users = page if isinstance(page, list) else getattr(page, "users", []) or []
    for u in users:
        meta = getattr(u, "app_metadata", None) or {}
        if meta.get("projeto_id"):
            print(f"[user] {u.email} já tem projeto {meta.get('projeto_id')}")
            continue
        # dono principal herda os dados atuais; qualquer outro ganha projeto novo (zerado)
        pid = MAIN if (u.email or "").lower() == MAIN_EMAIL else str(uuid.uuid4())
        db.auth.admin.update_user_by_id(u.id, {"app_metadata": {"projeto_id": pid}})
        tag = "PRINCIPAL" if pid == MAIN else "novo/zerado"
        print(f"[user] {u.email} -> {pid} ({tag})")
except Exception as e:
    print(f"[users] ERRO: {e}")

print("\nBackfill concluído. Agora rode a PARTE 2 do migration_multitenant.sql.")
