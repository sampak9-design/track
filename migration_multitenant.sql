-- ============================================================
-- MULTI-TENANT — rode no SQL Editor do Supabase, EM 2 PARTES.
-- Projeto principal (dados atuais): 1398a8e4-d176-49ea-ac3f-cb8b89e78ae2
-- ============================================================

-- ─────────────────────────────────────────────
-- PARTE 1 — rode ANTES do backfill (adiciona colunas)
-- ─────────────────────────────────────────────
alter table cadastros         add column if not exists projeto_id text;
alter table depositos         add column if not exists projeto_id text;
alter table configuracoes     add column if not exists projeto_id text;
alter table tracker_entradas  add column if not exists projeto_id text;
alter table tracker_pageviews add column if not exists projeto_id text;
alter table conversion_logs   add column if not exists projeto_id text;
alter table telegram_members  add column if not exists projeto_id text;
alter table telegram_canais   add column if not exists projeto_id text;
alter table booster_contas    add column if not exists projeto_id text;
alter table booster_campanhas add column if not exists projeto_id text;
alter table booster_acoes     add column if not exists projeto_id text;
alter table booster_auto      add column if not exists projeto_id text;
alter table bot_messages      add column if not exists projeto_id text;

-- config: cada projeto tem sua própria chave (remove uniques antigos e cria composto)
-- IMPORTANTE: a PK antiga (user_id, chave) impedia 2 projetos de terem a mesma chave.
alter table configuracoes drop constraint if exists configuracoes_chave_key;
alter table configuracoes drop constraint if exists configuracoes_pkey;
create unique index if not exists configuracoes_projeto_chave_uidx
  on configuracoes (projeto_id, chave);

-- índices pra performance de filtro
create index if not exists cadastros_projeto_idx         on cadastros (projeto_id);
create index if not exists depositos_projeto_idx         on depositos (projeto_id);
create index if not exists tracker_entradas_projeto_idx  on tracker_entradas (projeto_id);
create index if not exists tracker_pageviews_projeto_idx on tracker_pageviews (projeto_id);
create index if not exists conversion_logs_projeto_idx   on conversion_logs (projeto_id);
create index if not exists telegram_members_projeto_idx  on telegram_members (projeto_id);
create index if not exists telegram_canais_projeto_idx   on telegram_canais (projeto_id);
create index if not exists booster_contas_projeto_idx    on booster_contas (projeto_id);
create index if not exists booster_campanhas_projeto_idx on booster_campanhas (projeto_id);
create index if not exists booster_acoes_projeto_idx     on booster_acoes (projeto_id);
create index if not exists booster_auto_projeto_idx      on booster_auto (projeto_id);
create index if not exists bot_messages_projeto_idx      on bot_messages (projeto_id);

-- >>> AGORA rode o script de backfill (backfill_multitenant.py) <<<


-- ─────────────────────────────────────────────
-- PARTE 2 — rode DEPOIS do backfill (liga o RLS/isolamento)
-- ─────────────────────────────────────────────
-- O backend usa a service role (bypassa RLS), então continua funcionando.
-- O dashboard usa o token do login: só enxerga as linhas do próprio projeto.

-- cadastros e depositos são lidos direto do front → precisam de policy de SELECT
alter table cadastros enable row level security;
drop policy if exists proj_sel on cadastros;
create policy proj_sel on cadastros for select to authenticated
  using (projeto_id = (auth.jwt() -> 'app_metadata' ->> 'projeto_id'));

alter table depositos enable row level security;
drop policy if exists proj_sel on depositos;
create policy proj_sel on depositos for select to authenticated
  using (projeto_id = (auth.jwt() -> 'app_metadata' ->> 'projeto_id'));

-- as demais só são lidas via backend (service role) → RLS liga sem policy pra bloquear anon
alter table configuracoes     enable row level security;
alter table tracker_entradas  enable row level security;
alter table tracker_pageviews enable row level security;
alter table conversion_logs   enable row level security;
alter table telegram_members  enable row level security;
alter table telegram_canais   enable row level security;
alter table booster_contas    enable row level security;
alter table booster_campanhas enable row level security;
alter table booster_acoes     enable row level security;
alter table booster_auto      enable row level security;
alter table bot_messages      enable row level security;
