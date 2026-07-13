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

-- config: cada projeto tem sua própria chave (remove unique só-em-chave e cria composto)
alter table configuracoes drop constraint if exists configuracoes_chave_key;
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
