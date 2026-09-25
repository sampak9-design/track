-- ============================================================
-- NOTIFICAÇÕES PUSH (PWA) — rode uma vez no SQL Editor do Supabase.
-- Só CRIA uma tabela nova; não altera nem apaga nada existente.
-- ============================================================

create table if not exists push_inscricoes (
  id          bigint generated always as identity primary key,
  criado_em   timestamptz not null default now(),
  projeto_id  text not null,
  endpoint    text not null unique,     -- endereço do aparelho no serviço de push
  p256dh      text not null,
  auth        text not null,
  aparelho    text,                     -- descrição (ex.: iPhone · Safari)
  prefs       jsonb not null default '{"ftd":true,"deposito":true,"cadastro":false,"entrada":false,"alerta":true}'::jsonb
);

create index if not exists push_inscricoes_projeto_idx on push_inscricoes (projeto_id);

-- Acesso só pelo backend (service role).
alter table push_inscricoes enable row level security;
