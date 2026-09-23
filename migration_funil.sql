-- ============================================================
-- FUNIL DA PÁGINA — rode uma vez no SQL Editor do Supabase.
-- Só CRIA tabelas novas; não altera nem apaga nada existente.
-- ============================================================

-- 1 linha por visita (resumo, atualizado conforme o lead avança)
create table if not exists funil_sessoes (
  sessao_id        text primary key,
  projeto_id       text,
  external_id      text,              -- mesmo _trk_eid do tracker.js (liga às entradas no Telegram)
  criado_em        timestamptz not null default now(),
  atualizado_em    timestamptz not null default now(),
  page_url         text,
  lang             text,
  device           text,              -- mobile | desktop | tablet
  os               text,
  browser          text,
  screen_w         integer,
  screen_h         integer,
  referrer         text,
  utm_source       text,
  utm_medium       text,
  utm_campaign     text,
  utm_content      text,
  utm_term         text,
  etapa_max        text,              -- etapa mais avançada que o lead alcançou
  etapa_saida      text,              -- etapa em que estava quando saiu
  tempo_ativo_ms   integer not null default 0,
  cliques          integer not null default 0,
  clicou_telegram  boolean not null default false,
  saiu             boolean not null default false
);

-- 1 linha por ação (etapa, clique, saída)
create table if not exists funil_eventos (
  id           bigint generated always as identity primary key,
  criado_em    timestamptz not null default now(),
  projeto_id   text,
  sessao_id    text not null,
  tipo         text not null,         -- etapa | clique | saida
  nome         text,                  -- nome da etapa ou do elemento clicado
  etapa        text,                  -- etapa atual no momento da ação
  t_ms         integer,               -- milissegundos desde a entrada na página
  x            real,                  -- posição do clique (0 a 1, relativa à página)
  y            real,
  dados        jsonb
);

create index if not exists funil_sessoes_projeto_criado_idx on funil_sessoes (projeto_id, criado_em desc);
create index if not exists funil_sessoes_external_idx       on funil_sessoes (external_id);
create index if not exists funil_eventos_sessao_idx         on funil_eventos (sessao_id, t_ms);
create index if not exists funil_eventos_projeto_tipo_idx   on funil_eventos (projeto_id, tipo, criado_em desc);

-- Acesso só pelo backend (service role). Sem policies = ninguém lê/escreve direto do navegador.
alter table funil_sessoes enable row level security;
alter table funil_eventos enable row level security;
