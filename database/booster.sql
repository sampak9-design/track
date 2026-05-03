-- Booster: tabelas pra gerenciar userbots, campanhas de views/reações e ações executadas.

create table if not exists booster_contas (
  id              bigserial primary key,
  phone           text,
  username        text,
  first_name      text,
  user_id_tg      text,
  session_string  text,             -- StringSession do Telethon (base64)
  proxy           text,             -- ex: socks5://user:pass@host:porta
  has_2fa         boolean default false,
  senha_2fa_enc   text,             -- senha 2FA criptografada
  email_recovery  text,
  status          text default 'ativa' check (status in ('ativa','cooldown','banida','erro','pendente')),
  cooldown_ate    timestamptz,
  views_hoje      int default 0,
  reacoes_hoje    int default 0,
  total_views     int default 0,
  total_reacoes   int default 0,
  ultima_acao     timestamptz,
  banido_em       timestamptz,
  erro_msg        text,
  criado_em       timestamptz default now()
);
create index if not exists idx_booster_contas_status on booster_contas(status);

create table if not exists booster_campanhas (
  id                bigserial primary key,
  nome              text,
  canal_link        text not null,        -- ex: t.me/apollo/123 ou @apollo
  canal_username    text,
  msg_id            int,
  qtd_views         int default 0,
  qtd_reacoes       int default 0,
  reacoes_emojis    jsonb,                -- ['👍','❤️','🔥']
  delay_min_seg     int default 1,
  delay_max_seg     int default 30,
  janela_min        int default 30,       -- distribuir em N minutos
  status            text default 'pendente' check (status in ('pendente','rodando','pausada','concluida','cancelada','erro')),
  views_entregues   int default 0,
  reacoes_entregues int default 0,
  iniciado_em       timestamptz,
  finalizado_em     timestamptz,
  erro_msg          text,
  criado_em         timestamptz default now()
);
create index if not exists idx_booster_camp_status on booster_campanhas(status);

create table if not exists booster_acoes (
  id            bigserial primary key,
  campanha_id   bigint references booster_campanhas(id) on delete cascade,
  conta_id      bigint references booster_contas(id) on delete set null,
  tipo          text check (tipo in ('view','reacao')),
  emoji         text,
  status        text check (status in ('sucesso','erro','flood','banida')),
  erro_msg      text,
  executado_em  timestamptz default now()
);
create index if not exists idx_booster_acoes_camp on booster_acoes(campanha_id);
create index if not exists idx_booster_acoes_conta on booster_acoes(conta_id);
create index if not exists idx_booster_acoes_data on booster_acoes(executado_em desc);
