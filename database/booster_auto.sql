-- Auto-Boost: configuração por canal pra impulsionar TODOS os posts novos automaticamente.

create table if not exists booster_auto (
  id                  bigserial primary key,
  canal_id            bigint references telegram_canais(id) on delete cascade,
  canal_telegram_id   text not null,
  canal_nome          text,
  ativo               boolean default true,
  qtd_views           int default 0,
  qtd_reacoes         int default 0,
  reacoes_emojis      jsonb default '["👍","❤️","🔥"]'::jsonb,
  delay_min_seg       int default 2,
  delay_max_seg       int default 30,
  janela_min          int default 30,
  aguardar_min_antes  int default 0,         -- min de espera após o post pra começar a impulsionar
  criado_em           timestamptz default now(),
  atualizado_em       timestamptz default now()
);

create unique index if not exists idx_booster_auto_tg_id on booster_auto(canal_telegram_id);
create index if not exists idx_booster_auto_ativo on booster_auto(ativo);
