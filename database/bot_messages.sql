-- Tabela: bot_messages
-- Armazena histórico de mensagens trocadas entre o bot e os usuários.
-- Usada pelo Inbox da aba Automação.

create table if not exists bot_messages (
  id              bigserial primary key,
  user_id         text not null,
  username        text,
  first_name      text,
  last_name       text,
  canal_id        bigint references telegram_canais(id) on delete set null,
  canal_nome      text,
  direcao         text not null check (direcao in ('out', 'in')),
  evento          text,
  tipo            text default 'texto',
  conteudo        text,
  midia_url       text,
  botoes          jsonb,
  status          text,
  response_code   int,
  response_body   text,
  created_at      timestamptz default now()
);

create index if not exists idx_bot_messages_user    on bot_messages(user_id);
create index if not exists idx_bot_messages_canal   on bot_messages(canal_id);
create index if not exists idx_bot_messages_created on bot_messages(created_at desc);
