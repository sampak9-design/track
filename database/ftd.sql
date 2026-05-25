-- Migration: adiciona coluna `tipo` em depositos pra separar FTD de recorrentes
-- Roda essa SQL no SQL Editor do Supabase.
--
-- Após rodar, o endpoint POST /ftd vai gravar com tipo='ftd' e
-- POST/GET /deposito vai gravar com tipo='recorrente'.
-- Registros antigos (antes da migration) ficam com tipo=NULL — o cálculo de
-- FTD heurístico (primeiro depósito por email) continua cobrindo esses casos.

ALTER TABLE depositos
  ADD COLUMN IF NOT EXISTS tipo TEXT;

-- Index pra acelerar o dedup (busca por email + tipo + created_at)
CREATE INDEX IF NOT EXISTS idx_depositos_email_tipo_created
  ON depositos (email, tipo, created_at DESC);

-- (Opcional) Voltar atrás:
-- ALTER TABLE depositos DROP COLUMN IF EXISTS tipo;
-- DROP INDEX IF EXISTS idx_depositos_email_tipo_created;
