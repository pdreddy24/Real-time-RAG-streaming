-- migrations/001_rag_schema.sql
BEGIN;

CREATE SCHEMA IF NOT EXISTS rag;

-- Optional vector extension (won't break if missing)
DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS vector;
EXCEPTION WHEN OTHERS THEN
  -- vector extension not installed; continue
END $$;

CREATE TABLE IF NOT EXISTS rag.documents (
  id           bigserial PRIMARY KEY,
  event_id     text UNIQUE,
  source       text,
  title        text,
  wiki         text,
  domain       text,
  url          text,
  diff_url     text,
  ts_epoch     bigint,
  dt_iso       timestamptz,
  user_name    text,
  comment      text,
  content      text,
  created_at   timestamptz DEFAULT now()
);

-- If pgvector is present, store embeddings; else store JSON as fallback
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'vector') THEN
    EXECUTE 'ALTER TABLE rag.documents ADD COLUMN IF NOT EXISTS embedding vector(1536);';
  ELSE
    EXECUTE 'ALTER TABLE rag.documents ADD COLUMN IF NOT EXISTS embedding_json jsonb;';
  END IF;
END $$;

COMMIT;
