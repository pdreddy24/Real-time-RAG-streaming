CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS document_chunks (
  id          BIGSERIAL PRIMARY KEY,
  event_id    TEXT UNIQUE,
  source      TEXT NOT NULL,
  wiki        TEXT,
  title       TEXT,
  url         TEXT,
  content     TEXT NOT NULL,
  raw         JSONB,
  embedding   vector(1536),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Optional: helpful indexes (safe to keep even if empty)
CREATE INDEX IF NOT EXISTS idx_document_chunks_source ON document_chunks(source);
CREATE INDEX IF NOT EXISTS idx_document_chunks_created_at ON document_chunks(created_at DESC);
