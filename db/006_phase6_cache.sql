CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS embedding_cache (
  text_hash TEXT PRIMARY KEY,
  text      TEXT NOT NULL,
  embedding VECTOR(1536) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS qa_cache (
  id BIGSERIAL PRIMARY KEY,
  query TEXT NOT NULL,
  query_hash TEXT NOT NULL,
  query_embedding VECTOR(1536) NOT NULL,
  answer TEXT NOT NULL,
  citations JSONB NOT NULL DEFAULT '[]'::jsonb,
  mode TEXT NOT NULL,
  model TEXT,
  top_k INT NOT NULL DEFAULT 5,
  filters JSONB NOT NULL DEFAULT '{}'::jsonb,
  use_count INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qa_cache_query_hash ON qa_cache (query_hash);
CREATE INDEX IF NOT EXISTS idx_qa_cache_embedding ON qa_cache USING ivfflat (query_embedding vector_cosine_ops);
