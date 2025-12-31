CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS rag_chunks (
  id            TEXT PRIMARY KEY,
  source        TEXT NOT NULL,
  doc_id        TEXT NOT NULL,
  chunk_index   INT  NOT NULL,
  text          TEXT NOT NULL,
  metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
  embedding     vector(1536),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS rag_chunks_doc_id_idx ON rag_chunks (doc_id);
CREATE INDEX IF NOT EXISTS rag_chunks_metadata_gin ON rag_chunks USING gin (metadata);

-- Vector index (cosine). Tune lists later; start with 100.
CREATE INDEX IF NOT EXISTS rag_chunks_embedding_ivfflat
ON rag_chunks USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);
