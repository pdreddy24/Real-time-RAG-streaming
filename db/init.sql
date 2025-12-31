CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS rag_chunks (
  id BIGSERIAL PRIMARY KEY,
  doc_id TEXT NOT NULL,
  chunk_id INT NOT NULL,
  source TEXT,
  text TEXT NOT NULL,
  meta JSONB DEFAULT '{}'::jsonb,
  embedding vector(384),
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (doc_id, chunk_id)
);

-- Basic search index (optional but good)
CREATE INDEX IF NOT EXISTS rag_chunks_doc_id_idx ON rag_chunks(doc_id);
