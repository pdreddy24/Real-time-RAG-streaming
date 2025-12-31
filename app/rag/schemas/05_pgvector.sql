CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS docs (
  id BIGSERIAL PRIMARY KEY,
  source TEXT,
  doc_id TEXT NOT NULL,
  chunk_id INT NOT NULL,
  content TEXT NOT NULL,
  embedding VECTOR(384) NOT NULL,
  metadata JSONB DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS docs_doc_chunk_uq
ON docs (doc_id, chunk_id);

CREATE INDEX IF NOT EXISTS docs_embedding_idx
ON docs USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
