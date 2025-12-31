-- migrations/002_indexes.sql
BEGIN;

CREATE INDEX IF NOT EXISTS idx_docs_created_at ON rag.documents(created_at);
CREATE INDEX IF NOT EXISTS idx_docs_dt_iso     ON rag.documents(dt_iso);
CREATE INDEX IF NOT EXISTS idx_docs_wiki       ON rag.documents(wiki);
CREATE INDEX IF NOT EXISTS idx_docs_domain     ON rag.documents(domain);

-- Full-text search (makes API feel “fast” even without vector)
ALTER TABLE rag.documents
  ADD COLUMN IF NOT EXISTS content_tsv tsvector
  GENERATED ALWAYS AS (to_tsvector('english', coalesce(content,''))) STORED;

CREATE INDEX IF NOT EXISTS idx_docs_fts ON rag.documents USING GIN(content_tsv);

-- If vector exists, add ANN index (optional)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'vector') THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_docs_embedding_ann ON rag.documents USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);';
  END IF;
END $$;

COMMIT;
