-- rag/db/schema.sql
-- Warehouse model for Wikimedia → Kafka → Postgres + keep existing document_chunks for RAG

CREATE EXTENSION IF NOT EXISTS vector;

-- -------------------------
-- Bronze (raw truth)
-- -------------------------
CREATE TABLE IF NOT EXISTS bronze_wikimedia_event (
  kafka_topic     TEXT NOT NULL,
  kafka_partition INT  NOT NULL,
  kafka_offset    BIGINT NOT NULL,

  ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  source          TEXT NOT NULL DEFAULT 'wikimedia',
  payload         JSONB NOT NULL,

  PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_bronze_payload_gin
  ON bronze_wikimedia_event USING gin (payload);

-- -------------------------
-- Dimensions
-- -------------------------
CREATE TABLE IF NOT EXISTS dim_wiki (
  wiki_id   BIGSERIAL PRIMARY KEY,
  wiki_code TEXT NOT NULL UNIQUE,     -- e.g. enwiki
  domain    TEXT                      -- e.g. en.wikipedia.org
);

CREATE TABLE IF NOT EXISTS dim_page (
  page_id   BIGSERIAL PRIMARY KEY,
  wiki_id   BIGINT NOT NULL REFERENCES dim_wiki(wiki_id),
  namespace INT,
  title     TEXT NOT NULL,
  url       TEXT,

  UNIQUE (wiki_id, namespace, title)
);

CREATE TABLE IF NOT EXISTS dim_user (
  user_id   BIGSERIAL PRIMARY KEY,
  username  TEXT NOT NULL UNIQUE,
  is_bot    BOOLEAN
);

-- -------------------------
-- Fact table
-- -------------------------
CREATE TABLE IF NOT EXISTS fact_edit (
  kafka_topic     TEXT NOT NULL,
  kafka_partition INT  NOT NULL,
  kafka_offset    BIGINT NOT NULL,
  doc_key         TEXT NOT NULL UNIQUE,  -- topic/partition/offset

  occurred_at     TIMESTAMPTZ,
  wiki_id         BIGINT REFERENCES dim_wiki(wiki_id),
  page_id         BIGINT REFERENCES dim_page(page_id),
  user_id         BIGINT REFERENCES dim_user(user_id),

  event_type      TEXT,
  comment         TEXT,
  is_bot          BOOLEAN,
  is_minor        BOOLEAN,

  rev_old         BIGINT,
  rev_new         BIGINT,
  bytes_old       INT,
  bytes_new       INT,

  notify_url      TEXT,
  server          TEXT,

  PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_fact_edit_time ON fact_edit (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_fact_edit_page ON fact_edit (page_id);
CREATE INDEX IF NOT EXISTS idx_fact_edit_user ON fact_edit (user_id);

-- -------------------------
-- Optional: add kafka_offset to your existing RAG table if missing
-- (safe: nullable)
-- -------------------------
ALTER TABLE IF EXISTS document_chunks
  ADD COLUMN IF NOT EXISTS kafka_offset BIGINT;
