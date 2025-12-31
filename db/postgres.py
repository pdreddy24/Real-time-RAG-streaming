# rag/db/postgres.py
from __future__ import annotations

import os
from typing import Optional

import psycopg
from psycopg.rows import dict_row

from pgvector.psycopg import register_vector


def dsn_from_env() -> str:
    """
    Priority:
    1) POSTGRES_DSN
    2) DATABASE_URL
    3) build from POSTGRES_* pieces (works in Docker and host)
    """
    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if dsn:
        return dsn

    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    db = os.getenv("POSTGRES_DB", "postgres")

    # In your repo you already use POSTGRES_HOST_DOCKER=wikimedia-postgres
    host = os.getenv("POSTGRES_HOST_DOCKER") or os.getenv("POSTGRES_HOST") or "localhost"
    port = os.getenv("POSTGRES_PORT", "5432")

    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def connect_postgres(dsn: Optional[str] = None) -> psycopg.Connection:
    dsn = dsn or dsn_from_env()
    conn = psycopg.connect(dsn, autocommit=False, row_factory=dict_row)
    # Critical: makes Python Vector/list bind as Postgres "vector" not "double precision[]"
    register_vector(conn)
    return conn


def ensure_schema(conn: psycopg.Connection) -> None:
    """
    Creates:
      - vector extension
      - document_chunks (RAG store)
      - dim_wiki, dim_user, dim_page + fact_edit (modeled warehouse)
      - modeler_state (watermarking)
    """
    with conn.cursor() as cur:
        # pgvector extension
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

        # RAG chunk store
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS document_chunks (
              id              BIGSERIAL PRIMARY KEY,
              doc_key         TEXT UNIQUE,
              source          TEXT,
              title           TEXT,
              url             TEXT,
              content         TEXT,
              embedding       vector,
              created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

              kafka_topic     TEXT,
              kafka_partition INT,
              kafka_offset    BIGINT
            );
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_document_chunks_created_at
            ON document_chunks (created_at DESC);
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_document_chunks_kafka
            ON document_chunks (kafka_topic, kafka_partition, kafka_offset);
            """
        )

        # Model / star-ish schema
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dim_wiki (
              wiki          TEXT PRIMARY KEY,
              server        TEXT,
              first_seen_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dim_user (
              user_name     TEXT PRIMARY KEY,
              first_seen_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dim_page (
              wiki          TEXT NOT NULL REFERENCES dim_wiki(wiki),
              url           TEXT NOT NULL,
              title         TEXT,
              first_seen_at TIMESTAMPTZ DEFAULT now(),
              PRIMARY KEY (wiki, url)
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_edit (
              id              BIGSERIAL PRIMARY KEY,

              kafka_topic     TEXT NOT NULL,
              kafka_partition INT  NOT NULL,
              kafka_offset    BIGINT NOT NULL,

              doc_chunk_id    BIGINT NOT NULL REFERENCES document_chunks(id),

              event_type      TEXT,
              wiki            TEXT,
              title           TEXT,
              url             TEXT,
              user_name       TEXT,
              is_bot          BOOLEAN,
              comment         TEXT,
              server          TEXT,

              edited_at       TIMESTAMPTZ,
              ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )

        # Uniqueness = event identity
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_edit_event
            ON fact_edit (kafka_topic, kafka_partition, kafka_offset);
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_fact_edit_ingested_at
            ON fact_edit (ingested_at DESC);
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_fact_edit_edited_at
            ON fact_edit (edited_at DESC);
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS modeler_state (
              name             TEXT PRIMARY KEY,
              last_doc_chunk_id BIGINT NOT NULL DEFAULT 0,
              updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )

    conn.commit()
