# rag/db/bootstrap.py
from __future__ import annotations

import os
from pathlib import Path
import psycopg
from pgvector.psycopg import register_vector


def connect_postgres(dsn: str) -> psycopg.Connection:
    """
    One connection, properly configured for pgvector.
    """
    conn = psycopg.connect(dsn)
    register_vector(conn)  # critical: avoids vector <=> double precision[] issues
    conn.execute("SET statement_timeout = '30s'")
    return conn


def ensure_schema(conn: psycopg.Connection) -> None:
    """
    Applies schema.sql (idempotent).
    """
    here = Path(__file__).resolve().parent
    schema_path = here / "schema.sql"
    sql = schema_path.read_text(encoding="utf-8")

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def dsn_from_env() -> str:
    return (
        os.getenv("POSTGRES_DSN")
        or os.getenv("DATABASE_URL")
        or "postgresql://postgres:postgres@localhost:5432/postgres"
    )
