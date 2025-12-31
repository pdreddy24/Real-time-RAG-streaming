from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import psycopg2
from psycopg2.extras import Json


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def _vector_literal(vec) -> Optional[str]:
    """
    pgvector accepts a literal like: '[0.1,0.2,...]'.
    We pass it as text and cast to vector in SQL.
    """
    if not vec:
        return None
    return "[" + ",".join(str(float(x)) for x in vec) + "]"


@dataclass
class DB:
    database_url: str

    def connect(self):
        if not self.database_url:
            raise RuntimeError("DATABASE_URL is empty")
        return psycopg2.connect(self.database_url)

    def insert_wikimedia_edit(self, conn, e: Dict[str, Any]) -> None:
        """
        Inserts only the columns we know exist from your schema output:
        event_time, wiki, title, user_name, bot, change_type
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO wikimedia_edits (event_time, wiki, title, user_name, bot, change_type)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    e.get("event_time"),
                    e.get("wiki"),
                    e.get("title"),
                    e.get("user_name"),
                    bool(e.get("bot", False)),
                    e.get("change_type"),
                ),
            )
        conn.commit()

    def upsert_rag_chunk(
        self,
        conn,
        *,
        chunk_id: str,
        source: str,
        doc_id: str,
        chunk_index: int,
        text: str,
        metadata: Dict[str, Any],
        embedding: Optional[list],
    ) -> None:
        vec = _vector_literal(embedding)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO rag_chunks (id, source, doc_id, chunk_index, text, metadata, embedding)
                VALUES (%s, %s, %s, %s, %s, %s, %s::vector)
                ON CONFLICT (id) DO UPDATE
                SET text = EXCLUDED.text,
                    metadata = EXCLUDED.metadata,
                    embedding = EXCLUDED.embedding
                """,
                (
                    chunk_id,
                    source,
                    doc_id,
                    int(chunk_index),
                    text,
                    Json(metadata),
                    vec,
                ),
            )
        conn.commit()
