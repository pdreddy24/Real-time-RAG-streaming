
from __future__ import annotations

import os
import re
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple

from rag.db.postgres import connect_postgres, ensure_schema, dsn_from_env

logger = logging.getLogger("rag.modeler")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

BATCH_SIZE = int(os.getenv("MODELER_BATCH_SIZE", "500"))
SLEEP_SECONDS = float(os.getenv("MODELER_SLEEP_SECONDS", "5"))
STATE_NAME = os.getenv("MODELER_STATE_NAME", "default")


_kv_re = re.compile(r"(\w+)=([^|]+)")


def _parse_content(content: str) -> Dict[str, str]:
    """
    Parses your stored format:
      "type=edit | wiki=enwiki | title=... | user=... | bot=False | comment=... | server=... | timestamp=..."
    """
    out: Dict[str, str] = {}
    for m in _kv_re.finditer(content):
        k = m.group(1).strip()
        v = m.group(2).strip()
        out[k] = v
    return out


def _to_bool(v: Optional[str]) -> Optional[bool]:
    if v is None:
        return None
    vv = v.strip().lower()
    if vv in ("true", "1", "yes"):
        return True
    if vv in ("false", "0", "no"):
        return False
    return None


def _to_ts_from_unix(v: Optional[str]) -> Optional[datetime]:
    if not v:
        return None
    try:
        sec = int(v)
        return datetime.fromtimestamp(sec, tz=timezone.utc)
    except Exception:
        return None


@dataclass
class ChunkRow:
    id: int
    kafka_topic: Optional[str]
    kafka_partition: Optional[int]
    kafka_offset: Optional[int]
    title: Optional[str]
    url: Optional[str]
    content: Optional[str]
    created_at: datetime


def _get_state(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_doc_chunk_id FROM modeler_state WHERE name=%s;",
            (STATE_NAME,),
        )
        row = cur.fetchone()
        if not row:
            cur.execute(
                "INSERT INTO modeler_state (name, last_doc_chunk_id) VALUES (%s, 0) ON CONFLICT (name) DO NOTHING;",
                (STATE_NAME,),
            )
            conn.commit()
            return 0
        return int(row["last_doc_chunk_id"])


def _set_state(conn, last_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE modeler_state
            SET last_doc_chunk_id=%s, updated_at=now()
            WHERE name=%s;
            """,
            (last_id, STATE_NAME),
        )
    conn.commit()


def _fetch_batch(conn, after_id: int) -> List[ChunkRow]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, kafka_topic, kafka_partition, kafka_offset, title, url, content, created_at
            FROM document_chunks
            WHERE id > %s
            ORDER BY id ASC
            LIMIT %s;
            """,
            (after_id, BATCH_SIZE),
        )
        rows = cur.fetchall()

    out: List[ChunkRow] = []
    for r in rows:
        out.append(
            ChunkRow(
                id=int(r["id"]),
                kafka_topic=r.get("kafka_topic"),
                kafka_partition=r.get("kafka_partition"),
                kafka_offset=r.get("kafka_offset"),
                title=r.get("title"),
                url=r.get("url"),
                content=r.get("content"),
                created_at=r["created_at"],
            )
        )
    return out


def _upsert_dims_and_fact(conn, chunks: List[ChunkRow]) -> int:
    """
    Inserts dims + fact with ON CONFLICT so it’s idempotent.
    Returns number of fact rows inserted (best-effort).
    """
    inserted = 0

    with conn.cursor() as cur:
        for c in chunks:
            if not c.content:
                continue
            if c.kafka_topic is None or c.kafka_partition is None or c.kafka_offset is None:
                
                continue

            kv = _parse_content(c.content)

            event_type = kv.get("type") or "edit"
            wiki = kv.get("wiki") or ""
            user_name = kv.get("user") or kv.get("user_name") or ""
            is_bot = _to_bool(kv.get("bot"))
            comment = kv.get("comment") or ""
            server = kv.get("server") or ""

            edited_at = _to_ts_from_unix(kv.get("timestamp")) or c.created_at
            ingested_at = c.created_at

            # dim_wiki
            if wiki:
                cur.execute(
                    """
                    INSERT INTO dim_wiki (wiki, server, first_seen_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (wiki) DO UPDATE
                    SET server = COALESCE(EXCLUDED.server, dim_wiki.server);
                    """,
                    (wiki, server or None, edited_at),
                )

            # dim_user
            if user_name:
                cur.execute(
                    """
                    INSERT INTO dim_user (user_name, first_seen_at)
                    VALUES (%s, %s)
                    ON CONFLICT (user_name) DO NOTHING;
                    """,
                    (user_name, edited_at),
                )

            # dim_page (keyed by wiki+url)
            if wiki and c.url:
                cur.execute(
                    """
                    INSERT INTO dim_page (wiki, url, title, first_seen_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (wiki, url) DO UPDATE
                    SET title = EXCLUDED.title;
                    """,
                    (wiki, c.url, c.title, edited_at),
                )

            # fact_edit (event identity = kafka_topic/partition/offset)
            cur.execute(
                """
                INSERT INTO fact_edit (
                  kafka_topic, kafka_partition, kafka_offset,
                  doc_chunk_id,
                  event_type, wiki, title, url, user_name, is_bot, comment, server,
                  edited_at, ingested_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING;
                """,
                (
                    c.kafka_topic,
                    c.kafka_partition,
                    c.kafka_offset,
                    c.id,
                    event_type,
                    wiki or None,
                    c.title,
                    c.url,
                    user_name or None,
                    is_bot,
                    comment or None,
                    server or None,
                    edited_at,
                    ingested_at,
                ),
            )

            if cur.rowcount == 1:
                inserted += 1

    conn.commit()
    return inserted


def run_once() -> int:
    conn = connect_postgres(dsn_from_env())
    try:
        ensure_schema(conn)

        last_id = _get_state(conn)
        batch = _fetch_batch(conn, last_id)
        if not batch:
            return 0

        inserted = _upsert_dims_and_fact(conn, batch)
        _set_state(conn, batch[-1].id)

        logger.info("Modeled %d new chunks into dims/fact", len(batch))
        return inserted
    finally:
        conn.close()


def main() -> int:
    once = ("--once" in os.sys.argv)
    while True:
        _ = run_once()
        if once:
            break
        time.sleep(SLEEP_SECONDS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
