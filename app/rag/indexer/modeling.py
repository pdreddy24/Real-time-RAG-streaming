# rag/indexer/modeling.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional
import json


def _ts_to_dt(ts: Any) -> Optional[datetime]:
    """
    Wikimedia 'timestamp' in your rc looks like epoch seconds.
    """
    try:
        ts_int = int(ts)
        if ts_int <= 0:
            return None
        return datetime.fromtimestamp(ts_int, tz=timezone.utc)
    except Exception:
        return None


def persist_warehouse(cur, *, topic: str, partition: int, offset: int, event: Dict[str, Any]) -> None:
    """
    Writes:
      - bronze_wikimedia_event (raw)
      - dim_wiki, dim_page, dim_user
      - fact_edit
    Idempotent per (topic,partition,offset).
    """
    doc_key = f"{topic}/{partition}/{offset}"

    # Your cleaned event contains "raw" recentchange json
    rc = event.get("raw") or {}

    wiki_code = rc.get("wiki") or None
    domain = rc.get("server_name") or None
    namespace = rc.get("namespace")
    title = rc.get("title") or event.get("title") or None
    url = rc.get("title_url") or event.get("url") or None

    username = rc.get("user") or None
    is_bot = rc.get("bot")
    comment = rc.get("comment") or None
    event_type = rc.get("type") or None
    is_minor = rc.get("minor")

    occurred_at = _ts_to_dt(rc.get("timestamp"))

    rev_old = ((rc.get("revision") or {}).get("old"))
    rev_new = ((rc.get("revision") or {}).get("new"))
    bytes_old = ((rc.get("length") or {}).get("old"))
    bytes_new = ((rc.get("length") or {}).get("new"))

    notify_url = rc.get("notify_url") or None
    server = rc.get("server_name") or None

    # -------------------------
    # Bronze
    # -------------------------
    cur.execute(
        """
        INSERT INTO bronze_wikimedia_event (kafka_topic, kafka_partition, kafka_offset, source, payload)
        VALUES (%s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (kafka_topic, kafka_partition, kafka_offset)
        DO UPDATE SET payload = EXCLUDED.payload, ingested_at = now()
        """,
        (topic, partition, offset, event.get("source", "wikimedia"), json.dumps(event)),
    )

    # -------------------------
    # Dimensions
    # -------------------------
    wiki_id = None
    if wiki_code:
        cur.execute(
            """
            INSERT INTO dim_wiki (wiki_code, domain)
            VALUES (%s, %s)
            ON CONFLICT (wiki_code) DO UPDATE SET domain = EXCLUDED.domain
            RETURNING wiki_id
            """,
            (wiki_code, domain),
        )
        wiki_id = cur.fetchone()[0]

    page_id = None
    if wiki_id and title is not None:
        cur.execute(
            """
            INSERT INTO dim_page (wiki_id, namespace, title, url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (wiki_id, namespace, title)
            DO UPDATE SET url = COALESCE(EXCLUDED.url, dim_page.url)
            RETURNING page_id
            """,
            (wiki_id, namespace, title, url),
        )
        page_id = cur.fetchone()[0]

    user_id = None
    if username:
        cur.execute(
            """
            INSERT INTO dim_user (username, is_bot)
            VALUES (%s, %s)
            ON CONFLICT (username) DO UPDATE SET is_bot = COALESCE(EXCLUDED.is_bot, dim_user.is_bot)
            RETURNING user_id
            """,
            (username, is_bot),
        )
        user_id = cur.fetchone()[0]

    # -------------------------
    # Fact
    # -------------------------
    cur.execute(
        """
        INSERT INTO fact_edit (
          kafka_topic, kafka_partition, kafka_offset, doc_key,
          occurred_at, wiki_id, page_id, user_id,
          event_type, comment, is_bot, is_minor,
          rev_old, rev_new, bytes_old, bytes_new,
          notify_url, server
        )
        VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s)
        ON CONFLICT (kafka_topic, kafka_partition, kafka_offset)
        DO NOTHING
        """,
        (
            topic, partition, offset, doc_key,
            occurred_at, wiki_id, page_id, user_id,
            event_type, comment, is_bot, is_minor,
            rev_old, rev_new, bytes_old, bytes_new,
            notify_url, server,
        ),
    )
