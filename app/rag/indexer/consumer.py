import json
import logging
import os
import time
import hashlib
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError
from psycopg import sql
from psycopg_pool import ConnectionPool

from rag.indexer.embedder import build_embedder

logger = logging.getLogger("rag.indexer")


# -----------------------
# Settings (env-driven)
# -----------------------
def _env_bool(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return int(default)


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return float(default)


@dataclass
class Settings:
    # Kafka
    kafka_brokers: str
    kafka_topic: str
    kafka_group: str
    auto_offset_reset: str
    poll_timeout: float
    kafka_retry_sleep_seconds: float

    # Commit behavior
    enable_auto_commit: bool
    commit_every_n: int
    commit_every_seconds: float

    # Postgres
    postgres_dsn: str
    chunks_table: str

    # Embeddings (optional)
    embed_enabled: bool
    embed_model: str
    embed_dims: int
    openai_api_key: str

    # runtime
    pool: ConnectionPool | None = None
    embedder: object | None = None  # built once at startup

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_brokers=os.environ["KAFKA_BROKERS"],
            kafka_topic=os.environ["KAFKA_TOPIC"],
            kafka_group=os.environ["KAFKA_GROUP"],
            auto_offset_reset=os.getenv("AUTO_OFFSET_RESET", "earliest"),
            poll_timeout=_env_float("POLL_TIMEOUT", "1.0"),
            kafka_retry_sleep_seconds=_env_float("KAFKA_RETRY_SLEEP_SECONDS", "2"),

            # Default: manual commit (recommended for "commit after DB write")
            enable_auto_commit=_env_bool("ENABLE_AUTO_COMMIT", "false"),
            commit_every_n=_env_int("COMMIT_EVERY_N", "10"),
            commit_every_seconds=_env_float("COMMIT_EVERY_SECONDS", "5"),

            postgres_dsn=os.environ["POSTGRES_DSN"],
            chunks_table=os.getenv("CHUNKS_TABLE", "document_chunks"),
            embed_enabled=_env_bool("EMBED_ENABLED", "false"),
            embed_model=os.getenv("OPENAI_EMBED_MODEL", os.getenv("EMBED_MODEL", "text-embedding-3-small")),
            embed_dims=_env_int("EMBED_DIMS", "1536"),
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
        )


# -----------------------
# DB schema
# -----------------------
def _ensure_db_schema(s: Settings) -> None:
    table = s.chunks_table
    dims = int(s.embed_dims)

    q_ext = sql.SQL("CREATE EXTENSION IF NOT EXISTS vector;")

    q_table = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {t} (
            id BIGSERIAL PRIMARY KEY,
            doc_key TEXT,
            source TEXT,
            title TEXT,
            url TEXT,
            content TEXT NOT NULL,
            embedding VECTOR({dims}),
            created_at TIMESTAMPTZ DEFAULT now()
        );
    """).format(
        t=sql.Identifier(table),
        dims=sql.SQL(str(dims)),
    )

    q_doc_key_col = sql.SQL("ALTER TABLE {t} ADD COLUMN IF NOT EXISTS doc_key TEXT;").format(
        t=sql.Identifier(table),
    )

    q_doc_key_uq = sql.SQL("""
        CREATE UNIQUE INDEX IF NOT EXISTS {idx}
        ON {t} (doc_key);
    """).format(
        idx=sql.Identifier(f"{table}_doc_key_uq"),
        t=sql.Identifier(table),
    )

    q_hnsw = sql.SQL("""
        CREATE INDEX IF NOT EXISTS {idx}
        ON {t}
        USING hnsw (embedding vector_cosine_ops);
    """).format(
        idx=sql.Identifier(f"{table}_embedding_hnsw"),
        t=sql.Identifier(table),
    )

    with s.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(q_ext)
            cur.execute(q_table)
            cur.execute(q_doc_key_col)
            cur.execute(q_doc_key_uq)
            cur.execute(q_hnsw)
        conn.commit()


# -----------------------
# Helpers
# -----------------------
def _parse_value(b: bytes) -> dict:
    """
    Kafka value can be:
      - JSON object
      - JSON string of JSON (double-encoded)
    """
    raw = b.decode("utf-8", errors="ignore").strip()
    if not raw:
        raise ValueError("Empty message value")

    data = json.loads(raw)
    if isinstance(data, str):
        data = json.loads(data)

    if not isinstance(data, dict):
        raise ValueError(f"Expected dict JSON but got: {type(data)}")

    return data


def _extract_fields(d: dict) -> tuple[str | None, str | None, str | None, str]:
    source = d.get("source")
    title = d.get("title")
    url = d.get("url")
    content = d.get("content")

    if not content or not isinstance(content, str):
        raise ValueError("Missing required field 'content' (string)")

    return source, title, url, content


def _make_doc_key(source: str | None, title: str | None, url: str | None, content: str) -> str:
    """
    Deterministic id to dedupe replays.
    Prefer url/title plus content hash.
    """
    src = (source or "").strip()
    u = (url or "").strip()
    t = (title or "").strip()

    content_hash = hashlib.sha1(content.encode("utf-8", errors="ignore")).hexdigest()
    stable = f"v1|src={src}|url={u}|title={t}|content_sha1={content_hash}"
    return hashlib.sha1(stable.encode("utf-8", errors="ignore")).hexdigest()


def _maybe_embed(s: Settings, content: str) -> list[float] | None:
    """
    EMBED_ENABLED=false => None (no cost)
    """
    if not s.embed_enabled:
        return None

    # cost guardrail: fail fast if user explicitly chose openai but forgot key
    if (os.getenv("EMBED_PROVIDER", "").lower().strip() == "openai") and not s.openai_api_key:
        raise RuntimeError("EMBED_ENABLED=true but OPENAI_API_KEY is empty")

    if s.embedder is None:
        raise RuntimeError("Embedder not initialized")

    vec = s.embedder.embed_one(content)
    if vec is None:
        raise RuntimeError("Embedding returned None")

    if len(vec) != int(s.embed_dims):
        raise RuntimeError(f"Embedding dims mismatch: got {len(vec)} expected {int(s.embed_dims)}")

    return [float(x) for x in vec]


def _upsert_chunk(s: Settings, doc_key: str, source, title, url, content, embedding):
    """
    Upsert on doc_key to prevent duplicates.
    Don't overwrite an existing embedding with NULL.
    """
    q = sql.SQL("""
        INSERT INTO {t} (doc_key, source, title, url, content, embedding)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (doc_key) DO UPDATE
        SET
            source  = EXCLUDED.source,
            title   = EXCLUDED.title,
            url     = EXCLUDED.url,
            content = EXCLUDED.content,
            embedding = COALESCE(EXCLUDED.embedding, {t}.embedding);
    """).format(t=sql.Identifier(s.chunks_table))

    with s.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (doc_key, source, title, url, content, embedding))
        conn.commit()


# -----------------------
# Commit strategy
# -----------------------
def maybe_commit(consumer: Consumer, last_msg, processed_since_commit: int, last_commit_ts: float, s: Settings):
    """
    Commit ONLY when we have processed something.
    Commit using commit(message=last_msg) so we do not depend on local offset store.
    """
    if s.enable_auto_commit:
        return processed_since_commit, last_commit_ts

    if last_msg is None or processed_since_commit <= 0:
        return processed_since_commit, last_commit_ts

    now = time.time()
    due_by_n = processed_since_commit >= int(s.commit_every_n)
    due_by_time = (now - last_commit_ts) >= float(s.commit_every_seconds)

    if not (due_by_n or due_by_time):
        return processed_since_commit, last_commit_ts

    try:
        consumer.commit(message=last_msg, asynchronous=False)
        logger.info("Committed offsets (processed_since_commit=%s)", processed_since_commit)
        return 0, now
    except Exception as e:
        # Don’t spam stacks every second: log error + keep going.
        logger.error("Commit failed: %s", e, exc_info=True)
        return processed_since_commit, last_commit_ts


# -----------------------
# Main
# -----------------------
def main():
    s = Settings.from_env()

    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    logger.info("rag-indexer booting...")
    logger.info(
        "KAFKA_BROKERS=%s KAFKA_TOPIC=%s KAFKA_GROUP=%s AUTO_OFFSET_RESET=%s",
        s.kafka_brokers, s.kafka_topic, s.kafka_group, s.auto_offset_reset
    )
    logger.info("POSTGRES_DSN=%s", s.postgres_dsn)
    logger.info(
        "CHUNKS_TABLE=%s EMBED_DIMS=%s EMBED_ENABLED=%s ENABLE_AUTO_COMMIT=%s",
        s.chunks_table, s.embed_dims, s.embed_enabled, s.enable_auto_commit
    )

    # Create pool
    s.pool = ConnectionPool(conninfo=s.postgres_dsn, max_size=10)

    # Ensure schema (includes doc_key + indexes)
    _ensure_db_schema(s)

    # Build embedder once
    if s.embed_enabled:
        s.embedder = build_embedder()
        logger.info(
            "EMBED_PROVIDER=%s OPENAI_EMBED_MODEL=%s",
            os.getenv("EMBED_PROVIDER", "fastembed"),
            os.getenv("OPENAI_EMBED_MODEL", s.embed_model),
        )
        logger.info("Embedder initialized: %s", type(s.embedder).__name__)

    consumer = Consumer(
        {
            "bootstrap.servers": s.kafka_brokers,
            "group.id": s.kafka_group,
            "auto.offset.reset": s.auto_offset_reset,

            # IMPORTANT: if we are doing manual commit, disable auto-commit
            "enable.auto.commit": bool(s.enable_auto_commit),
        }
    )

    consumer.subscribe([s.kafka_topic])
    logger.info("Subscribed to topic: %s", s.kafka_topic)

    processed_since_commit = 0
    last_commit_ts = time.time()
    last_msg = None

    while True:
        msg = consumer.poll(timeout=s.poll_timeout)

        # periodic commit check (only commits if something processed)
        processed_since_commit, last_commit_ts = maybe_commit(
            consumer=consumer,
            last_msg=last_msg,
            processed_since_commit=processed_since_commit,
            last_commit_ts=last_commit_ts,
            s=s,
        )

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error("Kafka error: %s", msg.error())
            time.sleep(s.kafka_retry_sleep_seconds)
            continue

        try:
            # Guardrail: tombstone / empty value messages happen in real Kafka.
            val = msg.value()
            if val is None:
                logger.warning(
                    "Skipping message with NULL value (topic=%s partition=%s offset=%s)",
                    msg.topic(), msg.partition(), msg.offset()
                )
                # Still advance progress for this offset (so we don't re-poll it forever)
                last_msg = msg
                processed_since_commit += 1
                continue

            d = _parse_value(val)
            source, title, url, content = _extract_fields(d)
            doc_key = _make_doc_key(source, title, url, content)

            embedding = _maybe_embed(s, content)
            _upsert_chunk(s, doc_key, source, title, url, content, embedding)

            logger.info(
                "Upserted row (topic=%s partition=%s offset=%s embedded=%s doc_key=%s)",
                msg.topic(), msg.partition(), msg.offset(), embedding is not None, doc_key[:10]
            )

            # Mark progress
            last_msg = msg
            processed_since_commit += 1

        except Exception as e:
            logger.exception("Failed processing message: %s", e)
            time.sleep(s.kafka_retry_sleep_seconds)


if __name__ == "__main__":
    main()
