import json
import logging
import os
import time
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError
from psycopg import sql
from psycopg_pool import ConnectionPool

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

    # Postgres
    postgres_dsn: str
    chunks_table: str

    # Embeddings (optional)
    embed_enabled: bool
    embed_model: str
    embed_dims: int
    openai_api_key: str

    # Pool (created at runtime)
    pool: ConnectionPool | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_brokers=os.environ["KAFKA_BROKERS"],
            kafka_topic=os.environ["KAFKA_TOPIC"],
            kafka_group=os.environ["KAFKA_GROUP"],
            auto_offset_reset=os.getenv("AUTO_OFFSET_RESET", "earliest"),
            poll_timeout=_env_float("POLL_TIMEOUT", "1.0"),
            kafka_retry_sleep_seconds=_env_float("KAFKA_RETRY_SLEEP_SECONDS", "2"),
            postgres_dsn=os.environ["POSTGRES_DSN"],
            chunks_table=os.getenv("CHUNKS_TABLE", "document_chunks"),
            embed_enabled=_env_bool("EMBED_ENABLED", "false"),
            embed_model=os.getenv("EMBED_MODEL", "text-embedding-3-small"),
            embed_dims=_env_int("EMBED_DIMS", "1536"),
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
        )


# -----------------------
# DB schema (FIXED)
# -----------------------
def _ensure_db_schema(s) -> None:
    table = s.chunks_table
    dims = int(s.embed_dims)

    q_ext = sql.SQL("CREATE EXTENSION IF NOT EXISTS vector;")

    q_table = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {t} (
            id BIGSERIAL PRIMARY KEY,
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

    q_index = sql.SQL("""
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
            cur.execute(q_index)
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
    raw = b.decode("utf-8", errors="ignore").strip().strip("\r\n")
    data = json.loads(raw)
    if isinstance(data, str):
        data = json.loads(data)
    if not isinstance(data, dict):
        raise ValueError(f"Expected dict JSON but got: {type(data)}")
    return data


def _extract_fields(d: dict) -> tuple[str | None, str | None, str | None, str]:
    """
    Your test message: source,title,url,content
    """
    source = d.get("source")
    title = d.get("title")
    url = d.get("url")
    content = d.get("content")

    if not content or not isinstance(content, str):
        raise ValueError("Missing required field 'content' (string)")

    return source, title, url, content


def _maybe_embed(s: Settings, content: str):
    """
    EMBED_ENABLED=false => return None (no cost)
    If enabled, you must implement your embedding call here.
    """
    if not s.embed_enabled:
        return None

    # If you enable embeddings later, wire OpenAI here.
    # For now we fail loudly so you don't think you're embedding when you're not.
    if not s.openai_api_key:
        raise RuntimeError("EMBED_ENABLED=true but OPENAI_API_KEY is empty")

    raise NotImplementedError("Embedding call not implemented yet.")


def _insert_chunk(s: Settings, source, title, url, content, embedding):
    q = sql.SQL("""
        INSERT INTO {t} (source, title, url, content, embedding)
        VALUES (%s, %s, %s, %s, %s);
    """).format(t=sql.Identifier(s.chunks_table))

    with s.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (source, title, url, content, embedding))
        conn.commit()


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
    logger.info("CHUNKS_TABLE=%s EMBED_DIMS=%s EMBED_ENABLED=%s", s.chunks_table, s.embed_dims, s.embed_enabled)

    # Create pool
    s.pool = ConnectionPool(conninfo=s.postgres_dsn, max_size=10)

    # Ensure schema (this is where you were crashing)
    _ensure_db_schema(s)

    consumer = Consumer(
        {
            "bootstrap.servers": s.kafka_brokers,
            "group.id": s.kafka_group,
            "auto.offset.reset": s.auto_offset_reset,
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([s.kafka_topic])
    logger.info("Subscribed to topic: %s", s.kafka_topic)

    while True:
        msg = consumer.poll(timeout=s.poll_timeout)

        if msg is None:
            continue

        if msg.error():
            # Partition EOF is not a real error.
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error("Kafka error: %s", msg.error())
            time.sleep(s.kafka_retry_sleep_seconds)
            continue

        try:
            d = _parse_value(msg.value())
            source, title, url, content = _extract_fields(d)

            embedding = _maybe_embed(s, content)
            _insert_chunk(s, source, title, url, content, embedding)

            logger.info("Inserted row (topic=%s partition=%s offset=%s)", msg.topic(), msg.partition(), msg.offset())

        except NotImplementedError as e:
            logger.error("Embedding enabled but not implemented: %s", e)
            time.sleep(s.kafka_retry_sleep_seconds)

        except Exception as e:
            logger.exception("Failed processing message: %s", e)
            time.sleep(s.kafka_retry_sleep_seconds)


if __name__ == "__main__":
    main()
