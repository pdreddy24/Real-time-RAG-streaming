# rag/indexer/config.py
from __future__ import annotations

import os
from dataclasses import dataclass

# Optional local .env support
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


def _getenv(key: str, default: str = "") -> str:
    return (os.getenv(key, default) or "").strip()


@dataclass(frozen=True)
class Settings:
    # Kafka
    kafka_brokers: str
    kafka_topic: str
    kafka_group: str
    auto_offset_reset: str

    poll_timeout: float
    heartbeat_every_polls: int

    # Database
    postgres_dsn: str
    chunks_table: str

    # Embeddings
    openai_api_key: str
    embed_model: str
    embed_dims: int
    embed_enabled: bool

    # Chunking
    chunk_max_chars: int
    chunk_overlap_chars: int

    # Stability
    indexer_warmup_seconds: float
    kafka_retry_sleep_seconds: float

    # Logging
    log_level: str


def load_settings() -> Settings:
    kafka_brokers = _getenv("KAFKA_BROKERS", "wikimedia-redpanda:9092")
    kafka_topic = _getenv("KAFKA_TOPIC", "wikimedia.cleaned")
    kafka_group = _getenv("KAFKA_GROUP", "rag-indexer-v2")

    # CRITICAL: earliest so you don’t miss existing messages when group is new
    auto_offset_reset = _getenv("KAFKA_AUTO_OFFSET_RESET", "earliest").lower()
    if auto_offset_reset not in ("earliest", "latest"):
        auto_offset_reset = "earliest"

    postgres_dsn = _getenv("DATABASE_URL") or _getenv("POSTGRES_DSN")
    if not postgres_dsn:
        raise RuntimeError("Missing DATABASE_URL or POSTGRES_DSN for indexer")

    chunks_table = _getenv("CHUNKS_TABLE", "document_chunks")

    embed_enabled = _getenv("EMBED_ENABLED", "true").lower() in ("1", "true", "yes", "y")
    openai_api_key = _getenv("OPENAI_API_KEY", "")
    embed_model = _getenv("OPENAI_EMBED_MODEL", _getenv("EMBED_MODEL", "text-embedding-3-small"))
    embed_dims = int(_getenv("EMBED_DIMS", "1536"))

    if embed_enabled and not openai_api_key:
        raise RuntimeError("EMBED_ENABLED=true but OPENAI_API_KEY is missing")

    return Settings(
        kafka_brokers=kafka_brokers,
        kafka_topic=kafka_topic,
        kafka_group=kafka_group,
        auto_offset_reset=auto_offset_reset,
        poll_timeout=float(_getenv("POLL_TIMEOUT", "1.0")),
        heartbeat_every_polls=int(_getenv("HEARTBEAT_EVERY", "10")),
        postgres_dsn=postgres_dsn,
        chunks_table=chunks_table,
        openai_api_key=openai_api_key,
        embed_model=embed_model,
        embed_dims=embed_dims,
        embed_enabled=embed_enabled,
        chunk_max_chars=int(_getenv("CHUNK_MAX_CHARS", "1200")),
        chunk_overlap_chars=int(_getenv("CHUNK_OVERLAP_CHARS", "150")),
        indexer_warmup_seconds=float(_getenv("INDEXER_WARMUP_SECONDS", "0")),
        kafka_retry_sleep_seconds=float(_getenv("KAFKA_RETRY_SLEEP_SECONDS", "1.0")),
        log_level=_getenv("LOG_LEVEL", "INFO").upper(),
    )


settings = load_settings()
