from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_brokers: str = os.getenv("KAFKA_BROKERS", "wikimedia-redpanda:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "wikimedia.cleaned")
    kafka_group: str = os.getenv("KAFKA_GROUP", "rag-indexer-v2")

    poll_timeout: float = float(os.getenv("POLL_TIMEOUT", "1.0"))
    heartbeat_every_polls: int = int(os.getenv("HEARTBEAT_EVERY", "10"))

    database_url: str = os.getenv("DATABASE_URL", "")

    # embeddings
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    embed_model: str = os.getenv("OPENAI_EMBED_MODEL", os.getenv("EMBED_MODEL", "text-embedding-3-small"))
    embed_enabled: bool = os.getenv("EMBED_ENABLED", "true").lower() in ("1", "true", "yes", "y")

    # chunking
    chunk_max_chars: int = int(os.getenv("CHUNK_MAX_CHARS", "1200"))
    chunk_overlap_chars: int = int(os.getenv("CHUNK_OVERLAP_CHARS", "150"))

    # stability
    indexer_warmup_seconds: float = float(os.getenv("INDEXER_WARMUP_SECONDS", "5"))
    kafka_retry_sleep_seconds: float = float(os.getenv("KAFKA_RETRY_SLEEP_SECONDS", "1.0"))
