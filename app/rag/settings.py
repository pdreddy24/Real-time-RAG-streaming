from __future__ import annotations

from dataclasses import dataclass
import os


def _env_bool(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    # Kafka
    kafka_brokers: str
    kafka_topic: str
    kafka_group: str
    auto_offset_reset: str = "earliest"
    poll_timeout: float = 1.0
    heartbeat_every_polls: int = 5

    # Postgres
    postgres_dsn: str = ""
    chunks_table: str = "document_chunks"

    # Embeddings
    openai_api_key: str = ""
    embed_model: str = "text-embedding-3-small"
    embed_dims: int = 1536
    embed_enabled: bool = True

    # Chunking
    chunk_max_chars: int = 1200
    chunk_overlap_chars: int = 200

    # Runtime
    indexer_warmup_seconds: int = 5
    kafka_retry_sleep_seconds: int = 2
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        # required
        kafka_brokers = os.environ["KAFKA_BROKERS"]
        kafka_topic = os.environ["KAFKA_TOPIC"]
        kafka_group = os.environ["KAFKA_GROUP"]
        postgres_dsn = os.environ["POSTGRES_DSN"]

        return cls(
            kafka_brokers=kafka_brokers,
            kafka_topic=kafka_topic,
            kafka_group=kafka_group,
            auto_offset_reset=os.getenv("AUTO_OFFSET_RESET", "earliest"),
            poll_timeout=float(os.getenv("POLL_TIMEOUT", "1.0")),
            heartbeat_every_polls=int(os.getenv("HEARTBEAT_EVERY_POLLS", "5")),
            postgres_dsn=postgres_dsn,
            chunks_table=os.getenv("CHUNKS_TABLE", "document_chunks"),
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
            embed_model=os.getenv("EMBED_MODEL", "text-embedding-3-small"),
            embed_dims=int(os.getenv("EMBED_DIMS", "1536")),
            embed_enabled=_env_bool("EMBED_ENABLED", "true"),
            chunk_max_chars=int(os.getenv("CHUNK_MAX_CHARS", "1200")),
            chunk_overlap_chars=int(os.getenv("CHUNK_OVERLAP_CHARS", "200")),
            indexer_warmup_seconds=int(os.getenv("INDEXER_WARMUP_SECONDS", "5")),
            kafka_retry_sleep_seconds=int(os.getenv("KAFKA_RETRY_SLEEP_SECONDS", "2")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
