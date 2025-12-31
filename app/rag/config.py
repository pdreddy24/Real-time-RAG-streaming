# rag/config.py
from __future__ import annotations

import os
from dataclasses import dataclass

# Optional local .env support (docker-compose env works without this)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


def _getenv(key: str, default: str | None = None) -> str | None:
    v = os.getenv(key)
    if v is None or v == "":
        return default
    return v


def must_env(key: str) -> str:
    v = _getenv(key)
    if not v:
        raise RuntimeError(f"Missing env var {key}")
    return v


def as_int(key: str, default: int) -> int:
    v = _getenv(key, str(default))
    try:
        return int(v)  # type: ignore[arg-type]
    except Exception:
        raise RuntimeError(f"Env var {key} must be an int, got: {v}")


def as_float(key: str, default: float) -> float:
    v = _getenv(key, str(default))
    try:
        return float(v)  # type: ignore[arg-type]
    except Exception:
        raise RuntimeError(f"Env var {key} must be a float, got: {v}")


def as_bool(key: str, default: bool = False) -> bool:
    v = _getenv(key, "true" if default else "false")
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True)
class Settings:
    # Required
    POSTGRES_DSN: str
    OPENAI_API_KEY: str

    # Models
    OPENAI_CHAT_MODEL: str
    OPENAI_EMBED_MODEL: str
    EMBED_DIMS: int

    # RAG
    CHUNKS_TABLE: str

    # Phase 6 tuning
    CACHE_THRESHOLD_DEFAULT: float
    AUTO_SKIP_LLM_SCORE: float

    # Phase 7 tuning
    SERVICE_NAME: str
    LOG_LEVEL: str

    # Operational toggles
    ENABLE_METRICS: bool
    ENABLE_CACHE: bool


def load_settings(strict: bool = True) -> Settings:
    """
    strict=True  -> raise if required env vars missing (recommended for prod)
    strict=False -> allow import/smoke tests without real env
    """
    if strict:
        postgres_dsn = must_env("POSTGRES_DSN")
        openai_key = must_env("OPENAI_API_KEY")
    else:
        postgres_dsn = _getenv("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/db")  # dummy
        openai_key = _getenv("OPENAI_API_KEY", "dummy")

    return Settings(
        POSTGRES_DSN=postgres_dsn,
        OPENAI_API_KEY=openai_key,

        OPENAI_CHAT_MODEL=_getenv("OPENAI_CHAT_MODEL", "gpt-4.1-mini"),
        OPENAI_EMBED_MODEL=_getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small"),
        EMBED_DIMS=as_int("EMBED_DIMS", 1536),

        CHUNKS_TABLE=_getenv("CHUNKS_TABLE", "document_chunks"),

        CACHE_THRESHOLD_DEFAULT=as_float("CACHE_THRESHOLD", 0.92),
        AUTO_SKIP_LLM_SCORE=as_float("AUTO_SKIP_LLM_SCORE", 0.33),

        SERVICE_NAME=_getenv("SERVICE_NAME", "wikimedia-rag-api"),
        LOG_LEVEL=_getenv("LOG_LEVEL", "INFO"),

        ENABLE_METRICS=as_bool("ENABLE_METRICS", True),
        ENABLE_CACHE=as_bool("ENABLE_CACHE", True),
    )


# Default settings for runtime (strict in prod)
settings = load_settings(strict=True)

# Backwards-compatible module-level exports
POSTGRES_DSN = settings.POSTGRES_DSN
OPENAI_API_KEY = settings.OPENAI_API_KEY

OPENAI_CHAT_MODEL = settings.OPENAI_CHAT_MODEL
OPENAI_EMBED_MODEL = settings.OPENAI_EMBED_MODEL
EMBED_DIMS = settings.EMBED_DIMS

CHUNKS_TABLE = settings.CHUNKS_TABLE

CACHE_THRESHOLD_DEFAULT = settings.CACHE_THRESHOLD_DEFAULT
AUTO_SKIP_LLM_SCORE = settings.AUTO_SKIP_LLM_SCORE

SERVICE_NAME = settings.SERVICE_NAME
LOG_LEVEL = settings.LOG_LEVEL

ENABLE_METRICS = settings.ENABLE_METRICS
ENABLE_CACHE = settings.ENABLE_CACHE
