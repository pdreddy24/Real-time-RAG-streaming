# rag/api/embedder.py
import os
import logging
from typing import List, Optional

from fastembed import TextEmbedding

logger = logging.getLogger(__name__)

# Requested model (can be overridden)
REQUESTED_MODEL = os.getenv("FASTEMBED_MODEL", "BAAI/bge-small-en-v1.5")

# Cache path (mount this as a Docker volume for stability)
FASTEMBED_CACHE_PATH = os.getenv("FASTEMBED_CACHE_PATH", "/app/.cache/fastembed")

# One embedder per process
_embedder: Optional[TextEmbedding] = None
_supported_models: Optional[set] = None


def _load_supported_models() -> set:
    """
    FastEmbed returns a list of dicts (usually with key 'model').
    Keep this defensive so you don’t get surprised by minor library changes.
    """
    models = TextEmbedding.list_supported_models()
    out = set()

    for m in models:
        if isinstance(m, dict) and "model" in m:
            out.add(m["model"])
        elif isinstance(m, str):
            out.add(m)

    if not out:
        raise RuntimeError("FastEmbed returned no supported models; check fastembed version.")
    return out


def _pick_supported_model(requested: str) -> str:
    global _supported_models
    if _supported_models is None:
        _supported_models = _load_supported_models()

    if requested in _supported_models:
        return requested

    # Practical fallback: small + widely available in fastembed builds
    fallback = "sentence-transformers/all-MiniLM-L6-v2"
    if fallback in _supported_models:
        logger.warning("FASTEMBED_MODEL '%s' not supported. Falling back to '%s'.", requested, fallback)
        return fallback

    # Last resort: first supported model
    first = next(iter(_supported_models))
    logger.warning("FASTEMBED_MODEL '%s' not supported. Falling back to '%s'.", requested, first)
    return first


def _to_float_list(vec) -> List[float]:
    # Handles numpy arrays, python lists, etc.
    if hasattr(vec, "tolist"):
        vec = vec.tolist()
    return [float(x) for x in vec]


def get_embedder() -> TextEmbedding:
    global _embedder
    if _embedder is not None:
        return _embedder

    # Ensure cache env is set BEFORE initialization
    os.environ["FASTEMBED_CACHE_PATH"] = FASTEMBED_CACHE_PATH

    model_name = _pick_supported_model(REQUESTED_MODEL)

    logger.info("Initializing FastEmbed model='%s' cache='%s'", model_name, FASTEMBED_CACHE_PATH)
    _embedder = TextEmbedding(model_name=model_name)
    return _embedder


def embed_text(text: str) -> List[float]:
    text = (text or "").strip()
    if not text:
        # Hard opinion: fail fast; empty embeddings create garbage rows
        raise ValueError("embed_text() received empty text")

    emb = get_embedder()
    vec = next(iter(emb.embed([text])))
    return _to_float_list(vec)


def embed_texts(texts: List[str]) -> List[List[float]]:
    clean = [(t or "").strip() for t in texts]
    if not any(clean):
        raise ValueError("embed_texts() received only empty texts")

    emb = get_embedder()
    return [_to_float_list(v) for v in emb.embed(clean)]
