# rag/indexer/embedder.py
from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests


def _hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


@dataclass
class FastEmbedder:
    """
    Optional local embeddings using fastembed.
    Only works if `fastembed` is installed in the container.
    """
    model_name: str
    _cache: Dict[str, List[float]] = None
    _model = None

    def __post_init__(self):
        if self._cache is None:
            self._cache = {}

    def _get_model(self):
        if self._model is None:
            try:
                from fastembed import TextEmbedding  # lazy import
            except Exception as e:
                raise RuntimeError(
                    "fastembed is not installed. Either install it in rag/indexer/requirements.txt "
                    "or set EMBED_PROVIDER=openai."
                ) from e
            self._model = TextEmbedding(model_name=self.model_name)
        return self._model

    def _key(self, text: str) -> str:
        return f"{self.model_name}:{_hash(text)}"

    def embed_one(self, text: str) -> Optional[List[float]]:
        if not text or not text.strip():
            return None

        cache_key = self._key(text)
        if cache_key in self._cache:
            return self._cache[cache_key]

        model = self._get_model()
        try:
            vec = next(model.embed([text]))
            out = [float(x) for x in vec]
            self._cache[cache_key] = out
            return out
        except Exception:
            return None


@dataclass
class OpenAIEmbedder:
    """
    OpenAI embeddings via raw HTTP (no openai SDK dependency).
    """
    api_key: str
    model: str = "text-embedding-3-small"
    timeout_s: float = 30.0
    max_retries: int = 3
    _cache: Dict[str, List[float]] = None

    def __post_init__(self):
        if self._cache is None:
            self._cache = {}

    def _key(self, text: str) -> str:
        return f"{self.model}:{_hash(text)}"

    def embed_one(self, text: str) -> Optional[List[float]]:
        if not text or not text.strip():
            return None

        cache_key = self._key(text)
        if cache_key in self._cache:
            return self._cache[cache_key]

        url = "https://api.openai.com/v1/embeddings"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {"model": self.model, "input": text}

        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.post(
                    url,
                    headers=headers,
                    data=json.dumps(payload),
                    timeout=self.timeout_s,
                )
                r.raise_for_status()
                data = r.json()
                vec = data["data"][0]["embedding"]
                out = [float(x) for x in vec]
                self._cache[cache_key] = out
                return out
            except Exception:
                time.sleep(0.6 * attempt)

        return None


def build_embedder():
    provider = os.getenv("EMBED_PROVIDER", "openai").lower().strip()

    if provider == "openai":
        return OpenAIEmbedder(
            api_key=os.getenv("OPENAI_API_KEY", ""),
            model=os.getenv("OPENAI_EMBED_MODEL", os.getenv("EMBED_MODEL", "text-embedding-3-small")),
        )

    # default: fastembed (optional)
    return FastEmbedder(
        model_name=os.getenv("FASTEMBED_MODEL", "BAAI/bge-small-en-v1.5")
    )
