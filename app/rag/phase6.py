# rag/phase6.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row

try:
    # Optional: only used if you enable embeddings at query time.
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None


@dataclass
class _QueryResult:
    id: int
    source: str | None
    title: str | None
    url: str | None
    content: str | None
    score: float | None = None
    distance: float | None = None


class Phase6Rag:
    """
    RAG engine. It MUST consume Settings, not inherit from it.
    Keep this class focused: retrieval (+ later, generation).
    """

    def __init__(self, settings):
        self.s = settings

    # -------------------------
    # Public API
    # -------------------------
    def query(self, query: str, top_k: int = 3) -> dict[str, Any]:
        query = (query or "").strip()
        if not query:
            return {"query": query, "top_k": top_k, "results": [], "mode": "empty_query"}

        top_k = int(top_k or 3)
        top_k = max(1, min(top_k, 20))  # guardrails

        with psycopg.connect(self.s.postgres_dsn, row_factory=dict_row) as conn:
            # Prefer vector search if embeddings exist and at least one row has embedding.
            if self._embeddings_usable(conn):
                qvec = self._embed_query(query)
                if qvec:
                    rows = self._vector_search(conn, query, qvec, top_k)
                    return {
                        "query": query,
                        "top_k": top_k,
                        "mode": "vector",
                        "results": [self._to_public(r) for r in rows],
                    }

            # Fallback that works immediately (no embeddings required)
            rows = self._keyword_search(conn, query, top_k)
            return {
                "query": query,
                "top_k": top_k,
                "mode": "keyword",
                "results": [self._to_public(r) for r in rows],
            }

    # -------------------------
    # Retrieval strategies
    # -------------------------
    def _keyword_search(self, conn, q: str, top_k: int) -> list[_QueryResult]:
        # Practical: works with your current state (EMBED_ENABLED=False).
        # Also safe: parameterized (no SQL injection nonsense).
        like = f"%{q}%"
        sql = """
            SELECT
                id, source, title, url, content
            FROM document_chunks
            WHERE
                (title ILIKE %(like)s OR content ILIKE %(like)s)
            ORDER BY id DESC
            LIMIT %(k)s
        """
        rows = conn.execute(sql, {"like": like, "k": top_k}).fetchall()
        return [
            _QueryResult(
                id=int(r["id"]),
                source=r.get("source"),
                title=r.get("title"),
                url=r.get("url"),
                content=r.get("content"),
            )
            for r in rows
        ]

    def _vector_search(self, conn, q: str, qvec: list[float], top_k: int) -> list[_QueryResult]:
        # pgvector literal format: '[0.1,0.2,...]'::vector
        qlit = "[" + ",".join(f"{float(x):.8f}" for x in qvec) + "]"
        sql = """
            SELECT
                id, source, title, url, content,
                (embedding <-> (%(qvec)s)::vector) AS distance
            FROM document_chunks
            WHERE embedding IS NOT NULL
            ORDER BY embedding <-> (%(qvec)s)::vector
            LIMIT %(k)s
        """
        rows = conn.execute(sql, {"qvec": qlit, "k": top_k}).fetchall()
        out: list[_QueryResult] = []
        for r in rows:
            out.append(
                _QueryResult(
                    id=int(r["id"]),
                    source=r.get("source"),
                    title=r.get("title"),
                    url=r.get("url"),
                    content=r.get("content"),
                    distance=float(r["distance"]) if r.get("distance") is not None else None,
                )
            )
        return out

    # -------------------------
    # Embeddings
    # -------------------------
    def _embeddings_usable(self, conn) -> bool:
        # If the schema doesn't even have embedding column, vector search is impossible.
        has_col = conn.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name='document_chunks' AND column_name='embedding'
            LIMIT 1
            """
        ).fetchone()
        if not has_col:
            return False

        # If no rows have embeddings, vector search is pointless; use keyword.
        has_any = conn.execute(
            "SELECT 1 FROM document_chunks WHERE embedding IS NOT NULL LIMIT 1"
        ).fetchone()
        return bool(has_any)

    def _embed_query(self, text: str) -> list[float] | None:
        """
        Query-time embedding. Only used if embeddings exist in DB.
        If OpenAI isn't available or key is missing, silently fall back to keyword mode.
        """
        # You can enforce a setting later; for now keep it resilient.
        if OpenAI is None:
            return None

        api_key = getattr(self.s, "openai_api_key", None) or getattr(self.s, "OPENAI_API_KEY", None)
        # Settings.from_env likely already loaded it into env; still keep defensive.
        if not api_key:
            return None

        model = getattr(self.s, "embed_model", None) or "text-embedding-3-small"

        try:
            client = OpenAI(api_key=api_key)
            resp = client.embeddings.create(model=model, input=text)
            vec = resp.data[0].embedding
            return [float(x) for x in vec]
        except Exception:
            return None

    # -------------------------
    # Response shaping
    # -------------------------
    def _to_public(self, r: _QueryResult) -> dict[str, Any]:
        # Don’t blast huge content back to the API caller by default.
        preview = (r.content or "")[:500]
        return {
            "id": r.id,
            "source": r.source,
            "title": r.title,
            "url": r.url,
            "content_preview": preview,
            "distance": r.distance,
        }
