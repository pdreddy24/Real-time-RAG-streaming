from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastembed import TextEmbedding

from rag.api.db import get_conn
from rag.settings import settings

_model: Optional[TextEmbedding] = None


def _get_model() -> TextEmbedding:
    global _model
    if _model is None:
        _model = TextEmbedding(model_name=settings.fastembed_model)
    return _model


def embed_query(q: str) -> list[float]:
    m = _get_model()
    v = next(m.embed([q]))
    return [float(x) for x in v]


def _vector_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{x:.8f}" for x in vec) + "]"


def retrieve(
    question: str,
    top_k: int = 5,
    wiki: str | None = None,
    minutes: int | None = None,
) -> list[dict]:
    qvec = embed_query(question)
    dim = len(qvec)
    qlit = _vector_literal(qvec)

    where = ["embedding IS NOT NULL"]
    filters: list[object] = []

    if wiki:
        where.append("(metadata->>'wiki') = %s")
        filters.append(wiki)

    if minutes is not None and minutes > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)

        
        event_time_expr = """
        CASE
          WHEN (metadata->>'event_time') IS NULL OR (metadata->>'event_time') = '' THEN NULL
          WHEN (metadata->>'event_time') ~ '^[0-9]+$' THEN to_timestamp((metadata->>'event_time')::double precision)
          ELSE (metadata->>'event_time')::timestamptz
        END
        """
        where.append(f"({event_time_expr}) >= %s")
        filters.append(cutoff.isoformat())

    where_sql = " AND ".join(where)

    sql = f"""
    SELECT id, doc_id, source, chunk_index, text, metadata,
           (embedding <=> %s::vector({dim})) AS cosine_distance
    FROM rag_chunks
    WHERE {where_sql}
    ORDER BY embedding <=> %s::vector({dim})
    LIMIT %s;
    """

    params: list[object] = [qlit, *filters, qlit, top_k]

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    results: list[dict] = []
    for (rid, doc_id, source, chunk_index, text, metadata, dist) in rows:
        results.append(
            {
                "id": rid,
                "doc_id": doc_id,
                "source": source,
                "chunk_index": chunk_index,
                "text": text,
                "metadata": metadata,
                "cosine_distance": float(dist) if dist is not None else None,
            }
        )
    return results


def generate_answer(question: str, contexts: list[dict]) -> str:
    if not contexts:
        return (
            "No chunks match your filters yet. "
            "Widen the time window (minutes) or remove the wiki filter."
        )

    joined = "\n\n".join([c.get("text", "") for c in contexts[:5]])
    return f"Question: {question}\n\nTop contexts:\n{joined}"
