import json
import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from starlette.responses import Response

import psycopg
from psycopg_pool import ConnectionPool

logger = logging.getLogger("rag.api")
app = FastAPI()

def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"}

def json_utf8(data: Dict[str, Any], status_code: int = 200) -> Response:
    payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
    return Response(content=payload, status_code=status_code, media_type="application/json; charset=utf-8")

def _to_vector_literal(vec: List[float]) -> str:
    return "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"

# Settings
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@wikimedia-postgres:5432/postgres")
CHUNKS_TABLE = os.getenv("CHUNKS_TABLE", "document_chunks")

EMBED_ENABLED = _env_bool("EMBED_ENABLED", "false")
EMBED_DIMS = int(os.getenv("EMBED_DIMS", "1536"))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_EMBED_MODEL = os.getenv("OPENAI_EMBED_MODEL", os.getenv("EMBED_MODEL", "text-embedding-3-small"))

ANSWER_ENABLED = _env_bool("ANSWER_ENABLED", "false")
OPENAI_CHAT_MODEL = os.getenv("OPENAI_CHAT_MODEL", "gpt-4o-mini")

_pool: Optional[ConnectionPool] = None
_openai_client = None

@app.on_event("startup")
def _startup() -> None:
    global _pool
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger.info("API booting...")
    logger.info("POSTGRES_DSN=%s", POSTGRES_DSN)
    logger.info("CHUNKS_TABLE=%s", CHUNKS_TABLE)
    logger.info("EMBED_ENABLED=%s EMBED_DIMS=%s MODEL=%s", EMBED_ENABLED, EMBED_DIMS, OPENAI_EMBED_MODEL)
    logger.info("ANSWER_ENABLED=%s CHAT_MODEL=%s", ANSWER_ENABLED, OPENAI_CHAT_MODEL)

    _pool = ConnectionPool(conninfo=POSTGRES_DSN, max_size=10)

@app.on_event("shutdown")
def _shutdown() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None

def _get_openai_client():
    global _openai_client
    if _openai_client is not None:
        return _openai_client
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is empty but EMBED_ENABLED/ANSWER_ENABLED requires it")
    from openai import OpenAI
    _openai_client = OpenAI(api_key=OPENAI_API_KEY)
    return _openai_client

def _embed_query(text: str) -> List[float]:
    client = _get_openai_client()
    resp = client.embeddings.create(model=OPENAI_EMBED_MODEL, input=[text])
    vec = resp.data[0].embedding
    if len(vec) != EMBED_DIMS:
        raise RuntimeError(f"Embedding dims mismatch: got {len(vec)} expected {EMBED_DIMS}")
    return [float(x) for x in vec]

def _vector_search(q: str, top_k: int) -> List[Dict[str, Any]]:
    assert _pool is not None
    vec = _embed_query(q)
    vec_lit = _to_vector_literal(vec)

    sql_query = f"""
        SELECT id, source, title, url,
               left(content, 240) AS content_preview,
               (embedding <=> %s::vector) AS distance
        FROM {CHUNKS_TABLE}
        WHERE embedding IS NOT NULL
        ORDER BY embedding <=> %s::vector
        LIMIT %s;
    """
    with _pool.connection() as conn:
        rows = conn.execute(sql_query, (vec_lit, vec_lit, top_k)).fetchall()

    return [
        {
            "id": r[0],
            "source": r[1],
            "title": r[2],
            "url": r[3],
            "content_preview": r[4],
            "distance": float(r[5]),
        }
        for r in rows
    ]

def _text_search(q: str, top_k: int) -> List[Dict[str, Any]]:
    assert _pool is not None
    sql_query = f"""
        SELECT id, source, title, url,
               left(content, 240) AS content_preview
        FROM {CHUNKS_TABLE}
        WHERE content ILIKE %s OR title ILIKE %s
        ORDER BY id DESC
        LIMIT %s;
    """
    pat = f"%{q}%"
    with _pool.connection() as conn:
        rows = conn.execute(sql_query, (pat, pat, top_k)).fetchall()

    return [
        {
            "id": r[0],
            "source": r[1],
            "title": r[2],
            "url": r[3],
            "content_preview": r[4],
            "distance": None,
        }
        for r in rows
    ]

def _format_sources(rows: List[Dict[str, Any]]) -> str:
    blocks = []
    for i, r in enumerate(rows, start=1):
        blocks.append(
            f"[{i}] title: {r.get('title') or '(no title)'}\n"
            f"url: {r.get('url') or ''}\n"
            f"content: {r.get('content_preview') or ''}"
        )
    return "\n\n".join(blocks)

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/query")
async def query(request: Request):
    body_bytes = await request.body()
    try:
        body = json.loads(body_bytes.decode("utf-8"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON or invalid UTF-8 body: {e}")

    q = (body.get("query") or "").strip()
    top_k = int(body.get("top_k", 5))

    if not q:
        return json_utf8({"query": q, "top_k": top_k, "mode": "none", "results": []})

    try:
        if EMBED_ENABLED:
            results = _vector_search(q, top_k=top_k)
            mode = "vector"
        else:
            results = _text_search(q, top_k=top_k)
            mode = "text"
    except psycopg.Error as e:
        logger.exception("DB error")
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    except Exception as e:
        logger.exception("Query error")
        raise HTTPException(status_code=500, detail=f"Query error: {e}")

    return json_utf8({"query": q, "top_k": top_k, "mode": mode, "results": results})

@app.post("/ask")
async def ask(request: Request):
    body_bytes = await request.body()
    try:
        body = json.loads(body_bytes.decode("utf-8"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON or invalid UTF-8 body: {e}")

    q = (body.get("query") or "").strip()
    top_k = int(body.get("top_k", 3))

    max_distance_strict = float(body.get("max_distance_strict", 0.25))
    max_distance_relaxed = float(body.get("max_distance_relaxed", 0.35))

    if not q:
        return json_utf8({"query": q, "top_k": top_k, "mode": "none", "answer": "", "citations": []})

    if not ANSWER_ENABLED:
        return json_utf8({
            "query": q,
            "top_k": top_k,
            "mode": "answer_disabled",
            "answer": "Answer generation is disabled (set ANSWER_ENABLED=true).",
            "citations": [],
        })

    thresholds = {
        "max_distance_strict": max_distance_strict,
        "max_distance_relaxed": max_distance_relaxed,
    }

    # 1) Retrieve with quality gates
    try:
        if EMBED_ENABLED:
            rows = _vector_search(q, top_k=top_k)

            strict = [r for r in rows if r.get("distance") is not None and float(r["distance"]) <= max_distance_strict]
            if strict:
                rows = strict
                mode = "vector_strict"
            else:
                relaxed = [r for r in rows if r.get("distance") is not None and float(r["distance"]) <= max_distance_relaxed]
                if relaxed:
                    rows = relaxed
                    mode = "vector_relaxed"
                else:
                    # optional fallback to keyword search to avoid dead-ends
                    fallback = _text_search(q, top_k=top_k)
                    if fallback:
                        rows = fallback
                        mode = "text_fallback"
                    else:
                        rows = []
                        mode = "no_hit"
        else:
            rows = _text_search(q, top_k=top_k)
            mode = "text" if rows else "no_hit"

    except Exception as e:
        logger.exception("Ask retrieval error")
        raise HTTPException(status_code=500, detail=f"Retrieval error: {e}")

    if not rows:
        return json_utf8({
            "query": q,
            "top_k": top_k,
            "mode": mode,
            "thresholds": thresholds,
            "answer": "I couldn’t find a strong enough match in the index for this question.",
            "citations": [],
        })

    # 2) Generate answer grounded in sources
    try:
        client = _get_openai_client()
        system = (
            "You are a RAG assistant. Use ONLY the provided sources.\n"
            "If sources are insufficient, say so.\n"
            "Cite claims with [1], [2], etc. Do not invent citations."
        )
        sources = _format_sources(rows)
        user = f"Question: {q}\n\nSources:\n{sources}"

        resp = client.chat.completions.create(
            model=OPENAI_CHAT_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.2,
        )
        answer = resp.choices[0].message.content or ""
    except Exception as e:
        logger.exception("Ask generation error")
        raise HTTPException(status_code=500, detail=f"LLM call failed: {e}")

    return json_utf8({
        "query": q,
        "top_k": top_k,
        "mode": mode,
        "thresholds": thresholds,
        "answer": answer,
        "citations": rows,
    })
