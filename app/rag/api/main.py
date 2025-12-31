import os
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from openai import OpenAI

from pgvector import Vector
from pgvector.psycopg import register_vector

from rag.observability.metrics_fastapi import instrument_fastapi


def dsn_from_env() -> str:
    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        
        dsn = os.getenv("POSTGRES_DSN_DOCKER") or "postgresql://postgres:postgres@wikimedia-postgres:5432/postgres"
    return dsn


EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1)
    top_k: int = Field(3, ge=1, le=50)


class QueryResult(BaseModel):
    id: int
    title: str | None = None
    url: str | None = None
    content: str | None = None
    distance: float


class QueryResponse(BaseModel):
    query: str
    top_k: int
    results: List[QueryResult]
    answer: str


openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

POOL = ConnectionPool(
    conninfo=dsn_from_env(),
    min_size=1,
    max_size=10,
    kwargs={"autocommit": True},
    configure=register_vector,
    open=True,
)

app = FastAPI(title="Wikimedia RAG API", version="1.0.0")
instrument_fastapi(app)


@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


def embed_text(text: str) -> List[float]:
    try:
        resp = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=text)
        return resp.data[0].embedding
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Embedding failed: {e!s}")


def query_chunks(embedding: List[float], top_k: int) -> List[Dict[str, Any]]:
    qv = Vector(embedding)

    sql = """
    SELECT
      id,
      title,
      url,
      content,
      (embedding <=> %s) AS distance
    FROM document_chunks
    WHERE embedding IS NOT NULL
    ORDER BY embedding <=> %s
    LIMIT %s;
    """

    with POOL.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (qv, qv, top_k))
            return cur.fetchall()


@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest) -> QueryResponse:
    embedding = embed_text(req.query)

    rows = query_chunks(embedding, req.top_k)
    results = [QueryResult(**r) for r in rows]

    answer = (
        "This system retrieves *recent edit metadata* (title/user/comment/etc). "
        "It does not store full article text, so detailed Q&A is often not possible from this context alone."
    )

    return QueryResponse(query=req.query, top_k=req.top_k, results=results, answer=answer)
