import logging
import json
from fastapi import FastAPI, Request
from fastapi.responses import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from schemas.api import QueryRequest, QueryResponse, RetrievedChunk
from rag.engine import RagEngine
from rag.middleware import RequestIdAndMetricsMiddleware
from rag.health import db_ready
from rag.config import LOG_LEVEL, SERVICE_NAME

# Simple JSON logging (Phase 7)
class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for k in ("request_id", "method", "path", "status", "latency_ms"):
            if hasattr(record, k):
                payload[k] = getattr(record, k)
        return json.dumps(payload, ensure_ascii=False)

handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
root = logging.getLogger()
root.handlers = [handler]
root.setLevel(LOG_LEVEL)

app = FastAPI(title=SERVICE_NAME)
app.add_middleware(RequestIdAndMetricsMiddleware)

engine = RagEngine()

@app.get("/health")
def health():
    return {"status": "ok", "phase": "6/7/8"}

@app.get("/ready")
def ready():
    return {"ready": db_ready()}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest, request: Request):
    out = engine.run(
        query=req.query,
        top_k=req.top_k,
        mode=req.mode,
        use_cache=req.use_cache,
        cache_threshold=req.cache_threshold,
        max_context_chars=req.max_context_chars,
        filters=req.filters,
    )
    request_id = request.state.request_id

    return QueryResponse(
        query=out["query"],
        mode_used=out["mode_used"],
        cached=out["cached"],
        answer=out["answer"],
        results=[RetrievedChunk(**r) for r in out["results"]],
        citations=out["citations"],
        request_id=request_id,
    )
