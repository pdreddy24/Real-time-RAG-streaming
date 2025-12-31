import time
import uuid
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from rag.metrics import REQUESTS, LATENCY, ERRORS

log = logging.getLogger("rag")

class RequestIdAndMetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        request.state.request_id = request_id

        start = time.time()
        endpoint = request.url.path

        try:
            response: Response = await call_next(request)
            status = str(response.status_code)
            response.headers["x-request-id"] = request_id

            REQUESTS.labels(endpoint=endpoint, status=status).inc()
            LATENCY.labels(endpoint=endpoint).observe(time.time() - start)

            # Structured log line
            log.info("request",
                     extra={
                         "request_id": request_id,
                         "method": request.method,
                         "path": endpoint,
                         "status": response.status_code,
                         "latency_ms": int((time.time() - start) * 1000),
                     })
            return response

        except Exception as e:
            ERRORS.labels(where="middleware").inc()
            REQUESTS.labels(endpoint=endpoint, status="500").inc()
            LATENCY.labels(endpoint=endpoint).observe(time.time() - start)

            log.exception("request_failed",
                          extra={"request_id": request_id, "path": endpoint})
            raise
