import time
from fastapi import Response
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# High-signal metrics (don’t overthink labels; cardinality kills Prometheus)
REQUESTS = Counter(
    "rag_api_requests_total",
    "Total HTTP requests handled by rag-api",
    ["method", "path", "status"],
)

LATENCY = Histogram(
    "rag_api_request_latency_seconds",
    "HTTP request latency in seconds",
    ["path"],
)

INPROGRESS = Gauge(
    "rag_api_inprogress_requests",
    "In-progress HTTP requests",
    ["path"],
)


def instrument_fastapi(app) -> None:
    @app.middleware("http")
    async def _prom_middleware(request, call_next):
        path = request.url.path

        # don’t pollute metrics with internal endpoints
        if path in ("/metrics", "/health", "/docs", "/openapi.json", "/favicon.ico"):
            return await call_next(request)

        start = time.perf_counter()
        status = "500"

        INPROGRESS.labels(path=path).inc()
        try:
            response = await call_next(request)
            status = str(response.status_code)
            return response
        finally:
            INPROGRESS.labels(path=path).dec()
            REQUESTS.labels(method=request.method, path=path, status=status).inc()
            LATENCY.labels(path=path).observe(time.perf_counter() - start)

    @app.get("/metrics", include_in_schema=False)
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
