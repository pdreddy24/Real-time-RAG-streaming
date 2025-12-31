import os
import time
from prometheus_client import Counter, Gauge, Histogram, start_http_server

_METRICS_STARTED = False

def start_metrics_server() -> None:
    """
    Start Prometheus metrics server on 0.0.0.0 so Docker port mapping works.
    Safe to call multiple times.
    """
    global _METRICS_STARTED
    if _METRICS_STARTED:
        return

    port = int(os.getenv("METRICS_PORT", "9100"))
    start_http_server(port, addr="0.0.0.0")
    _METRICS_STARTED = True


# ---- Metrics ----
rag_indexer_messages_total = Counter(
    "rag_indexer_messages_total",
    "Total Kafka messages received by the indexer"
)

rag_indexer_processed_total = Counter(
    "rag_indexer_processed_total",
    "Total messages successfully processed"
)

rag_indexer_failures_total = Counter(
    "rag_indexer_failures_total",
    "Total processing failures"
)

rag_indexer_processing_latency_seconds = Histogram(
    "rag_indexer_processing_latency_seconds",
    "End-to-end processing latency per message (seconds)",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)

rag_indexer_last_success_unixtime = Gauge(
    "rag_indexer_last_success_unixtime",
    "Unix time of last successfully processed message"
)

rag_indexer_inflight = Gauge(
    "rag_indexer_inflight",
    "Messages currently being processed"
)

def mark_success() -> None:
    rag_indexer_last_success_unixtime.set(time.time())
