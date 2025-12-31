import os
import time
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Port must be reachable from host + Prometheus
METRICS_PORT = int(os.getenv("METRICS_PORT", "9100"))

# Start HTTP server once, early in process startup
_started = False
def start_metrics_server() -> None:
    global _started
    if _started:
        return
    start_http_server(METRICS_PORT, addr="0.0.0.0")
    _started = True

# --- Core metrics (keep naming consistent + grep-friendly) ---
rag_indexer_messages_total = Counter(
    "rag_indexer_messages_total",
    "Total Kafka messages received by the indexer"
)

rag_indexer_processed_total = Counter(
    "rag_indexer_processed_total",
    "Total messages successfully processed (embedded+stored)"
)

rag_indexer_failures_total = Counter(
    "rag_indexer_failures_total",
    "Total processing failures"
)

rag_indexer_processing_latency_seconds = Histogram(
    "rag_indexer_processing_latency_seconds",
    "End-to-end processing latency per message (seconds)",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30)
)

rag_indexer_last_success_unixtime = Gauge(
    "rag_indexer_last_success_unixtime",
    "Unix time of last successfully processed message"
)

rag_indexer_inflight = Gauge(
    "rag_indexer_inflight",
    "Messages currently being processed (best-effort)"
)

def mark_success() -> None:
    rag_indexer_last_success_unixtime.set(time.time())
