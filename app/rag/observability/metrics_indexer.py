import os
from prometheus_client import Counter, Histogram, Gauge, start_http_server

MESSAGES = Counter(
    "rag_indexer_messages_total",
    "Kafka messages processed by indexer",
    ["result"],  # ok|skipped|error
)

DB_INSERTS = Counter(
    "rag_indexer_db_inserts_total",
    "DB inserts performed by indexer",
    ["table"],  # document_chunks, etc.
)

EMBEDDINGS = Counter(
    "rag_indexer_embedding_requests_total",
    "Embedding requests made by indexer",
    ["result"],  # ok|error
)

PROCESS_SECONDS = Histogram(
    "rag_indexer_process_seconds",
    "Time to process one message (seconds)",
)

IN_FLIGHT = Gauge(
    "rag_indexer_inflight",
    "Messages currently being processed",
)


def start_metrics_server() -> None:
    port = int(os.getenv("METRICS_PORT", "9100"))
    start_http_server(port)
