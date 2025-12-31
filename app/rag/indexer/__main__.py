import json
import logging
import os
import time
from typing import Any, Dict, Optional, Tuple

from confluent_kafka import Consumer, KafkaException

from rag.indexer.metrics_server import (
    start_metrics_server,
    rag_indexer_messages_total,
    rag_indexer_processed_total,
    rag_indexer_failures_total,
    rag_indexer_processing_latency_seconds,
    rag_indexer_inflight,
    mark_success,
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
log = logging.getLogger("rag.indexer")


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def build_consumer() -> Tuple[Consumer, str, bool]:
    brokers = _env("KAFKA_BROKERS", "wikimedia-redpanda:9092")
    topic = _env("KAFKA_TOPIC", "wikimedia.cleaned")
    group = _env("KAFKA_GROUP", "rag-indexer-v3")
    auto_offset_reset = _env("AUTO_OFFSET_RESET", "latest")  # latest|earliest

    # IMPORTANT: store this boolean ourselves (Consumer has no .config)
    enable_auto_commit = _env("ENABLE_AUTO_COMMIT", "false").lower() == "true"

    conf = {
        "bootstrap.servers": brokers,
        "group.id": group,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": enable_auto_commit,
        "session.timeout.ms": int(_env("KAFKA_SESSION_TIMEOUT_MS", "30000")),
        "max.poll.interval.ms": int(_env("KAFKA_MAX_POLL_INTERVAL_MS", "300000")),
    }

    log.info(
        "Kafka config: brokers=%s topic=%s group=%s auto.offset.reset=%s enable.auto.commit=%s",
        brokers, topic, group, auto_offset_reset, enable_auto_commit
    )

    c = Consumer(conf)
    c.subscribe([topic])
    return c, topic, enable_auto_commit


def parse_message(value_bytes: Optional[bytes]) -> Dict[str, Any]:
    if not value_bytes:
        return {}
    try:
        return json.loads(value_bytes.decode("utf-8"))
    except Exception:
        return {"raw": value_bytes.decode("utf-8", errors="replace")}


def process_event(event: Dict[str, Any]) -> None:
    """
    Plug your real indexing logic here.
    Keep it stable: parse -> embed -> upsert -> done.
    """
    _ = event.get("content") or event.get("title") or event.get("raw")
    return


def main() -> None:
    # Metrics must come up regardless of Kafka state
    start_metrics_server()
    log.info("Metrics available on :%s/metrics", _env("METRICS_PORT", "9100"))

    consumer, topic, enable_auto_commit = build_consumer()

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                rag_indexer_failures_total.inc()
                log.warning("Kafka message error: %s", msg.error())
                continue

            rag_indexer_messages_total.inc()
            rag_indexer_inflight.inc()
            t0 = time.perf_counter()

            try:
                event = parse_message(msg.value())
                process_event(event)

                rag_indexer_processed_total.inc()
                mark_success()

                # Manual commit only when auto-commit is disabled
                if not enable_auto_commit:
                    consumer.commit(message=msg, asynchronous=False)

            except Exception as e:
                rag_indexer_failures_total.inc()
                log.exception("Processing failed: %s", e)

            finally:
                rag_indexer_processing_latency_seconds.observe(time.perf_counter() - t0)
                rag_indexer_inflight.dec()

    except KeyboardInterrupt:
        log.info("Shutting down (KeyboardInterrupt)")
    except KafkaException as e:
        log.exception("Kafka exception: %s", e)
        raise
    finally:
        consumer.close()
        log.info("Consumer closed")


if __name__ == "__main__":
    main()
