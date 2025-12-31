import json
import os
import time
import logging
import requests

from sseclient import SSEClient
from kafka import KafkaProducer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [producer] %(message)s",
)

WIKIMEDIA_SSE_URL = os.getenv("WIKIMEDIA_SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda-0:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wikimedia.cleaned")

CONNECT_TIMEOUT = float(os.getenv("SSE_CONNECT_TIMEOUT", "10"))
READ_TIMEOUT = float(os.getenv("SSE_READ_TIMEOUT", "60"))

RETRY_BASE_SECONDS = float(os.getenv("RETRY_BASE_SECONDS", "2"))
RETRY_MAX_SECONDS = float(os.getenv("RETRY_MAX_SECONDS", "30"))


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        retries=10,
        linger_ms=50,
        max_in_flight_requests_per_connection=5,
        value_serializer=lambda v: v.encode("utf-8"),
    )


def stream_forever():
    logging.info("SSE  : %s", WIKIMEDIA_SSE_URL)
    logging.info("Kafka: %s topic=%s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)

    producer = build_producer()
    backoff = RETRY_BASE_SECONDS
    last_event_id = None

    while True:
        try:
            headers = {"User-Agent": "wikimedia-streaming-rag/1.0"}
            if last_event_id:
                headers["Last-Event-ID"] = last_event_id

            # requests handles keep-alive; SSEClient wraps it
            resp = requests.get(
                WIKIMEDIA_SSE_URL,
                headers=headers,
                stream=True,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
            resp.raise_for_status()

            client = SSEClient(resp)

            logging.info("Connected to Wikimedia SSE stream.")
            backoff = RETRY_BASE_SECONDS

            for event in client.events():
                if not event.data:
                    continue

                # The stream sends JSON strings
                try:
                    obj = json.loads(event.data)
                except json.JSONDecodeError:
                    # If malformed, skip
                    continue

                # Track event id for resume
                if event.id:
                    last_event_id = event.id

                producer.send(KAFKA_TOPIC, json.dumps(obj, ensure_ascii=False))
                # flush occasionally via linger; but force flush every N msgs if you want

        except Exception as e:
            logging.error("Stream error: %s", str(e))
            logging.info("Reconnecting in %.1fs ...", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, RETRY_MAX_SECONDS)


if __name__ == "__main__":
    stream_forever()
