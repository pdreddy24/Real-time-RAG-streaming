# scripts/wikimedia_sse_producer.py
import json
import logging
import os
import random
import time
from typing import Dict, Iterable, Optional

import requests
from confluent_kafka import Producer

logger = logging.getLogger("wikimedia.producer")


SSE_URL_DEFAULT = "https://stream.wikimedia.org/v2/stream/recentchange"


def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def build_producer(brokers: str) -> Producer:
    # Linger/batching helps throughput; keep it simple.
    return Producer(
        {
            "bootstrap.servers": brokers,
            "client.id": os.getenv("KAFKA_CLIENT_ID", "wikimedia-sse-producer"),
            # You can tune these later if needed:
            "message.timeout.ms": 60000,
        }
    )


def delivery_report(err, msg):
    if err is not None:
        logger.error("Delivery failed: %s", err)


def sse_events(session: requests.Session, url: str, headers: Dict[str, str]) -> Iterable[dict]:
    """
    Minimal SSE parser:
      - accumulates 'data:' lines until blank line
      - ignores comments / keepalives
      - yields parsed JSON dict
    """
    with session.get(url, headers=headers, stream=True, timeout=(10, 60)) as r:
        r.raise_for_status()

        data_lines = []
        for raw_line in r.iter_lines(decode_unicode=True):
            if raw_line is None:
                continue

            line = raw_line.strip("\r")

            # blank line -> event boundary
            if line == "":
                if data_lines:
                    data_str = "\n".join(data_lines).strip()
                    data_lines = []
                    if data_str:
                        try:
                            yield json.loads(data_str)
                        except Exception:
                            # Wikimedia sends valid JSON; if this happens, log and skip.
                            logger.warning("Skipping non-JSON SSE data (first 200 chars): %r", data_str[:200])
                continue

            # comment / keepalive
            if line.startswith(":"):
                continue

            # we only care about data payloads
            if line.startswith("data:"):
                data_lines.append(line[len("data:") :].lstrip())


def normalize_recentchange(ev: dict) -> Optional[dict]:
    """
    Normalize the Wikimedia recentchange event into your pipeline schema.
    You can enrich later; for RAG indexing, keep it lean.
    """
    try:
        title = ev.get("title")
        # title_url is in some payloads; meta.uri is consistent
        url = ev.get("title_url") or (ev.get("meta") or {}).get("uri")

        if not title or not url:
            return None

        # Put the full event as content (what you're already doing).
        content = json.dumps(ev, ensure_ascii=False)

        return {
            "source": "wikimedia.recentchange",
            "title": title,
            "url": url,
            "content": content,
        }
    except Exception:
        return None


def main():
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    brokers = env("KAFKA_BROKERS")
    topic = env("KAFKA_TOPIC")
    sse_url = os.getenv("SSE_URL", SSE_URL_DEFAULT)

    # IMPORTANT: Wikimedia expects a real UA with contact info.
    user_agent = env("WIKIMEDIA_USER_AGENT", "wikimedia-streaming-rag/1.0 (contact: you@example.com)")

    logger.info("Starting Wikimedia producer")
    logger.info("SSE_URL=%s", sse_url)
    logger.info("KAFKA_BROKERS=%s TOPIC=%s", brokers, topic)

    producer = build_producer(brokers)

    headers = {
        "User-Agent": user_agent,
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
    }

    session = requests.Session()

    # Backoff settings
    backoff = 1.0
    backoff_max = 30.0

    count = 0
    log_every = int(os.getenv("LOG_EVERY", "50"))

    while True:
        try:
            logger.info("Connecting to SSE...")
            for ev in sse_events(session, sse_url, headers=headers):
                msg = normalize_recentchange(ev)
                if not msg:
                    continue

                payload = json.dumps(msg, ensure_ascii=False).encode("utf-8")

                # key helps partitioning + ordering (optional)
                key = (ev.get("meta") or {}).get("id")
                key_bytes = key.encode("utf-8") if isinstance(key, str) else None

                producer.produce(topic=topic, key=key_bytes, value=payload, on_delivery=delivery_report)
                producer.poll(0)  # serve delivery callbacks

                count += 1
                if count % log_every == 0:
                    logger.info("Produced %d messages (last title=%r)", count, msg.get("title"))

            # If we exit the generator loop, SSE stream ended; reconnect.
            logger.warning("SSE stream ended; reconnecting...")

        except requests.HTTPError as e:
            logger.error("HTTP error from SSE: %s", e)
        except requests.RequestException as e:
            logger.error("Stream/network error: %s", e)
        except KeyboardInterrupt:
            logger.info("Stopping producer...")
            break
        except Exception as e:
            logger.exception("Unexpected error: %s", e)

        # flush any buffered messages before reconnect/backoff
        try:
            producer.flush(10)
        except Exception:
            pass

        # exponential backoff with jitter
        sleep_s = min(backoff, backoff_max) + random.random()
        logger.info("Reconnecting in %.1fs", sleep_s)
        time.sleep(sleep_s)
        backoff = min(backoff * 2, backoff_max)

    producer.flush(10)


if __name__ == "__main__":
    main()
