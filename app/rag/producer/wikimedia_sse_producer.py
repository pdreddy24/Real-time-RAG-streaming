# rag/producer/wikimedia_sse_producer.py
import json
import logging
import os
import time
from typing import Any, Dict, Optional

import requests
from kafka import KafkaProducer

logger = logging.getLogger("rag.producer")


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return int(v)


def _env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _now_minute_bucket() -> int:
    return int(time.time() // 60)


def _build_content(rc: Dict[str, Any]) -> str:
    # Keep it cheap: we’re indexing the event payload, not fetching full article text.
    # This is enough for “what changed / by who / where / what type” style questions.
    parts = []
    parts.append(f"type={rc.get('type')}")
    parts.append(f"wiki={rc.get('wiki')}")
    parts.append(f"title={rc.get('title')}")
    parts.append(f"user={rc.get('user')}")
    parts.append(f"bot={rc.get('bot')}")
    if rc.get("comment"):
        parts.append(f"comment={rc.get('comment')}")
    if rc.get("server_name"):
        parts.append(f"server={rc.get('server_name')}")
    if rc.get("timestamp"):
        parts.append(f"timestamp={rc.get('timestamp')}")
    return " | ".join(str(p) for p in parts if p is not None)


def _build_url(rc: Dict[str, Any]) -> str:
    # Best-effort URL. recentchange payload usually has server_url + title.
    server_url = rc.get("server_url") or ""
    title = rc.get("title") or ""
    if server_url and title:
        return f"{server_url}/wiki/{title.replace(' ', '_')}"
    return rc.get("url") or ""


def _send(producer: KafkaProducer, topic: str, payload: Dict[str, Any]) -> None:
    producer.send(topic, json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    producer.flush(5)


def _consume_sse(url: str, headers: Dict[str, str], timeout_s: int = 60):
    # Stream SSE, parse events manually.
    with requests.get(url, headers=headers, stream=True, timeout=(10, timeout_s)) as r:
        r.raise_for_status()

        event_data_lines = []
        for raw_line in r.iter_lines(decode_unicode=True):
            if raw_line is None:
                continue
            line = raw_line.strip()

            # Blank line = end of SSE event
            if line == "":
                if event_data_lines:
                    data = "\n".join(event_data_lines)
                    event_data_lines = []
                    yield data
                continue

            # We only care about "data:" lines
            if line.startswith("data:"):
                event_data_lines.append(line[len("data:") :].lstrip())


def main() -> None:
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    url = _env_str("WIKIMEDIA_SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
    topic = _env_str("PRODUCER_TOPIC", _env_str("KAFKA_TOPIC", "wikimedia.cleaned"))
    brokers = _env_str("KAFKA_BROKERS", "wikimedia-redpanda:9092")
    wiki_filter = _env_str("WIKI_FILTER", "enwiki")
    max_events_per_min = _env_int("MAX_EVENTS_PER_MIN", 60)

    user_agent = _env_str("WIKIMEDIA_USER_AGENT", "")
    if not user_agent:
        # Don’t limp along with 403 spam; fail fast so you fix config.
        raise RuntimeError("WIKIMEDIA_USER_AGENT is required (set it in .env).")

    headers = {
        "User-Agent": user_agent,
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
    }

    logger.info(
        "starting producer: url=%s wiki=%s max_events_per_min=%s topic=%s brokers=%s",
        url, wiki_filter, max_events_per_min, topic, brokers
    )

    producer = KafkaProducer(bootstrap_servers=brokers)

    minute_bucket = _now_minute_bucket()
    sent_in_bucket = 0

    while True:
        try:
            for data in _consume_sse(url, headers=headers, timeout_s=60):
                # rate-limit by minute bucket
                now_bucket = _now_minute_bucket()
                if now_bucket != minute_bucket:
                    minute_bucket = now_bucket
                    sent_in_bucket = 0

                if sent_in_bucket >= max_events_per_min:
                    # hard throttle until minute flips
                    time.sleep(0.5)
                    continue

                try:
                    rc = json.loads(data)
                except Exception:
                    continue

                if wiki_filter and rc.get("wiki") != wiki_filter:
                    continue

                payload = {
                    "source": "wikimedia",
                    "title": rc.get("title") or "",
                    "url": _build_url(rc),
                    "content": _build_content(rc),
                    "raw": rc,  # keep raw for debugging / future enrichment
                }

                _send(producer, topic, payload)
                sent_in_bucket += 1

        except Exception as e:
            logger.warning("producer loop error: %s (retrying in 3s)", e)
            time.sleep(3)


if __name__ == "__main__":
    main()
