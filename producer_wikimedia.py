import json
import os
import time
import requests
from confluent_kafka import Producer

def iter_sse(url: str, headers: dict):
    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        event_type = None
        data_lines = []
        for raw in r.iter_lines(decode_unicode=True):
            if raw is None:
                continue
            line = raw.strip()
            if line == "":
                if data_lines:
                    yield event_type, "\n".join(data_lines)
                event_type = None
                data_lines = []
                continue
            if line.startswith("event:"):
                event_type = line.split(":", 1)[1].strip()
            elif line.startswith("data:"):
                data_lines.append(line.split(":", 1)[1].lstrip())
            # ignore id:, retry:, etc.

def main():
    sse_url = os.getenv("WIKIMEDIA_SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
    user_agent = os.getenv("USER_AGENT", "wikimedia-streaming-rag/1.0 (contact: you@example.com)")
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("KAFKA_BROKERS", "wikimedia-redpanda:9092")
    topic = os.getenv("KAFKA_TOPIC", "wikimedia.cleaned")

    print(f"[producer] starting | sse={sse_url} brokers={brokers} topic={topic}", flush=True)

    p = Producer({"bootstrap.servers": brokers})

    def delivery_cb(err, msg):
        if err:
            print(f"[producer] delivery failed: {err}", flush=True)

    headers = {"User-Agent": user_agent, "Accept": "text/event-stream"}

    while True:
        try:
            for ev_type, data in iter_sse(sse_url, headers=headers):
                if not data:
                    continue
                # data is JSON string from Wikimedia stream
                p.produce(topic, value=data.encode("utf-8"), callback=delivery_cb)
                p.poll(0)
        except Exception as e:
            print(f"[producer] error: {e} | reconnecting in 3s", flush=True)
            time.sleep(3)

if __name__ == "__main__":
    main()
