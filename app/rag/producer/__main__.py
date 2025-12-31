"""
Wikimedia SSE -> Kafka (cleaned events for RAG)

Goals:
- Produce a SMALL, HIGH-SIGNAL payload for downstream embedding + retrieval.
- Filter bot/system noise by default.
- Convert parsed HTML comments into plain text.
- Keep only operationally useful text in `content` (truncate it).
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass
from html import unescape as html_unescape
from typing import Any, Dict, Iterator, Optional

import requests
from kafka import KafkaProducer  # kafka-python


SSE_URL_DEFAULT = "https://stream.wikimedia.org/v2/stream/recentchange"

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class Config:
    # Inputs/outputs
    sse_url: str
    kafka_brokers: str
    kafka_topic: str

    # Filters
    allowed_namespaces: tuple[int, ...]   # default: (0,) => main/article ns only
    filter_bots: bool                    # default: True
    skip_users: tuple[str, ...]          # default: common noisy users/bots
    allow_wikis: tuple[str, ...]         # optional: limit to enwiki, etc. Empty => allow all

    # Payload controls
    max_content_chars: int               # default: 700

    # Ops controls
    reconnect_sleep_sec: float           # default: 2.0
    max_events_per_min: int              # default: 0 => unlimited
    kafka_flush_every: int               # default: 200
    user_agent: str                      # default: safe UA for Wikimedia


def _parse_int_list(env_val: str, default: tuple[int, ...]) -> tuple[int, ...]:
    if not (env_val or "").strip():
        return default
    out: list[int] = []
    for part in env_val.split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return tuple(out)


def _parse_str_list(env_val: str, default: tuple[str, ...]) -> tuple[str, ...]:
    if not (env_val or "").strip():
        return default
    return tuple(s.strip() for s in env_val.split(",") if s.strip())


def _parse_bool(env_val: str, default: bool) -> bool:
    if env_val is None:
        return default
    return env_val.strip().lower() in {"1", "true", "yes", "y"}


def load_config() -> Config:
    return Config(
        sse_url=os.getenv("WIKIMEDIA_SSE_URL", SSE_URL_DEFAULT),
        kafka_brokers=os.getenv("KAFKA_BROKERS", "localhost:9092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", os.getenv("PRODUCER_TOPIC", "wikimedia.cleaned")),

        allowed_namespaces=_parse_int_list(os.getenv("ALLOWED_NAMESPACES", "0"), default=(0,)),
        filter_bots=_parse_bool(os.getenv("FILTER_BOTS", "true"), default=True),
        skip_users=_parse_str_list(
            os.getenv(
                "SKIP_USERS",
                "MediaWiki message delivery,InternetArchiveBot,JJMC89 bot III"
            ),
            default=("MediaWiki message delivery", "InternetArchiveBot", "JJMC89 bot III"),
        ),
        allow_wikis=_parse_str_list(os.getenv("ALLOW_WIKIS", ""), default=()),

        max_content_chars=int(os.getenv("MAX_CONTENT_CHARS", os.getenv("MAX_CONTENT_CHARS", "700"))),

        reconnect_sleep_sec=float(os.getenv("RECONNECT_SLEEP_SEC", "2.0")),
        max_events_per_min=int(os.getenv("MAX_EVENTS_PER_MIN", "0")),
        kafka_flush_every=int(os.getenv("KAFKA_FLUSH_EVERY", "200")),

        user_agent=os.getenv(
            "WIKIMEDIA_USER_AGENT",
            "wikimedia-streaming-rag/1.0 (+https://example.com; contact:you@example.com)",
        ),
    )


# -----------------------------
# Text cleaning
# -----------------------------

def strip_html(html: str) -> str:
    no_tags = _TAG_RE.sub(" ", html)
    return html_unescape(no_tags)


def normalize_text(s: str) -> str:
    s = s.replace("\u200e", " ").replace("\u200f", " ")
    s = _WS_RE.sub(" ", s).strip()
    return s


def clean_comment(rc: Dict[str, Any]) -> str:
    parsed = rc.get("parsedcomment") or ""
    comment = rc.get("comment") or ""
    base = parsed if parsed else comment
    if not base:
        return ""
    base = strip_html(base) if parsed else base
    return normalize_text(base)


def truncate(s: str, max_chars: int) -> str:
    if max_chars <= 0 or len(s) <= max_chars:
        return s
    return s[: max_chars - 1].rstrip() + "…"


# -----------------------------
# Event shaping
# -----------------------------

def should_keep(rc: Dict[str, Any], cfg: Config) -> bool:
    # wiki filter (optional)
    wiki = (rc.get("wiki") or "").strip()
    if cfg.allow_wikis and wiki not in cfg.allow_wikis:
        return False

    # namespace filter
    ns = rc.get("namespace")
    if isinstance(ns, int) and ns not in cfg.allowed_namespaces:
        return False

    # bot filter
    user = (rc.get("user") or "").strip()
    bot = bool(rc.get("bot", False))
    if cfg.filter_bots and bot:
        return False

    # noisy users filter (even if filter_bots=false)
    if user and user in cfg.skip_users:
        return False

    # keep edits/new by default; drop other low-signal event types
    # (categorize/log can be very spammy)
    if rc.get("type") not in {"edit", "new"}:
        return False

    return True


def build_clean_event(rc: Dict[str, Any], cfg: Config) -> Optional[Dict[str, Any]]:
    if not should_keep(rc, cfg):
        return None

    meta = rc.get("meta") or {}
    rev = rc.get("revision") or {}
    length = rc.get("length") or {}

    title = (rc.get("title") or "").strip()
    wiki = (rc.get("wiki") or "").strip()
    domain = (meta.get("domain") or rc.get("server_name") or "").strip()

    title_url = (rc.get("title_url") or meta.get("uri") or "").strip()
    diff_url = (rc.get("notify_url") or "").strip()

    comment = clean_comment(rc)

    old_rev = rev.get("old")
    new_rev = rev.get("new")
    old_len = length.get("old")
    new_len = length.get("new")

    content_lines = [
        f"Title: {title}",
        f"Type: {rc.get('type')}",
        f"Wiki: {wiki}" + (f" ({domain})" if domain else ""),
    ]
    if comment:
        content_lines.append(f"Comment: {comment}")
    if diff_url:
        content_lines.append(f"Diff: {diff_url}")
    if old_rev or new_rev:
        content_lines.append(f"Revision: {old_rev} -> {new_rev}")
    if old_len is not None and new_len is not None:
        content_lines.append(f"Bytes: {old_len} -> {new_len}")

    content = truncate("\n".join(content_lines), cfg.max_content_chars)

    event_id = str(meta.get("id") or rc.get("id") or f"{wiki}:{title}:{rc.get('timestamp')}")

    return {
        "source": "wikimedia",
        "event_id": event_id,

        "title": title,
        "url": title_url,
        "diff_url": diff_url,

        "wiki": wiki,
        "domain": domain,

        "type": rc.get("type"),
        "namespace": rc.get("namespace"),

        "timestamp": rc.get("timestamp"),
        "dt": meta.get("dt"),

        "user": rc.get("user"),
        "bot": bool(rc.get("bot", False)),
        "minor": bool(rc.get("minor", False)),

        "comment": comment,

        # Embed THIS ONLY
        "content": content,

        # Minimal metadata (no raw payload)
        "meta": {
            "stream": meta.get("stream"),
            "topic": meta.get("topic"),
            "partition": meta.get("partition"),
            "offset": meta.get("offset"),
            "request_id": meta.get("request_id"),
        },

        "revision": {"old": old_rev, "new": new_rev},
        "length": {"old": old_len, "new": new_len},
    }


# -----------------------------
# SSE ingestion
# -----------------------------

def iter_recentchange_events(cfg: Config) -> Iterator[Dict[str, Any]]:
    headers = {"User-Agent": cfg.user_agent}

    with requests.get(cfg.sse_url, stream=True, timeout=60, headers=headers) as resp:
        resp.raise_for_status()

        for raw_line in resp.iter_lines(decode_unicode=True):
            if not raw_line:
                continue
            if not raw_line.startswith("data:"):
                continue

            data = raw_line[5:].strip()
            if not data or data == "[DONE]":
                continue

            try:
                yield json.loads(data)
            except json.JSONDecodeError:
                continue


# -----------------------------
# Kafka
# -----------------------------

def make_producer(cfg: Config) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[b.strip() for b in cfg.kafka_brokers.split(",") if b.strip()],
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=10,
        linger_ms=50,
    )


# -----------------------------
# Main
# -----------------------------

def _rate_limit_sleep(max_per_min: int, sent_in_window: int, window_start: float) -> tuple[int, float]:
    if max_per_min <= 0:
        return sent_in_window, window_start

    now = time.time()
    if now - window_start >= 60:
        return 0, now

    if sent_in_window >= max_per_min:
        sleep_for = 60 - (now - window_start)
        if sleep_for > 0:
            time.sleep(sleep_for)
        return 0, time.time()

    return sent_in_window, window_start


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | producer | %(message)s",
    )
    log = logging.getLogger("producer")

    cfg = load_config()
    log.info(
        "starting producer: url=%s topic=%s brokers=%s ns=%s filter_bots=%s allow_wikis=%s max_events_per_min=%s",
        cfg.sse_url,
        cfg.kafka_topic,
        cfg.kafka_brokers,
        cfg.allowed_namespaces,
        cfg.filter_bots,
        cfg.allow_wikis,
        cfg.max_events_per_min,
    )

    producer = make_producer(cfg)

    sent = 0
    win_start = time.time()
    win_sent = 0

    while True:
        try:
            for rc in iter_recentchange_events(cfg):
                cleaned = build_clean_event(rc, cfg)
                if cleaned is None:
                    continue

                win_sent, win_start = _rate_limit_sleep(cfg.max_events_per_min, win_sent, win_start)

                producer.send(cfg.kafka_topic, key=cleaned["event_id"], value=cleaned)
                sent += 1
                win_sent += 1

                if cfg.kafka_flush_every > 0 and sent % cfg.kafka_flush_every == 0:
                    producer.flush()
                    log.info("sent=%d latest_title=%s", sent, cleaned.get("title"))

        except Exception as e:
            log.warning("stream error: %s | reconnecting in %.1fs", e, cfg.reconnect_sleep_sec)
            time.sleep(cfg.reconnect_sleep_sec)


if __name__ == "__main__":
    main()
