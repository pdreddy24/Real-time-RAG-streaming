# rag/metrics.py
from __future__ import annotations

from rag.config import ENABLE_METRICS


class _NoopMetric:
    def inc(self, *_args, **_kwargs) -> None:
        return

    def observe(self, *_args, **_kwargs) -> None:
        return

    def labels(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
        return self


Counter = None
Histogram = None

if ENABLE_METRICS:
    try:
        from prometheus_client import Counter as _Counter  # type: ignore
        from prometheus_client import Histogram as _Histogram  # type: ignore

        Counter = _Counter
        Histogram = _Histogram
    except ModuleNotFoundError:
        # Metrics are optional. Don’t take the whole service down.
        Counter = None
        Histogram = None


def _counter(*args, **kwargs):  # type: ignore[no-untyped-def]
    if Counter is None:
        return _NoopMetric()
    return Counter(*args, **kwargs)


def _hist(*args, **kwargs):  # type: ignore[no-untyped-def]
    if Histogram is None:
        return _NoopMetric()
    return Histogram(*args, **kwargs)


CACHE_HITS = _counter("cache_hits_total", "RAG cache hits")
LLM_CALLS = _counter("llm_calls_total", "LLM calls")
RETRIEVE_ONLY = _counter("retrieve_only_total", "Retrieve-only responses")
ERRORS = _counter("errors_total", "Errors")
LATENCY = _hist("request_latency_seconds", "API latency")
