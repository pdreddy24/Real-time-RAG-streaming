from __future__ import annotations

from typing import Any, Dict, List
from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1)
    top_k: int = 5
    mode: str = "auto"  # "auto" | "llm" | "retrieve_only"
    use_cache: bool = True
    cache_threshold: float = 0.92
    max_context_chars: int = 12000
    filters: Dict[str, Any] = {}


class QueryResponse(BaseModel):
    query: str
    mode_used: str
    cached: bool
    answer: str
    results: List[Dict[str, Any]] = []
    citations: List[Dict[str, Any]] = []
