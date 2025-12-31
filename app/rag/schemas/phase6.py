from pydantic import BaseModel, Field
from typing import Any, Literal, Optional

Mode = Literal["auto", "retrieve_only", "llm"]

class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1)
    top_k: int = Field(5, ge=1, le=20)
    mode: Mode = "auto"
    use_cache: bool = True
    cache_threshold: float = Field(0.92, ge=0.0, le=1.0)
    max_context_chars: int = Field(12000, ge=2000, le=60000)
    filters: dict[str, Any] = Field(default_factory=dict)

class RetrievedChunk(BaseModel):
    id: str | int
    score: float
    source: Optional[str] = None
    title: Optional[str] = None
    url: Optional[str] = None
    snippet: str
    metadata: dict[str, Any] = Field(default_factory=dict)

class QueryResponse(BaseModel):
    query: str
    mode_used: Mode
    cached: bool
    answer: str
    results: list[RetrievedChunk]
    citations: list[dict[str, Any]] = Field(default_factory=list)
