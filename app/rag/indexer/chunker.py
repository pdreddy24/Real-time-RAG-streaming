from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Chunk:
    index: int
    text: str


def chunk_text(text: str, max_chars: int = 1200, overlap_chars: int = 150) -> List[Chunk]:
    """
    Very reliable baseline chunker (character-based).
    - No tokenizers needed.
    - Deterministic.
    """
    if not text:
        return []

    t = text.strip()
    if not t:
        return []

    if max_chars <= 0:
        return [Chunk(index=0, text=t)]

    overlap = max(0, min(overlap_chars, max_chars - 1))
    chunks: List[Chunk] = []
    start = 0
    i = 0

    while start < len(t):
        end = min(len(t), start + max_chars)
        piece = t[start:end].strip()
        if piece:
            chunks.append(Chunk(index=i, text=piece))
            i += 1
        if end >= len(t):
            break
        start = end - overlap

    return chunks
