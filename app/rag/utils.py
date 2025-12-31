import hashlib

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def to_pgvector_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{x:.8f}" for x in vec) + "]"

def parse_vector(v) -> list[float]:
    if isinstance(v, list):
        return [float(x) for x in v]
    if isinstance(v, str):
        s = v.strip().strip("[]")
        if not s:
            return []
        return [float(x) for x in s.split(",")]
    return [float(x) for x in list(v)]

def cosine_similarity_from_distance(dist: float) -> float:
    return 1.0 - float(dist)

def pack_citations(hits: list[dict], limit: int = 5) -> list[dict]:
    out = []
    for h in hits[:limit]:
        out.append({
            "id": h.get("id"),
            "source": h.get("source"),
            "title": h.get("title"),
            "url": h.get("url"),
            "score": h.get("score"),
        })
    return out
