import os
from typing import List
from fastembed import TextEmbedding

_model = None

def get_model_name() -> str:
    return os.getenv("FASTEMBED_MODEL", "BAAI/bge-small-en-v1.5")

def _get():
    global _model
    if _model is None:
        _model = TextEmbedding(model_name=get_model_name())
    return _model

def embed(texts: List[str]) -> List[List[float]]:
    """
    Return pure Python floats (not numpy.float32) so psycopg2/pgvector can adapt it.
    """
    model = _get()
    out: List[List[float]] = []
    for v in model.embed(texts):
        out.append([float(x) for x in v])
    return out
