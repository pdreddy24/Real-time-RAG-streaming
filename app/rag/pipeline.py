from __future__ import annotations

from rag.settings import Settings
from rag.phase6 import Phase6Rag


class RagPipeline:
    def __init__(self):
        settings = Settings.from_env()
        self.engine = Phase6Rag(settings)

    def query(self, query: str, top_k: int = 3):
        return self.engine.query(query, top_k=top_k)
