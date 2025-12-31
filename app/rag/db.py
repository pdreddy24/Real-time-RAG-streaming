from psycopg_pool import ConnectionPool
from rag.config import POSTGRES_DSN

_pool: ConnectionPool | None = None

def pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(conninfo=POSTGRES_DSN, min_size=1, max_size=10, open=True)
    return _pool
