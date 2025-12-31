import os

from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector

POSTGRES_DSN = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
if not POSTGRES_DSN:
    raise RuntimeError("POSTGRES_DSN or DATABASE_URL must be set for the API service")

# configure=register_vector ensures every new connection knows the `vector` type + adapters
pool = ConnectionPool(
    conninfo=POSTGRES_DSN,
    min_size=1,
    max_size=10,
    open=True,
    configure=register_vector,
)
