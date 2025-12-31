from rag.db import pool

def db_ready() -> bool:
    try:
        with pool().connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        return False
