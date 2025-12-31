import os
import time
from typing import List

import psycopg
from openai import OpenAI

BATCH_SIZE = int(os.getenv("BACKFILL_BATCH_SIZE", "25"))
SLEEP_S = float(os.getenv("BACKFILL_SLEEP_S", "0.2"))

POSTGRES_DSN = os.environ["POSTGRES_DSN"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
OPENAI_EMBED_MODEL = os.getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small")

TABLE = os.getenv("CHUNKS_TABLE", "document_chunks")
DIMS = int(os.getenv("EMBED_DIMS", "1536"))

client = OpenAI(api_key=OPENAI_API_KEY)

def main():
    with psycopg.connect(POSTGRES_DSN) as conn:
        while True:
            rows = conn.execute(
                f"""
                SELECT id, content
                FROM {TABLE}
                WHERE embedding IS NULL
                ORDER BY id
                LIMIT %s
                """,
                (BATCH_SIZE,),
            ).fetchall()

            if not rows:
                print("Backfill complete: no NULL embeddings remaining.")
                break

            ids = [r[0] for r in rows]
            texts = [r[1] for r in rows]

            resp = client.embeddings.create(model=OPENAI_EMBED_MODEL, input=texts)
            vecs: List[List[float]] = [d.embedding for d in resp.data]

            with conn.cursor() as cur:
                for _id, vec in zip(ids, vecs):
                    if len(vec) != DIMS:
                        raise RuntimeError(f"Embedding dims mismatch for id={_id}: got {len(vec)} expected {DIMS}")

                    cur.execute(
                        f"UPDATE {TABLE} SET embedding = %s::vector WHERE id = %s",
                        (vec, _id),
                    )

            conn.commit()
            print(f"Updated embeddings for ids: {ids}")
            time.sleep(SLEEP_S)

if __name__ == "__main__":
    main()
