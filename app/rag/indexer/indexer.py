from __future__ import annotations

import logging
import os
import sys
import time
from typing import Callable, Optional

logger = logging.getLogger("rag.indexer")


def _resolve_entrypoint() -> Callable[[], None]:
    """
    Single responsibility: find ONE callable consumer entrypoint and run it.
    Preferred: rag.indexer.consumer.main()

    This avoids "it worked locally but not in container" chaos.
    """

    candidates = [
        ("rag.indexer.consumer", ["main", "run", "start", "consume_forever"]),
        # keep these as future-proof options if you add other launchers later:
        ("rag.indexer.worker", ["main", "run", "start"]),
        ("rag.indexer.app", ["main", "run", "start"]),
    ]

    last_err: Optional[Exception] = None

    for mod_name, fn_names in candidates:
        try:
            mod = __import__(mod_name, fromlist=["*"])
            for fn_name in fn_names:
                fn = getattr(mod, fn_name, None)
                if callable(fn):
                    logger.info("Using entrypoint: %s.%s()", mod_name, fn_name)
                    return fn
        except Exception as e:
            last_err = e

    details = "\n".join([f"- {m}: {fns}" for m, fns in candidates])
    raise RuntimeError(
        "Cannot start indexer. No callable entrypoint found.\n"
        f"Checked:\n{details}\n"
        + (f"Last import error: {last_err!r}" if last_err else "")
    )


def main() -> None:
    # Make logging deterministic in Docker + local
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    logger.info("rag-indexer booting...")

    # Optional small warmup so dependencies settle (especially in docker-compose)
    warmup = float(os.getenv("INDEXER_WARMUP_SECONDS", "1.0"))
    if warmup > 0:
        time.sleep(warmup)

    fn = _resolve_entrypoint()

    try:
        fn()  # should block forever (consumer poll loop)
    except KeyboardInterrupt:
        logger.warning("rag-indexer interrupted")
    except Exception as e:
        logger.exception("rag-indexer crashed: %r", e)
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)
