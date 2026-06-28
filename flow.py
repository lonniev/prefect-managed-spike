"""Zero-to-one spike flow.

Proves a Prefect Managed work pool can run ONE long job to completion on
detached infrastructure — independently of the (recycling, stateless) process
that triggered it.

The sleep is deliberately longer than any serverless freeze/recycle window: if
this run reaches COMPLETED, the work survived on Prefect's own compute, not in
the caller's process. The reported host/pid will differ from the trigger's,
which is the whole point.
"""

from __future__ import annotations

import os
import socket
import time

from prefect import flow, get_run_logger


@flow(name="long-job", log_prints=True)
def long_job(total_seconds: int = 150, step: int = 15) -> dict:
    logger = get_run_logger()
    host = socket.gethostname()
    pid = os.getpid()
    logger.info("long_job START host=%s pid=%s total=%ss", host, pid, total_seconds)

    elapsed = 0
    while elapsed < total_seconds:
        time.sleep(min(step, total_seconds - elapsed))
        elapsed = min(elapsed + step, total_seconds)
        logger.info("…alive at %ss / %ss (host=%s pid=%s)", elapsed, total_seconds, host, pid)

    logger.info("long_job DONE host=%s pid=%s", host, pid)
    return {"ok": True, "slept_seconds": total_seconds, "ran_on_host": host, "ran_on_pid": pid}
