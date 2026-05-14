# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Logging helpers used by pipeline entry points."""

from __future__ import annotations

import logging
import sys
from contextlib import contextmanager
from datetime import UTC, datetime
from time import perf_counter
from typing import Iterator


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


@contextmanager
def log_job(logger: logging.Logger, job_name: str, **context: object) -> Iterator[None]:
    started = datetime.now(UTC).isoformat()
    start = perf_counter()
    logger.info("job_started name=%s started_at=%s context=%s", job_name, started, context)
    try:
        yield
    except Exception:
        logger.exception("job_failed name=%s elapsed_seconds=%.2f", job_name, perf_counter() - start)
        raise
    finally:
        finished = datetime.now(UTC).isoformat()
        logger.info("job_finished name=%s finished_at=%s elapsed_seconds=%.2f", job_name, finished, perf_counter() - start)
