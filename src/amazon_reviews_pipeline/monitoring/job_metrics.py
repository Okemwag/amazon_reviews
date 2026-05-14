# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Job metric records."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class JobMetric:
    job_name: str
    status: str
    started_at: str
    finished_at: str
    input_rows: int | None = None
    output_rows: int | None = None
    rejected_rows: int | None = None
    error_message: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)
