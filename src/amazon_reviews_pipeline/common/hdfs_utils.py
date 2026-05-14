# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Small wrappers for HDFS commands executed through Docker."""

from __future__ import annotations

import shlex
import subprocess


def hdfs_command(container: str, *args: str) -> list[str]:
    quoted_args = " ".join(shlex.quote(arg) for arg in args)
    return ["docker", "exec", container, "bash", "-c", f"hdfs dfs {quoted_args}"]


def run_hdfs(container: str, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(hdfs_command(container, *args), check=check, text=True)


def ensure_directories(container: str, paths: list[str]) -> None:
    for path in paths:
        run_hdfs(container, "-mkdir", "-p", path)


def upload_file(container: str, local_path: str, hdfs_path: str) -> None:
    run_hdfs(container, "-put", "-f", local_path, hdfs_path)
