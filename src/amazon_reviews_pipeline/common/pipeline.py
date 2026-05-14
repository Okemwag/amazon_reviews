# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shlex
import subprocess
import sys
import time
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DEFAULT_CONFIG = Path("configs/dev.toml")


@dataclass(frozen=True)
class PipelineConfig:
    path: Path
    values: dict[str, Any]

    @classmethod
    def load(cls, path: Path) -> "PipelineConfig":
        with path.open("rb") as handle:
            return cls(path=path, values=tomllib.load(handle))

    def get(self, section: str, key: str) -> str:
        return str(self.values[section][key])


def run_command(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    printable = " ".join(shlex.quote(part) for part in command)
    print(f"$ {printable}", flush=True)
    return subprocess.run(command, check=check, text=True)


def docker_exec(container: str, command: str) -> None:
    run_command(["docker", "exec", container, "bash", "-c", command])


def docker_exec_as_root(container: str, command: str) -> None:
    run_command(["docker", "exec", "--user", "root", container, "bash", "-c", command])


def docker_command(container: str, command: str) -> list[str]:
    return ["docker", "exec", container, "bash", "-c", command]


def wait_for(label: str, command: list[str], *, attempts: int = 30, delay_seconds: int = 10) -> None:
    for attempt in range(1, attempts + 1):
        try:
            result = subprocess.run(
                command,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=120,
            )
        except subprocess.TimeoutExpired:
            result = subprocess.CompletedProcess(command, returncode=124)
        if result.returncode == 0:
            print(f"{label} is ready", flush=True)
            return
        print(f"Waiting for {label} ({attempt}/{attempts})", flush=True)
        time.sleep(delay_seconds)
    printable = " ".join(shlex.quote(part) for part in command)
    raise RuntimeError(f"{label} did not become ready: {printable}")


def project_root(config_path: Path) -> Path:
    return config_path.resolve().parent.parent


def workspace_path(local_path: Path, root: Path, workspace: str) -> str:
    resolved = local_path.resolve()
    try:
        relative = resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"Input file must be inside project root: {root}") from exc
    return f"{workspace}/{relative.as_posix()}"


def file_profile(path: Path) -> tuple[int, str]:
    if path.suffix == ".gz":
        print(f"Scanning compressed input for row count and checksum: {path}", flush=True)
        line_count = 0
        digest = hashlib.sha256()
        with gzip.open(path, "rb") as handle:
            for line in handle:
                line_count += 1
                digest.update(line)
        return line_count, digest.hexdigest()

    print(f"Scanning input for row count and checksum: {path}", flush=True)
    line_count = 0
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024 * 16), b""):
            line_count += chunk.count(b"\n")
            digest.update(chunk)
    return line_count, digest.hexdigest()


def start_services(config: PipelineConfig) -> None:
    compose_file = config.get("docker", "compose_file")
    run_command(["docker", "compose", "-f", compose_file, "up", "-d"])


def wait_for_hdfs(config: PipelineConfig) -> None:
    wait_for(
        "HDFS",
        docker_command(config.get("docker", "namenode_container"), "hdfs dfs -ls /"),
    )


def wait_for_hive(config: PipelineConfig) -> None:
    wait_for(
        "HiveServer2",
        docker_command(
            config.get("docker", "hive_container"),
            f"beeline -u {shlex.quote(config.get('docker', 'hive_jdbc'))} -e 'SHOW DATABASES'",
        ),
        attempts=45,
    )


def wait_for_spark(config: PipelineConfig) -> None:
    wait_for(
        "Spark",
        docker_command(config.get("docker", "spark_container"), "/opt/spark/bin/spark-submit --version"),
    )


def ingest(config: PipelineConfig, input_file: Path) -> None:
    wait_for_hdfs(config)
    root = project_root(config.path)
    input_file = input_file if input_file.is_absolute() else root / input_file
    if not input_file.exists():
        raise FileNotFoundError(f"Missing input file: {input_file}")
    if not input_file.name.endswith((".jsonl", ".jsonl.gz", ".json.gz")):
        raise ValueError("Input must be a JSON Lines file ending in .jsonl, .jsonl.gz, or .json.gz")

    workspace = config.get("project", "workspace")
    manifest_dir = root / config.get("local", "manifest_dir")
    manifest_dir.mkdir(parents=True, exist_ok=True)

    ingested_at = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    dataset_name = input_file.name.removesuffix(".jsonl.gz").removesuffix(".json.gz").removesuffix(".jsonl")
    manifest_path = manifest_dir / f"{dataset_name}_{ingested_at}.json"
    hdfs_file = f"{config.get('hdfs', 'landing_reviews_dir')}/{input_file.name}"
    row_count, checksum = file_profile(input_file)

    manifest = {
        "dataset": "amazon_reviews_2023",
        "source_file": input_file.name,
        "local_path": str(input_file),
        "workspace_path": workspace_path(input_file, root, workspace),
        "hdfs_path": hdfs_file,
        "row_count": row_count,
        "byte_size": input_file.stat().st_size,
        "sha256": checksum,
        "ingested_at_utc": ingested_at,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    namenode = config.get("docker", "namenode_container")
    commands = [
        f"hdfs dfs -mkdir -p {shlex.quote(config.get('hdfs', 'landing_reviews_dir'))}",
        f"hdfs dfs -mkdir -p {shlex.quote(config.get('hdfs', 'manifest_dir'))}",
        f"hdfs dfs -mkdir -p {shlex.quote(config.get('hdfs', 'warehouse_dir'))}",
        "hdfs dfs -chmod -R 777 /user || true",
        "hdfs dfs -put -f "
        f"{shlex.quote(manifest['workspace_path'])} {shlex.quote(hdfs_file)}",
        "hdfs dfs -put -f "
        f"{shlex.quote(workspace_path(manifest_path, root, workspace))} "
        f"{shlex.quote(config.get('hdfs', 'manifest_dir') + '/' + manifest_path.name)}",
        f"hdfs dfs -ls {shlex.quote(config.get('hdfs', 'landing_reviews_dir'))}",
    ]
    docker_exec(namenode, " && ".join(commands))
    print(f"Ingestion manifest: {manifest_path}")


def create_tables(config: PipelineConfig) -> None:
    wait_for_hive(config)
    hive_container = config.get("docker", "hive_container")
    jdbc = config.get("docker", "hive_jdbc")
    ddl = config.get("sql", "ddl")
    docker_exec(hive_container, f"beeline -u {shlex.quote(jdbc)} -f {shlex.quote(ddl)}")


def spark_submit(config: PipelineConfig, job_path: str) -> None:
    wait_for_spark(config)
    spark_container = config.get("docker", "spark_container")
    master = config.get("docker", "spark_master")
    docker_exec_as_root(spark_container, "mkdir -p /workspace/output && chmod -R a+rwX /workspace/output")
    docker_exec(
        spark_container,
        f"/opt/spark/bin/spark-submit --master {shlex.quote(master)} {shlex.quote(job_path)}",
    )


def transform(config: PipelineConfig) -> None:
    spark_submit(config, config.get("spark", "transform_job"))


def quality(config: PipelineConfig) -> None:
    spark_submit(config, config.get("spark", "quality_job"))


def analysis(config: PipelineConfig) -> None:
    hive_container = config.get("docker", "hive_container")
    jdbc = config.get("docker", "hive_jdbc")
    sql = config.get("sql", "analysis")
    docker_exec(hive_container, f"beeline -u {shlex.quote(jdbc)} -f {shlex.quote(sql)}")


def train(config: PipelineConfig) -> None:
    spark_submit(config, config.get("spark", "train_job"))


def run_pipeline(config: PipelineConfig, input_file: Path, *, skip_up: bool) -> None:
    if not skip_up:
        start_services(config)
    ingest(config, input_file)
    create_tables(config)
    transform(config)
    quality(config)
    analysis(config)
    train(config)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Amazon Reviews end-to-end data pipeline")
    parser.add_argument(
        "command",
        choices=["up", "run", "ingest", "tables", "transform", "quality", "analysis", "train"],
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--input", type=Path, default=Path("data/raw/Office_Products.jsonl"))
    parser.add_argument("--skip-up", action="store_true", help="Do not start Docker Compose for the run command")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    config = PipelineConfig.load(args.config)

    if args.command == "up":
        start_services(config)
    elif args.command == "run":
        run_pipeline(config, args.input, skip_up=args.skip_up)
    elif args.command == "ingest":
        ingest(config, args.input)
    elif args.command == "tables":
        create_tables(config)
    elif args.command == "transform":
        transform(config)
    elif args.command == "quality":
        quality(config)
    elif args.command == "analysis":
        analysis(config)
    elif args.command == "train":
        train(config)


if __name__ == "__main__":
    main()
