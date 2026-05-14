# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Configuration loading helpers.

The project uses TOML for the runnable pipeline and YAML files for assignment
documentation. This module keeps the rest of the code from hardcoding paths.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from amazon_reviews_pipeline.common.constants import DEFAULT_CONFIG_PATH

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.8 in the Spark image.
    tomllib = None


def _parse_scalar(value: str) -> Any:
    value = value.strip().strip('"').strip("'")
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    try:
        return int(value)
    except ValueError:
        return value


def _load_simple_yaml(path: Path) -> dict[str, Any]:
    """Load the simple key/value YAML used by this repository."""
    config: dict[str, Any] = {}
    current_list: str | None = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line:
            continue
        if line.startswith("  - ") and current_list:
            config[current_list].append(_parse_scalar(line[4:]))
            continue
        if ":" in line:
            key, value = line.split(":", 1)
            key = key.strip()
            if value.strip():
                config[key] = _parse_scalar(value)
                current_list = None
            else:
                config[key] = []
                current_list = key
    return config


def _load_simple_toml(path: Path) -> dict[str, Any]:
    config: dict[str, Any] = {}
    section: dict[str, Any] | None = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            section_name = line[1:-1].strip()
            section = config.setdefault(section_name, {})
            continue
        if "=" in line and section is not None:
            key, value = line.split("=", 1)
            section[key.strip()] = _parse_scalar(value)
    return config


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    config_path = Path(path)
    if config_path.suffix == ".toml":
        if tomllib is not None:
            with config_path.open("rb") as handle:
                return tomllib.load(handle)
        return _load_simple_toml(config_path)
    if config_path.suffix in {".yaml", ".yml"}:
        return _load_simple_yaml(config_path)
    raise ValueError(f"Unsupported config format: {config_path}")


def config_value(config: dict[str, Any], section: str, key: str, default: Any | None = None) -> Any:
    try:
        return config[section][key]
    except KeyError:
        if default is not None:
            return default
        raise


def hdfs_paths(config: dict[str, Any]) -> dict[str, str]:
    values = config.get("hdfs", config)
    return {
        "bronze": values.get("bronze_reviews_dir", values.get("landing_reviews_dir", "")),
        "silver": values.get("silver_reviews_dir", values.get("processed_reviews_dir", "")),
        "gold": values.get("gold_dir", ""),
        "quarantine": values.get("quarantine_dir", ""),
        "models": values.get("models_dir", ""),
        "metrics": values.get("metrics_dir", values.get("quality_dir", "")),
    }
