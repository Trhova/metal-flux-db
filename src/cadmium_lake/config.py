from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from cadmium_lake.paths import CONFIGS_DIR


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


@lru_cache(maxsize=1)
def source_catalog() -> dict[str, Any]:
    return load_yaml(CONFIGS_DIR / "source_catalog.yaml")


@lru_cache(maxsize=1)
def analytes_config() -> dict[str, Any]:
    return load_yaml(CONFIGS_DIR / "analytes.yaml")


@lru_cache(maxsize=1)
def unit_mappings() -> dict[str, Any]:
    return load_yaml(CONFIGS_DIR / "unit_mappings.yaml")


@lru_cache(maxsize=1)
def matrix_taxonomy() -> dict[str, Any]:
    return load_yaml(CONFIGS_DIR / "matrix_taxonomy.yaml")
