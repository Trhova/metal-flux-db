from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable
from uuid import uuid4


def now_utc() -> datetime:
    return datetime.now(UTC)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_id(*parts: object) -> str:
    payload = "||".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def batched_records(records: Iterable[dict]) -> list[dict]:
    return [record for record in records]


def new_run_id() -> str:
    return uuid4().hex
