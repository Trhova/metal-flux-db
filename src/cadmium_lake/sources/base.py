from __future__ import annotations

import mimetypes
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from cadmium_lake.config import source_catalog
from cadmium_lake.models import (
    LinkageEdgeRecord,
    RawMeasurementRecord,
    ReviewQueueRecord,
    SampleRecord,
    SourceFileRecord,
    SourceRecord,
    StudyRecord,
    SummaryMeasurementRecord,
)
from cadmium_lake.paths import RAW_DIR, STAGING_DIR
from cadmium_lake.utils import now_utc, sha256_file, stable_id


@dataclass
class ParsedPayload:
    source_files: list[SourceFileRecord] = field(default_factory=list)
    studies_or_batches: list[StudyRecord] = field(default_factory=list)
    samples: list[SampleRecord] = field(default_factory=list)
    measurements_raw: list[RawMeasurementRecord] = field(default_factory=list)
    summary_measurements: list[SummaryMeasurementRecord] = field(default_factory=list)
    linkage_edges: list[LinkageEdgeRecord] = field(default_factory=list)
    review_queue: list[ReviewQueueRecord] = field(default_factory=list)


class BaseAdapter:
    source_id: str
    parser_version = "0.1.0"

    def __init__(self) -> None:
        match = next(
            item for item in source_catalog()["sources"] if item["source_id"] == self.source_id
        )
        self.config = match
        self.raw_dir = RAW_DIR / self.source_id
        self.staging_dir = STAGING_DIR / self.source_id
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)

    def source_record(self) -> SourceRecord:
        return SourceRecord(**self.config)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    def _download(self, url: str) -> bytes:
        with httpx.Client(follow_redirects=True, timeout=60.0) as client:
            response = client.get(url, headers={"User-Agent": "cadmium-lake/0.1.0"})
            response.raise_for_status()
            return response.content

    def _client(self) -> httpx.Client:
        return httpx.Client(
            follow_redirects=True,
            timeout=60.0,
            headers={"User-Agent": "cadmium-lake/0.1.0"},
        )

    def fetch(self) -> list[SourceFileRecord]:
        raise NotImplementedError

    def parse(self) -> ParsedPayload:
        raise NotImplementedError

    def _write_raw_file(self, filename: str, url: str, content: bytes) -> SourceFileRecord:
        path = self.raw_dir / filename
        path.write_bytes(content)
        mime_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        return SourceFileRecord(
            file_id=stable_id(self.source_id, url, filename),
            source_id=self.source_id,
            original_url=url,
            local_path=str(path),
            mime_type=mime_type,
            sha256=sha256_file(path),
            retrieved_at=now_utc(),
            parser_version=self.parser_version,
        )

    def _write_and_hash_existing(self, path: Path) -> str:
        return sha256_file(path)

    def _timestamp(self):
        return now_utc()

    def _staging_path(self, name: str) -> Path:
        return self.staging_dir / name

    def _records_from_raw_dir(self) -> list[Path]:
        return sorted(path for path in self.raw_dir.iterdir() if path.is_file())

    def _write_staging_json(self, name: str, payload: list[dict[str, Any]]) -> Path:
        import json

        path = self._staging_path(name)
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        return path
