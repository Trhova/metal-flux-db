from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import polars as pl

from cadmium_lake.io import append_duckdb_table
from cadmium_lake.models import PipelineRunRecord, PipelineRunStepRecord
from cadmium_lake.utils import new_run_id


@dataclass
class PipelineLogger:
    command: str
    run_id: str

    @classmethod
    def start(cls, command: str) -> "PipelineLogger":
        run_id = new_run_id()
        started_at = datetime.now(UTC)
        run = PipelineRunRecord(
            run_id=run_id,
            command=command,
            started_at=started_at,
            status="running",
        )
        append_duckdb_table("pipeline_runs", pl.DataFrame([run.model_dump()]))
        return cls(command=command, run_id=run_id)

    def step(self, step_name: str, status: str, details: str | None = None) -> None:
        step = PipelineRunStepRecord(
            run_step_id=new_run_id(),
            run_id=self.run_id,
            step_name=step_name,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            status=status,
            details=details,
        )
        append_duckdb_table("pipeline_run_steps", pl.DataFrame([step.model_dump()]))
