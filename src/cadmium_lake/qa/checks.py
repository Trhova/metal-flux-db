from __future__ import annotations

from pathlib import Path

import polars as pl

from cadmium_lake.io import read_duckdb_table
from cadmium_lake.paths import QA_DIR


def _write(name: str, frame: pl.DataFrame) -> Path:
    path = QA_DIR / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)
    return path


def run_qa_checks() -> dict[str, Path]:
    samples = read_duckdb_table("samples")
    raw = read_duckdb_table("measurements_raw")
    normalized = read_duckdb_table("measurements_normalized")
    source_files = read_duckdb_table("source_files")

    outputs: dict[str, Path] = {}

    unit_parse = normalized.select(
        [
            "measurement_id",
            "canonical_unit",
            "canonical_dimension",
            "uncertainty_flag",
        ]
    ) if not normalized.is_empty() else pl.DataFrame()
    outputs["unit_parse_report"] = _write("unit_parse_report", unit_parse)

    basis_report = raw.select(["measurement_id", "raw_basis_text"]).join(
        normalized.select(["measurement_id", "normalized_basis"]),
        on="measurement_id",
        how="left",
    ) if not raw.is_empty() and not normalized.is_empty() else pl.DataFrame()
    outputs["basis_preservation_report"] = _write("basis_preservation_report", basis_report)

    nondetect = raw.filter(pl.col("nondetect_flag") == True) if not raw.is_empty() else pl.DataFrame()
    outputs["nondetect_audit"] = _write("nondetect_audit", nondetect)

    duplicates = raw.group_by(["sample_id", "raw_value_text", "raw_unit"]).len().filter(pl.col("len") > 1) if not raw.is_empty() else pl.DataFrame()
    outputs["duplicate_candidate_report"] = _write("duplicate_candidate_report", duplicates)

    if not raw.is_empty():
        provenance = raw.join(samples.select(["sample_id", "source_id"]), on="sample_id", how="left")
        provenance = provenance.with_columns(
            (
                pl.col("page_or_sheet").is_not_null()
                & pl.col("table_or_figure").is_not_null()
                & pl.col("raw_unit").is_not_null()
            ).alias("provenance_complete")
        )
    else:
        provenance = pl.DataFrame()
    outputs["provenance_completeness_report"] = _write("provenance_completeness_report", provenance)

    impossible = normalized.filter(pl.col("canonical_value") < 0) if not normalized.is_empty() else pl.DataFrame()
    outputs["impossible_value_report"] = _write("impossible_value_report", impossible)

    hash_report = source_files.select(["file_id", "source_id", "sha256", "local_path"]) if not source_files.is_empty() else pl.DataFrame()
    outputs["source_hash_stability_report"] = _write("source_hash_stability_report", hash_report)
    return outputs
