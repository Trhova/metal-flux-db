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
    summary = read_duckdb_table("summary_measurements")
    source_files = read_duckdb_table("source_files")
    studies = read_duckdb_table("studies_or_batches")

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

    invalid_conversions = (
        raw.join(samples.select(["sample_id", "matrix_group"]), on="sample_id", how="left")
        .join(
            normalized.select(
                [
                    "measurement_id",
                    "canonical_unit",
                    "conversion_rule",
                    "uncertainty_flag",
                ]
            ),
            on="measurement_id",
            how="left",
        )
        .filter(pl.col("uncertainty_flag") == True)
        if not raw.is_empty() and not samples.is_empty() and not normalized.is_empty()
        else pl.DataFrame()
    )
    outputs["invalid_conversion_report"] = _write("invalid_conversion_report", invalid_conversions)

    missing_matrix_labels = samples.filter(pl.col("matrix_group").is_null() | (pl.col("matrix_group") == "")) if not samples.is_empty() else pl.DataFrame()
    outputs["missing_matrix_label_report"] = _write("missing_matrix_label_report", missing_matrix_labels)

    suspicious_values = (
        normalized.join(raw.select(["measurement_id", "sample_id", "raw_value_text", "raw_unit"]), on="measurement_id", how="left")
        .join(samples.select(["sample_id", "source_id", "matrix_group", "matrix_subtype", "sample_name"]), on="sample_id", how="left")
        .filter(
            (pl.col("canonical_value").is_not_null())
            & (
                (pl.col("canonical_value") <= 0)
                | (
                    (~pl.col("matrix_group").is_in(["blood", "water"]))
                    & (pl.col("canonical_value") > 1000)
                )
                | (
                    (pl.col("matrix_group").is_in(["blood", "water"]))
                    & (pl.col("canonical_value") > 50)
                )
            )
        )
        .with_columns(
            pl.when(pl.col("canonical_value") <= 0)
            .then(pl.lit("zero_or_negative_normalized_value"))
            .when((~pl.col("matrix_group").is_in(["blood", "water"])) & (pl.col("canonical_value") > 1000))
            .then(pl.lit("solid_matrix_gt_1000_mg_per_kg"))
            .when((pl.col("matrix_group").is_in(["blood", "water"])) & (pl.col("canonical_value") > 50))
            .then(pl.lit("liquid_matrix_gt_50_ug_per_l"))
            .otherwise(pl.lit("other_suspicious_value"))
            .alias("issue")
        )
        if not normalized.is_empty() and not raw.is_empty() and not samples.is_empty()
        else pl.DataFrame()
    )
    outputs["suspicious_value_report"] = _write("suspicious_value_report", suspicious_values)

    if not samples.is_empty():
        sample_meta = samples.join(studies.select(["study_id", "year_start", "year_end"]), on="study_id", how="left")
        completeness = (
            sample_meta.with_columns(
                [
                    (pl.col("country").is_null() | (pl.col("country") == "")).cast(pl.Float64).alias("missing_country"),
                    (pl.col("year_start").is_null() & pl.col("year_end").is_null()).cast(pl.Float64).alias("missing_year"),
                ]
            )
            .group_by("source_id")
            .agg(
                [
                    pl.len().alias("sample_count"),
                    (pl.mean("missing_country") * 100).alias("pct_missing_country"),
                    (pl.mean("missing_year") * 100).alias("pct_missing_year"),
                ]
            )
        )
    else:
        completeness = pl.DataFrame()
    outputs["metadata_completeness_report"] = _write("metadata_completeness_report", completeness)

    hash_report = source_files.select(["file_id", "source_id", "sha256", "local_path"]) if not source_files.is_empty() else pl.DataFrame()
    outputs["source_hash_stability_report"] = _write("source_hash_stability_report", hash_report)

    summary_report = (
        summary.select(
            [
                "summary_measurement_id",
                "source_id",
                "matrix_group",
                "statistic_name",
                "summary_unit",
                "summary_value",
                "lower_value",
                "upper_value",
            ]
        )
        if not summary.is_empty()
        else pl.DataFrame()
    )
    outputs["summary_measurement_report"] = _write("summary_measurement_report", summary_report)
    return outputs
