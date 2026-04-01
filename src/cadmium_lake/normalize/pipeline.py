from __future__ import annotations

import polars as pl

from cadmium_lake.io import read_duckdb_table, write_duckdb_table, write_parquet_table
from cadmium_lake.normalize.units import normalize_measurement


def run_normalization() -> pl.DataFrame:
    raw = read_duckdb_table("measurements_raw")
    samples = read_duckdb_table("samples")
    if raw.is_empty() or samples.is_empty():
        frame = pl.DataFrame(
            schema={
                "measurement_id": pl.String,
                "canonical_value": pl.Float64,
                "canonical_unit": pl.String,
                "canonical_dimension": pl.String,
                "conversion_rule": pl.String,
                "converted_from_unit": pl.String,
                "normalized_basis": pl.String,
                "moisture_adjustment_applied": pl.Boolean,
                "body_weight_adjustment_applied": pl.Boolean,
                "uncertainty_flag": pl.Boolean,
                "derived_flag": pl.Boolean,
                "derivation_notes": pl.String,
            }
        )
        write_duckdb_table("measurements_normalized", frame)
        write_parquet_table("measurements_normalized", frame)
        return frame

    joined = raw.join(samples.select(["sample_id", "matrix_group"]), on="sample_id", how="left")
    rows = []
    for record in joined.iter_rows(named=True):
        normalized = normalize_measurement(
            matrix_group=record["matrix_group"],
            raw_value=record["raw_value"],
            raw_unit=record["raw_unit"],
            raw_basis_text=record["raw_basis_text"],
        )
        rows.append(
            {
                "measurement_id": record["measurement_id"],
                "canonical_value": normalized.canonical_value,
                "canonical_unit": normalized.canonical_unit,
                "canonical_dimension": normalized.canonical_dimension,
                "conversion_rule": normalized.conversion_rule,
                "converted_from_unit": normalized.converted_from_unit,
                "normalized_basis": normalized.normalized_basis,
                "moisture_adjustment_applied": False,
                "body_weight_adjustment_applied": False,
                "uncertainty_flag": normalized.uncertainty_flag,
                "derived_flag": False,
                "derivation_notes": None,
            }
        )
    frame = pl.DataFrame(rows)
    write_duckdb_table("measurements_normalized", frame)
    write_parquet_table("measurements_normalized", frame)
    return frame
