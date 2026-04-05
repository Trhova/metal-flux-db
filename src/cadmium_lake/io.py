from __future__ import annotations

from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl

from cadmium_lake.paths import DB_PATH, PARQUET_DIR, ensure_directories


TABLE_ORDER = [
    "sources",
    "source_files",
    "studies_or_batches",
    "samples",
    "measurements_raw",
    "summary_measurements",
    "measurements_normalized",
    "linkage_edges",
    "review_queue",
    "pipeline_runs",
    "pipeline_run_steps",
]

TABLE_SCHEMAS: dict[str, dict[str, pl.DataType]] = {
    "sources": {
        "source_id": pl.String,
        "source_name": pl.String,
        "layer": pl.String,
        "organization": pl.String,
        "country_or_region": pl.String,
        "access_type": pl.String,
        "source_url": pl.String,
        "license_or_terms": pl.String,
        "retrieval_method": pl.String,
        "priority": pl.String,
        "notes": pl.String,
        "parser_class": pl.String,
        "status": pl.String,
        "maturity": pl.String,
    },
    "source_files": {
        "file_id": pl.String,
        "source_id": pl.String,
        "original_url": pl.String,
        "local_path": pl.String,
        "mime_type": pl.String,
        "sha256": pl.String,
        "retrieved_at": pl.String,
        "parser_version": pl.String,
    },
    "studies_or_batches": {
        "study_id": pl.String,
        "source_id": pl.String,
        "study_title": pl.String,
        "year_start": pl.Int64,
        "year_end": pl.Int64,
        "publication_year": pl.Int64,
        "country": pl.String,
        "citation": pl.String,
        "doi": pl.String,
        "pmid": pl.String,
        "repository_doi": pl.String,
        "notes": pl.String,
    },
    "samples": {
        "sample_id": pl.String,
        "source_id": pl.String,
        "study_id": pl.String,
        "matrix_group": pl.String,
        "matrix_subtype": pl.String,
        "sample_name": pl.String,
        "specimen_or_part": pl.String,
        "edible_portion_flag": pl.Boolean,
        "dry_wet_basis": pl.String,
        "as_sold_prepared_flag": pl.String,
        "location_name": pl.String,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
        "country": pl.String,
        "collection_date": pl.String,
        "collection_year": pl.Int64,
        "publication_year": pl.Int64,
        "year_for_plotting": pl.Int64,
        "year_for_plotting_source": pl.String,
        "analyte_method": pl.String,
        "lod": pl.Float64,
        "loq": pl.Float64,
        "comments": pl.String,
    },
    "measurements_raw": {
        "measurement_id": pl.String,
        "sample_id": pl.String,
        "analyte_name": pl.String,
        "raw_value": pl.Float64,
        "raw_value_text": pl.String,
        "raw_unit": pl.String,
        "nondetect_flag": pl.Boolean,
        "detection_qualifier": pl.String,
        "raw_basis_text": pl.String,
        "page_or_sheet": pl.String,
        "table_or_figure": pl.String,
        "row_label": pl.String,
        "column_label": pl.String,
        "extraction_method": pl.String,
        "confidence_score": pl.Float64,
    },
    "summary_measurements": {
        "summary_measurement_id": pl.String,
        "source_id": pl.String,
        "study_id": pl.String,
        "matrix_group": pl.String,
        "matrix_subtype": pl.String,
        "analyte_name": pl.String,
        "statistic_name": pl.String,
        "subgroup": pl.String,
        "item_label": pl.String,
        "raw_value_text": pl.String,
        "summary_value": pl.Float64,
        "lower_value": pl.Float64,
        "upper_value": pl.Float64,
        "summary_unit": pl.String,
        "summary_dimension": pl.String,
        "raw_basis_text": pl.String,
        "page_or_sheet": pl.String,
        "table_or_figure": pl.String,
        "extraction_method": pl.String,
        "confidence_score": pl.Float64,
        "derived_flag": pl.Boolean,
        "notes": pl.String,
    },
    "measurements_normalized": {
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
    },
    "linkage_edges": {
        "edge_id": pl.String,
        "from_sample_id": pl.String,
        "to_sample_id": pl.String,
        "relationship_type": pl.String,
        "linkage_confidence": pl.Float64,
        "notes": pl.String,
    },
    "review_queue": {
        "review_id": pl.String,
        "source_id": pl.String,
        "study_id": pl.String,
        "local_path": pl.String,
        "page_or_sheet": pl.String,
        "table_or_figure": pl.String,
        "issue_type": pl.String,
        "issue_summary": pl.String,
        "parsing_feasibility": pl.String,
        "status": pl.String,
        "notes": pl.String,
    },
    "pipeline_runs": {
        "run_id": pl.String,
        "command": pl.String,
        "started_at": pl.String,
        "completed_at": pl.String,
        "status": pl.String,
    },
    "pipeline_run_steps": {
        "run_step_id": pl.String,
        "run_id": pl.String,
        "step_name": pl.String,
        "started_at": pl.String,
        "completed_at": pl.String,
        "status": pl.String,
        "details": pl.String,
    },
}


def records_to_frame(records: Iterable[dict]) -> pl.DataFrame:
    rows = list(records)
    if not rows:
        return pl.DataFrame()
    return pl.from_dicts(rows, infer_schema_length=None, strict=False)


def empty_table_frame(name: str) -> pl.DataFrame:
    return pl.DataFrame(schema=TABLE_SCHEMAS[name])


def write_parquet_table(name: str, frame: pl.DataFrame) -> Path:
    ensure_directories()
    path = PARQUET_DIR / f"{name}.parquet"
    frame.write_parquet(path)
    return path


def write_duckdb_table(name: str, frame: pl.DataFrame) -> None:
    ensure_directories()
    with duckdb.connect(str(DB_PATH)) as conn:
        conn.register("frame_view", frame.to_arrow())
        conn.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM frame_view")
        conn.unregister("frame_view")


def append_duckdb_table(name: str, frame: pl.DataFrame) -> None:
    ensure_directories()
    with duckdb.connect(str(DB_PATH)) as conn:
        exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [name],
        ).fetchone()[0]
        conn.register("frame_view", frame.to_arrow())
        if exists:
            conn.execute(f"INSERT INTO {name} SELECT * FROM frame_view")
        else:
            conn.execute(f"CREATE TABLE {name} AS SELECT * FROM frame_view")
        conn.unregister("frame_view")


def read_duckdb_table(name: str) -> pl.DataFrame:
    if not DB_PATH.exists():
        return empty_table_frame(name) if name in TABLE_SCHEMAS else pl.DataFrame()
    with duckdb.connect(str(DB_PATH)) as conn:
        try:
            return pl.from_pandas(conn.execute(f"SELECT * FROM {name}").df())
        except duckdb.CatalogException:
            return empty_table_frame(name) if name in TABLE_SCHEMAS else pl.DataFrame()
