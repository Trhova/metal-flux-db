from __future__ import annotations

from collections import defaultdict

import duckdb
import polars as pl

from cadmium_lake.config import source_catalog
from cadmium_lake.io import empty_table_frame, read_duckdb_table, records_to_frame, write_duckdb_table, write_parquet_table
from cadmium_lake.models import SourceRecord
from cadmium_lake.paths import DB_PATH, ensure_directories
from cadmium_lake.sources import SOURCE_REGISTRY


def initialize_catalog_tables() -> None:
    ensure_directories()
    source_records = [SourceRecord(**record).model_dump(mode="json") for record in source_catalog()["sources"]]
    frame = records_to_frame(source_records)
    write_duckdb_table("sources", frame)
    write_parquet_table("sources", frame)
    existing = set()
    if DB_PATH.exists():
        with duckdb.connect(str(DB_PATH)) as conn:
            rows = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
            existing = {row[0] for row in rows}
    for table_name in [
        "source_files",
        "studies_or_batches",
        "samples",
        "measurements_raw",
        "measurements_normalized",
        "linkage_edges",
        "review_queue",
        "pipeline_runs",
        "pipeline_run_steps",
    ]:
        if table_name != "sources" and table_name not in existing:
            write_duckdb_table(table_name, empty_table_frame(table_name))


def selected_sources(source: str | None, include_literature: bool = False) -> list[str]:
    names = list(SOURCE_REGISTRY)
    if not include_literature:
        names = [name for name in names if name != "literature_search"]
    if source:
        if source not in SOURCE_REGISTRY:
            raise KeyError(f"Unknown source: {source}")
        return [source]
    return names


def fetch_sources(source: str | None = None) -> dict[str, int]:
    initialize_catalog_tables()
    results = {}
    source_files = []
    for source_id in selected_sources(source):
        adapter = SOURCE_REGISTRY[source_id]()
        try:
            fetched = adapter.fetch()
            source_files.extend([record.model_dump(mode="json") for record in fetched])
            results[source_id] = len(fetched)
        except Exception as exc:
            results[source_id] = -1
            print(f"fetch failed for {source_id}: {exc}")
    frame = records_to_frame(source_files)
    existing = read_duckdb_table("source_files")
    if frame.is_empty() and not existing.is_empty():
        frame = existing
    elif not existing.is_empty() and not frame.is_empty():
        frame = pl.concat([existing, frame], how="diagonal_relaxed").unique(subset=["file_id"], keep="last")
    elif frame.is_empty():
        frame = empty_table_frame("source_files")
    write_duckdb_table("source_files", frame)
    write_parquet_table("source_files", frame)
    return results


def parse_sources(source: str | None = None) -> dict[str, int]:
    initialize_catalog_tables()
    aggregated: dict[str, list[dict]] = defaultdict(list)
    results = {}
    for source_id in selected_sources(source):
        adapter = SOURCE_REGISTRY[source_id]()
        try:
            parsed = adapter.parse()
            table_counts = 0
            for table_name in [
                "source_files",
                "studies_or_batches",
                "samples",
                "measurements_raw",
                "linkage_edges",
                "review_queue",
            ]:
                records = [record.model_dump(mode="json") for record in getattr(parsed, table_name)]
                aggregated[table_name].extend(records)
                table_counts += len(records)
            results[source_id] = table_counts
        except Exception as exc:
            results[source_id] = -1
            print(f"parse failed for {source_id}: {exc}")
    for table_name, records in aggregated.items():
        frame = records_to_frame(records)
        existing = read_duckdb_table(table_name)
        if frame.is_empty() and not existing.is_empty():
            frame = existing
        elif not existing.is_empty() and not frame.is_empty():
            key = {
                "source_files": ["file_id"],
                "studies_or_batches": ["study_id"],
                "samples": ["sample_id"],
                "measurements_raw": ["measurement_id"],
                "linkage_edges": ["edge_id"],
                "review_queue": ["review_id"],
            }[table_name]
            frame = pl.concat([existing, frame], how="diagonal_relaxed").unique(subset=key, keep="last")
        elif frame.is_empty():
            frame = empty_table_frame(table_name)
        write_duckdb_table(table_name, frame)
        write_parquet_table(table_name, frame)
    return results


def run_literature_search(layer: str | None = None) -> dict[str, int]:
    initialize_catalog_tables()
    adapter = SOURCE_REGISTRY["literature_search"]()
    if layer:
        adapter.THEMES = {key: value for key, value in adapter.THEMES.items() if key == layer}
    parsed = adapter.parse()
    source_files = records_to_frame(record.model_dump(mode="json") for record in parsed.source_files)
    studies = records_to_frame(record.model_dump(mode="json") for record in parsed.studies_or_batches)
    samples = records_to_frame(record.model_dump(mode="json") for record in parsed.samples)
    raw = records_to_frame(record.model_dump(mode="json") for record in parsed.measurements_raw)
    linkage = records_to_frame(record.model_dump(mode="json") for record in parsed.linkage_edges)
    review = records_to_frame(record.model_dump(mode="json") for record in parsed.review_queue)
    existing_source_files = read_duckdb_table("source_files")
    existing_studies = read_duckdb_table("studies_or_batches")
    existing_samples = read_duckdb_table("samples")
    existing_raw = read_duckdb_table("measurements_raw")
    existing_linkage = read_duckdb_table("linkage_edges")
    existing_review = read_duckdb_table("review_queue")
    if source_files.is_empty() and not existing_source_files.is_empty():
        source_files = existing_source_files
    elif not existing_source_files.is_empty() and not source_files.is_empty():
        source_files = pl.concat([existing_source_files, source_files], how="diagonal_relaxed").unique(
            subset=["file_id"], keep="last"
        )
    elif source_files.is_empty():
        source_files = empty_table_frame("source_files")
    if studies.is_empty() and not existing_studies.is_empty():
        studies = existing_studies
    elif not existing_studies.is_empty() and not studies.is_empty():
        studies = pl.concat([existing_studies, studies], how="diagonal_relaxed").unique(subset=["study_id"], keep="last")
    elif studies.is_empty():
        studies = empty_table_frame("studies_or_batches")
    if samples.is_empty() and not existing_samples.is_empty():
        samples = existing_samples
    elif not existing_samples.is_empty() and not samples.is_empty():
        samples = pl.concat([existing_samples, samples], how="diagonal_relaxed").unique(subset=["sample_id"], keep="last")
    elif samples.is_empty():
        samples = empty_table_frame("samples")
    if raw.is_empty() and not existing_raw.is_empty():
        raw = existing_raw
    elif not existing_raw.is_empty() and not raw.is_empty():
        raw = pl.concat([existing_raw, raw], how="diagonal_relaxed").unique(subset=["measurement_id"], keep="last")
    elif raw.is_empty():
        raw = empty_table_frame("measurements_raw")
    if linkage.is_empty() and not existing_linkage.is_empty():
        linkage = existing_linkage
    elif not existing_linkage.is_empty() and not linkage.is_empty():
        linkage = pl.concat([existing_linkage, linkage], how="diagonal_relaxed").unique(subset=["edge_id"], keep="last")
    elif linkage.is_empty():
        linkage = empty_table_frame("linkage_edges")
    if review.is_empty() and not existing_review.is_empty():
        review = existing_review
    elif not existing_review.is_empty() and not review.is_empty():
        review = pl.concat([existing_review, review], how="diagonal_relaxed").unique(subset=["review_id"], keep="last")
    elif review.is_empty():
        review = empty_table_frame("review_queue")
    write_duckdb_table("source_files", source_files)
    write_parquet_table("source_files", source_files)
    write_duckdb_table("studies_or_batches", studies)
    write_parquet_table("studies_or_batches", studies)
    write_duckdb_table("samples", samples)
    write_parquet_table("samples", samples)
    write_duckdb_table("measurements_raw", raw)
    write_parquet_table("measurements_raw", raw)
    write_duckdb_table("linkage_edges", linkage)
    write_parquet_table("linkage_edges", linkage)
    write_duckdb_table("review_queue", review)
    write_parquet_table("review_queue", review)
    return {
        "source_files": source_files.height,
        "studies_or_batches": studies.height,
        "samples": samples.height,
        "measurements_raw": raw.height,
        "review_queue": review.height,
    }
