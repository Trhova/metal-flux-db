# QA And Tests

> 48 nodes · cohesion 0.10

## Key Concepts

- **initialize_catalog_tables()** (19 connections) — `src/cadmium_lake/pipeline.py`
- **test_pipeline.py** (16 connections) — `/home/trhova/metal-flux-db/tests/test_pipeline.py`
- **read_duckdb_table()** (15 connections) — `src/cadmium_lake/io.py`
- **parse_sources()** (13 connections) — `src/cadmium_lake/pipeline.py`
- **test_normalize_qa_and_views()** (12 connections) — `/home/trhova/metal-flux-db/tests/test_pipeline.py`
- **fetch_sources()** (11 connections) — `src/cadmium_lake/pipeline.py`
- **run_literature_search()** (11 connections) — `src/cadmium_lake/pipeline.py`
- **cli.py** (9 connections) — `src/cadmium_lake/cli.py`
- **test_parse_smoke_for_all_sources()** (9 connections) — `/home/trhova/metal-flux-db/tests/test_pipeline.py`
- **write_duckdb_table()** (8 connections) — `src/cadmium_lake/io.py`
- **write_text()** (8 connections) — `src/cadmium_lake/utils.py`
- **write_parquet_table()** (7 connections) — `src/cadmium_lake/io.py`
- **run_normalization()** (7 connections) — `src/cadmium_lake/normalize/pipeline.py`
- **empty_table_frame()** (6 connections) — `src/cadmium_lake/io.py`
- **ensure_directories()** (6 connections) — `src/cadmium_lake/paths.py`
- **io.py** (6 connections) — `src/cadmium_lake/io.py`
- **run_qa_checks()** (5 connections) — `/home/trhova/metal-flux-db/src/cadmium_lake/qa/checks.py`
- **append_duckdb_table()** (5 connections) — `src/cadmium_lake/io.py`
- **records_to_frame()** (5 connections) — `src/cadmium_lake/io.py`
- **pipeline.py** (5 connections) — `src/cadmium_lake/pipeline.py`
- **seed_usgs_fixture()** (4 connections) — `/home/trhova/metal-flux-db/tests/test_pipeline.py`
- **seed_washington_fixture()** (4 connections) — `/home/trhova/metal-flux-db/tests/test_pipeline.py`
- **seed_wqp_fixture()** (4 connections) — `/home/trhova/metal-flux-db/tests/test_pipeline.py`
- **test_fetch_manifest_hash_stability()** (4 connections) — `/home/trhova/metal-flux-db/tests/test_pipeline.py`
- **test_literature_curated_extractors()** (4 connections) — `/home/trhova/metal-flux-db/tests/test_pipeline.py`
- *... and 23 more nodes in this community*

## Relationships

- No strong cross-community connections detected

## Source Files

- `/home/trhova/metal-flux-db/src/cadmium_lake/qa/checks.py`
- `/home/trhova/metal-flux-db/src/cadmium_lake/sources/literature.py`
- `/home/trhova/metal-flux-db/src/cadmium_lake/viz/views.py`
- `/home/trhova/metal-flux-db/tests/test_pipeline.py`
- `src/cadmium_lake/cli.py`
- `src/cadmium_lake/io.py`
- `src/cadmium_lake/normalize/pipeline.py`
- `src/cadmium_lake/paths.py`
- `src/cadmium_lake/pipeline.py`
- `src/cadmium_lake/utils.py`

## Audit Trail

- EXTRACTED: 119 (46%)
- INFERRED: 139 (54%)
- AMBIGUOUS: 0 (0%)

---

*Part of the graphify knowledge wiki. See [[index]] to navigate.*