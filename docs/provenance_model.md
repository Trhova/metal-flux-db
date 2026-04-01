# Provenance Model

Every cadmium measurement must remain traceable to its origin. The pipeline preserves:

- source metadata
- source file hash and local path
- page, sheet, table, row, and column context when available
- raw value text and raw unit
- extraction method and confidence
- conversion rule and direct-vs-derived flags

The canonical lineage is:

`sources -> source_files -> studies_or_batches -> samples -> measurements_raw -> measurements_normalized`

`pipeline_runs` and `pipeline_run_steps` log execution metadata for reproducibility.

`review_queue` stores unresolved extraction work items with explicit provenance so ambiguous literature tables can be audited without losing context.
