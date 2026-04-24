# Cadmium Lake

Cadmium Lake is a provenance-first cadmium data lake and exploration layer. It ingests official monitoring datasets plus literature-search metadata, preserves raw artifacts and extraction context, normalizes only within scientifically compatible matrix/unit rules, and writes reproducible outputs to DuckDB and Parquet.

The core database is `data/curated/cadmium_lake.duckdb`. The main row-level provenance path is:

```text
sources -> source_files -> studies_or_batches -> samples -> measurements_raw -> measurements_normalized
```

For analysis, `cadmium-lake build-views` exports `measurement_master_view` to `data/curated/views/measurement_master_view.csv`. That view keeps each direct measured concentration tied to its source, study, sample, raw extraction location, raw value/unit, canonical value/unit, conversion rule, matrix, country, coordinates, and plotting year.

Current local build after the European official-data expansion contains 195,322 direct concentration rows in `measurement_master_view`, including EEA Waterbase water rows, EFSA seaweed/halophyte food occurrence rows, and FOREGS/EuroGeoSurveys topsoil/subsoil rows.

## Stack

- Python 3.11+
- Polars
- DuckDB
- Parquet
- Pydantic v2
- Pint
- HTTPX + Tenacity
- Typer

## Repository layout

```text
configs/
docs/
notebooks/
src/cadmium_lake/
tests/
data/              # local only, ignored by git
  raw/
  staging/
  curated/
```

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Reproduce the v1 pipeline

```bash
cadmium-lake fetch
cadmium-lake parse
cadmium-lake normalize
cadmium-lake qa
cadmium-lake literature-search
cadmium-lake build-views
```

This sequence populates:

- `data/raw/` with downloaded source files and fetch manifests
- `data/staging/` with parsed source-specific tables
- `data/curated/` with DuckDB, Parquet, QA reports, view exports, and plots

`data/` is intentionally local-only and is not tracked in GitHub. The repository tracks code, configs, and documentation needed to rebuild it.

## Commands

```bash
cadmium-lake fetch --source washington_fertilizer
cadmium-lake parse --source usgs_soil
cadmium-lake normalize --analysis-policy censored
cadmium-lake qa
cadmium-lake literature-search --layer crop
cadmium-lake build-views --with-plots
```

## Outputs

- DuckDB database: `data/curated/cadmium_lake.duckdb`
- Parquet mirrors for canonical tables in `data/curated/parquet/`
- Summary statistics table: `summary_measurements`
- QA reports in `data/curated/qa/`
- Gold analytical views in DuckDB plus `data/curated/views/`
- Minimal exploration notebook: `notebooks/exploration.ipynb`

Plot outputs are written to `data/curated/plots/` as both interactive `html` and static `pdf`:
- solid matrices including `feces` are shown as `ppm` when the canonical unit is `mg/kg`
- blood and water remain `ug/L` in matrix-specific plots
- cross-layer comparison uses `ppm-equivalent`, with blood and water approximated as `ug/L / 1000`
- conceptual Sankey output uses layer medians only; it is not mass-balanced and does not model flux or exposure

## Documentation

- Provenance model: [docs/provenance_model.md](/home/trhova/metal-flux-db/docs/provenance_model.md)
- Normalization rules: [docs/normalization_rules.md](/home/trhova/metal-flux-db/docs/normalization_rules.md)
- Source notes: [docs/source_notes](/home/trhova/metal-flux-db/docs/source_notes)
