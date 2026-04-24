# Graph Report - metal-flux-db  (2026-04-24)

## Corpus Check
- 35 files · ~18,627 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 287 nodes · 822 edges · 13 communities detected
- Extraction: 57% EXTRACTED · 43% INFERRED · 0% AMBIGUOUS · INFERRED: 354 edges (avg confidence: 0.77)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]

## God Nodes (most connected - your core abstractions)
1. `ParsedPayload` - 34 edges
2. `stable_id()` - 31 edges
3. `BaseAdapter` - 29 edges
4. `LiteratureSearchAdapter` - 20 edges
5. `StudyRecord` - 20 edges
6. `initialize_catalog_tables()` - 19 edges
7. `test_normalize_qa_and_views()` - 18 edges
8. `_download()` - 17 edges
9. `SampleRecord` - 16 edges
10. `RawMeasurementRecord` - 16 edges

## Surprising Connections (you probably didn't know these)
- `extract_hbm4eu_cadmium_summaries()` --calls--> `test_hbm4eu_summary_extractor()`  [INFERRED]
  src/cadmium_lake/sources/europe.py → tests/test_pipeline.py
- `initialize_catalog_tables()` --calls--> `test_parse_smoke_for_all_sources()`  [INFERRED]
  src/cadmium_lake/pipeline.py → tests/test_pipeline.py
- `initialize_catalog_tables()` --calls--> `test_normalize_qa_and_views()`  [INFERRED]
  src/cadmium_lake/pipeline.py → tests/test_pipeline.py
- `parse_sources()` --calls--> `test_parse_smoke_for_all_sources()`  [INFERRED]
  src/cadmium_lake/pipeline.py → tests/test_pipeline.py
- `parse_sources()` --calls--> `test_normalize_qa_and_views()`  [INFERRED]
  src/cadmium_lake/pipeline.py → tests/test_pipeline.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.08
Nodes (17): BaseAdapter, _download(), BaseAdapter, EsdacLucasSoilAdapter, ForegsGeochemicalAtlasSoilAdapter, GemasSoilAdapter, Hbm4euParcCadmiumAdapter, UkFsaTotalDietAdapter (+9 more)

### Community 1 - "Community 1"
Cohesion: 0.14
Nodes (21): ParsedPayload, BaseModel, extract_hbm4eu_cadmium_summaries(), parse_numeric_text(), CuratedFecesLiteratureAdapter, FecesConcentrationSummary, FecesStat, clean_text() (+13 more)

### Community 2 - "Community 2"
Cohesion: 0.12
Nodes (30): run_qa_checks(), _write(), AnalysisPolicy, fetch(), literature_search(), main(), normalize(), parse() (+22 more)

### Community 3 - "Community 3"
Cohesion: 0.14
Nodes (15): build_views_command(), FdaTdsAdapter, infer_collection_year(), load_cadmium_rows(), try_float(), try_int(), clean_id(), try_float() (+7 more)

### Community 4 - "Community 4"
Cohesion: 0.23
Nodes (18): seed_canada_fixture(), seed_eea_water_fixture(), seed_efsa_seaweed_fixture(), seed_fda_fixture(), seed_feces_literature_fixture(), seed_foregs_fixture(), seed_gsi_fixture(), seed_nhanes_fixture() (+10 more)

### Community 5 - "Community 5"
Cohesion: 0.23
Nodes (6): is_downloadable_supplement(), LiteratureSearchAdapter, normalize_doi(), normalize_title(), safe_filename(), safe_int()

### Community 6 - "Community 6"
Cohesion: 0.31
Nodes (19): build_basic_plots(), _comparison_pdf(), eligible_time_layers(), ordered_layers(), _plot_conceptual_sankey(), _plot_country_coverage_interactive(), _plot_coverage_static(), _plot_layer_comparison_interactive() (+11 more)

### Community 7 - "Community 7"
Cohesion: 0.19
Nodes (15): build_partial_date(), clean_text(), EfsaSeaweedOccurrenceAdapter, extract_tableau_metadata(), extract_uk_fsa_cadmium_rows(), inspect_gemas_public_services(), inspect_lucas_public_zip(), list_public_gemas_services() (+7 more)

### Community 8 - "Community 8"
Cohesion: 0.23
Nodes (14): test_basis_normalization(), test_blood_unit_identity(), test_feces_ugkg_to_mgkg(), test_food_ppb_to_mgkg(), test_mgkg_identity(), test_ng_per_g_to_ug_per_kg_equivalent_path(), test_ppm_only_for_solids(), test_ug_per_g_to_mg_per_kg() (+6 more)

### Community 9 - "Community 9"
Cohesion: 0.36
Nodes (9): test_water_subtype_classifier(), classify_eea_water_subtype(), classify_water_subtype(), clean_text(), infer_year(), load_wqp_csv(), normalize_liquid_unit(), try_float() (+1 more)

### Community 10 - "Community 10"
Cohesion: 0.48
Nodes (5): analytes_config(), load_yaml(), matrix_taxonomy(), source_catalog(), unit_mappings()

### Community 11 - "Community 11"
Cohesion: 0.62
Nodes (5): clean_text(), HealthCanadaTdsTraceElementsAdapter, infer_year(), normalize_unit(), try_float()

### Community 12 - "Community 12"
Cohesion: 0.4
Nodes (1): DuckDB views and plot builders.

## Knowledge Gaps
- **Thin community `Community 12`** (5 nodes): `DuckDB views and plot builders.`, `__init__.py`, `__init__.py`, `__init__.py`, `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `run_normalization()` connect `Community 2` to `Community 8`, `Community 4`?**
  _High betweenness centrality (0.112) - this node is a cross-community bridge._
- **Why does `read_duckdb_table()` connect `Community 2` to `Community 3`, `Community 4`, `Community 6`?**
  _High betweenness centrality (0.101) - this node is a cross-community bridge._
- **Why does `normalize_measurement()` connect `Community 8` to `Community 2`?**
  _High betweenness centrality (0.097) - this node is a cross-community bridge._
- **Are the 50 inferred relationships involving `str` (e.g. with `stable_id()` and `initialize_catalog_tables()`) actually correct?**
  _`str` has 50 INFERRED edges - model-reasoned connections that need verification._
- **Are the 33 inferred relationships involving `ParsedPayload` (e.g. with `EsdacLucasSoilAdapter` and `GemasSoilAdapter`) actually correct?**
  _`ParsedPayload` has 33 INFERRED edges - model-reasoned connections that need verification._
- **Are the 30 inferred relationships involving `stable_id()` (e.g. with `str` and `.parse()`) actually correct?**
  _`stable_id()` has 30 INFERRED edges - model-reasoned connections that need verification._
- **Are the 17 inferred relationships involving `BaseAdapter` (e.g. with `EsdacLucasSoilAdapter` and `GemasSoilAdapter`) actually correct?**
  _`BaseAdapter` has 17 INFERRED edges - model-reasoned connections that need verification._