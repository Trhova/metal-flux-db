# Cadmium Expansion Source Inventory

This inventory tracks reliable source families for future direct cadmium concentration ingestion. Include only measured concentration rows in the atlas; modeled exposure, intake, or risk outputs belong in `summary_measurements` or `review_queue`, not the direct concentration views.

## Water

- **Water Quality Portal (WQP)**: official USGS/EPA/National Water Quality Monitoring Council aggregator for physical/chemical water results. Implemented as `usgs_wqp_water` for bounded query windows.
  - Current WQP import uses filtered cadmium (`pCode=01025`) query windows for California, Arizona, Oregon, Washington, Idaho, and Texas across selected 1990-2026 windows, plus WQX/STORET-compatible `characteristicName=Cadmium` and `sampleMedia=Water` windows where those calls completed.
  - Local import after the 2026-04-24 expansion: 75,785 water samples and 16,907 normalized direct water concentration rows in `measurement_master_view`.
  - Some large WQP state/year chunks returned API timeouts or WQP 500s; those are intentionally skipped during fetch and can be retried as annual windows.
  - Current subtypes include groundwater, surface water, drinking water, irrigation water, and unspecified water. High values are retained as measured rows and surfaced through QA suspicious-value reports rather than dropped.
- **EPA STORET/WQX**: EPA water-quality exchange feeds are represented in WQP and can also be inventoried directly when source-specific metadata is needed.
- **EEA Waterbase Water Quality ICM**: implemented as `eea_waterbase_water` for direct disaggregated water sample rows queried from EEA Discodata/WISE SoE (`CAS_7440-43-9`, matrices `W` and `W-DIS`).
  - Current local import contributes 40,000 normalized direct European water concentration rows in `measurement_master_view` across Austria, Belgium, Czechia, France, Italy, and the United Kingdom.
  - Some Discodata country/year chunks time out or hang; imported chunk JSON files are preserved under `data/raw/eea_waterbase_water/` and can be expanded by adding smaller country/year windows.
- **FAO AQUASTAT**: useful pathway metadata for water/irrigation context, but concentration rows must be confirmed before inclusion as direct measurements.

## Crop

- Literature metadata APIs remain the discovery layer: Europe PMC, PubMed/PMC, OpenAlex, and DataCite.
- This layer is intentionally crop-focused for the human health pathway. It should not ingest general wild plant, ornamental plant, or non-food plant tissue measurements unless they are explicitly relevant edible crops or food-system inputs.
- Highest-priority crop themes: rice grain, wheat grain, leafy vegetables, potatoes, cocoa beans, edible crop tissue, and crop uptake studies with extractable measured concentrations.

## Feces

- Literature metadata APIs remain the discovery layer, but concentration ingestion is now handled by `feces_literature` so paper-derived values are clearly flagged as literature rather than API/database rows.
- Highest-priority themes: fecal cadmium concentration, stool cadmium `mg/kg`, and human feces cadmium.
- 2026-04-24 targeted check: the single previously imported feces row was removed because it was parsed from a maternal hair trace-element summary and mislabeled as stool.
- **Yabe et al. 2018 Kabwe children**: implemented from paper Table 3. Current import contributes 21 fecal cadmium concentration summary rows from 190 child fecal samples across Chowa, Kasanda, and Makululu. Units are `mg/kg` dry weight. `source_access_type` is `literature` and `source_retrieval_method` is `curated_pdf_and_article_extraction`.
- **Rose et al. 2015 human excreta review Table 5**: added two wet-weight feces concentration summary rows that are traceable to primary human-excreta studies: Schouw et al. 2002 Southern Thailand (`0.27 mg/kg`) and Vinneras et al. 2006 Swedish design values (`6.39 mg/kg`). These are flagged as `secondary_literature_review_table=true` and remain separate from API/database rows.
- Strong candidates for further manual/direct extraction include Tsuchiya & Iwao 1978 human food/feces/organ concentrations, Wang et al. 2012 Chinese male diet/blood/urine/feces concentrations, and newer stool ICP-MS/gut microbiome studies. Daily excretion outputs (`ug/day`) are explicitly excluded from direct concentration import unless the paper itself reports an actual feces concentration table.
- Excluded feces papers checked in the targeted pass: Bergback et al. 1994 Swedish women, Kjellstrom et al. 1978 Swedish feces, Nomiyama/Kikuchi Japanese volunteer studies, and battery-worker fecal elimination outputs where fecal cadmium is reported as `ug/day` rather than concentration.

## Food

- **FDA Total Diet Study**: implemented for US food concentration rows.
- **Health Canada Canadian Total Diet Study trace elements**: implemented for official Canadian food composite concentration rows from 1993-2018. Current import contributes 2,429 normalized direct cadmium food measurements with sample code, collection date/location, operator/LOD, method, project, and product metadata.
- **WHO GEMS/Food / FOSCOLLAB**: candidate global occurrence source.
- **EFSA seaweed and halophyte heavy-metal occurrence data**: implemented as `efsa_seaweed_occurrence` from the official EFSA Knowledge Junction/Zenodo raw occurrence workbook. Current import contributes 1,757 normalized direct cadmium food concentration rows and 2,093 traceable cadmium food records total, with non-detect rows retained in raw provenance but excluded from non-null concentration plots.
- **EFSA chemical occurrence data**: broader EU occurrence source remains a candidate beyond the seaweed/halophyte workbook now implemented.
- National total diet studies to inventory next: Japan, China, Australia/New Zealand, and additional EU member state surveys.

## Soil

- **USGS soil geochemical release**: implemented for US soil concentration rows.
- **LUCAS topsoil trace elements**: official EU source, but heavy-metals access is approval-gated.
- **GEMAS**: official European geochemistry source; public direct cadmium table access still needs a reproducible export path.
- **FOREGS/EuroGeoSurveys Geochemical Atlas of Europe**: implemented as `foregs_geochemical_atlas_soil` from the official GTK raw analytical ZIP downloads. Current import contributes 1,623 normalized direct soil concentration rows in `measurement_master_view`: 840 topsoil and 783 subsoil Cd `mg/kg` rows across 26 countries, with GTN sample IDs, coordinates, country codes, method-file provenance, and detection-limit notes.
- **Geological Survey Ireland Dublin SURGE**: implemented for official Dublin urban topsoil cadmium feature rows exposed through an ArcGIS REST layer. Current import contributes 1,058 normalized direct soil concentration rows in `measurement_master_view`.
- **EEA/JRC heavy-metal concentration polygons**: identified but not imported into the direct atlas because the exposed ArcGIS layer is a NUTS/agricultural-soil polygon product, not sample-level measured rows. It can be staged separately as an aggregated/context layer if needed.

## Human Biomonitoring

- **CDC NHANES**: implemented for US blood cadmium rows. Current import covers public whole-blood cadmium files from 1999-2023 and contributes 88,564 normalized direct blood concentration rows in `measurement_master_view`.
- **HBM4EU/PARC**: inventoried as summary/dashboard outputs.
- **CHMS Canada**: candidate source; row-level health microdata may require controlled access.
- Additional national surveys should be added only when direct blood/urine concentration rows or defensible public summaries are available.

## Fertilizer

- **Washington State fertilizer product database**: implemented for product-level cadmium rows. Current expanded import fetched the full available product-detail set and contributes 9,357 normalized fertilizer concentration rows in `measurement_master_view`.
- The WSDA detail endpoint is slow because every product requires a separate request. The adapter now checkpoints `fertilizer_detail.json` while fetching and can resume. Set `CADMIUM_LAKE_WSDA_DETAIL_LIMIT` to cap a run, or leave it unset to continue toward the full product list.

## Current Direct-Row Gaps

- **Crop and feces** remain sparse without broader literature table extraction. The repo currently stores literature/API discovery metadata and a few curated direct records, but direct crop/feces concentration data at scale will require either structured supplementary tables or controlled/manual extraction from papers.
- **Non-US blood biomonitoring** is mostly summary or controlled-access. These sources should be inventoried, but row-level import requires datasets that expose direct concentrations with sample-level metadata.
- **European official soil at EU scale** is partially covered by FOREGS now. LUCAS heavy metals remains approval-gated and GEMAS still needs a reproducible public cadmium export path.
