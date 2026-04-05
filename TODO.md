# Next Steps

- Add `water` as a direct-measurement matrix with canonical `ug/L` and ppm-equivalent visualization.
- Implement water ingestion starting with the USGS Water Quality Portal, then evaluate EEA water datasets and other official sources.
- Expand plant literature mining through Europe PMC, PubMed/PMC, OpenAlex, and DataCite, prioritizing direct concentration tables and supplements.
- Expand feces/stool literature mining through the same APIs, prioritizing direct concentration measurements with source-table provenance.
- Add fertilizer sources beyond Washington, especially phosphate fertilizer cadmium and heavy-metals product datasets.
- Expand food geography with WHO GEMS/Food, EFSA, and additional national total diet study datasets.
- Expand human biomonitoring coverage with HBM4EU/PARC, CHMS, KNHANES, and literature-backed blood cadmium studies.
- Improve year coverage across all direct measurements by filling `collection_year`, `publication_year`, and `year_for_plotting`.
- Update cross-layer plots and views to include `water` while keeping solids in ppm and liquids in `ug/L` canonically.
- Add a conceptual median-based Sankey later for storytelling, without implying a mass-balanced flux model.
- Keep the atlas limited to measured, traceable, correctly normalized direct concentrations.
