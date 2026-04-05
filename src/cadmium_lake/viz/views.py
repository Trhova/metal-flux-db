from __future__ import annotations

import duckdb

from cadmium_lake.paths import DB_PATH, VIEWS_DIR


DIRECT_MEASUREMENT_BASE = """
    WITH direct_measurements AS (
      SELECT
        n.measurement_id,
        s.sample_id,
        s.source_id,
        src.source_name,
        s.study_id,
        st.study_title,
        st.citation,
        st.doi,
        s.collection_year,
        COALESCE(s.publication_year, st.publication_year, st.year_start, st.year_end) AS publication_year,
        COALESCE(
          s.year_for_plotting,
          s.collection_year,
          s.publication_year,
          st.publication_year,
          st.year_start,
          st.year_end
        ) AS year_for_plotting,
        COALESCE(
          s.year_for_plotting_source,
          CASE
            WHEN s.collection_year IS NOT NULL THEN 'collection_year'
            WHEN s.publication_year IS NOT NULL OR st.publication_year IS NOT NULL OR st.year_start IS NOT NULL OR st.year_end IS NOT NULL
              THEN 'publication_year'
            ELSE NULL
          END
        ) AS year_for_plotting_source,
        COALESCE(s.country, st.country) AS country,
        s.matrix_group AS layer,
        s.matrix_subtype,
        s.sample_name,
        s.specimen_or_part,
        s.dry_wet_basis,
        s.location_name,
        r.raw_value,
        r.raw_value_text,
        r.raw_unit,
        r.raw_basis_text,
        r.page_or_sheet,
        r.table_or_figure,
        r.row_label,
        r.column_label,
        r.extraction_method,
        r.confidence_score,
        n.canonical_value,
        n.canonical_unit,
        n.canonical_dimension,
        n.conversion_rule,
        n.converted_from_unit,
        n.normalized_basis,
        CASE
          WHEN s.matrix_group IN ('fertilizer', 'soil', 'plant', 'food', 'feces') AND n.canonical_unit = 'mg/kg' THEN n.canonical_value
          WHEN s.matrix_group = 'blood' AND n.canonical_unit = 'ug/L' THEN n.canonical_value / 1000.0
          ELSE NULL
        END AS ppm_equivalent,
        CASE
          WHEN s.matrix_group IN ('fertilizer', 'soil', 'plant', 'food', 'feces') AND n.canonical_unit = 'mg/kg' THEN n.canonical_value
          WHEN s.matrix_group = 'blood' AND n.canonical_unit = 'ug/L' THEN n.canonical_value
          ELSE n.canonical_value
        END AS display_value,
        CASE
          WHEN s.matrix_group IN ('fertilizer', 'soil', 'plant', 'food', 'feces') AND n.canonical_unit = 'mg/kg' THEN 'ppm'
          WHEN s.matrix_group = 'blood' AND n.canonical_unit = 'ug/L' THEN 'ug/L'
          ELSE n.canonical_unit
        END AS display_unit
      FROM measurements_normalized n
      JOIN measurements_raw r USING (measurement_id)
      JOIN samples s USING (sample_id)
      LEFT JOIN studies_or_batches st USING (study_id)
      LEFT JOIN sources src ON s.source_id = src.source_id
      WHERE n.canonical_value IS NOT NULL
    )
"""


VIEW_SQL = {
    "layer_comparison_view": f"""
        CREATE OR REPLACE VIEW layer_comparison_view AS
        {DIRECT_MEASUREMENT_BASE}
        SELECT
          measurement_id,
          sample_id,
          source_id,
          source_name,
          study_id,
          study_title,
          citation,
          doi,
          collection_year,
          publication_year,
          year_for_plotting,
          year_for_plotting_source,
          country,
          layer,
          matrix_subtype,
          sample_name,
          specimen_or_part,
          dry_wet_basis,
          location_name,
          raw_value,
          raw_value_text,
          raw_unit,
          raw_basis_text,
          page_or_sheet,
          table_or_figure,
          row_label,
          column_label,
          extraction_method,
          confidence_score,
          canonical_value,
          canonical_unit,
          canonical_dimension,
          conversion_rule,
          converted_from_unit,
          normalized_basis,
          ppm_equivalent,
          CASE WHEN ppm_equivalent > 0 THEN log10(ppm_equivalent) END AS log10_ppm_equivalent,
          display_value,
          display_unit
        FROM direct_measurements
        WHERE ppm_equivalent IS NOT NULL
        ORDER BY layer, ppm_equivalent
    """,
    "layer_distribution_view": """
        CREATE OR REPLACE VIEW layer_distribution_view AS
        SELECT *
        FROM layer_comparison_view
    """,
    "time_trend_view": f"""
        CREATE OR REPLACE VIEW time_trend_view AS
        {DIRECT_MEASUREMENT_BASE}
        SELECT
          measurement_id,
          sample_id,
          source_id,
          source_name,
          study_id,
          study_title,
          citation,
          doi,
          collection_year,
          publication_year,
          year_for_plotting,
          year_for_plotting_source,
          country,
          layer,
          matrix_subtype,
          canonical_unit,
          display_value,
          display_unit,
          ppm_equivalent,
          raw_value_text,
          raw_unit,
          page_or_sheet,
          table_or_figure
        FROM direct_measurements
        WHERE year_for_plotting IS NOT NULL
          AND display_value IS NOT NULL
    """,
    "source_coverage_view": """
        CREATE OR REPLACE VIEW source_coverage_view AS
        SELECT
          src.source_id,
          src.layer,
          COALESCE(sam.sample_count, 0) AS sample_count,
          COALESCE(raw.measurement_count, 0) AS measurement_count,
          COALESCE(sm.summary_measurement_count, 0) AS summary_measurement_count,
          yrs.min_year,
          yrs.max_year,
          COALESCE(sam.country_count, 0) AS country_count
        FROM sources src
        LEFT JOIN (
          SELECT source_id, COUNT(DISTINCT sample_id) AS sample_count, COUNT(DISTINCT country) AS country_count
          FROM samples
          GROUP BY 1
        ) sam USING (source_id)
        LEFT JOIN (
          SELECT source_id, COUNT(DISTINCT measurement_id) AS measurement_count
          FROM measurements_raw
          JOIN samples USING (sample_id)
          GROUP BY 1
        ) raw USING (source_id)
        LEFT JOIN (
          SELECT source_id, COUNT(DISTINCT summary_measurement_id) AS summary_measurement_count
          FROM summary_measurements
          GROUP BY 1
        ) sm USING (source_id)
        LEFT JOIN (
          SELECT source_id, MIN(year_start) AS min_year, MAX(year_end) AS max_year
          FROM studies_or_batches
          GROUP BY 1
        ) yrs USING (source_id)
    """,
    "soil_plant_pairs_view": """
        CREATE OR REPLACE VIEW soil_plant_pairs_view AS
        SELECT e.edge_id, e.relationship_type, s1.sample_id AS soil_sample_id, s2.sample_id AS plant_sample_id
        FROM linkage_edges e
        JOIN samples s1 ON e.from_sample_id = s1.sample_id
        JOIN samples s2 ON e.to_sample_id = s2.sample_id
        WHERE s1.matrix_group = 'soil' AND s2.matrix_group = 'plant'
    """,
    "plant_food_pairs_view": """
        CREATE OR REPLACE VIEW plant_food_pairs_view AS
        SELECT e.edge_id, e.relationship_type, s1.sample_id AS plant_sample_id, s2.sample_id AS food_sample_id
        FROM linkage_edges e
        JOIN samples s1 ON e.from_sample_id = s1.sample_id
        JOIN samples s2 ON e.to_sample_id = s2.sample_id
        WHERE s1.matrix_group = 'plant' AND s2.matrix_group = 'food'
    """,
    "food_feces_view": """
        CREATE OR REPLACE VIEW food_feces_view AS
        SELECT e.edge_id, e.relationship_type, s1.sample_id AS food_sample_id, s2.sample_id AS feces_sample_id
        FROM linkage_edges e
        JOIN samples s1 ON e.from_sample_id = s1.sample_id
        JOIN samples s2 ON e.to_sample_id = s2.sample_id
        WHERE s1.matrix_group = 'food' AND s2.matrix_group = 'feces'
    """,
    "feces_blood_view": """
        CREATE OR REPLACE VIEW feces_blood_view AS
        SELECT e.edge_id, e.relationship_type, s1.sample_id AS feces_sample_id, s2.sample_id AS blood_sample_id
        FROM linkage_edges e
        JOIN samples s1 ON e.from_sample_id = s1.sample_id
        JOIN samples s2 ON e.to_sample_id = s2.sample_id
        WHERE s1.matrix_group = 'feces' AND s2.matrix_group = 'blood'
    """,
    "chain_summary_view": """
        CREATE OR REPLACE VIEW chain_summary_view AS
        SELECT
          matrix_group AS layer,
          COUNT(DISTINCT sample_id) AS samples,
          COUNT(DISTINCT measurement_id) AS measurements,
          COUNT(DISTINCT measurement_id) FILTER (WHERE canonical_value IS NOT NULL) AS normalized_measurements
        FROM samples
        LEFT JOIN measurements_raw USING (sample_id)
        LEFT JOIN measurements_normalized USING (measurement_id)
        GROUP BY 1
        ORDER BY 1
    """,
}


def build_views() -> None:
    VIEWS_DIR.mkdir(parents=True, exist_ok=True)
    allowed = {f"{name}.csv" for name in VIEW_SQL}
    for path in VIEWS_DIR.glob("*.csv"):
        if path.name not in allowed:
            path.unlink()
    with duckdb.connect(str(DB_PATH)) as conn:
        for name, sql in VIEW_SQL.items():
            conn.execute(sql)
            csv_path = VIEWS_DIR / f"{name}.csv"
            conn.execute(f"COPY (SELECT * FROM {name}) TO '{csv_path}' (HEADER, DELIMITER ',')")
