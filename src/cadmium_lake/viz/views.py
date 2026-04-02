from __future__ import annotations

import duckdb

from cadmium_lake.paths import DB_PATH, VIEWS_DIR


VIEW_SQL = {
    "layer_distribution_view": """
        CREATE OR REPLACE VIEW layer_distribution_view AS
        SELECT
          s.matrix_group AS layer,
          n.canonical_unit,
          n.canonical_value,
          CASE
            WHEN s.matrix_group IN ('fertilizer', 'soil', 'plant', 'food') AND n.canonical_unit = 'mg/kg' THEN n.canonical_value
            WHEN s.matrix_group = 'gut' AND n.canonical_unit = 'fraction' THEN n.canonical_value * 100.0
            ELSE n.canonical_value
          END AS display_value,
          CASE
            WHEN s.matrix_group IN ('fertilizer', 'soil', 'plant', 'food') AND n.canonical_unit = 'mg/kg' THEN 'ppm'
            WHEN s.matrix_group = 'gut' AND n.canonical_unit = 'fraction' THEN 'bioaccessible %'
            WHEN n.canonical_unit = 'ug/kg_bw/day' THEN 'ug/kg bw/day'
            ELSE n.canonical_unit
          END AS display_unit,
          CASE WHEN n.canonical_value > 0 THEN log10(n.canonical_value) ELSE NULL END AS log10_canonical_value
        FROM measurements_normalized n
        JOIN measurements_raw r USING (measurement_id)
        JOIN samples s USING (sample_id)
        WHERE n.canonical_value IS NOT NULL
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
    "food_gut_view": """
        CREATE OR REPLACE VIEW food_gut_view AS
        SELECT e.edge_id, e.relationship_type, s1.sample_id AS food_sample_id, s2.sample_id AS gut_sample_id
        FROM linkage_edges e
        JOIN samples s1 ON e.from_sample_id = s1.sample_id
        JOIN samples s2 ON e.to_sample_id = s2.sample_id
        WHERE s1.matrix_group = 'food' AND s2.matrix_group = 'gut'
    """,
    "gut_blood_view": """
        CREATE OR REPLACE VIEW gut_blood_view AS
        SELECT e.edge_id, e.relationship_type, s1.sample_id AS gut_sample_id, s2.sample_id AS blood_sample_id
        FROM linkage_edges e
        JOIN samples s1 ON e.from_sample_id = s1.sample_id
        JOIN samples s2 ON e.to_sample_id = s2.sample_id
        WHERE s1.matrix_group = 'gut' AND s2.matrix_group = 'blood'
    """,
    "chain_summary_view": """
        CREATE OR REPLACE VIEW chain_summary_view AS
        SELECT
          layer,
          COALESCE(samples, 0) AS samples,
          COALESCE(measurements, 0) AS measurements,
          COALESCE(normalized_measurements, 0) AS normalized_measurements,
          COALESCE(summary_measurements, 0) AS summary_measurements
        FROM (
          SELECT
            matrix_group AS layer,
            COUNT(DISTINCT sample_id) AS samples,
            COUNT(DISTINCT measurement_id) AS measurements,
            COUNT(DISTINCT measurement_id) FILTER (WHERE canonical_value IS NOT NULL) AS normalized_measurements
          FROM samples
          LEFT JOIN measurements_raw USING (sample_id)
          LEFT JOIN measurements_normalized USING (measurement_id)
          GROUP BY 1
        ) base
        FULL OUTER JOIN (
          SELECT matrix_group AS layer, COUNT(DISTINCT summary_measurement_id) AS summary_measurements
          FROM summary_measurements
          GROUP BY 1
        ) summary USING (layer)
        ORDER BY 1
    """,
}


def build_views() -> None:
    VIEWS_DIR.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(DB_PATH)) as conn:
        for name, sql in VIEW_SQL.items():
            conn.execute(sql)
            csv_path = VIEWS_DIR / f"{name}.csv"
            conn.execute(f"COPY (SELECT * FROM {name}) TO '{csv_path}' (HEADER, DELIMITER ',')")
