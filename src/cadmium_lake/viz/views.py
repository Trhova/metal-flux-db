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
          COUNT(DISTINCT sam.sample_id) AS sample_count,
          COUNT(DISTINCT raw.measurement_id) AS measurement_count,
          MIN(st.year_start) AS min_year,
          MAX(st.year_end) AS max_year,
          COUNT(DISTINCT sam.country) AS country_count
        FROM sources src
        LEFT JOIN samples sam USING (source_id)
        LEFT JOIN measurements_raw raw USING (sample_id)
        LEFT JOIN studies_or_batches st USING (study_id)
        GROUP BY 1, 2
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
          s.matrix_group AS layer,
          COUNT(DISTINCT s.sample_id) AS samples,
          COUNT(DISTINCT r.measurement_id) AS measurements,
          COUNT(DISTINCT n.measurement_id) FILTER (WHERE n.canonical_value IS NOT NULL) AS normalized_measurements
        FROM samples s
        LEFT JOIN measurements_raw r USING (sample_id)
        LEFT JOIN measurements_normalized n USING (measurement_id)
        GROUP BY 1
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
