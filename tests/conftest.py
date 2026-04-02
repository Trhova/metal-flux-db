from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from cadmium_lake import io as io_module
from cadmium_lake import paths
from cadmium_lake import pipeline as pipeline_module
from cadmium_lake.qa import checks as qa_checks_module
from cadmium_lake.sources import base as base_module
from cadmium_lake.viz import plots as plots_module
from cadmium_lake.viz import views as views_module


@pytest.fixture(autouse=True)
def clean_data_dirs():
    test_data_dir = paths.REPO_ROOT / ".test-data"
    raw_dir = test_data_dir / "raw"
    staging_dir = test_data_dir / "staging"
    curated_dir = test_data_dir / "curated"

    original_data_dir = paths.DATA_DIR
    original_raw_dir = paths.RAW_DIR
    original_staging_dir = paths.STAGING_DIR
    original_curated_dir = paths.CURATED_DIR
    original_parquet_dir = paths.PARQUET_DIR
    original_qa_dir = paths.QA_DIR
    original_views_dir = paths.VIEWS_DIR
    original_plots_dir = paths.PLOTS_DIR
    original_logs_dir = paths.LOGS_DIR
    original_db_path = paths.DB_PATH
    original_io_db_path = io_module.DB_PATH
    original_io_parquet_dir = io_module.PARQUET_DIR
    original_pipeline_db_path = pipeline_module.DB_PATH
    original_qa_dir_module = qa_checks_module.QA_DIR
    original_base_raw_dir = base_module.RAW_DIR
    original_base_staging_dir = base_module.STAGING_DIR
    original_plots_dir_module = plots_module.PLOTS_DIR
    original_views_dir_module = views_module.VIEWS_DIR
    original_views_db_path = views_module.DB_PATH

    paths.DATA_DIR = test_data_dir
    paths.RAW_DIR = raw_dir
    paths.STAGING_DIR = staging_dir
    paths.CURATED_DIR = curated_dir
    paths.PARQUET_DIR = curated_dir / "parquet"
    paths.QA_DIR = curated_dir / "qa"
    paths.VIEWS_DIR = curated_dir / "views"
    paths.PLOTS_DIR = curated_dir / "plots"
    paths.LOGS_DIR = curated_dir / "logs"
    paths.DB_PATH = curated_dir / "cadmium_lake.duckdb"
    io_module.DB_PATH = paths.DB_PATH
    io_module.PARQUET_DIR = paths.PARQUET_DIR
    pipeline_module.DB_PATH = paths.DB_PATH
    qa_checks_module.QA_DIR = paths.QA_DIR
    base_module.RAW_DIR = paths.RAW_DIR
    base_module.STAGING_DIR = paths.STAGING_DIR
    plots_module.PLOTS_DIR = paths.PLOTS_DIR
    views_module.VIEWS_DIR = paths.VIEWS_DIR
    views_module.DB_PATH = paths.DB_PATH

    for path in [raw_dir, staging_dir, curated_dir]:
        if path.exists():
            shutil.rmtree(path)
    test_data_dir.mkdir(parents=True, exist_ok=True)
    yield
    paths.DATA_DIR = original_data_dir
    paths.RAW_DIR = original_raw_dir
    paths.STAGING_DIR = original_staging_dir
    paths.CURATED_DIR = original_curated_dir
    paths.PARQUET_DIR = original_parquet_dir
    paths.QA_DIR = original_qa_dir
    paths.VIEWS_DIR = original_views_dir
    paths.PLOTS_DIR = original_plots_dir
    paths.LOGS_DIR = original_logs_dir
    paths.DB_PATH = original_db_path
    io_module.DB_PATH = original_io_db_path
    io_module.PARQUET_DIR = original_io_parquet_dir
    pipeline_module.DB_PATH = original_pipeline_db_path
    qa_checks_module.QA_DIR = original_qa_dir_module
    base_module.RAW_DIR = original_base_raw_dir
    base_module.STAGING_DIR = original_base_staging_dir
    plots_module.PLOTS_DIR = original_plots_dir_module
    views_module.VIEWS_DIR = original_views_dir_module
    views_module.DB_PATH = original_views_db_path
    if test_data_dir.exists():
        shutil.rmtree(test_data_dir)
