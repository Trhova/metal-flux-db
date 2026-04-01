from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIGS_DIR = REPO_ROOT / "configs"
DATA_DIR = REPO_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
CURATED_DIR = DATA_DIR / "curated"
PARQUET_DIR = CURATED_DIR / "parquet"
QA_DIR = CURATED_DIR / "qa"
VIEWS_DIR = CURATED_DIR / "views"
PLOTS_DIR = CURATED_DIR / "plots"
LOGS_DIR = CURATED_DIR / "logs"
DB_PATH = CURATED_DIR / "cadmium_lake.duckdb"


def ensure_directories() -> None:
    for path in [
        RAW_DIR,
        STAGING_DIR,
        CURATED_DIR,
        PARQUET_DIR,
        QA_DIR,
        VIEWS_DIR,
        PLOTS_DIR,
        LOGS_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)
