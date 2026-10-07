"""Single source of truth for project paths."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
RAW = DATA / "raw"
LANDING = DATA / "landing" / "api"
BRONZE = DATA / "bronze"
SILVER = DATA / "silver"
GOLD = DATA / "gold"
QUARANTINE = DATA / "quarantine"
WAREHOUSE = DATA / "warehouse"
DB_PATH = WAREHOUSE / "healthcare.duckdb"

for _p in (RAW, LANDING, BRONZE, SILVER, GOLD, QUARANTINE, WAREHOUSE):
    _p.mkdir(parents=True, exist_ok=True)

# Story "as of" date: the extract snapshot date used throughout the project.
SNAPSHOT_DATE = "2025-07-01"
