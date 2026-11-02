"""Small end-to-end tests for the local (DuckDB) version of the pipeline.

They replay the captured fixtures -> raw lake -> bronze/silver/gold and assert
the properties the resume story depends on. No Databricks workspace needed.
Run with: uv run pytest
"""

import json
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
DEMO_DATES = ["2026-10-15", "2026-10-20"]


def _run(script: str, *args: str) -> None:
    subprocess.run([sys.executable, str(ROOT / "scripts" / script), *args], check=True)


def _replay(raw_dir: Path, date: str) -> None:
    _run(
        "ingest.py",
        "--date",
        date,
        "--from-fixtures",
        str(ROOT / "fixtures/raw"),
        "--out",
        str(raw_dir),
    )


def _build(raw_dir: Path, db_path: Path, app_path: Path) -> duckdb.DuckDBPyConnection:
    _run(
        "build_model.py",
        "--raw",
        str(raw_dir),
        "--db",
        str(db_path),
        "--app-out",
        str(app_path),
    )
    return duckdb.connect(str(db_path))


def _demo_db(tmp_path: Path) -> tuple[duckdb.DuckDBPyConnection, Path]:
    raw_dir = tmp_path / "raw"
    for date in DEMO_DATES:
        _replay(raw_dir, date)
    app_path = tmp_path / "app" / "farm_daily.parquet"
    return _build(raw_dir, tmp_path / "leher.duckdb", app_path), app_path


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple]:
    return con.sql(sql).fetchall()


def test_unified_table_has_one_row_per_farm_per_day(tmp_path):
    con, app_path = _demo_db(tmp_path)

    assert _rows(con, "SELECT count(*) FROM gold_farm_daily")[0][0] == 10  # 5 farms x 2 days
    assert (
        _rows(
            con,
            "SELECT count(*) FROM (SELECT farm_id, obs_date FROM gold_farm_daily GROUP BY ALL HAVING count(*) > 1)",
        )[0][0]
        == 0
    )
    observed_dates = sorted(
        str(r[0]) for r in _rows(con, "SELECT DISTINCT obs_date FROM gold_farm_daily")
    )
    assert observed_dates == DEMO_DATES

    # Every row is keyed by (farm_id, obs_date) and carries the farm dimension.
    assert (
        _rows(con, "SELECT count(*) FROM gold_farm_daily WHERE crop IS NULL OR soil_type IS NULL")[
            0
        ][0]
        == 0
    )

    # Quantities that failed a rule are in quarantine with a reason, not dropped.
    assert _rows(con, "SELECT count(*) FROM quarantine")[0][0] == 6
    reasons = dict(_rows(con, "SELECT reason, count(*) FROM quarantine GROUP BY reason"))
    assert reasons == {"sentinel_missing_value": 5, "rain_out_of_range": 1}

    # The app contract was exported as a small parquet mirror.
    assert app_path.exists()
    assert _rows(con, f"SELECT count(*) FROM read_parquet('{app_path}')")[0][0] == 10


def test_freshness_fallback_is_visible_and_attributed(tmp_path):
    con, _ = _demo_db(tmp_path)

    # 2026-10-20: NASA POWER had not published that day (-999 fill value), so the
    # agro signal is quarantined and gold falls back to the latest known value.
    rows = _rows(
        con,
        """
        SELECT farm_id, weather_is_stale, agro_as_of, agro_is_stale, data_status
        FROM gold_farm_daily WHERE obs_date = '2026-10-20' ORDER BY farm_id
        """,
    )
    assert len(rows) == 5
    for farm_id, weather_stale, agro_as_of, agro_stale, status in rows:
        assert weather_stale is False  # Open-Meteo answered for the day
        assert status == "degraded"  # never silently promoted to "complete"
        if farm_id == "F003":
            # F003's 2026-10-15 agro row had impossible rain (678 mm) and was
            # quarantined, so there is no earlier value to fall back to.
            assert agro_as_of is None and agro_stale is True
        else:
            assert str(agro_as_of) == "2026-10-15"
            assert agro_stale is True


def test_rerun_keeps_one_row_and_latest_revision_wins(tmp_path):
    raw_dir = tmp_path / "raw"
    for date in DEMO_DATES:
        _replay(raw_dir, date)
    con = _build(raw_dir, tmp_path / "leher.duckdb", tmp_path / "app.parquet")

    before = _rows(
        con, "SELECT temp_max_c FROM gold_farm_daily WHERE obs_date='2026-10-15' AND farm_id='F001'"
    )[0][0]
    con.close()  # release DuckDB's per-path instance cache before rebuilding

    # Simulate an afternoon re-ingest of the same day: a second raw revision with
    # a later fetched_at. Gold must still have one row per farm/day, latest wins.
    raw_file = next((raw_dir / "source=weather/date=2026-10-15").glob("*.json"))
    revision = json.loads(raw_file.read_text())
    from datetime import datetime, timedelta

    original_fetch = datetime.fromisoformat(revision["_meta"]["fetched_at"])
    revision["_meta"]["fetched_at"] = (original_fetch + timedelta(days=1)).isoformat()
    revision["farms"][0]["response"]["daily"]["temperature_2m_max"] = [33.3]
    (raw_file.parent / "weather_revision_2.json").write_text(json.dumps(revision))

    con = _build(raw_dir, tmp_path / "leher.duckdb", tmp_path / "app.parquet")
    rows = _rows(
        con,
        "SELECT temp_max_c FROM gold_farm_daily WHERE obs_date='2026-10-15' AND farm_id='F001'",
    )
    assert len(rows) == 1
    assert rows[0][0] == pytest.approx(33.3)
    assert rows[0][0] != before
    assert _rows(con, "SELECT count(*) FROM gold_farm_daily")[0][0] == 10
