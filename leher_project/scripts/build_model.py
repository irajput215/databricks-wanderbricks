#!/usr/bin/env python3
"""Local mirror of the two Lakeflow pipelines, on DuckDB (no Spark needed).

It implements exactly the same medallion contract as
``pipelines/farm_conditions/farm_conditions.py``:

    RAW files  ->  bronze_*  ->  silver_* (+ quarantine)  ->  gold_farm_daily
                                                             ->  app_farm_daily

Why a mirror instead of running Spark locally? So `make demo` works in seconds
without a Databricks workspace. On Databricks the same tables are built by
Spark Declarative Pipelines (see docs/DATA_MODEL.md for the layer contract).

Usage:
    python scripts/build_model.py                    # data/raw -> data/leher.duckdb
    python scripts/build_model.py --raw fixtures/raw --db /tmp/x.duckdb
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

# Explicit JSON schemas: the raw envelope is parsed in silver, not at ingestion,
# so the exact API payload stays in bronze and can be replayed later.
W_SCHEMA = """{"_meta":{"source":"VARCHAR","obs_date":"VARCHAR","fetched_at":"VARCHAR"},
 "farms":[{"farm_id":"VARCHAR","response":{"daily":{
   "time":["VARCHAR"],"temperature_2m_max":["DOUBLE"],"temperature_2m_min":["DOUBLE"],
   "temperature_2m_mean":["DOUBLE"],"precipitation_sum":["DOUBLE"],
   "wind_speed_10m_max":["DOUBLE"],"relative_humidity_2m_mean":["DOUBLE"],
   "et0_fao_evapotranspiration":["DOUBLE"]}}}]}"""

A_SCHEMA = """{"_meta":{"source":"VARCHAR","obs_date":"VARCHAR","fetched_at":"VARCHAR"},
 "farms":[{"farm_id":"VARCHAR","response":{"properties":{"parameter":{
   "T2M":"MAP(VARCHAR, DOUBLE)","PRECTOTCORR":"MAP(VARCHAR, DOUBLE)",
   "RH2M":"MAP(VARCHAR, DOUBLE)","GWETROOT":"MAP(VARCHAR, DOUBLE)"}}}}]}"""

S_SCHEMA = """{"_meta":{"source":"VARCHAR","obs_date":"VARCHAR","fetched_at":"VARCHAR"},
 "farms":[{"farm_id":"VARCHAR","response":{"features":[
   {"id":"VARCHAR","collection":"VARCHAR","datetime":"VARCHAR","cloud_cover":"DOUBLE"}]}}]}"""

FARM_COLS = (
    "farm_id, farmer_name, crop, district, state, lat, lon, area_ha, soil_type, "
    "registered_on, updated_at"
)

# Sanity bounds + freshness windows. In production these live in a config table
# per region/season; for the demo they are constants you can explain in one line.
LIMITS = {
    "temp_min_c": (-20.0, 60.0),
    "temp_max_c": (-20.0, 60.0),
    "rain_mm": (0.0, 500.0),
    "wind_max_kmh": (0.0, 250.0),
    "humidity_pct": (0.0, 100.0),
}
WEATHER_LOOKBACK_DAYS = 1  # yesterday's conditions are acceptable, marked stale
AGRO_LOOKBACK_DAYS = 7  # soil wetness moves slowly, a week back is fine
SATELLITE_LOOKBACK_DAYS = 7  # Sentinel-2 revisit is ~5 days
SATELLITE_CLOUD_MAX = 40.0  # a scene cloudier than this is not usable for weeds


def _raw_glob(raw: Path, source: str) -> str:
    return str(raw / f"source={source}" / "date=*" / "*")


def build(raw_dir: Path, db_path: Path) -> duckdb.DuckDBPyConnection:
    """Build all layers in a fresh DuckDB file and return the connection."""
    for source in ("weather", "agro", "satellite", "farms"):
        if not list((raw_dir / f"source={source}").glob("date=*")):
            raise SystemExit(
                f"no raw data for source={source} under {raw_dir}. "
                "Run `make demo` (offline fixtures) or `make ingest DATE=...` first."
            )

    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.unlink(missing_ok=True)
    con = duckdb.connect(str(db_path))
    glob = lambda source: _raw_glob(raw_dir, source)  # noqa: E731

    # ---------------------------------------------------------------- bronze
    # Raw layer: the exact file content as landed, plus lineage (file path and
    # the ingestion date partition). Nothing is parsed or dropped here.
    for source in ("weather", "agro", "satellite"):
        con.execute(
            f"""
            CREATE OR REPLACE TABLE bronze_{source} AS
            SELECT
                filename AS _source_file,
                content  AS _value,
                regexp_extract(filename, 'date=([0-9-]+)', 1) AS _partition_date
            FROM read_text('{glob(source)}')
            """
        )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE bronze_farms AS
        SELECT {FARM_COLS}, filename AS _source_file,
               regexp_extract(filename, 'date=([0-9-]+)', 1) AS _partition_date
        FROM read_csv_auto('{glob("farms")}', filename = true, union_by_name = true)
        """
    )

    # ------------------------------------------------------- farm dimension
    # A registry snapshot lands every day; the dimension is the latest row per
    # farm. New farms appear tomorrow; changed attributes overwrite the old row.
    con.execute(
        """
        CREATE OR REPLACE TABLE silver_farms AS
        SELECT farm_id, farmer_name, crop, district, state,
               CAST(lat AS DOUBLE) AS lat, CAST(lon AS DOUBLE) AS lon,
               CAST(area_ha AS DOUBLE) AS area_ha, soil_type, registered_on, updated_at,
               CAST(snapshot_date AS DATE) AS snapshot_date
        FROM (
            SELECT *, CAST(_partition_date AS DATE) AS snapshot_date,
                   row_number() OVER (
                       PARTITION BY farm_id
                       ORDER BY _partition_date DESC, updated_at DESC
                   ) AS rn
            FROM bronze_farms
        )
        WHERE rn = 1
        """
    )

    # ------------------------------------------------------------ stg layer
    # One staging view per source: flatten the API payload to (farm_id, obs_date)
    # rows and attach a _reason if the row must not reach silver. Staging views
    # are shared by both the valid table and the quarantine table.
    con.execute(
        f"""
        CREATE OR REPLACE VIEW stg_weather AS
        WITH parsed AS (
            SELECT _source_file, _partition_date,
                   from_json(_value, $w${W_SCHEMA}$w$) AS p
            FROM bronze_weather
        ), exploded AS (
            SELECT _source_file, _partition_date,
                   CAST(p._meta.obs_date AS DATE)       AS obs_date,
                   CAST(p._meta.fetched_at AS TIMESTAMP) AS fetched_at,
                   f.farm_id                            AS farm_id,
                   CAST(f.response.daily."time"[1] AS DATE) AS weather_date,
                   f.response.daily.temperature_2m_max[1]   AS temp_max_c,
                   f.response.daily.temperature_2m_min[1]   AS temp_min_c,
                   f.response.daily.temperature_2m_mean[1]  AS temp_mean_c,
                   f.response.daily.precipitation_sum[1]    AS rain_mm,
                   f.response.daily.wind_speed_10m_max[1]   AS wind_max_kmh,
                   f.response.daily.relative_humidity_2m_mean[1] AS humidity_pct,
                   f.response.daily.et0_fao_evapotranspiration[1] AS et0_mm
            FROM parsed, UNNEST(p.farms) AS t(f)
        )
        SELECT e.*,
               CASE
                   WHEN e.farm_id IS NULL THEN 'missing_farm_id'
                   WHEN e.obs_date IS NULL THEN 'missing_obs_date'
                   WHEN dim.farm_id IS NULL THEN 'unknown_farm_id'
                   WHEN e.weather_date IS NULL OR e.weather_date <> e.obs_date THEN 'date_mismatch'
                   WHEN e.temp_max_c IS NULL OR e.temp_min_c IS NULL THEN 'missing_temperature'
                   WHEN e.temp_max_c NOT BETWEEN {LIMITS["temp_max_c"][0]} AND {LIMITS["temp_max_c"][1]}
                     OR e.temp_min_c NOT BETWEEN {LIMITS["temp_min_c"][0]} AND {LIMITS["temp_min_c"][1]}
                       THEN 'temperature_out_of_range'
                   WHEN e.rain_mm IS NULL OR e.rain_mm NOT BETWEEN {LIMITS["rain_mm"][0]} AND {LIMITS["rain_mm"][1]}
                       THEN 'rain_out_of_range'
                   WHEN e.wind_max_kmh IS NULL OR e.wind_max_kmh NOT BETWEEN {LIMITS["wind_max_kmh"][0]} AND {LIMITS["wind_max_kmh"][1]}
                       THEN 'wind_out_of_range'
                   WHEN e.humidity_pct IS NULL OR e.humidity_pct NOT BETWEEN {LIMITS["humidity_pct"][0]} AND {LIMITS["humidity_pct"][1]}
                       THEN 'humidity_out_of_range'
               END AS _reason
        FROM exploded e
        LEFT JOIN silver_farms dim ON dim.farm_id = e.farm_id
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE VIEW stg_agro AS
        WITH parsed AS (
            SELECT _source_file, from_json(_value, $a${A_SCHEMA}$a$) AS p
            FROM bronze_agro
        ), exploded AS (
            SELECT _source_file,
                   CAST(p._meta.obs_date AS DATE)        AS obs_date,
                   CAST(p._meta.fetched_at AS TIMESTAMP) AS fetched_at,
                   f.farm_id                             AS farm_id,
                   map_extract_value(f.response.properties.parameter.T2M,
                       strftime(CAST(p._meta.obs_date AS DATE), '%Y%m%d')) AS temp_mean_c,
                   map_extract_value(f.response.properties.parameter.PRECTOTCORR,
                       strftime(CAST(p._meta.obs_date AS DATE), '%Y%m%d')) AS rain_mm,
                   map_extract_value(f.response.properties.parameter.RH2M,
                       strftime(CAST(p._meta.obs_date AS DATE), '%Y%m%d')) AS humidity_pct,
                   map_extract_value(f.response.properties.parameter.GWETROOT,
                       strftime(CAST(p._meta.obs_date AS DATE), '%Y%m%d')) AS root_zone_wetness
            FROM parsed, UNNEST(p.farms) AS t(f)
        )
        SELECT e.*,
               CASE
                   WHEN e.farm_id IS NULL THEN 'missing_farm_id'
                   WHEN e.obs_date IS NULL THEN 'missing_obs_date'
                   WHEN dim.farm_id IS NULL THEN 'unknown_farm_id'
                   WHEN e.temp_mean_c = -999 OR e.rain_mm = -999
                     OR e.humidity_pct = -999 OR e.root_zone_wetness = -999
                       THEN 'sentinel_missing_value'          -- NASA POWER fill value
                   WHEN e.temp_mean_c IS NULL OR e.temp_mean_c NOT BETWEEN {LIMITS["temp_max_c"][0]} AND {LIMITS["temp_max_c"][1]}
                       THEN 'temperature_out_of_range'
                   WHEN e.rain_mm IS NULL OR e.rain_mm NOT BETWEEN {LIMITS["rain_mm"][0]} AND {LIMITS["rain_mm"][1]}
                       THEN 'rain_out_of_range'
                   WHEN e.humidity_pct IS NULL OR e.humidity_pct NOT BETWEEN {LIMITS["humidity_pct"][0]} AND {LIMITS["humidity_pct"][1]}
                       THEN 'humidity_out_of_range'
                   WHEN e.root_zone_wetness IS NULL OR e.root_zone_wetness NOT BETWEEN 0 AND 1
                       THEN 'root_zone_wetness_out_of_range'
               END AS _reason
        FROM exploded e
        LEFT JOIN silver_farms dim ON dim.farm_id = e.farm_id
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE VIEW stg_satellite AS
        WITH parsed AS (
            SELECT _source_file, from_json(_value, $s${S_SCHEMA}$s$) AS p
            FROM bronze_satellite
        ), exploded AS (
            SELECT _source_file,
                   CAST(p._meta.obs_date AS DATE)        AS obs_date,
                   CAST(p._meta.fetched_at AS TIMESTAMP) AS fetched_at,
                   f.farm_id                             AS farm_id,
                   sc.id                                 AS scene_id,
                   CAST(substring(sc.datetime, 1, 10) AS DATE) AS scene_date,
                   sc.cloud_cover                        AS cloud_pct
            FROM parsed, UNNEST(p.farms) AS t(f), UNNEST(f.response.features) AS u(sc)
        )
        SELECT e.*,
               CASE
                   WHEN e.farm_id IS NULL THEN 'missing_farm_id'
                   WHEN e.obs_date IS NULL THEN 'missing_obs_date'
                   WHEN dim.farm_id IS NULL THEN 'unknown_farm_id'
                   WHEN e.scene_id IS NULL THEN 'missing_scene_id'
                   WHEN e.scene_date IS NULL THEN 'missing_scene_date'
                   WHEN e.scene_date > e.obs_date THEN 'scene_date_in_future'
                   WHEN e.cloud_pct IS NULL OR e.cloud_pct NOT BETWEEN 0 AND 100
                       THEN 'cloud_cover_out_of_range'
               END AS _reason
        FROM exploded e
        LEFT JOIN silver_farms dim ON dim.farm_id = e.farm_id
        """
    )

    # ------------------------------------------------------ silver + quarantine
    # silver_* is the typed, deduplicated-but-append-only contract. Rows that
    # fail a rule go to quarantine with a machine-readable reason, never silently
    # dropped: "how many rows did source X lose today" must be answerable.
    con.execute(
        """
        CREATE OR REPLACE TABLE silver_weather AS
        SELECT farm_id, obs_date, weather_date, temp_max_c, temp_min_c, temp_mean_c,
               rain_mm, wind_max_kmh, humidity_pct, et0_mm, fetched_at, _source_file
        FROM stg_weather WHERE _reason IS NULL
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE silver_agro AS
        SELECT farm_id, obs_date, root_zone_wetness, temp_mean_c AS agro_temp_mean_c,
               rain_mm AS agro_rain_mm, humidity_pct AS agro_humidity_pct,
               fetched_at, _source_file
        FROM stg_agro WHERE _reason IS NULL
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE silver_satellite AS
        SELECT farm_id, obs_date, scene_id, scene_date, cloud_pct, fetched_at, _source_file
        FROM stg_satellite WHERE _reason IS NULL
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE quarantine AS
        SELECT 'weather' AS source, farm_id, obs_date, _reason AS reason, fetched_at, _source_file
        FROM stg_weather WHERE _reason IS NOT NULL
        UNION ALL
        SELECT 'agro', farm_id, obs_date, _reason, fetched_at, _source_file
        FROM stg_agro WHERE _reason IS NOT NULL
        UNION ALL
        SELECT 'satellite', farm_id, obs_date, _reason, fetched_at, _source_file
        FROM stg_satellite WHERE _reason IS NOT NULL
        """
    )

    # -------------------------------------------------------------- gold layer
    # The unified table: ONE row per farm per day. A source can be missing for a
    # day, so each source is joined twice: first try the exact day, then fall
    # back to the latest row inside its freshness window. The fallback is never
    # hidden: *_as_of + *_is_stale tell the app how old every value is.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE gold_farm_daily AS
        WITH
        w_latest AS (
            SELECT * FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY farm_id, obs_date ORDER BY fetched_at DESC) rn
                FROM silver_weather
            ) WHERE rn = 1
        ),
        a_latest AS (
            SELECT * FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY farm_id, obs_date ORDER BY fetched_at DESC) rn
                FROM silver_agro
            ) WHERE rn = 1
        ),
        s_latest AS (
            SELECT * FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY farm_id, scene_id ORDER BY fetched_at DESC) rn
                FROM silver_satellite
            ) WHERE rn = 1
        ),
        obs_dates AS (
            SELECT obs_date FROM silver_weather
            UNION
            SELECT obs_date FROM silver_agro
        ),
        spine AS (
            SELECT f.farm_id, d.obs_date
            FROM silver_farms f CROSS JOIN obs_dates d
        ),
        w_asof AS (
            SELECT s.farm_id, s.obs_date,
                   w.temp_max_c, w.temp_min_c, w.temp_mean_c, w.rain_mm,
                   w.wind_max_kmh, w.humidity_pct, w.et0_mm,
                   w.obs_date AS weather_as_of, w.fetched_at AS weather_fetched_at,
                   row_number() OVER (
                       PARTITION BY s.farm_id, s.obs_date ORDER BY w.obs_date DESC) rn
            FROM spine s
            LEFT JOIN w_latest w
              ON w.farm_id = s.farm_id
             AND w.obs_date BETWEEN s.obs_date - INTERVAL {WEATHER_LOOKBACK_DAYS} DAY AND s.obs_date
        ),
        a_asof AS (
            SELECT s.farm_id, s.obs_date,
                   a.root_zone_wetness, a.obs_date AS agro_as_of,
                   a.fetched_at AS agro_fetched_at,
                   row_number() OVER (
                       PARTITION BY s.farm_id, s.obs_date ORDER BY a.obs_date DESC) rn
            FROM spine s
            LEFT JOIN a_latest a
              ON a.farm_id = s.farm_id
             AND a.obs_date BETWEEN s.obs_date - INTERVAL {AGRO_LOOKBACK_DAYS} DAY AND s.obs_date
        ),
        s_asof AS (
            SELECT s.farm_id, s.obs_date, sc.scene_id, sc.scene_date, sc.cloud_pct,
                   row_number() OVER (
                       PARTITION BY s.farm_id, s.obs_date
                       -- prefer a usable scene, then the most recent one
                       ORDER BY coalesce(sc.cloud_pct > {SATELLITE_CLOUD_MAX}, true),
                                sc.scene_date DESC, sc.cloud_pct ASC) rn
            FROM spine s
            LEFT JOIN s_latest sc
              ON sc.farm_id = s.farm_id
             AND sc.scene_date BETWEEN s.obs_date - INTERVAL {SATELLITE_LOOKBACK_DAYS} DAY AND s.obs_date
        )
        SELECT
            f.farm_id,
            sp.obs_date,
            f.farmer_name, f.crop, f.district, f.state, f.lat, f.lon, f.area_ha, f.soil_type,
            w.temp_max_c, w.temp_min_c, w.temp_mean_c, w.rain_mm, w.wind_max_kmh,
            w.humidity_pct, w.et0_mm,
            w.weather_as_of,
            (w.weather_as_of IS DISTINCT FROM sp.obs_date) AS weather_is_stale,
            w.weather_fetched_at,
            a.root_zone_wetness,
            CASE
                WHEN a.root_zone_wetness IS NULL THEN NULL
                WHEN a.root_zone_wetness >= 0.50 THEN 'none'
                WHEN a.root_zone_wetness >= 0.35 THEN 'mild'
                WHEN a.root_zone_wetness >= 0.20 THEN 'moderate'
                ELSE 'severe'
            END AS drought_level,
            a.agro_as_of,
            (a.agro_as_of IS DISTINCT FROM sp.obs_date) AS agro_is_stale,
            a.agro_fetched_at,
            sc.scene_id AS satellite_scene_id,
            sc.scene_date AS satellite_scene_date,
            sc.cloud_pct AS satellite_cloud_pct,
            (sc.scene_id IS NOT NULL AND sc.cloud_pct <= {SATELLITE_CLOUD_MAX}) AS satellite_is_usable,
            CASE
                WHEN w.weather_as_of IS NULL AND a.agro_as_of IS NULL THEN 'unavailable'
                WHEN w.weather_as_of = sp.obs_date AND a.agro_as_of = sp.obs_date THEN 'complete'
                ELSE 'degraded'
            END AS data_status,
            now() AS updated_at
        FROM spine sp
        JOIN silver_farms f ON f.farm_id = sp.farm_id
        LEFT JOIN w_asof w
          ON w.farm_id = sp.farm_id AND w.obs_date = sp.obs_date AND w.rn = 1
        LEFT JOIN a_asof a
          ON a.farm_id = sp.farm_id AND a.obs_date = sp.obs_date AND a.rn = 1
        LEFT JOIN s_asof sc
          ON sc.farm_id = sp.farm_id AND sc.obs_date = sp.obs_date AND sc.rn = 1
        ORDER BY sp.obs_date, sp.farm_id
        """
    )

    # ------------------------------------------------------- app-facing contract
    # What the backend API selects from PostgreSQL. Keeping this narrow means the
    # app never depends on pipeline-internal columns and reads a small table.
    con.execute(
        """
        CREATE OR REPLACE TABLE app_farm_daily AS
        SELECT farm_id, obs_date, crop, district,
               temp_max_c, temp_min_c, rain_mm, wind_max_kmh, humidity_pct,
               drought_level, data_status,
               weather_as_of, agro_as_of, satellite_is_usable, updated_at
        FROM gold_farm_daily
        """
    )
    return con


def print_report(
    con: duckdb.DuckDBPyConnection, raw_dir: Path, db_path: Path, app_path: Path
) -> None:
    counts = con.sql(
        "SELECT (SELECT count(*) FROM bronze_weather), (SELECT count(*) FROM bronze_agro), "
        "(SELECT count(*) FROM bronze_satellite), (SELECT count(*) FROM bronze_farms)"
    ).fetchall()[0]
    silver = con.sql(
        "SELECT (SELECT count(*) FROM silver_farms), (SELECT count(*) FROM silver_weather), "
        "(SELECT count(*) FROM silver_agro), (SELECT count(*) FROM silver_satellite)"
    ).fetchall()[0]
    gold_n = con.sql("SELECT count(*) FROM gold_farm_daily").fetchall()[0][0]

    print(f"\n[build] raw={raw_dir}  db={db_path}")
    print(
        f"[build] bronze rows: weather={counts[0]} agro={counts[1]} satellite={counts[2]} farms={counts[3]}"
    )
    print(
        f"[build] silver rows: farms={silver[0]} weather={silver[1]} agro={silver[2]} satellite={silver[3]}"
    )

    print("\nquarantine (rows that failed a rule, with the reason kept):")
    con.sql(
        "SELECT source, reason, count(*) AS rows FROM quarantine "
        "GROUP BY source, reason ORDER BY source, reason"
    ).show(max_width=100)
    con.sql(
        "SELECT source, reason, farm_id, obs_date FROM quarantine ORDER BY source, obs_date, farm_id"
    ).show(max_width=100)

    print(f"\ngold_farm_daily: {gold_n} rows = one row per farm per obs_date")
    print("\nfreshness per day (fallbacks are visible, never silent):")
    con.sql(
        """
        SELECT obs_date,
               count(*)                                    AS farms,
               count(*) FILTER (weather_as_of = obs_date)  AS weather_fresh,
               count(*) FILTER (agro_as_of = obs_date)     AS agro_fresh,
               count(*) FILTER (agro_is_stale AND agro_as_of IS NOT NULL) AS agro_using_older,
               count(*) FILTER (agro_as_of IS NULL)        AS agro_missing,
               string_agg(DISTINCT data_status, ',' ORDER BY data_status) AS status
        FROM gold_farm_daily GROUP BY obs_date ORDER BY obs_date
        """
    ).show(max_width=120)

    print("\napp_farm_daily (what the backend/API reads):")
    con.sql(
        "SELECT obs_date, farm_id, crop, temp_max_c, rain_mm, drought_level, data_status "
        "FROM app_farm_daily ORDER BY obs_date, farm_id"
    ).show(max_width=120)

    print(f"\n[build] app mirror exported -> {app_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", default="data/raw", type=Path, help="raw lake root")
    parser.add_argument("--db", default="data/leher.duckdb", type=Path, help="DuckDB output file")
    parser.add_argument(
        "--app-out", default="data/app/farm_daily.parquet", type=Path, help="app mirror parquet"
    )
    args = parser.parse_args()

    con = build(args.raw, args.db)
    args.app_out.parent.mkdir(parents=True, exist_ok=True)
    app_path = str(args.app_out).replace("'", "''")
    con.execute(
        f"COPY (SELECT * FROM app_farm_daily ORDER BY obs_date, farm_id) "
        f"TO '{app_path}' (FORMAT PARQUET)"
    )
    print_report(con, args.raw, args.db, args.app_out)
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
