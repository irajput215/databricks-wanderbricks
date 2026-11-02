"""Pipeline 1 of 2 -- the Leher farm-conditions pipeline (Lakeflow / Spark Declarative Pipelines).

RAW (Unity Catalog Volume)
  -> bronze_*        raw API envelopes, exactly as landed (Auto Loader, append-only)
  -> silver_*        typed + validated rows, one revision per farm/day
     quarantine      rows that failed a rule, with a reason (never silently dropped)
  -> gold_farm_daily ONE row per farm per obs_date: weather + drought + satellite + farm dim
  -> app_farm_daily  narrow contract the backend mirrors to PostgreSQL

Same layer contract as the local DuckDB mirror in scripts/build_model.py and
docs/DATA_MODEL.md. The pipeline owns all writes; functions below only return
DataFrames.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RAW_PATH = spark.conf.get("leher.raw_path", "data/raw")

# --- Business rules. Deliberately in one block: they are the interview answer. ---
TEMP_RANGE = (-20.0, 60.0)  # plausible daily temperature, degrees C
RAIN_RANGE = (0.0, 500.0)  # >500 mm/day is bad data for these field sizes
WIND_RANGE = (0.0, 250.0)  # km/h
HUMIDITY_RANGE = (0.0, 100.0)  # %
CLOUD_MAX = 40.0  # Sentinel-2 scene cloudier than this is not usable for weeds
WEATHER_LOOKBACK_DAYS = 1  # yesterday's weather is acceptable, but marked stale
AGRO_LOOKBACK_DAYS = 7  # soil wetness moves slowly; a week back is still useful
SATELLITE_LOOKBACK_DAYS = 7  # Sentinel-2 revisit is ~5 days

# --- Explicit JSON contracts for the raw envelopes. Parsing happens in silver,
# --- not in Auto Loader, so the raw payload stays replayable in the Volume.
_META = "struct<source:string,obs_date:string,fetched_at:string,api:string>"
W_SCHEMA = (
    f"struct<_meta:{_META},farms:array<struct<farm_id:string,response:struct<daily:struct<"
    "time:array<string>,temperature_2m_max:array<double>,temperature_2m_min:array<double>,"
    "temperature_2m_mean:array<double>,precipitation_sum:array<double>,"
    "wind_speed_10m_max:array<double>,relative_humidity_2m_mean:array<double>,"
    "et0_fao_evapotranspiration:array<double>>>>>>"
)
_PARAMS = (
    "struct<T2M:map<string,double>,PRECTOTCORR:map<string,double>,"
    "RH2M:map<string,double>,GWETROOT:map<string,double>>"
)
A_SCHEMA = (
    f"struct<_meta:{_META},farms:array<struct<farm_id:string,response:struct<properties:"
    f"struct<parameter:{_PARAMS}>>>>>"
)
S_SCHEMA = (
    f"struct<_meta:{_META},farms:array<struct<farm_id:string,response:struct<features:"
    "array<struct<id:string,collection:string,datetime:string,cloud_cover:double>>>>>>"
)


# --------------------------------------------------------------------------- #
# Bronze: one streaming table per source. Raw bytes in, lineage columns added.
# --------------------------------------------------------------------------- #
def _raw_stream(subpath: str) -> DataFrame:
    """Auto Loader over one source prefix; files are one-line JSON envelopes.

    No schema location is set: the pipeline manages checkpoints/schema state.
    Only changed files are read, so a rerun of the job does not re-ingest.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("recursiveFileLookup", "true")
        .load(f"{RAW_PATH}/{subpath}")
        .select(
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("value").alias("_value"),
        )
        .withColumn(
            "_partition_date",
            F.to_date(F.regexp_extract("_source_file", r"date=(\d{4}-\d{2}-\d{2})", 1)),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(name="bronze_weather", comment="Raw Open-Meteo envelopes, append-only and replayable")
@dp.expect("source_file_present", "_source_file IS NOT NULL")
def bronze_weather() -> DataFrame:
    return _raw_stream("source=weather")


@dp.table(
    name="bronze_agro", comment="Raw NASA POWER envelopes (root-zone soil wetness = drought signal)"
)
@dp.expect("source_file_present", "_source_file IS NOT NULL")
def bronze_agro() -> DataFrame:
    return _raw_stream("source=agro")


@dp.table(
    name="bronze_satellite", comment="Raw Sentinel-2 scene-metadata envelopes from the STAC API"
)
@dp.expect("source_file_present", "_source_file IS NOT NULL")
def bronze_satellite() -> DataFrame:
    return _raw_stream("source=satellite")


@dp.table(
    name="bronze_farms", comment="Daily farm-registry snapshot exported from the app database"
)
def bronze_farms() -> DataFrame:
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .load(f"{RAW_PATH}/source=farms")
        .select("*", F.col("_metadata.file_path").alias("_source_file"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# --------------------------------------------------------------------------- #
# Farm dimension: latest registry snapshot per farm. A slowly-changing dimension
# from the app database -- not an API. It is the anchor every source joins to.
# --------------------------------------------------------------------------- #
@dp.materialized_view(
    name="silver_farms",
    comment="Latest farm-registry attributes per farm_id; the dimension behind every join",
)
def silver_farms() -> DataFrame:
    return spark.sql(
        """
        SELECT farm_id, farmer_name, crop, district, state,
               CAST(lat AS DOUBLE) AS lat, CAST(lon AS DOUBLE) AS lon,
               CAST(area_ha AS DOUBLE) AS area_ha, soil_type, registered_on, updated_at,
               CAST(snapshot_date AS DATE) AS snapshot_date
        FROM (
            SELECT *,
                   CAST(regexp_extract(_source_file, 'date=([0-9-]{10})', 1) AS DATE) AS snapshot_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY farm_id
                       ORDER BY regexp_extract(_source_file, 'date=([0-9-]{10})', 1) DESC,
                                updated_at DESC
                   ) AS rn
            FROM bronze_farms
        )
        WHERE rn = 1
        """
    )


# --------------------------------------------------------------------------- #
# Parsing: raw envelope -> rows keyed by (farm_id, obs_date).
# Each API has a different shape, but all three end up with the same key.
# --------------------------------------------------------------------------- #
def _parse_weather(raw: DataFrame) -> DataFrame:
    parsed = raw.select("_source_file", "_ingested_at", F.from_json("_value", W_SCHEMA).alias("p"))
    farms = parsed.select(
        "_source_file",
        "_ingested_at",
        F.col("p._meta.obs_date").cast("date").alias("obs_date"),
        F.col("p._meta.fetched_at").cast("timestamp").alias("fetched_at"),
        F.explode("p.farms").alias("f"),
    )
    # One API call asks for exactly one obs_date, so the daily arrays have one element.
    daily = "f.response.daily"
    return farms.select(
        "_source_file",
        "_ingested_at",
        "obs_date",
        "fetched_at",
        F.col("f.farm_id").alias("farm_id"),
        F.col(f"{daily}.time")[0].cast("date").alias("weather_date"),
        F.col(f"{daily}.temperature_2m_max")[0].alias("temp_max_c"),
        F.col(f"{daily}.temperature_2m_min")[0].alias("temp_min_c"),
        F.col(f"{daily}.temperature_2m_mean")[0].alias("temp_mean_c"),
        F.col(f"{daily}.precipitation_sum")[0].alias("rain_mm"),
        F.col(f"{daily}.wind_speed_10m_max")[0].alias("wind_max_kmh"),
        F.col(f"{daily}.relative_humidity_2m_mean")[0].alias("humidity_pct"),
        F.col(f"{daily}.et0_fao_evapotranspiration")[0].alias("et0_mm"),
    )


def _parse_agro(raw: DataFrame) -> DataFrame:
    parsed = raw.select("_source_file", "_ingested_at", F.from_json("_value", A_SCHEMA).alias("p"))
    farms = parsed.select(
        "_source_file",
        "_ingested_at",
        F.col("p._meta.obs_date").cast("date").alias("obs_date"),
        F.col("p._meta.fetched_at").cast("timestamp").alias("fetched_at"),
        F.explode("p.farms").alias("f"),
    )
    # NASA POWER returns {parameter: {T2M: {"20261015": 24.0}, ...}}: a map keyed by
    # date. Pick the key that matches the envelope's obs_date.
    params = "f.response.properties.parameter"
    ymd = F.date_format("obs_date", "yyyyMMdd")
    return farms.select(
        "_source_file",
        "_ingested_at",
        "obs_date",
        "fetched_at",
        F.col("f.farm_id").alias("farm_id"),
        F.element_at(F.col(f"{params}.T2M"), ymd).cast("double").alias("temp_mean_c"),
        F.element_at(F.col(f"{params}.PRECTOTCORR"), ymd).cast("double").alias("rain_mm"),
        F.element_at(F.col(f"{params}.RH2M"), ymd).cast("double").alias("humidity_pct"),
        F.element_at(F.col(f"{params}.GWETROOT"), ymd).cast("double").alias("root_zone_wetness"),
    )


def _parse_satellite(raw: DataFrame) -> DataFrame:
    parsed = raw.select("_source_file", "_ingested_at", F.from_json("_value", S_SCHEMA).alias("p"))
    farms = parsed.select(
        "_source_file",
        "_ingested_at",
        F.col("p._meta.obs_date").cast("date").alias("obs_date"),
        F.col("p._meta.fetched_at").cast("timestamp").alias("fetched_at"),
        F.explode("p.farms").alias("f"),
    )
    scenes = farms.select(
        "_source_file",
        "_ingested_at",
        "obs_date",
        "fetched_at",
        F.col("f.farm_id").alias("farm_id"),
        F.explode("f.response.features").alias("sc"),
    )
    return scenes.select(
        "_source_file",
        "_ingested_at",
        "obs_date",
        "fetched_at",
        "farm_id",
        F.col("sc.id").alias("scene_id"),
        F.to_date(F.substring("sc.datetime", 1, 10)).alias("scene_date"),
        F.col("sc.cloud_cover").alias("cloud_pct"),
    )


# --------------------------------------------------------------------------- #
# Validation: every row gets a machine-readable reason or passes (NULL reason).
# --weather rules are the same list documented in docs/DATA_MODEL.md.
# --------------------------------------------------------------------------- #
def _with_rule(
    parsed: DataFrame, farms: DataFrame, checks: list[tuple[str, F.Column]]
) -> DataFrame:
    known = farms.select(F.col("farm_id").alias("_known_farm"))
    df = parsed.join(known, parsed["farm_id"] == known["_known_farm"], "left")

    reason = F.when(F.col("farm_id").isNull(), "missing_farm_id")
    reason = reason.when(F.col("obs_date").isNull(), "missing_obs_date")
    reason = reason.when(F.col("_known_farm").isNull(), "unknown_farm_id")
    for name, condition in checks:
        reason = reason.when(condition, name)
    return df.withColumn("_reason", reason).drop("_known_farm")


def _flag_weather(parsed: DataFrame, farms: DataFrame) -> DataFrame:
    lo, hi = TEMP_RANGE
    rain = RAIN_RANGE
    wind = WIND_RANGE
    humidity = HUMIDITY_RANGE
    return _with_rule(
        parsed,
        farms,
        [
            (
                "date_mismatch",
                F.col("weather_date").isNull() | (F.col("weather_date") != F.col("obs_date")),
            ),
            ("missing_temperature", F.col("temp_max_c").isNull() | F.col("temp_min_c").isNull()),
            (
                "temperature_out_of_range",
                ~F.col("temp_max_c").between(lo, hi) | ~F.col("temp_min_c").between(lo, hi),
            ),
            ("rain_out_of_range", F.col("rain_mm").isNull() | ~F.col("rain_mm").between(*rain)),
            (
                "wind_out_of_range",
                F.col("wind_max_kmh").isNull() | ~F.col("wind_max_kmh").between(*wind),
            ),
            (
                "humidity_out_of_range",
                F.col("humidity_pct").isNull() | ~F.col("humidity_pct").between(*humidity),
            ),
        ],
    )


def _flag_agro(parsed: DataFrame, farms: DataFrame) -> DataFrame:
    lo, hi = TEMP_RANGE
    rain = RAIN_RANGE
    humidity = HUMIDITY_RANGE
    sentinel = (
        (F.col("temp_mean_c") == -999)
        | (F.col("rain_mm") == -999)
        | (F.col("humidity_pct") == -999)
        | (F.col("root_zone_wetness") == -999)
    )
    return _with_rule(
        parsed,
        farms,
        [
            # NASA POWER uses -999 as its fill value when a day has not arrived yet.
            ("sentinel_missing_value", sentinel),
            (
                "temperature_out_of_range",
                F.col("temp_mean_c").isNull() | ~F.col("temp_mean_c").between(lo, hi),
            ),
            ("rain_out_of_range", F.col("rain_mm").isNull() | ~F.col("rain_mm").between(*rain)),
            (
                "humidity_out_of_range",
                F.col("humidity_pct").isNull() | ~F.col("humidity_pct").between(*humidity),
            ),
            (
                "root_zone_wetness_out_of_range",
                F.col("root_zone_wetness").isNull() | ~F.col("root_zone_wetness").between(0.0, 1.0),
            ),
        ],
    )


def _flag_satellite(parsed: DataFrame, farms: DataFrame) -> DataFrame:
    return _with_rule(
        parsed,
        farms,
        [
            ("missing_scene_id", F.col("scene_id").isNull()),
            ("missing_scene_date", F.col("scene_date").isNull()),
            ("scene_date_in_future", F.col("scene_date") > F.col("obs_date")),
            (
                "cloud_cover_out_of_range",
                F.col("cloud_pct").isNull() | ~F.col("cloud_pct").between(0.0, 100.0),
            ),
        ],
    )


# --------------------------------------------------------------------------- #
# Silver: typed + validated. Append-only revisions; gold keeps the latest one.
# --------------------------------------------------------------------------- #
def _silver(df: DataFrame) -> DataFrame:
    return df.filter(F.col("_reason").isNull()).drop("_reason")


@dp.table(
    name="silver_weather", comment="Typed, validated daily weather per farm (append-only revisions)"
)
@dp.expect("key_not_null", "farm_id IS NOT NULL AND obs_date IS NOT NULL")
def silver_weather() -> DataFrame:
    return _silver(
        _flag_weather(
            _parse_weather(spark.readStream.table("bronze_weather")),
            spark.read.table("silver_farms"),
        )
    )


@dp.table(
    name="silver_agro", comment="Typed drought/soil-wetness signal per farm (append-only revisions)"
)
@dp.expect("key_not_null", "farm_id IS NOT NULL AND obs_date IS NOT NULL")
def silver_agro() -> DataFrame:
    return _silver(
        _flag_agro(
            _parse_agro(spark.readStream.table("bronze_agro")), spark.read.table("silver_farms")
        )
    )


@dp.table(
    name="silver_satellite", comment="Sentinel-2 scene candidates per farm; gold picks the best one"
)
@dp.expect("key_not_null", "farm_id IS NOT NULL AND obs_date IS NOT NULL")
def silver_satellite() -> DataFrame:
    return _silver(
        _flag_satellite(
            _parse_satellite(spark.readStream.table("bronze_satellite")),
            spark.read.table("silver_farms"),
        )
    )


def _quarantine_rows(flagged: DataFrame, source: str) -> DataFrame:
    return flagged.filter(F.col("_reason").isNotNull()).select(
        F.lit(source).alias("source"),
        "farm_id",
        "obs_date",
        F.col("_reason").alias("reason"),
        "_source_file",
        "_ingested_at",
    )


@dp.table(
    name="quarantine",
    comment="Rows that failed a rule, with the reason kept: how many rows each source lost is answerable",
)
def quarantine() -> DataFrame:
    farms = spark.read.table("silver_farms")
    weather = _quarantine_rows(
        _flag_weather(_parse_weather(spark.readStream.table("bronze_weather")), farms), "weather"
    )
    agro = _quarantine_rows(
        _flag_agro(_parse_agro(spark.readStream.table("bronze_agro")), farms), "agro"
    )
    satellite = _quarantine_rows(
        _flag_satellite(_parse_satellite(spark.readStream.table("bronze_satellite")), farms),
        "satellite",
    )
    return weather.unionByName(agro).unionByName(satellite)


# --------------------------------------------------------------------------- #
# Gold: one row per farm per day. Each source is joined exact-day first, then
# falls back inside its freshness window; *_as_of / *_is_stale make the fallback
# visible so the app can show "last updated" instead of hiding it.
# --------------------------------------------------------------------------- #
GOLD_SQL = f"""
WITH w_latest AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY farm_id, obs_date ORDER BY fetched_at DESC) rn
        FROM silver_weather
    ) WHERE rn = 1
),
a_latest AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY farm_id, obs_date ORDER BY fetched_at DESC) rn
        FROM silver_agro
    ) WHERE rn = 1
),
s_latest AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY farm_id, scene_id ORDER BY fetched_at DESC) rn
        FROM silver_satellite
    ) WHERE rn = 1
),
obs_dates AS (
    SELECT obs_date FROM silver_weather
    UNION
    SELECT obs_date FROM silver_agro
),
spine AS (
    SELECT f.farm_id, d.obs_date FROM silver_farms f CROSS JOIN obs_dates d
),
w_asof AS (
    SELECT s.farm_id, s.obs_date,
           w.temp_max_c, w.temp_min_c, w.temp_mean_c, w.rain_mm, w.wind_max_kmh,
           w.humidity_pct, w.et0_mm,
           w.obs_date AS weather_as_of, w.fetched_at AS weather_fetched_at,
           ROW_NUMBER() OVER (PARTITION BY s.farm_id, s.obs_date ORDER BY w.obs_date DESC) rn
    FROM spine s
    LEFT JOIN w_latest w
      ON w.farm_id = s.farm_id
     AND w.obs_date BETWEEN s.obs_date - INTERVAL {WEATHER_LOOKBACK_DAYS} DAY AND s.obs_date
),
a_asof AS (
    SELECT s.farm_id, s.obs_date,
           a.root_zone_wetness, a.obs_date AS agro_as_of, a.fetched_at AS agro_fetched_at,
           ROW_NUMBER() OVER (PARTITION BY s.farm_id, s.obs_date ORDER BY a.obs_date DESC) rn
    FROM spine s
    LEFT JOIN a_latest a
      ON a.farm_id = s.farm_id
     AND a.obs_date BETWEEN s.obs_date - INTERVAL {AGRO_LOOKBACK_DAYS} DAY AND s.obs_date
),
s_asof AS (
    SELECT s.farm_id, s.obs_date, sc.scene_id, sc.scene_date, sc.cloud_pct,
           ROW_NUMBER() OVER (
               PARTITION BY s.farm_id, s.obs_date
               ORDER BY COALESCE(sc.cloud_pct > {CLOUD_MAX}, true), sc.scene_date DESC, sc.cloud_pct ASC
           ) rn
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
    (w.weather_as_of IS NULL OR w.weather_as_of <> sp.obs_date) AS weather_is_stale,
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
    (a.agro_as_of IS NULL OR a.agro_as_of <> sp.obs_date) AS agro_is_stale,
    a.agro_fetched_at,
    sc.scene_id AS satellite_scene_id,
    sc.scene_date AS satellite_scene_date,
    sc.cloud_pct AS satellite_cloud_pct,
    (sc.scene_id IS NOT NULL AND sc.cloud_pct <= {CLOUD_MAX}) AS satellite_is_usable,
    CASE
        WHEN w.weather_as_of IS NULL AND a.agro_as_of IS NULL THEN 'unavailable'
        WHEN w.weather_as_of = sp.obs_date AND a.agro_as_of = sp.obs_date THEN 'complete'
        ELSE 'degraded'
    END AS data_status,
    current_timestamp() AS updated_at
FROM spine sp
JOIN silver_farms f ON f.farm_id = sp.farm_id
LEFT JOIN w_asof w ON w.farm_id = sp.farm_id AND w.obs_date = sp.obs_date AND w.rn = 1
LEFT JOIN a_asof a ON a.farm_id = sp.farm_id AND a.obs_date = sp.obs_date AND a.rn = 1
LEFT JOIN s_asof sc ON sc.farm_id = sp.farm_id AND sc.obs_date = sp.obs_date AND sc.rn = 1
ORDER BY sp.obs_date, sp.farm_id
"""


@dp.materialized_view(
    name="gold_farm_daily",
    comment="Unified field-day table: exactly one row per farm per obs_date, with freshness flags",
)
def gold_farm_daily() -> DataFrame:
    return spark.sql(GOLD_SQL)


@dp.materialized_view(
    name="app_farm_daily",
    comment="Narrow contract the backend mirrors to PostgreSQL; the app never queries Databricks",
)
def app_farm_daily() -> DataFrame:
    return spark.sql(
        """
        SELECT farm_id, obs_date, crop, district,
               temp_max_c, temp_min_c, rain_mm, wind_max_kmh, humidity_pct,
               drought_level, data_status,
               weather_as_of, agro_as_of, satellite_is_usable, updated_at
        FROM gold_farm_daily
        """
    )
