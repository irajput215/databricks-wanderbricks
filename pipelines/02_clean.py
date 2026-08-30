"""
02_clean.py — Silver: validated, unit-converted GSOD daily weather.

Streaming table fed by gsod_bronze. @dlt.expect_or_drop enforces business
rules in real time (invalid rows are dropped, not silently kept). Converts
GSOD conventions: tenths -> real units, sentinel missing values -> NULL.
"""
import dlt
from pyspark.sql.functions import col, to_date, when, round as _round

# GSOD missing-value sentinels (per the GSOD readme)
_TEMP_SENTINEL = 9999.9   # TEMP/MAX/MIN/DEWP etc.
_PRCP_SENTINEL = 99.99    # PRCP
_WDSP_SENTINEL = 999.9    # WDSP/VISIB


@dlt.table(
    name="gsod_silver",
    comment="Validated clean daily weather with real units and NULL for missing",
)
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
@dlt.expect_or_drop("valid_station", "station IS NOT NULL")
@dlt.expect_or_drop("valid_temp", "temp_c IS NULL OR (temp_c >= -90 AND temp_c <= 60)")
def gsod_silver():
    raw = dlt.read_stream("gsod_bronze")
    return (
        raw
        .withColumn("date", to_date(col("DATE"), "yyyyMMdd"))
        .withColumn(
            "temp_c",
            when(col("TEMP") >= _TEMP_SENTINEL * 0.99, None).otherwise(_round(col("TEMP") / 10.0, 2)),
        )
        .withColumn(
            "max_c",
            when(col("MAX") >= _TEMP_SENTINEL * 0.99, None).otherwise(_round(col("MAX") / 10.0, 2)),
        )
        .withColumn(
            "min_c",
            when(col("MIN") >= _TEMP_SENTINEL * 0.99, None).otherwise(_round(col("MIN") / 10.0, 2)),
        )
        .withColumn(
            "dewp_c",
            when(col("DEWP") >= _TEMP_SENTINEL * 0.99, None).otherwise(_round(col("DEWP") / 10.0, 2)),
        )
        .withColumn(
            "prcp_mm",
            when(col("PRCP") >= _PRCP_SENTINEL * 0.99, None).otherwise(_round(col("PRCP") / 10.0, 2)),
        )
        .withColumn(
            "wdsp_knots",
            when(col("WDSP") >= _WDSP_SENTINEL * 0.99, None).otherwise(_round(col("WDSP") / 10.0, 1)),
        )
        .withColumn("latitude", col("LATITUDE"))
        .withColumn("longitude", col("LONGITUDE"))
        .withColumn("elevation", col("ELEVATION"))
        .withColumn("station_name", col("NAME"))
        .withColumn("frshtt", col("FRSHTT"))
        .select(
            "station", "date", "temp_c", "max_c", "min_c", "dewp_c",
            "prcp_mm", "wdsp_knots", "latitude", "longitude", "elevation",
            "station_name", "frshtt", "ingestion_time",
        )
    )
