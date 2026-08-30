"""
02_clean.py — Silver: validated, unit-converted GSOD daily weather.

Streaming table fed by gsod_bronze. @dlt.expect_or_drop enforces business
rules in real time (invalid rows are dropped, not silently kept).

Units: the bucket stores US units — temperatures in °F, precipitation in
inches, wind in knots. Converted here to °C / mm; GSOD missing-value
sentinels become NULL.
"""
import dlt
from pyspark.sql.functions import col, to_date, when, round as _round

# GSOD missing-value sentinels (per the GSOD readme)
_TEMP_SENTINEL = 9999.9   # TEMP/MAX/MIN/DEWP/SLP
_WIND_SENTINEL = 999.9    # STP/VISIB/WDSP/MXSPD/GUST/SNDP
_PRCP_SENTINEL = 99.99    # PRCP


def _f_to_c(raw_col):
    """°F -> °C with the 9999.9-style sentinel mapped to NULL."""
    return when(col(raw_col) >= _TEMP_SENTINEL * 0.99, None) \
        .otherwise(_round((col(raw_col) - 32.0) * 5.0 / 9.0, 2))


@dlt.table(
    name="gsod_silver",
    comment="Validated clean daily weather in metric units with NULL for missing",
)
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
@dlt.expect_or_drop("valid_station", "station IS NOT NULL")
@dlt.expect_or_drop("valid_temp", "temp_c IS NULL OR (temp_c >= -90 AND temp_c <= 60)")
def gsod_silver():
    raw = dlt.read_stream("gsod_bronze")
    return (
        raw
        .withColumn("date", to_date(col("DATE"), "yyyy-MM-dd"))
        .withColumn("temp_c", _f_to_c("TEMP"))
        .withColumn("max_c", _f_to_c("MAX"))
        .withColumn("min_c", _f_to_c("MIN"))
        .withColumn("dewp_c", _f_to_c("DEWP"))
        .withColumn(
            "prcp_mm",
            when(col("PRCP") >= _PRCP_SENTINEL * 0.99, None)
            .otherwise(_round(col("PRCP") * 25.4, 2)),          # inches -> mm
        )
        .withColumn(
            "wdsp_knots",
            when(col("WDSP") >= _WIND_SENTINEL * 0.99, None)
            .otherwise(_round(col("WDSP"), 1)),
        )
        .withColumn(
            "visib_miles",
            when(col("VISIB") >= _WIND_SENTINEL * 0.99, None).otherwise(col("VISIB")),
        )
        .withColumn("latitude", col("LATITUDE"))
        .withColumn("longitude", col("LONGITUDE"))
        .withColumn("elevation", col("ELEVATION"))
        .withColumn("station_name", col("NAME"))
        .withColumn("frshtt", col("FRSHTT"))
        .select(
            "station", "date", "temp_c", "max_c", "min_c", "dewp_c",
            "prcp_mm", "wdsp_knots", "visib_miles", "latitude", "longitude",
            "elevation", "station_name", "frshtt", "ingestion_time",
        )
    )
