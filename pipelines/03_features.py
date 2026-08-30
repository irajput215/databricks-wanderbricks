"""
03_features.py — Gold: per-station daily forecasting features.

Materialized view over gsod_silver (dlt.read = batch recompute each run, so
lag/rolling window functions are legal — they are NOT on a streaming read).
Adds calendar features + lag and rolling temperature features per station.

Forecast target: temp_c at date t, explained by features known at t-1 and
earlier (lags + rolling stats), exactly what 05_train_xgb.py consumes.

For a REAL-TIME variant (sub-minute rolling windows on a stream), replace the
lag/rolling block with window() + groupBy aggregation on dlt.read_stream —
see README Part 3 for the comparison.
"""
import dlt
import pyspark.sql.functions as F
from pyspark.sql import Window

LAG_WINDOWS = [1, 7, 14, 28]
ROLLING_WINDOWS = [7, 14]


@dlt.table(
    name="weather_features",
    comment="Per-station daily features (calendar + lags + rolling) for forecasting",
)
def weather_features():
    silver = dlt.read("gsod_silver")
    station_w = Window.partitionBy("station").orderBy("date")

    df = silver.withColumn("day_of_week", F.dayofweek("date")) \
               .withColumn("day_of_year", F.dayofyear("date")) \
               .withColumn("month", F.month("date")) \
               .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))

    for lag in LAG_WINDOWS:
        df = df.withColumn(f"temp_lag_{lag}", F.lag("temp_c", lag).over(station_w))

    for w in ROLLING_WINDOWS:
        rolling = (
            Window.partitionBy("station")
            .orderBy("date")
            .rowsBetween(-w, -1)          # strictly prior w days, excluding today
        )
        df = df.withColumn(f"temp_rollmean_{w}", F.avg("temp_c").over(rolling))

    return df.select(
        "station", "date", "temp_c",  # temp_c = target
        "day_of_week", "day_of_year", "month", "is_weekend",
        *[f"temp_lag_{lag}" for lag in LAG_WINDOWS],
        *[f"temp_rollmean_{w}" for w in ROLLING_WINDOWS],
    )
