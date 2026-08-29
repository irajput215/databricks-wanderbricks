"""
03_features.py — build the date-keyed feature table (gold) for forecasting.

Calendar features, lags, rolling statistics -> dev.gold.forecast_features,
registered in the Feature Store. See README Part 2, step 3.
"""
import pyspark.sql.functions as F
from databricks.feature_store import FeatureStoreClient

from pyspark.sql import DataFrame

LAG_WINDOWS = [1, 7, 14, 28]        # TODO: tune to your series
ROLLING_WINDOWS = [7, 14]           # TODO: tune


def add_time_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("day_of_week", F.dayofweek("sale_date"))
         .withColumn("day_of_month", F.dayofmonth("sale_date"))
         .withColumn("month", F.month("sale_date"))
         .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
    )


def add_lag_and_rolling(df: DataFrame) -> DataFrame:
    out = df
    for lag in LAG_WINDOWS:
        out = out.withColumn(f"value_lag_{lag}", F.lag("value", lag).over(
            F.window("sale_date", "1 day").orderBy("sale_date")))
    for w in ROLLING_WINDOWS:
        out = out.withColumn(f"rolling_mean_{w}", F.avg("value").over(
            F.window("sale_date", f"{w} days").orderBy("sale_date")))
    return out


def main() -> None:
    clean = spark.table("dev.silver.sales_clean")
    features = add_lag_and_rolling(add_time_features(clean))
    fs = FeatureStoreClient()
    fs.create_table(
        name="dev.gold.forecast_features",
        primary_keys=["sale_date"],
        df=features,
        schema=features.schema,
    )


if __name__ == "__main__":
    main()
