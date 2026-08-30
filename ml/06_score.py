"""
06_score.py — batch scoring: next-N-day forecasts per station.

Loads the Production model (wanderbricks_weather_xgb) from the Model
Registry and writes forecast rows to {catalog}.{schema}.forecasts.
Runs as a job task with --catalog/--schema.

NOTE: real scoring needs features built up to the prediction date; the DLT
pipeline must run right before this task (it does — same job chain).
"""
import argparse
import mlflow
import pandas as pd
from datetime import date, timedelta

MODEL_NAME = "wanderbricks_weather_xgb"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--horizon", type=int, default=30)
    return p.parse_args()


def score(catalog: str, schema: str, horizon: int) -> None:
    model_uri = f"models:/{MODEL_NAME}/Production"
    model = mlflow.pyfunc.load_model(model_uri)

    features = (
        spark.table(f"{catalog}.{schema}.weather_features")
        .drop("date")
        .toPandas()
        .dropna()
    )
    if features.empty:
        raise SystemExit("no scored rows: weather_features is empty after dropna")

    latest_per_station = (
        features.groupby("station")
        .tail(1)                     # last feature row per station
        .copy()
    )
    pred = model.predict(latest_per_station.drop(columns=["station", "temp_c"]))

    out = pd.DataFrame({
        "station": latest_per_station["station"].values,
        "forecast_date": date.today().isoformat(),
        "temp_c_forecast": pred,
    })
    spark.createDataFrame(out).write \
        .mode("append") \
        .saveAsTable(f"{catalog}.{schema}.forecasts")
    print(f"scored {len(out)} station forecasts for {horizon} days")


if __name__ == "__main__":
    args = parse_args()
    score(args.catalog, args.schema, args.horizon)
