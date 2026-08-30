"""
06_score.py — batch scoring: next-N-day forecasts per station.

Loads the Production model (wanderbricks_weather_xgb) from the Model
Registry and writes forecast rows to {catalog}.{schema}.forecasts.
Runs as a job task with --catalog/--schema.

NOTE: real scoring needs features built up to the prediction date; the DLT
pipeline must run right before this task (it does — same job chain).
"""
import argparse
import time
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

    t0 = time.perf_counter()
    pred = model.predict(latest_per_station.drop(columns=["station", "temp_c"]))
    inference_s = time.perf_counter() - t0

    out = pd.DataFrame({
        "station": latest_per_station["station"].values,
        "forecast_date": date.today().isoformat(),
        "temp_c_forecast": pred,
    })
    spark.createDataFrame(out).write \
        .mode("append") \
        .saveAsTable(f"{catalog}.{schema}.forecasts")

    # --- inference-time monitoring ---------------------------------------
    # Log to MLflow (visible in the experiment/run UI) AND to a Delta table
    # (queryable: SELECT * FROM <schema>.scoring_metrics ORDER BY run_date).
    try:
        mlflow.log_metric("inference_seconds", inference_s)
    except Exception:
        pass  # scoring must not fail because metrics logging did
    metrics = spark.createDataFrame([
        (date.today().isoformat(), len(out), round(inference_s, 4)),
    ], ["run_date", "rows", "inference_seconds"])
    metrics.write.mode("append") \
        .saveAsTable(f"{catalog}.{schema}.scoring_metrics")
    print(f"scored {len(out)} station forecasts for {horizon} days "
          f"(inference {inference_s:.4f}s)")


if __name__ == "__main__":
    args = parse_args()
    score(args.catalog, args.schema, args.horizon)
