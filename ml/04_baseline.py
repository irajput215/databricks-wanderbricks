"""
04_baseline.py — Prophet baseline forecast per station, MLflow-tracked.

The simple, explainable baseline: its RMSE is the bar the XGBoost model
must beat. Runs as a job task with --catalog/--schema pointing at the
environment's tables (dev vs prod).
"""
import argparse
import mlflow
import pandas as pd
from prophet import Prophet
from sklearn.metrics import mean_squared_error, mean_absolute_error
import numpy as np


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--station", default=None, help="defaults to the station with most data")
    p.add_argument("--horizon", type=int, default=30)
    return p.parse_args()


def load_series(catalog: str, schema: str, station: str | None) -> pd.DataFrame:
    df = spark.table(f"{catalog}.{schema}.gsod_silver") \
        .select("station", "date", "temp_c") \
        .filter("temp_c IS NOT NULL")

    if station is None:
        station = df.groupBy("station").count() \
            .orderBy("count", ascending=False) \
            .limit(1).collect()[0][0]
    print(f"baseline station: {station}")

    # Prophet wants ds (date) and y (value)
    return (
        df.filter(f"station = '{station}'")
        .orderBy("date")
        .toPandas()
        .rename(columns={"date": "ds", "temp_c": "y"})
    )


def train_and_log(series: pd.DataFrame, horizon: int) -> None:
    split = len(series) - horizon
    train, test = series.iloc[:split], series.iloc[split:]

    mlflow.set_experiment("/Shared/wanderbricks_forecast")
    with mlflow.start_run(run_name="baseline_prophet"):
        model = Prophet(daily_seasonality=True)
        model.fit(train)
        forecast = model.predict(test[["ds"]])

        rmse = float(np.sqrt(mean_squared_error(test["y"], forecast["yhat"])))
        mae = float(mean_absolute_error(test["y"], forecast["yhat"]))

        mlflow.log_metrics({"rmse": rmse, "mae": mae})
        mlflow.log_param("model", "prophet")
        mlflow.prophet.log_model(model, "model")
        print(f"baseline rmse={rmse:.3f} mae={mae:.3f}")


if __name__ == "__main__":
    args = parse_args()
    train_and_log(load_series(args.catalog, args.schema, args.station), args.horizon)
