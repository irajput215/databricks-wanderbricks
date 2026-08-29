"""
04_baseline.py — Prophet (or ARIMA) baseline forecast, MLflow-tracked.

The simple, explainable baseline. Its error becomes the bar the ML model
must beat. See README Part 2, step 4.
"""
import mlflow
import pandas as pd
from prophet import Prophet  # available on Databricks ML Runtime


def load_series() -> pd.DataFrame:
    # Prophet wants columns ds (date) and y (value)
    return (
        spark.table("dev.silver.sales_clean")
        .select("sale_date", "value")
        .orderBy("sale_date")
        .toPandas()
        .rename(columns={"sale_date": "ds", "value": "y"})
    )


def train_and_log(horizon: int = 30) -> None:
    series = load_series()
    split = len(series) - horizon
    train, test = series.iloc[:split], series.iloc[split:]

    with mlflow.start_run(run_name="baseline_prophet"):
        model = Prophet(daily_seasonality=True)
        model.fit(train)
        forecast = model.predict(test[["ds"]])

        from sklearn.metrics import mean_squared_error, mean_absolute_error
        import numpy as np
        rmse = float(np.sqrt(mean_squared_error(test["y"], forecast["yhat"])))
        mae = float(mean_absolute_error(test["y"], forecast["yhat"]))

        mlflow.log_metrics({"rmse": rmse, "mae": mae})
        mlflow.log_param("model", "prophet")
        mlflow.prophet.log_model(model, "model")
        mlflow.log_artifact("series.csv", {"series": series.to_csv()})

        print(f"baseline rmse={rmse:.3f} mae={mae:.3f}")


if __name__ == "__main__":
    train_and_log()
