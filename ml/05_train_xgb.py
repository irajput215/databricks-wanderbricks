"""
05_train_xgb.py — XGBoost forecast model on gold features, MLflow-tracked.

Reads weather_features (temp_c = target; lags/rolling/calendar = features),
trains XGBoost, logs to MLflow, and registers the model in the Model
Registry as `wanderbricks_weather_xgb`. Runs as a job task with
--catalog/--schema.
"""
import argparse
import mlflow
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.model_selection import train_test_split
import numpy as np

MODEL_NAME = "wanderbricks_weather_xgb"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--test-size", type=float, default=0.1, help="held-out tail fraction")
    p.add_argument("--n-estimators", type=int, default=300)
    return p.parse_args()


def load_features(catalog: str, schema: str) -> pd.DataFrame:
    # station identity is dropped for the skeleton (one global model); a
    # station-aware model (encoded station or per-station training) is future
    # work. Must match what 06_score.py drops for serving.
    return (
        spark.table(f"{catalog}.{schema}.weather_features")
        .drop("date", "station")
        .toPandas()
        .dropna()
    )


def train_and_register(df: pd.DataFrame, test_size: float, n_estimators: int) -> None:
    y = df.pop("temp_c")
    X = df
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, shuffle=False)  # no shuffle: respect time order

    with mlflow.start_run(run_name="xgb_weather"):
        model = xgb.XGBRegressor(
            n_estimators=n_estimators,
            max_depth=5,
            learning_rate=0.05,
            objective="reg:squarederror",
        )
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
        pred = model.predict(X_test)
        rmse = float(np.sqrt(mean_squared_error(y_test, pred)))
        mae = float(mean_absolute_error(y_test, pred))

        mlflow.log_metrics({"rmse": rmse, "mae": mae})
        mlflow.log_params({"model": "xgboost", "n_estimators": n_estimators})
        mlflow.xgboost.log_model(model, "model")
        print(f"xgb rmse={rmse:.3f} mae={mae:.3f}")

        mlflow.register_model(
            f"runs:/{mlflow.active_run().info.run_id}/model", MODEL_NAME)


if __name__ == "__main__":
    args = parse_args()
    train_and_register(
        load_features(args.catalog, args.schema), args.test_size, args.n_estimators)
