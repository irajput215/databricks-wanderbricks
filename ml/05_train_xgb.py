"""
05_train_xgb.py — XGBoost forecast model on lag/rolling features, MLflow-tracked.

Uses the gold feature table; compares against the Prophet baseline; registers
the best model in the Model Registry. See README Part 2, step 5-6.
"""
import mlflow
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.model_selection import train_test_split
import numpy as np


def load_features() -> pd.DataFrame:
    return spark.table("dev.gold.forecast_features").drop("sale_date").toPandas()


def train_and_register(horizon: int = 30, n_estimators: int = 300) -> None:
    df = load_features().dropna()
    y = df.pop("value")
    X = df
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=horizon / len(df), shuffle=False)

    with mlflow.start_run(run_name="xgb_forecast"):
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

        # Register for serving / retraining pipeline
        mlflow.register_model(
            f"runs:/{mlflow.active_run().info.run_id}/model", "forecast_model")


if __name__ == "__main__":
    train_and_register()
