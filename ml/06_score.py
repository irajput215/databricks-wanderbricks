"""
06_score.py — batch scoring: append forecasts for the next N days.

Reads the production model from the Model Registry, scores on the latest
gold features, and writes forecast rows to a Delta table. See README Part 2,
step 7.
"""
import mlflow
import pandas as pd
from datetime import date, timedelta


def score(horizon: int = 30) -> None:
    model_uri = "models:/forecast_model/Production"
    model = mlflow.pyfunc.load_model(model_uri)

    latest = spark.table("dev.gold.forecast_features").toPandas()
    # NOTE: production scoring needs the same lag/rolling features built up to
    # the prediction date; 03_features.py must run right before this task.
    forecast = model.predict(latest.tail(horizon))
    dates = [date.today() + timedelta(days=i) for i in range(1, horizon + 1)]

    out = pd.DataFrame({"forecast_date": dates, "value_forecast": forecast})
    spark.createDataFrame(out).write \
        .mode("append") \
        .saveAsTable("dev.gold.forecasts")


if __name__ == "__main__":
    score()
