"""
Wanderbricks — Streamlit dashboard for the live weather-forecast endpoint.

Shows the real-time forecast from the Databricks Model Serving endpoint
(`wanderbricks_weather_xgb`), with a station picker backed by REAL feature
rows from `weather_features` and history from `gsod_silver`, plus a measured
latency badge. Runs locally (profile auth) or on Streamlit Community Cloud
(env secrets).

Run locally:
    uv run streamlit run app.py

Cloud (Streamlit Community Cloud -> Secrets):
    DATABRICKS_HOST=https://dbc-2944edfb-cd25.cloud.databricks.com
    DATABRICKS_TOKEN=<token>
    ENDPOINT_NAME=dev_iraonfridays_wanderbricks-weather-serve   # dev
    # ENDPOINT_NAME=wanderbricks-weather-serve                  # prod
"""
import json
import os
import subprocess
import time

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

HOST = os.environ.get("DATABRICKS_HOST", "https://dbc-2944edfb-cd25.cloud.databricks.com")
ENDPOINT = os.environ.get(
    "ENDPOINT_NAME", "dev_iraonfridays_wanderbricks-weather-serve")

FEATURE_COLS = [
    "day_of_week", "day_of_year", "month", "is_weekend",
    "temp_lag_1", "temp_lag_7", "temp_lag_14", "temp_lag_28",
    "temp_rollmean_7", "temp_rollmean_14",
]

st.set_page_config(page_title="Wanderbricks", page_icon="🌤️", layout="wide")
st.title("🌤️ Wanderbricks — Weather Forecast")
st.caption(f"Endpoint: `{ENDPOINT}` · data: NOAA GSOD via Databricks")


# ----------------------------------------------------------------- config
def get_token() -> str:
    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        return token
    try:  # local: reuse the CLI token cache
        out = subprocess.check_output(
            ["databricks", "auth", "token", "irajput"], timeout=15)
        return json.loads(out)["access_token"]
    except Exception as exc:  # noqa: BLE001
        st.error(f"No token: set DATABRICKS_TOKEN or fix the CLI cache ({exc})")
        st.stop()


# ----------------------------------------------------------------- data
@st.cache_data(ttl=300, show_spinner=False)
def load_station_features() -> pd.DataFrame:
    """Latest feature row per station + recent history, via Databricks Connect.

    Falls back to a single sample row (data.json) if Connect is unavailable
    (e.g. Streamlit Cloud without a configured session).
    """
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.profile("irajput").serverless().getOrCreate()
        feats = (
            spark.table("workspace.iraonfridays.weather_features")
            .drop("date").toPandas().dropna()
        )
        latest = feats.groupby("station").tail(1)
        history = (
            spark.table("workspace.iraonfridays.gsod_silver")
            .select("station", "date", "temp_c")
            .filter("temp_c IS NOT NULL")
            .toPandas()
        )
        spark.stop()
        return latest, history
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Databricks Connect unavailable ({exc}) — using sample payload.")
        sample = json.loads(open("data.json").read())["dataframe_records"][0]
        df = pd.DataFrame([{"station": "SAMPLE", **sample}])
        return df, pd.DataFrame()


# ----------------------------------------------------------------- serve
def call_endpoint(payload: dict) -> tuple[float, float]:
    t0 = time.perf_counter()
    resp = requests.post(
        f"{HOST}/serving-endpoints/{ENDPOINT}/invocations",
        headers={"Authorization": f"Bearer {get_token()}"},
        json={"dataframe_records": payload},
        timeout=60,
    )
    elapsed = time.perf_counter() - t0
    resp.raise_for_status()
    return float(resp.json()["predictions"][0]), elapsed


# ----------------------------------------------------------------- UI
latest, history = load_station_features()
stations = sorted(latest["station"].unique().tolist())
station = st.selectbox("Weather station", stations, index=0)

row = latest[latest["station"] == station].iloc[0]
payload = [{c: float(row[c]) for c in FEATURE_COLS}]

col_l, col_r = st.columns([1, 2])
pred_c = None
with col_l:
    st.subheader("Live prediction")
    try:
        pred_c, latency = call_endpoint(payload)
        st.metric("Predicted temp", f"{pred_c:.1f} °C")
        st.metric("Round-trip latency", f"{latency * 1000:.0f} ms")
    except Exception as exc:  # noqa: BLE001
        st.error(f"Endpoint call failed: {exc}")

with col_r:
    st.subheader("History + forecast")
    hist = history[history["station"] == station].sort_values("date").tail(60)
    fig = go.Figure()
    if not hist.empty:
        fig.add_trace(go.Scatter(
            x=hist["date"], y=hist["temp_c"], mode="lines", name="observed °C"))
    if pred_c is not None:
        fig.add_trace(go.Scatter(
            x=[hist["date"].iloc[-1]] if not hist.empty else [None],
            y=[pred_c], mode="markers", marker=dict(size=12, color="red"),
            name="forecast"))
    fig.update_layout(height=320, margin=dict(l=10, r=10, t=20, b=10))
    st.plotly_chart(fig, use_container_width=True)

st.caption(
    "Forecast from `wanderbricks_weather_xgb` (XGBoost on lag/rolling features). "
    "Deployed via Databricks Asset Bundles; source: github.com/irajput215/databricks-wanderbricks")
