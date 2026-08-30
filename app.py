"""
Wanderbricks — Streamlit dashboard for the live weather-forecast endpoint.

Shows the real-time forecast from the Databricks Model Serving endpoint,
with:
  - a geospatial map of stations (name + coordinates, colored by temperature),
  - a station picker (real names + lat/lon from gsod_silver),
  - live prediction + measured latency,
  - observed-vs-forecast chart,
  - model monitoring: batch inference time (scoring_metrics) + MLflow runs.

Runs locally (profile auth) or on Streamlit Community Cloud (env secrets).

Run locally:
    uv run streamlit run app.py
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
CATALOG = os.environ.get("CATALOG", "workspace")
SCHEMA = os.environ.get("SCHEMA", "iraonfridays")

FEATURE_COLS = [
    "day_of_week", "day_of_year", "month", "is_weekend",
    "temp_lag_1", "temp_lag_7", "temp_lag_14", "temp_lag_28",
    "temp_rollmean_7", "temp_rollmean_14",
]

st.set_page_config(page_title="Wanderbricks", page_icon="🌤️", layout="wide")
st.title("🌤️ Wanderbricks — Weather Forecast")
st.caption(f"Endpoint: `{ENDPOINT}` · data: NOAA GSOD via Databricks")


# ----------------------------------------------------------------- spark/connect
def _get_token() -> str:
    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        return token
    try:
        out = subprocess.check_output(
            ["databricks", "auth", "token", "irajput"], timeout=15)
        return json.loads(out)["access_token"]
    except Exception as exc:  # noqa: BLE001
        st.error(f"No token: set DATABRICKS_TOKEN or fix the CLI cache ({exc})")
        st.stop()


_spark = None


def get_spark():
    """Lazily create one Databricks Connect session (local only)."""
    global _spark
    if _spark is None:
        try:
            from databricks.connect import DatabricksSession
            _spark = DatabricksSession.builder.profile("irajput").serverless().getOrCreate()
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Databricks Connect unavailable ({exc}) — sample data only.")
            return None
    return _spark


def _sample_frame() -> pd.DataFrame:
    sample = json.load(open("data.json"))["dataframe_records"][0]
    return pd.DataFrame([{
        "station": "SAMPLE", "station_name": "Sample station",
        "latitude": None, "longitude": None, "elevation": None, **sample,
    }])


# ----------------------------------------------------------------- data (cached)
@st.cache_data(ttl=600, show_spinner=False)
def load_stations_and_features() -> pd.DataFrame:
    """Per-station metadata (name/lat/lon) + latest feature row + latest temp."""
    spark = get_spark()
    if spark is None:
        return _sample_frame()
    feats = (
        spark.table(f"{CATALOG}.{SCHEMA}.weather_features")
        .drop("date").toPandas().dropna()
    )
    latest = feats.groupby("station").tail(1)
    meta = (
        spark.table(f"{CATALOG}.{SCHEMA}.gsod_silver")
        .select("station", "station_name", "latitude", "longitude", "elevation")
        .dropDuplicates(["station"])
        .toPandas()
    )
    df = latest.merge(meta, on="station", how="left")
    df = df.dropna(subset=["latitude", "longitude"])
    return df


@st.cache_data(ttl=600, show_spinner=False)
def load_history(station: str) -> pd.DataFrame:
    spark = get_spark()
    if spark is None:
        return pd.DataFrame()
    return (
        spark.table(f"{CATALOG}.{SCHEMA}.gsod_silver")
        .filter(f"station = '{station}'")
        .select("date", "temp_c")
        .filter("temp_c IS NOT NULL")
        .orderBy("date", ascending=False)
        .limit(120)
        .toPandas()
        .sort_values("date")
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_scoring_metrics() -> pd.DataFrame:
    spark = get_spark()
    if spark is None:
        return pd.DataFrame()
    try:
        return (
            spark.table(f"{CATALOG}.{SCHEMA}.scoring_metrics")
            .orderBy("run_date")
            .toPandas()
        )
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def load_mlflow_runs() -> pd.DataFrame:
    try:
        from mlflow.tracking import MlflowClient
        client = MlflowClient(tracking_uri=f"databricks://irajput")
        exp = next(
            (e for e in client.search_experiments()
             if e.name == "/Shared/wanderbricks_forecast"), None)
        if exp is None:
            return pd.DataFrame()
        rows = []
        for run in client.search_runs(
                [exp.experiment_id], order_by=["start_time DESC"], max_results=20):
            m = run.data.metrics
            p = run.data.params
            rows.append({
                "run": run.data.tags.get("mlflow.runName", run.info.run_id[:8]),
                "model": p.get("model", "?"),
                "rmse": round(m.get("rmse", float("nan")), 3),
                "mae": round(m.get("mae", float("nan")), 3),
                "inference_s": round(m.get("inference_seconds", float("nan")), 4),
            })
        return pd.DataFrame(rows)
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


# ----------------------------------------------------------------- endpoint
def call_endpoint(payload: dict) -> tuple[float, float]:
    t0 = time.perf_counter()
    resp = requests.post(
        f"{HOST}/serving-endpoints/{ENDPOINT}/invocations",
        headers={"Authorization": f"Bearer {_get_token()}"},
        json={"dataframe_records": payload},
        timeout=90,
    )
    elapsed = time.perf_counter() - t0
    resp.raise_for_status()
    return float(resp.json()["predictions"][0]), elapsed


# ----------------------------------------------------------------- UI
stations = load_stations_and_features()
if stations.empty:
    st.error("No station data available (Connect offline and no sample).")
    st.stop()

# Geospatial map (click a station to select it)
map_fig = go.Figure(go.Scattergeo(
    lat=stations["latitude"], lon=stations["longitude"],
    mode="markers",
    marker=dict(
        size=5, color=stations["temp_c"],
        colorscale="RdYlBu_r", showscale=True,
        colorbar=dict(title="°C"),
    ),
    text=[
        f"{n} ({s})" for n, s in zip(stations["station_name"], stations["station"])],
    customdata=stations["station"],
    hovertemplate="%{text}<br>temp %{marker.color:.1f} °C<extra></extra>",
))
map_fig.update_geos(showland=True, landcolor="rgba(200,220,230,0.4)",
                    showcountries=True, coastlinecolor="grey")
map_fig.update_layout(height=360, margin=dict(l=0, r=0, t=10, b=0),
                      title="All stations — latest temperature")
st.plotly_chart(map_fig, use_container_width=True, on_select="rerun")

# Selected station from map click or dropdown
try:
    sel = st.session_state.get("selection")
    if sel and sel.get("points"):
        st.session_state["station"] = sel["points"][0]["customdata"]
except Exception:  # noqa: BLE001
    pass

labels = {
    s: f"{n} — {s} ({lat:.2f}, {lon:.2f})" if pd.notna(lat) else f"{n} — {s}"
    for s, n, lat, lon in zip(stations["station"], stations["station_name"],
                              stations["latitude"], stations["longitude"])
}
station = st.selectbox(
    "Weather station", options=stations["station"].tolist(),
    format_func=lambda s: labels.get(s, s),
    key="station",
)

row = stations[stations["station"] == station].iloc[0]
payload = [{c: float(row[c]) for c in FEATURE_COLS}]

col_l, col_r = st.columns([1, 2])
pred_c = None
with col_l:
    st.subheader("Live prediction")
    st.caption(f"{row['station_name']} · lat {row['latitude']:.2f} · "
               f"lon {row['longitude']:.2f} · elev {row['elevation']:.0f} m")
    try:
        pred_c, latency = call_endpoint(payload)
        st.metric("Predicted temp", f"{pred_c:.1f} °C")
        st.metric("Round-trip latency", f"{latency * 1000:.0f} ms")
    except Exception as exc:  # noqa: BLE001
        st.error(f"Endpoint call failed: {exc}")

with col_r:
    st.subheader("History + forecast")
    hist = load_history(station)
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

# ---------------------------------------------------------------- monitoring
st.divider()
st.subheader("📈 Model monitoring")

scoring = load_scoring_metrics()
runs = load_mlflow_runs()

c1, c2 = st.columns(2)
with c1:
    st.markdown("**Batch inference time** (`scoring_metrics`)")
    if not scoring.empty:
        st.dataframe(scoring, use_container_width=True, hide_index=True)
        st.line_chart(scoring.set_index("run_date")["inference_seconds"])
    else:
        st.caption("No scoring_metrics rows (run the job first).")
with c2:
    st.markdown("**Recent MLflow runs** (`/Shared/wanderbricks_forecast`)")
    if not runs.empty:
        st.dataframe(runs, use_container_width=True, hide_index=True)
    else:
        st.caption("No MLflow runs found (run the job first).")

st.caption(
    "Live endpoint latency is measured per call here. Server-side p50/p95/p99 "
    "latency is in the endpoint's Metrics tab in Databricks.")

st.caption(
    "Forecast from `wanderbricks_weather_xgb` (XGBoost on lag/rolling features). "
    "Deployed via Databricks Asset Bundles; source: github.com/irajput215/databricks-wanderbricks")
