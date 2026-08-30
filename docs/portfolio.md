# Wanderbricks — Portfolio & Interview Guide

Everything in this file is **true of the code that exists in this repo** and
was **verified by real runs** (see `docs/development-log.md` for evidence).
Use these numbers in resumes, interviews, and the LinkedIn post — and do not
claim anything that is not here.

## Verified facts (memorize these)

| Fact | Verified value |
| --- | --- |
| Dataset | NOAA GSOD daily weather, public `s3://noaa-gsod-pds/` (AWS Open Data) |
| Stations ingested (2025) | **11,329** distinct stations (count from the scoring run) |
| Data quality | `@dlt.expect_or_drop` gates; unit standardization US→metric; sentinels → NULL |
| Baseline | Prophet, MLflow-tracked |
| ML model | XGBoost on lag + rolling + calendar features (per-station, one global model) |
| Registry | Unity Catalog model with signature + `@Production` **alias** (UC has no stages) |
| Batch scoring | **11,329 station forecasts in 0.0491 s** (~230k forecasts/sec) |
| Serving | Serverless endpoint, tested live (`{"predictions": [23.86]}`) |
| Serving latency | Warm round-trip **~1 s**; cold start (scale-to-zero) **~42 s** |
| Delivery | Databricks Asset Bundles, dev/prod targets, validate + deploy from CLI, git-versioned |
| Notebooks | Run in the workspace **and** locally (Databricks Connect + SDK), dual-mode bootstrap |
| Dashboard | Streamlit app hitting the live endpoint with real feature rows |
| Inference monitoring | `scoring_metrics` Delta table + MLflow `inference_seconds` |

## Resume bullets

### Option A — MLOps / ML Engineer

- Built an end-to-end weather-forecasting platform on Databricks: DLT medallion
  pipeline (Bronze→Silver→Gold) with **Auto Loader incremental ingestion** of a
  public NOAA dataset (11,329 stations), `@dlt.expect_or_drop` data-quality
  gates, and unit standardization (US→metric, missing-sentinel handling).
- Productionized ML with **MLflow**: experiment tracking, model signature
  enforcement, Unity Catalog Model Registry with a **`@Production` alias**;
  Prophet baseline vs XGBoost on lag/rolling features with a no-leakage
  temporal split.
- Shipped **dual inference**: a serverless real-time serving endpoint
  (scale-to-zero, tested live) and batch scoring of **11,329 forecasts in
  0.049 s (~230k/s)**, with inference-time monitoring (`scoring_metrics` +
  MLflow).
- Adopted **project-as-code** with Databricks Asset Bundles: dev/prod targets,
  CLI validate/deploy, everything git-versioned.
- Built a **Streamlit live dashboard** over the endpoint (real feature rows,
  measured latency) and made all notebooks run in-workspace or locally via
  Databricks Connect.

### Option B — Data / Platform Engineer

- Designed a **Medallion streaming pipeline** (Bronze→Silver→Gold) with Auto
  Loader incremental ingestion, DLT expectations for data quality, and
  explicit unit conversion + missing-value handling.
- Engineered **temporal features** (per-station lags, rolling means, calendar)
  as a materialized view — the choice that makes window functions legal where
  a streaming read would not allow them.
- Structured **governance** with Unity Catalog (catalog/schema/volumes),
  Lakehouse Monitoring statements, and a `scoring_metrics` inference-time
  table.
- Orchestrated the daily **ingest → train → score** chain with Databricks Jobs
  + bundles — native scheduling, no Airflow dependency.

## LinkedIn post

> **Hook:** "How I built a full-stack ML forecasting platform on Databricks —
> from a public NOAA dataset to a live serverless API and a Streamlit
> dashboard, with zero Airflow overhead."
>
> - Incremental streaming ingest: Auto Loader + Delta Lake (11,329 stations)
> - Data-quality gates and US→metric standardization in Delta Live Tables
> - No data leakage in temporal features (strict prior-window lags/rolling)
> - Dual deployment: real-time serverless endpoint + batch scoring at ~230k
>   forecasts/sec
> - Project-as-code with Databricks Asset Bundles; notebooks run in-workspace
>   and locally
> - Live demo: [link to Streamlit app] · Code: github.com/irajput215/databricks-wanderbricks

## Interview prep — how to talk about it

**Lead with the debugging saga** — it is your strongest signal. Six real
failures, each diagnosed and fixed:

1. DATE format mismatch (bucket uses ISO, not `yyyyMMdd`) → empty silver
2. Bucket layout discovery: `<year>/<station>.csv`, not `.op.gz`; subset schema
   maps positionally → misaligned bronze
3. Auto Loader **persists schema state** — it evolves instead of replacing;
   fix = fresh schema-tracking path + `schemaEvolutionMode=none` + clean reset
4. US units (°F/inches/knots), not metric tenths → conversions in silver
5. Unity Catalog requires **model signatures**
6. Unity Catalog forbids stages → **aliases** (`@Production`)

**Do not claim** (not implemented here — reviewable in minutes):
- Watermarking / sliding windows on a stream (features use a materialized view)
- Liquid clustering, multi-terabyte scale, GitHub Actions CI/CD
- "<50 ms" serving latency (measured: warm ~1 s, cold ~42 s with scale-to-zero)
- Anything about Airflow *integration* (we chose Databricks Jobs natively)
- `@Champion` alias (we use `@Production`)

**If asked "what would you do next?":** retraining-on-drift task, station-aware
modeling (encode station or per-station models), prod promotion, full-history
ingest, Datadog wiring. These are all documented as next steps.

## Demo tips

- **Warm the endpoint before recording** (`curl` once) — a scale-to-zero cold
  start is ~42 s and kills a demo.
- Screen-record the Streamlit app with a station change + latency badge, then
  a Databricks job-run DAG (the pipeline flow is visual proof).
- Keep `docs/development-log.md` and `docs/decisions.md` handy — they show
  engineering judgment, not just "it worked".
