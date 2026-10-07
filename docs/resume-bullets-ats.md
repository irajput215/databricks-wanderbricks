# Wanderbricks — ATS-Optimized Resume Bullets

Everything below is true of this repo and verified by real runs
(see `docs/portfolio.md` and `docs/development-log.md`). Bullets are written
for ATS parsing: action-verb start, keyword-dense (tools, methods, concepts
spelled out), quantified with verified numbers, single-column plain text.

> **Do-not-claim guardrail (from portfolio.md):** no GitHub Actions CI/CD, no
> `<50 ms` serving latency (measured warm ~1 s / cold ~42 s), no watermarking or
> sliding windows, no Airflow integration, and the registry alias is
> `@Production` (not `@Champion`). Nothing below violates this.

---

## Option A — ML / MLOps Engineer (primary)

1. **Architected and deployed** an end-to-end time-series forecasting platform on
   Databricks for daily weather prediction (NOAA GSOD, 11,329 stations),
   covering incremental ingestion, feature engineering, model training, batch
   scoring, and real-time serving.
2. **Built** a medallion-architecture data pipeline with Delta Live Tables
   (Bronze → Silver → Gold) and Auto Loader incremental ingestion from S3,
   implementing `@dlt.expect_or_drop` data-quality gates and unit
   standardization (US → metric, missing-value sentinels → NULL) for 11,329
   station files of raw weather data.
3. **Productionized** ML with MLflow and the Unity Catalog Model Registry:
   trained an XGBoost forecaster on engineered lag, rolling-mean, and calendar
   features using a no-leakage temporal split, achieving RMSE 2.50 °C and
   MAE 1.78 °C — a ~78% RMSE improvement over a Prophet baseline (11.57 °C).
4. **Deployed** dual inference paths: a serverless Model Serving REST endpoint
   (scale-to-zero, tested live) and a batch scoring job that generated 11,329
   station forecasts in 0.049 s (~230k forecasts/sec), with inference-time
   monitoring logged to MLflow and a Delta table.
5. **Automated** the daily ingest → train → score workflow with Databricks Jobs
   and Databricks Asset Bundles (project-as-code): dev/prod targets, CLI
   validate/deploy, and git-versioned infrastructure ready for CI/CD.
6. **Implemented** end-to-end observability with Lakehouse Monitoring (data
   quality and drift monitors), MLflow metrics (`inference_seconds`), and
   serving latency percentiles (p50/p95/p99) for the REST endpoint.
7. **Built** a Streamlit dashboard connected to the live serving endpoint, and
   enabled dual-mode notebooks that run in the Databricks workspace or locally
   via Databricks Connect and the Databricks SDK.
8. **Diagnosed and resolved** six production data-engineering issues (date-format
   mismatch, sticky Auto Loader schema state, US-unit conversion, Unity Catalog
   model-signature and alias constraints), documenting each decision in
   ADR-style records and shipping a fully verified end-to-end pipeline.

## Option B — Data / Platform Engineer

1. **Engineered** a streaming medallion pipeline (Bronze → Silver → Gold) in
   Delta Live Tables with Auto Loader incremental ingestion, controlled schema
   evolution, and declarative data-quality expectations, processing NOAA GSOD
   weather data for 11,329 global stations.
2. **Designed** temporal feature engineering (per-station lags, rolling means,
   calendar features) as a materialized view — the pattern that makes window
   functions legal where a streaming read would not allow them.
3. **Implemented** Unity Catalog governance (catalogs, schemas, volumes), model
   registry registration with enforced signatures and `@Production` aliases,
   and Lakehouse Monitoring for data quality and drift.
4. **Orchestrated** the daily ingest → train → score chain with native
   Databricks Jobs and Asset Bundles, replacing external Airflow dependency
   with declarative, version-controlled scheduling.
5. **Instrumented** batch inference performance, logging scoring throughput
   (11,329 forecasts in 0.049 s) to MLflow and a `scoring_metrics` Delta table
   for run-over-run tracking.

## Option C — Data Scientist / Forecasting

1. **Developed** a time-series forecasting model (XGBoost on lag, rolling-mean,
   and calendar features) that outperformed a Prophet baseline by ~78% RMSE
   (2.50 °C vs 11.57 °C), validated with a strictly prior-window temporal split
   to prevent data leakage.
2. **Built** an end-to-end forecasting system on Databricks from a public NOAA
   dataset, shipping predictions for 11,329 stations through batch scoring
   (0.049 s) and a live serverless REST endpoint.
3. **Tracked** experiments, model versions, and metrics with MLflow, enforcing
   model signatures and promoting the champion to `@Production` in the Unity
   Catalog Model Registry.
4. **Engineered** station-level temporal features (lags, rolling means, calendar
   indicators) with explicit missing-sentinel handling and unit
   standardization, ensuring train/serve feature consistency.

---

## Skills section block (drop into "Skills")

Databricks (Asset Bundles, Jobs, Serverless Compute), Delta Lake, Delta Live
Tables (DLT), Auto Loader, Unity Catalog, Apache Spark / PySpark, MLflow
(Tracking, Model Registry, Model Serving), Python, SQL, XGBoost, Prophet,
scikit-learn, pandas, NumPy, Time-Series Forecasting, Feature Engineering,
Data Quality & Validation, Medallion Architecture, Batch & Streaming
Ingestion, REST API Deployment, Streamlit, Databricks Connect, Databricks SDK,
Lakehouse Monitoring, Git, Infrastructure-as-Code, Datadog.

## ATS checklist (make these true before submitting)

- [ ] **Mirror the job description:** copy the JD's exact tool names and
      phrases (e.g., "Delta Lake", "MLflow", "Unity Catalog") into your
      bullets — ATS keyword matching is literal.
- [ ] **Use standard headings:** "Professional Experience", "Skills",
      "Education" — no creative section names.
- [ ] **Single-column layout:** no tables, graphics, columns, or text boxes;
      standard font (Arial/Calibri); save as `.docx` or text-based PDF.
- [ ] **Lead with the action verb** and put numbers in every bullet.
- [ ] **Spell out acronyms** at first use (e.g., "Delta Live Tables (DLT)").
- [ ] **No critical info in headers/footers** — ATS often skips them.
- [ ] Pick **one** option (A/B/C) per application and tailor to the role title.
