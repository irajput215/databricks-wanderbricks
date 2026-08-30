# Wanderbricks — Databricks End-to-End Forecasting Project

An end-to-end **data + ML/AI engineering** project on Databricks for **time-series
forecasting**, built the modern way: project-as-code with the Databricks CLI,
Delta Lake + Delta Live Tables for the data layer, MLflow for tracking and the
model registry, Databricks Jobs for orchestration, and Lakehouse Monitoring +
Datadog for observability — with a retraining loop so the model stays fresh.

This README teaches the platform first (so every feature later in the procedure
makes sense), then lays out the project step by step.

> **Docs:** the running development timeline lives in
> [`docs/development-log.md`](docs/development-log.md) and the decision records
> (why each choice was made) in [`docs/decisions.md`](docs/decisions.md).

## Project status — first full end-to-end run: SUCCESS ✅

Live in the dev workspace (`dbc-2944edfb-cd25.cloud.databricks.com`), against
real NOAA GSOD 2025 data:

| Stage | Result |
| --- | --- |
| DLT pipeline (bronze → silver → gold) | ✅ three flows, real °C data |
| Prophet baseline | ✅ MLflow-tracked |
| XGBoost train | ✅ registered `wanderbricks_weather_xgb` v1 → `@Production` |
| Batch scoring | ✅ **11,329 station forecasts in 0.0491 s** |
| Monitoring | ✅ `scoring_metrics` (run_date, rows, inference_seconds) |

**What it took (the real-world lessons, recorded in `docs/decisions.md` D14–D16
and the dev log):** the AWS GSOD bucket stores US units (°F/inches/knots) in
`<year>/<station>.csv` layout; Auto Loader schema state is sticky (fresh path +
`schemaEvolutionMode=none` on schema changes); Unity Catalog models need
signatures and use aliases instead of stages.

**Serving endpoint: LIVE** — `wanderbricks_weather_serve` is deployed and tested
(`{"predictions": [23.86]}`). Dev mode prefixes resource names (`dev_<user>_`), so
the endpoint is `dev_iraonfridays_wanderbricks-weather-serve` in dev and
`wanderbricks-weather-serve` in prod — `notebooks/04_test_serving` has the dev
name as default. Remaining: promote to prod, add the retraining-on-drift task,
and optionally wire Datadog.

---

## Part 1 — Databricks feature tour (with examples)

Everything below is a feature of the Databricks platform. The CLI (`databricks`)
and notebooks/SQL are the two surfaces you drive them from. "Warehouse" = your
workspace URL; you can run every example below in a notebook or SQL editor.

### 1. Workspace, notebooks, and repos

The workspace is your IDE + file system in the cloud. Notebooks are per-language
(Python, SQL, Scala, R) and cells run on a cluster.

```
Workspace/
├── Users/<you>/
│   ├── 01_ingest.py          ← notebooks or Python files
│   └── shared/               ← team folders
└── Repos/<you>/<git-repo>/   ← sync from GitHub/GitLab/Bitbucket
```

### 2. Clusters

Compute for notebooks and jobs. Two flavors:

- **All-purpose clusters** — for interactive development (SQL + notebook runs).
- **Job clusters** — ephemeral, created per job run, auto-terminate after; cheaper.

```sh
databricks clusters list                          # see your clusters
databricks clusters start <cluster-id>            # start one
# Or let Jobs manage ephemeral clusters automatically — recommended.
```

`Databricks Runtime` = Spark + Delta + MLflow + libraries preinstalled
(`ML Runtime` adds the ML stack: XGBoost, Prophet, sklearn, etc.).

### 3. SQL Warehouse

A dedicated compute for SQL analytics — **the "warehouse" in Lakehouse**.
You query Delta tables with plain SQL, and power dashboards from it.

```sql
SELECT date, sum(revenue) AS daily_revenue
FROM gold.daily_revenue
GROUP BY date
ORDER BY date;
```

### 4. Unity Catalog

Governance layer: catalogs → schemas → tables, with permissions, lineage, and
audit. It replaces the old per-workspace metastore.

```sql
CREATE CATALOG IF NOT EXISTS dev;
CREATE SCHEMA IF NOT EXISTS dev.bronze;
GRANT SELECT ON SCHEMA dev.bronze TO `data-eng@company.com`;
```

Everything in this project lives under a catalog, e.g. `main` or your own.

### 5. Delta Lake — the storage format

ACID transactions on cloud storage (S3/ADLS/GCS) with time travel, schema
evolution, and file optimization. Every table in this project is Delta.

```sql
-- Time travel: read a table as it was N versions ago
SELECT * FROM dev.gold.forecast_features VERSION AS OF 5;

-- History of every change
DESCRIBE HISTORY dev.gold.forecast_features;
```

```python
# Merge upserts (CDC-style)
from delta.tables import DeltaTable
DeltaTable.forName(spark, "dev.silver.cleaned_sales") \
    .alias("t") \
    .merge(df.alias("s"), "t.id = s.id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

```sh
# Compact small files + drop old versions (run in a notebook cell)
OPTIMIZE dev.silver.cleaned_sales;
VACUUM dev.silver.cleaned_sales RETAIN 168 HOURS;
```

### 6. Auto Loader — incremental ingestion from cloud storage

Reads new files as they land in S3, tracks what it has seen, infers/validates
schema. **This is how your S3 data enters the lakehouse.**

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")     # or csv / json
    .option("cloudFiles.schemaLocation", "/Volumes/dev/bronze/_schemas/sales")
    .load("s3://your-bucket/raw/sales/")
)

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/Volumes/dev/bronze/_checkpoints/sales") \
    .table("dev.bronze.sales_raw")
```

### 7. Delta Live Tables (DLT) — declarative pipelines

**Your data-engineering tool.** Instead of manually chaining
spark.read → write → read → write, you declare tables and the dependency graph,
and DLT runs the pipeline with built-in quality checks, retries, and
incremental processing.

```python
import dlt
from pyspark.sql.functions import col

@dlt.table
def sales_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load("s3://your-bucket/raw/sales/")
    )

@dlt.table
@dlt.expect("valid_id", "id IS NOT NULL")
@dlt.expect("positive_revenue", "revenue >= 0")
def sales_clean():
    return (
        dlt.read("sales_raw")
        .filter(col("id").isNotNull())
        .withColumn("sale_date", col("sale_date").cast("date"))
    )
```

Declared expectations = **built-in data quality monitoring** (the DLT answer to
running your own data-quality tool). `databricks bundle deploy` runs this.

### 8. MLflow — experiment tracking + model registry

Databricks' native ML lifecycle tool (it IS the company that created MLflow).

```python
import mlflow

mlflow.set_experiment("/Users/you/wanderbricks")
with mlflow.start_run():
    mlflow.log_param("model", "xgboost")
    mlflow.log_param("lags", 14)
    mlflow.log_metric("rmse", 12.3)
    mlflow.log_artifact("features.csv")
    mlflow.xgboost.log_model(model, "model")   # any framework works

    # Register to the Model Registry with a stage
    model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
    mlflow.register_model(model_uri, "wanderbricks_weather_xgb")
```

The **Model Registry** stages models (`None → Staging → Production → Archived`)
with versioning and lineage — the backbone of your retraining loop.

### 9. Feature Store

Central place for ML features so training and serving use the same data.

```python
from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()

fs.create_table(
    name="dev.gold.forecast_features",
    primary_keys=["date"],
    df=features_df,
    schema=features_df.schema,
)
```

### 10. Model Serving — real-time + batch inference

Deploy a registered model as a REST endpoint (serverless) or run batch scoring
as a job.

```sh
databricks serving-endpoints create \
  --name forecast-serve \
  --config '{"served_models":[{"model_name":"wanderbricks_weather_xgb","model_version":"2"}]}'
```

```sh
curl -X POST https://<workspace>/serving-endpoints/forecast-serve/invocations \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -d '{"dataframe_split": {"columns": ["date"], "data": [["2026-09-01"]]}}'
```

### 11. Workflows / Jobs — orchestration

**Your orchestrator.** Jobs chain tasks (notebooks, Python files, DLT
pipelines, SQL), with dependencies, retries, schedules, and alerting — all
declared in the bundle.

```yaml
resources:
  jobs:
    wanderbricks_pipeline:
      name: wanderbricks-pipeline
      schedule: { quartz_cron_expression: "0 0 6 * * ?", timezone_id: UTC }
      tasks:
        - task_key: ingest          # notebook or .py
          notebook_task: { notebook_path: "/pipelines/01_ingest" }
        - task_key: clean
          depends_on: [{ task_key: ingest }]
          notebook_task: { notebook_path: "/pipelines/02_clean" }
        - task_key: train
          depends_on: [{ task_key: clean }]
          notebook_task: { notebook_path: "/ml/03_train" }
        - task_key: refresh_features
          depends_on: [{ task_key: clean }]
          notebook_task: { notebook_path: "/ml/04_refresh_features" }
```

**Airflow, if you want it alongside:** the official
`apache-airflow-providers-databricks` gives Airflow operators
(`DatabricksSubmitRunOperator`, `DatabricksCreateJobsOperator`) so an external
Airflow DAG can trigger Databricks jobs. Databricks Jobs is the native choice;
Airflow is worth it only when you already run Airflow for other systems.

### 12. Monitoring — Lakehouse Monitoring + Datadog

**Databricks Lakehouse Monitoring** tracks data quality, drift, and freshness
on any table, and can alert on monitors:

```sql
CREATE MONITOR dev.gold.forecast_features
  ON TABLE dev.gold.forecast_features
  WITH (metric_type = 'table', schedule = 'DAILY');
```

**Datadog integration** — Databricks can export workspace metrics
(cluster/job/DBU/streaming metrics) to Datadog via the **Datadog
Databricks Integration** (metrics export) and the **Databricks plugin for
Datadog** (log/trace collection from jobs). You monitor Databricks *from*
Datadog, and monitor your *data* with Lakehouse Monitoring. Typical split:
Datadog = infrastructure + job health; Lakehouse Monitoring = data quality +
model drift. Model-serving latency and job failures also flow into Datadog.

### 13. Databricks Asset Bundles — project as code

**The CLI's killer feature.** One `databricks.yml` at the repo root declares
all resources (jobs, DLT pipelines, serving endpoints) + targets (dev/prod):

```sh
databricks auth login --host https://<workspace>.cloud.databricks.com
databricks bundle init --template default-dlt   # scaffold from a template
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run wanderbricks_pipeline --refresh-all
databricks bundle destroy --target dev          # teardown
```

Everything is version-controlled and CI/CD-ready (GitHub Actions has the
first-party `databricks/setup-cli` action).

### 14. AI/GenAI extras (for later)

Foundation Model APIs (served models like Llama/GPT as `POST /serving-endpoints/`),
Vector Search + embeddings for RAG, and Agent Framework for building agents —
all governed by Unity Catalog. A forecasting project doesn't need them, but
know they exist on the same platform.

---

## Part 2 — The end-to-end forecasting procedure

### Goal

Take raw time-series data (from S3 — added later) and ship a **forecasting
model** that is automatically refreshed, monitored, and retrained — the full
data + ML engineering lifecycle.

### Architecture

```
S3 (raw data)                        ← you will attach this later
   │  Auto Loader (incremental, schema-tracked)
   ▼
dev.bronze.sales_raw        ── Delta, append-only raw
   │  DLT pipeline: clean + expectations
   ▼
dev.silver.sales_clean      ── dedup, types, quality checks (DLT expectations)
   │  DLT/feature build
   ▼
dev.gold.forecast_features  ── date-based feature table (Feature Store)
   │
   ├──► Train (MLflow): Prophet/ARIMA baseline + XGBoost w/ lag features
   │        ├─ log params/metrics/artifacts
   │        └─ register best → Model Registry (Staging → Production)
   ├──► Serve: Model Serving endpoint (real-time) or batch scoring job
   ├──► Monitor: Lakehouse Monitoring (drift/quality) + Datadog (jobs/infra)
   └──► Retrain loop: daily job → detect drift → retrain → promote via registry
```

### Folder layout (this repo)

```
wanderbricks/
├── README.md                 ← you are here
├── databricks.yml            ← bundle: targets, jobs, pipelines, serving
├── pipelines/
│   ├── 01_ingest.py          ← Auto Loader → bronze
│   ├── 02_clean.py           ← DLT expectations → silver
│   └── 03_features.py        ← feature table → gold
├── ml/
│   ├── 04_baseline.py        ← Prophet/ARIMA baseline
│   ├── 05_train_xgb.py       ← lag features + XGBoost, MLflow tracked
│   └── 06_score.py           ← batch scoring or serving refresh
├── monitoring/
│   └── monitors.sql          ← CREATE MONITOR statements
└── notebooks/                ← scratch / exploration
```

### Step-by-step

1. **Ingest (bronze)** — Auto Loader streams S3 files into `dev.bronze.sales_raw`
   with schema tracking and a checkpoint.
2. **Clean (silver)** — a DLT pipeline dedupes, casts, and applies
   `@dlt.expect` quality rules; failures are recorded, not silently dropped.
3. **Features (gold)** — build the date-keyed feature table (calendar features,
   lags, rolling stats) and register it in the Feature Store.
4. **Baseline forecast** — Prophet (or ARIMA) as the simple, explainable
   baseline; log its RMSE as the bar to beat.
5. **ML forecast** — XGBoost (or similar) on lag/rolling features; tune with
   MLflow tracking; compare against the baseline in the experiment UI.
6. **Register** — the best run's model goes to the Model Registry
   (`Staging` → validate → `Production`).
7. **Serve** — real-time endpoint via `serving-endpoints`, or a scheduled batch
   scoring task appending forecasts to a Delta table.
8. **Orchestrate** — one Databricks Job chains ingest → clean → features →
   train-or-skip → score, on a daily schedule with alerting (that's your
   "Airflow" role, native; add the Airflow provider only if you run Airflow).
9. **Monitor** — `CREATE MONITOR` on gold features (drift/quality) +
   Datadog integration for job health and serving latency.
10. **Retrain loop** — the daily job checks forecast error vs. a threshold
    (MLflow metric comparison); if drift/error breaches, it retrains and
    registers a new version; promotion to Production stays a human decision
    (or automated with an approval policy you set).

### Your stated learning targets, mapped

| You want to learn | Where it lives in this project |
|---|---|
| DLT (declarative pipelines) | Steps 2–3: silver/gold are DLT tables with expectations |
| Data quality / monitoring | DLT expectations + Lakehouse Monitoring monitors |
| MLflow | Steps 4–6: experiments, registry, lineage |
| Orchestration | Step 8: Databricks Jobs (Airflow provider optional) |
| Retraining | Step 10: scheduled freshness/error check → retrain → promote |
| Datadog (optional) | Step 9: metrics export + job logs into Datadog |

---

## Getting started checklist

- [ ] Databricks workspace ready (yours: yes)
- [ ] `brew install databricks/tap/databricks` and `databricks auth login`
- [ ] A Unity Catalog catalog/schema (e.g. `dev.bronze|silver|gold`)
- [ ] S3 bucket with raw data wired to the workspace (you will add this)
- [ ] `databricks bundle init` → scaffold this repo's `databricks.yml`
- [ ] Follow Part 2, step by step, checking off each stage

> Note: this repo intentionally does not contain credentials. Auth lives in
> `~/.databrickscfg`; secrets stay in Databricks Secrets, never in git.

---

## Part 3 — The real dataset: NOAA GSOD (weather forecasting)

Wanderbricks now targets **NOAA Global Surface Summary of the Day (GSOD)** —
daily weather summaries for 9000+ stations worldwide, 1929 to present, updated
1–2 days behind real time, served from the public bucket `s3://noaa-gsod-pds/`.

**Why it fits:** daily granularity (perfect for Prophet / lag-based XGBoost),
decades of history, and a real-world streaming-style ingestion source.

### Batch vs. real-time — the decision that matters

| | Triggered (this project) | Continuous |
| :--- | :--- | :--- |
| DLT setting | `continuous: false` (default) | `continuous: true` |
| When it runs | On schedule / on job call | 24/7 |
| Pickup | Auto Loader picks up new files each run | Auto Loader picks up within seconds |
| Cost | Compute up, process, down | Serverless auto-scale, always on |
| Right for | Daily data with 1–2 day freshness | Sub-minute freshness (Kafka, IoT) |

GSOD is **daily data** → triggered is the correct, cost-efficient choice. The
pipeline code is identical either way (Auto Loader `readStream` + DLT streaming
tables); flip `continuous: true` only if you later move to an hourly/Kafka
source. For sub-second serving, register the model and add a Model Serving
endpoint (README Part 1, step 10) — the endpoint config is added once the first
model version exists.

### GSOD data facts wired into the code

- **Layout:** `s3://noaa-gsod-pds/<year>/<station>.csv` — one plain quoted CSV per
  station per year (the AWS bucket, not the classic `.op.gz` layout).
  `01_ingest.py` currently points at a single recent year for validation; widen
  to the bucket root for full history.
- **US units (verified on the live bucket):** temperatures in **°F**, precipitation
  in **inches**, wind in **knots** — converted to °C/mm in `02_clean.py`.
- **Full 28-column schema in bronze** — including the `*_ATTRIBUTES` quality-flag
  columns; a subset schema misaligns after `NAME` (verified on a failed run).
- **Missing values are sentinels:** `9999.9` (temps/pressure), `99.99` (precip),
  `999.9` (wind/visibility) → converted to `NULL` in `02_clean.py`.
- **Quality gates:** `@dlt.expect_or_drop` drops rows with null dates/stations
  or physically impossible temperatures.
- **Features (`weather_features`):** per-station day-of-week/year/month,
  weekend flag, `temp_lag_{1,7,14,28}`, `temp_rollmean_{7,14}` (strictly prior
  days), with `temp_c` as the forecast target.
- **Public bucket access:** reading `s3://noaa-gsod-pds/` works from serverless
  compute for workspace admins. If access is ever denied, either set the
  anonymous AWS credentials provider as a Spark config or copy a subset into
  your own bucket.

### End-to-end flow

```
s3://noaa-gsod-pds/2025/  ──Auto Loader──►  gsod_bronze (ST)
                                              │  DLT: clean + unit conversion
                                              ▼
                                          gsod_silver (ST)  ← @dlt.expect_or_drop
                                              │  DLT: features (materialized view)
                                              ▼
                                        weather_features (MV)
                                              │
                    ┌─────────────────────────┼──────────────────────────┐
                    ▼                         ▼                          ▼
             04_baseline.py           05_train_xgb.py            06_score.py
             (Prophet, MLflow)        (register model)          (write forecasts)
```

The whole chain is one daily Databricks job: DLT pipeline → baseline → train →
score, deployed and runnable from the bundle.

---

## Part 4 — Monitoring, pipeline flow & testing the serving model

### 1. See the flow of the data pipeline

Every run renders as a **task DAG** — this is the ground truth of the flow:

- **Job run page** (workspace → Workflows → the run) draws the graph
  `dlt_pipeline → baseline → train → score` with per-task duration, cluster
  info, and logs.
- **DLT pipeline UI** draws its own bronze → silver → gold graph with per-table
  rows/quality metrics and data-lineage links.
- **CLI:**
  ```sh
  databricks bundle run wanderbricks_job --refresh-all       # run + follow progress
  databricks jobs list-runs --job-id <job-id>                # history
  databricks jobs get-run --run-id <run-id>                  # task states + durations
  ```

### 2. Batch inference / prediction time

`06_score.py` now times the scoring call and records it two ways:

- **MLflow metric** `inference_seconds` (run page → Metrics).
- **Delta table** `{schema}.scoring_metrics` (run_date, rows, inference_seconds):

  ```sql
  SELECT * FROM workspace.iraonfridays.scoring_metrics ORDER BY run_date DESC;
  ```

Training-time evaluation timing lives in the MLflow run for `05_train_xgb.py`
(rmse/mae + run duration — visible in the Experiment UI).

### 3. Real-time serving latency

Once the endpoint is enabled (below), its **Metrics tab** shows server-side
latency percentiles (p50/p95/p99), requests/second, and error rate — the
numbers that matter for a REST API. `scale_to_zero_enabled: true` means the
first request after idle pays a cold start; disable it for guaranteed latency.

### 4. Datadog (optional, for cross-tool dashboards)

Databricks → **Settings → Datadog integration** (admin): paste your Datadog
API key + site, and the workspace exports job metrics, cluster/serverless
metrics, DBU usage, and **serving-endpoint latency** to Datadog. That's the
layer for org-wide dashboards/alerts; Lakehouse Monitoring
(`monitoring/monitors.sql`) stays the data-quality/drift layer.

### 5. Testing the serving model

The endpoint resource is ready but disabled (`resources/wanderbricks_serving.endpoint.yml.example` —
the bundle skips `.example` files because deploy fails while no model version
exists). Enable after the first training run:

```sh
git mv resources/wanderbricks_serving.endpoint.yml.example \
       resources/wanderbricks_serving.endpoint.yml
databricks bundle deploy --target dev
```

Then test **from a notebook** (easiest — token handled for you):
**`notebooks/04_test_serving.ipynb`** (imported to
`/Users/<you>/wanderbricks/notebooks/04_test_serving`) builds a real payload
from `weather_features`, POSTs to the endpoint, prints the prediction and the
round-trip time.

Or **from the CLI/terminal**:

```sh
DATABRICKS_TOKEN=$(databricks auth token --profile dev | jq -r '.token_value')
curl -X POST \
  https://dbc-2944edfb-cd25.cloud.databricks.com/serving-endpoints/wanderbricks-weather-serve/invocations \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"dataframe_records": [{"day_of_week":3,"day_of_year":200,"month":7,"is_weekend":0,
        "temp_lag_1":24.5,"temp_lag_7":22.1,"temp_lag_14":20.3,"temp_lag_28":18.7,
        "temp_rollmean_7":23.2,"temp_rollmean_14":22.4}]}'
```

Expected: `{"predictions": [<temperature °C>]}`. The workspace UI also has a
built-in **Query endpoint** playground on the endpoint page.

---

## Running the notebooks locally (Databricks Connect + SDK)

Every notebook in `notebooks/` now boots in **two modes** — the Databricks
workspace (native `spark`/`dbutils`) or your laptop:

```python
# first cell of each notebook — workspace OR local
try: spark
except NameError:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.profile("irajput").serverless().getOrCreate()

try: dbutils
except NameError:
    from databricks.sdk import WorkspaceClient
    dbutils = WorkspaceClient(profile="irajput").dbutils
```

Verified locally: `WorkspaceClient(profile="irajput")` authenticates and
`DatabricksSession...serverless()` returns a live Spark (v4.2.0). Widgets fall
back to plain variables if neither surface provides them. `04_test_serving`
also falls back to the CLI token cache locally.

Notes:
- Uses your `[irajput]` profile in `~/.databrickscfg`.
- `dbutils.fs.ls("/")` fails on this workspace — **Public DBFS root is
  disabled**; use `/Volumes/...` paths instead.
- Local runs spin serverless compute (small credit cost, like the job runs).
