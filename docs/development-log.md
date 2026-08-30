# Wanderbricks — Development Log

A running journal of every step taken on this project, in order, with what it
produced. Decision rationale lives in [decisions.md](decisions.md); this file
is the timeline.

**Convention:** every development step adds a dated entry here. Steps are
numbered per day (`2026-08-30 · 01`, `02`, …).

---

## 2026-08-30

### 01 · Project inception
Captain asked for an end-to-end Databricks **data + ML/AI engineering** project
and chose **time-series forecasting** as the problem. Learning goals named:
DLT, data-quality/monitoring, MLflow, orchestration, retraining — preferably
Databricks-native, with Airflow/Datadog optional alongside.

### 02 · Repo created + teaching README
Created `~/databricks-forecasting/` (captain-specified location, git initialized
on `main`). Wrote a teaching README: Part 1 = Databricks feature tour (14
features with examples), Part 2 = the end-to-end forecasting procedure.
Committed `8165cde`.

### 03 · CLI installed, bundle scaffolded
Installed Databricks CLI v1.14.1 (`brew install databricks/tap/databricks`).
Captain authenticated against `dbc-2944edfb-cd25.cloud.databricks.com`.
Scaffolded the project with `databricks bundle init default-python`, lifted the
generated config to the repo root, dropped the taxi sample, and wired
`databricks.yml` (dev/prod targets) + the DLT pipeline + daily job resources.
Committed `c5cf945` after first validation.

### 04 · Discovery: serverless-only workspace
First deploy failed with `Only serverless compute is supported in the
workspace`. The job's classic ML cluster was rejected. Switched the job to
**serverless compute with an environment spec** (prophet, xgboost,
scikit-learn as dependencies). Committed `1405809`. Deployed successfully:
`[dev] wanderbricks-job` + `[dev] forecasting-pipeline` live.

### 05 · Rename: forecasting → Wanderbricks
Captain renamed the project to **Wanderbricks** (matches GitHub repo
`databricks-wanderbricks`). Destroyed old-named resources, renamed bundle/job/
pipeline/README/pyproject, redeployed under `wanderbricks`. Committed `de2a53c`.

### 06 · Real dataset: NOAA GSOD wired end to end
Captain proposed NOAA GSOD (`s3://noaa-gsod-pds/`, daily weather, 1929–now)
and asked to wire it. Verified bucket schema + public-access story, then:
- `01_ingest.py` — Auto Loader streaming table with explicit GSOD schema
  (tenths of °C/mm, sentinels like 9999.9), currently scoped to year 2025
- `02_clean.py` — streaming table, `@dlt.expect_or_drop` gates, unit
  conversion, sentinels → NULL
- `03_features.py` — materialized view: per-station calendar + lag/rolling
  temperature features
- `ml/04|05|06` — Prophet baseline / XGBoost / batch scoring, now
  env-aware via `--catalog/--schema`
- Pipeline configured **triggered** (not continuous) — daily data, cost
- Deploy hit one fix: DLT library glob needs `**`, not `*`
Committed `a31e5d4`. Validated + deployed (both resources updated).

### 07 · Decision documentation
Captain asked for a docs folder explaining steps and decision thought
process. Created `docs/development-log.md` (this file) +
`docs/decisions.md` (ADR-style rationale), backfilled with the above.

### 08 · Exploration notebooks
Captain asked for notebooks to explore data and experiment. Generated three
git-tracked `.ipynb` files with `nbformat` (uv venv): bronze/silver explorer,
gold-features explorer (nulls + correlation + plots), and a quick-forecast lab
(Prophet + XGBoost trials mirroring `ml/`). Deployed via the bundle and
imported into the workspace at
`/Users/iraonfridays@gmail.com/wanderbricks/notebooks/` for direct interactive
use. Committed `9a92d...` (see git log).
