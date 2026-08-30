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

### 09 · Monitoring, pipeline-flow visibility, and serving-test wiring
Captain asked how to monitor inference/prediction time, see the pipeline flow,
and test the serving model. Wired: `06_score.py` now times scoring and logs
`inference_seconds` to MLflow + a `scoring_metrics` Delta table; fixed a latent
bug in `05_train_xgb.py` (string `station` column would break XGBoost — dropped
from features); added `resources/wanderbricks_serving.endpoint.yml.example`
(ready-to-enable, skipped by the bundle glob until a model version exists);
added `notebooks/04_test_serving.ipynb` (imported to the workspace); README
Part 4 documents the job/DLT run DAGs, CLI run commands, latency surfaces, the
Datadog integration, and curl/notebook serving tests.

### 10 · First PR — created, reviewed, merged
Opened PR #1 "Wanderbricks: end-to-end GSOD weather forecasting on Databricks"
(branch `feat/gsod-forecasting`, 7 commits: serverless fix through monitoring/
serving work) with a full description (summary, per-area table, decisions,
runbook, status). Captain reviewed and merged. Local `main` fast-forwarded to
the merge commit and the merged branch deleted locally + remotely. First
reviewable milestone of the project.

### 11 · First live pipeline run — SUCCESS
First `bundle run wanderbricks_pipeline --refresh-all` failed on
[UC_VOLUME_NOT_FOUND]: Auto Loader's schemaLocation pointed at a volume that
didn't exist. Fix: created managed volume `workspace.iraonfridays._schemas`
(`databricks volumes create`). Re-run COMPLETED: gsod_bronze + gsod_silver
(streaming tables) + weather_features (materialized view) all built from the
public GSOD 2025 data. Tables verified in the catalog.

### 12 · Debugging saga: schema, units, and UC quirks — then FULL SUCCESS
First end-to-end job run surfaced a chain of real-world issues, each fixed:
1. **Empty silver** — DATE parse format was yyyyMMdd but the bucket uses ISO dates.
2. **Misaligned bronze columns** — the bucket layout is `<year>/<station>.csv`
   (plain quoted CSV, 28 cols), not `.op.gz`; a subset schema maps positionally
   and Auto Loader's persisted schema state evolved rather than replaced.
   Fixed with the full 28-column schema, a FRESH schemaLocation path, and
   `schemaEvolutionMode=none`, then a full reset (drop tables, recreate volume,
   `--full-refresh-all`).
3. **US units discovered** — the AWS bucket stores °F / inches / knots, not
   metric tenths; silver now converts °F→°C and in→mm.
4. **MLflow experiment missing** — added `mlflow.set_experiment("/Shared/wanderbricks_forecast")`.
5. **UC requires model signatures** — added `infer_signature` on log_model.
6. **UC forbids stages** — switched to a `@Production` alias set at registration,
   loaded via `models:/<catalog>.<schema>.<name>@Production`.
RESULT: full job run SUCCESS — Prophet baseline, XGBoost registered as
`workspace.iraonfridays.wanderbricks_weather_xgb@Production`, and
**11,329 station forecasts scored in 0.0491s** (recorded in
`scoring_metrics`). First complete end-to-end run of the project.

### 13 · Serving endpoint ENABLED and tested
The model existed (v1 + @Production alias), so the endpoint resource was
enabled: `git mv resources/wanderbricks_serving.endpoint.yml.example → .yml`,
`bundle deploy` → `model_serving_endpoints.wanderbricks_weather_serve` created.
Discovered dev-mode naming: resources get a `dev_<user>_` prefix, so the
endpoint is `dev_iraonfridays_wanderbricks-weather-serve` (prod:
`wanderbricks-weather-serve`). Tested live via curl with a feature-row payload:
`{"predictions": [23.86]}`. Updated `notebooks/04_test_serving` (endpoint
default + dev-name note) and re-imported to the workspace. Captain's earlier
`404 ENDPOINT_NOT_FOUND` was the endpoint not being deployed yet.
