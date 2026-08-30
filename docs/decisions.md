# Wanderbricks — Decision Records

ADR-style records of the choices made on this project: **context → options →
decision → why**. The timeline of what happened is in
[development-log.md](development-log.md).

---

## D1 · Project direction: Databricks end-to-end forecasting

- **Date:** 2026-08-30
- **Context:** Captain wanted a real data + ML/AI engineering project using
  agentic engineering, and chose Databricks. Wanted to learn DLT, data quality,
  MLflow, orchestration, retraining.
- **Options considered:** (a) build another agent-experience CLI in the AXI
  family, (b) implement the earlier x-linkedin-agent, (c) a Databricks
  end-to-end project.
- **Decision:** (c) — Databricks forecasting.
- **Why:** the AXI space is mature/crowded (a tool without a felt gap is a
  trap); a Databricks project covers the exact technologies the captain wanted
  to learn on a real platform with a real dataset, and time-series forecasting
  is a bounded problem with clear success criteria (forecast error).

## D2 · Project location: `~/databricks-forecasting` (outside the fleet registry)

- **Date:** 2026-08-30
- **Context:** Captain explicitly asked for a new folder at `~` with git
  initialized, rather than the fleet's standard `projects/` location.
- **Decision:** follow the captain's explicit location; keep it out of the
  fleet registry as a personal learning project.
- **Why:** explicit captain instruction wins over convention; the fleet
  machinery is for managed clones. If it ever needs fleet-managed workers, it
  can be cloned into the registry then.

## D3 · Teaching README before code

- **Date:** 2026-08-30
- **Context:** Captain asked to "teach me every feature of Databricks with
  examples" before the procedure.
- **Decision:** the README is a full platform tour + project procedure, written
  first, and later extended (Part 3: GSOD) as the project grew.
- **Why:** the captain is learning the platform; a doc-first project makes every
  later step self-explanatory and keeps a single source of truth.

## D4 · Project-as-code with Databricks Asset Bundles

- **Date:** 2026-08-30
- **Context:** Modern Databricks supports notebooks, direct job creation, and
  Declarative Automation Bundles (`databricks.yml` + CLI).
- **Decision:** bundles as the delivery layer: one config file declares the DLT
  pipeline, the daily job, and targets (dev/prod); everything is version
  controlled and CI/CD-ready.
- **Why:** matches the captain's "end-to-end engineering" goal (git, validation,
  deploy/destroy), enables dev/prod separation, and is the documented modern
  path (CLI ≥ 0.218+).

## D5 · Serverless compute (constraint discovered at deploy, not chosen)

- **Date:** 2026-08-30
- **Context:** First deploy of the job with a classic ML cluster was rejected:
  `Only serverless compute is supported in the workspace.`
- **Decision:** job tasks run on serverless compute with an `environment_key`
  spec (prophet, xgboost, scikit-learn); no job clusters.
- **Why:** it was a hard workspace constraint, not a preference. Serverless is
  also simpler operationally (no cluster management) — a durable fact about
  this workspace that shapes every future resource.

## D6 · Triggered (batch) DLT pipeline, not continuous streaming

- **Date:** 2026-08-30
- **Context:** The captain's blueprint included a continuous 24/7 streaming
  design (watermarks, 5-minute windows, sub-second serving).
- **Decision:** the DLT pipeline runs **triggered** on the daily schedule;
  `continuous: true` is documented but off.
- **Why:** the dataset is **daily** GSOD updates with 1–2 day freshness.
  Continuous mode keeps serverless compute running around the clock for no
  freshness benefit. The streaming-read primitives (Auto Loader `readStream`,
  DLT streaming tables) are identical either way, so flipping to continuous
  later is a one-line change — the decision is reversible at low cost.

## D7 · DLT owns the whole bronze → gold layer; the job only does ML

- **Date:** 2026-08-30
- **Context:** The original job had a standalone `ingest` task running
  `01_ingest.py`, with DLT only for silver/gold.
- **Decision:** move ingestion into the DLT pipeline (bronze streaming table)
  and remove the standalone task; the job chain is now
  `dlt_pipeline → baseline → train → score`.
- **Why:** one declarative owner for the data layer — DLT manages checkpoints,
  streaming tables, and quality in one pipeline; the job becomes purely the ML
  orchestration. Fewer moving parts, and bronze/silver/gold land in the same
  catalog/schema via pipeline config.

## D8 · Gold features as a materialized view, not a streaming table

- **Date:** 2026-08-30
- **Context:** Lag/rolling window functions (`F.lag`, `avg over rowsBetween`)
  are not legal on a streaming read.
- **Decision:** `weather_features` reads `gsod_silver` with `dlt.read` (batch),
  making it a materialized view recomputed each pipeline run.
- **Why:** per-station daily features need whole-series window functions;
  Spark Structured Streaming forbids them on streams. An MV is exactly the
  right primitive for "recompute the feature table when new data lands". The
  real-time variant (window/groupBy aggregation) is documented for the
  continuous future.

## D9 · NOAA GSOD as the dataset

- **Date:** 2026-08-30
- **Context:** Captain asked to wire a real dataset from S3; the plan proposed
  NOAA GSOD.
- **Decision:** use `s3://noaa-gsod-pds/` (daily weather, 9000+ stations,
  1929–present, public bucket).
- **Why:** daily granularity matches the Prophet/lag-XGBoost design; decades of
  history support training; it's publicly readable from serverless for admins;
  and the 1–2 day freshness is fine for a daily pipeline. Scoped to one recent
  year initially to validate cheaply, widenable to full history.

## D10 · Explicit GSOD schema + sentinel handling in the pipeline

- **Date:** 2026-08-30
- **Context:** GSOD files are gzipped per-station-per-year CSVs with values in
  tenths and numeric sentinels for missing data.
- **Decision:** explicit schema in `01_ingest.py` (matched by name, subset of
  columns), unit conversion + sentinel → NULL in `02_clean.py`, quality gates
  via `@dlt.expect_or_drop`.
- **Why:** deterministic parsing beats inference on gzipped files; cleaning in
  the silver layer keeps raw bronze append-only (the medallion principle) and
  makes the sentinel/unit conventions explicit and testable.
