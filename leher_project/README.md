# Leher — one reliable field update per farm, per day

A small, runnable reference implementation of the data platform behind Leher's
morning farmer update. Three real, keyless APIs and the farm registry are landed
in one raw lake, cleaned into a typed layer with a quarantine table, and joined
on **one key: `farm_id` + `obs_date`** into `gold_farm_daily`.

- **Run it in seconds locally** with DuckDB: `make demo` (no Databricks account).
- **Run the same data model on Databricks** with Lakeflow Spark Declarative
  Pipelines, Auto Loader, Unity Catalog and a scheduled job: `databricks bundle deploy`.

The demo is deliberately small: 4 Python files (~1,300 lines, half of which is
the DuckDB mirror of the same model) plus ~130 lines of bundle/pipeline config.
It is the data-engineering part of the story, not the computer-vision model.

---

## What it shows

| Resume claim | Where it lives in this repo |
| --- | --- |
| Two pipelines on one raw lake | `pipelines/farm_conditions/` and `pipelines/imagery/` |
| Multiple API integrations unified on field + day | `scripts/ingest.py` -> `gold_farm_daily` in `scripts/build_model.py` |
| Bad data quarantined, not silently dropped | `quarantine` table + reason codes in both pipelines |
| A missing source still reaches the farmer (marked stale) | `*_as_of` / `*_is_stale` / `data_status` in `GOLD_SQL` |
| Independent branches: imagery failure cannot block conditions | `resources/leher_daily.job.yml` |
| Batch schedule, not streaming, because decisions are daily | 5 AM cron + triggered serverless pipelines |
| Modern Databricks | Declarative Automation Bundle, Spark Declarative Pipelines, Auto Loader, UC Volume, serverless |

## Quickstart (local, no Databricks)

```bash
make setup     # uv sync: requests + duckdb
make demo      # replay captured raw files -> data/leher.duckdb -> printed report
```

`make demo` replays two real days captured from the APIs (`fixtures/raw/`), so it
is deterministic and needs no network. It builds the same layers as the
Databricks pipelines and ends with a report like:

```text
[build] bronze rows: weather=2 agro=2 satellite=2 farms=10
[build] silver rows: farms=5 weather=10 agro=4 satellite=29

quarantine:
  agro  sentinel_missing_value   5     # NASA POWER had not published 2026-10-20 yet
  agro  rain_out_of_range        1     # 678 mm in a day: impossible, quarantined

gold_farm_daily: 10 rows = one row per farm per obs_date
freshness per day:
  2026-10-15  5 farms   weather_fresh=5  agro_fresh=4  agro_missing=1
  2026-10-20  5 farms   weather_fresh=5  agro_fresh=0  agro_using_older=4
app_farm_daily (what the backend reads):
  2026-10-15  F001  tomato  30.8 C  7.2 mm  drought: none   complete
  ...
  2026-10-20  F001  tomato  28.7 C  0.1 mm  drought: none   degraded
```

Note the second day: weather is fresh, the agro signal is stale, and the unified
row says so instead of hiding it. That is the "app shows yesterday's values,
marked with when they were last updated" behaviour.

## How the data flows

```text
 Open-Meteo weather ─┐
 NASA POWER (drought)─┤   per farm, per day        RAW LAKE (S3 / UC Volume)
 Planetary Comp. STAC─┼──► one envelope each ───►  source=<s>/date=<d>/<file>
 farm registry CSV ──┘                              │
                                                    │  Auto Loader (Spark) / DuckDB mirror
                                    ┌───────────────┴──────────────────┐
                                    ▼                                  ▼
                   PIPELINE 1: farm conditions            PIPELINE 2: imagery branch
                   bronze_*  ->  silver_* + quarantine     bronze_flight_manifests
                   -> gold_farm_daily -> app_farm_daily    -> silver_flight_status
                                    │                          (tiling/blur/weed model
                                    ▼                           deliberately out of scope)
                             backend += PostgreSQL mirror
```

Every source is resolved to the same `(farm_id, obs_date)` grain:

| Source | How it becomes a farm/day row |
| --- | --- |
| Open-Meteo (weather) | one call per farm's lat/lon; daily arrays parsed per date |
| NASA POWER (drought signal) | one call per farm; date-keyed maps selected for the envelope's `obs_date` |
| Planetary Computer STAC | farm bbox search; scene metadata joined to the farm and the observation date |
| Farm registry | daily snapshot export; latest snapshot per `farm_id` is the dimension |
| Drone manifests | `flight_id` in the path; one status row per uploaded flight |

## Repo map

```text
scripts/ingest.py           3 API connectors + farm registry -> raw lake (retries, manifest)
scripts/build_model.py      local DuckDB mirror of the medallion model + report + app parquet
pipelines/farm_conditions/  Databricks pipeline 1: Auto Loader -> bronze/silver/quarantine -> gold
pipelines/imagery/          Databricks pipeline 2: flight manifests -> flight status
resources/*.yml             bundle resources: 2 pipelines + 1 scheduled job (two branches)
databricks.yml              bundle targets (dev/prod), catalog/schema variables
fixtures/                   real captured API responses + 5-farm registry + 3 drone manifests
docs/DATA_MODEL.md          grain, layer contract, quality rules, freshness semantics
docs/RESUME_CHEATSHEET.md   2-minute demo script + interview answers + what not to claim
```

## Run it on Databricks

```bash
databricks auth login --host https://<your-workspace-url>    # Free Edition is fine
databricks schemas create leher_demo workspace
databricks volumes create workspace leher_demo raw MANAGED
databricks bundle deploy -t dev
databricks bundle run leher_daily -t dev
```

The job runs `scripts/ingest.py` (live APIs, today's date in IST) and then two
independent pipeline tasks. Tables appear as
`workspace.leher_demo.{bronze_*, silver_*, quarantine, gold_farm_daily, app_farm_daily}`
and in the imagery pipeline under the same schema. `databricks bundle validate`
checks the bundle after login.

If your workspace has no `workspace` catalog, change the `catalog` variable in
`databricks.yml`. If a serverless Python task cannot write to `/Volumes/...`,
give `ingest_apis` a single-node `new_cluster` instead of the environment - the
rest of the job is unchanged.

## Use live data

```bash
make ingest DATE=2026-10-15   # calls the real APIs into data/raw
make refresh-fixtures         # re-capture fixtures/raw for the demo dates (network)
```

## Scope and honesty

This repo is a demo built to explain the Leher data platform; it is not the
production system. Specifically:

- The three APIs are real and keyless (Open-Meteo, NASA POWER, Planetary
  Computer STAC) but were **not** necessarily the exact vendors Leher used. The
  point is the contract: different shapes, one farm/day key.
- The volume numbers in the story (drone imagery ~10 GB/week vs API data in MB)
  are not reproduced here; the imagery branch only reads flight manifests.
- The weed model, tiling and blur scoring are summarised in
  `docs/RESUME_CHEATSHEET.md`, not implemented.
- The 5 AM schedule, PostgreSQL mirror and upload script are plausible design
  details, not evidence of what Leher actually ran. Confirm them before claiming
  them in an interview — the cheatsheet lists exactly what to check.

Read next: [docs/DATA_MODEL.md](docs/DATA_MODEL.md) for the table contracts, then
[docs/RESUME_CHEATSHEET.md](docs/RESUME_CHEATSHEET.md) for the interview script.
