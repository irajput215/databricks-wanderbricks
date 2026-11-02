# Data model — one row per farm per day

Everything in the platform is designed around one grain:

```text
(farm_id, obs_date)  ->  exactly one row in gold_farm_daily
```

`farm_id` is the business key the farmer and the app think in. `obs_date` is the
farm-local day the observation belongs to (Asia/Kolkata). Sources do not arrive
in that shape: weather comes per lat/lon, drought data is keyed by date inside a
map, satellite scenes are keyed by scene timestamp and geometry, drone imagery is
keyed by flight. The pipeline's job is to resolve every source to this grain; a
source that cannot be resolved to a real farm and a real day is quarantined.

## Source to key resolution

| Source | Arrives as | Resolved by | Becomes rows in |
| --- | --- | --- | --- |
| Open-Meteo weather | daily arrays per lat/lon request | per-farm request, envelope `obs_date`, array element for that day | `silver_weather` (farm/day) |
| NASA POWER agro | `{T2M: {"20261015": 24.0}}` maps | envelope `_meta.obs_date` -> `%Y%m%d` map key | `silver_agro` (farm/day) |
| Sentinel-2 STAC | scene list per farm bbox | scene datetime -> `scene_date`; gold picks best scene within 7 days of `obs_date` | `silver_satellite` (farm/scene) |
| Farm registry | daily CSV snapshot from the app DB | `farm_id`; latest snapshot wins | `silver_farms` (farm dimension) |
| Drone flight | manifest per uploaded flight | `flight_id` + `farm_id` in the envelope/path | `silver_flight_status` (flight) |

Satellite is intentionally **not** squeezed to one row per farm/day. It is a
one-to-many feed keyed by scene; the `gold` layer applies the "as-of" rule that
turns it into one row (latest usable scene within 7 days). Keeping the raw grain
honest is what lets the model change without rewriting the landing zone.

## Raw lake layout

One directory per source and date; files are immutable JSON envelopes (the API
response plus `_meta` request context), except the registry snapshot (CSV) and
drone manifests. The same layout works for S3 and a Unity Catalog Volume.

```text
<raw>/source=weather/date=2026-10-15/weather_<fetched-at>.json
<raw>/source=agro/date=2026-10-15/agro_<fetched-at>.json
<raw>/source=satellite/date=2026-10-15/satellite_<fetched-at>.json
<raw>/source=farms/date=2026-10-15/farms_<fetched-at>.csv
<raw>/source=drone/farm_id=F001/date=2026-10-15/flight_id=F001-20261015-01/manifest.json
<raw>/_manifests/date=2026-10-15/run_<timestamp>.json      # which sources succeeded
```

A file name carries the ingestion timestamp. A rerun of the same day lands a new
file next to the old one rather than overwriting; the latest `fetched_at` wins
in gold. Nothing in the raw layer is ever edited.

## Layer contract

| Layer | Databricks object | Local mirror | Grain | Write pattern |
| --- | --- | --- | --- | --- |
| Raw | UC Volume paths | `data/raw/` | one envelope per source/day | append-only files |
| Bronze | `bronze_weather`, `bronze_agro`, `bronze_satellite`, `bronze_farms` | same names in DuckDB | one row per raw file, raw payload + lineage | streaming table, Auto Loader |
| Silver | `silver_weather`, `silver_agro`, `silver_satellite`, `silver_farms` | same names | typed rows; revisions allowed | streaming tables (farm dim is a materialized view) |
| Quarantine | `quarantine` | same | failed row + `source` + `reason` | streaming table |
| Gold | `gold_farm_daily` | same | **one row per farm per `obs_date`** | materialized view (batch dedupe + as-of joins) |
| App | `app_farm_daily` | exported `data/app/farm_daily.parquet` | narrow column contract | materialized view / file mirror |

`silver_*` is append-only: a re-ingested day adds a second revision. Gold keeps
the newest revision per `(farm_id, obs_date)` with `ROW_NUMBER() ... ORDER BY
fetched_at DESC`. That is the no-duplicates guarantee: re-running a day is safe
and idempotent at the gold grain, even though raw and silver keep history.

## `gold_farm_daily` columns

| Group | Columns | Notes |
| --- | --- | --- |
| Key | `farm_id`, `obs_date` | unique together |
| Farm dimension | `farmer_name`, `crop`, `district`, `state`, `lat`, `lon`, `area_ha`, `soil_type` | latest registry snapshot |
| Weather | `temp_max_c`, `temp_min_c`, `temp_mean_c`, `rain_mm`, `wind_max_kmh`, `humidity_pct`, `et0_mm` | Open-Meteo, metric units |
| Weather freshness | `weather_as_of`, `weather_is_stale`, `weather_fetched_at` | `weather_as_of` = day the value belongs to |
| Drought signal | `root_zone_wetness`, `drought_level` | NASA POWER `GWETROOT`; level is a demo threshold rule: `>=0.50 none`, `>=0.35 mild`, `>=0.20 moderate`, else `severe` |
| Agro freshness | `agro_as_of`, `agro_is_stale`, `agro_fetched_at` | fallback window 7 days |
| Satellite | `satellite_scene_id`, `satellite_scene_date`, `satellite_cloud_pct`, `satellite_is_usable` | latest **usable** scene within 7 days; usable = cloud <= 40% |
| Status | `data_status`, `updated_at` | `complete` = weather + agro for this exact day; else `degraded`/`unavailable` |

`app_farm_daily` is the only table the backend should depend on: it drops
internal lineage columns and keeps the fields the farmer app renders.

## Validation rules (silver split)

Rows that fail any rule are written to `quarantine` with a machine-readable
reason. They are counted, not silently dropped: "how many rows did source X lose
today" must be answerable. Rules are constants at the top of
`pipelines/farm_conditions/farm_conditions.py` and `scripts/build_model.py`.

| Source | Rule (`reason`) | Condition |
| --- | --- | --- |
| all | `missing_farm_id`, `missing_obs_date` | key columns empty |
| all | `unknown_farm_id` | `farm_id` not in the farm registry |
| weather | `date_mismatch` | parsed weather day != envelope `obs_date` |
| weather | `missing_temperature`, `temperature_out_of_range` | null, or outside -20..60 C |
| weather | `rain_out_of_range`, `wind_out_of_range`, `humidity_out_of_range` | rain 0..500 mm, wind 0..250 km/h, humidity 0..100% |
| agro | `sentinel_missing_value` | NASA POWER fill value `-999` (day not published yet) |
| agro | `rain_out_of_range`, `humidity_out_of_range`, `root_zone_wetness_out_of_range`, `temperature_out_of_range` | same sanity bounds |
| satellite | `missing_scene_id`, `missing_scene_date`, `scene_date_in_future`, `cloud_cover_out_of_range` | scene identity and cloud 0..100% |

In production these bounds live in a small config table per crop/region; here
they are constants so the demo can be read top to bottom.

## Freshness and fallbacks (the part a farmer notices)

Each source is joined twice in `GOLD_SQL`: first the exact `obs_date`, then, if
that is missing, the latest row inside the source's freshness window.

| Source | Lookback | Why | Marking |
| --- | --- | --- | --- |
| Weather | 1 day | yesterday's conditions are still useful, but not today's claim | `weather_as_of`, `weather_is_stale` |
| Agro / drought | 7 days | soil wetness moves slowly | `agro_as_of`, `agro_is_stale` |
| Satellite | 7 days | Sentinel-2 revisit ~5 days | `satellite_scene_date`, `satellite_is_usable` |

If nothing is found in the window the columns are null and `data_status` says
`degraded` (or `unavailable` when both weather and agro are missing). The app can
render "last updated ..." from the `*_as_of` columns instead of pretending the
data is current. There is deliberately no silent forward-fill.

## Adding a new source (the checklist that matters in interviews)

1. Write a connector in `scripts/ingest.py` that lands the raw response under
   `source=<name>/date=<obs_date>/` with `_meta.obs_date` + `_meta.fetched_at`.
2. Define its explicit schema and parse it into rows that carry `farm_id` and
   `obs_date` (add a `_parse_<source>` + `_flag_<source>` pair in the pipeline).
3. Decide which piece of the new source is required vs optional for
   `data_status`; add its lookback window if it is allowed to be stale.
4. Add the join to `GOLD_SQL` (exact day first, as-of fallback second) and add
   its columns/freshness flags to the gold + app contract.
5. Add the rule names to the table above and one fixture day; then the rules are
   testable without the real API being up.

## Local mirror vs Databricks

`scripts/build_model.py` implements the same contract with DuckDB so `make demo`
runs without a workspace. `pipelines/farm_conditions/farm_conditions.py` is the
Spark/Declarative Pipeline implementation of the same layers. They are kept
behaviourally identical (example: both produce 10 gold rows and 6 quarantine rows
for the demo data), but only the Databricks pipelines are the production shape
(Auto Loader, streaming tables, serverless compute, Unity Catalog).
