# Interview cheatsheet — Leher, using this repo

The repo is a small, runnable reference implementation of the **data platform**
part of the Leher story: multi-source ingestion, one unified farm/day table, a
quarantine layer and visible freshness fallbacks. It is not the production
system, and it is not the weed model.

## 2-minute live demo

Run `make demo`, then narrate the printed report:

```bash
make demo
```

1. **"Two real sources plus satellite and the farm registry are landed raw."**
   `scripts/ingest.py` — 3 keyless APIs, retries, one envelope per source/day.
2. **"Everything is resolved to one key: farm_id + day."**
   Point at `gold_farm_daily: 10 rows = one row per farm per obs_date`.
3. **"Bad data goes to quarantine, with a reason."**
   `-999` from NASA POWER = the day was not published; `678 mm` rain = impossible.
   That 10/20 agro signal is why the 10/20 rows say `degraded`.
4. **"The fallback is visible, not hidden."**
   `agro_as_of = 2026-10-15` on a 10/20 row + `data_status = degraded`; the app
   can render "last updated".
5. **"On Databricks the same contract runs as two independent pipelines."**
   `resources/leher_daily.job.yml`: ingest → farm conditions branch + imagery
   branch. If imagery fails, conditions still land.

If asked to go deeper, open (in this order):
`docs/DATA_MODEL.md` → `pipelines/farm_conditions/farm_conditions.py:413`
(`GOLD_SQL`, the as-of joins) → `scripts/build_model.py:298` (the readable
DuckDB mirror) → `resources/leher_daily.job.yml` (branches and schedule).

## 30-second pitch (adapt to the real story)

> "Leher's promise was that a farmer opens the app in the morning and knows
> what's happening in the field. The data existed in pieces: weather and drought
> APIs, satellite scene metadata, farm registry data, drone imagery. I built the
> pipeline that lands each source raw, rotates it around one key - farm and
> observation date - and produces exactly one clean row per farm per day. Rows
> that don't belong to a real farm or have impossible values go to a quarantine
> table with a reason instead of reaching a farmer. When a source is late, the
> unified table falls back to the last known value and marks it stale, so the app
> can always say how fresh the data is. On Databricks this is two independent
> pipeline branches, so an imagery failure never blocks the morning update."

## Six decisions and one-line answers

| Decision | Why | How to say it |
| --- | --- | --- |
| One shared key (`farm_id`, `obs_date`) | It is what a farmer thinks in, and what makes unrelated sources joinable | "Every source had to resolve to farm + day or be quarantined." |
| Batch on a schedule, not streaming | Farmers decide once a day; drone flights are weekly; streaming would cost more for no user benefit | "Daily batch was the product requirement, not a compromise." |
| Quarantine with a reason, not silent drops | The app must never show nonsense, but data loss must be countable | "A missing value is a metric, not an exception to swallow." |
| As-of fallback with `*_as_of` + `*_is_stale` | A stale value beats no value, but only if the app can label it | "We never silently forward-filled." |
| Two independent pipeline branches | A vision/image failure should not remove today's weather | "The imagery branch checks for a manifest and exits in seconds when nothing new arrived." |
| Serverless/small clusters, one platform | Tiny team, spiky daily batch, image work benefits from a few machines | "The tabular part was small; Databricks earned its place through the image work and one platform to operate." |

## Likely questions — short answers

1. **Walk me through one day.** Ingest task calls weather/drought/registry APIs
   with retries and writes raw envelopes to the Volume; pipeline 1 parses them to
   field/day rows, quarantines impossible rows, joins the farm dimension and
   merges into gold; pipeline 2 reads any new flight manifests. Gold is mirrored
   to the app-facing table.
2. **Weather API down?** Retry with backoff; the source is marked failed in the
   run manifest; gold falls back inside a 1-day window and marks it stale; the
   next run fills the gap.
3. **How do reruns avoid duplicates?** Raw files carry a fetch timestamp; gold
   keeps the latest revision per `(farm_id, obs_date)`; the demo test proves a
   second revision does not add a row.
4. **Why not just drop bad rows?** Then you cannot answer "how much did we
   lose?" Quarantine gives an audit trail and a metric to alert on.
5. **Why Databricks for this volume?** The daily tabular data is tiny, the image
   work is the reason for distributed compute; one platform for both pipelines
   meant a very small team only operated one system.
6. **Why run the imagery branch daily if the drone flies weekly?** It checks for
   new manifests; no manifest means seconds of work, and a late/extra flight is
   still processed the next morning without schedule changes.
7. **What is the grain and why?** `(farm_id, obs_date)` for the app table; raw
   satellite stays at scene grain and is folded in with an as-of rule because
   scenes do not arrive daily.
8. **How did you validate it?** Local DuckDB mirror of the same model, three
   tests: one row per farm/day, fallback always marked, rerun latest-revision
   wins. Spark implementation and mirror produce the same demo output.
9. **What would you change at 10x?** Streaming silver with watermarks, a config
   table for quality bounds, registry as SCD2, alerting on quarantine rate and
   freshness, and contract tests per source.
10. **What was hard?** Not the API calls; deciding what "same day, same farm"
    means for each source and what to do when a source is late without lying to
    the farmer.

## What NOT to claim (from the revision sheet)

- The weed model was **team work**. Say: "My part was the data behind the
  model." Do not say you trained it.
- Augmentation was applied to **training only**. Never say inference or
  validation data was augmented.
- Use the real volume number from the story (~10 GB/week of drone imagery, MB/day
  of API data). Do not invent 30 GB/day.
- Do not claim Databricks Model Serving for a daily batch job unless an endpoint
  really existed; "scheduled batch inference" is the safer phrase.
- Do not claim irrigation timing came from weed detection - name the real signal.
- Confirm before claiming these were really used: the 5 AM schedule, the
  PostgreSQL copy, and `aws s3 sync` as the upload method. This repo uses them as
  plausible defaults, which is not evidence they happened.
- This repo is a reconstruction for interview prep. Say "I rebuilt a small
  version to explain the design", never "this is the production code".

## Numbers to remember

2 pipelines · 3 API sources + registry + drone manifests · 1 grain
(`farm_id`, `obs_date`) · ~10 GB/week drone imagery vs MB/day APIs · ~78%
precision on held-out farms (real project, not reproducible from this repo).

## Questions to ask them

- Do you work with unstructured data (images, documents) or mostly tables?
- How do you detect when a model's input data stops looking like its training
  data?
- Who owns the data contract between the pipeline and the app team?
