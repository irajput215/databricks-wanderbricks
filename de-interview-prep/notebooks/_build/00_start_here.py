TITLE = "00 - Start here: schema, grain, and how to practise"
CELLS = [
    ("md", """**Healthcare claims - Data Engineer practice project.**

One small, realistic claims warehouse (11,981 claims, 18 months) plus everything around it:
a messy source layer, an ingestion pipeline, a data-quality scorecard and a deliberately broken dashboard to investigate.

| Notebook | What it drills |
|---|---|
| 01 grain & aggregation | Q1-Q16 - basic aggregation, changing the grain |
| 02 joins | Q17-Q25 - relationships, anti-joins |
| 03 grain traps | Q26-Q33 - fan-out, `COUNT(*)` vs `COUNT(DISTINCT)` |
| 04 window functions | Q34-Q40 - ranking, % of total, MoM growth |
| 05 business KPIs | Q41-Q45 - approval/denial rate, provider performance |
| 06 DE scenarios | Q46-Q50 - KPI tables, the +40% investigation, the real design problem |
| 07 PySpark core | the same patterns in Spark: dedupe, top-N, LAG, anti-join, nested JSON |
| 08 PySpark incremental | watermark loads, idempotent upsert, late data, SCD2 |
| 09 ingestion | paginated API, retries/backoff, validation, dead-letter, idempotency |
| 10 data quality | DQ checks, quarantine, reconciliation, root-cause investigation |
| 11 gaps & islands | the 3 patterns: sequential values, status changes, gaps |

Every notebook runs top-to-bottom. `q("SQL")` is the only function you need."""),
    ("md", """### The layers

| Layer | Schema | What it is |
|---|---|---|
| Raw | `raw.*` | exactly as landed - strings, defects intact |
| Bronze | `bronze.*` | validated + typed, **duplicates preserved**, `_ingest_batch`, `_loaded_at` |
| Silver | `silver.*` | deduplicated, conformed, FKs valid |
| Main | `main.*` | views over silver with plain names - **all SQL drills use these** |
| Gold | `gold.*` | monthly / provider KPI marts (Q46-Q48 reference answers) |
| DQ | `dq.*` | `quarantine`, `dq_results`, `dq_metrics` |
| Dashboard | `dashboard.*` | deliberately broken marts used by the Q49 investigation |"""),
    ("code", """tables("main")"""),
    ("md", """### Grain - the first question for every problem

| Table | Grain | Rows | Key |
|---|---|---|---|
| `members` | 1 row per member | 1,200 | `member_id` |
| `providers` | 1 row per provider | 220 | `provider_id` |
| `provider_specialty_history` | 1 row per provider per specialty period | 60 | `provider_id, effective_start` |
| `claims` | 1 row per claim | 11,981 | `claim_id` |
| `claim_services` | 1 row per claim **service line** | 38,592 | `claim_id, service_id` |
| `payments` | 1 row per payment transaction | 11,465 | `payment_id` |
| `plans` / `procedures` / `diagnoses` | 1 row per code | 5 / 32 / 30 | code |"""),
    ("code", """q('''
SELECT 'members' AS tbl, COUNT(*) AS rows FROM members
UNION ALL SELECT 'providers', COUNT(*) FROM providers
UNION ALL SELECT 'claims', COUNT(*) FROM claims
UNION ALL SELECT 'claim_services', COUNT(*) FROM claim_services
UNION ALL SELECT 'payments', COUNT(*) FROM payments
ORDER BY rows DESC
''')"""),
    ("code", """columns("claims")"""),
    ("md", """### The practice loop (do this on every question)

```
1. Business question:  what decision does this feed?
2. Final grain:        one row per ______ ?
3. Source tables:      which tables?
4. Source grains:      one row per ______ in each
5. Grain risk:         will any join multiply rows?
```

Then write SQL. Then answer out loud: **"why does this produce one row per X?"**

Run `assert_grain(df, "member_id")` on any result where you claim a grain - it catches fan-out instantly."""),
    ("code", """# Worked example (Q3): total claim count for each member
df = q('''
SELECT member_id, COUNT(*) AS claim_count
FROM claims
GROUP BY member_id
''')

assert_grain(df, "member_id")
df.head()"""),
    ("md", """### Databricks Connection & Unity Catalog Check
Verify that Databricks Connect is attached to Serverless Compute and that the `workspace.healthcare` Delta tables are queryable."""),
    ("code", """# Verify Databricks session
spark.sql("SELECT current_user() AS user, current_catalog() AS catalog, current_schema() AS schema").show()

# Query Delta table in Unity Catalog via Spark
spark.sql('''
SELECT status, COUNT(*) AS claim_count, ROUND(SUM(paid_amount), 2) AS total_paid
FROM claims
GROUP BY status
ORDER BY claim_count DESC
''').show()"""),
    ("md", """### DuckDB Practice Warehouse & DQ Health Check
The local DuckDB warehouse (`data/warehouse/healthcare.duckdb`) is also ready for instant local drills."""),
    ("code", """q('''
SELECT status, COUNT(*) AS checks
FROM dq.dq_results
GROUP BY status
ORDER BY status
''')"""),

    ("code", """# the single deliberate FAIL - notebook 10 investigates it
q('''
SELECT check_name, actual, threshold, status
FROM dq.dq_results
WHERE status != 'PASS'
ORDER BY status, check_name
''')"""),
    ("md", """### Rebuilding

```bash
.venv/bin/python src/generate_sources.py     # regenerate messy raw + API pages (deterministic)
.venv/bin/python src/build_warehouse.py      # rebuild raw -> bronze -> silver -> gold + DQ
.venv/bin/python src/dq_checks.py            # print the scorecard
.venv/bin/python tools/build_notebooks.py    # rebuild .ipynb from notebooks/_build/*.py
JUPYTER_PATH="$PWD/.jupyter" .venv/bin/python tools/run_notebooks.py   # execute all notebooks
```"""),
]
