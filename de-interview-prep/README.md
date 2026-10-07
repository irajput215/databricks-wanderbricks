# Healthcare Claims — DE Interview Practice Lab

A small but realistic claims warehouse + a messy source layer, so you practise the whole chain:
**ingest → validate → model → aggregate → reconcile → explain**.

- **11,981 claims** across 18 months (2024-01 → 2025-06), 1,200 members, 220 providers, 38,592 service lines, 11,465 payments
- 50 SQL drills (grain → joins → grain traps → windows → KPIs → DE problems) in 6 notebooks
- 2 PySpark notebooks, 1 ingestion notebook, 1 data-quality investigation notebook, 1 gaps-and-islands notebook
- **One deliberately broken dashboard**: it reports **+40%** claim growth for Apr-2025; the source says **+5%**. Notebook 10 makes you find out why.

## Quick start (2 minutes)

```bash
cd de-interview-prep
bash tools/bootstrap.sh                        # venv + kernel (already done if .venv exists)
.venv/bin/python src/build_warehouse.py        # build the warehouse from the generated sources
JUPYTER_PATH="$PWD/.jupyter" .venv/bin/python tools/run_notebooks.py   # execute every notebook to check it works
```

Then open `notebooks/00_start_here.ipynb` and pick the **"DE Prep (duckdb + pyspark)"** kernel.

In VS Code: open any notebook → *Select Kernel* → *Python Environments* → `de-interview-prep/.venv/bin/python`.

## How to practise

For **every** question, fill in these five lines before writing SQL:

```
1. Business question:  what decision does this feed?
2. Final grain:        one row per ______ ?
3. Source tables:      which tables?
4. Source grains:      one row per ______ in each
5. Grain risk:         will any join multiply rows?
```

Each drill notebook has a `# YOUR TURN` cell (write your query there) directly above a `# SOLUTION` cell.
Copy the solution's `assert_grain(...)` call onto your own result — it fails loudly when a join fans out.

## Layout

```
src/          generate_sources.py  build_warehouse.py  dq_checks.py  de_helpers.py  fake_api.py  paths.py
data/         raw/ landing/ (messy sources)  bronze/ silver/ gold/ (parquet)  warehouse/healthcare.duckdb
notebooks/    00-11 drills (.ipynb)   _build/ (the python specs they are generated from)
tools/        bootstrap.sh  build_notebooks.py  run_notebooks.py  verify_project.py
sql/          gold_kpis.sql (Q46-Q48 reference)  q49_investigation.sql (stage-by-stage investigation)
SCHEMA.md     tables, grain, keys, and the 14 deliberate source defects
```

## The practice tables (`main.*`)

```
plans ──< members ──< claims >── providers >── provider_specialty_history
                       │  │
                       │  └──< claim_services >── procedures
                       └──< payments
claims >── diagnoses
```

| Table | Grain | Rows |
|---|---|---|
| `members` | 1 row per member | 1,200 |
| `providers` | 1 row per provider | 220 |
| `provider_specialty_history` | 1 row per provider per specialty period (SCD2) | 60 |
| `claims` | 1 row per claim | 11,981 |
| `claim_services` | 1 row per claim **service line** | 38,592 |
| `payments` | 1 row per payment transaction | 11,465 |
| `plans` / `procedures` / `diagnoses` | 1 row per code | 5 / 32 / 30 |

Full column list and the defect inventory: [SCHEMA.md](SCHEMA.md).

## Querying from Python (no notebook)

```python
import duckdb
con = duckdb.connect("de-interview-prep/data/warehouse/healthcare.duckdb", read_only=True)
con.sql("SELECT status, COUNT(*) FROM claims GROUP BY 1").df()
```

## Rebuilding everything

```bash
.venv/bin/python src/generate_sources.py    # deterministic (seed 20250701) - messy raw + 25 API pages
.venv/bin/python src/build_warehouse.py     # raw -> bronze -> silver -> gold + DQ + dashboard
.venv/bin/python src/dq_checks.py           # print the DQ scorecard and metrics
.venv/bin/python tools/build_notebooks.py   # regenerate all .ipynb from notebooks/_build/*.py
.venv/bin/python tools/run_notebooks.py     # execute all notebooks (verification)
.venv/bin/python tools/verify_project.py    # independent checks on the warehouse invariants
```

`notebooks/_build/*.py` is the source of truth for the notebooks; edit a spec and rebuild rather than editing `.ipynb` by hand.

## Running on Databricks (Serverless & Unity Catalog)

All 12 notebooks connect seamlessly to Databricks Serverless Compute using the Databricks CLI authentication profile (`irajput`):

- **Unity Catalog Delta Tables**: All silver tables are deployed as Delta tables under `workspace.healthcare.*` (`claims`, `claim_services`, `members`, `providers`, `payments`, etc.).
- **Unity Catalog Volume**: Parquet files are stored under `/Volumes/workspace/iraonfridays/de_prep/`.
- **Pre-configured Preamble**: Every notebook automatically initializes:
  ```python
  from de_helpers import get_spark, get_dbutils, spark_q, q, assert_grain
  spark = get_spark()     # DatabricksSession via Databricks CLI profile "irajput"
  dbutils = get_dbutils() # WorkspaceClient dbutils
  ```
  Both `spark.sql("SELECT ... FROM claims")` (running on Databricks) and `q("SELECT ... FROM claims")` (running on local DuckDB) work out of the box with zero configuration!

