# Modern SQL: QUALIFY for Deduplication and Top-N Ranking

In modern cloud data warehouses and lakehouses (Databricks, Snowflake, BigQuery, and DuckDB), **`QUALIFY` is the de facto standard** for deduplication and Top-$N$ ranking.

---

### 1. Why `QUALIFY` Won: The SQL Order of Execution Problem

In standard SQL, you could **never** filter a window function in a `WHERE` clause because `WHERE` executes *before* window functions are computed:

```text
1. FROM / JOIN
2. WHERE                     <-- Evaluated before window functions exist!
3. GROUP BY
4. HAVING
5. WINDOW COMPUTATION        <-- Window functions are evaluated here
6. QUALIFY                   <-- Filters window functions directly!
7. SELECT
8. DISTINCT
9. ORDER BY
10. LIMIT
```

#### The Legacy Way (Verbose Subquery / CTE):
```sql
WITH ranked AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY updated_at DESC) AS rn
    FROM bronze.claims
)
SELECT * EXCLUDE (rn)
FROM ranked
WHERE rn = 1;
```

#### The Modern Way (`QUALIFY`):
```sql
SELECT *
FROM bronze.claims
QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY updated_at DESC) = 1;
```
> **Benefits:** No nested subqueries, no CTE clutter, and no dummy columns (`rn`) you have to remember to drop.

---

### 2. The 3 Most Common Production Patterns with `QUALIFY`

#### Pattern A: Exact Deduplication (Latest Record / SCD1)
Use `ROW_NUMBER() = 1`:
```sql
-- Guarantees exactly 1 row per entity (even if updated_at ties)
SELECT *
FROM raw_events
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id 
    ORDER BY event_timestamp DESC, ingest_batch_id DESC
) = 1;
```

#### Pattern B: Top-$N$ Ranking with Ties (e.g., Top 3 Products per Category)
Use `DENSE_RANK() <= N` or `RANK() <= N`:
```sql
-- Keeps ties in the top 3 spots
SELECT category, product_name, revenue
FROM sales
QUALIFY DENSE_RANK() OVER (
    PARTITION BY category 
    ORDER BY revenue DESC
) <= 3;
```

#### Pattern C: State-Change Detection (Using `LAG` in `QUALIFY`)
You aren't limited to ranks! Any window function works inside `QUALIFY`:
```sql
-- Keep only rows where status changed from the previous row
SELECT user_id, event_time, status
FROM user_logs
QUALIFY LAG(status) OVER (PARTITION BY user_id ORDER BY event_time) <> status 
     OR LAG(status) OVER (PARTITION BY user_id ORDER BY event_time) IS NULL;
```

---

### 3. Under the Hood: The Optimizer Advantage

In modern distributed query engines (Databricks Photon, Spark 3.0+, Snowflake):

When the query planner sees:
```sql
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1
```
It does **not** sort all millions of rows across the cluster. It optimizes this into a **Top-N / WindowExec Heap**:
* Each partition task maintains a bounded heap of size $K$ (e.g., $K=1$ for `rn = 1`).
* Unneeded records are discarded on the fly before shuffles or spills to disk.

---

### 4. Engine Support Cheat Sheet

| Engine | Supports `QUALIFY`? | Notes |
| :--- | :---: | :--- |
| **Databricks (Spark SQL)** | ✅ Yes | Native since Spark 3.0+ / Databricks Runtime |
| **Snowflake** | ✅ Yes | Core feature used heavily in Snowflake pipelines |
| **Google BigQuery** | ✅ Yes | Fully supported |
| **DuckDB** | ✅ Yes | Fully supported |
| **PostgreSQL** | ❌ No | Still requires CTE / subquery (or Postgres-specific `DISTINCT ON`) |
| **MySQL / MS SQL Server**| ❌ No | Still requires CTE / subquery |
