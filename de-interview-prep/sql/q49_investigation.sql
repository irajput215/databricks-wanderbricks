-- ---------------------------------------------------------------------------
-- Q49 investigation: "the dashboard says +40% but the source says +5%".
-- Work the pipeline stage by stage; each query localises the break further.
-- Run with: duckdb data/warehouse/healthcare.duckdb  (or q("...") in the notebooks)
-- ---------------------------------------------------------------------------

-- 1. Counts at every stage -----------------------------------------------------
SELECT 'raw.claims_api_records'  AS stage, COUNT(*) AS rows FROM raw.claims_api_records
UNION ALL SELECT 'bronze.claims (as landed)',  COUNT(*) FROM bronze.claims
UNION ALL SELECT 'silver.claims (published)',  COUNT(*) FROM silver.claims
UNION ALL SELECT 'dashboard.claims_dashboard', COUNT(*) FROM dashboard.claims_dashboard;

-- 2. Duplicate ingestion? ------------------------------------------------------
SELECT _ingest_batch, COUNT(*) AS rows, COUNT(DISTINCT claim_id) AS distinct_claims,
       COUNT(*) - COUNT(DISTINCT claim_id) AS duplicated_rows
FROM bronze.claims
GROUP BY 1
ORDER BY rows DESC;

-- 3. Join multiplication? ------------------------------------------------------
SELECT COUNT(*) AS service_line_rows,
       COUNT(DISTINCT claim_id) AS distinct_claims,
       ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT claim_id), 3) AS fan_out_factor
FROM dashboard.claims_dashboard;

-- 4. Localise the break by month ----------------------------------------------
SELECT s.month, s.source_claims, d.dashboard_rows,
       ROUND(d.dashboard_rows * 1.0 / s.source_claims, 2) AS inflation_factor
FROM (SELECT date_trunc('month', claim_date) AS month, COUNT(*) AS source_claims
      FROM silver.claims GROUP BY 1) s
JOIN dashboard.monthly_claim_counts d USING (month)
WHERE s.month >= DATE '2025-01-01'
ORDER BY s.month;

-- 5. The headline numbers ------------------------------------------------------
WITH src AS (
    SELECT COUNT(*) FILTER (WHERE claim_date >= DATE '2025-03-01' AND claim_date < DATE '2025-04-01') AS mar,
           COUNT(*) FILTER (WHERE claim_date >= DATE '2025-04-01' AND claim_date < DATE '2025-05-01') AS apr
    FROM silver.claims
), dash AS (
    SELECT SUM(dashboard_count) FILTER (WHERE month = TIMESTAMP '2025-03-01') AS mar,
           SUM(dashboard_count) FILTER (WHERE month = TIMESTAMP '2025-04-01') AS apr
    FROM dashboard.monthly_claim_counts
)
SELECT ROUND((src.apr / src.mar - 1) * 100, 2)  AS source_growth_pct,   -- ~+5%
       ROUND((dash.apr / dash.mar - 1) * 100, 2) AS dashboard_growth_pct -- ~+40%
FROM src, dash;

-- 6. Where did records disappear? (raw -> silver) ------------------------------
SELECT reason, COUNT(*) AS records
FROM dq.quarantine
GROUP BY 1 ORDER BY records DESC;

-- 7. The corrected monthly series ---------------------------------------------
SELECT date_trunc('month', claim_date) AS month,
       COUNT(*)                        AS total_claims,
       COUNT(DISTINCT claim_id)        AS distinct_claims
FROM silver.claims
GROUP BY 1 ORDER BY 1;
