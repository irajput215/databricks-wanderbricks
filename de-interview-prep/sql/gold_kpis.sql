-- ---------------------------------------------------------------------------
-- Gold KPI marts (reference SQL, mirrors src/build_warehouse.py)
-- These are the answers the Q46-Q48 drills must reconcile against.
-- ---------------------------------------------------------------------------

-- Q46: monthly claims KPI.  GRAIN: one row per month.
CREATE OR REPLACE TABLE gold.monthly_claims_kpi AS
SELECT date_trunc('month', claim_date)                                     AS month,
       COUNT(*)                                                            AS total_claims,
       COUNT(*) FILTER (WHERE status = 'Approved')                         AS approved_claims,
       COUNT(*) FILTER (WHERE status = 'Denied')                           AS denied_claims,
       COUNT(*) FILTER (WHERE status = 'Submitted')                        AS submitted_claims,
       ROUND(COUNT(*) FILTER (WHERE status = 'Approved') * 100.0
             / COUNT(*), 2)                                                AS approval_rate,
       ROUND(COUNT(*) FILTER (WHERE status = 'Denied') * 100.0
             / COUNT(*), 2)                                                AS denial_rate,
       SUM(billed_amount)                                                  AS total_billed,
       SUM(paid_amount)                                                    AS total_paid,
       ROUND(SUM(paid_amount) / COUNT(*), 2)                               AS average_claim_amount
FROM silver.claims
GROUP BY 1;

-- Q47/Q48: provider monthly KPI.  GRAIN: one row per provider per month.
-- Each fact is aggregated in its OWN cte before joining - that is what stops
-- claim_services and payments from multiplying the claim amounts.
CREATE OR REPLACE TABLE gold.provider_monthly_kpi AS
WITH claim_agg AS (
    SELECT provider_id,
           date_trunc('month', claim_date)                    AS month,
           COUNT(*)                                           AS claim_count,
           COUNT(*) FILTER (WHERE status = 'Approved')        AS approved_claim_count,
           COUNT(*) FILTER (WHERE status = 'Denied')          AS denied_claim_count,
           SUM(billed_amount)                                 AS total_billed,
           SUM(paid_amount)                                   AS total_paid
    FROM silver.claims
    GROUP BY 1, 2
),
service_agg AS (
    SELECT c.provider_id,
           date_trunc('month', c.claim_date) AS month,
           COUNT(*)                          AS total_services
    FROM silver.claims c
    JOIN silver.claim_services s USING (claim_id)
    GROUP BY 1, 2
)
SELECT a.provider_id, a.month, a.claim_count, a.approved_claim_count, a.denied_claim_count,
       ROUND(a.approved_claim_count * 100.0 / a.claim_count, 2) AS approval_rate,
       ROUND(a.denied_claim_count   * 100.0 / a.claim_count, 2) AS denial_rate,
       a.total_billed, a.total_paid,
       ROUND(a.total_paid / a.claim_count, 2)                   AS average_paid_amount,
       COALESCE(s.total_services, 0)                            AS total_services,
       ROUND(COALESCE(s.total_services, 0) * 1.0 / a.claim_count, 3) AS avg_services_per_claim
FROM claim_agg a
LEFT JOIN service_agg s USING (provider_id, month);

-- Q48: ranking inside each state.  GRAIN: one row per provider per month.
CREATE OR REPLACE TABLE gold.provider_monthly_kpi_ranked AS
SELECT k.*, p.provider_name, p.specialty, p.state, p.network_status,
       rank() OVER (PARTITION BY p.state, k.month ORDER BY k.total_paid DESC) AS provider_rank_in_state
FROM gold.provider_monthly_kpi k
JOIN silver.providers p USING (provider_id);

-- Point-in-time specialty (SCD2): use the specialty that was valid when the claim happened,
-- not the provider's current specialty.
--   FROM silver.claims c
--   JOIN silver.provider_specialty_history h
--     ON h.provider_id = c.provider_id
--    AND c.claim_date BETWEEN h.effective_start AND COALESCE(h.effective_end, DATE '9999-12-31')
