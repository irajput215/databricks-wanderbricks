TITLE = "06 - Data engineering scenarios (Q46-Q50)"
CELLS = [
    # ------------------------------------------------------------------ Q46
    ("md", """#### Q46 - Monthly claims KPI table
**Business question:** publish one monthly row with volume, approval, denial and money.
**Final grain:** one row per calendar month = `date_trunc('month', claim_date)`, 18 rows for 2024-01..2025-06.
**Counted unit:** `claim_id`, i.e. claims - never service lines, so nothing is joined before the count.
**Source tables:** claims (via the `main.claims` view over `silver.claims`)
**Source grains:** claims = 1 row per claim
**Grain risk:** none - a single aggregation with no joins."""),

    ("code", """# YOUR TURN
# output columns: month, total_claims, approved_claims, denied_claims, approval_rate,
#                 denial_rate, total_billed, total_paid, average_claim_amount
# grain: month
# write your query here"""),

    ("code", """# SOLUTION
MONTHLY_KPI = '''
SELECT
    date_trunc('month', claim_date) AS month,
    COUNT(*) AS total_claims,
    COUNT(*) FILTER (WHERE status = 'Approved') AS approved_claims,
    COUNT(*) FILTER (WHERE status = 'Denied') AS denied_claims,
    ROUND(COUNT(*) FILTER (WHERE status = 'Approved') * 100.0
          / NULLIF(COUNT(*), 0), 2) AS approval_rate,
    ROUND(COUNT(*) FILTER (WHERE status = 'Denied') * 100.0
          / NULLIF(COUNT(*), 0), 2) AS denial_rate,
    SUM(billed_amount) AS total_billed,
    SUM(paid_amount) AS total_paid,
    ROUND(SUM(paid_amount) / NULLIF(COUNT(*), 0), 2) AS average_claim_amount
FROM claims
GROUP BY 1
'''
df = q(MONTHLY_KPI + 'ORDER BY month')
assert_grain(df, 'month')
df"""),

    ("md", """**Reconcile:** the hand-written query must equal the reference mart `gold.monthly_claims_kpi` - 0 rows on both sides of the `EXCEPT`."""),

    ("code", """# RECONCILE against the reference build
q(f'''
WITH mine AS ({MONTHLY_KPI}),
ref AS (
    SELECT month, total_claims, approved_claims, denied_claims, approval_rate, denial_rate,
           total_billed, total_paid, average_claim_amount
    FROM gold.monthly_claims_kpi
)
SELECT 'only_in_mine' AS difference, COUNT(*) AS rows_diff
FROM (SELECT * FROM mine EXCEPT SELECT * FROM ref)
UNION ALL
SELECT 'only_in_ref', COUNT(*)
FROM (SELECT * FROM ref EXCEPT SELECT * FROM mine)
''')"""),

    # ------------------------------------------------------------------ Q47
    ("md", """#### Q47 - Provider monthly KPI dataset
**Business question:** give each provider a monthly scorecard.
**Final grain:** provider_id + month
**Source tables:** claims
**Source grains:** claims = 1 row per claim
**Grain risk:** none while only claims are aggregated - one pass, group by both keys"""),

    ("code", """# YOUR TURN
# output columns: provider_id, month, claim_count, approved_claim_count,
#                 denied_claim_count, approval_rate, total_billed, total_paid,
#                 average_paid_amount
# grain: provider_id + month
# write your query here"""),

    ("code", """# SOLUTION
PROVIDER_MONTHLY = '''
SELECT
    c.provider_id,
    date_trunc('month', c.claim_date) AS month,
    COUNT(*) AS claim_count,
    COUNT(*) FILTER (WHERE c.status = 'Approved') AS approved_claim_count,
    COUNT(*) FILTER (WHERE c.status = 'Denied') AS denied_claim_count,
    ROUND(COUNT(*) FILTER (WHERE c.status = 'Approved') * 100.0
          / NULLIF(COUNT(*), 0), 2) AS approval_rate,
    SUM(c.billed_amount) AS total_billed,
    SUM(c.paid_amount) AS total_paid,
    ROUND(SUM(c.paid_amount) / NULLIF(COUNT(*), 0), 2) AS average_paid_amount
FROM claims c
GROUP BY 1, 2
'''
df = q(PROVIDER_MONTHLY + 'ORDER BY provider_id, month')
assert_grain(df, 'provider_id', 'month')
df"""),

    ("md", """**Reconcile:** must equal `gold.provider_monthly_kpi` on the shared columns - 0 rows on both sides of the `EXCEPT`."""),

    ("code", """# RECONCILE against the reference build
q(f'''
WITH mine AS ({PROVIDER_MONTHLY}),
ref AS (
    SELECT provider_id, month, claim_count, approved_claim_count, denied_claim_count,
           approval_rate, total_billed, total_paid, average_paid_amount
    FROM gold.provider_monthly_kpi
)
SELECT 'only_in_mine' AS difference, COUNT(*) AS rows_diff
FROM (SELECT * FROM mine EXCEPT SELECT * FROM ref)
UNION ALL
SELECT 'only_in_ref', COUNT(*)
FROM (SELECT * FROM ref EXCEPT SELECT * FROM mine)
''')"""),

    # ------------------------------------------------------------------ Q48
    ("md", """#### Q48 - Provider monthly KPIs, enriched and ranked
**Business question:** rank providers inside their state, month by month.
**Final grain:** provider_id + month (the dimension lookup keeps it)
**Source tables:** claims, providers
**Source grains:** claims = 1 row per claim; providers = 1 row per provider
**Grain risk:** none - providers is unique on provider_id, so the join cannot fan out."""),

    ("code", """# YOUR TURN
# output columns: provider_id, month, claim_count, approved_claim_count,
#                 denied_claim_count, approval_rate, total_billed, total_paid,
#                 average_paid_amount, provider_name, specialty, state,
#                 provider_rank_in_state
# grain: provider_id + month
# write your query here"""),

    ("code", """# SOLUTION - reuse Q47 as the base CTE instead of retyping it
PROVIDER_MONTHLY_RANKED = f'''
WITH base AS ({PROVIDER_MONTHLY}),
ranked AS (
    SELECT
        b.provider_id,
        b.month,
        b.claim_count,
        b.approved_claim_count,
        b.denied_claim_count,
        b.approval_rate,
        b.total_billed,
        b.total_paid,
        b.average_paid_amount,
        p.provider_name,
        p.specialty,
        p.state,
        rank() OVER (PARTITION BY p.state, b.month ORDER BY b.total_paid DESC)
            AS provider_rank_in_state
    FROM base b
    JOIN providers p USING (provider_id)
)
SELECT * FROM ranked
'''
df = q(PROVIDER_MONTHLY_RANKED + 'ORDER BY state, month, provider_rank_in_state')
assert_grain(df, 'provider_id', 'month')
df"""),

    ("md", """**Reconcile:** must equal `gold.provider_monthly_kpi_ranked` on the shared columns - 0 rows on both sides of the `EXCEPT`."""),

    ("code", """# RECONCILE against the reference build
q(f'''
WITH mine AS ({PROVIDER_MONTHLY_RANKED}),
ref AS (
    SELECT provider_id, month, claim_count, approved_claim_count, denied_claim_count,
           approval_rate, total_billed, total_paid, average_paid_amount,
           provider_name, specialty, state, provider_rank_in_state
    FROM gold.provider_monthly_kpi_ranked
)
SELECT 'only_in_mine' AS difference, COUNT(*) AS rows_diff
FROM (SELECT * FROM mine EXCEPT SELECT * FROM ref)
UNION ALL
SELECT 'only_in_ref', COUNT(*)
FROM (SELECT * FROM ref EXCEPT SELECT * FROM mine)
''')"""),

    # ------------------------------------------------------------------ Q49
    ("md", """#### Q49 - Investigation: why does the dashboard say +40% when the source says +5%?
**Business question:** is the Mar -> Apr 2025 claim surge real?
**Final grain:** one row per pipeline stage, then one row per month.
**Evidence chain:** raw.claims_api_records -> bronze.claims -> silver.claims -> dashboard.claims_dashboard -> dashboard.monthly_claim_counts
**Grain risk:** every join to claim_services fans a claim out to service-line grain, so counting rows instead of claims explodes the number.
**Method:** walk the chain stage by stage, quantify each jump, name the batch, then restate the truth from silver."""),

    ("code", """# STAGE TABLE - how many rows does each stage hold, and where do they move?
q('''
WITH stage_counts AS (
    SELECT 1 AS step, 'raw.claims_api_records' AS stage, COUNT(*) AS row_count
    FROM raw.claims_api_records
    UNION ALL
    SELECT 2, 'bronze.claims', COUNT(*) FROM bronze.claims
    UNION ALL
    SELECT 3, 'silver.claims', COUNT(*) FROM silver.claims
    UNION ALL
    SELECT 4, 'dashboard.claims_dashboard', COUNT(*) FROM dashboard.claims_dashboard
    UNION ALL
    SELECT 5, 'dashboard.monthly_claim_counts', CAST(SUM(dashboard_count) AS BIGINT)
    FROM dashboard.monthly_claim_counts
)
SELECT
    step,
    stage,
    row_count,
    row_count - LAG(row_count) OVER (ORDER BY step) AS delta_vs_prev,
    ROUND((row_count - LAG(row_count) OVER (ORDER BY step)) * 100.0
          / NULLIF(LAG(row_count) OVER (ORDER BY step), 0), 2) AS pct_vs_prev
FROM stage_counts
ORDER BY step
''')"""),

    ("code", """# QUERY 1 - raw vs bronze vs silver: the small, explainable drops
# 12,061 landed -> 12,358 in bronze (+302 replay rows, -5 rejected records) -> 11,981 published
q('''
WITH layer_counts AS (
    SELECT 1 AS step, 'raw.claims_api_records' AS layer,
           COUNT(*) AS rows, COUNT(DISTINCT claim_id) AS distinct_claim_ids
    FROM raw.claims_api_records
    UNION ALL
    SELECT 2, 'bronze.claims', COUNT(*), COUNT(DISTINCT claim_id) FROM bronze.claims
    UNION ALL
    SELECT 3, 'silver.claims', COUNT(*), COUNT(DISTINCT claim_id) FROM silver.claims
)
SELECT
    layer,
    rows,
    distinct_claim_ids,
    rows - LAG(rows) OVER (ORDER BY step) AS delta,
    ROUND((rows - LAG(rows) OVER (ORDER BY step)) * 100.0
          / NULLIF(LAG(rows) OVER (ORDER BY step), 0), 2) AS pct_change
FROM layer_counts
ORDER BY step
''')"""),

    ("code", """# QUERY 2 - who duplicated the claim_ids? the replay batch is not idempotent
q('''
WITH per_batch AS (
    SELECT claim_id, _ingest_batch, COUNT(*) AS rows_this_batch
    FROM bronze.claims
    GROUP BY 1, 2
),
duplicate_ids AS (
    SELECT claim_id FROM per_batch GROUP BY 1 HAVING SUM(rows_this_batch) > 1
)
SELECT
    p._ingest_batch,
    CAST(SUM(p.rows_this_batch) AS BIGINT) AS batch_rows,
    COUNT(*) AS distinct_claim_ids,
    COUNT(*) FILTER (WHERE p.claim_id IN (SELECT claim_id FROM duplicate_ids))
        AS duplicated_claim_ids,
    COUNT(*) FILTER (WHERE p.rows_this_batch > 1) AS repeated_within_same_batch
FROM per_batch p
GROUP BY 1
ORDER BY batch_rows DESC
''')"""),

    ("code", """# QUERY 3 - the dashboard mart sits at service-line grain, not claim grain
q('''
SELECT
    COUNT(*) AS dashboard_rows,
    COUNT(DISTINCT claim_id) AS distinct_claims,
    ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT claim_id), 0), 2) AS rows_per_claim
FROM dashboard.claims_dashboard
''')"""),

    ("code", """# QUERY 4 - quantify the April inflation: dashboard rows vs published claims
q('''
WITH source_counts AS (
    SELECT date_trunc('month', claim_date) AS month, COUNT(*) AS source_claims
    FROM silver.claims
    WHERE claim_date >= DATE '2025-03-01' AND claim_date < DATE '2025-05-01'
    GROUP BY 1
),
dashboard_counts AS (
    SELECT month, dashboard_count
    FROM dashboard.monthly_claim_counts
    WHERE month IN (DATE '2025-03-01', DATE '2025-04-01')
)
SELECT
    d.month,
    s.source_claims,
    d.dashboard_count,
    ROUND(d.dashboard_count * 1.0 / NULLIF(s.source_claims, 0), 2) AS dashboard_inflation_x,
    ROUND((s.source_claims - LAG(s.source_claims) OVER (ORDER BY d.month)) * 100.0
          / NULLIF(LAG(s.source_claims) OVER (ORDER BY d.month), 0), 2) AS source_mom_pct,
    ROUND((d.dashboard_count - LAG(d.dashboard_count) OVER (ORDER BY d.month)) * 100.0
          / NULLIF(LAG(d.dashboard_count) OVER (ORDER BY d.month), 0), 2) AS dashboard_mom_pct
FROM dashboard_counts d
JOIN source_counts s USING (month)
ORDER BY d.month
''')"""),

    ("code", """# QUERY 5 - the corrected answer: count claims at claim grain from silver
q('''
WITH monthly_claims AS (
    SELECT date_trunc('month', claim_date) AS month, COUNT(*) AS total_claims
    FROM silver.claims
    GROUP BY 1
)
SELECT
    month,
    total_claims,
    total_claims - LAG(total_claims) OVER (ORDER BY month) AS mom_change,
    ROUND((total_claims - LAG(total_claims) OVER (ORDER BY month)) * 100.0
          / NULLIF(LAG(total_claims) OVER (ORDER BY month), 0), 2) AS mom_growth_pct
FROM monthly_claims
ORDER BY month
''')"""),

    ("md", """**Root cause:**
- (a) join fan-out: `dashboard.claims_dashboard` joins `bronze.claims` to `bronze.claim_services_raw`, so it holds one row per service line (3.32x claims), and the mart then counts rows instead of distinct claims.
- (b) duplicated April load: the non-idempotent replay batch `2025-04-16T02:05:00Z` re-inserted 302 rows / 298 claim_ids that the first load already contained, and every one of them is dated April.
**Fix:** dedupe to one row per `claim_id` (`row_number()` on `updated_at DESC`) before joining any fact, count at claim grain (keep `COUNT(DISTINCT claim_id)` as the guard), and make the write idempotent - `MERGE`/upsert on `claim_id` or overwrite the batch partition."""),

    # ------------------------------------------------------------------ Q50
    ("md", """#### Q50 - The real DE problem: a reliable monthly provider KPI dataset
**Business question:** one trustworthy row per provider per month, built from dirty layered data in a single SELECT.
**Final grain:** provider_id + month (3,672 rows = 215 providers x 18 months, thinned)
**Source tables:** bronze.claims (deduped), silver.claim_services, silver.payments, silver.providers, silver.provider_specialty_history
**Source grains:** claims = 1 row per claim; claim_services = 1 row per service line; payments = 1 row per payment; specialty history = 1 row per provider per period (SCD2)
**Grain risk:** high - both claim_services and payments are many-per-claim, so each must be collapsed to provider+month in its own CTE before any join."""),

    ("code", """# YOUR TURN
# output columns: provider_id, month, provider_name, state, specialty, claim_count,
#                 approved_claim_count, denied_claim_count, approval_rate, denial_rate,
#                 total_billed, total_paid, average_paid_amount, total_services,
#                 avg_services_per_claim, total_payment_amount, provider_rank_in_state
# grain: provider_id + month
# write your query here"""),

    ("code", """# SOLUTION - the final SELECT you would materialise (no CREATE TABLE here: this file is read-only)
PROVIDER_MONTHLY_KPI = '''
WITH claims_dedup AS (
    -- (1) one row per claim_id: newest version wins; unresolved FK rows never publish
    SELECT b.claim_id, b.member_id, b.provider_id, b.claim_date, b.status,
           b.billed_amount, b.paid_amount
    FROM (
        SELECT *, row_number() OVER (PARTITION BY claim_id
                                     ORDER BY updated_at DESC, _loaded_at DESC) AS rn
        FROM bronze.claims
    ) b
    WHERE b.rn = 1
      AND b.member_id IN (SELECT member_id FROM silver.members)
      AND b.provider_id IN (SELECT provider_id FROM silver.providers)
),
claims_pit AS (
    -- (2) point-in-time specialty: a claim must use the SCD2 row covering its claim_date.
    --     Joining today's providers.specialty would re-label historical claims with the
    --     specialty the provider holds now, silently corrupting specialty-level reporting.
    SELECT c.*, h.specialty AS specialty_at_claim_date
    FROM claims_dedup c
    LEFT JOIN silver.provider_specialty_history h
           ON h.provider_id = c.provider_id
          AND c.claim_date BETWEEN h.effective_start
                               AND COALESCE(h.effective_end, DATE '9999-12-31')
),
claim_agg AS (
    -- (3) claims aggregated once, at the final grain
    SELECT
        provider_id,
        date_trunc('month', claim_date) AS month,
        COUNT(*) AS claim_count,
        COUNT(*) FILTER (WHERE status = 'Approved') AS approved_claim_count,
        COUNT(*) FILTER (WHERE status = 'Denied') AS denied_claim_count,
        SUM(billed_amount) AS total_billed,
        SUM(paid_amount) AS total_paid,
        -- 14 provider-months genuinely switch specialty mid-month: take the specialty
        -- in effect at that provider-month's last claim.
        arg_max(specialty_at_claim_date, claim_date) AS specialty_pit
    FROM claims_pit
    GROUP BY 1, 2
),
service_agg AS (
    -- (4) service lines collapsed to provider+month BEFORE the join (38,592 rows -> 3,672)
    SELECT c.provider_id, date_trunc('month', c.claim_date) AS month,
           COUNT(*) AS total_services
    FROM claims_dedup c
    JOIN silver.claim_services s USING (claim_id)
    GROUP BY 1, 2
),
payment_agg AS (
    -- (5) the payment register collapsed separately: never join two fact tables in one query
    SELECT c.provider_id, date_trunc('month', c.claim_date) AS month,
           SUM(p.payment_amount) AS total_payment_amount
    FROM claims_dedup c
    JOIN silver.payments p USING (claim_id)
    GROUP BY 1, 2
),
kpi AS (
    SELECT
        a.provider_id,
        a.month,
        pr.provider_name,
        pr.state,
        COALESCE(a.specialty_pit, pr.specialty) AS specialty,
        a.claim_count,
        a.approved_claim_count,
        a.denied_claim_count,
        ROUND(a.approved_claim_count * 100.0 / NULLIF(a.claim_count, 0), 2) AS approval_rate,
        ROUND(a.denied_claim_count * 100.0 / NULLIF(a.claim_count, 0), 2) AS denial_rate,
        a.total_billed,
        a.total_paid,
        ROUND(a.total_paid / NULLIF(a.claim_count, 0), 2) AS average_paid_amount,
        COALESCE(s.total_services, 0) AS total_services,
        ROUND(COALESCE(s.total_services, 0) * 1.0 / NULLIF(a.claim_count, 0), 3)
            AS avg_services_per_claim,
        COALESCE(p.total_payment_amount, 0) AS total_payment_amount
    FROM claim_agg a
    JOIN silver.providers pr USING (provider_id)
    LEFT JOIN service_agg s USING (provider_id, month)
    LEFT JOIN payment_agg p USING (provider_id, month)
)
SELECT
    k.*,
    rank() OVER (PARTITION BY k.state, k.month ORDER BY k.total_paid DESC)
        AS provider_rank_in_state
FROM kpi k
'''
df = q(PROVIDER_MONTHLY_KPI + 'ORDER BY provider_id, month')
assert_grain(df, 'provider_id', 'month')
df"""),

    ("code", """# VERIFY - grain holds, and the numbers match the reference mart on every shared column
df = q(PROVIDER_MONTHLY_KPI)
assert_grain(df, 'provider_id', 'month')
q(f'''
WITH mine AS (
    SELECT provider_id, month, provider_name, state, claim_count, approved_claim_count,
           denied_claim_count, approval_rate, denial_rate, total_billed, total_paid,
           average_paid_amount, total_services, avg_services_per_claim,
           provider_rank_in_state
    FROM ({PROVIDER_MONTHLY_KPI})
),
ref AS (
    SELECT provider_id, month, provider_name, state, claim_count, approved_claim_count,
           denied_claim_count, approval_rate, denial_rate, total_billed, total_paid,
           average_paid_amount, total_services, avg_services_per_claim,
           provider_rank_in_state
    FROM gold.provider_monthly_kpi_ranked
)
SELECT 'only_in_mine' AS difference, COUNT(*) AS rows_diff
FROM (SELECT * FROM mine EXCEPT SELECT * FROM ref)
UNION ALL
SELECT 'only_in_ref', COUNT(*)
FROM (SELECT * FROM ref EXCEPT SELECT * FROM mine)
UNION ALL
SELECT 'specialty_differs_from_current_specialty',
       (SELECT COUNT(*)
        FROM (SELECT provider_id, month, specialty FROM ({PROVIDER_MONTHLY_KPI})) m
        JOIN gold.provider_monthly_kpi_ranked g USING (provider_id, month)
        WHERE m.specialty IS DISTINCT FROM g.specialty)
''')"""),

    ("md", """**Design decisions:**
- **Grain:** one row per `provider_id` + `month`; `claim_id` is the counted unit, so nothing finer survives the aggregation.
- **claim_services cannot multiply amounts:** service lines are summed in `service_agg` to provider+month first, so the join adds one `total_services` column, never extra claim rows.
- **payments cannot multiply:** the payment register is summed in `payment_agg` to the same grain; the two facts never meet at claim grain.
- **Duplicates:** `row_number()` over `claim_id` ordered by `updated_at DESC, _loaded_at DESC` keeps one version, which also absorbs the 302-row April replay batch.
- **Late-arriving claims:** every metric is keyed on `claim_date` (event time), not ingest time, so a late claim lands in its true month - re-run the affected months with `MERGE` on (provider_id, month).
- **Historical specialty:** the SCD2 join on `claim_date BETWEEN effective_start AND COALESCE(effective_end, '9999-12-31')` gives the specialty in force when the claim happened; using `providers.specialty` would mislabel 203 historical provider-months.
- **Idempotency:** dedupe + full recompute of each (provider_id, month) key means re-running the same batch rewrites identical rows instead of appending duplicates."""),
]
