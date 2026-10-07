TITLE = "04 - Window functions (Q34-Q40)"
CELLS = [
    ("md", """Window functions rank, share and compare without collapsing rows - the detail stays visible.
`RANK() OVER (PARTITION BY ... ORDER BY ...)` for ranking, `LAG() OVER (ORDER BY ...)` for time.
Order of execution matters: `WHERE` runs before the window, `QUALIFY` runs after it, which is why
top-N needs `QUALIFY` (or a ranked CTE + `WHERE rn <= 3`)."""),

    # ------------------------------------------------------------------ Q34
    ("md", """#### Q34 - Rank providers by total paid
**Business question:** who are our biggest providers by paid dollars?
**Final grain:** provider
**Source tables:** claims, providers
**Source grains:** claims = 1 row per claim; providers = 1 row per provider
**Watch out:** aggregate first, then rank - a window over raw claims ranks claims, not providers"""),
    ("code", """# YOUR TURN
# output columns: provider_id | provider_name | total_paid | rank
# ordered by rank
# write your query here"""),
    ("code", """# SOLUTION
ranked = q('''
WITH provider_paid AS (
    SELECT
        provider_id,
        SUM(paid_amount) AS total_paid
    FROM claims
    GROUP BY provider_id
)
SELECT
    pp.provider_id,
    p.provider_name,
    ROUND(pp.total_paid, 2)                   AS total_paid,
    RANK() OVER (ORDER BY pp.total_paid DESC) AS rank
FROM provider_paid AS pp
JOIN providers AS p
    ON p.provider_id = pp.provider_id
ORDER BY rank
''')

assert_grain(ranked, "provider_id")
ranked.head(10)"""),
    ("md", """**Why one row per provider:** the CTE collapses 11,981 claims to 215 provider rows, so the
window ranks providers. `RANK` gives tied rows the same rank and then skips (1,1,3) - no two
provider totals tie here, so `RANK`, `DENSE_RANK` and `ROW_NUMBER` would agree."""),

    # ------------------------------------------------------------------ Q35
    ("md", """#### Q35 - Rank providers within each state
**Business question:** who is the biggest provider inside each state?
**Final grain:** provider
**Source tables:** claims, providers
**Source grains:** claims = 1 row per claim; providers = 1 row per provider
**Watch out:** the ranking restarts per state, so `state` must be available in the CTE"""),
    ("code", """# YOUR TURN
# output columns: state | provider_id | provider_name | total_paid | state_rank
# ordered by state, state_rank
# write your query here"""),
    ("code", """# SOLUTION
by_state = q('''
WITH provider_paid AS (
    SELECT
        p.state,
        p.provider_id,
        p.provider_name,
        SUM(c.paid_amount) AS total_paid
    FROM claims AS c
    JOIN providers AS p
        ON p.provider_id = c.provider_id
    GROUP BY p.state, p.provider_id, p.provider_name
)
SELECT
    state,
    provider_id,
    provider_name,
    ROUND(total_paid, 2)                                      AS total_paid,
    RANK() OVER (PARTITION BY state ORDER BY total_paid DESC)  AS state_rank
FROM provider_paid
ORDER BY state, state_rank
''')

assert_grain(by_state, "provider_id")
by_state.head(12)"""),
    ("md", """**Why one row per provider:** a provider belongs to exactly one state, so partitioning by
state cannot duplicate it - the same row set simply gets 20 independent rankings instead of one."""),

    # ------------------------------------------------------------------ Q36
    ("md", """#### Q36 - Top 3 providers in every state
**Business question:** the three biggest providers by paid dollars inside each state
**Final grain:** provider (up to 3 rows per state)
**Source tables:** claims, providers
**Source grains:** claims = 1 row per claim; providers = 1 row per provider
**Watch out:** a window result cannot be filtered in `WHERE` - use `QUALIFY`, or a ranked CTE and `WHERE state_rank <= 3`"""),
    ("code", """# YOUR TURN
# output columns: state | provider_id | provider_name | total_paid | state_rank
# only state_rank <= 3, ordered by state, state_rank
# write your query here"""),
    ("code", """# SOLUTION
top3 = q('''
WITH provider_paid AS (
    SELECT
        p.state,
        p.provider_id,
        p.provider_name,
        SUM(c.paid_amount) AS total_paid
    FROM claims AS c
    JOIN providers AS p
        ON p.provider_id = c.provider_id
    GROUP BY p.state, p.provider_id, p.provider_name
)
SELECT
    state,
    provider_id,
    provider_name,
    ROUND(total_paid, 2)                                      AS total_paid,
    RANK() OVER (PARTITION BY state ORDER BY total_paid DESC)  AS state_rank
FROM provider_paid
QUALIFY state_rank <= 3
ORDER BY state, state_rank
''')

assert_grain(top3, "state", "provider_id")
print(f"{len(top3)} rows - {top3.state.nunique()} states x up to 3 providers")
top3"""),
    ("md", """**RANK vs DENSE_RANK vs ROW_NUMBER:** on a tie, `RANK` gives 1,1,3 (so "top 3" can return 4
rows), `DENSE_RANK` gives 1,1,2, and `ROW_NUMBER` always returns exactly 3 rows but picks one of
the tied rows arbitrarily. With no ties in provider totals all three agree here - choose
deliberately anyway. Without `QUALIFY` the same result is `SELECT * FROM (...) WHERE state_rank <= 3`."""),

    # ------------------------------------------------------------------ Q37
    ("md", """#### Q37 - Share of total company spending
**Business question:** what % of all paid dollars went to this provider?
**Final grain:** provider
**Source tables:** claims, providers
**Source grains:** claims = 1 row per claim; providers = 1 row per provider
**Watch out:** an empty `OVER ()` is the company total - aggregate to provider grain first or the denominator counts claims"""),
    ("code", """# YOUR TURN
# output columns: provider_id | provider_name | total_paid | pct_of_company
# ordered by total_paid DESC
# write your query here"""),
    ("code", """# SOLUTION
share = q('''
WITH provider_paid AS (
    SELECT
        provider_id,
        SUM(paid_amount) AS total_paid
    FROM claims
    GROUP BY provider_id
)
SELECT
    pp.provider_id,
    p.provider_name,
    ROUND(pp.total_paid, 2)                                     AS total_paid,
    ROUND(100 * pp.total_paid / SUM(pp.total_paid) OVER (), 2)  AS pct_of_company
FROM provider_paid AS pp
JOIN providers AS p
    ON p.provider_id = pp.provider_id
ORDER BY total_paid DESC
''')

assert_grain(share, "provider_id")
print(f"shares sum to {share.pct_of_company.sum():.2f}% (rounding drift, not missing money)")
share.head(10)"""),
    ("md", """**Why one row per provider:** the window is computed over the aggregated provider rows, so the
denominator is the 36,897,727.38 company total and every row keeps its provider detail. The
rounded shares add to 99.97% - a good reminder that rounded percentages never have to sum to 100."""),

    # ------------------------------------------------------------------ Q38
    ("md", """#### Q38 - Share of spending within the provider's state
**Business question:** what % of the state's paid dollars went to this provider?
**Final grain:** provider
**Source tables:** claims, providers
**Source grains:** claims = 1 row per claim; providers = 1 row per provider
**Watch out:** only the window changes (`PARTITION BY state`) - same numerator, different question"""),
    ("code", """# YOUR TURN
# output columns: state | provider_id | provider_name | total_paid | pct_of_state
# ordered by state, total_paid DESC
# write your query here"""),
    ("code", """# SOLUTION
state_share = q('''
WITH provider_paid AS (
    SELECT
        p.state,
        p.provider_id,
        p.provider_name,
        SUM(c.paid_amount) AS total_paid
    FROM claims AS c
    JOIN providers AS p
        ON p.provider_id = c.provider_id
    GROUP BY p.state, p.provider_id, p.provider_name
)
SELECT
    state,
    provider_id,
    provider_name,
    ROUND(total_paid, 2)                                                       AS total_paid,
    ROUND(100 * total_paid / SUM(total_paid) OVER (PARTITION BY state), 2)      AS pct_of_state
FROM provider_paid
ORDER BY state, total_paid DESC
''')

assert_grain(state_share, "provider_id")
state_share.head(10)"""),
    ("md", """**Why one row per provider:** partitioning by state keeps the provider grain and only changes
the denominator - PRV0103 is 2.79% of the company (Q37) but 16.69% of California, and Thompson
Medical Group is 44.49% of Arizona, which is what concentration risk looks like."""),

    # ------------------------------------------------------------------ Q39
    ("md", """#### Q39 - Month-over-month change in total paid
**Business question:** how much did paid dollars move versus the previous month?
**Final grain:** month
**Source tables:** claims
**Source grains:** claims = 1 row per claim
**Watch out:** aggregate to month FIRST - `LAG(paid_amount)` on raw claims compares the previous *claim*"""),
    ("code", """# YOUR TURN
# output columns: month | total_paid | previous_month_paid | change
# ordered by month
# write your query here"""),
    ("code", """# SOLUTION
mom = q('''
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', claim_date) AS month,
        SUM(paid_amount)                AS total_paid
    FROM claims
    GROUP BY DATE_TRUNC('month', claim_date)
)
SELECT
    STRFTIME(month, '%Y-%m')                                      AS month,
    ROUND(total_paid, 2)                                          AS total_paid,
    ROUND(LAG(total_paid) OVER (ORDER BY month), 2)               AS previous_month_paid,
    ROUND(total_paid - LAG(total_paid) OVER (ORDER BY month), 2)  AS change
FROM monthly
ORDER BY month
''')

assert_grain(mom, "month")
print(f"{len(mom)} months, {mom.change.notna().sum()} with a previous month")
mom"""),
    ("md", """**Why one row per month:** the CTE gives one row per month, so `LAG` reaches exactly one month
back on the ordered rows. All 18 months are present, so no `LAG` silently jumps a gap - check that
with a row count before trusting any month-over-month series."""),

    # ------------------------------------------------------------------ Q40
    ("md", """#### Q40 - Month-over-month % growth
**Business question:** what is the growth rate month over month?
**Final grain:** month
**Source tables:** claims
**Source grains:** claims = 1 row per claim
**Watch out:** divide by `NULLIF(previous_month_paid, 0)` - a zero prior month must yield NULL, never an error"""),
    ("code", """# YOUR TURN
# output columns: month | total_paid | previous_month_paid | pct_growth
# first month has no previous month -> pct_growth must be NULL, not 0 and not 100
# write your query here"""),
    ("code", """# SOLUTION
growth = q('''
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', claim_date) AS month,
        SUM(paid_amount)                AS total_paid
    FROM claims
    GROUP BY DATE_TRUNC('month', claim_date)
),
with_lag AS (
    SELECT
        month,
        total_paid,
        LAG(total_paid) OVER (ORDER BY month) AS previous_month_paid
    FROM monthly
)
SELECT
    STRFTIME(month, '%Y-%m')                                                          AS month,
    ROUND(total_paid, 2)                                                              AS total_paid,
    ROUND(previous_month_paid, 2)                                                     AS previous_month_paid,
    ROUND(100 * (total_paid - previous_month_paid) / NULLIF(previous_month_paid, 0), 2) AS pct_growth
FROM with_lag
ORDER BY month
''')

assert_grain(growth, "month")
print(f"first month on its own: {growth.iloc[0].month} -> pct_growth = {growth.iloc[0].pct_growth}")
growth"""),
    ("md", """**Why the first month is NULL:** there is no prior month, so `LAG` returns NULL and the
division stays NULL - that is the honest answer. `NULLIF(previous_month_paid, 0)` buys the same
protection for a real zero month instead of a divide-by-zero. Use `COALESCE(..., 0)` only if the
business explicitly wants "no comparison" reported as 0%."""),
]
