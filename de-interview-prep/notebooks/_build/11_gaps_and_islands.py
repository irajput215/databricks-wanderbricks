TITLE = "11 - Gaps & islands"
CELLS = [
("md", """**The three patterns you actually get asked, on real claims data:**
- **A - sequential values:** `value - ROW_NUMBER()` finds consecutive-day runs
- **B - state changes:** `LAG` + cumulative `SUM` finds islands of days above a threshold
- **C - gaps:** `LAG` + `date_diff` measures the gap between events
- plus the variation that breaks all of them: duplicates in the input.
"""),
("md", """**Pattern A - consecutive days of claim activity per provider**
- **Business question:** which providers billed on consecutive calendar days, and for how long?
- **Final grain:** one row per provider per run (`provider_id` + `island_key`) -> `run_start, run_end, run_days`.
- **Source tables:** `silver.claims`.
- **Source grain:** one row per claim - so collapse to `provider_id, claim_date` first.
- **What breaks it:** duplicate rows per provider-day; they get different `ROW_NUMBER`s and split the run.
"""),
("code", """# YOUR TURN: return run_start, run_end, run_days per provider.
# Expected columns: ['provider_id', 'run_start', 'run_end', 'run_days'].
expected_columns = ['provider_id', 'run_start', 'run_end', 'run_days']
print('islands output will have:', expected_columns)
"""),
("code", """# SOLUTION
print(q('''
WITH activity AS (
    SELECT DISTINCT provider_id, claim_date FROM silver.claims
), islands AS (
    SELECT provider_id, claim_date,
           claim_date - (row_number() OVER (PARTITION BY provider_id ORDER BY claim_date))::INT AS island_key
    FROM activity
)
SELECT provider_id, MIN(claim_date) AS run_start, MAX(claim_date) AS run_end, COUNT(*) AS run_days
FROM islands
GROUP BY provider_id, island_key
ORDER BY run_days DESC, provider_id
LIMIT 10
''').to_string(index=False))
print(q('''
WITH activity AS (
    SELECT DISTINCT provider_id, claim_date FROM silver.claims
), islands AS (
    SELECT provider_id, claim_date,
           claim_date - (row_number() OVER (PARTITION BY provider_id ORDER BY claim_date))::INT AS island_key
    FROM activity
), runs AS (
    SELECT provider_id, island_key, COUNT(*) AS run_days FROM islands GROUP BY 1, 2
)
SELECT run_days, COUNT(*) AS runs FROM runs GROUP BY 1 ORDER BY 1 DESC
''').to_string(index=False))
"""),
("md", """**Why the difference is constant:** inside a run, both `claim_date` and `ROW_NUMBER()` advance by exactly one day, so `claim_date - ROW_NUMBER()` never changes. Any gap makes the date jump further than the counter, so the difference drops to a new value - that value *is* the island.
"""),
("md", """**Drill 2 - longest streak per provider**
- **Business question:** what is each provider's best consecutive-day run?
- **Final grain:** one row per provider.
- **Source tables:** `silver.claims`.
- **Source grain:** claims -> provider/day -> runs.
- **What breaks it:** picking `MAX(run_days)` without the run's dates loses the window the streak happened in.
"""),
("code", """# YOUR TURN: islands -> aggregate -> row_number to keep only the longest run per provider.
# Expected columns: ['provider_id', 'run_start', 'run_end', 'run_days'].
expected_columns = ['provider_id', 'run_start', 'run_end', 'run_days']
print('one row per provider:', expected_columns)
"""),
("code", """# SOLUTION
streaks = q('''
WITH activity AS (
    SELECT DISTINCT provider_id, claim_date FROM silver.claims
), islands AS (
    SELECT provider_id, claim_date,
           claim_date - (row_number() OVER (PARTITION BY provider_id ORDER BY claim_date))::INT AS island_key
    FROM activity
), runs AS (
    SELECT provider_id, island_key, MIN(claim_date) AS run_start, MAX(claim_date) AS run_end,
           COUNT(*) AS run_days
    FROM islands GROUP BY 1, 2
)
SELECT provider_id, run_start, run_end, run_days
FROM runs
QUALIFY row_number() OVER (PARTITION BY provider_id ORDER BY run_days DESC, run_start) = 1
ORDER BY run_days DESC, provider_id
LIMIT 10
''')
print(streaks.to_string(index=False))
print()
print(q('''
WITH activity AS (
    SELECT DISTINCT provider_id, claim_date FROM silver.claims
), islands AS (
    SELECT provider_id, claim_date,
           claim_date - (row_number() OVER (PARTITION BY provider_id ORDER BY claim_date))::INT AS island_key
    FROM activity
), runs AS (
    SELECT provider_id, island_key, COUNT(*) AS run_days FROM islands GROUP BY 1, 2
), best AS (
    SELECT provider_id, MAX(run_days) AS best_run FROM runs GROUP BY 1
)
SELECT COUNT(*) AS providers, MAX(best_run) AS longest_streak, ROUND(AVG(best_run), 2) AS avg_best_streak FROM best
''').to_string(index=False))
"""),
("md", """**Takeaway:** `QUALIFY` lets you keep the ranking filter in the same query - islands -> aggregate -> rank -> filter, no self-join and no subquery nesting.
"""),
("md", """**Pattern B - islands of days above a volume threshold**
- **Business question:** when did PRV0048 have sustained service volume (>= 2 lines/day), and for how many days in a row?
- **Final grain:** one row per island (`island_key`) -> `run_start, run_end, run_days, services_in_run`.
- **Source tables:** `silver.claim_services` joined to `silver.claims`.
- **Source grains:** claims = 1 row/claim, claim_services = 1 row/service line.
- **What breaks it:** a threshold the data never crosses - check the distribution first (`claims/day` tops out at 3 here).
"""),
("code", """# YOUR TURN: flag each provider-day TRUE/FALSE, then build islands with a cumulative SUM.
# Expected columns: ['activity_date', 'services', 'above_threshold', 'island_key'].
expected_columns = ['activity_date', 'services', 'above_threshold', 'island_key']
print('flagged day grain:', expected_columns)
"""),
("code", """# SOLUTION
flagged = q('''
WITH daily AS (
    SELECT c.provider_id, s.service_date AS activity_date, COUNT(*) AS services
    FROM silver.claim_services s JOIN silver.claims c USING (claim_id)
    WHERE c.provider_id = 'PRV0048'
    GROUP BY 1, 2
), flagged AS (
    SELECT provider_id, activity_date, services, services >= 2 AS above_threshold FROM daily
)
SELECT activity_date, services, above_threshold,
       CAST(SUM(CASE WHEN above_threshold THEN 0 ELSE 1 END)
           OVER (PARTITION BY provider_id ORDER BY activity_date
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INTEGER) AS island_key
FROM flagged
ORDER BY activity_date
LIMIT 12
''')
print(flagged.to_string(index=False))
print()
print(q('''
WITH daily AS (
    SELECT c.provider_id, s.service_date AS activity_date, COUNT(*) AS services
    FROM silver.claim_services s JOIN silver.claims c USING (claim_id)
    WHERE c.provider_id = 'PRV0048'
    GROUP BY 1, 2
), flagged AS (
    SELECT provider_id, activity_date, services, services >= 2 AS above_threshold FROM daily
), islands AS (
    SELECT provider_id, activity_date, services, above_threshold,
           CAST(SUM(CASE WHEN above_threshold THEN 0 ELSE 1 END)
               OVER (PARTITION BY provider_id ORDER BY activity_date
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INTEGER) AS island_key
    FROM flagged
)
SELECT MIN(activity_date) AS run_start, MAX(activity_date) AS run_end, COUNT(*) AS run_days,
       SUM(services) AS services_in_run
FROM islands WHERE above_threshold
GROUP BY island_key
ORDER BY run_days DESC, run_start
LIMIT 8
''').to_string(index=False))
"""),
("md", """**Takeaway:** every FALSE day opens a new `island_key`, so TRUE days sharing a key are consecutive *in the rows you have*. A missing day is not a FALSE day - for true calendar islands, join a date spine first, flag every calendar day, then group.
"""),
("md", """**Pattern C - gaps between consecutive claims**
- **Business question:** which members went more than 90 days without a claim?
- **Final grain:** one row per gap event -> `member_id, previous_claim_date, claim_date, gap_days`.
- **Source tables:** `silver.claims`.
- **Source grain:** one row per claim - one claim per member per day is not guaranteed, so a self-join would fan out.
- **What breaks it:** `LAG` returns NULL on the first claim - filter it or you report a fake gap.
"""),
("code", """# YOUR TURN: LAG the previous claim date per member and keep gaps above 90 days.
# Expected columns: ['member_id', 'previous_claim_date', 'claim_date', 'gap_days'].
expected_columns = ['member_id', 'previous_claim_date', 'claim_date', 'gap_days']
print('gap output will have:', expected_columns)
"""),
("code", """# SOLUTION
print(q('''
WITH ordered AS (
    SELECT member_id, claim_date,
           lag(claim_date) OVER (PARTITION BY member_id ORDER BY claim_date) AS previous_claim_date
    FROM silver.claims
)
SELECT member_id, previous_claim_date, claim_date,
       date_diff('day', previous_claim_date, claim_date) AS gap_days
FROM ordered
WHERE date_diff('day', previous_claim_date, claim_date) > 90
ORDER BY gap_days DESC, member_id
LIMIT 10
''').to_string(index=False))
print()
print(q('''
WITH ordered AS (
    SELECT member_id, claim_date,
           lag(claim_date) OVER (PARTITION BY member_id ORDER BY claim_date) AS previous_claim_date
    FROM silver.claims
), gaps AS (
    SELECT member_id, date_diff('day', previous_claim_date, claim_date) AS gap_days FROM ordered
)
SELECT COUNT(*) AS gap_events, COUNT(DISTINCT member_id) AS members_affected,
       ROUND(AVG(gap_days), 1) AS avg_gap_days, MAX(gap_days) AS max_gap_days
FROM gaps WHERE gap_days > 90
''').to_string(index=False))
"""),
("md", """**Takeaway:** `LAG` computes the gap without a self-join, and its default frame already handles the ordering - the only trap is the NULL first row.
"""),
("md", """**Variation - duplicates break the streak logic**
- **Business question:** the same streak query on `bronze.claims` gives different runs. Why?
- **Final grain:** one row per provider per day - so `SELECT DISTINCT provider_id, claim_date` first.
- **Source tables:** `bronze.claims` (12,358 rows, 354 claim_ids loaded more than once).
- **Source grain:** one row per loaded record - *not* one row per claim.
- **What breaks it:** two rows on the same day get two different `ROW_NUMBER`s, so the island key changes mid-run and one run becomes two.
"""),
("code", """# YOUR TURN: run Pattern A on bronze.claims with and without SELECT DISTINCT.
# Expected columns: ['provider_id', 'run_start', 'run_end', 'run_days'] for both variants.
expected_columns = ['provider_id', 'run_start', 'run_end', 'run_days']
print('compare the two variants:', expected_columns)
"""),
("code", """# SOLUTION
broken = q('''
WITH islands AS (
    SELECT provider_id, claim_date,
           claim_date - (row_number() OVER (PARTITION BY provider_id ORDER BY claim_date))::INT AS island_key
    FROM bronze.claims
)
SELECT COUNT(*) AS runs, MAX(run_len) AS longest_run FROM (
    SELECT provider_id, island_key, COUNT(*) AS run_len FROM islands GROUP BY 1, 2
)
''')
fixed = q('''
WITH islands AS (
    SELECT provider_id, claim_date,
           claim_date - (row_number() OVER (PARTITION BY provider_id ORDER BY claim_date))::INT AS island_key
    FROM (SELECT DISTINCT provider_id, claim_date FROM bronze.claims)
)
SELECT COUNT(*) AS runs, MAX(run_len) AS longest_run FROM (
    SELECT provider_id, island_key, COUNT(*) AS run_len FROM islands GROUP BY 1, 2
)
''')
print('bronze.claims, duplicates kept :', broken.iloc[0].to_dict())
print('bronze.claims, DISTINCT first  :', fixed.iloc[0].to_dict())
print()
print('one provider, side by side (PRV0015):')
print(q('''
WITH broken AS (
    SELECT provider_id, claim_date,
           claim_date - (row_number() OVER (PARTITION BY provider_id ORDER BY claim_date))::INT AS island_key
    FROM bronze.claims WHERE provider_id = 'PRV0015'
), fixed AS (
    SELECT provider_id, claim_date,
           claim_date - (row_number() OVER (PARTITION BY provider_id ORDER BY claim_date))::INT AS island_key
    FROM (SELECT DISTINCT provider_id, claim_date FROM bronze.claims) WHERE provider_id = 'PRV0015'
)
SELECT 'duplicates kept' AS variant, MIN(claim_date) AS run_start, MAX(claim_date) AS run_end, COUNT(*) AS run_days
FROM broken GROUP BY island_key HAVING COUNT(*) > 2
UNION ALL
SELECT 'DISTINCT first', MIN(claim_date), MAX(claim_date), COUNT(*)
FROM fixed GROUP BY island_key HAVING COUNT(*) > 2
ORDER BY variant, run_start
''').to_string(index=False))
"""),
("md", """**Closing table - question wording -> pattern:**
- "consecutive days in a row" -> **A:** `value - ROW_NUMBER()` marks the run; MIN/MAX/COUNT describes it.
- "longest / best streak" -> **A + ranking:** build runs, then `ROW_NUMBER() OVER (PARTITION BY entity ORDER BY length DESC)` (or `QUALIFY`).
- "days above a threshold, then it flipped back" -> **B:** flag TRUE/FALSE, then `SUM(CASE WHEN NOT flag THEN 1 ELSE 0 END) OVER (ORDER BY day ROWS UNBOUNDED PRECEDING)` is the island key.
- "gap since the previous event" -> **C:** `LAG(event_date)` then `date_diff('day', prev, current)`.
- "missing months / ids / days" -> **C variant:** calendar or dimension spine `LEFT JOIN` + `IS NULL`.
- "my streak query broke" -> **first question:** is the input one row per entity per day? `SELECT DISTINCT` before any window function.
"""),
]
