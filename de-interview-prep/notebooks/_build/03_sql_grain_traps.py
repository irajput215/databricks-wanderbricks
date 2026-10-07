TITLE = "03 - Grain traps (Q26-Q33)"
CELLS = [
    ("md", """Every drill below is a grain problem: the answer moves with the number of rows you join.
Wrong answers are shown on purpose and labelled **Trap:** - they are exactly what an
interviewer hopes you will catch. Read the two `**Source grains:**` lines before writing SQL."""),

    # ------------------------------------------------------------------ Q26
    ("md", """#### Q26 - Total claim cost vs total service amount
**Business question:** do the claim header and its service lines tell the same money story?
**Final grain:** one row (company totals)
**Source tables:** claims, claim_services
**Source grains:** claims = 1 row per claim; claim_services = 1 row per service line
**Watch out:** different metrics - billed header vs charged lines - and they *should* reconcile"""),
    ("code", """# YOUR TURN
# output columns: total_billed_claims | total_paid_claims | total_service_amount | billed_minus_services
# write your query here"""),
    ("code", """# SOLUTION
totals = q('''
SELECT
    ROUND((SELECT SUM(billed_amount) FROM claims), 2)           AS total_billed_claims,
    ROUND((SELECT SUM(paid_amount) FROM claims), 2)             AS total_paid_claims,
    ROUND((SELECT SUM(service_amount) FROM claim_services), 2)  AS total_service_amount,
    ROUND((SELECT SUM(billed_amount) FROM claims)
        - (SELECT SUM(service_amount) FROM claim_services), 2)  AS billed_minus_services
''')

# how many claim headers disagree with their own service lines?
mismatched = q1('''
WITH per_claim AS (
    SELECT
        c.claim_id,
        c.billed_amount,
        COALESCE(SUM(s.service_amount), 0) AS line_total
    FROM claims AS c
    LEFT JOIN claim_services AS s
        ON s.claim_id = c.claim_id
    GROUP BY c.claim_id, c.billed_amount
)
SELECT COUNT(*) FILTER (WHERE ROUND(billed_amount, 2) <> ROUND(line_total, 2))
FROM per_claim
''')
print(f"claims whose billed_amount != SUM(service_amount): {mismatched}")
totals"""),
    ("md", """**Should they reconcile?** Yes: `billed_amount` is meant to be the sum of the claim's own
service lines, so the 53,370.59 gap is a data-quality finding, not a definition - it is entirely
explained by the claims counted above whose header disagrees with its lines. `paid_amount`
(36,897,727.38) is a third, smaller metric: what the plan actually paid after adjudication."""),

    # ------------------------------------------------------------------ Q27
    ("md", """#### Q27 - Total paid: the fan-out trap
**Business question:** what did we pay in total across all claims?
**Final grain:** one row (company total)
**Source tables:** claims, claim_services
**Source grains:** claims = 1 row per claim; claim_services = 1 row per service line
**Trap:** the naive join repeats `paid_amount` once per service line - 4.5x too much when weighted by dollars"""),
    ("code", """# YOUR TURN
# 1) trap_total    = SUM(paid_amount) after joining claim_services   (wrong on purpose)
# 2) correct_total = SUM(paid_amount) from claims alone
# output columns: trap_total | correct_total | inflation_factor
# write your query here"""),
    ("code", """# SOLUTION
# WRONG on purpose - this is the answer the join hands you, not the answer to the question
trap = q('''
SELECT
    ROUND(SUM(c.paid_amount), 2) AS trap_total_paid
FROM claims AS c
JOIN claim_services AS s
    ON s.claim_id = c.claim_id
''')

correct = q('''
SELECT
    ROUND(SUM(paid_amount), 2) AS correct_total_paid
FROM claims
''')

trap_total = trap.trap_total_paid[0]
correct_total = correct.correct_total_paid[0]
print(f"trap    claim x service fan-out : {trap_total:>18,.2f}")
print(f"correct one row per claim       : {correct_total:>18,.2f}")
print(f"inflation factor                : {trap_total / correct_total:>18.2f}x")"""),
    ("md", """**Why the number changed:** the join emits one row per (claim, service line), so each claim's
`paid_amount` is counted once per line - the average claim has 3.22 lines, and 4.53 line-weighted
because expensive claims carry more lines. `SUM` never deduplicates."""),

    # ------------------------------------------------------------------ Q28
    ("md", """#### Q28 - Total paid per provider without double counting
**Business question:** how much did each provider actually get paid?
**Final grain:** provider
**Source tables:** claims, providers
**Source grains:** claims = 1 row per claim; providers = 1 row per provider; claim_services = 1 row per service line
**Grain risk:** joining claim_services multiplies claim rows - aggregate services FIRST, then join"""),
    ("code", """# YOUR TURN
# output columns: provider_id | provider_name | specialty | total_paid
# ordered by total_paid DESC
# write your query here"""),
    ("code", """# SOLUTION
by_provider = q('''
WITH paid_per_provider AS (
    SELECT
        provider_id,
        SUM(paid_amount) AS total_paid
    FROM claims
    GROUP BY provider_id
)
SELECT
    pp.provider_id,
    p.provider_name,
    p.specialty,
    ROUND(pp.total_paid, 2) AS total_paid
FROM paid_per_provider AS pp
JOIN providers AS p
    ON p.provider_id = pp.provider_id
ORDER BY total_paid DESC
''')

assert_grain(by_provider, "provider_id")
by_provider.head(10)"""),
    ("md", """**Why one row per provider:** `claims` is already one row per claim, so the CTE's
`SUM(paid_amount)` is a true provider total; the later join to `providers` is many-to-one and
cannot change it. `SUM` ignores the 1,160 NULL `paid_amount` rows (not adjudicated) and keeps the
10 negative ones - they are reversals, not errors."""),

    # ------------------------------------------------------------------ Q29
    ("md", """#### Q29 - Claims per provider: COUNT(*) vs COUNT(DISTINCT)
**Business question:** how many claims did each provider submit?
**Final grain:** provider
**Source tables:** claims, claim_services, providers
**Source grains:** claims = 1 row per claim; claim_services = 1 row per service line
**Trap:** `COUNT(*)` after the join counts service lines - 38,592 company-wide instead of 11,981 claims"""),
    ("code", """# YOUR TURN
# output columns: provider_id | provider_name | wrong_count_star | right_count_distinct | difference
# ordered by difference DESC
# write your query here"""),
    ("code", """# SOLUTION
claim_counts = q('''
WITH counts AS (
    SELECT
        c.provider_id,
        COUNT(*)                   AS wrong_count_star,
        COUNT(DISTINCT c.claim_id) AS right_count_distinct
    FROM claims AS c
    JOIN claim_services AS s
        ON s.claim_id = c.claim_id
    GROUP BY c.provider_id
)
SELECT
    co.provider_id,
    p.provider_name,
    co.wrong_count_star,
    co.right_count_distinct,
    co.wrong_count_star - co.right_count_distinct AS difference
FROM counts AS co
JOIN providers AS p
    ON p.provider_id = co.provider_id
ORDER BY difference DESC, co.provider_id
''')

assert_grain(claim_counts, "provider_id")
print(f"company totals: COUNT(*) = {claim_counts.wrong_count_star.sum():,} "
      f"vs COUNT(DISTINCT claim_id) = {claim_counts.right_count_distinct.sum():,}")
claim_counts.head(10)"""),
    ("md", """**Why one row per provider:** `COUNT(DISTINCT claim_id)` counts the distinct claims that
survive the fan-out, which is the claim grain the question asks for. The five providers with no
claims vanish in an INNER JOIN - start from `providers LEFT JOIN claims` if you need them."""),

    # ------------------------------------------------------------------ Q30
    ("md", """#### Q30 - Average number of services per claim
**Business question:** how many service lines does a typical claim have?
**Final grain:** one row (company average)
**Source tables:** claims, claim_services
**Source grains:** claims = 1 row per claim; claim_services = 1 row per service line
**Watch out:** the average is per claim, so collapse to one row per claim *before* averaging"""),
    ("code", """# YOUR TURN
# step 1: count service lines per claim
# step 2: average that count
# output columns: claims | service_lines | avg_services_per_claim | max_services_per_claim
# write your query here"""),
    ("code", """# SOLUTION
per_claim = q('''
SELECT
    c.claim_id,
    COUNT(s.service_id) AS service_lines
FROM claims AS c
LEFT JOIN claim_services AS s
    ON s.claim_id = c.claim_id
GROUP BY c.claim_id
''')

assert_grain(per_claim, "claim_id")
print(f"claims                 : {len(per_claim):,}")
print(f"service lines          : {per_claim.service_lines.sum():,}")
print(f"avg services per claim : {per_claim.service_lines.mean():.4f}")
print(f"max services per claim : {per_claim.service_lines.max()}")"""),
    ("md", """**Why one row per claim:** with a count per claim you can average, take percentiles or bucket
them - `AVG()` straight over the joined rows only matches (3.2211) by luck, and would silently
ignore any claim with zero service lines. Here all 11,981 claims have at least one line."""),

    # ------------------------------------------------------------------ Q31
    ("md", """#### Q31 - Provider with the highest average services per claim
**Business question:** which provider bills the most service lines per claim?
**Final grain:** provider (ranked list, winner first)
**Source tables:** claims, claim_services, providers
**Source grains:** claims = 1 row per claim; claim_services = 1 row per service line
**Watch out:** require `claims >= 20` - a low-volume provider wins on noise, not on billing behaviour"""),
    ("code", """# YOUR TURN
# output columns: provider_id | provider_name | claims | avg_services_per_claim
# only providers with at least 20 claims, ordered by avg_services_per_claim DESC
# write your query here"""),
    ("code", """# SOLUTION
MIN_CLAIMS = 20

QUERY = '''
WITH services_per_claim AS (
    SELECT
        c.provider_id,
        c.claim_id,
        COUNT(s.service_id) AS service_lines
    FROM claims AS c
    LEFT JOIN claim_services AS s
        ON s.claim_id = c.claim_id
    GROUP BY c.provider_id, c.claim_id
),
per_provider AS (
    SELECT
        provider_id,
        COUNT(*)           AS claims,
        AVG(service_lines) AS avg_services_per_claim
    FROM services_per_claim
    GROUP BY provider_id
)
SELECT
    pp.provider_id,
    p.provider_name,
    pp.claims,
    ROUND(pp.avg_services_per_claim, 3) AS avg_services_per_claim
FROM per_provider AS pp
JOIN providers AS p
    ON p.provider_id = pp.provider_id
WHERE pp.claims >= {min_claims}
ORDER BY avg_services_per_claim DESC, pp.claims DESC
LIMIT 10
'''

best = q(QUERY.format(min_claims=MIN_CLAIMS))
assert_grain(best, "provider_id")

for threshold in (20, 50, 60):
    row = q(QUERY.format(min_claims=threshold)).iloc[0]
    print(f"min_claims={threshold:>2} -> {row.provider_id} {row.provider_name:<22}"
          f" {row.claims:>3} claims, {row.avg_services_per_claim} services/claim")
best"""),
    ("md", """**Why the minimum matters:** the winner is threshold-sensitive - `>= 20` picks PRV0051 (47
claims), `>= 50` picks PRV0129 and `>= 60` picks PRV0074 - because a 4.0 average over a few dozen
claims is mostly noise. A provider with 2 claims of 8 lines each would top any unfiltered list, so
always publish the claim count beside the average."""),

    # ------------------------------------------------------------------ Q32
    ("md", """#### Q32 - Claims with more than 5 service lines
**Business question:** which claims carry an unusually large number of service lines?
**Final grain:** claim
**Source tables:** claims, claim_services, providers
**Source grains:** claims = 1 row per claim; claim_services = 1 row per service line
**Watch out:** `WHERE` filters rows *before* grouping - a count condition belongs in `HAVING`"""),
    ("code", """# YOUR TURN
# output columns: claim_id | provider_name | claim_date | status | service_lines | billed_amount | paid_amount
# only claims with more than 5 service lines
# write your query here"""),
    ("code", """# SOLUTION
many_lines = q('''
SELECT
    c.claim_id,
    p.provider_name,
    c.claim_date,
    c.status,
    COUNT(s.service_id)       AS service_lines,
    ROUND(c.billed_amount, 2) AS billed_amount,
    ROUND(c.paid_amount, 2)   AS paid_amount
FROM claims AS c
JOIN claim_services AS s
    ON s.claim_id = c.claim_id
JOIN providers AS p
    ON p.provider_id = c.provider_id
GROUP BY ALL
HAVING COUNT(s.service_id) > 5
ORDER BY service_lines DESC, c.claim_id
''')

assert_grain(many_lines, "claim_id")
share = len(many_lines) / q1('SELECT COUNT(*) FROM claims')
print(f"{len(many_lines):,} claims have more than 5 service lines ({share:.2%} of all claims)")
many_lines.head(10)"""),
    ("md", """**Why one row per claim:** the join fans out to 38,592 service rows, then `GROUP BY` collapses
them back to 11,981 claims and `HAVING` keeps the 1,838 big ones. `GROUP BY ALL` is DuckDB
shorthand for grouping by every non-aggregated column."""),

    # ------------------------------------------------------------------ Q33
    ("md", """#### Q33 - Total service amount per provider (charged, not paid)
**Business question:** how much did each provider bill in service lines?
**Final grain:** provider
**Source tables:** claim_services, claims, providers
**Source grains:** claim_services = 1 row per service line; claims = 1 row per claim
**Watch out:** never mix `claims.paid_amount` (paid, per claim) and `claim_services.service_amount` (charged, per line) in one SUM"""),
    ("code", """# YOUR TURN
# output columns: provider_id | provider_name | specialty | total_service_amount
# ordered by total_service_amount DESC
# write your query here"""),
    ("code", """# SOLUTION
charged = q('''
SELECT
    p.provider_id,
    p.provider_name,
    p.specialty,
    ROUND(SUM(s.service_amount), 2) AS total_service_amount
FROM claim_services AS s
JOIN claims AS c
    ON c.claim_id = s.claim_id
JOIN providers AS p
    ON p.provider_id = c.provider_id
GROUP BY p.provider_id, p.provider_name, p.specialty
ORDER BY total_service_amount DESC
''')

assert_grain(charged, "provider_id")
charged.head(10)"""),
    ("md", """**Why one row per provider:** service lines are the grain being summed, so every line
contributes exactly once. Use `claims.paid_amount` for "what did we pay this provider" and
`claim_services.service_amount` for "what did they charge, and for which services": 82.7M charged
vs 36.9M paid, a 2.2x gap from denials, network discounts and 8 negative reversal lines."""),
]
