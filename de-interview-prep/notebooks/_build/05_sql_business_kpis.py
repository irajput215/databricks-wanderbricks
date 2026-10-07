TITLE = "05 - Business KPIs (Q41-Q45)"
CELLS = [
    # ------------------------------------------------------------------ Q41
    ("md", """#### Q41 - Monthly claim approval rate
**Business question:** what share of claims is approved each month?
**Final grain:** month
**Source tables:** claims
**Source grains:** claims = 1 row per claim
**Grain risk:** none - one aggregation over claims, no joins"""),

    ("code", """# YOUR TURN
# output columns: month, total_claims, approved_claims, approval_rate
# write your query here"""),

    ("code", """# SOLUTION
df = q('''
SELECT
    date_trunc('month', claim_date) AS month,
    COUNT(*) AS total_claims,
    COUNT(*) FILTER (WHERE status = 'Approved') AS approved_claims,
    ROUND(COUNT(*) FILTER (WHERE status = 'Approved') * 100.0
          / NULLIF(COUNT(*), 0), 2) AS approval_rate
FROM claims
GROUP BY 1
ORDER BY 1
''')
assert_grain(df, 'month')
df"""),

    # ------------------------------------------------------------------ Q42
    ("md", """#### Q42 - Monthly denial rate
**Business question:** what share of claims is denied each month?
**Final grain:** month
**Source tables:** claims
**Source grains:** claims = 1 row per claim
**Grain risk:** none - the denominator is the same COUNT(*) as Q41, so the rates compare directly"""),

    ("code", """# YOUR TURN
# output columns: month, total_claims, denied_claims, denial_rate
# write your query here"""),

    ("code", """# SOLUTION
df = q('''
SELECT
    date_trunc('month', claim_date) AS month,
    COUNT(*) AS total_claims,
    COUNT(*) FILTER (WHERE status = 'Denied') AS denied_claims,
    ROUND(COUNT(*) FILTER (WHERE status = 'Denied') * 100.0
          / NULLIF(COUNT(*), 0), 2) AS denial_rate
FROM claims
GROUP BY 1
ORDER BY 1
''')
assert_grain(df, 'month')
df"""),

    # ------------------------------------------------------------------ Q43
    ("md", """#### Q43 - Monthly average claim value
**Business question:** what is the average value of a claim each month?
**Final grain:** month
**Source tables:** claims
**Source grains:** claims = 1 row per claim
**Grain risk:** none - but the metric splits in two once paid_amount is NULL.
**Watch out:** paid_amount is NULL for unpaid claims, so AVG() and SUM()/COUNT(*) disagree."""),

    ("code", """# YOUR TURN
# output columns: month, number_of_claims, claims_with_paid_amount, total_paid,
#                 average_claim_value, avg_paid_amount_per_adjudicated_claim, unpaid_claims
# write your query here"""),

    ("code", """# SOLUTION
df = q('''
SELECT
    date_trunc('month', claim_date) AS month,
    COUNT(*) AS number_of_claims,
    COUNT(paid_amount) AS claims_with_paid_amount,
    SUM(paid_amount) AS total_paid,
    ROUND(SUM(paid_amount) / NULLIF(COUNT(*), 0), 2) AS average_claim_value,
    ROUND(AVG(paid_amount), 2) AS avg_paid_amount_per_adjudicated_claim,
    COUNT(*) - COUNT(paid_amount) AS unpaid_claims
FROM claims
GROUP BY 1
ORDER BY 1
''')
assert_grain(df, 'month')
df"""),

    ("md", """**Why one row per month, two different averages:** `AVG(paid_amount)` skips NULLs, so it averages only adjudicated claims and always reads higher; `SUM(paid_amount) / COUNT(*)` charges every unpaid `Submitted` claim as zero and is the honest cost-per-claim-submitted. The gap is the pending backlog - `unpaid_claims` shows it is ~8-10% of each month, so quote the denominator when you report the number."""),

    # ------------------------------------------------------------------ Q44
    ("md", """#### Q44 - Provider performance
**Business question:** which providers perform well on cost and approval?
**Final grain:** provider
**Source tables:** claims, providers
**Source grains:** claims = 1 row per claim; providers = 1 row per provider
**Grain risk:** none if you aggregate claims before joining the dimension"""),

    ("code", """# YOUR TURN
# output columns: provider_id, provider_name, total_claims, approved_claims,
#                 denied_claims, approval_rate, total_paid, average_claim_amount
# write your query here"""),

    ("code", """# SOLUTION
df = q('''
WITH claim_agg AS (
    SELECT
        provider_id,
        COUNT(*) AS total_claims,
        COUNT(*) FILTER (WHERE status = 'Approved') AS approved_claims,
        COUNT(*) FILTER (WHERE status = 'Denied') AS denied_claims,
        ROUND(COUNT(*) FILTER (WHERE status = 'Approved') * 100.0
              / NULLIF(COUNT(*), 0), 2) AS approval_rate,
        SUM(paid_amount) AS total_paid,
        ROUND(SUM(paid_amount) / NULLIF(COUNT(*), 0), 2) AS average_claim_amount
    FROM claims
    GROUP BY 1
)
SELECT
    a.provider_id,
    p.provider_name,
    a.total_claims,
    a.approved_claims,
    a.denied_claims,
    a.approval_rate,
    a.total_paid,
    a.average_claim_amount
FROM claim_agg a
JOIN providers p USING (provider_id)
ORDER BY a.total_paid DESC
''')
assert_grain(df, 'provider_id')
df"""),

    ("md", """**Why one row per provider:** the aggregation already collapsed claims to one row per `provider_id`, so the dimension join is a 1:1 lookup that cannot change the grain; `assert_grain` proves it."""),

    # ------------------------------------------------------------------ Q45
    ("md", """#### Q45 - Provider monthly performance
**Business question:** how does each provider trend month over month?
**Final grain:** provider + month
**Source tables:** claims
**Source grains:** claims = 1 row per claim
**Grain risk:** none - group by provider and month in the same pass
**Watch out:** only 220 providers exist, so thin months produce noisy 100%/0% rates."""),

    ("code", """# YOUR TURN
# output columns: provider_id, month, claim_count, approval_rate, denial_rate,
#                 total_paid, average_claim_amount
# grain: provider_id + month
# write your query here"""),

    ("code", """# SOLUTION
df = q('''
SELECT
    provider_id,
    date_trunc('month', claim_date) AS month,
    COUNT(*) AS claim_count,
    ROUND(COUNT(*) FILTER (WHERE status = 'Approved') * 100.0
          / NULLIF(COUNT(*), 0), 2) AS approval_rate,
    ROUND(COUNT(*) FILTER (WHERE status = 'Denied') * 100.0
          / NULLIF(COUNT(*), 0), 2) AS denial_rate,
    SUM(paid_amount) AS total_paid,
    ROUND(SUM(paid_amount) / NULLIF(COUNT(*), 0), 2) AS average_claim_amount
FROM claims
GROUP BY 1, 2
ORDER BY 1, 2
''')
assert_grain(df, 'provider_id', 'month')
df"""),

    ("md", """**Read it like an analyst:** if one provider's denial rate jumps while its `claim_count` is flat, that is a coding/eligibility signal, not a volume signal - and a rate on 2 claims is noise, not a trend.
**Next query:** that provider's denied procedures for the month against its own trailing 3-month baseline."""),
]
