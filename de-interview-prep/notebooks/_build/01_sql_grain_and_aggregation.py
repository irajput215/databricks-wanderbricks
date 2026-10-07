TITLE = "01 - Grain & aggregation (Q1-Q16)"
CELLS = [
    ("md", """**Drill 01 - grain and aggregation (Q1-Q16).**
Every question states its result grain *before* the SQL, then proves it with `assert_grain`.
Each question has a `YOUR TURN` stub (the expected columns are named - write your query) and a `SOLUTION` cell.
`OK one row per [...]` means the grain held; `FAIL ... duplicate rows` means the result is finer than you claimed."""),

    # ------------------------------------------------------------------ Q1
    ("md", """#### Q1 - Total number of claims
**Business question:** how many claims are in the warehouse?
**Final grain:** single row
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id, so the result is the number of claims submitted
**Grain risk:** none - one table, no join, no fan-out
**Watch out:** this counts claim rows, not people or service lines - 11,981 claims come from 1,195 members"""),
    ("code", """# YOUR TURN -> columns: claim_count
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT COUNT(*) AS claim_count
FROM claims
''')
df"""),

    # ------------------------------------------------------------------ Q2
    ("md", """#### Q2 - Total paid amount
**Business question:** how much has the plan paid out in total?
**Final grain:** single row
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id
**Grain risk:** none - no join, no fan-out
**Watch out:** SUM skips NULL paid_amount (1,160 claims are not adjudicated yet) - that is correct here, NULL is not zero"""),
    ("code", """# YOUR TURN -> columns: total_paid
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT ROUND(SUM(paid_amount), 2) AS total_paid
FROM claims
''')
df"""),

    # ------------------------------------------------------------------ Q3
    ("md", """#### Q3 - Total claim count per member
**Business question:** how many claims does each member have?
**Final grain:** one row per member
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id
**Grain risk:** none - member_id is already on claims, so no join is needed"""),
    ("md", """**Why one row per member:** the GROUP BY key *is* the result grain - 1,195 members have claims, so 1,195 rows."""),
    ("code", """# YOUR TURN -> columns: member_id, claim_count
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT member_id, COUNT(*) AS claim_count
FROM claims
GROUP BY member_id
ORDER BY claim_count DESC, member_id
''')
assert_grain(df, 'member_id')
df.head(10)"""),

    # ------------------------------------------------------------------ Q4
    ("md", """#### Q4 - Total paid amount per member
**Business question:** how much has each member cost the plan?
**Final grain:** one row per member
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id
**Grain risk:** none - group the claims, no join
**Watch out:** a member whose claims are all unadjudicated gets NULL, not 0 - use COALESCE if the report needs a number"""),
    ("code", """# YOUR TURN -> columns: member_id, total_paid
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT member_id, ROUND(SUM(paid_amount), 2) AS total_paid
FROM claims
GROUP BY member_id
ORDER BY total_paid DESC NULLS LAST, member_id
''')
assert_grain(df, 'member_id')
df.head(10)"""),

    # ------------------------------------------------------------------ Q5
    ("md", """#### Q5 - Number of unique members who submitted claims
**Business question:** how many members actually use their plan?
**Final grain:** single row
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id, many claims per member
**Grain risk:** none - COUNT(DISTINCT member_id) collapses the many-claims-per-member relationship
**Watch out:** 1,195 of 1,200 members have claims; the missing 5 need a LEFT JOIN (Q23)"""),
    ("code", """# YOUR TURN -> columns: members_with_claims
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT COUNT(DISTINCT member_id) AS members_with_claims
FROM claims
''')
df"""),

    # ------------------------------------------------------------------ Q6
    ("md", """#### Q6 - Average claim amount
**Business question:** what does a typical claim cost?
**Final grain:** single row
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id
**Grain risk:** none - no join, no fan-out
**Watch out:** AVG ignores NULL, so the denominator is 10,821 adjudicated claims, not all 11,981"""),
    ("code", """# YOUR TURN -> columns: avg_paid_per_claim
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT ROUND(AVG(paid_amount), 2) AS avg_paid_per_claim
FROM claims
''')
df"""),

    # ------------------------------------------------------------------ Q7
    ("md", """#### Q7 - Maximum and minimum claim amount
**Business question:** what is the spread of claim cost?
**Final grain:** single row
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id
**Grain risk:** none - no join, no fan-out
**Watch out:** the raw MIN is negative (10 reversal claims); FILTER isolates the non-negative floor"""),
    ("code", """# YOUR TURN -> columns: max_paid, min_paid, min_non_negative_paid
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT
    ROUND(MAX(paid_amount), 2) AS max_paid,
    ROUND(MIN(paid_amount), 2) AS min_paid,
    ROUND(MIN(paid_amount) FILTER (WHERE paid_amount >= 0), 2) AS min_non_negative_paid
FROM claims
''')
df"""),

    # ------------------------------------------------------------------ Q8
    ("md", """#### Q8 - Total claims by status
**Business question:** how are claims split across Submitted / Approved / Denied?
**Final grain:** one row per status
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id
**Grain risk:** none - status is a column on claims, no join needed"""),
    ("code", """# YOUR TURN -> columns: status, claim_count
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT status, COUNT(*) AS claim_count
FROM claims
GROUP BY status
ORDER BY claim_count DESC
''')
assert_grain(df, 'status')
df"""),

    # ------------------------------------------------------------------ Q9
    ("md", """#### Q9 - Monthly claim count
**Business question:** how does claim volume trend month over month?
**Final grain:** one row per month
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id
**Grain risk:** none - many claims collapse into one month, no join"""),
    ("md", """**Why one row per month:** grouping by the truncated date folds every claim in that month into one row - 18 months, not 11,981 claims."""),
    ("code", """# YOUR TURN -> columns: claim_month, claim_count
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT date_trunc('month', claim_date) AS claim_month, COUNT(*) AS claim_count
FROM claims
GROUP BY claim_month
ORDER BY claim_month
''')
assert_grain(df, 'claim_month')
df"""),

    # ------------------------------------------------------------------ Q10
    ("md", """#### Q10 - Monthly total paid amount
**Business question:** how much does the plan pay out each month?
**Final grain:** one row per month
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id
**Grain risk:** none - SUM inside the month, no join
**Watch out:** paid_amount is booked on claim_date here; finance reports on payment_date from payments"""),
    ("code", """# YOUR TURN -> columns: claim_month, total_paid
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT date_trunc('month', claim_date) AS claim_month, ROUND(SUM(paid_amount), 2) AS total_paid
FROM claims
GROUP BY claim_month
ORDER BY claim_month
''')
assert_grain(df, 'claim_month')
df"""),

    # ------------------------------------------------------------------ Q11
    ("md", """#### Q11 - Monthly claim count per member
**Business question:** how many claims does each member submit each month?
**Final grain:** one row per member per month
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id
**Grain risk:** none - two grouping keys instead of one, still no join"""),
    ("md", """**Why member + month:** two grouping keys mean one row per member per month, only for months with activity - 8,946 rows, not 1,195 x 18."""),
    ("code", """# YOUR TURN -> columns: member_id, claim_month, claim_count
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT member_id, date_trunc('month', claim_date) AS claim_month, COUNT(*) AS claim_count
FROM claims
GROUP BY member_id, claim_month
ORDER BY member_id, claim_month
''')
assert_grain(df, 'member_id', 'claim_month')
df.head(10)"""),

    # ------------------------------------------------------------------ Q12
    ("md", """#### Q12 - Monthly paid amount per member
**Business question:** what does each member cost the plan per month?
**Final grain:** one row per member per month
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id
**Grain risk:** none - add money to the Q11 grouping, no join"""),
    ("code", """# YOUR TURN -> columns: member_id, claim_month, total_paid
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT member_id, date_trunc('month', claim_date) AS claim_month, ROUND(SUM(paid_amount), 2) AS total_paid
FROM claims
GROUP BY member_id, claim_month
ORDER BY member_id, claim_month
''')
assert_grain(df, 'member_id', 'claim_month')
df.head(10)"""),

    # ------------------------------------------------------------------ Q13
    ("md", """#### Q13 - Monthly claim count per provider
**Business question:** how many claims does each provider submit each month?
**Final grain:** one row per provider per month
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id
**Grain risk:** none - provider_id is already on claims, no join to providers needed"""),
    ("code", """# YOUR TURN -> columns: provider_id, claim_month, claim_count
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT provider_id, date_trunc('month', claim_date) AS claim_month, COUNT(*) AS claim_count
FROM claims
GROUP BY provider_id, claim_month
ORDER BY provider_id, claim_month
''')
assert_grain(df, 'provider_id', 'claim_month')
df.head(10)"""),

    # ------------------------------------------------------------------ Q14
    ("md", """#### Q14 - Monthly paid amount per provider
**Business question:** how much does each provider get paid each month?
**Final grain:** one row per provider per month
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id
**Grain risk:** none - same grouping as Q13, money instead of a count"""),
    ("code", """# YOUR TURN -> columns: provider_id, claim_month, total_paid
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT provider_id, date_trunc('month', claim_date) AS claim_month, ROUND(SUM(paid_amount), 2) AS total_paid
FROM claims
GROUP BY provider_id, claim_month
ORDER BY provider_id, claim_month
''')
assert_grain(df, 'provider_id', 'claim_month')
df.head(10)"""),

    # ------------------------------------------------------------------ Q15
    ("md", """#### Q15 - Claim count by member state and month
**Business question:** which states generate claims, and when?
**Final grain:** one row per state per month
**Source tables:** claims (A) + members (B)
**Source grains:** A = 1 row per claim_id; B = 1 row per member_id; A -> B many-to-one on member_id, so no fan-out
**Grain risk:** an inner join drops members with no claims - fine here, we are counting claims"""),
    ("md", """**Why state + month:** state lives on members, so the join is required before grouping - 360 rows for 20 states x 18 months."""),
    ("code", """# YOUR TURN -> columns: state, claim_month, claim_count
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT m.state, date_trunc('month', c.claim_date) AS claim_month, COUNT(*) AS claim_count
FROM claims AS c
JOIN members AS m ON m.member_id = c.member_id
GROUP BY m.state, claim_month
ORDER BY m.state, claim_month
''')
assert_grain(df, 'state', 'claim_month')
df.head(10)"""),

    # ------------------------------------------------------------------ Q16
    ("md", """#### Q16 - Total paid amount by provider and month
**Business question:** how much did each provider get paid each month?
**Final grain:** one row per provider per month
**Source tables:** claims (A) + providers (B)
**Source grains:** A = 1 row per claim_id; B = 1 row per provider_id; A -> B many-to-one on provider_id, so no fan-out
**Grain risk:** the join adds the name only - group by provider_id *and* provider_name, 56 names are shared by more than one provider"""),
    ("code", """# YOUR TURN -> columns: provider_id, provider_name, claim_month, total_paid
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT p.provider_id, p.provider_name, date_trunc('month', c.claim_date) AS claim_month,
       ROUND(SUM(c.paid_amount), 2) AS total_paid
FROM claims AS c
JOIN providers AS p ON p.provider_id = c.provider_id
GROUP BY p.provider_id, p.provider_name, claim_month
ORDER BY p.provider_name, claim_month
''')
assert_grain(df, 'provider_id', 'provider_name', 'claim_month')
df.head(10)"""),
]
