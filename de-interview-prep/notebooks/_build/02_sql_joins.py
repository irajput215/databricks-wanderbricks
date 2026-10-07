TITLE = "02 - Joins & relationships (Q17-Q25)"
CELLS = [
    ("md", """**Drill 02 - joins and relationships (Q17-Q25).**
Every plan names table A's grain, table B's grain, the join relationship and the result grain.
The dimension tables here are 1 row per key, so a correct join never fans out - `assert_grain` proves it.
Inner joins silently drop the 5 providers and 5 members with no claims; Q23 and Q24 hunt them down with an anti-join."""),

    # ------------------------------------------------------------------ Q17
    ("md", """#### Q17 - Provider name and total claims per provider
**Business question:** which providers handle the most claims?
**Final grain:** one row per provider
**Source tables:** claims (A) + providers (B)
**Source grains:** A = 1 row per claim_id; B = 1 row per provider_id; A -> B many-to-one on provider_id, so no fan-out
**Grain risk:** the inner join keeps only the 215 providers that have claims - the other 5 show up in Q24"""),
    ("code", """# YOUR TURN -> columns: provider_id, provider_name, claim_count
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT p.provider_id, p.provider_name, COUNT(*) AS claim_count
FROM claims AS c
JOIN providers AS p ON p.provider_id = c.provider_id
GROUP BY p.provider_id, p.provider_name
ORDER BY claim_count DESC, p.provider_name
''')
assert_grain(df, 'provider_id', 'provider_name')
df.head(10)"""),

    # ------------------------------------------------------------------ Q18
    ("md", """#### Q18 - Provider name and total paid amount
**Business question:** which providers are paid the most?
**Final grain:** one row per provider
**Source tables:** claims (A) + providers (B)
**Source grains:** A = 1 row per claim_id; B = 1 row per provider_id; A -> B many-to-one on provider_id, so no fan-out
**Grain risk:** grouping by provider_id alone would still be correct, but two providers can share a name - keep both keys"""),
    ("code", """# YOUR TURN -> columns: provider_id, provider_name, total_paid
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT p.provider_id, p.provider_name, ROUND(SUM(c.paid_amount), 2) AS total_paid
FROM claims AS c
JOIN providers AS p ON p.provider_id = c.provider_id
GROUP BY p.provider_id, p.provider_name
ORDER BY total_paid DESC, p.provider_name
''')
assert_grain(df, 'provider_id', 'provider_name')
df.head(10)"""),

    # ------------------------------------------------------------------ Q19
    ("md", """#### Q19 - Total paid amount by provider specialty
**Business question:** which specialties cost the plan the most?
**Final grain:** one row per specialty
**Source tables:** claims (A) + providers (B)
**Source grains:** A = 1 row per claim_id; B = 1 row per provider_id; A -> B many-to-one on provider_id, so no fan-out
**Watch out:** providers.specialty is the *current* specialty - a point-in-time answer needs provider_specialty_history"""),
    ("code", """# YOUR TURN -> columns: specialty, claim_count, total_paid
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT p.specialty, COUNT(*) AS claim_count, ROUND(SUM(c.paid_amount), 2) AS total_paid
FROM claims AS c
JOIN providers AS p ON p.provider_id = c.provider_id
GROUP BY p.specialty
ORDER BY total_paid DESC
''')
assert_grain(df, 'specialty')
df"""),

    # ------------------------------------------------------------------ Q20
    ("md", """#### Q20 - Total claims by member state
**Business question:** where do our claims come from?
**Final grain:** one row per member state
**Source tables:** claims (A) + members (B)
**Source grains:** A = 1 row per claim_id; B = 1 row per member_id; A -> B many-to-one on member_id, so no fan-out
**Grain risk:** state lives on the member, so a claim is credited to the member's current state, not the state at service time"""),
    ("code", """# YOUR TURN -> columns: state, claim_count
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT m.state, COUNT(*) AS claim_count
FROM claims AS c
JOIN members AS m ON m.member_id = c.member_id
GROUP BY m.state
ORDER BY claim_count DESC
''')
assert_grain(df, 'state')
df"""),

    # ------------------------------------------------------------------ Q21
    ("md", """#### Q21 - Total paid amount by member insurance plan
**Business question:** which plans carry the paid cost?
**Final grain:** one row per plan
**Source tables:** claims (A) + members (B) + plans (C)
**Source grains:** A -> B many-to-one on member_id, then B -> C many-to-one on plan_id: two hops, still no fan-out
**Grain risk:** each hop must land on a 1-row-per-key table - a second claim-level join would double-count the money"""),
    ("code", """# YOUR TURN -> columns: plan_name, claim_count, total_paid
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT pl.plan_name, COUNT(*) AS claim_count, ROUND(SUM(c.paid_amount), 2) AS total_paid
FROM claims AS c
JOIN members AS m ON m.member_id = c.member_id
JOIN plans AS pl ON pl.plan_id = m.plan_id
GROUP BY pl.plan_id, pl.plan_name
ORDER BY total_paid DESC
''')
assert_grain(df, 'plan_name')
df"""),

    # ------------------------------------------------------------------ Q22
    ("md", """#### Q22 - Top 10 providers by total paid amount
**Business question:** who are the ten highest-paid providers?
**Final grain:** one row per provider, limited to 10 rows
**Source tables:** claims (A) + providers (B)
**Source grains:** A = 1 row per claim_id; B = 1 row per provider_id; A -> B many-to-one on provider_id, so no fan-out
**Watch out:** aggregate first, then ORDER BY and LIMIT - a LIMIT inside the join would rank claims, not providers"""),
    ("code", """# YOUR TURN -> columns: provider_id, provider_name, specialty, total_paid
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT p.provider_id, p.provider_name, p.specialty, ROUND(SUM(c.paid_amount), 2) AS total_paid
FROM claims AS c
JOIN providers AS p ON p.provider_id = c.provider_id
GROUP BY p.provider_id, p.provider_name, p.specialty
ORDER BY total_paid DESC
LIMIT 10
''')
assert_grain(df, 'provider_id', 'provider_name')
df"""),

    # ------------------------------------------------------------------ Q23
    ("md", """#### Q23 - Members who never submitted a claim
**Business question:** who is enrolled but not using the plan?
**Final grain:** one row per member (5 rows)
**Source tables:** members (A) + claims (B)
**Source grains:** A = 1 row per member_id; B = 1 row per claim_id; A LEFT JOIN B on member_id keeps every member
**Grain risk:** B is the many side, so the LEFT JOIN fans out - the NULL filter throws the matched rows away again"""),
    ("md", """**Why LEFT JOIN:** the members with no claims are exactly the rows where the right table has no match - an inner join returns none of them."""),
    ("code", """# YOUR TURN -> columns: member_id, first_name, last_name, state
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT m.member_id, m.first_name, m.last_name, m.state
FROM members AS m
LEFT JOIN claims AS c ON c.member_id = m.member_id
WHERE c.claim_id IS NULL
ORDER BY m.member_id
''')
assert_grain(df, 'member_id')
df"""),

    # ------------------------------------------------------------------ Q24
    ("md", """#### Q24 - Providers who never received a claim
**Business question:** which providers are on the network but idle?
**Final grain:** one row per provider (5 rows)
**Source tables:** providers (A) + claims (B)
**Source grains:** A = 1 row per provider_id; B = 1 row per claim_id; A LEFT JOIN B on provider_id keeps every provider
**Grain risk:** filtering on the *right* table's key (c.claim_id IS NULL) is what turns the outer join into an anti-join"""),
    ("code", """# YOUR TURN -> columns: provider_id, provider_name, specialty
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT p.provider_id, p.provider_name, p.specialty
FROM providers AS p
LEFT JOIN claims AS c ON c.provider_id = p.provider_id
WHERE c.claim_id IS NULL
ORDER BY p.provider_id
''')
assert_grain(df, 'provider_id')
df"""),

    # ------------------------------------------------------------------ Q25
    ("md", """#### Q25 - Members with more than 5 claims
**Business question:** which members are the high utilisers?
**Final grain:** one row per member (1,079 rows)
**Source tables:** claims
**Source grains:** claims = 1 row per claim_id; no join, so no fan-out
**Grain risk:** none - the threshold filter runs on the group, not on the individual claim rows"""),
    ("md", """**Why HAVING:** WHERE filters rows before grouping, HAVING filters the aggregated groups afterwards."""),
    ("code", """# YOUR TURN -> columns: member_id, claim_count
# write your query here"""),
    ("code", """# SOLUTION
df = q('''
SELECT member_id, COUNT(*) AS claim_count
FROM claims
GROUP BY member_id
HAVING COUNT(*) > 5
ORDER BY claim_count DESC, member_id
''')
assert_grain(df, 'member_id')
df.head(10)"""),
]
