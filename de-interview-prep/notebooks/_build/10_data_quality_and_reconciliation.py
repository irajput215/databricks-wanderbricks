TITLE = "10 - Data quality & reconciliation"
CELLS = [
("md", """**What this notebook does:**
- a reusable `run_check()` framework (PASS / WARN / FAIL) and 8 checks of your own against `raw.*`, `bronze.*`, `silver.*`
- compare your scorecard with the reference one in `dq.dq_results` / `dq.dq_metrics`
- quarantine, null and duplicate profiling
- **the investigation:** why the dashboard says +40% MoM while the source says +5%
- reconciliation queries in the style interviewers ask for
"""),
("md", """**Drill 1 - a reusable check framework**
- **Business question:** does the published layer still satisfy its contracts?
- **Rule:** 0 violations -> PASS, <= threshold -> WARN, > threshold -> FAIL (same rule as the reference scorecard).
- **Families covered:** PK uniqueness, FK integrity, null %, domain, freshness, duplicate ids.
- **Grain:** one row per check; columns `check_name, layer, actual, threshold, status, note`.
- **What breaks it:** checks that only count rows - always assert a grain or a domain, not just a total.
"""),
("code", """# YOUR TURN: write run_check(name, sql, threshold, layer, note) returning PASS/WARN/FAIL.
# Expected scorecard columns: ['check_name', 'layer', 'actual', 'threshold', 'status', 'note'].
expected_columns = ['check_name', 'layer', 'actual', 'threshold', 'status', 'note']
print('scorecard will have:', expected_columns)
"""),
("code", """# SOLUTION
import pandas as pd

RESULTS = []


def run_check(name, sql, threshold, layer='silver', note=''):
    '''0 violations -> PASS, <= threshold -> WARN, above -> FAIL.'''
    actual = float(q1(sql) or 0)
    status = 'PASS' if actual == 0 else ('WARN' if actual <= threshold else 'FAIL')
    RESULTS.append(dict(check_name=name, layer=layer, actual=actual, threshold=threshold, status=status, note=note))
    return status


# -- grain / key -----------------------------------------------------------------
run_check('claims.claim_id_unique',
          "SELECT COUNT(*) FROM (SELECT claim_id FROM silver.claims GROUP BY 1 HAVING COUNT(*) > 1)",
          0, 'silver', 'one row per claim in the published table')
# -- referential integrity -------------------------------------------------------
run_check('claims.member_fk_valid',
          "SELECT COUNT(*) FROM silver.claims c LEFT JOIN silver.members m USING (member_id) WHERE m.member_id IS NULL",
          0, 'silver', 'every claim resolves to a member')
run_check('claims.provider_fk_valid',
          "SELECT COUNT(*) FROM silver.claims c LEFT JOIN silver.providers p USING (provider_id) WHERE p.provider_id IS NULL",
          0, 'silver', 'every claim resolves to a provider')
run_check('claim_services.claim_fk_valid',
          "SELECT COUNT(*) FROM silver.claim_services s LEFT JOIN silver.claims c USING (claim_id) WHERE c.claim_id IS NULL",
          0, 'silver', 'orphan service lines must never publish')
# -- null % / domain -------------------------------------------------------------
run_check('claims.billed_amount_null_pct',
          "SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE billed_amount IS NULL) / COUNT(*), 2) FROM silver.claims",
          1, 'silver', 'null percentage on a critical amount column (1% tolerated)')
run_check('raw.claims_status_in_domain',
          "SELECT COUNT(*) FROM raw.claims_api_records WHERE status NOT IN ('Submitted', 'Approved', 'Denied')",
          0, 'raw', '41 as-landed statuses outside the domain: 40 mixed-case + 1 N/A')
# -- idempotency + freshness -----------------------------------------------------
run_check('bronze.claims_duplicate_claim_ids',
          "SELECT COUNT(*) FROM (SELECT claim_id FROM bronze.claims GROUP BY 1 HAVING COUNT(*) > 1)",
          0, 'bronze', 'idempotency gap left in bronze')
run_check('freshness.bronze_claims_newest_load',
          "SELECT CASE WHEN date_diff('hour', MAX(_loaded_at), now()) > 24 THEN 1 ELSE 0 END FROM bronze.claims",
          0, 'bronze', 'the newest load must be less than 24 hours old')

scorecard = pd.DataFrame(RESULTS)
print(scorecard.to_string(index=False, max_colwidth=42))
print()
print('mine:', scorecard['status'].value_counts().to_dict())
print()
print('age of bronze rows by batch (a replay keeps its original load timestamp):')
print(q('''
SELECT _ingest_batch, COUNT(*) AS rows_, MIN(_loaded_at) AS loaded_at,
       date_diff('hour', MIN(_loaded_at), now()) AS age_hours
FROM bronze.claims GROUP BY 1 ORDER BY loaded_at DESC
''').to_string(index=False))
"""),
("md", """**Drill 2 - compare with the reference scorecard**
- **Business question:** does my framework agree with the pipeline's own DQ run?
- **Output:** counts per status from `dq.dq_results`, then every non-PASS row.
"""),
("code", """print(q('''
SELECT status, COUNT(*) AS checks FROM dq.dq_results GROUP BY 1 ORDER BY 1
''').to_string(index=False))
print()
print(q('''
SELECT check_name, layer, severity, actual, threshold, status
FROM dq.dq_results WHERE status <> 'PASS' ORDER BY status, actual DESC
''').to_string(index=False, max_colwidth=40))
print()
print(q('SELECT metric, value FROM dq.dq_metrics ORDER BY metric').to_string(index=False, max_colwidth=46))
"""),
("md", """- **The FAIL** `dashboard.reconciles_to_source` (99.73% off, threshold 1%): the published dashboard does not agree with `silver.claims` - drill 5 explains exactly why.
- **The 7 WARNs** are all explainable business noise: reversals (10 negative `paid_amount`), paid>billed (12), billed != sum(service lines) (23), claims before enrollment (8), approved-not-yet-paid (21), pending-with-payment (7), and 486 paid claims that do not tie to the payment register.
"""),
("md", """**Drill 3 - quarantine analysis**
- **Business question:** what is being rejected, and is the reject rate stable?
- **Source tables:** `dq.quarantine` (236 records; one row per rejected record, not per claim).
- **Output columns:** `source`, `reason`, `records` (+ share of the quarantine).
- **What breaks it:** a silent drop - if a record is rejected and not counted, nobody notices until the numbers are wrong.
"""),
("code", """# YOUR TURN: group dq.quarantine by source and reason, biggest first.
# Expected columns: ['source', 'reason', 'records', 'pct_of_quarantine'].
expected_columns = ['source', 'reason', 'records', 'pct_of_quarantine']
print('quarantine report will have:', expected_columns)
"""),
("code", """# SOLUTION
print(q('''
SELECT source, reason, COUNT(*) AS records,
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM dq.quarantine), 1) AS pct_of_quarantine
FROM dq.quarantine GROUP BY 1, 2 ORDER BY records DESC
''').to_string(index=False, max_colwidth=46))
print()
print(q('''
SELECT source, COUNT(*) AS records, MIN(quarantined_at) AS first_seen, MAX(quarantined_at) AS last_seen
FROM dq.quarantine GROUP BY 1 ORDER BY records DESC
''').to_string(index=False))
"""),
("md", """**Reading it:** 111 payment rows (orphan `claim_id` or an unparseable amount) and 105 orphan service lines are source-system defects; 15 claims failed member/provider FK resolution; 5 malformed API records are the ones notebook 09 dead-letters. Nothing here is a pipeline bug - but the count trend is the metric to alert on.
"""),
("md", """**Drill 4 - null and duplicate profiling**
- **Business question:** which key columns can be trusted, and where is the grain broken?
- **Method:** generate the SQL per table from `(table, key_columns, value_column)` instead of hand-writing four queries.
- **Output:** `table, key_columns, rows_total, null_pct_keys, null_pct_value, duplicate_pct`.
- **What breaks it:** profiling the silver layer only - the duplicates live in `bronze.claims`.
"""),
("code", """# SOLUTION
PROFILE = [
    ('silver.claims', ['claim_id'], 'paid_amount'),
    ('silver.claim_services', ['claim_id', 'service_id'], 'service_amount'),
    ('silver.payments', ['payment_id'], 'payment_amount'),
    ('silver.members', ['member_id'], 'plan_id'),
]

profiles = []
for table, keys, value_column in PROFILE:
    total = q1('SELECT COUNT(*) FROM ' + table)
    distinct_keys = q1('SELECT COUNT(*) FROM (SELECT DISTINCT {} FROM {})'.format(', '.join(keys), table))
    any_key_null = ' OR '.join('{} IS NULL'.format(key) for key in keys)
    row = q('''
        SELECT COUNT(*) AS rows_total,
               ROUND(100.0 * COUNT(*) FILTER (WHERE {any_key_null}) / COUNT(*), 2) AS null_pct_keys,
               ROUND(100.0 * COUNT(*) FILTER (WHERE {value_column} IS NULL) / COUNT(*), 2) AS null_pct_value
        FROM {table}
    '''.format(any_key_null=any_key_null, value_column=value_column, table=table)).iloc[0].to_dict()
    row['table'] = table
    row['key_columns'] = ' + '.join(keys)
    row['duplicate_pct'] = round(100.0 * (total - distinct_keys) / total, 2)
    profiles.append(row)

print(pd.DataFrame(profiles)[
    ['table', 'key_columns', 'rows_total', 'null_pct_keys', 'null_pct_value', 'duplicate_pct']
].to_string(index=False))
print('value columns: paid_amount / service_amount / payment_amount / plan_id')
"""),
("md", """**Drill 5 - THE INVESTIGATION: "claims are up 40% month over month"**
- **Business question:** the dashboard shows +40% (2025-03 -> 2025-04); the source system reports +5%. Which is right?
- **Method:** walk the pipeline stage by stage, one query per stage, and quantify each defect you find.
- **Stages:** `raw.claims_api_records` -> `bronze.claims` -> `silver.claims` -> `dashboard.claims_dashboard` -> `dashboard.monthly_claim_counts`.
- **Suspects:** duplicated loads, join fan-out, dropped records, a date-boundary mismatch.
"""),
("code", """print('step 1 - stage counts')
for label, sql in [
    ('raw.claims_api_records', 'SELECT COUNT(*) FROM raw.claims_api_records'),
    ('bronze.claims', 'SELECT COUNT(*) FROM bronze.claims'),
    ('silver.claims', 'SELECT COUNT(*) FROM silver.claims'),
    ('dashboard.claims_dashboard (rows)', 'SELECT COUNT(*) FROM dashboard.claims_dashboard'),
    ('dashboard.claims_dashboard (distinct claims)', 'SELECT COUNT(DISTINCT claim_id) FROM dashboard.claims_dashboard'),
]:
    print('  {:>44} : {:>7,}'.format(label, q1(sql)))
"""),
("code", """print('step 2 - duplicates: which batch added rows twice?')
print(q('''
SELECT _ingest_batch, COUNT(*) AS rows_loaded, COUNT(DISTINCT claim_id) AS distinct_claims,
       COUNT(*) - COUNT(DISTINCT claim_id) AS surplus_rows
FROM bronze.claims GROUP BY 1 ORDER BY rows_loaded DESC
''').to_string(index=False))
print(q('''
SELECT _ingest_batch, COUNT(*) AS duplicated_claim_ids
FROM (SELECT claim_id, _ingest_batch FROM bronze.claims GROUP BY 1, 2 HAVING COUNT(*) > 1)
GROUP BY 1 ORDER BY duplicated_claim_ids DESC
''').to_string(index=False))
print('the 2025-04-16T02:05:00Z batch is a re-run: 302 April claims loaded a second time')
"""),
("code", """print('step 3 - join multiplication: what grain is the dashboard mart?')
print(q('''
SELECT COUNT(*) AS dashboard_rows, COUNT(DISTINCT claim_id) AS distinct_claims,
       ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT claim_id), 2) AS rows_per_claim
FROM dashboard.claims_dashboard
''').to_string(index=False))
print(q('''
SELECT COUNT(*) AS service_lines, COUNT(DISTINCT claim_id) AS claims,
       ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT claim_id), 2) AS rows_per_claim
FROM silver.claim_services
''').to_string(index=False))
print('claims_dashboard is one row per service line - COUNT(*) there counts lines, not claims')
"""),
("code", """print('step 4 - which records never reached silver?')
print(q('''
WITH source AS (SELECT DISTINCT claim_id, member_id, provider_id, claim_date FROM raw.claims_api_records)
SELECT COUNT(*) AS source_claim_ids,
       COUNT(*) FILTER (WHERE s.claim_id IS NULL) AS dropped_before_publish,
       COUNT(*) FILTER (WHERE s.claim_id IS NULL AND r.claim_id IS NULL) AS missing_claim_id,
       COUNT(*) FILTER (WHERE s.claim_id IS NULL AND r.claim_id IS NOT NULL AND r.member_id IS NOT NULL
             AND NOT EXISTS (SELECT 1 FROM silver.members m WHERE m.member_id = r.member_id)) AS member_fk_failed,
       COUNT(*) FILTER (WHERE s.claim_id IS NULL AND r.claim_id IS NOT NULL
             AND (r.member_id IS NULL OR EXISTS (SELECT 1 FROM silver.members m WHERE m.member_id = r.member_id))) AS other_malformed
FROM source r LEFT JOIN silver.claims s USING (claim_id)
''').to_string(index=False))
print(q('''
SELECT reason, COUNT(*) AS records FROM dq.quarantine WHERE source = 'claims_api'
GROUP BY 1 ORDER BY records DESC
''').to_string(index=False))
"""),
("code", """print('step 5 - date boundary: which month does the mart group by?')
print(q('''
SELECT service_month AS month, COUNT(*) AS dashboard_rows,
       COUNT(*) FILTER (WHERE claim_month = service_month) AS same_claim_month,
       COUNT(*) FILTER (WHERE claim_month <> service_month) AS shifted_across_month_end
FROM dashboard.claims_dashboard
WHERE service_month IN (DATE '2025-03-01', DATE '2025-04-01')
GROUP BY 1 ORDER BY 1
''').to_string(index=False))
print(q('''
SELECT service_month, claim_month, COUNT(*) AS dashboard_rows
FROM dashboard.claims_dashboard
WHERE service_month <> claim_month AND service_month >= DATE '2025-03-01'
GROUP BY 1, 2 ORDER BY dashboard_rows DESC LIMIT 5
''').to_string(index=False))
print('the mart aggregates on service_month while the source reports claim_date - month-end lines move')
"""),
("code", """print('step 6 - how big is the gap? (dashboard vs source)')
dashboard = q('''
SELECT month, dashboard_count, distinct_claims_in_dashboard FROM dashboard.monthly_claim_counts
WHERE month IN (DATE '2025-03-01', DATE '2025-04-01') ORDER BY month
''')
source = q('''
SELECT date_trunc('month', claim_date) AS month, COUNT(*) AS source_count FROM silver.claims
WHERE claim_date >= DATE '2025-03-01' AND claim_date < DATE '2025-05-01' GROUP BY 1 ORDER BY 1
''')
print(dashboard.to_string(index=False))
print(source.to_string(index=False))
print('dashboard MoM: {:+,} rows ({:+.1%})'.format(
    dashboard['dashboard_count'].iloc[1] - dashboard['dashboard_count'].iloc[0],
    dashboard['dashboard_count'].iloc[1] / dashboard['dashboard_count'].iloc[0] - 1))
print('source    MoM: {:+,} claims ({:+.1%})'.format(
    source['source_count'].iloc[1] - source['source_count'].iloc[0],
    source['source_count'].iloc[1] / source['source_count'].iloc[0] - 1))
print('(the mart is calibrated to show ~+40%; the exact integers move if the warehouse is rebuilt - the conclusion does not)')
"""),
("md", """**Drill 6 - the corrected query**
- **Business question:** what is the true March -> April growth?
- **Fix all four defects at once:** publish from `silver` (deduped), count `DISTINCT claim_id` (claim grain), group on `claim_date`.
- **Output columns:** `claim_month`, `claims`.
"""),
("code", """# YOUR TURN: write the corrected monthly claim count (silver, one row per claim, claim_date month).
# Expected columns: ['claim_month', 'claims'].
expected_columns = ['claim_month', 'claims']
print('corrected output will have:', expected_columns)
"""),
("code", """# SOLUTION
corrected = q('''
SELECT date_trunc('month', c.claim_date) AS claim_month, COUNT(DISTINCT c.claim_id) AS claims
FROM silver.claims c
JOIN silver.claim_services s USING (claim_id)
WHERE c.claim_date >= DATE '2025-01-01'
GROUP BY 1 ORDER BY 1
''')
print(corrected.to_string(index=False))
print('true MoM 2025-03 -> 2025-04: {:+,} claims ({:+.1%})'.format(
    corrected['claims'].iloc[3] - corrected['claims'].iloc[2],
    corrected['claims'].iloc[3] / corrected['claims'].iloc[2] - 1))
"""),
("md", """**Findings & fixes:**
- **Root cause 1 - join fan-out:** `dashboard.claims_dashboard` is one row per service line (39,813 rows for 11,996 claims); the mart counts lines and calls them claims.
- **Root cause 2 - non-idempotent replay:** batch `2025-04-16T02:05:00Z` reloaded 302 April claims, so bronze April is 1,216 rows vs 906 published.
- **Root cause 3 - date-boundary mismatch:** the mart groups on `service_month`, the source reports `claim_date`, so month-end service lines are pulled into the wrong month (106 lines of March land in April).
- **Root cause 4 - silent drops:** 20 source rows (15 member-FK failures + 4 malformed + 1 missing id) never reach silver; harmless here, invisible in the dashboard.
- **Fix:** publish from silver, `COUNT(DISTINCT claim_id)`, group on `claim_date`, and make the load an overwrite/upsert on `claim_id`.
- **Verify:** the corrected query gives 863 -> 906 (+5.0%), which matches the source system; MoM growth was never +40%.
- **Monitoring:** row-count reconciliation per stage per month, a duplicate-`claim_id` check on bronze, a mart-vs-source variance check (threshold 1%), and alerting on quarantine volume.
"""),
("md", """**Drill 7 - reconciliation queries, interview style**
- **Business question:** prove source and target agree - and if they do not, localise it.
- **Patterns:** count vs count, `LEFT JOIN ... IS NULL` for missing ids, `GROUP BY ... HAVING COUNT(*) > 1` for duplicates, sum vs sum for money, then by month.
- **Grain:** every query returns one row per grain you are reconciling.
"""),
("code", """print('recon 1 - source vs target count')
print(q('''
SELECT 'all rows' AS grain, (SELECT COUNT(*) FROM raw.claims_api_records) AS source_count,
       (SELECT COUNT(*) FROM silver.claims) AS target_count
UNION ALL
SELECT 'distinct claim_id', (SELECT COUNT(DISTINCT claim_id) FROM raw.claims_api_records),
       (SELECT COUNT(DISTINCT claim_id) FROM silver.claims)
''').to_string(index=False))
print('12,061 records -> 12,000 distinct claim_ids (60 duplicate versions, 1 missing id) -> 11,981 published (19 dropped)')

print('recon 2 - missing ids (LEFT JOIN ... IS NULL)')
print(q('''
SELECT r.claim_id, r.member_id, r.provider_id, r.claim_date
FROM (SELECT DISTINCT claim_id, member_id, provider_id, claim_date FROM raw.claims_api_records) r
LEFT JOIN silver.claims c USING (claim_id)
WHERE c.claim_id IS NULL ORDER BY r.claim_date NULLS LAST LIMIT 6
''').to_string(index=False))
print('missing ids in total:', q1('''
SELECT COUNT(*) FROM (SELECT DISTINCT claim_id FROM raw.claims_api_records) r
LEFT JOIN silver.claims c USING (claim_id) WHERE c.claim_id IS NULL
'''))

print('recon 3 - duplicate ids')
print(q('''
SELECT claim_id, COUNT(*) AS versions, MIN(updated_at) AS first_version, MAX(updated_at) AS last_version
FROM bronze.claims GROUP BY 1 HAVING COUNT(*) > 1 ORDER BY versions DESC, claim_id LIMIT 5
''').to_string(index=False))
print('claim_ids loaded more than once:', q1(
    'SELECT COUNT(*) FROM (SELECT claim_id FROM bronze.claims GROUP BY 1 HAVING COUNT(*) > 1)'))
"""),
("code", """print('recon 4 - financial reconciliation')
# compare like for like: claims.paid_amount against SETTLED payments only.
# a voided payment is not money out, so including it would manufacture a fake variance.
print(q('''
SELECT (SELECT SUM(paid_amount) FROM silver.claims)                              AS claims_paid_amount,
       (SELECT SUM(payment_amount) FROM silver.payments
         WHERE payment_status = 'Paid')                                          AS payments_settled,
       (SELECT SUM(payment_amount) FROM silver.payments
         WHERE payment_status = 'Voided')                                        AS payments_voided_excluded,
       (SELECT SUM(paid_amount) FROM silver.claims)
         - (SELECT SUM(payment_amount) FROM silver.payments
             WHERE payment_status = 'Paid')                                      AS variance,
       (SELECT COUNT(*) FROM (
            SELECT c.claim_id, c.paid_amount FROM silver.claims c
            LEFT JOIN silver.payments p ON p.claim_id = c.claim_id AND p.payment_status = 'Paid'
            WHERE c.paid_amount IS NOT NULL AND c.paid_amount > 0
            GROUP BY 1, 2 HAVING ABS(c.paid_amount - COALESCE(SUM(p.payment_amount), 0)) > 0.01)) AS unmatched_paid_claims
''').to_string(index=False))

print('recon 5 - by month, to localise the break')
print(q('''
WITH joined AS (
    SELECT c.claim_id, c.claim_date, c.paid_amount, COUNT(p.payment_id) AS payment_rows
    FROM silver.claims c LEFT JOIN silver.payments p USING (claim_id) GROUP BY 1, 2, 3)
SELECT date_trunc('month', claim_date) AS month, COUNT(*) AS claims,
       COUNT(*) FILTER (WHERE paid_amount > 0) AS paid_claims,
       COUNT(*) FILTER (WHERE paid_amount > 0 AND payment_rows = 0) AS paid_without_payment_row,
       SUM(paid_amount) AS claims_paid
FROM joined WHERE claim_date >= DATE '2025-01-01' GROUP BY 1 ORDER BY 1
''').to_string(index=False))
"""),
("md", """**Takeaway:** reconcile at the grain you publish (claim vs service line), compare money as well as counts, and always localise by month or source before you go looking for the bug - the stage-by-stage walk in drill 5 is the same move.
"""),
]
