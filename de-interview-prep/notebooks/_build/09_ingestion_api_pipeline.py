TITLE = "09 - API ingestion: pagination, retries, idempotency"
CELLS = [
("md", """**Build:** a production-shaped ingestion of the claims API into parquet.
- API: paginated REST, **12,061 records over 25 pages of 500** (`fake_api.ClaimsAPI`, offline, deterministic failures)
- output: `data/scratch/landing/claims_raw.jsonl` (raw, replayable) + `data/scratch/bronze/claims.parquet` (typed)
- drills: paginate -> retry -> validate / dead-letter -> watermark -> idempotent replay
- read-only warehouse gives the counter-example: `bronze.claims` 12,358 rows vs `silver.claims` 11,981
"""),
("md", """**Drill 1 - one page is not the dataset**
- **Goal:** call the API once and measure what you actually got.
- **API shape:** `GET /claims?page=N&limit=500` -> `{total, data[]}`.
- **Gotcha:** HTTP 200 with 500 rows looks like success; it is 4% of the data.
"""),
("code", """import json
import re
import time

import pandas as pd
import requests
from fake_api import ClaimsAPI

api = ClaimsAPI()
resp = api.get(page=1, limit=500)
payload = resp.json()
print('HTTP', resp.status_code)
print('rows on page 1 :', len(payload['data']))
print('rows in source :', payload['total'])
print('coverage       : {:.1f}%'.format(100 * len(payload['data']) / payload['total']))
"""),
("md", """**Drill 2 - paginate until the API says stop**
- **Goal:** collect every page and report progress per page.
- **Final grain:** one row per record (12,061 dicts), page order preserved.
- **Output columns:** `page`, `rows_on_page`, `running_total`.
- **Gotcha:** stop on an empty page, not on `total` - and expect pages + 1 calls.
"""),
("code", """# YOUR TURN: loop pages until a batch comes back empty, collecting every record.
# Expected progress columns: ['page', 'rows_on_page', 'running_total'].
expected_columns = ['page', 'rows_on_page', 'running_total']
print('build a loop that reports:', expected_columns)
"""),
("code", """# SOLUTION
PAGE_LIMIT = 500


def fetch_all(fetch_one, limit=PAGE_LIMIT, since=None, quiet=False):
    '''Page through the API until a page returns no data.'''
    records, page = [], 1
    while True:
        batch = fetch_one(page=page, limit=limit, since=since)
        if not batch:
            break
        records.extend(batch)
        if not quiet:
            print('  page {:>2} -> {:>3} rows | running total {:>6,}'.format(page, len(batch), len(records)))
        page += 1
    return records


api = ClaimsAPI()
records = fetch_all(lambda **kw: api.get(**kw, timeout=30).json()['data'])
print('transport calls:', api.calls, '| records landed:', len(records))
"""),
("md", """**Drill 3 - retries with bounded backoff**
- **Goal:** survive 429/5xx/timeouts without hammering the API.
- **Policy:** 3 attempts, sleep `0.01 * 2**attempt`, retry only transient failures.
- **Never retry:** 400/401/403/404/422 - fail fast, alert, do not spin.
- **Output:** retry log with columns `page`, `attempt`, `reason` + injected failure count.
"""),
("code", """# YOUR TURN: write get_with_retry(api, max_attempts=3, **kwargs) returning a Response.
# Expected retry log columns: ['page', 'attempt', 'reason'].
expected_columns = ['page', 'attempt', 'reason']
print('the retry log will have:', expected_columns)
"""),
("code", """# SOLUTION
RETRYABLE = {429, 500, 502, 503, 504}          # transient only
RETRY_LOG = []


def get_with_retry(api, max_attempts=3, **kwargs):
    '''Bounded exponential backoff; a permanent 4xx is re-raised immediately.'''
    for attempt in range(max_attempts):
        try:
            resp = api.get(timeout=30, **kwargs)
            if resp.status_code in RETRYABLE:
                raise requests.exceptions.HTTPError('{} retryable'.format(resp.status_code), response=resp)
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout:
            RETRY_LOG.append(dict(page=kwargs.get('page'), attempt=attempt + 1, reason='timeout'))
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status not in RETRYABLE:
                raise                          # 401/404 -> the caller must see it
            RETRY_LOG.append(dict(page=kwargs.get('page'), attempt=attempt + 1, reason='HTTP {}'.format(status)))
        time.sleep(0.01 * 2 ** attempt)        # deterministic backoff, keeps the drill fast
    raise RuntimeError('gave up after {} attempts: {}'.format(max_attempts, kwargs))


hard_api = ClaimsAPI(fail_every=7, timeout_every=11)   # every 7th call fails, every 11th times out
raw_records = fetch_all(lambda **kw: get_with_retry(hard_api, **kw).json()['data'], quiet=True)
print('records landed   :', len(raw_records))
print('transport calls  :', hard_api.calls)
print('retries needed   :', len(RETRY_LOG))
print('failures injected:', hard_api.failures_injected)
print(pd.DataFrame(RETRY_LOG).to_string(index=False))
"""),
("code", """print('retry policy by status code:')
for status in (429, 500, 503, 401, 404, 422):
    print('  {:>3} -> {}'.format(status, 'retry with backoff' if status in RETRYABLE else 'fail fast + alert'))


class UnauthorizedTransport:
    '''Stub transport that always answers 401, to prove we do not retry it.'''

    def __init__(self):
        self.calls = 0

    def get(self, **kwargs):
        self.calls += 1
        response = requests.Response()
        response.status_code = 401
        raise requests.exceptions.HTTPError('401 unauthorized', response=response)


blocked = UnauthorizedTransport()
try:
    get_with_retry(blocked, page=1)
except requests.exceptions.HTTPError as exc:
    print('401 ->', exc, '| transport calls:', blocked.calls, '(no retry)')
"""),
("md", """**Drill 4 - validate, then dead-letter**
- **Required keys:** claim_id, member_id, provider_id, claim_date, status, billed_amount.
- **Rules:** date matches `YYYY-MM-DD`; status in Submitted/Approved/Denied (case-insensitive); amounts numeric or `$1,234.56`.
- **Expected:** 5 rejected records and 1 coerced currency amount out of 12,061.
- **Why:** one dirty record must never kill the batch - it goes to the dead-letter file with a reason.
"""),
("code", """# YOUR TURN: validate(record) -> (clean_record, [reasons]); split_records(records) -> (valid, rejected).
# Expected rejected columns: ['claim_id', 'reasons'].
expected_columns = ['claim_id', 'reasons']
print('rejected rows will have:', expected_columns)
"""),
("code", """# SOLUTION
REQUIRED = ['claim_id', 'member_id', 'provider_id', 'claim_date', 'status', 'billed_amount']
DATE_RE = re.compile(r'^\\d{4}-\\d{2}-\\d{2}$')
STATUSES = {'SUBMITTED', 'APPROVED', 'DENIED'}
MONEY_RE = re.compile(r'^\\$?\\s*-?[\\d,]+(\\.\\d{1,2})?$')


def parse_money(value):
    '''Return (amount, was_coerced); currency strings like $1,250.50 are coerced.'''
    if value is None or isinstance(value, bool):
        return None, False
    if isinstance(value, (int, float)):
        return float(value), False
    if isinstance(value, str) and MONEY_RE.match(value.strip()):
        return float(value.strip().lstrip('$').replace(',', '')), True
    return None, False


def validate(record):
    reasons = ['missing_' + key for key in REQUIRED if record.get(key) in (None, '')]
    if 'missing_claim_date' not in reasons and not DATE_RE.match(str(record['claim_date'])):
        reasons.append('invalid_claim_date')
    if 'missing_status' not in reasons and str(record['status']).strip().upper() not in STATUSES:
        reasons.append('invalid_status')
    billed, coerced_billed = parse_money(record.get('billed_amount'))
    if 'missing_billed_amount' not in reasons and billed is None:
        reasons.append('invalid_billed_amount')
    paid, coerced_paid = parse_money(record.get('paid_amount'))
    if record.get('paid_amount') not in (None, '') and paid is None:
        reasons.append('invalid_paid_amount')
    clean = dict(record)
    if not reasons:
        clean['status'] = str(clean['status']).strip().title()      # case-insensitive domain
        clean['billed_amount'] = billed
        clean['paid_amount'] = paid
        clean['_coerced_amounts'] = coerced_billed or coerced_paid
    return clean, reasons


def split_records(records):
    valid, rejected = [], []
    for record in records:
        clean, reasons = validate(record)
        if reasons:
            rejected.append(dict(claim_id=record.get('claim_id'), reasons=', '.join(reasons)))
        else:
            valid.append(clean)
    return valid, rejected


valid, rejected = split_records(raw_records)
print('valid   :', len(valid))
print('rejected:', len(rejected))
print('coerced currency amounts:', sum(row['_coerced_amounts'] for row in valid))
print(pd.DataFrame(rejected).to_string(index=False))
"""),
("md", """**Drill 5 - land raw, then write typed parquet**
- **Goal:** two artefacts - what arrived, and what the platform will use.
- **Grain:** one row per API record in raw; one row per validated claim in the parquet.
- **Gotcha:** write raw before validation, or the rejects are lost and you cannot re-process them.
"""),
("code", """# SOLUTION
SCRATCH = ROOT / 'data' / 'scratch'
LANDING = SCRATCH / 'landing'
BRONZE = SCRATCH / 'bronze'
LANDING.mkdir(parents=True, exist_ok=True)
BRONZE.mkdir(parents=True, exist_ok=True)

raw_path = LANDING / 'claims_raw.jsonl'
with raw_path.open('w') as handle:
    for record in raw_records:                     # exactly as received, defects intact
        handle.write(json.dumps(record) + '\\n')

reject_path = LANDING / 'claims_rejected.jsonl'
with reject_path.open('w') as handle:
    for row in rejected:
        handle.write(json.dumps(row) + '\\n')


def to_bronze_frame(rows):
    '''Cast the validated records to the bronze types.'''
    frame = pd.DataFrame(rows)
    for column in ('claim_date', 'submitted_date', 'updated_at', 'ingested_at'):
        frame[column] = pd.to_datetime(frame[column], errors='coerce')
    frame['billed_amount'] = frame['billed_amount'].astype(float)
    frame['paid_amount'] = pd.to_numeric(frame['paid_amount'], errors='coerce')
    return frame


bronze_frame = to_bronze_frame(valid)
bronze_path = BRONZE / 'claims.parquet'
bronze_frame.to_parquet(bronze_path, index=False)

print('raw      :', raw_path.relative_to(ROOT), '{:.2f} MB'.format(raw_path.stat().st_size / 1e6), len(raw_records), 'lines')
print('rejected :', reject_path.relative_to(ROOT), len(rejected), 'lines')
print('typed    :', bronze_path.relative_to(ROOT), '{:.2f} MB'.format(bronze_path.stat().st_size / 1e6), len(bronze_frame), 'rows')
print(pd.read_parquet(bronze_path,
                      columns=['claim_id', 'claim_date', 'status', 'billed_amount', 'paid_amount', '_coerced_amounts']
                      ).head(3).to_string(index=False))
"""),
("md", """**Raw replayability:** the JSONL is the immutable record of what the API actually sent - when a rule changes you re-run the transform over the same bytes instead of asking the API for history it may not keep. Bronze is derived, never the source of truth.
"""),
("md", """**Drill 6 - watermark / incremental load**
- **Goal:** re-ask the API only for what changed.
- **Watermark:** `max(updated_at)` of the batch just landed, passed as `since=`.
- **Expected:** 0 new records - a correct, successful no-op, not an error.
- **Gotcha:** offset pagination + `since` still walks every page; a real API would offer a cursor.
"""),
("code", """# YOUR TURN: derive the watermark from bronze_frame and call the API with since=<watermark>.
# Expected print: the watermark timestamp and the number of newer records.
expected_columns = ['updated_at']
print('derive max(updated_at) from:', expected_columns)
"""),
("code", """# SOLUTION
watermark = bronze_frame['updated_at'].max()
print('watermark = max(updated_at) just landed:', watermark)

incremental_api = ClaimsAPI()
newer = fetch_all(lambda **kw: incremental_api.get(**kw, timeout=30).json()['data'],
                  since=str(watermark), quiet=True)
print('records with updated_at > watermark:', len(newer), '-> nothing to load, exit successfully')

rewind = str(watermark - pd.Timedelta(days=7))
window = [record for record in raw_records if str(record.get('updated_at', '')) > rewind]
print('rows updated in the last 7 days of the batch:', len(window), '-> the size of a catch-up window')
print('the fixture applies `since` inside each page, so the page-1 call above returns 0:',
      len(fetch_all(lambda **kw: ClaimsAPI().get(**kw, timeout=30).json()['data'],
                    since=rewind, quiet=True)))
print('lesson: offset pagination + since is fragile - real APIs expose a cursor/keyset token')
"""),
("md", """**Drill 7 - idempotency: re-run and prove it is a no-op**
- **Goal:** run the identical ingestion twice, overwrite the same path.
- **Expected:** row count identical after the second run (12,056 validated rows).
- **Counter-example:** `bronze.claims_replay` - a non-idempotent April re-run worth 302 rows; `bronze.claims` ends with 354 claim_ids loaded more than once.
"""),
("code", """# YOUR TURN: re-run the full ingest and overwrite bronze_path; compare row counts.
# Expected output: rows before == rows after, and a stable distinct claim_id count.
expected_columns = ['rows_before', 'rows_after', 'distinct_claim_ids']
print('compare:', expected_columns)
"""),
("code", """# SOLUTION
rerun_api = ClaimsAPI()
rerun_records = fetch_all(lambda **kw: rerun_api.get(**kw, timeout=30).json()['data'], quiet=True)
valid_rerun, rejected_rerun = split_records(rerun_records)

rows_before = len(pd.read_parquet(bronze_path))
to_bronze_frame(valid_rerun).to_parquet(bronze_path, index=False)   # same path, overwrite = replay-safe
replayed = pd.read_parquet(bronze_path)

print('run 1: {} valid / {} rejected'.format(len(valid), len(rejected)))
print('run 2: {} valid / {} rejected'.format(len(valid_rerun), len(rejected_rerun)))
print('rows before re-run:', rows_before, '| rows after re-run:', len(replayed))
print('distinct claim_ids :', replayed['claim_id'].nunique(), '| stable:', rows_before == len(replayed))
"""),
("code", """# what a non-idempotent replay looks like in the warehouse
print(q('''
SELECT COUNT(*) AS bronze_rows, COUNT(DISTINCT claim_id) AS distinct_claim_ids,
       COUNT(*) - COUNT(DISTINCT claim_id) AS surplus_rows,
       (SELECT COUNT(*) FROM (SELECT claim_id FROM bronze.claims GROUP BY 1 HAVING COUNT(*) > 1)) AS duplicated_claim_ids
FROM bronze.claims
''').to_string(index=False))
print('(the main batch contributes 60 re-versioned claims; the replay batch contributes the other 302 rows)')

print(q('''
SELECT _ingest_batch, COUNT(*) AS rows_loaded, MIN(claim_date) AS first_claim, MAX(claim_date) AS last_claim
FROM bronze.claims GROUP BY 1 ORDER BY rows_loaded DESC
''').to_string(index=False))

print(q('''
SELECT date_trunc('month', claim_date) AS claim_month,
       COUNT(*) AS bronze_rows,
       COUNT(*) FILTER (WHERE _ingest_batch = '2025-04-16T02:05:00Z') AS replay_rows
FROM bronze.claims
WHERE claim_date >= DATE '2025-03-01' AND claim_date < DATE '2025-05-01'
GROUP BY 1 ORDER BY 1
''').to_string(index=False))
print('silver April rows:', q1("SELECT COUNT(*) FROM silver.claims WHERE date_trunc('month', claim_date) = DATE '2025-04-01'"))
"""),
("md", """**Fix in one line:** write raw once and make the load an overwrite/upsert on the business key (`claim_id`, keeping `max(updated_at)`), so re-running a batch replaces its rows instead of appending them.
"""),
("md", """**Interview answer in 6 lines:**
1. **Auth:** OAuth2 client-credentials, token cached, refreshed once on 401 - a 401 is a re-auth, never a retry loop.
2. **Pagination:** page until an empty batch (or cursor), honour `limit`, persist the last cursor so a crash resumes instead of restarting.
3. **Timeout + retry:** explicit connect/read timeout on every call; 3 attempts with exponential backoff + jitter, retrying only 429/5xx/timeouts and honouring `Retry-After`; 4xx fails fast.
4. **Validation:** schema, required keys, date format, status domain and amount parsing before publishing; rejects go to a dead-letter file with a reason and never block the batch.
5. **Idempotency:** land raw immutably, then overwrite/upsert by business key so a re-run is a no-op; watermark (`max(updated_at)`) drives incremental loads.
6. **Observability:** log rows in/out, pages, retries, rejects, watermark and freshness per run, and alert on retry exhaustion or a sudden reject-rate jump.
"""),
]
