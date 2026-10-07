# Schema & data dictionary

Domain: US health-plan claims processing. All tables are small (11,981 claims) but every
row is referentially sane, and the source layers contain **deliberate, documented defects**
so you can practise ingestion + data quality.

## Layers (all inside `data/warehouse/healthcare.duckdb`)

| Layer | Schema | What it is | Use it for |
|---|---|---|---|
| Raw | `raw.*` | exactly as landed: all strings, defects intact | ingestion/DQ drills, "what did the source actually send?" |
| Bronze | `bronze.*` | validated + type-cast, **duplicates preserved**, `_ingest_batch` / `_loaded_at` | dedup, idempotency, incremental drills |
| Silver | `silver.*` | deduplicated, conformed, FKs valid | the publishable layer |
| Main | `main.*` | views over silver with the plain names | **all 50 SQL questions** |
| Gold | `gold.*` | monthly / provider KPI marts | Q46–Q50 reference answers |
| DQ | `dq.*` | `quarantine`, `dq_results`, `dq_metrics` | DQ scorecard + investigation |
| Dashboard | `dashboard.*` | deliberately broken marts | Q49 investigation |

## Practice tables (`main.*`)

| Table | Grain | Rows | Primary key | Foreign keys |
|---|---|---|---|---|
| `members` | 1 row per member | 1,200 | `member_id` | `plan_id → plans` |
| `providers` | 1 row per provider | 220 | `provider_id` | — |
| `provider_specialty_history` | 1 row per provider per specialty period (SCD2) | 60 | `provider_id, effective_start` | `provider_id → providers` |
| `plans` | 1 row per plan | 5 | `plan_id` | — |
| `procedures` | 1 row per CPT code | 32 | `procedure_code` | — |
| `diagnoses` | 1 row per ICD-10 code | 30 | `diagnosis_code` | — |
| `claims` | 1 row per claim | 11,981 | `claim_id` | `member_id → members`, `provider_id → providers`, `primary_diagnosis_code → diagnoses` |
| `claim_services` | 1 row per claim **service line** | 38,592 | `claim_id, service_id` | `claim_id → claims`, `procedure_code → procedures` |
| `payments` | 1 row per payment transaction | 11,465 | `payment_id` | `claim_id → claims` |

Key columns: `claims(claim_date, submitted_date, updated_at, ingested_at, status, claim_type, billed_amount, paid_amount)`,
`claim_services(procedure_code, service_date, units, service_amount)`, `payments(payment_date, payment_amount, payment_method, payment_status)`.

Data window: **2024-01-01 → 2025-06-30** (18 months). Status mix: Approved 8,661 · Denied 2,172 · Submitted 1,148.
`paid_amount` is `NULL` for most `Submitted` claims (not adjudicated yet).

## Relationship diagram

```
plans ──< members ──< claims >── providers >── provider_specialty_history
                       │  │
                       │  └──< claim_services >── procedures
                       └──< payments
claims >── diagnoses
```

## Deliberate source defects (this is the interesting part)

| # | Defect | Where | Expected handling |
|---|---|---|---|
| 1 | 60 claims appear twice with different `updated_at` (old + current version) | `raw.claims_api_records`, `bronze.claims` | dedup with `row_number()` on `updated_at DESC` |
| 2 | 302 April claims loaded a second time by a job that was re-run (not idempotent) | `bronze.claims_replay` | idempotent write / dedup |
| 3 | 40 status values in mixed case (`" approved "`, `"denied"`) | raw only | `trim` + `upper`; conform to `Approved/Denied/Submitted` |
| 4 | 25 member states dirty (`ca`, `" TX "`, `California`) | `raw.members_extract` | reference lookup + fallback `UNKNOWN` |
| 5 | 6 malformed API records (missing key, `03/14/2024` date, `"$1,250.50"`, `"N/A"` status) | API pages | 5 rejected to `dq.quarantine`, 1 amount coerced |
| 6 | 40+ service lines with an orphan `claim_id` | `raw.claim_services_extract` | quarantine, never publish |
| 7 | 10 negative `paid_amount`, 8 negative `service_amount` (legitimate reversals) | claims / services | keep, but flag: domain knowledge matters |
| 8 | 12 claims where `paid_amount > billed_amount` | claims | flag as quality issue |
| 9 | 23 claims where `billed_amount ≠ SUM(service_amount)` | claims | flag as quality issue |
| 10 | 25 duplicate `payment_id`, 5% of payment amounts as `"$1,250.50"` strings | `raw.payments_stream` | dedup + parse |
| 11 | ~486 paid claims where the payment register does not tie back to `claims.paid_amount` | payments | reconciliation drill |
| 12 | 30 providers changed specialty → `provider_specialty_history` has date ranges | provider dim | SCD2 / point-in-time join |
| 13 | 60 claims ingested 75–150 days after `updated_at` | claims | late-arriving data |
| 14 | 15 claims with a `member_id` that does not exist; 8 claims dated before enrollment | claims | FK validation / DQ rules |

## Data-quality scorecard (after the reference pipeline)

`10 PASS · 7 WARN · 1 FAIL` — the single FAIL is `dashboard.reconciles_to_source`
(dashboard says **+40%** Mar→Apr, source says **+5%**). Q49 is the investigation.

Quarantine (`dq.quarantine`, 236 records): 111 payments (orphan claim or unparseable amount), 105 orphan service lines,
15 claims whose member/provider FK does not resolve, 5 malformed API records (missing key, bad date format,
out-of-domain status). `--` duplicates are not quarantined, they are collapsed during the silver dedupe.
