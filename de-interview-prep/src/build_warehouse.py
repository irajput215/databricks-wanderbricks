"""
Build the medallion warehouse from the generated sources.

    python src/build_warehouse.py

Layers created inside data/warehouse/healthcare.duckdb
------------------------------------------------------
raw        exactly as landed (all strings, defects included)
bronze     validated + type-coerced, still duplicated  (+ ingestion metadata)
silver     deduplicated, conformed, referentially valid (the publishable layer)
main       practice views over silver, named members/claims/... (used by the SQL notebooks)
gold       monthly + provider KPI marts
dq         dq.quarantine and dq.dq_results
dashboard  deliberately flawed marts used by the investigation notebook

Also exports parquet copies of bronze/silver/gold for the PySpark notebooks.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone

import duckdb
import pandas as pd

from dq_checks import metrics, run_checks
from paths import BRONZE, DB_PATH, GOLD, LANDING, RAW, SILVER, SNAPSHOT_DATE
from paths import QUARANTINE as QDIR

STATUS_DOMAIN = {"SUBMITTED", "APPROVED", "DENIED"}
REQUIRED_CLAIM_FIELDS = ["claim_id", "member_id", "provider_id", "claim_date", "status", "billed_amount"]
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
NULLISH = {"", "NA", "N/A", "NULL", "NONE", "NAN"}

STATE_LOOKUP = [
    ("AL", "Alabama"), ("AK", "Alaska"), ("AZ", "Arizona"), ("AR", "Arkansas"), ("CA", "California"),
    ("CO", "Colorado"), ("CT", "Connecticut"), ("DE", "Delaware"), ("DC", "District of Columbia"),
    ("FL", "Florida"), ("GA", "Georgia"), ("HI", "Hawaii"), ("ID", "Idaho"), ("IL", "Illinois"),
    ("IN", "Indiana"), ("IA", "Iowa"), ("KS", "Kansas"), ("KY", "Kentucky"), ("LA", "Louisiana"),
    ("ME", "Maine"), ("MD", "Maryland"), ("MA", "Massachusetts"), ("MI", "Michigan"), ("MN", "Minnesota"),
    ("MS", "Mississippi"), ("MO", "Missouri"), ("MT", "Montana"), ("NE", "Nebraska"), ("NV", "Nevada"),
    ("NH", "New Hampshire"), ("NJ", "New Jersey"), ("NM", "New Mexico"), ("NY", "New York"),
    ("NC", "North Carolina"), ("ND", "North Dakota"), ("OH", "Ohio"), ("OK", "Oklahoma"), ("OR", "Oregon"),
    ("PA", "Pennsylvania"), ("RI", "Rhode Island"), ("SC", "South Carolina"), ("SD", "South Dakota"),
    ("TN", "Tennessee"), ("TX", "Texas"), ("UT", "Utah"), ("VT", "Vermont"), ("VA", "Virginia"),
    ("WA", "Washington"), ("WV", "West Virginia"), ("WI", "Wisconsin"), ("WY", "Wyoming"),
]

TARGET_NAIVE_GROWTH = 0.40   # the +40% the business reports between Mar-2025 and Apr-2025


# --------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------
def to_amount(value):
    """Coerce money to float. Returns (amount, error_reason)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None, None
    if isinstance(value, (int, float)):
        return float(value), None
    text = str(value).strip().replace("$", "").replace(",", "")
    if text.upper() in NULLISH:
        return None, None if text == "" else "non_numeric"
    try:
        return float(text), None
    except ValueError:
        return None, "non_numeric"


def to_ts(value):
    if value is None or str(value).strip().upper() in NULLISH:
        return None
    return str(value).strip()[:19]


def read_api_pages():
    pages = sorted(LANDING.glob("claims_page_*.json"))
    records, raw_rows = [], []
    for path in pages:
        payload = json.loads(path.read_text())
        for rec in payload["data"]:
            raw_rows.append({**{k: (None if v is None else str(v)) for k, v in rec.items()},
                             "_source_file": path.name, "_page": payload["page"]})
            records.append((rec, path.name))
    return records, pd.DataFrame(raw_rows)


def validate_claim(rec, source_file):
    """Schema + domain validation. Returns (clean_record, [reasons])."""
    reasons = [
        f"missing_{f}" for f in REQUIRED_CLAIM_FIELDS
        if rec.get(f) is None or str(rec.get(f)).strip().upper() in NULLISH
    ]
    if "missing_claim_date" not in reasons and not DATE_RE.match(str(rec.get("claim_date", ""))):
        reasons.append("invalid_claim_date_format")
    status = str(rec.get("status", "")).strip().upper()
    if "missing_status" not in reasons and status not in STATUS_DOMAIN:
        reasons.append("status_out_of_domain")
    billed, billed_err = to_amount(rec.get("billed_amount"))
    if billed_err:
        reasons.append(f"billed_amount_{billed_err}")
    paid, paid_err = to_amount(rec.get("paid_amount"))
    if paid_err:
        reasons.append(f"paid_amount_{paid_err}")
    if reasons:
        return None, reasons
    clean = {
        "claim_id": str(rec["claim_id"]).strip(),
        "member_id": str(rec["member_id"]).strip(),
        "provider_id": str(rec["provider_id"]).strip(),
        "claim_date": str(rec["claim_date"]).strip(),
        "submitted_date": to_ts(rec.get("submitted_date")),
        "updated_at": to_ts(rec.get("updated_at")),
        "status": status.title(),
        "claim_type": rec.get("claim_type"),
        "primary_diagnosis_code": rec.get("primary_diagnosis_code"),
        "billed_amount": billed,
        "paid_amount": paid,
        "ingested_at": to_ts(rec.get("ingested_at")),
        "source_system": rec.get("source_system", "CLAIMS_API"),
        "_source_file": source_file,
        "_coerced_amounts": bool(
            isinstance(rec.get("billed_amount"), str) or isinstance(rec.get("paid_amount"), str)),
    }
    return clean, []


# --------------------------------------------------------------------------------------
# layer builders
# --------------------------------------------------------------------------------------
def build_raw(con):
    """Land every source exactly as received - no validation, no coercion."""
    records, claims_raw_df = read_api_pages()
    con.register("api_records_df", claims_raw_df)
    con.sql("CREATE OR REPLACE TABLE raw.claims_api_records AS SELECT * FROM api_records_df")
    con.sql("""
        CREATE OR REPLACE TABLE raw.members_extract       AS SELECT * FROM read_csv_auto('data/raw/members.csv');
        CREATE OR REPLACE TABLE raw.providers_extract     AS SELECT * FROM read_csv_auto('data/raw/providers.csv');
        CREATE OR REPLACE TABLE raw.plans_extract         AS SELECT * FROM read_csv_auto('data/raw/plans.csv');
        CREATE OR REPLACE TABLE raw.procedures_extract    AS SELECT * FROM read_csv_auto('data/raw/procedures.csv');
        CREATE OR REPLACE TABLE raw.diagnoses_extract     AS SELECT * FROM read_csv_auto('data/raw/diagnoses.csv');
        CREATE OR REPLACE TABLE raw.claim_services_extract AS SELECT * FROM read_csv_auto('data/raw/claim_services.csv');
        CREATE OR REPLACE TABLE raw.specialty_history_extract AS SELECT * FROM read_csv_auto('data/raw/provider_specialty_history.csv');
    """)
    payments = [json.loads(line) for line in (RAW / "payments.jsonl").read_text().splitlines() if line.strip()]
    pay_df = pd.DataFrame([{k: (None if v is None else str(v)) for k, v in p.items()} for p in payments])
    con.register("payments_df", pay_df)
    con.sql("CREATE OR REPLACE TABLE raw.payments_stream AS SELECT * FROM payments_df")
    con.register("state_lookup_df", pd.DataFrame(STATE_LOOKUP, columns=["state_code", "state_name"]))
    con.sql("CREATE OR REPLACE TABLE raw.state_lookup AS SELECT * FROM state_lookup_df")
    return records


def build_bronze(con, records):
    """Validate, coerce and land into bronze - duplicates are preserved on purpose."""
    batch = f"{SNAPSHOT_DATE}T02:05:00Z"
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    clean_rows, quarantine = [], []
    for i, (rec, source_file) in enumerate(records):
        cleaned, reasons = validate_claim(rec, source_file)
        if cleaned:
            cleaned["_ingest_batch"] = batch
            cleaned["_loaded_at"] = loaded_at
            clean_rows.append(cleaned)
        else:
            quarantine.append(dict(record_id=f"claims_api:{rec.get('claim_id') or f'row{i}'}",
                                   source="claims_api", reason=";".join(reasons),
                                   raw_record=json.dumps(rec, default=str), quarantined_at=loaded_at))
    con.register("claims_clean_df", pd.DataFrame(clean_rows))
    con.sql(f"""
        CREATE OR REPLACE TABLE bronze.claims_first_load AS
        SELECT * REPLACE (CAST(claim_date AS DATE) AS claim_date,
                          CAST(submitted_date AS DATE) AS submitted_date,
                          CAST(updated_at AS TIMESTAMP) AS updated_at,
                          CAST(ingested_at AS TIMESTAMP) AS ingested_at,
                          CAST(_loaded_at AS TIMESTAMP) AS _loaded_at,
                          CAST(billed_amount AS DECIMAL(12,2)) AS billed_amount,
                          CAST(paid_amount AS DECIMAL(12,2)) AS paid_amount)
        FROM claims_clean_df
    """)
    q = pd.DataFrame(quarantine)
    con.register("quarantine_df", q)
    con.sql("CREATE OR REPLACE TABLE dq.quarantine AS SELECT * FROM quarantine_df")

    # flat-file + stream sources land straight into bronze (typing only, no dedup)
    con.sql("""
        CREATE OR REPLACE TABLE bronze.members_raw AS
        SELECT member_id, first_name, last_name, CAST(dob AS DATE) dob, gender, state,
               CAST(enrollment_date AS DATE) enrollment_date, plan_id,
               CAST(updated_at AS TIMESTAMP) updated_at, 'members.csv' AS _source_file,
               CAST(now() AS TIMESTAMP) AS _loaded_at
        FROM raw.members_extract;

        CREATE OR REPLACE TABLE bronze.providers_raw AS
        SELECT provider_id, npi, provider_name, specialty, state, network_status,
               CAST(updated_at AS TIMESTAMP) updated_at, 'providers.csv' AS _source_file,
               CAST(now() AS TIMESTAMP) AS _loaded_at
        FROM raw.providers_extract;

        CREATE OR REPLACE TABLE bronze.specialty_history_raw AS SELECT * FROM raw.specialty_history_extract;
        CREATE OR REPLACE TABLE bronze.plans_raw      AS SELECT * FROM raw.plans_extract;
        CREATE OR REPLACE TABLE bronze.procedures_raw AS SELECT * FROM raw.procedures_extract;
        CREATE OR REPLACE TABLE bronze.diagnoses_raw  AS SELECT * FROM raw.diagnoses_extract;

        CREATE OR REPLACE TABLE bronze.claim_services_raw AS
        SELECT claim_id, CAST(service_id AS INTEGER) service_id, procedure_code,
               CAST(service_date AS DATE) service_date, CAST(units AS INTEGER) units,
               CAST(service_amount AS DECIMAL(12,2)) service_amount, claim_type,
               'claim_services.csv' AS _source_file, CAST(now() AS TIMESTAMP) AS _loaded_at
        FROM raw.claim_services_extract;

        CREATE OR REPLACE TABLE bronze.payments_raw AS
        SELECT p.payment_id, p.claim_id, TRY_CAST(p.payment_date AS DATE) payment_date,
               TRY_CAST(replace(replace(p.payment_amount, '$', ''), ',', '') AS DECIMAL(12,2)) payment_amount,
               p.payment_method, p.payment_status, 'payments.jsonl' AS _source_file,
               CAST(now() AS TIMESTAMP) AS _loaded_at
        FROM raw.payments_stream p;
    """)
    return len(clean_rows), len(quarantine)


def create_replay_batch(con, limit_rows: int):
    """Simulate a non-idempotent re-run: April claims loaded a second time on 2025-04-16."""
    con.sql(f"""
        CREATE OR REPLACE TABLE bronze.claims_replay AS
        SELECT * REPLACE ('2025-04-16T02:05:00Z' AS _ingest_batch,
                          CAST('2025-04-16 02:05:00' AS TIMESTAMP) AS _loaded_at)
        FROM (
            SELECT * FROM bronze.claims_first_load
            WHERE claim_date >= DATE '2025-04-01' AND claim_date < DATE '2025-05-01'
            ORDER BY claim_id LIMIT {limit_rows}
        )
    """)
    con.sql("""
        CREATE OR REPLACE TABLE bronze.claims AS
        SELECT * FROM bronze.claims_first_load UNION ALL SELECT * FROM bronze.claims_replay
    """)


def build_silver(con):
    con.sql("""
        CREATE OR REPLACE TABLE silver.state_lookup AS SELECT * FROM raw.state_lookup;

        CREATE OR REPLACE TABLE silver.plans AS
        SELECT plan_id, plan_name, plan_type, CAST(monthly_premium AS DECIMAL(10,2)) monthly_premium
        FROM bronze.plans_raw;

        CREATE OR REPLACE TABLE silver.procedures AS
        SELECT procedure_code, procedure_description, procedure_category,
               CAST(standard_fee AS DECIMAL(10,2)) standard_fee, specialty
        FROM bronze.procedures_raw;

        CREATE OR REPLACE TABLE silver.diagnoses AS
        SELECT diagnosis_code, diagnosis_description, diagnosis_category FROM bronze.diagnoses_raw;

        CREATE OR REPLACE TABLE silver.members AS
        WITH dedup AS (
            SELECT *, row_number() OVER (PARTITION BY member_id ORDER BY updated_at DESC, _loaded_at DESC) rn
            FROM bronze.members_raw
        )
        SELECT member_id, first_name, last_name, dob, gender,
               COALESCE(by_code.state_code, by_name.state_code, 'UNKNOWN') AS state,
               enrollment_date, plan_id, updated_at
        FROM dedup d
        LEFT JOIN silver.state_lookup by_code ON upper(trim(d.state)) = by_code.state_code
        LEFT JOIN silver.state_lookup by_name ON upper(trim(d.state)) = upper(by_name.state_name)
        WHERE rn = 1;

        CREATE OR REPLACE TABLE silver.providers AS
        WITH dedup AS (
            SELECT *, row_number() OVER (PARTITION BY provider_id ORDER BY updated_at DESC, _loaded_at DESC) rn
            FROM bronze.providers_raw
        )
        SELECT provider_id, npi, trim(provider_name) AS provider_name, trim(specialty) AS specialty,
               upper(trim(state)) AS state, network_status, updated_at
        FROM dedup WHERE rn = 1;

        CREATE OR REPLACE TABLE silver.provider_specialty_history AS
        SELECT provider_id, specialty, TRY_CAST(CAST(effective_start AS VARCHAR) AS DATE) effective_start,
               TRY_CAST(CAST(effective_end AS VARCHAR) AS DATE) effective_end,
               CAST(is_current AS BOOLEAN) is_current, change_reason
        FROM bronze.specialty_history_raw;

        CREATE OR REPLACE TABLE silver.claims AS
        WITH dedup AS (
            SELECT *, row_number() OVER (PARTITION BY claim_id ORDER BY updated_at DESC, _loaded_at DESC) rn
            FROM bronze.claims
        )
        SELECT claim_id, member_id, provider_id, claim_date, submitted_date, updated_at, status,
               claim_type, primary_diagnosis_code, billed_amount, paid_amount, ingested_at,
               source_system, _ingest_batch, _loaded_at
        FROM dedup d
        WHERE rn = 1
          AND member_id IN (SELECT member_id FROM silver.members)
          AND provider_id IN (SELECT provider_id FROM silver.providers);

        CREATE OR REPLACE TABLE silver.claim_services AS
        SELECT DISTINCT s.claim_id, s.service_id, s.procedure_code, s.service_date, s.units,
               s.service_amount, s.claim_type
        FROM bronze.claim_services_raw s
        WHERE s.claim_id IN (SELECT claim_id FROM silver.claims);

        CREATE OR REPLACE TABLE silver.payments AS
        SELECT DISTINCT payment_id, claim_id, payment_date, payment_amount, payment_method, payment_status
        FROM bronze.payments_raw p
        WHERE p.claim_id IN (SELECT claim_id FROM silver.claims)
          AND p.payment_amount IS NOT NULL;
    """)

    # quarantine the rows that silver drops, with a reason
    con.sql("""
        INSERT INTO dq.quarantine
        SELECT 'claims_api:' || claim_id, 'claims_fk', 'member_or_provider_fk_unresolved',
               to_json(STRUCT_PACK(claim_id := claim_id, member_id := member_id, provider_id := provider_id)),
               CAST(now() AS TIMESTAMP)
        FROM (SELECT DISTINCT claim_id, member_id, provider_id FROM bronze.claims) b
        WHERE claim_id NOT IN (SELECT claim_id FROM silver.claims);

        INSERT INTO dq.quarantine
        SELECT 'claim_services:' || claim_id || ':' || service_id, 'claim_services', 'orphan_claim_id',
               to_json(STRUCT_PACK(claim_id := claim_id, service_id := service_id, service_amount := service_amount)),
               CAST(now() AS TIMESTAMP)
        FROM bronze.claim_services_raw s
        WHERE s.claim_id NOT IN (SELECT claim_id FROM silver.claims);

        INSERT INTO dq.quarantine
        SELECT 'payments:' || payment_id, 'payments', 'orphan_claim_id_or_unparseable_amount',
               to_json(STRUCT_PACK(payment_id := payment_id, claim_id := claim_id, payment_amount := payment_amount)),
               CAST(now() AS TIMESTAMP)
        FROM bronze.payments_raw p
        WHERE p.claim_id NOT IN (SELECT claim_id FROM silver.claims) OR p.payment_amount IS NULL;
    """)


def build_main_views(con):
    """Practice-facing views: exactly the schema used by the 50 SQL questions."""
    con.sql("""
        CREATE OR REPLACE VIEW main.members           AS SELECT * FROM silver.members;
        CREATE OR REPLACE VIEW main.providers         AS SELECT * FROM silver.providers;
        CREATE OR REPLACE VIEW main.claims            AS SELECT * FROM silver.claims;
        CREATE OR REPLACE VIEW main.claim_services    AS SELECT * FROM silver.claim_services;
        CREATE OR REPLACE VIEW main.payments          AS SELECT * FROM silver.payments;
        CREATE OR REPLACE VIEW main.plans             AS SELECT * FROM silver.plans;
        CREATE OR REPLACE VIEW main.procedures        AS SELECT * FROM silver.procedures;
        CREATE OR REPLACE VIEW main.diagnoses         AS SELECT * FROM silver.diagnoses;
        CREATE OR REPLACE VIEW main.provider_specialty_history AS SELECT * FROM silver.provider_specialty_history;
    """)


def build_gold(con):
    con.sql("""
        CREATE OR REPLACE TABLE gold.monthly_claims_kpi AS
        SELECT date_trunc('month', claim_date) AS month,
               COUNT(*) AS total_claims,
               COUNT(*) FILTER (WHERE status = 'Approved') AS approved_claims,
               COUNT(*) FILTER (WHERE status = 'Denied')   AS denied_claims,
               COUNT(*) FILTER (WHERE status = 'Submitted') AS submitted_claims,
               ROUND(COUNT(*) FILTER (WHERE status = 'Approved') * 100.0 / COUNT(*), 2) AS approval_rate,
               ROUND(COUNT(*) FILTER (WHERE status = 'Denied')   * 100.0 / COUNT(*), 2) AS denial_rate,
               SUM(billed_amount) AS total_billed,
               SUM(paid_amount)   AS total_paid,
               ROUND(SUM(paid_amount) / COUNT(*), 2) AS average_claim_amount
        FROM silver.claims GROUP BY 1;

        CREATE OR REPLACE TABLE gold.provider_monthly_kpi AS
        WITH claim_agg AS (
            SELECT provider_id, date_trunc('month', claim_date) AS month,
                   COUNT(*) AS claim_count,
                   COUNT(*) FILTER (WHERE status = 'Approved') AS approved_claim_count,
                   COUNT(*) FILTER (WHERE status = 'Denied')   AS denied_claim_count,
                   SUM(billed_amount) AS total_billed,
                   SUM(paid_amount)   AS total_paid
            FROM silver.claims GROUP BY 1, 2
        ), service_agg AS (
            SELECT c.provider_id, date_trunc('month', c.claim_date) AS month,
                   COUNT(*) AS total_services, COUNT(DISTINCT s.claim_id) AS claims_with_services
            FROM silver.claims c JOIN silver.claim_services s USING (claim_id)
            GROUP BY 1, 2
        )
        SELECT a.provider_id, a.month, a.claim_count, a.approved_claim_count, a.denied_claim_count,
               ROUND(a.approved_claim_count * 100.0 / a.claim_count, 2) AS approval_rate,
               ROUND(a.denied_claim_count   * 100.0 / a.claim_count, 2) AS denial_rate,
               a.total_billed, a.total_paid,
               ROUND(a.total_paid / a.claim_count, 2) AS average_paid_amount,
               COALESCE(s.total_services, 0) AS total_services,
               ROUND(COALESCE(s.total_services, 0) * 1.0 / a.claim_count, 3) AS avg_services_per_claim
        FROM claim_agg a LEFT JOIN service_agg s USING (provider_id, month);

        CREATE OR REPLACE TABLE gold.provider_monthly_kpi_ranked AS
        SELECT k.*, p.provider_name, p.specialty, p.state, p.network_status,
               rank() OVER (PARTITION BY p.state, k.month ORDER BY k.total_paid DESC) AS provider_rank_in_state
        FROM gold.provider_monthly_kpi k JOIN silver.providers p USING (provider_id);

        CREATE OR REPLACE TABLE gold.provider_kpi AS
        SELECT c.provider_id, p.provider_name, p.specialty, p.state,
               COUNT(*) AS total_claims,
               COUNT(*) FILTER (WHERE c.status = 'Approved') AS approved_claims,
               COUNT(*) FILTER (WHERE c.status = 'Denied')   AS denied_claims,
               ROUND(COUNT(*) FILTER (WHERE c.status = 'Approved') * 100.0 / COUNT(*), 2) AS approval_rate,
               SUM(c.paid_amount) AS total_paid,
               ROUND(AVG(c.paid_amount), 2) AS average_claim_amount
        FROM silver.claims c JOIN silver.providers p USING (provider_id)
        GROUP BY 1, 2, 3, 4;
    """)


def build_dashboard(con, limit_rows: int):
    create_replay_batch(con, limit_rows)
    con.sql("""
        CREATE OR REPLACE TABLE dashboard.claims_dashboard AS
        SELECT b.claim_id, b.member_id, b.provider_id, b.status, b.claim_type,
               b.claim_date, date_trunc('month', b.claim_date) AS claim_month,
               s.service_date, date_trunc('month', s.service_date) AS service_month,
               b.billed_amount, b.paid_amount, s.service_amount, s.procedure_code,
               b._ingest_batch, b._loaded_at
        FROM bronze.claims b
        JOIN bronze.claim_services_raw s USING (claim_id);

        CREATE OR REPLACE TABLE dashboard.monthly_claim_counts AS
        SELECT service_month AS month,
               COUNT(*) AS dashboard_count,
               COUNT(DISTINCT claim_id) AS distinct_claims_in_dashboard
        FROM dashboard.claims_dashboard GROUP BY 1;
    """)
    return con.sql("""
        SELECT d.dashboard_count, s.source_count
        FROM dashboard.monthly_claim_counts d
        JOIN (SELECT date_trunc('month', claim_date) AS mth, COUNT(*) AS source_count
              FROM silver.claims GROUP BY 1) s ON d.month = s.mth
        WHERE d.month = DATE '2025-04-01'
    """).fetchone(), con.sql("""
        SELECT d.dashboard_count, s.source_count
        FROM dashboard.monthly_claim_counts d
        JOIN (SELECT date_trunc('month', claim_date) AS mth, COUNT(*) AS source_count
              FROM silver.claims GROUP BY 1) s ON d.month = s.mth
        WHERE d.month = DATE '2025-03-01'
    """).fetchone()


def calibrate_dashboard(con):
    """Make the naive dashboard show ~+40% Mar->Apr while the source shows ~+5%."""
    apr, mar = con.sql("""
        SELECT COUNT(*) FILTER (WHERE date_trunc('month', claim_date) = DATE '2025-04-01'),
               COUNT(*) FILTER (WHERE date_trunc('month', claim_date) = DATE '2025-03-01')
        FROM silver.claims
    """).fetchone()
    source_growth = apr / mar - 1
    limit_rows = max(1, int(round((1 + TARGET_NAIVE_GROWTH) * mar - apr)))
    naive_growth = 0.0
    for _ in range(6):
        (apr_dash, _), (mar_dash, _) = build_dashboard(con, limit_rows)
        naive_growth = apr_dash / mar_dash - 1
        if abs(naive_growth - TARGET_NAIVE_GROWTH) < 0.015:
            break
        limit_rows = max(1, int(round(limit_rows * (1 + TARGET_NAIVE_GROWTH) / (1 + naive_growth))))
    return dict(replay_rows=limit_rows, mar=mar, apr=apr,
                source_growth=round(source_growth, 4), naive_growth=round(naive_growth, 4))


def export_parquet(con):
    for layer, out in (("bronze", BRONZE), ("silver", SILVER), ("gold", GOLD)):
        for (tbl,) in con.sql(f"SELECT table_name FROM information_schema.tables "
                              f"WHERE table_schema = '{layer}' ORDER BY 1").fetchall():
            con.sql(f"COPY {layer}.{tbl} TO '{out / (tbl + '.parquet')}' (FORMAT PARQUET)")
    con.sql(f"COPY dq.quarantine TO '{QDIR / 'quarantine.parquet'}' (FORMAT PARQUET)")


def main():
    con = duckdb.connect(str(DB_PATH))
    con.sql("""
        CREATE SCHEMA IF NOT EXISTS raw; CREATE SCHEMA IF NOT EXISTS bronze;
        CREATE SCHEMA IF NOT EXISTS silver; CREATE SCHEMA IF NOT EXISTS gold;
        CREATE SCHEMA IF NOT EXISTS dq; CREATE SCHEMA IF NOT EXISTS dashboard;
    """)
    records = build_raw(con)
    clean, rejected = build_bronze(con, records)
    # the replay batch is calibrated later, but bronze.claims must exist before silver is built.
    # silver dedup prefers the first load (higher _loaded_at), so calibration cannot change silver.
    mar, apr = con.sql("""
        SELECT COUNT(*) FILTER (WHERE date_trunc('month', claim_date) = DATE '2025-03-01'),
               COUNT(*) FILTER (WHERE date_trunc('month', claim_date) = DATE '2025-04-01')
        FROM bronze.claims_first_load
    """).fetchone()
    create_replay_batch(con, max(1, int(round((1 + TARGET_NAIVE_GROWTH) * mar - apr))))
    build_silver(con)
    build_main_views(con)
    build_gold(con)
    calib = calibrate_dashboard(con)

    dq = run_checks(con)
    con.register("dq_df", dq)
    con.sql("CREATE OR REPLACE TABLE dq.dq_results AS SELECT * FROM dq_df")
    mt = metrics(con)
    con.register("metrics_df", mt)
    con.sql("CREATE OR REPLACE TABLE dq.dq_metrics AS SELECT * FROM metrics_df")

    export_parquet(con)
    con.close()

    print(f"raw API records landed      {len(records):>8,}")
    print(f"bronze claims (validated)   {clean:>8,}   quarantined at ingest: {rejected}")
    print(f"replay batch rows (Apr)     {calib['replay_rows']:>8,}   (simulated non-idempotent re-run)")
    print(f"source growth Mar->Apr      {calib['source_growth'] * 100:>7.2f}%")
    print(f"dashboard growth Mar->Apr   {calib['naive_growth'] * 100:>7.2f}%   <- the +40% the business sees")
    print(f"dq checks: {dict(dq.status.value_counts())}")


if __name__ == "__main__":
    main()
