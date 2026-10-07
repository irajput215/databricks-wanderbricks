"""
Data-quality checks for the healthcare warehouse.

Three families:
  HARD  -> must be exactly 0 violations (blockers)
  SOFT  -> tolerated up to a documented threshold (PASS / WARN / FAIL)
  INFO  -> observability metrics, no pass/fail

    python src/dq_checks.py        # prints the scorecard
"""
from __future__ import annotations

import duckdb
import pandas as pd

from paths import DB_PATH

# (check_name, layer, severity, sql_returning_one_number, note)
HARD = [
    ("members.member_id_unique", "silver", "high",
     "SELECT COUNT(*) FROM (SELECT member_id FROM silver.members GROUP BY 1 HAVING COUNT(*) > 1)",
     "Grain of members must be one row per member."),
    ("claims.claim_id_unique", "silver", "high",
     "SELECT COUNT(*) FROM (SELECT claim_id FROM silver.claims GROUP BY 1 HAVING COUNT(*) > 1)",
     "Duplicate claim versions must be collapsed before publishing."),
    ("claims.member_fk_valid", "silver", "high",
     "SELECT COUNT(*) FROM silver.claims c LEFT JOIN silver.members m USING (member_id) WHERE m.member_id IS NULL",
     "Every claim must resolve to a member."),
    ("claims.provider_fk_valid", "silver", "high",
     "SELECT COUNT(*) FROM silver.claims c LEFT JOIN silver.providers p USING (provider_id) WHERE p.provider_id IS NULL",
     "Every claim must resolve to a provider."),
    ("claim_services.claim_fk_valid", "silver", "high",
     "SELECT COUNT(*) FROM silver.claim_services s LEFT JOIN silver.claims c USING (claim_id) WHERE c.claim_id IS NULL",
     "Orphan service lines must be quarantined, never published."),
    ("payments.claim_fk_valid", "silver", "high",
     "SELECT COUNT(*) FROM silver.payments p LEFT JOIN silver.claims c USING (claim_id) WHERE c.claim_id IS NULL",
     "Every payment must resolve to a claim."),
    ("claims.status_in_domain", "silver", "high",
     "SELECT COUNT(*) FROM silver.claims WHERE status NOT IN ('Submitted','Approved','Denied')",
     "status domain is Submitted / Approved / Denied after trimming and upper-casing."),
    ("claims.claim_date_not_null", "silver", "high",
     "SELECT COUNT(*) FROM silver.claims WHERE claim_date IS NULL", "claim_date is the reporting date."),
    ("gold.monthly_claims_kpi_grain", "gold", "high",
     "SELECT COUNT(*) FROM (SELECT month FROM gold.monthly_claims_kpi GROUP BY 1 HAVING COUNT(*) > 1)",
     "gold KPI table must be one row per month."),
    ("gold.provider_monthly_kpi_grain", "gold", "high",
     "SELECT COUNT(*) FROM (SELECT provider_id, month FROM gold.provider_monthly_kpi GROUP BY 1, 2 HAVING COUNT(*) > 1)",
     "gold provider KPI table must be one row per provider per month."),
]

# (check_name, layer, severity, sql, warn_threshold, note)
SOFT = [
    ("claims.paid_amount_negative", "silver", "medium",
     "SELECT COUNT(*) FROM silver.claims WHERE paid_amount < 0", 20,
     "Negative paid amounts are claim reversals: legitimate but must be explained."),
    ("claims.paid_exceeds_billed", "silver", "medium",
     "SELECT COUNT(*) FROM silver.claims WHERE paid_amount > billed_amount", 20,
     "Paid cannot normally exceed billed; usually a source or adjustment defect."),
    ("claims.billed_vs_service_lines", "silver", "medium",
     "SELECT COUNT(*) FROM (SELECT c.claim_id FROM silver.claims c JOIN silver.claim_services s USING (claim_id) "
     "GROUP BY 1 HAVING ABS(MAX(c.billed_amount) - SUM(s.service_amount)) > 0.01)", 30,
     "Claim billed amount should equal the sum of its service lines."),
    ("claims.before_enrollment", "silver", "medium",
     "SELECT COUNT(*) FROM silver.claims c JOIN silver.members m USING (member_id) WHERE c.claim_date < m.enrollment_date", 20,
     "A claim dated before enrollment usually means a member-id reuse or a date defect."),
    ("payments.reconciled_to_claims", "silver", "medium",
     "SELECT COUNT(*) FROM (SELECT c.claim_id, c.paid_amount, COALESCE(SUM(p.payment_amount), 0) paid "
     "FROM silver.claims c LEFT JOIN silver.payments p ON p.claim_id = c.claim_id AND p.payment_status = 'Paid' "
     "WHERE c.paid_amount IS NOT NULL AND c.paid_amount > 0 GROUP BY 1, 2 "
     "HAVING ABS(c.paid_amount - COALESCE(SUM(p.payment_amount), 0)) > 0.01)", 5000,
     "Paid claims whose payment register does not tie back to claims.paid_amount."),
    ("claims.paid_amount_null_approved", "silver", "low",
     "SELECT COUNT(*) FROM silver.claims WHERE status = 'Approved' AND paid_amount IS NULL", 5000,
     "Approved but not yet paid: pending payment run."),
    ("claims.submitted_with_paid_amount", "silver", "medium",
     "SELECT COUNT(*) FROM silver.claims WHERE status = 'Submitted' AND paid_amount > 0", 50,
     "A pending claim should not carry a paid amount; usually a status/amount update race."),
]

INFO = [
    ("rows.raw_api_records", "SELECT COUNT(*) FROM raw.claims_api_records", "rows landed from the API pages"),
    ("rows.bronze_claims", "SELECT COUNT(*) FROM bronze.claims", "as-landed claims incl. replay batch"),
    ("rows.bronze_claims_duplicate_ids", "SELECT COUNT(*) FROM (SELECT claim_id FROM bronze.claims GROUP BY 1 HAVING COUNT(*) > 1)",
     "claim_ids loaded more than once (idempotency gap)"),
    ("rows.silver_claims", "SELECT COUNT(*) FROM silver.claims", "published claims"),
    ("rows.silver_claim_services", "SELECT COUNT(*) FROM silver.claim_services", "published service lines"),
    ("rows.silver_payments", "SELECT COUNT(*) FROM silver.payments", "published payments"),
    ("quarantine.records", "SELECT COUNT(*) FROM dq.quarantine", "records rejected during ingestion"),
    ("freshness.bronze_claims_max_loaded_at", "SELECT MAX(_loaded_at) FROM bronze.claims", "latest load timestamp"),
]


def run_checks(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    rows = []
    for name, layer, sev, sql, note in HARD:
        actual = con.sql(sql).fetchone()[0]
        rows.append(dict(check_name=name, layer=layer, severity=sev, actual=actual,
                         threshold=0, status="PASS" if actual == 0 else "FAIL", note=note))
    for name, layer, sev, sql, warn, note in SOFT:
        actual = con.sql(sql).fetchone()[0]
        status = "PASS" if actual == 0 else ("WARN" if actual <= warn else "FAIL")
        rows.append(dict(check_name=name, layer=layer, severity=sev, actual=actual,
                         threshold=warn, status=status, note=note))
    # dashboard vs source reconciliation is the deliberate failure the investigation notebook solves
    diff_pct = con.sql("""
        SELECT ABS(dashboard_count - source_count) * 100.0 / NULLIF(source_count, 0)
        FROM dashboard.monthly_claim_counts d
        JOIN (SELECT month, COUNT(*) source_count FROM silver.claims GROUP BY 1) s USING (month)
        ORDER BY ABS(dashboard_count - source_count) DESC LIMIT 1
    """).fetchone()[0] or 0
    rows.append(dict(check_name="dashboard.reconciles_to_source", layer="dashboard", severity="high",
                     actual=round(float(diff_pct), 2), threshold=1.0,
                     status="PASS" if diff_pct <= 1.0 else "FAIL",
                     note="Dashboard monthly claim counts must match the published claims table."))
    return pd.DataFrame(rows)


def metrics(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return pd.DataFrame(
        [dict(metric=n, value=str(con.sql(s).fetchone()[0]), description=d) for n, s, d in INFO]
    ).sort_values("metric")


def main() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = run_checks(con)
    pd.set_option("display.width", 200)
    print(df.to_string(index=False))
    print()
    print(metrics(con).to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
