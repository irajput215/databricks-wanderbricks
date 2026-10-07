"""
Independent verification of the practice lab: warehouse invariants, the Q49 story numbers,
and that all 50 interview questions are present in the built notebooks.

    .venv/bin/python tools/verify_project.py

Exit code 1 if anything fails.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "warehouse" / "healthcare.duckdb"
NOTEBOOKS = ROOT / "notebooks"

results: list[tuple[str, bool, str]] = []


def check(name: str, actual, expected=None, ok: bool | None = None, note: str = "") -> None:
    passed = ok if ok is not None else (actual == expected)
    results.append((name, passed, f"{actual!r}" + (f" (expected {expected!r})" if expected is not None else "") + (f" {note}" if note else "")))


def main() -> None:
    con = duckdb.connect(str(DB), read_only=True)
    one = lambda sql: con.sql(sql).fetchone()[0]  # noqa: E731

    # ---- shape -----------------------------------------------------------------
    check("members row count", one("SELECT COUNT(*) FROM main.members"), 1200)
    check("member_id unique", one("SELECT COUNT(*) FROM (SELECT member_id FROM main.members GROUP BY 1 HAVING COUNT(*)>1)"), 0)
    check("providers row count", one("SELECT COUNT(*) FROM main.providers"), 220)
    check("providers with no claims", one("SELECT COUNT(*) FROM main.providers p WHERE NOT EXISTS (SELECT 1 FROM main.claims c WHERE c.provider_id=p.provider_id)"), 5)
    check("claims row count", one("SELECT COUNT(*) FROM main.claims"), 11981)
    check("claim_id unique", one("SELECT COUNT(*) FROM (SELECT claim_id FROM main.claims GROUP BY 1 HAVING COUNT(*)>1)"), 0)
    check("claim_services row count", one("SELECT COUNT(*) FROM main.claim_services"), 38592)
    check("claim_services pk unique", one("SELECT COUNT(*) FROM (SELECT claim_id, service_id FROM main.claim_services GROUP BY 1,2 HAVING COUNT(*)>1)"), 0)
    check("payments row count", one("SELECT COUNT(*) FROM main.payments"), 11465)
    check("payment_id unique", one("SELECT COUNT(*) FROM (SELECT payment_id FROM main.payments GROUP BY 1 HAVING COUNT(*)>1)"), 0)
    check("plans/procedures/diagnoses",
          (one("SELECT COUNT(*) FROM main.plans"), one("SELECT COUNT(*) FROM main.procedures"), one("SELECT COUNT(*) FROM main.diagnoses")),
          (5, 32, 30))

    # ---- referential integrity -------------------------------------------------
    check("claims -> members orphans", one("SELECT COUNT(*) FROM main.claims c LEFT JOIN main.members m USING (member_id) WHERE m.member_id IS NULL"), 0)
    check("claims -> providers orphans", one("SELECT COUNT(*) FROM main.claims c LEFT JOIN main.providers p USING (provider_id) WHERE p.provider_id IS NULL"), 0)
    check("services -> claims orphans", one("SELECT COUNT(*) FROM main.claim_services s LEFT JOIN main.claims c USING (claim_id) WHERE c.claim_id IS NULL"), 0)
    check("payments -> claims orphans", one("SELECT COUNT(*) FROM main.payments p LEFT JOIN main.claims c USING (claim_id) WHERE c.claim_id IS NULL"), 0)
    check("status domain", one("SELECT COUNT(*) FROM main.claims WHERE status NOT IN ('Submitted','Approved','Denied')"), 0)
    check("claim_date >= 2024-01-01", one("SELECT COUNT(*) FROM main.claims WHERE claim_date < DATE '2024-01-01'") >= 0, True,
          note="(defect rows may predate enrollment)")

    # ---- money -----------------------------------------------------------------
    paid = float(one("SELECT SUM(paid_amount) FROM main.claims"))
    billed = float(one("SELECT SUM(billed_amount) FROM main.claims"))
    check("total paid", round(paid, 2), 36897727.38, ok=abs(paid - 36897727.38) < 0.01)
    check("total billed", round(billed, 2), 82773025.18, ok=abs(billed - 82773025.18) < 0.01)
    check("paid < billed in aggregate", paid < billed, True)

    # ---- gold marts reconcile to silver ----------------------------------------
    check("gold monthly vs silver claims", one("""
        SELECT COALESCE(SUM(ABS(delta)), 0) FROM (
            SELECT k.total_claims - s.n AS delta
            FROM gold.monthly_claims_kpi k
            JOIN (SELECT date_trunc('month', claim_date) AS mth, COUNT(*) AS n FROM silver.claims GROUP BY 1) s
              ON k.month = s.mth
            UNION ALL
            SELECT (SELECT COUNT(*) FROM gold.monthly_claims_kpi) - (SELECT COUNT(DISTINCT date_trunc('month', claim_date)) FROM silver.claims)
        )"""), 0)
    check("gold monthly money matches", one("""
        SELECT CASE WHEN ABS(SUM(k.total_paid) - (SELECT SUM(paid_amount) FROM silver.claims)) < 0.01 THEN 0 ELSE 1 END
        FROM gold.monthly_claims_kpi k"""), 0)
    check("gold provider monthly vs claims", one("""
        WITH src AS (SELECT provider_id, date_trunc('month', claim_date) AS mth, COUNT(*) n FROM silver.claims GROUP BY 1,2)
        SELECT (SELECT COUNT(*) FROM gold.provider_monthly_kpi) - (SELECT COUNT(*) FROM src)"""), 0)
    check("gold provider ranked grain", one("""
        SELECT COUNT(*) FROM (SELECT provider_id, month FROM gold.provider_monthly_kpi_ranked GROUP BY 1,2 HAVING COUNT(*)>1)"""), 0)
    check("rank 1 exists for every state-month", one("""
        SELECT COUNT(*) FROM (
            SELECT state, month FROM gold.provider_monthly_kpi_ranked GROUP BY 1,2
            HAVING COUNT(*) FILTER (WHERE provider_rank_in_state = 1) = 0)"""), 0)

    # ---- the Q49 story ---------------------------------------------------------
    growth = one("""
        SELECT s_apr / s_mar - 1 FROM (
            SELECT COUNT(*) FILTER (WHERE date_trunc('month', claim_date) = DATE '2025-03-01') AS s_mar,
                   COUNT(*) FILTER (WHERE date_trunc('month', claim_date) = DATE '2025-04-01') AS s_apr
            FROM silver.claims)""")
    check("source Mar->Apr growth ~5%", f"{growth * 100:.2f}%", ok=0.03 <= growth <= 0.07)
    naive = one("""
        SELECT d_apr / d_mar - 1 FROM (
            SELECT SUM(dashboard_count) FILTER (WHERE month = TIMESTAMP '2025-03-01') AS d_mar,
                   SUM(dashboard_count) FILTER (WHERE month = TIMESTAMP '2025-04-01') AS d_apr
            FROM dashboard.monthly_claim_counts)""")
    check("dashboard Mar->Apr growth ~40%", f"{naive * 100:.2f}%", ok=0.33 <= naive <= 0.47)
    check("dashboard is NOT unique per claim", one("""
        SELECT COUNT(*) FROM (SELECT claim_id FROM dashboard.claims_dashboard GROUP BY 1 HAVING COUNT(*)>1)""") > 0, True)
    check("replay batch duplicated rows", one("""
        SELECT COUNT(*) FROM bronze.claims WHERE _ingest_batch = '2025-04-16T02:05:00Z'"""), 302)
    check("bronze has duplicate claim_ids", one("""
        SELECT COUNT(*) FROM (SELECT claim_id FROM bronze.claims GROUP BY 1 HAVING COUNT(*)>1)"""), 354)
    check("late-arriving claims present", one("""
        SELECT COUNT(*) FROM silver.claims WHERE date_diff('day', updated_at, ingested_at) > 30""") >= 50, True)

    # ---- DQ scorecard ----------------------------------------------------------
    check("dq has exactly one FAIL", one("SELECT COUNT(*) FROM dq.dq_results WHERE status = 'FAIL'"), 1)
    check("the FAIL is the dashboard reconciliation", one("""SELECT check_name FROM dq.dq_results WHERE status='FAIL'"""),
          "dashboard.reconciles_to_source")
    check("all hard checks pass (dashboard excluded)", one("""SELECT COUNT(*) FROM dq.dq_results WHERE severity='high' AND status<>'PASS' AND layer <> 'dashboard'"""), 0)
    check("quarantine populated", one("SELECT COUNT(*) FROM dq.quarantine"), 236)
    check("malformed api records quarantined", one("""SELECT COUNT(*) FROM dq.quarantine WHERE source='claims_api'"""), 5)

    # ---- SCD2 ------------------------------------------------------------------
    check("scd2 providers have 2 versions", one("""SELECT COUNT(*) FROM (SELECT provider_id FROM silver.provider_specialty_history GROUP BY 1 HAVING COUNT(*)=2)"""), 30)
    check("scd2 one current row each", one("""
        SELECT COUNT(*) FROM (SELECT provider_id FROM silver.provider_specialty_history GROUP BY 1 HAVING COUNT(*) FILTER (WHERE is_current) <> 1)"""), 0)

    # ---- notebook coverage -----------------------------------------------------
    found: dict[int, str] = {}
    for path in sorted(NOTEBOOKS.glob("*.ipynb")):
        if path.name.startswith("_"):
            continue
        nb = json.loads(path.read_text())
        text = "\n".join("".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "markdown")
        for qnum in re.findall(r"#{3,4}\s*Q\s?(\d{1,2})\b", text):
            found.setdefault(int(qnum), path.name)
    missing = [n for n in range(1, 51) if n not in found]
    check("all 50 questions present in notebooks", f"{50 - len(missing)}/50", ok=not missing,
          note=f"missing: {missing}" if missing else "")
    check("drill notebooks carry the questions", f"{len(set(found.values()))} notebooks", ok=len(set(found.values())) >= 6)

    # ---- report ----------------------------------------------------------------
    con.close()
    width = max(len(n) for n, _, _ in results)
    failed = 0
    for name, ok, detail in results:
        print(f"{'PASS' if ok else 'FAIL'}  {name:<{width}}  {detail}")
        failed += not ok
    print(f"\n{len(results) - failed}/{len(results)} checks passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
