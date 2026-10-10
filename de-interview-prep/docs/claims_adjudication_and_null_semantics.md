# Q2 Deep Dive: Claims Adjudication & NULL Semantics in Aggregations

This document captures key data engineering notes and domain concepts discussed around **Q2 (Total paid amount)** in `01_sql_grain_and_aggregation.ipynb`.

---

## 1. Business Context & Problem Statement

### Q2 Metadata
- **Business question:** How much has the plan paid out in total?
- **Final grain:** Single row
- **Source tables:** `claims`
- **Source grains:** `claims = 1 row per claim_id`
- **Grain risk:** None (single table, no join, no fan-out)
- **Watch out:** `SUM` skips `NULL` `paid_amount` (1,160 claims are not adjudicated yet) — that is correct here, `NULL` is not zero.

---

## 2. What is Claim Adjudication?

In healthcare and insurance, **adjudication** is the process where the insurance company reviews a submitted claim and decides:
1. **Whether to pay it** (approved, denied, or partially approved).
2. **How much to pay** (the plan's share vs. what the member owes in deductible, copay, or coinsurance).

### Claim Lifecycle Diagram

```
1. Claim Submitted (e.g., Provider bills $500)
       │
       ▼
2. In Review / Pending Adjudication  ───► paid_amount IS NULL (not decided yet)
       │
       ▼
3. ADJUDICATION DECISION
       │
       ├─► Approved & Paid           ───► paid_amount = $400 (Plan pays $400, member pays $100)
       ├─► Denied                    ───► paid_amount = $0.00 (Plan pays $0)
       └─► Fully applied to deductible──► paid_amount = $0.00 (Member pays full bill)
```

### Data Engineering Representation

| Status | `paid_amount` Value | Meaning in the Data |
| :--- | :--- | :--- |
| **Not Adjudicated** (In processing) | `NULL` | **Unknown.** The decision hasn't been made yet. It is not zero; it just doesn't exist yet. |
| **Adjudicated & Denied** | `0.00` | **Known.** The decision is final, and the plan owes $0. |
| **Adjudicated & Approved** | `> 0.00` | **Known.** The decision is final, and the plan paid this amount. |

---

## 3. Why `SUM(paid_amount)` is Technically Correct vs. Why It Can Be Misleading

### Mathematical Behavior
In standard SQL, `SUM(paid_amount)` automatically ignores `NULL` values.
Notice that:
```sql
SUM(paid_amount) == SUM(COALESCE(paid_amount, 0))
```
Adding `$0` doesn't alter the arithmetic sum.

### Why it answers: *"How much has the plan paid out in total?"*
- The question is asking for **actual cash outflow / realized disbursements to date**.
- For an unadjudicated claim, the plan has disbursed **$0 so far**.
- Summing only the adjudicated claims accurately reflects the total dollars that have left the plan's bank account.

### Why "Total Paid" Can Still Be Misleading to Business Stakeholders
In real-world BI reporting, non-technical stakeholders often conflate:
1. **Cash paid to date** (realized payout so far).
2. **Ultimate incurred liability** (what the plan will eventually owe once all 1,160 pending claims finish processing).

If an executive uses a lone `total_paid` metric for forecasting or budgeting without realizing 1,160 claims are in-flight, they will dangerously underestimate actual plan costs.

---

## 4. The Critical Danger: `NULL` vs. `0` in Other Aggregations

While `SUM` produces the same total whether `NULL` is treated as 0 or ignored, other aggregations diverge significantly:

- **`AVG(paid_amount)`**:
  - `AVG(paid_amount)` ignores `NULL`s, computing the average payout across **settled/adjudicated** claims.
  - `AVG(COALESCE(paid_amount, 0))` forces pending claims to `$0`, artificially deflating the average payout.
- **`COUNT(paid_amount)` vs `COUNT(*)`**:
  - `COUNT(*)` counts all claims submitted.
  - `COUNT(paid_amount)` counts only claims that have reached final adjudication.

---

## 5. Production Best Practices for Data Engineers

To eliminate ambiguity in production pipelines and dashboards:

### Option A: Explicit Intentional Filtering
Make the filter explicit so downstream consumers know `NULL` handling was an intentional business rule:
```sql
SELECT ROUND(SUM(paid_amount), 2) AS total_paid
FROM claims
WHERE paid_amount IS NOT NULL; -- or WHERE claim_status = 'ADJUDICATED'
```

### Option B: Companion Completeness Metrics
Report the monetary total alongside adjudication pipeline counts:
```sql
SELECT 
    ROUND(SUM(paid_amount), 2)                         AS total_paid_to_date,
    COUNT(paid_amount)                                 AS adjudicated_claim_count,
    COUNT(*) - COUNT(paid_amount)                       AS pending_claim_count,
    ROUND(100.0 * COUNT(paid_amount) / COUNT(*), 1)       AS adjudication_rate_pct
FROM claims;
```
