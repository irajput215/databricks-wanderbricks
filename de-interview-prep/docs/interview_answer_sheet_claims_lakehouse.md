# Interview Answer Sheet: Claims Lakehouse Projects

Prepared for a mid-level Data Engineer interview. Two resume projects on one claims dataset at Carelon (2021 to 2023).

---

## 0. Ground Rules

- **Only say what you can defend.** Exact dollar splits and exact weekly hours in earlier drafts were illustrative. Use ranges ('on the order of', 'roughly') unless you have the real figure.
- **Say 'my team' where it is true.** Name what you personally built, and what the lead architect or platform team owned (cluster sizing, enterprise IAM).
- **Before the interview, mark each number below as Real or Estimate** and speak accordingly.

---

## 1. Numbers Sheet (Single Source of Truth)

| Item | Value to use | Notes |
| :--- | :--- | :--- |
| **History** | ~36 months rolling, ~3 TB Delta/Parquet | Analytical lakehouse footprint |
| **`claims` (header)** | ~350M rows | One row per claim header |
| **`claim_services` (lines)** | ~1.2B rows, ~3 to 4 lines per claim | One row per service/procedure line |
| **`payments`** | ~320M rows | Financial transactions |
| **`members` / `providers` / `reference codes`** | ~25M / ~1.5M / ~150K rows | Master dimensions |
| **Daily batch** | ~350K headers and ~1.2M service lines | ~15 to 25 GB raw delta |
| **Check** | 350K $\times$ ~1,080 days = ~378M headers | Mathematically matches history |
| **Monthly provider disbursement** | roughly $25M to $35M | Illustrative; do NOT quote a daily dollar total |
| **Arrival** | Clearinghouse 837 EDI converted upstream to compressed JSON/CSV | Landed in ADLS Gen2 |
| **Runtime** | Databricks Runtime 10.4 LTS or 11.3 LTS | Historical production reality |
| **Layout** | Partition by `service_year_month`, Z-ORDER by `provider_id` | Proven indexing pattern |
| **Team** | 6-person pod: 1 lead architect, 3 DEs, 1 QA/DQ engineer, 1 Scrum Master/PO | Pod structure |

> **Resume wording fix:** The resume says '1M+ daily claim batches'. Your numbers say ~350K claims and ~1.2M service lines per day. Say **'1M+ daily claim-line records'** (or change the resume) so you are not caught between 350K and 1M.

---

## 2. How the Two Projects Relate (Answer to "What is different?")

> *"Project 2 was the foundation: incremental ingestion from ADLS, explicit schemas, a basic quarantine path, idempotent writes and shared metric definitions. Project 1 came as volume grew to ~3 TB. It focused on performance at scale, grain-safe modeling, a financial reconciliation ledger, observability and governance."*

- **Quarantine:** Project 2 built the basic valid/invalid split with reason codes. Project 1 extended it with the ledger so quarantined dollars are accounted for.
- **Idempotency:** Project 2 used partition overwrite and MERGE. Project 1 added deterministic dedup and Repair-and-Rerun safety.
- **Medallion:** Shared foundation, not a separate build.

---

## 3. Architecture (What to Draw on a Whiteboard)

```text
[Clearinghouse 837 EDI] 
        │ (Upstream conversion)
        ▼
1. Land: Compressed JSON/CSV in ADLS Gen2 (Triggered by ADF)
        │
        ▼
2. Bronze: Auto Loader in Directory Listing mode (Checkpointed)
        │  Raw, immutable Snappy Parquet (_ingest_batch_id, _ingest_timestamp)
        ▼
3. Contract Validation: Explicit StructType schema parsing
        │
        ├──► Invalid Rows ──► Quarantine Table (Reason codes: ERR_NULL_PK, ERR_INVALID_CODE)
        ▼ Clean Rows
4. Silver (Delta Lake):
        │  Deterministic Dedup (row_number over claim_id, updated_at DESC, tie-breaker)
        │  MERGE INTO silver.claims ON (claim_id, service_year_month)
        ▼
5. Gold (Delta Lake): Grain-Isolated Aggregations
        │  Service lines aggregated to claim_id BEFORE joining headers
        ▼
6. Reconciliation Ledger: 3-way balance
        │  Source Gross $ = Curated Target $ + Quarantined $ + Deduplicated $
        ▼
7. DQ Gatekeeper: Evaluates 9 telemetry metrics
        │  Alerts via Slack/PagerDuty, gates Power BI dashboard semantic model refresh
```

* **Workflows DAG Order:** `Ingest` $\to$ `Contract Validation & Quarantine` $\to$ `Silver Merge` $\to$ `Gold Aggregation` $\to$ `Reconciliation Ledger` $\to$ `DQ Gatekeeper`.
* *Why Gatekeeper runs last:* Because the financial-variance metric depends on the reconciliation ledger completing.
* **Parquet vs. Delta:** Bronze is plain Snappy Parquet. Silver and Gold are Delta Lake (Parquet data files + `_delta_log`), which makes atomic `MERGE` and schema enforcement possible.

---

## 4. Model Answers to the Hardest Questions

### 8 Hours to 27 Minutes (94% Speedup)
* **The Arithmetic:** 480 min to 27 min is a 94.4% reduction (~17.8x faster). This was the daily batch merge and reconciliation job.
* **Your Answer:**
  > *"The baseline was a daily job that kept re-reading historical data. It spilled hundreds of GB in shuffles, had lots of tiny files, and three large hospital networks caused one task to run for hours. I tackled it in three steps:
  >
  > 1. **Incremental watermark loads** took it from about 8 hours to about 1.5 hours. This was the biggest win, because we stopped reprocessing history.
  > 2. **Partition pruning and Z-ORDER on provider_id** let Spark skip most historical files in the joins and merge.
  > 3. **Broadcast joins and AQE** (skew join handling, partition coalescing) removed the hot-provider straggler and avoidable shuffles. Steps 2 and 3 took the remaining 1.5 hours to about 27 minutes."*
* **Be ready for follow-ups:**
  * *Did cluster change?* Yes, we moved from a fixed 8-node `Standard_D16s_v4` cluster to an autoscaling 2-to-6 node `Standard_E16s_v4` (Memory-Optimized) cluster.
  * *Cost impact:* Shorter runtimes on autoscaling job clusters significantly lowered total DBU costs.

### "You say you eliminated shuffles. Which remain?"
> *"I eliminated the avoidable shuffles: the joins from the large claims fact tables to small dimension tables (providers, reference codes, plans) became BroadcastHashJoins. Wide operations such as the deduplication window, group-by aggregations, and the large claim-to-claim merge still shuffle. AQE reduced their cost by coalescing post-shuffle partitions and splitting skewed ones dynamically."*

### 55% Faster Ingestion (Project 2)
* **Baseline:** Bronze ingestion, ~45 min, scanning full directory trees and parsing ~50 string columns.
* **After:** ~20 min (55.6% speedup).
* **Drivers:** Incremental watermark/file tracking, Parquet downstream, and selecting only the ~15 needed business columns early.
> *"Column pruning is automatic on Parquet, so what I did was select needed columns early and define explicit schemas. On the raw JSON/CSV landing files the parse cost is still paid, so most of the I/O saving showed up downstream in Silver. I confirmed projection pushdown in the physical plan with `ReadSchema` and `PushedFilters`."*

### 35% Less Review Time
> *"Reviewers used to work through error spreadsheets after failed batch crashes. Once bad rows were routed to a quarantine table with structured reason codes (null key, invalid ICD/CPT code, negative amount, future service date, schema mismatch), they could triage by error type and only review actionable exceptions. It cut review effort by roughly a third."*
>
> *(Note: Do NOT say "35% of exactly 15 to 18 hours". Clarify that reviewers were claims operations / coding audit staff).*

### 75% Lower MTTD
* **Baseline:** Roughly 4 hours to detect. (The batch finished around 2:30 AM, but issues only surfaced when business analysts opened dashboards around 8:30 AM).
* **After:** Automated DQ Gatekeeper checks run right after the batch and alert on-call engineers within about an hour (sub-60 min).
* **Math:** $\frac{240 - 60}{240} = \mathbf{75\% \text{ reduction}}$.

### Idempotency and "MERGE Committed but a Downstream Task Failed"
> *"Every write is safe to repeat. The source batch is deduplicated with `row_number` over the claim key, ordered by `updated_at` with a deterministic tie-breaker (such as the ingest file name and row offset). MERGE matches on the business key, updating only when `source.updated_at > target.updated_at`.*
> 
> *On Repair-and-Rerun, Databricks Workflows re-runs only the failed task and its dependents; the already-committed MERGE is not repeated. Even if it were re-executed, matching on the key with the timestamp check makes the result identical, so payments are never duplicated."*
* *Why deterministic tie-breaker matters:* If two duplicate rows tie on the timestamp and the winner is non-deterministic, a rerun could pick a different row and mutate the target table.

### Partition Overwrite vs. MERGE
* **Overwrite (`replaceWhere` or dynamic partition overwrite):** Best for full historical slice reloads or immutable daily partitions. *Risk:* A malformed predicate can drop more data than intended.
* **MERGE:** Best for fine-grained claim-level upserts and status corrections. *Risk:* Multiple source rows matching one target row will cause the merge to fail with `Cannot perform Merge as multiple source rows matched`, which is why source deduplication is mandatory before merging.

### Grain and Fan-Out
* **Header grain:** 1 row per claim (`claim_id`).
* **Line grain:** 1 row per claim procedure (`claim_id` + `line_number`).
* **The Rule:** Approved payment amounts stored at the header level must **never** be summed after joining directly to lines. Always roll lines up to the claim level first, then join.
* **How to test:** Uniqueness assertions on grain keys (`assert_grain(df, "claim_id")`) and reconciliation checks ensuring Gold aggregate totals equal Silver header totals.

---

## 5. The Four Stories (Safe, Defensible Wording)

### A. Fan-Out Bug
> *"While validating a draft Power BI provider-disbursement report in staging, an analyst had joined `claims` directly to `claim_services` and summed the header payment. A claim with four lines was counted four times, and totals came out roughly 2.5 to 3 times too high. Finance flagged the discrepancy before release.*
> 
> *I traced it to the one-to-many join, then rebuilt the models so lines are aggregated to claim level before the join, and added grain-uniqueness assertions to prevent regression."*

### B. Reconciliation Variance
> *"In a daily batch of roughly 350K claims, we saw a variance of several thousand dollars between landing and the curated table. The ledger balanced it: `Source Gross $ = Curated $ + Quarantined $ + Deduplicated $`.*
> 
> *Part was quarantined claims (invalid codes, negative amounts) and the rest was exact duplicate resubmissions from a re-sent clearinghouse file. The unexplained variance was zero, so finance could certify the load."*

### C. Metric Definition Disagreement
> *"Finance counted a paid claim when the payment cleared the bank (`payment_status = 'CLEARED'`). Operations counted it at adjudication approval (`adjudication_status = 'APPROVED'`). Their monthly counts never matched.*
> 
> *We agreed on two named metrics defined once in shared semantic logic: `incurred_claims_payable` for finance and `adjudicated_claim_throughput` for operations, both derived from the same pipeline."*

### D. Production Incident (Schema Change)
> *"An upstream vendor changed provider tax ID from integer to alphanumeric with leading zeros. The nightly merge failed. Delta's ACID commit meant no partial data was left behind.*
> 
> *I checked the last good version with time travel, then fixed it by casting `provider_tax_id` to `STRING` explicitly at Bronze parsing (which also preserved leading zeros), and used Repair-and-Rerun to re-run only the failed task and its dependents. SLA was restored in about 35 minutes."*

---

## 6. Specifics Checklist

* **3-Way Reconciliation Formula:**
  $$\mathbf{\text{Source Gross \$}} = \mathbf{\text{Curated Target \$}} + \mathbf{\text{Quarantined \$}} + \mathbf{\text{Deduplicated \$}}$$
* **9 Monitoring Metrics:**
  1. Row-count anomaly
  2. Null-rate spike on primary/foreign keys
  3. Duplicate ratio
  4. Freshness / SLA lag
  5. Schema drift
  6. Financial variance
  7. Quarantine rejection rate
  8. Shuffle spill (GB)
  9. Run-duration drift
* **Quarantine Table Schema & Codes:**
  * *Schema:* `quarantine_id`, `source_system`, `payload_raw`, `rejection_code`, `rejection_reason`, `quarantined_at`, `batch_id`, `status`.
  * *Codes:* `ERR_NULL_PK`, `ERR_INVALID_CODE`, `ERR_FUTURE_SERVICE_DATE`, `ERR_NEGATIVE_AMOUNT`, `ERR_SCHEMA_MISMATCH`.
* **MERGE Keys:**
  * `claims`: `target.claim_id = source.claim_id AND target.service_year_month = source.service_year_month` (with `source.updated_at > target.updated_at`).
  * `claim_services`: `target.claim_id = source.claim_id AND target.line_number = source.line_number`.
* **Governance and the 40+ Views:**
  * At Carelon, group-based dynamic views using `is_account_group_member()` in `CASE` expressions masked PHI (SSN, DOB) and filtered provider data per tenant. 
  * The 40+ near-duplicate views were consolidated into a small set of parameterized governed views on shared base tables.
  * *"Today in modern Unity Catalog, I recommend native Row Filters and Column Masks directly on the base table to avoid view nesting overhead."*

---

## 7. Corrections to Avoid

* **Liquid Clustering and Runtime 13.3:** Not part of your 2021 to 2023 production work. Say you used partitioning plus Z-ORDER, and that you know Liquid Clustering from later study.
* **Deletion Vectors and Native Row Filters/Column Masks:** Same—mention only as 'what I would do now'.
* **Auto Loader:** Use one mode: **Directory Listing**.
* **Seniority:** Present as a **strong mid-level engineer** with ownership beyond your years, not 'senior'.

---

## 8. Code Snippets to Practice Writing from Memory

### Deterministic Deduplication Window
```python
from pyspark.sql import functions as F, Window

w = Window.partitionBy('claim_id').orderBy(
    F.col('updated_at').desc(),
    F.col('_source_file').desc(),
    F.col('_row_offset').desc()
)

dedup = (
    batch.withColumn('rn', F.row_number().over(w))
         .filter('rn = 1')
         .drop('rn')
)
```

### Delta Lake MERGE with Partition Pruning
```sql
MERGE INTO silver.claims t
USING dedup s
ON t.claim_id = s.claim_id 
   AND t.service_year_month = s.service_year_month
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

---

## 9. Weak Areas (How to Answer JVM/JAR Questions)

> *"My focus has been modern cloud data engineering in PySpark, SQL, and Databricks. While I know Spark runs on the JVM and uses JDBC driver JARs when connecting to external databases like SQL Server or Oracle, our platform and DevOps teams standardized cluster base images and library coordinates, so I focused on building reliable medallion pipelines, data modeling, and performance optimization."*
