# Work Experience & Interview Prep Alignment

The short answer is: **This `de-prep` lab covers ~80% of what you need—the technical mechanics, code, and healthcare domain logic match your resume almost 1:1.** 

However, **you still need targeted preparation for the specific "production war stories"** behind each bullet point. An interviewer will use this lab's concepts to test your coding, but they will grill your resume on *scale, architecture decisions, and cluster internals*.

Here is an exact breakdown of what `de-prep` covers vs. the specific talking points you must prepare individually.

---

### Part 1: Where `de-prep` Prepares You (The 80%)

Notice how the modules in this lab directly mirror your resume bullets:

| Your Resume Bullet | Matching `de-prep` Notebook / Asset | What It Gives You |
| :--- | :--- | :--- |
| *"Quarantine tables for invalid records, PySpark StructType schemas"* | **Notebook 09 & 10**, `SCHEMA.md` (Defects #5, #6, #14) | Real code showing how malformed records are rejected to `dq.quarantine` while clean records flow to silver. |
| *"Idempotent claims writes using partition-aware overwrites and MERGE"* | **Notebook 08** (`pyspark_incremental_scd2.ipynb`) | Hands-on PySpark Delta `MERGE` patterns, handling late-arriving claims, and deduping on `updated_at`. |
| *"Metrics for claim counts, paid amounts, and reconciliation exceptions"* | **Notebook 01, 03, 05, 10** | Domain definitions: cash paid vs liability, NULL adjudication handling, financial tie-back discrepancies. |
| *"Automated SQL reconciliation controls to validate record counts"* | **Notebook 10** (`q49_investigation.sql`) | The exact drill where a dashboard shows +40% growth but the source shows +5%, finding duplicate batches. |
| *"Medallion architecture on Databricks & Delta Lake"* | `src/build_warehouse.py`, Databricks CLI setup | Understanding how data moves cleanly from `raw` → `bronze` → `silver` → `gold`. |

When an interviewer asks you to **live-code SQL or PySpark**, or explain **healthcare claims grain (claim level vs service line level)**, this lab will make you look like a top-tier candidate.

---

### Part 2: What You Must Prepare Individually (The 20%)

A senior interviewer will take your resume points and dig deeper into **production scale and architecture tradeoffs**. `de-prep` uses a small warehouse (12k claims), so you must be ready to speak about the **3TB production reality**:

#### 1. The "8 hours to 27 minutes" story (The #1 question you will get)
You will be asked: *"Why was it taking 8 hours initially, and what exact knobs did you turn to reach 27 minutes?"*
* **The Root Causes to mention:**
  - **Data skew:** A few large providers or dates caused 90% of the executors to idle while one executor choked on a huge partition.
  - **Small file problem:** Downstream jobs reading millions of tiny 2MB files from S3 instead of compacted Delta files.
  - **Shuffles:** Large table-to-table joins without broadcast or proper bucketing.
* **The Fixes you applied:**
  - Partitioning strategy: Partitioned by `claim_year_month` (not daily, which causes too many partitions, and not member_id, which causes high cardinality).
  - Delta optimizations: Ran `OPTIMIZE ... ZORDER BY (member_id, claim_id)` to co-locate claims for member-level queries.
  - Broadcast joins on dimension tables (`plans`, `providers`, `diagnoses`).
  - Enabled Databricks AQE (Adaptive Query Execution) to dynamically coalesce shuffle partitions and handle skew joins.

#### 2. The Cluster & Infrastructure details
Be ready with exact numbers:
- **Cluster configuration:** *"We ran a multi-node cluster with 8–16 memory-optimized worker nodes (e.g., `r5.2xlarge` or Databricks equivalent) with auto-scaling enabled."*
- **S3 / Storage:** *"Raw files arrived as gzipped JSON/CSV from external clearinghouses, and we landed them in bronze as Delta Lake tables with Snappy compression."*

#### 3. The ICD Code Ingestion & FastAPI project
An interviewer might ask: *"Why build a FastAPI service for ICD codes instead of just loading a table?"*
- **Your answer:** *"Downstream clinical analytics and front-end claim validation microservices needed sub-second lookups for valid ICD-10 diagnosis codes during pre-adjudication checks, so we exposed the versioned code tables via a lightweight FastAPI service backed by cached S3/Delta data."*
- Explain annual releases: *"CMS releases updated ICD-10 codes every October, so our scraper automated fetching release files from CMS/CDC sites, validating code formats, and publishing new versions without downtime."*

#### 4. Orchestration & Failures
- What orchestrated your pipelines? (e.g., Databricks Workflows or Apache Airflow).
- What happened when a batch failed midway? (Explain **idempotency**: because you used Delta `MERGE` and atomic overwrites, re-running a failed job did not produce duplicate records).

---

### Recommended Game Plan

1. **Finish the `de-prep` Notebooks (Focus on 01, 03, 08, 09, 10):**
   - This cements your live coding, PySpark syntax, and healthcare SQL problem-solving.
2. **Draft a 2-minute "Story Script" for your 3 key achievements:**
   - **Story 1:** Performance Tuning (8 hrs → 27 mins, partitioning, Z-ORDER, AQE).
   - **Story 2:** Medallion + DQ Quarantine (StructType schema enforcement, invalid record quarantine, financial reconciliation).
   - **Story 3:** The ICD ingestion service (scraping, versioning, API serving).

If you do both, you won't just pass the coding rounds—you will defend every word on your resume with total confidence.