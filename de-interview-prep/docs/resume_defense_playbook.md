# Complete Senior Data Engineer Resume Defense Playbook
### Master Edition: Defending Every Experience Bullet & Project on Your Resume

This playbook is your definitive preparation guide for Senior & Staff Data Engineer interviews. It covers **100% of the roles, bullets, and projects on your resume**, providing:
1. **The 60-Second Executive Pitch** (Conversational, confident, natural).
2. **Under the Hood: Technical Architecture, Cluster Knobs & Tradeoffs**.
3. **The Exact Code / Implementation Snippets** (SQL, PySpark, dbt, LangGraph).
4. **The Grilling: 3–5 Brutal Follow-Up Questions Asked by Bar Raisers & Model Answers**.
5. **Common Traps & Junior Red Flags to Avoid**.

---

## Master Resume Architecture Map

```text
 ┌────────────────────────────────────────────────────────────────────────┐
 │                      YOUR EXPERIENCE & PROJECTS                        │
 ├────────────────────────────────────────────────────────────────────────┤
 │ PART 1: Carelon Global Solutions — Associate Data Engineer III         │
 │   • Story 1: 3 TB Workload Optimization (8 Hours to 27 Minutes)        │
 │   • Story 2: Unity Catalog Governance (Row Filters & Dynamic Masks)    │
 │   • Story 3: Grain Isolation & The Fan-Out Trap ($25M+ Saved)          │
 │   • Story 4: 3-Way Reconciliation Ledger ($10,000 Variance Audit)      │
 │   • Story 5: 100% Idempotent Retries & Workflows Repair-and-Rerun      │
 │   • Story 6: Pipeline Observability Across 9 Core Monitoring Metrics   │
 ├────────────────────────────────────────────────────────────────────────┤
 │ PART 2: Carelon Global Solutions — Associate Data Engineer II & I      │
 │   • Story 7: S3 + PySpark Medallion Ingestion (-55% Processing Time)   │
 │   • Story 8: StructType Schema Enforcement & Clinical Quarantine (-35%)│
 │   • Story 9: Cross-Functional Metric Alignment & Null Semantics        │
 │   • Story 10: Versioned ICD-10 Code Scraper & FastAPI Search Microservice│
 ├────────────────────────────────────────────────────────────────────────┤
 │ PART 3: Project 1 — Zomato AI Data Platform (Snowflake + dbt)          │
 │   • Story 11: Snowflake Kimball Star Schema Modeling (10M+ Records)    │
 │   • Story 12: dbt Incremental Merge & SCD Type 2 Snapshots             │
 │   • Story 13: Dockerized Airflow DAGs & GitHub Actions CI/CD Makefile  │
 │   • Story 14: Streamlit Analytics & LiteLLM Text-to-SQL Guardrails     │
 ├────────────────────────────────────────────────────────────────────────┤
 │ PART 4: Project 2 — Agentic Incident-Response Platform (LangGraph)     │
 │   • Story 15: Autonomous Pipeline Triage in Under 20 Seconds           │
 │   • Story 16: The 9 Forensic SRE Diagnostic Tools                      │
 │   • Story 17: Sandboxed Read-Only sqlglot AST Parsing & HITL Approval  │
 │   • Story 18: Production LangSmith Observability & Token Optimization  │
 └────────────────────────────────────────────────────────────────────────┘
```

---

# PART 1: Carelon Global Solutions — Associate Data Engineer III

---

## War Story 1: The 3 TB Performance Tuning (8 Hours to 27 Minutes)

### The Resume Bullet:
> *"Optimized a 3 TB daily healthcare claims workload on Databricks, applying partitioning and Z-ORDER on `(service_date, provider_id)`, broadcast joins and AQE to eliminate cluster shuffles, cutting batch runtime from 8 hours to 27 minutes (94% speedup)."*

### 1. The 60-Second Executive Pitch
> *"When I stepped in, our daily batch pipeline took 8 hours to process 3 TB of incoming claims, frequently missing our 6:00 AM SLA for downstream reporting. 
> 
> Inspecting the Spark UI revealed three glaring bottlenecks: over **400 GB of shuffle spill to disk**, severe **data skew** caused by three massive hospital networks that choked single executors for over 2.5 hours, and thousands of tiny uncompacted 2 MB parquet files causing S3 throttling.
> 
> I re-architected the layout: replaced static directory partitioning with **Liquid Clustering / Z-ORDER** on `(service_date, provider_id)`, broadcast small dimension tables (`providers`, `plans`) to eliminate cluster shuffle exchanges, enabled Adaptive Query Execution (AQE) with skew join mitigation, and scheduled compaction via Delta `OPTIMIZE`. This reduced execution time from 8 hours to 27 minutes—a 94% reduction—while cutting daily DBU cloud spend by 65%."*

### 2. Under the Hood & Cluster Knobs
* **Cluster:** Driver: 1x `r5.2xlarge` (8 cores, 64 GB). Workers: 8–16 auto-scaling `r5.4xlarge` (16 cores, 128 GB RAM each, NVMe SSDs).
* **The Fixes:**
  1. `spark.sql.adaptive.enabled = true` & `spark.sql.adaptive.skewJoin.enabled = true`: Dynamically splits 15 GB skewed partitions into smaller sub-partitions.
  2. `spark.sql.autoBroadcastJoinThreshold = 67108864` (64 MB): Forces `BroadcastHashJoinExec`, avoiding shuffle exchanges.
  3. `OPTIMIZE silver.claims ZORDER BY (service_date, provider_id)`: Co-locates multi-dimensional data within parquet files to maximize file skipping.

### 3. The Follow-Up Grilling
* **Q: "Why Z-ORDER / Liquid Clustering over standard Hive partitioning by date?"**
  * *Answer:* *"Partitioning by `date/provider_id` creates over 180,000 subdirectories, causing the small file problem and overwhelming S3 metadata. Liquid Clustering / Z-ORDER maps multi-dimensional data using space-filling Hilbert curves within file blocks, keeping files ~512 MB, enabling data skipping without creating thousands of directories."*
* **Q: "How did you spot shuffle spill in Spark UI?"**
  * *Answer:* *"In the Stage Details tab, I sorted tasks by runtime and checked Spill (Memory) and Spill (Disk). When memory spill exceeded executor RAM, Spark serialized partitions to NVMe disk. I tuned `spark.sql.shuffle.partitions` dynamically via AQE and pruned bulky raw JSON strings before shuffling."*

---

## War Story 2: Unity Catalog Governance (Row Filters & Dynamic Column Masks)

### The Resume Bullet:
> *"Enforced Lakehouse governance with Databricks Unity Catalog, using access grants and dynamic views to mask HIPAA/PHI fields (SSN, DOB) and isolate multi-tenant provider data, consolidating 40+ duplicated SQL views."*

### 1. The 60-Second Executive Pitch
> *"Our platform suffered from severe **view sprawl**. Every time a clinical or billing team requested access, engineers created custom SQL views like `vw_claims_florida` or `vw_claims_masked_ssn`. We accumulated over 40 duplicate views, permissions drifted constantly, and schema updates broke downstream reports.
> 
> I consolidated access onto Databricks **Unity Catalog** using a 3-level namespace (`catalog.schema.table`). To satisfy HIPAA's Minimum Necessary Rule, I implemented **Dynamic Column Masks** that check `is_account_group_member()`: compliance auditors see plaintext SSN/DOB, claims adjudicators see last 4 digits (`***-**-1234`), and general BI users see asterisks. 
> 
> Simultaneously, I attached **Row Filters** directly to the Delta tables to restrict clinicians to their specific `provider_id`. This deprecated all 40+ views, centralized access into version-controlled SQL functions, and automated our SOC 2 and HIPAA audit logs."*

### 2. The Code You Implemented
```sql
-- Dynamic Masking Function
CREATE OR REPLACE FUNCTION prod.security.mask_ssn_fn(ssn_val STRING)
RETURN CASE 
  WHEN IS_ACCOUNT_GROUP_MEMBER('compliance_officers') THEN ssn_val
  WHEN IS_ACCOUNT_GROUP_MEMBER('claims_adjudicators') THEN '***-**-' || RIGHT(ssn_val, 4)
  ELSE '***-**-****'
END;

ALTER TABLE prod.silver.members ALTER COLUMN ssn SET MASK prod.security.mask_ssn_fn;

-- Row Filter Function
CREATE OR REPLACE FUNCTION prod.security.provider_filter(provider_id_val STRING)
RETURN IS_ACCOUNT_GROUP_MEMBER('claims_admin') OR CURRENT_USER() = provider_id_val || '@healthcare.com';

ALTER TABLE prod.silver.claims SET ROW FILTER prod.security.provider_filter ON (provider_id);
```

### 3. The Follow-Up Grilling
* **Q: "Does Dynamic Column Masking slow down queries?"**
  * *Answer:* *"No physical table rewrite or disk I/O penalty occurs. The Catalyst optimizer compiles the masking function into an inline scalar projection (`ProjectExec`) in memory. It adds less than 1% CPU overhead while eliminating the cost of scanning separate view tables."*
* **Q: "How do you fulfill GDPR Right to be Forgotten on immutable Parquet?"**
  * *Answer:* *"A standard `DELETE` updates the Delta log, but historical parquet files still hold deleted data for Time Travel. To achieve physical erasure, we run the `DELETE`, set table retention to 0 hours (`delta.deletedFileRetentionDuration = 0 hours`), and run `VACUUM table RETAIN 0 HOURS` to permanently wipe old parquet files from S3."*

---

## War Story 3: Grain Isolation & The Fan-Out Trap ($25M+ Saved)

### The Resume Bullet:
> *"Engineered grain-isolated aggregation models separating claim headers from claim service lines, preventing multi-counting fan-out bugs in reporting on \$25M+ in monthly provider disbursements."*

### 1. The 60-Second Executive Pitch
> *"In health insurance, the biggest financial data risk is grain mismatch between **claim headers** and **claim service lines**. A single claim header contains the final adjudicated payment amount, but it links to multiple procedure lines in a 1-to-many relationship.
> 
> In legacy provider reports, an analyst joined the claims table directly to service lines and executed `SUM(paid_amount)`. An emergency visit claim with a \$5,000 paid amount and 6 procedure lines was summed 6 times, turning a \$5,000 payment into \$30,000 of phantom money! Across our network, this artificially inflated provider reporting by over \$25M.
> 
> I rebuilt our data architecture to enforce **strict grain isolation**: header metrics (`paid_amount`, claim counts) are strictly calculated at the header grain, while procedure volumes and units are aggregated at the line grain. If a report requires both, line items are pre-aggregated to the header grain before joining, enforced via automated `assert_grain()` unit tests in our CI/CD pipelines."*

### 2. The Code: The Bug vs. Grain-Safe Pre-Aggregation
```sql
-- FATAL BUG: Summing header paid_amount after joining lines multiplies money!
-- SELECT c.provider_id, SUM(c.paid_amount) FROM claims c JOIN claim_services cs ...

-- CORRECT: Pre-aggregate lines before joining
WITH line_summary AS (
    SELECT claim_id, COUNT(service_id) AS proc_count, SUM(service_amount) AS line_billed
    FROM silver.claim_services GROUP BY claim_id
)
SELECT 
    c.provider_id,
    COUNT(c.claim_id) AS total_claims,
    SUM(c.paid_amount) AS total_paid_amount, -- 100% ACCURATE
    SUM(ls.proc_count) AS total_procedures
FROM silver.claims c
LEFT JOIN line_summary ls ON c.claim_id = ls.claim_id
GROUP BY c.provider_id;
```

---

## War Story 4: 3-Way Reconciliation Ledger ($10,000 Variance)

### The Resume Bullet:
> *"Developed an automated 3-way financial reconciliation ledger and quarantine framework for 1M+ daily claim batches, tracing a \$10,000 variance to its source and isolating 2,800+ schema and primary-key violations for review."*

### 1. The 60-Second Executive Pitch
> *"Our pipeline ingests approximately 1,000,000 medical claim records daily from clearinghouses. In financial data engineering, every dollar entering the Bronze landing zone must tie back to the penny in Gold reporting.
> 
> Our automated reconciliation gatekeeper flagged a **\$10,000 gross variance**: source ingested claims totaled \$12,500,000, but our target curated table reported \$12,490,000. 
> 
> Rather than assuming records were lost, our automated 3-way reconciliation ledger localized the discrepancy immediately: \$2,400 came from 300 records with unparseable non-numeric amounts that were safely routed to our Quarantine table, and \$7,600 came from 10 duplicate claim submissions that were rightfully pruned by our window deduplication engine. 
> 
> Adding quarantined dollars and pruned duplicate dollars to the target total yielded a net unexplained variance of **\$0.00**. We published an auditable reconciliation certificate and alerted the upstream clearinghouse to re-submit the 300 malformed claims."*

### 2. Under the Hood: The Reconciliation Math
```text
Source Ingested Gross Dollars:                   $12,500,000.00
  LESS: Quarantined Malformed Amounts (300 rows):    -$2,400.00
  LESS: Deduplicated Duplicate Claims (10 rows):     -$7,600.00
---------------------------------------------------------------
Expected Target Curated Dollars:                 $12,490,000.00
Actual Target Curated Dollars:                   $12,490,000.00
---------------------------------------------------------------
NET UNEXPLAINED VARIANCE:                                 $0.00  ==> RECONCILED!
```

---

## War Story 5: 100% Idempotent Retries & Databricks Workflows Repair-and-Rerun

### The Resume Bullet:
> *"Designed idempotent pipeline retries using deterministic PySpark window deduplication and Delta Lake `MERGE INTO`, leveraging Databricks Workflows Repair-and-Rerun to recover from mid-flight failures without duplicating payments."*

### 1. The 60-Second Executive Pitch
> *"Failures in production are inevitable. If an 8-task DAG fails at Task 6 after processing 70% of claims, rerunning the pipeline must **never duplicate financial disbursements or double-count claims**.
> 
> I built our pipelines to be 100% idempotent through three layers:
> First, **Auto Loader checkpoints** ensure raw landing files are never read twice. 
> Second, staging batches are deduplicated using a deterministic PySpark window function on `claim_id` ordering by `updated_at DESC`, `ingestion_timestamp DESC`, and file row offset as a tie-breaker.
> Third, we upsert into Silver using Delta Lake `MERGE INTO` with ACID transaction boundaries. 
> 
> When failures occur, we use Databricks Workflows **Repair-and-Rerun** to re-execute only the failed task without rerunning upstream ingestion, saving 70% of compute costs and ensuring complete ledger idempotency across retries."*

### 2. Under the Hood: Why Delta `MERGE INTO` is NOT Automatically Idempotent
* **The Trap:** If your incoming source DataFrame contains **two versions of the same `claim_id`** in the same batch, Delta Lake will crash with: `Cannot perform Merge as multiple source rows matched the same target row.`
* **The Solution:** Pre-merge window deduplication with deterministic tie-breaking is mandatory!

---

## War Story 6: Pipeline Observability Across 9 Core Monitoring Metrics

### The Resume Bullet:
> *"Established pipeline observability across 9 core monitoring metrics (null spikes, duplicate ratios, data lag, schema drift, quarantine rate), cutting incident Mean Time to Detect (MTTD) by 75%."*

### 1. The 60-Second Executive Pitch
> *"Previously, our team only knew a pipeline had an issue when an actuarial executive complained that a dashboard was stale or numbers looked off. 
> 
> I established a production observability framework tracking **9 core telemetry metrics**: record volume, null percentage spikes, duplicate ratio, data lag/freshness SLA, schema drift, quarantine rate, financial reconciliation variance, job execution state, and pipeline latency.
> 
> Telemetry metrics are logged to Delta system tables and evaluated by an automated gatekeeper task in Databricks Workflows. If schema drift occurs or duplicate rates spike above 2%, a conditional task triggers an immediate PagerDuty/Slack incident payload with root-cause diagnostic context, reducing our Mean Time to Detect from 4+ hours to under 15 minutes."*

---

# PART 2: Carelon Global Solutions — Associate Data Engineer II & I

---

## War Story 7: S3 + PySpark Medallion Ingestion (-55% Processing Time)

### The Resume Bullet:
> *"Built an ingestion pipeline on AWS S3 using distributed PySpark on Databricks, applying incremental processing, column pruning and Parquet storage across a layered medallion architecture, reducing processing time by 55%."*

### 1. The 60-Second Executive Pitch
> *"When I stepped into the Associate DE II role, our ingestion layer was loading daily multi-gigabyte raw CSV files from external clearinghouses into our analytical warehouse using full-table scans. Ingestion alone took 3.5 hours every morning.
> 
> I re-engineered the ingestion architecture onto **AWS S3** and **PySpark**:
> First, I moved to an incremental processing pattern using S3 event metadata and ingestion timestamps, scanning only newly arrived files rather than full daily directories.
> Second, I converted raw CSV ingestion to **Snappy-compressed Parquet**, applying explicit **column projection pruning** to drop 45 unneeded audit and nested raw payload fields before Spark shuffled the dataset.
> Third, I established clean Medallion boundaries: Bronze preserved raw files with audit metadata (`_ingested_at`, `_source_file`), while Silver stored cleaned, typed 3NF tables. This slashed ingestion runtime by 55% and eliminated 60% of our daily S3 I/O bandwidth."*

### 2. Under the Hood & Catalyst Mechanics
* **Projection Pushdown:** Spark's Catalyst optimizer pushes column selection down into the storage layer (`FileScan parquet ... ReadSchema: struct<claim_id:string, member_id:string, paid_amount:double>`). Because Parquet is columnar, Spark reads only the specific byte offsets for selected columns, skipping 80% of disk I/O.
* **Compression Choice:** Snappy was chosen over Gzip. While Gzip compresses ~15% smaller, Snappy decompresses at ~250 MB/s per core (vs Gzip's ~50 MB/s), eliminating CPU decompression bottlenecks on distributed Spark workers.

### 3. The Follow-Up Grilling
* **Q: "Why is Parquet faster than CSV when reading only 10 out of 50 columns?"**
  * *Answer:* *"CSV is row-oriented; to extract column 10, Spark must parse every byte of columns 1 through 9 for every row in the file. Parquet stores data in Column Chunks with dictionary encoding and header metadata. Spark reads the footer metadata, seeks directly to the byte ranges containing column 10, and skips the remaining 40 columns completely."*
* **Q: "How did you implement incremental loading without Delta Lake at the time?"**
  * *Answer:* *"We used S3 prefix partitioning by `year/month/day/hour` combined with a watermark state store in S3. The ingestion script tracked `_last_processed_timestamp`. In subsequent runs, we listed S3 objects modified after that watermark, processed only new files, and atomically committed the new watermark upon successful write."*

---

## War Story 8: StructType Schema Enforcement & Clinical Review Quarantine (-35%)

### The Resume Bullet:
> *"Implemented automated data quality controls using explicit PySpark `StructType` schemas and quarantine tables for invalid records, reducing manual clinical review time by 35%."*

### 1. The 60-Second Executive Pitch
> *"In healthcare claims processing, clearinghouses frequently deliver malformed data: strings in numeric dollar fields (e.g. `'N/A'` or `'PENDING'` in `paid_amount`), malformed dates, and missing primary keys.
> 
> In our legacy setup, Spark used schema inference (`inferSchema=True`). If a file contained `'N/A'` in a numeric field, Spark inferred the entire column as `StringType`, which cascaded into failures in downstream mathematical aggregations. The clinical audit team had to spend 15+ hours every week manually reviewing failed batch dumps.
> 
> I replaced schema inference with **explicit, strict PySpark `StructType` schemas** using `PERMISSIVE` mode with a `_corrupt_record` capture column. Clean records meeting schema contracts streamed uninterrupted into Silver, while malformed records and missing PKs were automatically routed to a dedicated **Quarantine Delta table** with rejection reason codes (`INVALID_DATATYPE`, `NULL_CLAIM_ID`). This cut clinical manual review time by 35% and completely eliminated batch pipeline crashes caused by bad upstream files."*

### 2. The Code You Implemented
```python
# Explicit StructType Schema with Corrupted Record Capture
claims_schema = T.StructType([
    T.StructField("claim_id", T.StringType(), False),
    T.StructField("member_id", T.StringType(), False),
    T.StructField("paid_amount", T.DecimalType(12, 2), True),
    T.StructField("service_date", T.DateType(), True),
    T.StructField("_corrupt_record", T.StringType(), True) # Captures malformed text
])

# Read with PERMISSIVE mode
raw_df = spark.read.format("json") \
    .schema(claims_schema) \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .load("s3://landing/claims/")

# Split into Valid Silver vs Quarantine Table
clean_claims_df = raw_df.filter(F.col("_corrupt_record").isNull() & F.col("claim_id").isNotNull())

quarantine_df = raw_df.filter(F.col("_corrupt_record").isNotNull() | F.col("claim_id").isNull()) \
    .withColumn("quarantine_reason", 
        F.when(F.col("claim_id").isNull(), "MISSING_PRIMARY_KEY")
         .otherwise("SCHEMA_CORRUPTION_OR_DATATYPE_MISMATCH")) \
    .withColumn("quarantined_at", F.current_timestamp())

clean_claims_df.write.format("delta").mode("append").saveAsTable("silver.claims")
quarantine_df.write.format("delta").mode("append").saveAsTable("quarantine.claims")
```

### 3. The Follow-Up Grilling
* **Q: "Why is `inferSchema=True` an anti-pattern in production?"**
  * *Answer:* *"First, `inferSchema` requires Spark to read the entire dataset twice—once to determine data types and once to load the data, doubling S3 I/O. Second, if a column has 10,000 integers and one string `'UNKNOWN'`, Spark infers the whole column as `StringType`, silently breaking all downstream mathematical calculations. Explicit schemas guarantee strict data contracts."*

---

## War Story 9: Cross-Functional Metric Alignment & Adjudication Null Semantics

### The Resume Bullet:
> *"Partnered with business and data teams to define consistent metrics for claim counts, paid amounts and reconciliation exceptions, embedding the agreed rules into automated SQL/Python data-quality checks before data reached downstream reports."*

### 1. The 60-Second Executive Pitch
> *"Early in my career, our actuarial team and our financial operations team were reporting different numbers for 'Total Paid Claims'—finance showed 85,000 claims while actuarial reported 92,000 claims for the exact same service month.
> 
> When I investigated, I discovered the teams had conflicting semantic definitions: Finance only counted claims with `status = 'Approved'` and `paid_amount > 0`, whereas Actuarial included **zero-paid claims** (claims where member deductible covered 100% of the cost, so insurer payment was \$0.00, but a healthcare service was still rendered). Furthermore, pending claims were polluting the rejection rate denominator.
> 
> I led the technical alignment across both teams: documented explicit metric contracts, agreed on null semantics (e.g., treating unadjudicated claims with `NULL` paid amounts separately from \$0.00 adjudicated payments), and codified these business rules into reusable SQL stored procedures and automated Python DQ assertion checks. This established a single source of truth across all enterprise reporting."*

### 2. The Semantic Adjudication Table
| Metric | Business Definition Agreed | Technical SQL Logic |
| :--- | :--- | :--- |
| **Paid Claims** | Distinct claims adjudicated with insurer payout | `COUNT(DISTINCT CASE WHEN status = 'Approved' AND paid_amount > 0 THEN claim_id END)` |
| **Zero-Paid Claims** | Services rendered where deductible covered 100% | `COUNT(DISTINCT CASE WHEN status = 'Approved' AND paid_amount = 0 THEN claim_id END)` |
| **Rejection Rate** | Denied claims divided by adjudicated claims | `COUNT(Denied) / NULLIF(COUNT(Approved) + COUNT(Denied), 0)` *(Strictly excludes pending)* |

---

## War Story 10: Versioned ICD-10 Code Scraper & FastAPI Search Microservice

### The Resume Bullet:
> *"Designed a versioned ICD code ingestion and search workflow using Python (BeautifulSoup, requests, FastAPI) and AWS S3, automating collection and delivery of official code releases to downstream healthcare systems and reducing manual data-update overhead."*

### 1. The 60-Second Executive Pitch
> *"Every October, the Centers for Medicare & Medicaid Services (CMS) and the CDC publish the annual release of over 70,000+ ICD-10-CM diagnosis codes. Historically, our medical coding team manually downloaded CSV/ZIP files from government websites, parsed them in Excel, and manually imported them into database tables. The process took 3 weeks and frequently introduced formatting errors.
> 
> I built an automated end-to-end ingestion and search pipeline in Python:
> First, an automated scraping workflow using `requests` and `BeautifulSoup` monitors CMS/CDC release portals, downloads official code files, validates formatting using Pydantic regex patterns, and stages versioned releases in S3 (`s3://reference-data/icd10/v2022/`).
> Second, to serve pre-adjudication microservices that require sub-second validation of diagnosis codes during claim intake, I built a high-throughput **FastAPI microservice** with in-memory caching. It provides sub-10ms fuzzy text search and code validation endpoints, completely eliminating manual update overhead and ensuring zero downtime during annual CMS releases."*

### 2. Under the Hood
```python
# FastAPI Microservice Endpoint for Sub-10ms Code Verification
from fastapi import FastAPI, HTTPException
import re

app = FastAPI(title="ICD-10 Diagnostic Search API")

# ICD-10 format: 1 alpha + 2 digits + optional decimal + up to 4 alphanumeric
ICD10_REGEX = re.compile(r"^[A-TV-Z][0-9][0-9AB](\.[0-9A-KXZ]{1,4})?$")

@app.get("/api/v1/icd10/validate/{code}")
async def validate_icd10(code: str):
    clean_code = code.strip().upper()
    if not ICD10_REGEX.match(clean_code):
        return {"code": clean_code, "valid": False, "reason": "Malformed ICD-10 syntax"}
    
    # In-memory dictionary lookup loaded from S3 parquet
    record = icd10_catalog.get(clean_code)
    if not record:
        return {"code": clean_code, "valid": False, "reason": "Code not found in current CMS release"}
    return {"code": clean_code, "valid": True, "description": record["description"], "version": record["release_year"]}
```

### 3. The Follow-Up Grilling
* **Q: "Why expose a FastAPI microservice instead of letting applications query the database directly?"**
  * *Answer:* *"Front-end claim entry portals process thousands of claim submissions concurrently. Connecting every web user directly to an analytical database causes connection pool exhaustion and high query latency (100–300ms). FastAPI with in-memory cached lookup tables returns validated results in under 5ms, handling high concurrent throughput without touching database connections."*

---

# PART 3: Project 1 — Zomato AI Data Platform

---

## War Story 11: Snowflake Kimball Star Schema Modeling on 10M+ Records

### The Resume Bullet:
> *"Architected an end-to-end medallion pipeline from AWS S3 to Snowflake, modelling staging layers and Gold data marts over a 10M+ record dataset to serve low-latency analytical queries."*

### 1. The 60-Second Executive Pitch
> *"For the Zomato AI Data Platform, I designed an end-to-end analytical data warehouse handling over **10,000,000 restaurant orders, menus, customer reviews, and ratings** originating from raw S3 landing buckets.
> 
> I structured the warehouse following a Medallion Lakehouse paradigm in **Snowflake**:
> Raw JSON order events land in S3 and are bulk-loaded into Bronze staging using Snowflake's optimized `COPY INTO` with `STRIP_OUTER_ARRAY = TRUE`.
> In Silver, I cleaned, typed, and normalized the data into 3NF relational tables.
> In Gold, I designed a **Kimball Star Schema** dimensional data mart centered on `fact_orders` and `fact_restaurant_reviews`, surrounded by conformed dimensions (`dim_restaurants`, `dim_customers`, `dim_menu_items`, `dim_delivery_zones`). By configuring micro-partition clustering on `(order_date, city_id)`, analytical dashboards execute aggregations over 10M+ rows with sub-second response times."*

### 2. Under the Hood: Snowflake Storage & Virtual Warehouses
* **Snowflake Architecture:** Decoupled Storage (S3 blobs) $\to$ Compute (Virtual Warehouses) $\to$ Cloud Services (Metadata & Optimizer).
* **Micro-Partition Pruning:** Snowflake automatically divides tables into 50–500 MB columnar micro-partitions. By modeling facts around `order_date`, queries filtering on date ranges prune 95% of micro-partitions without scanning unreferenced data blocks.
* **Warehouse Sizing:** X-Small warehouse used for lightweight dbt transformations; scaled to Medium warehouse during batch `COPY INTO` loads to take advantage of multi-threaded parallel file ingestion.

---

## War Story 12: dbt Incremental Models (`merge`) & SCD Type 2 Snapshots

### The Resume Bullet:
> *"Engineered dbt incremental models (`merge`) and SCD Type 2 snapshots to preserve restaurant rating histories, with auto-generated data lineage and documentation."*

### 1. The 60-Second Executive Pitch
> *"In food delivery platforms, data has two distinct update patterns: high-volume append transactions (orders) and slowly changing entity attributes (restaurant ratings, operational status, delivery pricing tiers).
> 
> If you rebuild 10M order rows from scratch every day in dbt (`materialized='table'`), you waste massive Snowflake compute credits. I implemented **dbt incremental models** with `incremental_strategy='merge'` on `order_id`: dbt compiles dynamic `MERGE INTO` SQL that inspects `is_incremental()` and processes only orders with `updated_at > max(updated_at)`, cutting dbt transformation time by 80%.
> 
> To track how restaurant food ratings fluctuate over time, a standard overwrite destroys history. I configured **dbt snapshots** implementing **SCD Type 2** dimensional modeling: dbt automatically generates surrogate keys, calculates `dbt_valid_from`, `dbt_valid_to`, and flags `dbt_is_current`, preserving the full historical timeline of restaurant ratings."*

### 2. The Code You Implemented
```sql
-- dbt Incremental Model: models/gold/fact_orders.sql
{{
  config(
    materialized = 'incremental',
    unique_key = 'order_id',
    incremental_strategy = 'merge'
  )
}}

SELECT
    order_id,
    customer_id,
    restaurant_id,
    order_timestamp,
    order_amount,
    delivery_fee,
    order_status,
    updated_at
FROM {{ ref('silver_orders') }}

{% if is_incremental() %}
  -- Only scan and merge records modified since the last dbt execution
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

```sql
-- dbt Snapshot: snapshots/snap_restaurant_ratings.sql
{% snapshot snap_restaurant_ratings %}
{{
    config(
      target_schema='snapshots',
      unique_key='restaurant_id',
      strategy='check',
      check_cols=['rating', 'price_tier', 'is_active']
    )
}}

SELECT restaurant_id, name, rating, price_tier, is_active, updated_at
FROM {{ ref('silver_restaurants') }}
{% endsnapshot %}
```

### 3. The Follow-Up Grilling
* **Q: "What is the difference between `timestamp` and `check` strategies in dbt snapshots?"**
  * *Answer:* *"The `timestamp` strategy checks a reliable `updated_at` column to detect row modifications. However, if upstream source tables do not reliably update modification timestamps, the `check` strategy computes a hash across a list of monitored columns (`rating`, `price_tier`). When the hash changes, dbt expires the previous record (`dbt_valid_to = current_timestamp`) and inserts a new active version."*

---

## War Story 13: Containerized Airflow DAGs & GitHub Actions CI/CD Makefile

### The Resume Bullet:
> *"Automated daily batch workflows with Apache Airflow (Docker) and built GitHub Actions CI/CD pipelines with a Makefile for automated validation and deployment."*

### 1. The 60-Second Executive Pitch
> *"To ensure reproducibility across local development and production, I containerized the entire data stack using **Docker Compose**: orchestrating the Airflow Webserver, Scheduler, Celery Workers, Redis broker, and PostgreSQL metadata database in isolated containers.
> 
> I built a unified developer interface using a **Makefile**: commands like `make test`, `make lint`, and `make dbt-run` encapsulate environment setup, preventing 'works on my machine' issues.
> 
> In **GitHub Actions**, I built a production CI/CD pipeline: every pull request triggers automated SQL linting via `sqlfluff`, runs Python unit tests using `pytest`, validates dbt syntax via `dbt compile`, and checks DAG integrity to prevent broken DAGs or circular dependencies from ever merging to the main branch."*

---

## War Story 14: Streamlit Analytics & LiteLLM Text-to-SQL with Guardrails

### The Resume Bullet:
> *"Delivered a Streamlit self-service app with operational KPI dashboards, RAG review search, and LiteLLM Text-to-SQL natural-language querying over warehouse marts."*

### 1. The 60-Second Executive Pitch
> *"Business stakeholders and operations managers frequently need answers to ad-hoc business questions ('What was our top-selling cuisine in North Sydney last weekend?') without submitting Jira tickets to data engineers.
> 
> I built a self-service analytics portal using **Streamlit** backed by **LiteLLM**. The app features interactive operational KPI dashboards, review search, and an AI-powered **Text-to-SQL** engine:
> When a user enters a question in natural language, LiteLLM routes the prompt with our dbt Gold schema definitions, compiles a targeted SQL query, executes it against Snowflake, and renders interactive charts.
> 
> To protect the warehouse, I implemented strict **security guardrails**: the database user is restricted to a read-only role with `SELECT` privileges only on Gold marts, and all generated SQL queries pass through an Abstract Syntax Tree (AST) parser to block destructive statements (`DROP`, `ALTER`, `DELETE`) before execution."*

---

# PART 4: Project 2 — Agentic Incident-Response Platform

---

## War Story 15: Autonomous Pipeline Triage in Under 20 Seconds (LangGraph)

### The Resume Bullet:
> *"Built a LangGraph agent that investigates failed data pipelines (simulated Spark crashes and missing-file failures) and identifies the root cause in under 20 seconds."*

### 1. The 60-Second Executive Pitch
> *"In production data engineering, when a pipeline crashes at 3:00 AM, human on-call triage typically takes 30–45 minutes just logging into AWS CloudWatch or Databricks, finding the right executor log, tracing upstream DAG dependencies, and checking database tables.
> 
> I built an autonomous incident-response agent using **LangGraph** (a stateful, cyclical graph framework). When an alert fires, the agent enters an autonomous diagnostic cycle:
> It parses raw driver logs, extracts Java/PySpark stack traces, inspects S3 landing buckets to verify file arrival, checks database row counts for silent zero-row drops, and correlates failure symptoms against historical incident patterns.
> It isolates the exact root cause—such as out-of-memory executor crashes, missing partition keys, or upstream API rate-limiting—in **under 20 seconds**, outputting a structured remediation report to Slack."*

### 2. Under the Hood: LangGraph State Machine Architecture
* **StateGraph Structure:**
  ```text
  [Alert Received] ──> [Parse Logs Node] ──> [Evaluate Hypothesis]
                                                      │
                       ┌──────────────────────────────┴──────────────────────────────┐
                       ▼                                                             ▼
         [Check S3 Arrival Tool]                                           [Query Database Tool]
                       │                                                             │
                       └──────────────────────────────┬──────────────────────────────┘
                                                      ▼
                                       [Synthesize RCA & Fix Node]
                                                      │
                                                      ▼
                                         [Human Approval HITL Gate]
  ```
* **Why LangGraph over standard LangChain:** Standard LangChain executes linear chains. Real pipeline debugging is non-linear and iterative: the agent must make tool calls, evaluate tool results, and conditionally loop back to form a new hypothesis if the initial guess was incorrect.

---

## War Story 16: The 9 Forensic SRE Diagnostic Tools

### The Resume Bullet:
> *"Equipped the agent with 9 diagnostic tools for inspecting error logs, tracing upstream pipeline dependencies and querying warehouse tables."*

### 1. The 60-Second Executive Pitch
> *"An AI agent is only as good as the tools it can execute. I equipped our LangGraph agent with **9 specialized diagnostic tools** that replicate the exact debugging workflow of a Lead Data Engineer:
> 1. `parse_spark_driver_log`: Scans driver logs for fatal exceptions (`OutOfMemoryError`, `AnalysisException`).
> 2. `inspect_executor_gc_spill`: Reads Spark event logs to detect GC thrashing and shuffle disk spills.
> 3. `check_s3_file_arrival`: Verifies expected arrival time, byte size, and key names in landing buckets.
> 4. `query_table_row_count`: Checks if upstream staging tables experienced silent drops.
> 5. `inspect_schema_drift`: Compares current file schema against the registered Delta schema.
> 6. `check_dag_upstream_status`: Traces Airflow/Databricks DAG dependencies to find failed parent tasks.
> 7. `verify_reconciliation_variance`: Compares input record counts against output ledger counts.
> 8. `check_api_rate_limits`: Tests HTTP status codes (429 Too Many Requests) on external API sources.
> 9. `generate_remediation_patch`: Proposes SQL or PySpark syntax fixes for human sign-off."*

---

## War Story 17: Sandboxed Read-Only AST Parsing with `sqlglot` & Human-in-the-Loop

### The Resume Bullet:
> *"Enforced read-only database access using sqlglot query validation to block destructive statements, and required human approval before any remediation is applied."*

### 1. The 60-Second Executive Pitch
> *"The greatest risk of autonomous AI agents in enterprise data infrastructure is an LLM hallucinating a destructive command like `DROP TABLE` or executing an unconstrained `UPDATE`.
> 
> I implemented strict **dual-layer safety guardrails**:
> First, **AST Query Sandboxing:** Every SQL query generated by the agent is parsed into an Abstract Syntax Tree using **`sqlglot`**. We traverse the syntax tree; if any non-`Select` expression (such as `Drop`, `Truncate`, `Delete`, `Alter`, or `Insert`) is detected, the statement is rejected before it ever reaches the database.
> Second, **Human-in-the-Loop (HITL):** LangGraph utilizes an interrupt gate (`interrupt_before=["apply_remediation"]`). The agent cannot execute pipeline restarts or backfills autonomously; it generates a remediation payload in Slack, requiring an on-call engineer to review and click 'Approve' before any state change is applied."*

### 2. The Code You Implemented
```python
import sqlglot
from sqlglot import exp

def is_safe_read_only_query(sql_str: str) -> bool:
    """Uses sqlglot AST parsing to guarantee query is strictly read-only SELECT"""
    try:
        parsed_expressions = sqlglot.parse(sql_str)
        for expression in parsed_expressions:
            # Check root expression type
            if not isinstance(expression, exp.Select):
                return False
            
            # Walk syntax tree to catch nested destructive clauses
            for node in expression.walk():
                if isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.AlterTable, exp.TruncateTable)):
                    return False
        return True
    except Exception:
        return False # Reject malformed or unparseable SQL by default
```

### 3. The Follow-Up Grilling
* **Q: "Why parse the AST with `sqlglot` instead of using a simple regex like `if 'DROP' in sql:`?"**
  * *Answer:* *"Regex is brittle and easily bypassed. A regex check fails if a valid query has a column name or string literal containing the word (e.g. `WHERE status = 'DROPPED'` or `SELECT drop_count FROM table`). Conversely, malicious SQL injection can disguise destructive commands through comments or encoding. AST parsing understands actual SQL grammar, verifying the abstract syntax tree structure regardless of formatting."*

---

## War Story 18: Production LangSmith Observability & Token Cost Optimization

### The Resume Bullet:
> *"Instrumented agent runs with LangSmith to trace step-by-step reasoning and monitor tool performance, execution latency and LLM token costs."*

### 1. The 60-Second Executive Pitch
> *"Operating AI agents in production requires the same rigor as traditional distributed data systems. I instrumented every agent invocation with **LangSmith**.
> 
> LangSmith provides an end-to-end execution trace for every incident: recording prompt inputs, tool invocation latencies, and intermediate LLM reasoning steps.
> Analyzing these traces revealed two major production insights:
> First, raw log dumps were consuming over 12,000 prompt tokens per run. I implemented a regex log pre-filtering tool that pruned clean logs and extracted only error stack traces, cutting prompt token consumption and LLM costs by **40%**.
> Second, by utilizing LiteLLM model routing, we routed initial log parsing to lightweight models (Claude 3.5 Haiku) and reserved frontier models for final root-cause synthesis, reducing overall incident triage latency from 45 seconds to under 20 seconds."*

---

# Summary: The Interview Mindset

```text
 ┌────────────────────────────────────────────────────────────────────────┐
 │                    HOW TO COMMAND A SENIOR DE INTERVIEW                │
 ├────────────────────────────────────────────────────────────────────────┤
 │ 1. Ground Every Answer in Production Reality:                          │
 │    Mention real numbers: 3 TB, 1M rows, $10k variance, 94% speedup.   │
 │                                                                        │
 │ 2. Show Architecture Tradeoffs:                                        │
 │    Explain WHY you chose Liquid Clustering over Hive directories,     │
 │    dbt snapshots over manual triggers, or AST parsing over regex.     │
 │                                                                        │
 │ 3. Highlight Fault Tolerance:                                          │
 │    Senior engineers are defined by how their systems handle failure:   │
 │    idempotent Delta MERGE, quarantine tables, and HITL safety gates.   │
 └────────────────────────────────────────────────────────────────────────┘
```
