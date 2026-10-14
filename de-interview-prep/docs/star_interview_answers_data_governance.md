# Master Interview Defense: Data Governance & Unity Catalog (STAR-Style)

### Practical, Battle-Tested Answers for Senior & Lead Data Engineering Interviews
**Target Stack:** Databricks Unity Catalog, Delta Lake, Azure / AWS Cloud Storage, System Tables

---

## Quick Navigation
1. [The 2-Minute Anchor Story: Modernizing Lakehouse Governance](#1-the-2-minute-anchor-story-modernizing-lakehouse-governance)
2. [STAR Drill 1: Access Control (RBAC vs. ABAC & Preventing Role Explosion)](#2-star-drill-1-access-control-rbac-vs-abac--preventing-role-explosion)
3. [STAR Drill 2: Privacy Compliance (HIPAA PHI & GDPR Right to be Forgotten)](#3-star-drill-2-privacy-compliance-hipaa-phi--gdpr-right-to-be-forgotten)
4. [STAR Drill 3: Native Lineage for Impact & Root-Cause Analysis](#4-star-drill-3-native-lineage-for-impact--root-cause-analysis)
5. [STAR Drill 4: Audit Logs & Cost Attribution via System Tables](#5-star-drill-4-audit-logs--cost-attribution-via-system-tables)
6. [STAR Drill 5: Fine-Grained Security (Row Filters & Column Masks vs. Dynamic Views)](#6-star-drill-5-fine-grained-security-row-filters--column-masks-vs-dynamic-views)
7. [STAR Drill 6: Delta Sharing (Zero-Copy External Partner Collaboration)](#7-star-drill-6-delta-sharing-zero-copy-external-partner-collaboration)
8. [STAR Drill 7: Governing Unstructured Data (Volumes & RAG/AI Pipelines)](#8-star-drill-7-governing-unstructured-data-volumes--ragai-pipelines)
9. [Senior Judgment: "What I Would NOT Do and Why"](#9-senior-judgment-what-i-would-not-do-and-why)

---

## 1. The 2-Minute Anchor Story: Modernizing Lakehouse Governance

**Interview Question:** *"Tell me about a time you implemented or modernized data governance in your pipelines."*

* **Situation:**  
  At Carelon, we managed a 3 TB daily healthcare claims platform on Azure Databricks. As our analytics user base grew to over 50 analysts and multiple hospital partners, data security was becoming an operational bottleneck. To enforce HIPAA compliance for PHI (SSNs, patient names, diagnosis codes) and multi-tenant provider isolation, previous teams had built over 40 bespoke, hardcoded SQL views. This caused massive "view drift"—whenever a table schema changed, downstream views broke—and queries suffered performance penalties from deeply nested view layers.

* **Task:**  
  My objective was to modernize our governance architecture using Databricks Unity Catalog: eliminate the 40+ brittle SQL views, enforce HIPAA/GDPR privacy controls directly on base tables, establish automated audit logging, and ensure zero-copy external data exchange without compromising performance.

* **Action:**  
  1. **Fine-Grained Access Control:** I replaced the 40+ views with native Unity Catalog **Row Filters** and **Dynamic Column Masks**. I wrote centralized SQL UDFs using `is_account_group_member()` that dynamically redacted SSNs to `XXX-XX-1234` for general analysts while passing cleartext only to audited claims adjudicators.
  2. **Regulatory Compliance (GDPR/HIPAA):** For GDPR "Right to be Forgotten" deletion requests, I engineered a purge workflow: running Delta `DELETE`, executing `REORG TABLE ... APPLY (PURGE)` to clear Deletion Vectors, and running `VACUUM` to physically remove tombstoned Parquet files from ADLS Gen2.
  3. **Automated Auditing:** I built an anomaly detection pipeline querying `system.access.audit` to detect suspicious mass extractions (>100,000 rows of sensitive member data) and joined `system.billing.usage` with custom tags to allocate compute costs back to business units.
  4. **Unstructured Data:** I migrated legacy DBFS mounts to Unity Catalog **Volumes** (`/Volumes/prod/claims/scans`) to govern raw clinical PDF scans and images with the same RBAC as tabular data.

* **Result:**  
  * Deprecated **42 brittle SQL views**, cutting security maintenance overhead from days to minutes during schema updates.
  * Achieved **100% HIPAA audit compliance** with automated, immutable audit logs in `system.access.audit`.
  * Preserved Spark query performance by eliminating nested view joins, allowing Catalyst optimizer and file pruning to work directly on base Delta tables.

---

## 2. STAR Drill 1: Access Control (RBAC vs. ABAC & Preventing Role Explosion)

**Interview Question:** *"How do you design access control across hundreds of tables without creating massive administrative overhead?"*

* **Situation:**  
  In large enterprise lakehouses, configuring permissions table-by-table creates "role explosion"—you end up with hundreds of bespoke roles (e.g., `claims_ny_analyst`, `claims_ca_analyst`, `claims_read_no_ssn`), making access audits almost impossible.

* **Task:**  
  Design a scalable, low-maintenance access control architecture that enforces least privilege without administrative chaos.

* **Action:**  
  I implemented a **hybrid RBAC + ABAC strategy**:
  * **Coarse-Grained RBAC (Hierarchical Inheritance):** Privileges are assigned only to SCIM-synced enterprise IdP groups (never individual users) at the Catalog and Schema level (`USE CATALOG`, `USE SCHEMA`, `SELECT`).
  * **Fine-Grained ABAC (Attribute-Based Access Control):** Instead of creating custom per-table roles, we tagged columns containing sensitive attributes (e.g., `tag.phi = 'true'`, `tag.pii = 'restricted'`).
  * We tied centralized dynamic masking policies to these tags. Any new table or column tagged `phi` automatically inherited redaction logic without writing a single new `GRANT` statement.

* **Result:**  
  Reduced the required security roles from over 60 down to **7 functional domain groups** (e.g., `claims_adjudicators`, `analytics_engineers`, `data_scientists`). Onboarding new datasets into production required zero manual security ticket escalations.

* **Sound-Bite Pitch:**  
  > *"RBAC is simple to understand but causes role explosion at scale. I use RBAC for coarse-grained folder and schema boundaries, and layer ABAC tags for context-driven data policies."*

---

## 3. STAR Drill 2: Privacy Compliance (HIPAA PHI & GDPR Right to be Forgotten)

**Interview Question:** *"How do you physically delete customer data for GDPR/CCPA in a Delta Lakehouse where time travel and snapshots exist?"*

* **Situation:**  
  Under GDPR Article 17 ("Right to be Forgotten") and CCPA, when a patient or user requests deletion, their records must be physically unrecoverable within 30 days. However, Delta Lake's default behavior is append-only versioning: running a SQL `DELETE` only creates a new Delta table snapshot while the customer's data remains fully readable in older Parquet files via Time Travel (`VERSION AS OF`).

* **Task:**  
  Build an automated, compliant hard-delete pipeline that guarantees physical erasure from storage while minimizing compute cost and avoiding pipeline downtime.

* **Action:**  
  I developed a 3-step deterministic purging pipeline:
  1. **Logical Deletion:**  
     Executed `DELETE FROM prod.silver.members WHERE member_id = 'XYZ';`.
  2. **Purging Deletion Vectors:**  
     Because our tables used Delta Deletion Vectors for fast writes, the Parquet files still physically contained the deleted rows. I executed:  
     ```sql
     REORG TABLE prod.silver.members APPLY (PURGE);
     ```  
     This rewritten the affected Parquet data files without the tombstoned rows.
  3. **Physical File Removal:**  
     I scheduled a maintenance job that purged historical commits older than our compliance retention window using:  
     ```sql
     VACUUM prod.silver.members RETAIN 168 HOURS; -- 7 days retention
     ```  
     For immediate legal removal requests, we used a dedicated job with zero-retention override (`SET spark.databricks.delta.vacuum.parallelDelete.enabled = true;`).
  4. **Verification & Audit:** Verified zero trace in metadata and logged the compliance event into our audit ledger.

* **Result:**  
  Achieved 100% automated GDPR compliance with zero manual DBA intervention, ensuring that deleted records could never be restored via Time Travel after the 7-day SLA.

---

## 4. STAR Drill 3: Native Lineage for Impact & Root-Cause Analysis

**Interview Question:** *"How do you trace data lineage, and how has it helped you in production?"*

* **Situation:**  
  During a major core pipeline modernization, upstream source database teams notified us that an integer column `provider_tax_id` was being changed to a formatted string `VARCHAR(20)`. In a pipeline with hundreds of silver tables and gold marts, changing a column blindly risked breaking downstream executive dashboards and ML pipelines.

* **Task:**  
  Perform a complete downstream dependency impact analysis before applying the schema change, without relying on stale confluence documentation.

* **Action:**  
  Rather than grepping through git repositories, I queried Unity Catalog's native system lineage tables:
  ```sql
  SELECT DISTINCT 
      target_table_full_name,
      target_column_name
  FROM system.access.column_lineage
  WHERE source_table_full_name = 'prod_healthcare.silver.dim_providers'
    AND source_column_name = 'provider_tax_id';
  ```
  Unity Catalog captures lineage automatically at runtime directly from the Catalyst physical query plans without needing manual instrumentation or third-party agents.

* **Result:**  
  Identified **3 gold financial marts** and **1 downstream Power BI semantic model** that relied on mathematical checks against that column. We notified the downstream teams and refactored the models ahead of the release, resulting in **zero production incidents** during the migration.

---

## 5. STAR Drill 4: Audit Logs & Cost Attribution via System Tables

**Interview Question:** *"How do you monitor who is accessing sensitive data and track lakehouse infrastructure costs?"*

* **Situation:**  
  Enterprise security required continuous auditability for HIPAA compliance to detect potential data exfiltration. Simultaneously, leadership complained about surging Databricks DBU bills without visibility into which business units were driving compute expenses.

* **Task:**  
  Build centralized security monitoring and finops dashboards using native Unity Catalog system tables.

* **Action:**  
  1. **SOC 2 & HIPAA Anomaly Detection:**  
     I scheduled an hourly PySpark alerting job over `system.access.audit`. It tracked high-volume data extraction anomalies:
     ```sql
     -- Flagging non-ETL users reading > 100k sensitive rows
     SELECT event_time, user_identity.email, request_params.table_full_name, read_statistics.rows_read
     FROM system.access.audit
     WHERE event_date = CURRENT_DATE()
       AND request_params.table_full_name LIKE '%.silver.members%'
       AND read_statistics.rows_read > 100000
       AND user_identity.email NOT LIKE 'service_principal_%';
     ```
  2. **Cost Attribution & FinOps:**  
     We enforced cluster tagging (`Team: Claims`, `Env: Prod`). I joined `system.billing.usage` with `system.billing.list_prices` to calculate exact dollar spend by tag, job, and workspace, rendering an executive dashboard in Databricks SQL.

* **Result:**  
  * Detected a compromised contractor service account attempting a bulk download of 850k patient records within 15 minutes of occurrence.
  * Identified 4 unutilized, oversized interactive dev clusters running 24/7, reducing idle compute costs by **$14,000/month (18% cost reduction)**.

---

## 6. STAR Drill 5: Fine-Grained Security (Row Filters & Column Masks vs. Dynamic Views)

**Interview Question:** *"What is the difference between a Dynamic View and a Row Filter/Column Mask in Unity Catalog? Why would you choose one over the other?"*

* **Situation:**  
  Historically, Databricks handled fine-grained access control by creating SQL Dynamic Views with `current_user()` or `is_member()` logic. While functional, having 40+ views caused massive management overhead and performance degradation.

* **Task:**  
  Upgrade our table security model to modern Unity Catalog native Row Filters and Column Masks.

* **Action:**  
  * **Why Row Filters/Masks Win:**  
    Unlike dynamic views (which are separate database objects that wrap the base table), **Row Filters and Column Masks are attached directly to the base table**.
  * **Syntax & Implementation:**
    ```sql
    -- 1. Create centralized mask function
    CREATE OR REPLACE FUNCTION prod.governance.mask_ssn(ssn STRING)
    RETURN IF(is_account_group_member('claims_pii_authorized'), ssn, CONCAT('XXX-XX-', RIGHT(ssn, 4)));

    -- 2. Attach mask directly to base table column
    ALTER TABLE prod.silver.members 
    ALTER COLUMN ssn SET MASK prod.governance.mask_ssn;

    -- 3. Create row filter function
    CREATE OR REPLACE FUNCTION prod.governance.filter_provider_tenant(provider_id STRING)
    RETURN is_account_group_member('admin_group') 
        OR is_account_group_member(CONCAT('provider_', provider_id));

    -- 4. Attach row filter directly to base table
    ALTER TABLE prod.silver.claims 
    SET ROW FILTER prod.governance.filter_provider_tenant ON (provider_id);
    ```

* **Result & Trade-Off Comparison:**
  * **No Bypass Risk:** Users cannot bypass the security logic by discovering the base table, because the base table *itself* enforces the mask for all queries.
  * **Catalyst Optimizer Performance:** Predicate pushdown and file pruning operate cleanly on the base table.
  * **Lineage Integrity:** Lineage points directly to the real table rather than fracturing across dozens of intermediate views.

---

## 7. STAR Drill 6: Delta Sharing (Zero-Copy External Partner Collaboration)

**Interview Question:** *"How do you share live data with external third parties or cross-cloud partners securely without copying data?"*

* **Situation:**  
  Our healthcare plan needed to provide daily adjudicated claims summaries to 12 partner hospital networks. The legacy process used batch Python scripts to export CSVs, encrypt them with PGP, and push them to external **SFTP servers**. This resulted in high storage duplication, fragile pipelines, and lack of revocation control once files left our perimeter.

* **Task:**  
  Implement a modern, zero-copy, secure data exchange mechanism that supports both Databricks and non-Databricks consumers.

* **Action:**  
  I implemented **Delta Sharing**, an open protocol for real-time zero-copy data sharing:
  1. **Provider Setup:** Configured a Delta Share on our Unity Catalog Metastore, adding specific views containing only the partner's aggregated data partitions.
  2. **Recipient Provisioning:**
     * **Databricks-to-Databricks:** For partners on Databricks, we used native Databricks sharing—no tokens or credential files required; the share mounted directly as a read-only catalog in their workspace.
     * **Open Sharing:** For non-Databricks partners (e.g., querying via pandas, Power BI, or Snowflake), we issued secure, revocable bearer credential profiles with activation links.
  3. **Security & Egress Control:** Enforced IP whitelisting on recipients and deployed shares in the same cloud region to eliminate cross-region egress fees.

* **Result:**  
  * Replaced **12 fragile SFTP batch jobs**, eliminating 400 GB/month of duplicated CSV files.
  * Gained **instant revocation capabilities**—if a hospital contract ended, access was terminated with a single `REVOKE` statement, rendering previously downloaded bearer tokens immediately invalid.

---

## 8. STAR Drill 7: Governing Unstructured Data (Volumes & RAG/AI Pipelines)

**Interview Question:** *"How do you govern unstructured files (PDFs, DICOM images, audio) in Unity Catalog, especially for modern AI / RAG pipelines?"*

* **Situation:**  
  Our clinical analytics team was building an AI model to extract clinical diagnoses from doctor consultation PDFs. Previously, teams uploaded files to ad-hoc S3/ADLS folders or unmanaged DBFS mounts (`/FileStore/`), which had zero table-level governance, no access auditing, and exposed raw credentials in notebook init scripts.

* **Task:**  
  Incorporate unstructured file storage under the same enterprise governance model as our structured Delta tables.

* **Action:**  
  1. **Unity Catalog Volumes:** Created managed and external Volumes inside the 3-level namespace:
     ```sql
     CREATE VOLUME prod_healthcare.bronze.clinical_pdf_records
       COMMENT 'Governed storage for raw medical consultation PDFs';
     ```
  2. **Role-Based Access Control:** Replaced broad cloud IAM bucket access with granular SQL grants:
     ```sql
     GRANT READ VOLUME ON VOLUME prod_healthcare.bronze.clinical_pdf_records TO `clinical_ai_engineers`;
     ```
  3. **Native POSIX Integration for AI:** Data scientists accessed files using standard POSIX paths (`/Volumes/prod_healthcare/bronze/clinical_pdf_records/doc_102.pdf`) using native Python libraries (PyMuPDF, LangChain, PyTorch) without needing cloud storage credentials.
  4. **Downstream Governance in RAG:** When documents were chunked and vectorized, we carried the volume metadata and document security classification tags (`tag.sensitivity = 'restricted'`) into the Delta vector index tables.

* **Result:**  
  Centralized all structured and unstructured clinical assets under one governance pane, ensuring every single document read by AI pipelines was recorded in `system.access.audit`.

---

## 9. Senior Judgment: "What I Would NOT Do and Why"

Senior interviewers look for judgment. When discussing governance, state what you avoid:

1. **I would NOT use legacy Dynamic Views over base tables for enterprise FGAC:**
   * *Why:* Dynamic views create severe view sprawling, obscure table lineage, and fail if a user is accidentally granted direct access to the underlying table. Native Row Filters and Column Masks enforce security at the storage engine level.
2. **I would NOT use DBFS mounts (`dbutils.fs.mount`) or hardcoded IAM access keys:**
   * *Why:* DBFS mounts are workspace-wide; any user with cluster compute access can browse the entire bucket. I mandate Unity Catalog Storage Credentials and External Locations governed by SQL RBAC.
3. **I would NOT grant permissions to individual user emails:**
   * *Why:* It breaks immediately when personnel change. Grants must always be assigned to SCIM-synced IdP groups (Entra ID / Okta) mapped to functional roles.
4. **I would NOT rely on Delta `DELETE` alone for GDPR compliance:**
   * *Why:* A plain `DELETE` only marks metadata and leaves rows fully retrievable via Delta Time Travel. Hard legal deletion requires `REORG ... APPLY (PURGE)` and `VACUUM`.

---

## Quick Revision Cheat Sheet for Interview Day

```text
┌───────────────────────────┬────────────────────────────────────────────────────────┐
│ Governance Pillar         │ Exact Unity Catalog Object / Command                   │
├───────────────────────────┼────────────────────────────────────────────────────────┤
│ Coarse Access Control     │ GRANT USE CATALOG, USE SCHEMA, SELECT TO `group`       │
│ Attribute Access (ABAC)   │ ALTER TABLE ... SET TAGS ('phi' = 'true')              │
│ Dynamic Column Mask       │ ALTER TABLE t ALTER COLUMN c SET MASK mask_fn          │
│ Row-Level Security (RLS)  │ ALTER TABLE t SET ROW FILTER filter_fn ON (col)        │
│ Group Evaluation in UDF   │ is_account_group_member('group_name')                  │
│ Audit Access Logs         │ system.access.audit (table_accessed, rows_read)        │
│ Cost & Billing Logs       │ system.billing.usage JOIN system.billing.list_prices   │
│ Table & Column Lineage    │ system.access.table_lineage, column_lineage            │
│ GDPR Hard Deletion        │ DELETE -> REORG TABLE ... APPLY (PURGE) -> VACUUM      │
│ Unstructured Files        │ CREATE VOLUME catalog.schema.volume_name               │
│ Zero-Copy Partner Sharing │ Delta Sharing (CREATE SHARE, ADD TABLE, RECIPIENT)     │
└───────────────────────────┴────────────────────────────────────────────────────────┘
```
