# Enterprise Storage Security: Databricks Unity Catalog vs. Snowflake

### A Senior Data Engineer's Deep-Dive Architecture, IAM Trust Handshake, and Interview Defense Playbook

---

## 1. Executive Summary & The Core Problem

In legacy cloud data architectures (Hive Metastore, standalone Spark clusters, early warehouse setups), storage access was frequently configured using **anti-patterns**:
- Storing long-lived cloud credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, or Azure SAS tokens) in notebook secrets, environment variables, or cluster configs.
- Hardcoded S3/ADLS bucket mounts in DBFS (`dbutils.fs.mount`), exposing entire storage containers to any user with compute access.
- Inability to rotate keys without taking down production pipelines.
- Complete lack of unified, granular auditing at the individual user/query level.

Modern Lakehouse architectures (**Databricks Unity Catalog**) and Cloud Data Warehouses (**Snowflake**) resolve this by enforcing **Cloud Identity Federation** governed purely by **Database SQL RBAC (Role-Based Access Control)**.

```text
❌ LEGACY ANTI-PATTERN (HIGH RISK):
[Data Engineer / Analyst] ──► Uses Access Key / Secret Key / DBFS Mount
                              └──► Static keys leak, no key rotation, unmetered access.

✅ MODERN DECOUPLED ARCHITECTURE (LEAST PRIVILEGE):
[Data Analyst / Pipeline]
       │ (1) Runs SQL: SELECT * FROM claims_silver
       ▼
[Data Platform: UC or Snowflake]
       │ (2) Evaluates SQL RBAC (Does role/user have SELECT?)
       │ (3) Requests short-lived STS AssumeRole tokens via Cloud IAM Trust Handshake
       ▼
[Cloud Storage: S3 / ADLS / GCS] (Zero long-lived static credentials exist in the platform!)
```

---

## 2. Architectural Comparison Matrix

| Architectural Layer | Databricks Unity Catalog (UC) | Snowflake |
| :--- | :--- | :--- |
| **Cloud IAM Anchor** | AWS IAM Role / Azure Access Connector | AWS IAM Role / Azure Storage Account IAM |
| **Identity Bridge Object** | `STORAGE CREDENTIAL` | `STORAGE INTEGRATION` |
| **Path Scoping Object** | `EXTERNAL LOCATION` (binds URL to Credential) | `STAGE` (binds URL to Integration) |
| **Allowed Paths Defined In** | `EXTERNAL LOCATION` (`URL 's3://bucket/path/'`) | `STORAGE INTEGRATION` (`STORAGE_ALLOWED_LOCATIONS`) |
| **Raw / Unstructured Data** | **Unity Catalog Volumes** (`catalog.schema.volume`) | **Snowflake Stages** (`@my_db.my_schema.my_stage`) |
| **Structured Table Access** | `GRANT SELECT ON TABLE ...` | `GRANT SELECT ON TABLE ...` |
| **Direct File Read Access** | `GRANT READ FILES ON EXTERNAL LOCATION ...` | `GRANT READ ON STAGE ...` |
| **Direct File Write Access** | `GRANT WRITE FILES ON EXTERNAL LOCATION ...` | `GRANT WRITE ON STAGE ...` |
| **Table Creation Privilege** | `GRANT CREATE TABLE ON EXTERNAL LOCATION ...` | `GRANT USAGE ON INTEGRATION ...` |
| **Engine Bypass Defense** | Spark Driver blocks direct `s3://` URIs without `READ FILES` | Virtual Warehouse blocks direct bucket access without stage grants |

```mermaid
graph TD
    subgraph Cloud Infrastructure (AWS / Azure)
        IAMRole[AWS IAM Role / Azure Managed Identity]
        S3Bucket[S3 Bucket: enterprise-lakehouse-prod]
        IAMRole -->|IAM Policy: Read/Write| S3Bucket
    end

    subgraph Databricks Unity Catalog
        UC_Cred[Storage Credential: aws_prod_cred]
        UC_Loc1[External Location: s3_claims_landing]
        UC_Loc2[External Location: s3_claims_silver]
        UC_Vol[Volume: medical_scans_raw]
        UC_Tbl[Table: prod.silver.claims]

        UC_Cred -->|Assumes Role| IAMRole
        UC_Cred --> UC_Loc1
        UC_Cred --> UC_Loc2
        UC_Loc1 --> UC_Vol
        UC_Loc2 --> UC_Tbl
    end

    subgraph Snowflake
        SF_Int[Storage Integration: s3_prod_integration]
        SF_Stg1[External Stage: raw_claims_stage]
        SF_Stg2[External Stage: silver_claims_stage]
        SF_Tbl[Table: prod_dw.silver.claims]

        SF_Int -->|Assumes Role| IAMRole
        SF_Int --> SF_Stg1
        SF_Int --> SF_Stg2
        SF_Stg2 --> SF_Tbl
    end
```

---

## 3. The 4-Tier Security Handshake: Mechanics & Code

### Tier 1: Cloud IAM Setup & The Confused Deputy Defense

Both platforms rely on an **AWS IAM Trust Relationship** using `sts:AssumeRole`.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowPlatformAssumeRoleWithExternalId",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod"
        /* For Snowflake: "arn:aws:iam::123456789012:user/snowflake-service-user" */
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          /* DEFENSE AGAINST THE CONFUSED DEPUTY ATTACK */
          "sts:ExternalId": "00000000-0000-0000-0000-000000000000"
        }
      }
    }
  ]
}
```

> **Why the External ID is Critical (The Confused Deputy Attack):**
> A confused deputy attack occurs when an attacker in their own AWS/Databricks/Snowflake tenant tricks the shared multi-tenant SaaS service into assuming an IAM role in *your* private AWS account to access your buckets.
> By configuring a unique `sts:ExternalId` (generated by Snowflake or tied to your Databricks Metastore ID) in your IAM trust policy condition, AWS guarantees that only requests originating from **your specific tenant** can successfully assume the role.

---

### Tier 2: The Identity Bridge (Platform Admin Level)

#### Databricks Unity Catalog: `STORAGE CREDENTIAL`
In Databricks, the storage credential purely represents the cloud identity. It does not dictate what paths are allowed.
```sql
-- Executed by Account Admin or Metastore Admin
CREATE STORAGE CREDENTIAL aws_prod_cred
  AUTHENTICATION_TYPE AWS_IAM_ROLE
  COMMENT 'Central AWS IAM Role for production data ingestion and tables';

-- Describe credential to inspect IAM ARN and External ID
DESCRIBE STORAGE CREDENTIAL aws_prod_cred;
```

#### Snowflake: `STORAGE INTEGRATION`
In Snowflake, the integration pairs the IAM identity **and** explicitly whitelists allowed/blocked cloud storage paths.
```sql
-- Executed by ACCOUNTADMIN
CREATE OR REPLACE STORAGE INTEGRATION s3_prod_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::987654321098:role/snowflake_s3_role'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://enterprise-lakehouse-prod/claims/',
    's3://enterprise-lakehouse-prod/pharmacy/'
  )
  STORAGE_BLOCKED_LOCATIONS = (
    's3://enterprise-lakehouse-prod/claims/restricted_pii/'
  );

-- Retrieve generated Snowflake User ARN and External ID to paste into AWS IAM:
DESC INTEGRATION s3_prod_integration;
```

---

### Tier 3: Path Scoping & Storage Abstractions

#### Databricks Unity Catalog: `EXTERNAL LOCATION` & `VOLUME`
Databricks decouples identity from paths. An administrator can create multiple isolated `EXTERNAL LOCATION` objects referencing the same underlying `STORAGE CREDENTIAL`.
```sql
-- 1. Create External Location for Raw Ingestion Landing
CREATE EXTERNAL LOCATION s3_claims_landing
  URL 's3://enterprise-lakehouse-prod/claims/landing/'
  WITH (STORAGE CREDENTIAL aws_prod_cred)
  COMMENT 'Landing zone for Auto Loader claims streaming';

-- 2. Create External Location for Silver Lakehouse Tables
CREATE EXTERNAL LOCATION s3_claims_silver
  URL 's3://enterprise-lakehouse-prod/claims/silver/'
  WITH (STORAGE CREDENTIAL aws_prod_cred);

-- 3. Governing Unstructured Files (PDFs, DICOM scans) with Volumes
CREATE EXTERNAL VOLUME prod_healthcare.bronze.medical_records_raw
  LOCATION 's3://enterprise-lakehouse-prod/claims/landing/scans/'
  COMMENT 'Governed volume for raw imaging';
```

#### Snowflake: `STAGE`
In Snowflake, paths within the integration's whitelist are scoped using `STAGE` objects.
```sql
-- 1. Create External Stage for Raw Ingestion Landing
CREATE OR REPLACE STAGE raw_claims_stage
  URL = 's3://enterprise-lakehouse-prod/claims/landing/'
  STORAGE_INTEGRATION = s3_prod_integration
  FILE_FORMAT = (TYPE = 'PARQUET');

-- 2. Create External Stage for Silver Data Directory
CREATE OR REPLACE STAGE silver_claims_stage
  URL = 's3://enterprise-lakehouse-prod/claims/silver/'
  STORAGE_INTEGRATION = s3_prod_integration
  FILE_FORMAT = (TYPE = 'PARQUET');
```

---

### Tier 4: End-User SQL RBAC (The Principle of Least Privilege)

This is the most critical distinction in enterprise security: **Data Analysts never touch cloud credentials or external locations.**

#### Databricks Unity Catalog RBAC:
```sql
-- Pipeline Service Principals: Ingest raw files and build tables
GRANT READ FILES ON EXTERNAL LOCATION s3_claims_landing TO `de_ingestion_sp`;
GRANT CREATE TABLE ON EXTERNAL LOCATION s3_claims_silver TO `de_pipeline_sp`;

-- Data Scientists: Access unstructured images via governed Volume
GRANT READ VOLUME ON VOLUME prod_healthcare.bronze.medical_records_raw TO `clinical_ds_group`;

-- Business Analysts: Access structured tables ONLY
-- Note: Analysts have ZERO permissions on S3 or the External Location!
GRANT SELECT ON TABLE prod_healthcare.silver.claims TO `claims_analysts`;
```

#### Snowflake RBAC:
```sql
-- Pipeline Role: Ingest raw files into the warehouse
GRANT USAGE ON INTEGRATION s3_prod_integration TO ROLE de_pipeline_role;
GRANT USAGE, READ ON STAGE raw_claims_stage TO ROLE de_pipeline_role;

-- Business Analysts: Access structured tables ONLY
-- Note: Analysts have ZERO permissions on the Stage or Integration!
GRANT SELECT ON TABLE prod_dw.silver.claims TO ROLE claims_analyst_role;

-- Ingestion Execution:
COPY INTO prod_dw.bronze.claims_raw
FROM @raw_claims_stage
FILE_FORMAT = (TYPE = 'PARQUET');
```

---

## 4. Five Elite Senior Interview Defense Traps

### Trap 1: "If an analyst runs `SELECT * FROM silver.claims`, do they need IAM permissions on S3 or grants on the External Location / Stage?"
> **Senior DE Answer:**
> **"No, absolutely not.** In both Unity Catalog and Snowflake, table-level access is completely decoupled from cloud storage access.
> When an analyst executes `SELECT`, the query engine checks the **table-level grant** in the metadata catalog. If authorized, the engine acts as an authorized proxy, assuming the storage credential to read the Parquet/Delta files on behalf of the user.
> Granting `READ FILES` or stage access to an analyst would violate the Principle of Least Privilege because it would allow them to bypass table-level access controls, row filters, and column masks to read raw files directly."

---

### Trap 2: "What is the Confused Deputy Problem, and how do Snowflake `STORAGE_AWS_EXTERNAL_ID` and Databricks Metastore IDs prevent it?"
> **Senior DE Answer:**
> "A confused deputy vulnerability occurs in multi-tenant SaaS environments when an attacker with their own Snowflake or Databricks account configures an integration pointing to *your* private AWS IAM Role ARN, attempting to coerce the SaaS provider into reading your bucket.
> AWS prevents this via `sts:ExternalId`. When Snowflake or Databricks requests an STS `AssumeRole` token for your bucket, it passes the unique identifier assigned to your specific organization's tenant. AWS verifies that the External ID matches your trust policy condition. If another customer tries to target your IAM role ARN from their tenant, AWS immediately rejects the STS assume-role call because their tenant cannot supply your unique External ID."

---

### Trap 3: "What is the key architectural difference in how Databricks and Snowflake scope storage paths?"
> **Senior DE Answer:**
> "Snowflake couples path whitelisting directly into the **`STORAGE INTEGRATION`** object via `STORAGE_ALLOWED_LOCATIONS` and `STORAGE_BLOCKED_LOCATIONS`. An engineer cannot create a stage pointing to any path outside that whitelist.
> Databricks separates identity from paths:
> 1. A **`STORAGE CREDENTIAL`** encapsulates purely the cloud identity (IAM role / Managed Identity).
> 2. An **`EXTERNAL LOCATION`** binds that credential to a specific URL prefix.
> This makes Databricks more modular: a central platform team creates one Storage Credential, and domain teams can create dozens of isolated External Locations (e.g., Bronze, Silver, HR, Claims) with independent SQL grants (`READ FILES`, `WRITE FILES`, `CREATE TABLE`) without altering the underlying cloud credential."

---

### Trap 4: "What happens if a rogue developer runs `spark.read.parquet('s3://...')` directly in a Databricks notebook?"
> **Senior DE Answer:**
> "On a Unity Catalog-governed cluster (Single User or Shared Access Mode), direct cloud storage access via raw `s3://` or `abfss://` URIs is intercepted at the Spark driver.
> The driver validates if the authenticated user has explicit `READ FILES` privileges on the corresponding `EXTERNAL LOCATION` in Unity Catalog. If not, the execution immediately throws a `SecurityException: Permission denied`.
> To read data securely, users must either query the catalog table (`catalog.schema.table`) or reference a governed Unity Catalog **Volume** (`/Volumes/catalog/schema/volume_name/`)."

---

### Trap 5: "How do Databricks Volumes compare to Snowflake Stages for unstructured data?"
> **Senior DE Answer:**
> "They serve the identical architectural purpose: bringing unstructured and semi-structured files (PDFs, DICOM medical images, audio, raw CSVs) under the database governance umbrella.
> In Snowflake, files are staged in an **Internal or External Stage** (`@stage`), and metadata is queried using `DIRECTORY TABLES`.
> In Databricks, **Unity Catalog Volumes** are first-class securable objects inside the 3-level namespace (`catalog.schema.volume`). They expose standard POSIX file paths (`/Volumes/...`) for native Python libraries (OpenCV, PyTorch) while enforcing SQL grants (`READ VOLUME`, `WRITE VOLUME`) and logging access in `system.access.audit`."

---

## 5. The 60-Second Executive Pitch Script

When an interviewer asks: *"How do you handle storage security and access delegation across Databricks and Snowflake?"*

> *"In both platforms, our primary security mandate is the complete elimination of static, long-lived cloud access keys. We implement Cloud Identity Federation via AWS IAM AssumeRole and short-lived STS tokens.*
>
> *In **Snowflake**, we configure a `STORAGE INTEGRATION` that binds the IAM role ARN, whitelists allowed S3 prefixes, and establishes a trust relationship using Snowflake's unique `STORAGE_AWS_EXTERNAL_ID` to prevent Confused Deputy exploits. We then bind database stages to that integration and grant stage privileges strictly to ingestion pipelines.*
>
> *In **Databricks Unity Catalog**, the architecture is cleanly layered: an account-level `STORAGE CREDENTIAL` holds the cloud identity, while granular `EXTERNAL LOCATION` objects govern specific bucket prefixes. We manage all access via SQL grants: pipeline service principals get `READ FILES` and `CREATE TABLE`, while downstream business analysts only ever receive `SELECT` on catalog tables or `READ VOLUME` on governed volumes.*
>
> *The end user never touches the cloud storage layer directly, maintaining complete least-privilege isolation and centralized audit compliance."*
