Searched for "cloudFiles"

Here is the exact **Auto Loader (`cloudFiles`)** production code and the **options breakdown** you should know inside and out for your interview.

---

### 1. The Production PySpark Auto Loader Code

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Define explicit baseline schema
claims_raw_schema = StructType([
    StructField("claim_id", StringType(), False),
    StructField("member_id", StringType(), False),
    StructField("provider_id", StringType(), False),
    StructField("service_date", StringType(), True),
    StructField("billed_amount", DoubleType(), True),
    StructField("paid_amount", DoubleType(), True)
])

# 2. Read incrementally with Auto Loader (cloudFiles)
raw_stream = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")                                    # Source file format
         .option("cloudFiles.useNotifications", "false")                         # Directory Listing Mode!
         .option("cloudFiles.schemaLocation", "abfss://checkpoints@.../schema/") # Persistent schema path
         .option("cloudFiles.schemaEvolutionMode", "rescue")                     # Catch schema drift in _rescued_data
         .option("cloudFiles.maxFilesPerTrigger", 50)                            # Rate limiting batch size
         .schema(claims_raw_schema)
         .load("abfss://landing@carelonstorage.dfs.core.windows.net/clearinghouse/claims/")
)

# 3. Append ingestion audit metadata
bronze_stream = (
    raw_stream
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingest_batch_id", F.lit(batch_id))
)

# 4. Write incrementally as a Scheduled Batch Job (trigger availableNow)
query = (
    bronze_stream.writeStream
                 .format("delta")
                 .outputMode("append")
                 .option("checkpointLocation", "abfss://checkpoints@.../bronze_claims/")
                 .trigger(availableNow=True)  # Key: processes new files as a batch, then shuts down!
                 .toTable("prod.bronze.claims_raw")
)
```

---

### 2. The 5 Core Options Interviewers Grill On (And "Why" You Chose Them)

#### Option 1: `cloudFiles.useNotifications = "false"` (Directory Listing Mode)
* **What it means:** Spark discovers newly landed files by listing the directory in ADLS Gen2, maintaining an internal RocksDB state store of processed files.
* **Why you chose it:** 
  > *"We chose Directory Listing mode because our clearinghouse files arrived in **predictable daily batches** (about 10 to 30 large compressed files per morning). It required **zero additional Azure infrastructure**—no Event Grid subscriptions or Azure Storage Queues, and no elevated Azure IAM permissions. Directory listing is fast and cost-effective for batch arrivals under a few thousand files per run."*
* **When would you use File Notifications (`true`)?**  
  > *"Only if files were landing continuously every few seconds (millions of small files), where directory listing calls to cloud storage become a bottleneck."*

---

#### Option 2: `.trigger(availableNow=True)` (The Batch vs. Streaming Question)
* **The Interviewer Trap:** *"Auto Loader uses `readStream` and `writeStream`. Were you paying for a 24/7 running streaming cluster?"*
* **Your Killer Answer:**
  > *"**No, absolutely not.** We used **`trigger(availableNow=True)`** (or `trigger(once=True)` in DBR 10.4). This gives us all the benefits of streaming—incremental checkpointing and state tracking—**with the cost profile of a scheduled batch job**. 
  > Spark spins up on an automated Databricks Workflow job cluster at 2:30 AM, processes all newly arrived files since the last run, commits the checkpoint, and **automatically shuts the cluster down** in ~27 minutes."*

---

#### Option 3: `cloudFiles.schemaEvolutionMode = "rescue"`
* **What it does:** If an upstream vendor sends an unexpected new column, or an integer field suddenly contains a string (like `"N/A"`), Spark does **not** fail the job. 
* **The `_rescued_data` column:** Auto Loader automatically adds a hidden column called `_rescued_data` and stores the malformed or unmapped columns as a JSON string!
* **Why you chose it:**
  > *"We used `rescue` mode so unexpected vendor payload changes wouldn't crash our 2:30 AM production pipeline. Valid columns loaded into Bronze as expected, while corrupt or unmapped attributes were captured in `_rescued_data` for our quarantine audit jobs to inspect."*
* **Other modes if asked:**
  * `addNewColumns`: Automatically alters the Delta table schema to add newly discovered columns.
  * `failOnNewColumns`: Immediately throws an exception if any column doesn't match the schema (strict contract).

---

#### Option 4: `cloudFiles.schemaLocation`
* **What it does:** A directory in cloud storage (ADLS Gen2) where Auto Loader stores the schema protocol across runs.
* **Why it matters:** Auto Loader needs a persistent location to persist the evolving schema; without it, Spark cannot resume incremental schema tracking after cluster restarts.

---

#### Option 5: `cloudFiles.maxFilesPerTrigger = 50` (Backpressure & Rate Limiting)
* **What it does:** Limits the number of files processed in a single micro-batch.
* **Why you chose it:**
  > *"It protects the cluster from Out-Of-Memory (OOM) errors during backfill scenarios. If an upstream clearinghouse was down for 4 days and suddenly dumped 500 backlogged files at once, `maxFilesPerTrigger` ensured Spark processed them in predictable 50-file micro-batches rather than choking the driver."*

---

### The 30-Second Interview Summary

> *"We used Databricks Auto Loader (`cloudFiles`) configured in **Directory Listing mode** because our files arrived in predictable daily batches from ADLS Gen2, eliminating the need for Azure Event Grid queues. 
> 
> We used **`trigger(availableNow=True)`** to run as an incremental batch job on an automated job cluster, keeping compute costs low. 
> 
> To handle upstream schema drift, we set **`schemaEvolutionMode = 'rescue'`**, which diverted malformed or unexpected attributes into the **`_rescued_data`** column instead of crashing our production run."*