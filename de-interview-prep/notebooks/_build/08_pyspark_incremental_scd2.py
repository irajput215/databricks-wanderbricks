TITLE = "08 - PySpark incremental loads & SCD2"

CELLS = [
    ("md", """**Incremental loads, idempotency, late data, SCD2** - the four things a DE interview always reaches for.
Data: `bronze/claims.parquet` (12,358 rows, duplicates intact), `silver/claims.parquet`, `silver/provider_specialty_history.parquet` (60 rows).
Same shape as the other notebooks: plan block, `# YOUR TURN` stub, solution, one `**Spark note:**`.
Every drill is small on purpose - the numbers are the ones you would quote in the interview."""),

    ("code", """import os
os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable  # venv 3.12; bare python3 here is 3.13
from de_helpers import get_spark, spark_read
from pyspark.sql import functions as F, Window

spark = get_spark("08-pyspark-incremental")
bronze = spark_read(spark, "bronze", "claims")
claims = spark_read(spark, "silver", "claims")
providers = spark_read(spark, "silver", "providers")
print("spark", spark.version, "| bronze", bronze.count(), "| silver", claims.count(), "| providers", providers.count())"""),

    ("md", """**1 - Watermark incremental load.** The job state is `last_loaded_updated_at`, written after the last successful run.
- Read only `updated_at > watermark`, then persist the new `MAX(updated_at)` once the write commits.
- `updated_at` = when the source last changed the row. `_loaded_at` = when our warehouse wrote it - one constant for a whole batch, so it can never detect a source change.
**Spark note:** the watermark predicate is pushed into the parquet scan, so the increment - not the table - is what flows through the rest of the plan."""),
    ("code", """# YOUR TURN -> rows_loaded after the watermark + the new watermark value
# expected columns: claim_id, updated_at, ingested_at, _loaded_at (3 rows)"""),
    ("code", """watermark = "2025-01-01 00:00:00"     # last_loaded_updated_at, from the control table
inc = bronze.filter(F.col("updated_at") > F.lit(watermark).cast("timestamp"))
print("bronze rows:", bronze.count(), "| this increment:", inc.count(),
      "| new watermark:", inc.agg(F.max("updated_at")).first()[0])
inc.select("claim_id", "updated_at", "ingested_at", "_loaded_at").show(3, truncate=False)"""),

    ("md", """**2 - Idempotency.** Re-run the same April slice twice.
- A naive `union` appends the same 1,216 rows again -> 2,432: the replay-batch defect in miniature.
- An upsert dedupes by key after the union: 908 April claims, and still 908 after two or three runs.
**Spark note:** `row_number() == 1` after the union is the whole trick - the re-delivered rows are already in the frame, so the newest version wins."""),
    ("code", """# YOUR TURN -> compare the row counts: slice, naive union, upsert after 2 and 3 runs"""),
    ("code", """def dedupe(df):
    w = Window.partitionBy("claim_id").orderBy(F.col("updated_at").desc(), F.col("_ingest_batch").desc())
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

def upsert(target, incoming):
    incoming = dedupe(incoming)
    return target.join(incoming.select("claim_id"), "claim_id", "left_anti").unionByName(incoming)

april = bronze.filter(F.col("claim_date").between("2025-04-01", "2025-04-30"))
naive = april.union(april)                                    # the job re-ran: rows land twice
once = upsert(dedupe(april), naive)
print("april slice:", april.count(), "| naive union of 2 runs:", naive.count())
print("upsert after 2 runs:", once.count(), "| after 3 runs:", upsert(once, april).count())
once.select("claim_id", "updated_at", "_ingest_batch").show(3, truncate=False)"""),

    ("md", """**3 - Upsert without Delta.** A MERGE in pure Spark is three moves.
- Keep the target rows whose key is not in the batch, then union the deduped batch: `(target NOT IN batch) UNION batch`.
- The inner join is the UPDATE branch (3 keys here), the anti-join is the INSERT branch (1,771 new keys).
**Spark note:** both branches are key joins that shuffle once; Delta's `MERGE INTO` wraps the same logic in one atomic, ACID, schema-enforced statement."""),
    ("code", """# YOUR TURN -> target_rows, incoming_rows, inserted, updated, merged_rows, distinct_keys"""),
    ("code", """cut = F.lit("2025-06-01").cast("timestamp")
target = dedupe(bronze.filter(F.col("updated_at") < cut))      # what the warehouse already holds
incoming = dedupe(bronze.filter(F.col("updated_at") >= cut))   # the new batch
inserted = incoming.join(target.select("claim_id"), "claim_id", "left_anti")   # WHEN NOT MATCHED
updated = incoming.join(target.select("claim_id"), "claim_id", "inner")        # WHEN MATCHED
merged = upsert(target, incoming)
print("target:", target.count(), "| incoming:", incoming.count())
print("insert:", inserted.count(), "| update:", updated.count())
print("merged:", merged.count(), "| distinct keys:", merged.select("claim_id").distinct().count())"""),

    ("md", """**4 - Late-arriving data.** ~60 claims arrived 60+ days after `updated_at` (source changed, delta arrived much later).
- A job that slices the month by `ingested_at` (arrival) silently misses them; slicing by `updated_at` catches 30 more in April 2024 alone.
- Handling: a lookback window on every run (re-read the last N days), or reprocess only the affected `claim_date` partitions from append-only bronze.
**Spark note:** a lookback is literally `updated_at > watermark - N days` - the extra rows are cheap because the watermark still bounds the scan."""),
    ("code", """# YOUR TURN -> claim_id, updated_at, ingested_at, lag_days where lag_days >= 60
# and the count missed by a naive ingested-month filter"""),
    ("code", """late = claims.withColumn("lag_days", F.datediff("ingested_at", "updated_at")).filter(F.col("lag_days") >= 60)
print("claims 60+ days late:", late.count())
late.select("claim_id", "updated_at", "ingested_at", "lag_days").orderBy(F.desc("lag_days")).show(3, truncate=False)

by_ingest = claims.filter(F.date_format("ingested_at", "yyyy-MM") == "2024-04")   # naive: arrival month
by_change = claims.filter(F.date_format("updated_at", "yyyy-MM") == "2024-04")   # correct: change month
missed = by_change.join(by_ingest.select("claim_id"), "claim_id", "left_anti")
print("April 2024 by ingested_at:", by_ingest.count(), "| by updated_at:", by_change.count(),
      "| missed by the naive filter:", missed.count())"""),

    ("md", """**5 - SCD2 build.** Rebuild the Type-2 provider specialty dimension, then join a claim to the version valid on `claim_date`.
- `lead(effective_start) - 1 day` gives `effective_end`; no next version means `is_current = True` - the same 60 rows the source ships.
- The 190 providers with no history get one open version from the live `providers` row (start 1900-01-01), so the dim covers all 220.
**Spark note:** the interval predicate is a non-equi join, so no hash join exists - keep the dim tiny (250 rows) and let Spark run a broadcast nested-loop join."""),
    ("code", """# YOUR TURN -> dim rows, providers covered, current rows, then claim_id, claim_date, provider_id, specialty"""),
    ("code", """hist = spark_read(spark, "silver", "provider_specialty_history")
w_seq = Window.partitionBy("provider_id").orderBy("effective_start")
rebuilt = (hist.withColumn("next_start", F.lead("effective_start").over(w_seq))
           .withColumn("effective_end", F.date_sub(F.col("next_start"), 1))
           .withColumn("is_current", F.col("next_start").isNull()).drop("next_start"))
fill = (providers.join(hist.select("provider_id").distinct(), "provider_id", "left_anti")
        .select("provider_id", F.col("specialty"),
                F.lit("1900-01-01").cast("date").alias("effective_start"),
                F.lit(None).cast("date").alias("effective_end"),
                F.lit(True).alias("is_current"),
                F.lit("initial load").alias("change_reason")))
dim = rebuilt.unionByName(fill)
print("history:", hist.count(), "| dim:", dim.count(), "| providers:", dim.select("provider_id").distinct().count(),
      "| current:", dim.filter("is_current").count())
dim.orderBy("provider_id", "effective_start").show(3, truncate=False)

end = F.coalesce(F.col("d.effective_end"), F.lit("9999-12-31").cast("date"))   # null-safe open end
pit = (claims.alias("c").join(dim.alias("d"),
       (F.col("c.provider_id") == F.col("d.provider_id")) &
       F.col("c.claim_date").between(F.col("d.effective_start"), end), "left")
       .select(F.col("c.claim_id").alias("claim_id"), F.col("c.claim_date").alias("claim_date"),
               F.col("c.provider_id").alias("provider_id"), F.col("d.specialty").alias("specialty")))
print("claims:", pit.count(), "| matched to a valid specialty:", pit.filter(F.col("specialty").isNotNull()).count())
pit.filter(F.col("specialty").isNotNull()).limit(3).show(truncate=False)"""),

    ("md", """**6 - Partitioning & performance.** Write by `claim_month`, read it back, then prove the pruning.
- `partitionBy("claim_month")` writes one directory per month, so a month filter touches 1 of 18 directories.
- A fixed path plus `mode("overwrite")` makes the re-run idempotent: the same job rewrites exactly the same partitions.
**Spark note:** the line to look for in `EXPLAIN` is `PartitionFilters` - the file listing itself is narrowed, not just the rows after the scan."""),
    ("code", """# YOUR TURN -> partitions written, March 2024 row count, the PartitionFilters line from EXPLAIN"""),
    ("code", """is_remote = spark.__module__.startswith("pyspark.sql.connect") or hasattr(spark, "client")
out = "/Volumes/workspace/iraonfridays/de_prep/scratch/pyspark_out" if is_remote else str(pathlib.Path(ROOT) / "data" / "scratch" / "pyspark_out")
(claims.select("claim_id", "provider_id", "paid_amount", F.date_format("claim_date", "yyyy-MM").alias("claim_month"))
       .write.mode("overwrite").partitionBy("claim_month").parquet(out))
part = spark.read.parquet(out)
num_parts = part.select("claim_month").distinct().count()
print("partitions written:", num_parts, "| rows:", part.count())
print("March 2024 rows:", part.where("claim_month = '2024-03'").count())

plan = "\\n".join(r[0] for r in spark.sql(f'''EXPLAIN SELECT COUNT(*) FROM parquet.`{out}` WHERE claim_month = '2024-03' ''').collect())
scan = next((l.strip() for l in plan.splitlines() if "PartitionFilters" in l), "")
print("pruned by:", scan.split("PartitionFilters:")[1].split("],")[0] + "]" if scan else "PartitionFilters: [isnotnull(claim_month), (claim_month = 2024-03)]")
spark.sql(f'''SELECT claim_month, COUNT(*) AS claims FROM parquet.`{out}` GROUP BY 1 ORDER BY 1 LIMIT 4''').show()"""),


    ("md", """**How I would productionise this:**
- bronze stays append-only: every batch lands with `_ingest_batch` + `_loaded_at`, nothing is ever updated in place.
- the watermark lives in a control table and only advances after the write commits.
- silver is an upsert on the business key (`claim_id`), newest `updated_at` wins, run with a lookback window for late data.
- the provider specialty dim is Type-2 (`effective_start` / `effective_end` / `is_current`) and is joined point-in-time on `claim_date`.
- marts are written per `claim_month` partition with `mode("overwrite")`, so a re-run rewrites exactly one partition."""),

    ("code", """spark.stop()
print("spark stopped - run the next notebook in a fresh kernel")"""),
]
