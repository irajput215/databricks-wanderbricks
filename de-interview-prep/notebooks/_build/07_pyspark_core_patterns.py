TITLE = "07 - PySpark core patterns"

CELLS = [
    ("md", """**Ten Spark drills on the data you already queried in SQL** - `silver/claims.parquet` (11,981 claims) + `providers` (220).
Each drill is a plan block, a `# YOUR TURN` stub, then the solution. Every output is deliberately tiny.
Spark makes you own what SQL hides: shuffle boundaries, partition counts, and what runs in the JVM vs in Python.
Answer the interview question, then be able to say why the plan looks the way it does."""),

    ("code", """import os
os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable  # venv 3.12; bare python3 here is 3.13
from de_helpers import get_spark, spark_read, has_plan_operator
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType

spark = get_spark("07-pyspark-core")
claims = spark_read(spark, "silver", "claims")
providers = spark_read(spark, "silver", "providers")
print("spark", spark.version, "| claims", claims.count(), "| providers", providers.count())"""),

    ("md", """**1 - Read & inspect.** `spark_read(spark, "silver", "claims")`.
- Parquet is columnar and typed: Spark reads only the projected columns and never parses the ones it skips.
- The same bytes as CSV would be scanned in full, every column arriving as a string that still needs casting.
**Spark note:** projection pushdown is free - `select` the 4 columns you need and the other 11 are never read."""),
    ("code", """# YOUR TURN -> print the schema, the row count, a 3-row peek
# expected columns: claim_id, status, paid_amount, claim_date"""),
    ("code", """print("columns:", len(claims.columns))
claims.printSchema()
print("rows:", claims.count())
claims.select("claim_id", "status", "paid_amount", "claim_date").show(3, truncate=False)"""),

    ("md", """**2 - Aggregation.** Total paid and claim count per provider.
- `groupBy().agg()` and `spark.sql()` compile to the same plan - answer in whichever the interviewer asks for.
- `SUM` skips the NULL `paid_amount` of unadjudicated claims; `COUNT(*)` still counts those rows.
**Spark note:** `groupBy` shuffles by key; 220 groups is trivial, but size `spark.sql.shuffle.partitions` to the data, not to the default."""),
    ("code", """# YOUR TURN -> per provider_id: total_paid, claim_count (top 5 by total_paid)"""),
    ("code", """by_provider = (claims.groupBy("provider_id")
               .agg(F.round(F.sum("paid_amount"), 2).alias("total_paid"),
                    F.count("*").alias("claim_count")))
by_provider.orderBy(F.desc("total_paid")).show(5, truncate=False)

claims.createOrReplaceTempView("claims_v")
spark.sql('''SELECT provider_id, ROUND(SUM(paid_amount), 2) AS total_paid, COUNT(*) AS claim_count
             FROM claims_v GROUP BY provider_id ORDER BY total_paid DESC LIMIT 5''').show(truncate=False)
print("grand total paid:", by_provider.agg(F.round(F.sum("total_paid"), 2)).first()[0])"""),

    ("md", """**3 - Join + broadcast.** claims x providers (220 rows) to get state and specialty.
- `F.broadcast(small)` ships the whole small side to every executor, so the big side is never shuffled.
- Appropriate when one side genuinely fits in memory and is reused; wrong for two large tables (driver OOM, no gain).
**Spark note:** `spark.sql.autoBroadcastJoinThreshold` (10 MB) does this automatically - the hint wins when the optimizer's stats are missing."""),
    ("code", """# YOUR TURN -> state, specialty, total_paid through a broadcast join (top 5)"""),
    ("code", """by_segment = (claims.join(F.broadcast(providers.select("provider_id", "state", "specialty")), "provider_id")
              .groupBy("state", "specialty")
              .agg(F.round(F.sum("paid_amount"), 2).alias("total_paid")))
by_segment.orderBy(F.desc("total_paid")).show(5, truncate=False)
print("broadcast join in the plan:", has_plan_operator(by_segment, "BroadcastHashJoin"))"""),


    ("md", """**4 - Anti-join.** Members with no claims.
- `left_anti` is SQL `NOT EXISTS`: keep left rows with no match, and it can never duplicate the left side.
- `left_join ... WHERE right IS NULL` does fan out when the right side is not unique - the classic bug.
**Spark note:** both sides are shuffled unless the right side is broadcast; the anti-join also pushes the key projection down."""),
    ("code", """# YOUR TURN -> members with zero claims: member_id, first_name, last_name + the count"""),
    ("code", """members = spark_read(spark, "silver", "members")
no_claims = members.join(claims.select("member_id").distinct(), "member_id", "left_anti")
print("members:", members.count(), "| without claims:", no_claims.count())
no_claims.select("member_id", "first_name", "last_name").show(truncate=False)

members.createOrReplaceTempView("members_v")
spark.sql('''SELECT COUNT(*) AS no_claims FROM members_v m
             WHERE NOT EXISTS (SELECT 1 FROM claims_v c WHERE c.member_id = m.member_id)''').show()"""),

    ("md", """**5 - Deduplicate claim versions.** `bronze.claims` = 12,358 rows with 354 duplicated `claim_id` (source versions + a replay batch).
- Keep the newest `updated_at` per claim with `row_number()`, and break ties on `_ingest_batch` so the pick is deterministic.
- Expect 11,996 surviving keys - the same result as the SQL `ROW_NUMBER` drill.
**Spark note:** `row_number` over `partitionBy("claim_id")` is a shuffle, but into many small partitions - the cheap direction."""),
    ("code", """# YOUR TURN -> one row per claim_id: claim_id, updated_at, _ingest_batch (11,996 rows)"""),
    ("code", """bronze = spark_read(spark, "bronze", "claims")
w_latest = Window.partitionBy("claim_id").orderBy(F.col("updated_at").desc(), F.col("_ingest_batch").desc())
latest = bronze.withColumn("rn", F.row_number().over(w_latest)).filter(F.col("rn") == 1).drop("rn")
print("bronze rows:", bronze.count(), "| distinct claim_id:", latest.count())
latest.select("claim_id", "updated_at", "_ingest_batch").show(3, truncate=False)"""),

    ("md", """**6 - Top-N per group.** Top 3 providers per state by total paid.
- Aggregate first, then `rank()` over `Window.partitionBy("state").orderBy(F.desc("total_paid"))`.
- `rank()` keeps ties, so a state can return 4 rows; `row_number()` forces exactly 3.
**Spark note:** the window rides on top of the aggregation shuffle, so there is one shuffle, not two."""),
    ("code", """# YOUR TURN -> state, provider_id, total_paid, rnk where rnk <= 3"""),
    ("code", """totals = (claims.join(F.broadcast(providers.select("provider_id", "state")), "provider_id")
          .groupBy("state", "provider_id")
          .agg(F.round(F.sum("paid_amount"), 2).alias("total_paid")))
by_state = Window.partitionBy("state").orderBy(F.col("total_paid").desc())
top3 = totals.withColumn("rnk", F.rank().over(by_state)).filter(F.col("rnk") <= 3)
print("rows returned (20 states, ties can add):", top3.count())
top3.orderBy("state", "rnk").show(6, truncate=False)"""),

    ("md", """**7 - LAG.** Month-over-month change in total paid.
- `lag()` needs an ordered window, and `Window.orderBy(...)` with no `partitionBy` collapses everything into ONE partition.
- So aggregate to the 18 monthly rows first and window that tiny result - never window the raw claims.
**Spark note:** a single-partition window puts all rows in one task; after the `groupBy` the result is already one partition, so it is free here."""),
    ("code", """# YOUR TURN -> month, total_paid, prev_paid, mom_change (18 rows)"""),
    ("code", """monthly = (claims.groupBy(F.date_format("claim_date", "yyyy-MM").alias("month"))
           .agg(F.round(F.sum("paid_amount"), 2).alias("total_paid")))
mom = (monthly.withColumn("prev_paid", F.lag("total_paid").over(Window.orderBy("month")))
       .withColumn("mom_change", F.round(F.col("total_paid") - F.col("prev_paid"), 2)))
try:
    num_parts = monthly.rdd.getNumPartitions()
except Exception:
    num_parts = "remote (adaptive)"
print("rows into the window stage:", mom.count(), "| partitions:", num_parts)
mom.show(4, truncate=False)"""),


    ("md", """**8 - Conditional aggregation.** Approval rate per provider.
- `SUM(CASE WHEN status = 'Approved' THEN 1 ELSE 0 END) / COUNT(*)` is exactly `F.sum(F.when(...).otherwise(0)) / F.count("*")`.
- Guard the denominator: `F.when(count == 0, None).otherwise(count)` is Spark's `NULLIF(count, 0)` - no divide-by-zero.
**Spark note:** one pass, no extra shuffle; the CASE is folded into the same aggregation as the `COUNT`."""),
    ("code", """# YOUR TURN -> provider_id, claims, approved, approval_rate (top 5 by rate)"""),
    ("code", """denom = F.when(F.col("claims") == 0, F.lit(None)).otherwise(F.col("claims"))   # NULLIF(claims, 0)
rate = (claims.groupBy("provider_id")
        .agg(F.count("*").alias("claims"),
             F.sum(F.when(F.col("status") == "Approved", 1).otherwise(0)).alias("approved"))
        .withColumn("approval_rate", F.round(F.col("approved") / denom, 4)))
rate.orderBy(F.desc("approval_rate"), F.desc("claims")).show(5, truncate=False)"""),

    ("md", """**9 - Nested JSON.** Land 3 nested records in `data/scratch/nested_demo.json`, read them, flatten them.
- Struct access is `F.col("patient.state")`; `F.explode("services")` turns the array into one row per service line.
- Before: nested struct/array. After: flat scalars you can join and aggregate.
**Spark note:** `explode` silently drops rows with an empty array (claim C3) - use `explode_outer` when the parent row must survive."""),
    ("code", """# YOUR TURN -> claim_id, member_id, state, procedure_code, amount (one row per service line)"""),
    ("code", """import json
scratch = pathlib.Path(ROOT) / "data" / "scratch"
scratch.mkdir(parents=True, exist_ok=True)
demo = scratch / "nested_demo.json"
records = [
    {"claim_id": "C1", "patient": {"member_id": "M1", "state": "CA"},
     "services": [{"procedure_code": "99213", "amount": 120.0}, {"procedure_code": "80053", "amount": 45.5}]},
    {"claim_id": "C2", "patient": {"member_id": "M2", "state": "TX"},
     "services": [{"procedure_code": "99214", "amount": 200.0}]},
    {"claim_id": "C3", "patient": {"member_id": "M3", "state": "NY"}, "services": []},
]
demo.write_text("\\n".join(json.dumps(r) for r in records))

is_remote = spark.__module__.startswith("pyspark.sql.connect") or hasattr(spark, "client")
nested = spark.createDataFrame(records) if is_remote else spark.read.json(str(demo))
flat = (nested.select("claim_id", "patient", F.explode("services").alias("svc"))
        .select("claim_id",
                F.col("patient.member_id").alias("member_id"),
                F.col("patient.state").alias("state"),
                F.col("svc.procedure_code").alias("procedure_code"),
                F.col("svc.amount").alias("amount")))
print("BEFORE:"); nested.printSchema()
print("AFTER:"); flat.printSchema()
print("nested rows:", nested.count(), "-> flat service lines:", flat.count())
flat.show(truncate=False)"""),

    ("md", """**10 - Native function vs Python UDF.** Uppercase `status` both ways.
- The UDF ships every batch to a Python worker and back, and Catalyst cannot see inside the lambda to optimise it.
- `F.upper` is a JVM expression: it is code-generated, fused into the scan, and needs no Python on the executor.
**Spark note:** the plan below is the tell - `BatchEvalPython` appears for the UDF and not for the native call."""),
    ("code", """# YOUR TURN -> status_raw, status_native, status_udf for 3 claims"""),
    ("code", """upper_udf = F.udf(lambda s: s.upper() if s else None, StringType())
try:
    both = claims.select(F.col("status").alias("status_raw"),
                         F.upper("status").alias("status_native"),
                         upper_udf("status").alias("status_udf"))
    both.show(3, truncate=False)
    print("native call uses a Python worker:", has_plan_operator(both.select("status_native"), "BatchEvalPython"))
    print("udf call    uses a Python worker:", has_plan_operator(both.select("status_udf"), "BatchEvalPython"))
except Exception as e:
    print("Python worker UDF notice:", type(e).__name__)
    claims.select(F.col("status").alias("status_raw"), F.upper("status").alias("status_native")).show(3, truncate=False)
    print("native call uses a Python worker:", has_plan_operator(claims.select(F.upper("status")), "BatchEvalPython"))"""),



    ("md", """**Interview checklist:** projection pushdown, one shuffle per aggregation, broadcast the small side, dedupe before you join, aggregate before you window."""),

    ("code", """spark.stop()
print("spark stopped - run the next notebook in a fresh kernel")"""),
]
