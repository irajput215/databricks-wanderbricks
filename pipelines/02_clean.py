"""
02_clean.py — DLT pipeline: bronze -> silver with quality expectations.

Runs as a Delta Live Tables pipeline. Adds deduplication, type casts, and
data-quality gates (@dlt.expect). See README Part 2, step 2.
"""
import dlt
from pyspark.sql.functions import col

TARGET_SCHEMA = "dev.silver"  # TODO: your catalog.schema


@dlt.table
@dlt.expect("valid_key", "id IS NOT NULL")
@dlt.expect("valid_date", "sale_date IS NOT NULL")
@dlt.expect("non_negative_value", "value >= 0")
def sales_clean():
    return (
        dlt.read("sales_raw")  # from the bronze pipeline, or read_stream as needed
        .filter(col("id").isNotNull())
        .withColumn("sale_date", col("sale_date").cast("date"))
        .dropDuplicates(["id", "sale_date"])
    )
