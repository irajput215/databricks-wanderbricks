"""Pipeline 2 of 2 -- the drone imagery branch (Lakeflow / Spark Declarative Pipelines).

Runs every morning next to the farm-conditions pipeline, on its own branch of
the job. It first checks whether a new flight manifest has arrived; when the
drone has not flown, there is nothing new and the branch finishes in seconds.

This demo covers the *flight metadata* half of the imagery pipeline (the half
that needed the most data engineering). The demo intentionally does not ship the
tiling / blur-scoring / weed model: see docs/RESUME_CHEATSHEET.md for how to talk
about it honestly.

RAW (UC Volume, written by the field uploader)
  -> bronze_flight_manifests   one JSON manifest per completed flight, written last
  -> silver_flight_status      one row per flight: image counts + completeness check
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RAW_PATH = spark.conf.get("leher.raw_path", "data/raw")

MANIFEST_SCHEMA = (
    "struct<flight_id:string,farm_id:string,flight_date:string,uploaded_at:string,"
    "image_count:int,size_mb:bigint,images:array<string>>"
)


@dp.table(
    name="bronze_flight_manifests",
    comment="Raw flight manifests. The field uploader writes _manifest.json last, after images land",
)
def bronze_flight_manifests() -> DataFrame:
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("recursiveFileLookup", "true")
        .load(f"{RAW_PATH}/source=drone")
        .select(
            F.col("path").alias("_source_file"),
            F.col("content").cast("string").alias("_value"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )


def _parse_flight_status(raw: DataFrame) -> DataFrame:
    """Turn one manifest per flight into status rows, keyed by flight_id."""
    parsed = raw.select(
        "_source_file",
        "_ingested_at",
        F.from_json("_value", MANIFEST_SCHEMA).alias("m"),
    ).filter(F.col("m.flight_id").isNotNull())  # ignore stray files, e.g. partial uploads
    return parsed.select(
        F.coalesce(
            F.col("m.flight_id"), F.regexp_extract("_source_file", r"flight_id=([^/]+)", 1)
        ).alias("flight_id"),
        F.col("m.farm_id").alias("farm_id"),
        F.to_date(F.col("m.flight_date")).alias("flight_date"),
        F.to_timestamp(F.col("m.uploaded_at")).alias("uploaded_at"),
        F.col("m.image_count").alias("image_count"),
        F.size(F.col("m.images")).alias("images_listed"),
        F.col("m.size_mb").alias("size_mb"),
        # The field uploader writes the manifest last, so a mismatch means the
        # upload was incomplete and the flight should be re-sent, not processed.
        F.when(F.size(F.col("m.images")) == F.col("m.image_count"), "complete")
        .otherwise("incomplete")
        .alias("upload_status"),
        # A late flight (flown earlier, uploaded this morning) is still processed:
        # that is why the branch runs daily even though the drone flies weekly.
        F.to_date(F.regexp_extract("_source_file", r"date=(\d{4}-\d{2}-\d{2})", 1)).alias(
            "received_date"
        ),
        "_source_file",
        "_ingested_at",
    )


@dp.table(
    name="silver_flight_status",
    comment="One row per uploaded drone flight: image count, completeness, upload placement",
)
def silver_flight_status() -> DataFrame:
    return _parse_flight_status(spark.readStream.table("bronze_flight_manifests"))
