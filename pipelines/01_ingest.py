"""
01_ingest.py — Auto Loader ingestion into bronze.

Streams raw files from S3 into `dev.bronze.sales_raw` (Delta) with
schema tracking and checkpoints.

TODO(schema): fill in your S3 path, format, and column expectations once the
raw data is attached. See README Part 2, step 1.
"""
from pyspark.sql import DataFrame


def build_raw_stream(source_path: str) -> DataFrame:
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")  # TODO: parquet/json per your data
        .option("cloudFiles.schemaLocation", "/Volumes/dev/bronze/_schemas/sales")
        .load(source_path)
    )


def main() -> None:
    source_path = "s3://your-bucket/raw/sales/"  # TODO: real bucket path
    df = build_raw_stream(source_path)
    df.writeStream \
        .format("delta") \
        .option("checkpointLocation", "/Volumes/dev/bronze/_checkpoints/sales") \
        .table("dev.bronze.sales_raw")


if __name__ == "__main__":
    main()
