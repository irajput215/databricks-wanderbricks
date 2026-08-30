"""
01_ingest.py — Bronze: NOAA GSOD daily weather, Auto Loader streaming table.

Reads the public GSOD bucket (s3://noaa-gsod-pds/) incrementally as new
daily files land. Explicit schema = deterministic parsing of the .op.gz CSVs.
Runs as a Delta Live Tables STREAMING TABLE inside wanderbricks_pipeline
(triggered on schedule — not continuous — so compute runs only when new
data arrives; set continuous: true in the pipeline yml for 24/7 pickup).

GSOD reference: https://registry.opendata.aws/noaa-gsod/
Layout: s3://noaa-gsod-pds/<year>/<usaf-wban>-<year>.op.gz, one file per
station per year. Values are in TENTHS (temp/10 = °C, precip/10 = mm);
missing values are sentinels like 9999.9 (handled in 02_clean).
"""
import dlt
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
)

# Subset of the GSOD columns, matched BY NAME against the file header.
GSOD_SCHEMA = StructType([
    StructField("STATION", StringType()),
    StructField("DATE", StringType()),          # yyyyMMdd
    StructField("LATITUDE", DoubleType()),
    StructField("LONGITUDE", DoubleType()),
    StructField("ELEVATION", DoubleType()),
    StructField("NAME", StringType()),
    StructField("TEMP", DoubleType()),          # tenths of °C
    StructField("DEWP", DoubleType()),          # tenths of °C
    StructField("SLP", DoubleType()),
    StructField("STP", DoubleType()),
    StructField("VISIB", DoubleType()),
    StructField("WDSP", DoubleType()),          # tenths of knots
    StructField("MXSPD", DoubleType()),
    StructField("GUST", DoubleType()),
    StructField("MAX", DoubleType()),           # tenths of °C
    StructField("MIN", DoubleType()),           # tenths of °C
    StructField("PRCP", DoubleType()),          # tenths of mm
    StructField("SNDP", DoubleType()),
    StructField("FRSHTT", StringType()),        # fog/rain/snow/hail/thunder/tornado flags
])

# Source bucket. Point at one recent year for the first validation run;
# widen to the bucket root (s3://noaa-gsod-pds/) for full 90+ year history.
SOURCE_PATH = "s3://noaa-gsod-pds/2025/"

# Auto Loader needs a durable location for its schema-tracking state.
# Configured from the pipeline yml (gsod.schema.location); falls back here.
SCHEMA_LOCATION = spark.conf.get(
    "gsod.schema.location",
    "/Volumes/workspace/gsod/_schemas/gsod",  # TODO: point at a real UC volume
)


@dlt.table(
    name="gsod_bronze",
    comment="Real-time raw GSOD daily weather stream from the public NOAA bucket",
)
def gsod_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(GSOD_SCHEMA)
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .load(SOURCE_PATH)
        .withColumn("ingestion_time", current_timestamp())
    )
