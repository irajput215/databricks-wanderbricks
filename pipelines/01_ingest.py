"""
01_ingest.py — Bronze: NOAA GSOD daily weather, Auto Loader streaming table.

Reads the public GSOD bucket (s3://noaa-gsod-pds/) incrementally as new
daily files land. The AWS noaa-gsod-pds bucket stores files as
`<year>/<station>.csv` — plain quoted CSV, ONE file per station per year —
with US units: temperatures in °F, precipitation in inches, wind in knots.
Unit conversion to metric happens in 02_clean (silver).

The schema below is the FULL 28-column header (including the *_ATTRIBUTES
quality-flag columns) so column mapping is unambiguous — a subset schema
misaligned after NAME (verified on a failed run).

GSOD reference: https://registry.opendata.aws/noaa-gsod/
"""
import dlt
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
)

# Full 28-column header of the noaa-gsod-pds CSVs, in header order.
GSOD_SCHEMA = StructType([
    StructField("STATION", StringType()),
    StructField("DATE", StringType()),            # yyyy-MM-dd
    StructField("LATITUDE", DoubleType()),
    StructField("LONGITUDE", DoubleType()),
    StructField("ELEVATION", DoubleType()),
    StructField("NAME", StringType()),
    StructField("TEMP", DoubleType()),            # °F
    StructField("TEMP_ATTRIBUTES", StringType()),
    StructField("DEWP", DoubleType()),            # °F
    StructField("DEWP_ATTRIBUTES", StringType()),
    StructField("SLP", DoubleType()),             # hPa
    StructField("SLP_ATTRIBUTES", StringType()),
    StructField("STP", DoubleType()),             # hPa
    StructField("STP_ATTRIBUTES", StringType()),
    StructField("VISIB", DoubleType()),           # miles
    StructField("VISIB_ATTRIBUTES", StringType()),
    StructField("WDSP", DoubleType()),            # knots
    StructField("WDSP_ATTRIBUTES", StringType()),
    StructField("MXSPD", DoubleType()),           # knots
    StructField("GUST", DoubleType()),            # knots
    StructField("MAX", DoubleType()),             # °F
    StructField("MAX_ATTRIBUTES", StringType()),
    StructField("MIN", DoubleType()),             # °F
    StructField("MIN_ATTRIBUTES", StringType()),
    StructField("PRCP", DoubleType()),            # inches
    StructField("PRCP_ATTRIBUTES", StringType()),
    StructField("SNDP", DoubleType()),            # inches
    StructField("FRSHTT", StringType()),          # fog/rain/snow/hail/thunder/tornado flags
])

# Source bucket. Point at one recent year for validation; widen to the bucket
# root for full 90+ year history.
SOURCE_PATH = "s3://noaa-gsod-pds/2025/"

# Auto Loader needs a durable location for its schema-tracking state.
# Configured from the pipeline yml (gsod.schema.location); falls back here.
# NOTE: this path must be FRESH (or emptied) when the schema changes — Auto
# Loader persists inferred/evolved schema state here and evolves rather than
# replaces it, which silently misaligned columns after a schema change.
SCHEMA_LOCATION = spark.conf.get(
    "gsod.schema.location",
    "/Volumes/workspace/gsod/_schemas/gsod_v2",
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
        .option("cloudFiles.schemaEvolutionMode", "none")
        .load(SOURCE_PATH)
        .withColumn("ingestion_time", current_timestamp())
    )
