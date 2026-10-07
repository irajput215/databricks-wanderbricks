"""
Helpers imported by every notebook.

    from de_helpers import q, q1, assert_grain, tables
"""
from __future__ import annotations

import pathlib

import duckdb
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "warehouse" / "healthcare.duckdb"
SILVER = ROOT / "data" / "silver"
BRONZE = ROOT / "data" / "bronze"
GOLD = ROOT / "data" / "gold"

pd.set_option("display.max_columns", 60)
pd.set_option("display.width", 200)
pd.set_option("display.max_rows", 100)
pd.set_option("display.float_format", lambda v: f"{v:,.2f}")

_con: duckdb.DuckDBPyConnection | None = None


def connect(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Open (once) the practice warehouse."""
    global _con
    if _con is None:
        _con = duckdb.connect(str(DB_PATH), read_only=read_only)
    return _con


def q(sql: str, params: list | None = None) -> pd.DataFrame:
    """Run SQL and return a DataFrame - the only function you need for the SQL drills."""
    return connect().sql(sql).df() if params is None else connect().execute(sql, params).df()


def q1(sql: str):
    """Run SQL and return a single value (first row, first column)."""
    return connect().sql(sql).fetchone()[0]


def explain(sql: str) -> pd.DataFrame:
    """Show DuckDB's physical plan - useful for the performance discussions."""
    return connect().sql(f"EXPLAIN {sql}").df()


def tables(schema: str = "main", engine: str = "auto") -> pd.DataFrame:
    """List tables/views in a schema (DuckDB or Databricks Unity Catalog)."""
    if engine == "spark" or (engine == "auto" and schema in ("healthcare", "workspace.healthcare")):
        try:
            s = get_spark()
            target_schema = schema.split(".")[-1]
            return s.sql(f"SHOW TABLES IN workspace.{target_schema}").toPandas()
        except Exception:
            pass
    return q(f"""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables WHERE table_schema = '{schema}' ORDER BY table_name
    """)


def columns(table: str, engine: str = "auto") -> pd.DataFrame:
    """Column names and types for a table (schema-qualified or main.*)."""
    if engine == "spark" or engine == "auto":
        try:
            s = get_spark()
            tbl_name = table if "." in table else f"workspace.healthcare.{table}"
            return s.sql(f"DESCRIBE {tbl_name}").toPandas()
        except Exception:
            pass
    name = table if "." in table else f"main.{table}"
    return q(f"DESCRIBE {name}")



def assert_grain(df, *keys: str, verbose: bool = True) -> bool:
    """Check that a result really is one row per key combination.

    Supports both pandas DataFrames (from DuckDB q) and PySpark DataFrames (from spark.table).
    This is the habit that catches fan-out bugs: run it on every result you produce.
    """
    # PySpark DataFrame
    if hasattr(df, "dropDuplicates") and hasattr(df, "count"):
        total = df.count()
        if total == 0:
            if verbose:
                print("empty result - nothing to check")
            return True
        unique = df.dropDuplicates(list(keys)).count()
        dupes = total - unique
        label = " + ".join(keys)
        if dupes == 0:
            if verbose:
                print(f"OK   one row per [{label}] - {total:,} rows")
            return True
        print(f"FAIL {dupes:,} duplicate rows for [{label}] - the grain is finer than you think")
        if verbose:
            from pyspark.sql import functions as F, Window
            w = Window.partitionBy(*keys)
            sample = (
                df.withColumn("_dupe_cnt", F.count("*").over(w))
                .filter(F.col("_dupe_cnt") > 1)
                .drop("_dupe_cnt")
                .limit(5)
                .toPandas()
            )
            print(sample.to_string(index=False))
        return False

    # pandas DataFrame
    if df.empty:
        if verbose:
            print("empty result - nothing to check")
        return True
    dupes = int(df.duplicated(subset=list(keys)).sum())
    label = " + ".join(keys)
    if dupes == 0:
        if verbose:
            print(f"OK   one row per [{label}] - {len(df):,} rows")
        return True
    sample = df[df.duplicated(subset=list(keys), keep=False)].head(5)
    print(f"FAIL {dupes:,} duplicate rows for [{label}] - the grain is finer than you think")
    if verbose:
        print(sample.to_string(index=False))
    return False



_spark = None
_w = None


def get_dbutils(profile: str = "irajput"):
    """Get Databricks dbutils object using authenticated Databricks CLI profile."""
    global _w
    from databricks.sdk import WorkspaceClient

    if _w is None:
        _w = WorkspaceClient(profile=profile)
    return _w.dbutils


def fs_ls(path: str = "/Volumes/workspace/iraonfridays/de_prep/silver") -> pd.DataFrame:
    """Pretty-print Databricks Volumes or DBFS directory contents as a DataFrame.

    Example:
        fs_ls("silver")
        fs_ls("/Volumes/workspace/iraonfridays/de_prep/silver")
    """
    if not path.startswith(("/Volumes", "dbfs:/")):
        path = f"/Volumes/workspace/iraonfridays/de_prep/{path}"
    items = get_dbutils().fs.ls(path)
    df = pd.DataFrame(items)
    if not df.empty and "size" in df.columns:
        df["size_kb"] = (df["size"] / 1024).round(1)
        cols = [c for c in ["name", "size_kb", "size", "modificationTime", "path"] if c in df.columns]
        df = df[cols]
    return df


def display(obj, limit: int = 100):
    """Databricks display() function that works both on Databricks and in VS Code notebooks.

    Renders Spark DataFrames, dbutils.fs.ls output, and pandas DataFrames as rich DB-like tables.
    """
    try:
        from IPython.display import display as ipy_display
    except ImportError:
        ipy_display = None

    # 1. Spark DataFrame -> convert to pandas for rich HTML rendering
    if hasattr(obj, "toPandas") and callable(getattr(obj, "toPandas")):
        pdf = obj.limit(limit).toPandas() if hasattr(obj, "limit") else obj.toPandas()
        return ipy_display(pdf) if ipy_display else print(pdf.to_string())

    # 2. dbutils.fs.ls output list of FileInfo -> render as table
    if isinstance(obj, list) and obj and hasattr(obj[0], "path") and hasattr(obj[0], "name"):
        pdf = pd.DataFrame(obj)
        if "size" in pdf.columns:
            pdf["size_kb"] = (pdf["size"] / 1024).round(1)
            cols = [c for c in ["name", "size_kb", "size", "modificationTime", "path"] if c in pdf.columns]
            pdf = pdf[cols]
        return ipy_display(pdf) if ipy_display else print(pdf.to_string())

    # 3. DuckDB relation
    if hasattr(obj, "df") and callable(getattr(obj, "df")):
        pdf = obj.df()
        return ipy_display(pdf) if ipy_display else print(pdf.to_string())

    # 4. Standard objects / pandas DataFrame
    if ipy_display:
        return ipy_display(obj)
    print(obj)




def get_spark(
    app_name: str = "de-prep",
    cores: int = 2,
    use_databricks: bool | None = None,
    profile: str | None = None,
    cluster_id: str | None = None,
    catalog: str = "workspace",
    schema: str = "healthcare",
):
    """SparkSession constructor supporting both local Spark and Databricks Connect.

    If Databricks Connect is available or use_databricks=True, connects to Databricks
    (serverless or cluster) using the Databricks CLI authentication profile (like in
    databricks-forecasting/notebooks). Sets default catalog and schema.
    """
    global _spark
    if _spark is not None:
        return _spark

    import os
    import sys

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    # Auto-detect Databricks Connect if use_databricks is not explicitly set
    if use_databricks is None:
        try:
            from databricks.connect import DatabricksSession  # noqa: F401
            use_databricks = True
        except ImportError:
            use_databricks = False

    if use_databricks:
        from databricks.connect import DatabricksSession

        builder = DatabricksSession.builder
        profile_name = profile or os.environ.get("DATABRICKS_CONFIG_PROFILE", "irajput")
        builder = builder.profile(profile_name)

        if cluster_id:
            builder = builder.clusterId(cluster_id)
        else:
            builder = builder.serverless()

        _spark = builder.getOrCreate()
        try:
            _spark.sql(f"USE CATALOG {catalog}")
            _spark.sql(f"USE SCHEMA {schema}")
        except Exception:
            pass
        return _spark

    from pyspark.sql import SparkSession

    _spark = (
        SparkSession.builder.master(f"local[{cores}]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .getOrCreate()
    )
    _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def spark_q(sql: str) -> pd.DataFrame:
    """Run SQL query directly on Databricks Serverless Compute (workspace.healthcare) and return a pandas DataFrame."""
    spark = get_spark()
    return spark.sql(sql).toPandas()



def spark_read(
    spark,
    layer: str,
    table: str,
    volume_base: str = "/Volumes/workspace/iraonfridays/de_prep",
):
    """Read a parquet export as a Spark DataFrame: spark_read(spark, 'silver', 'claims').

    Seamlessly supports both local PySpark and remote Databricks Connect:
    1. If running with Databricks Connect, first attempts to read from Unity Catalog Volume
       (e.g., /Volumes/workspace/iraonfridays/de_prep/<layer>/<table>.parquet).
    2. If the volume path does not exist, loads local parquet via pandas and creates a Spark DataFrame.
    3. If running with local PySpark, reads directly from the local filesystem.
    """
    is_remote = (
        spark.__module__.startswith("pyspark.sql.connect")
        or hasattr(spark, "client")
        or "DatabricksSession" in type(spark).__name__
    )

    if is_remote:
        volume_path = f"{volume_base}/{layer}/{table}.parquet"
        try:
            return spark.read.parquet(volume_path)
        except Exception:
            local_path = ROOT / "data" / layer / f"{table}.parquet"
            if local_path.exists():
                import pandas as pd
                return spark.createDataFrame(pd.read_parquet(local_path))
            raise

    return spark.read.parquet(str(ROOT / "data" / layer / f"{table}.parquet"))


def plan_string(df) -> str:
    """Return the physical execution plan as a string, compatible with both local Spark and Databricks Connect."""
    if hasattr(df, "_jdf"):
        try:
            return df._jdf.queryExecution().executedPlan().toString()
        except Exception:
            pass
    import contextlib
    import io

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain()
    return buf.getvalue()


def has_plan_operator(df, operator_name: str) -> bool:
    """Check if an operator appears in the physical plan (case-insensitive)."""
    return operator_name.lower() in plan_string(df).lower()


