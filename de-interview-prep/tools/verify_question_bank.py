"""
Verify the interview question bank: every question must have a working DuckDB SQL solution
AND a working PySpark solution, and the two must return the same rows.

    python tools/verify_question_bank.py                        # the whole bank (spec)
    python tools/verify_question_bank.py 07 08                  # only questions 7 and 8
    python tools/verify_question_bank.py --part data/scratch/qbank/part1.json
    python tools/verify_question_bank.py --no-spark             # DuckDB half only

The Spark half needs a real PySpark (a local Spark session), so run this with the throwaway
verification venv, not the Databricks Connect one:

    uv venv data/scratch/qbank-venv --python 3.12
    uv pip install --python data/scratch/qbank-venv/bin/python "pyspark==4.0.0" duckdb pandas pyarrow
    JAVA_HOME=$(/usr/libexec/java_home -v 21) data/scratch/qbank-venv/bin/python tools/verify_question_bank.py

(JAVA_HOME must point at Java 17-21; the JDK 23/24 on this machine breaks Spark's Hadoop shim.)

Question contract (enforced here)
---------------------------------
tables  : name starts with "q<NN>_" (NN = question number), `cols` is the same DDL for both
          engines, `rows` is a list of tuples in column order.
sql     : ONE DuckDB statement (SELECT/WITH ...), no trailing semicolon, using only that
          question's tables.
pyspark : PySpark DataFrame code, must end in display(<dataframe>).
Both results must have the same column names and the same rows (order-insensitive).
"""
from __future__ import annotations

import datetime as dt
import decimal
import json
import numbers
import pathlib
import re
import sys
import traceback

ROOT = pathlib.Path(__file__).resolve().parent.parent
SPEC = ROOT / "notebooks" / "_build" / "17_interview_question_bank.py"

SCALARS = {
    "INT": int,
    "BIGINT": int,
    "SMALLINT": int,
    "DOUBLE": float,
    "FLOAT": float,
    "REAL": float,
    "BOOLEAN": bool,
    "STRING": str,
    "VARCHAR": str,
    "TEXT": str,
}


# --------------------------------------------------------------------------- helpers
def cast_value(value, sql_type: str):
    """Turn a JSON scalar into the python value both engines expect for `sql_type`."""
    if value is None:
        return None
    t = sql_type.upper()
    if t == "DATE":
        return value if isinstance(value, dt.date) else dt.date.fromisoformat(str(value))
    if t.startswith("TIMESTAMP"):
        return value if isinstance(value, dt.datetime) else dt.datetime.fromisoformat(str(value))
    if t.startswith("DECIMAL") or t.startswith("NUMERIC"):
        return decimal.Decimal(str(value))
    if t in SCALARS:
        return SCALARS[t](value)
    raise ValueError(f"unsupported type {sql_type!r} (use INT/BIGINT/DOUBLE/BOOLEAN/STRING/DATE/TIMESTAMP)")


def parse_cols(cols: str) -> list[tuple[str, str]]:
    out = []
    for part in cols.split(","):
        bits = part.strip().split()
        if len(bits) < 2:
            raise ValueError(f"bad column definition {part!r}")
        name, sql_type = bits[0], " ".join(bits[1:])
        out.append((name, sql_type))
    return out


def table_rows(spec: dict) -> tuple[list[str], list[list]]:
    cols = parse_cols(spec["cols"])
    names = [c for c, _ in cols]
    rows = [[cast_value(v, t) for v, (_, t) in zip(row, cols)] for row in spec["rows"]]
    for row in rows:
        if len(row) != len(names):
            raise ValueError(f"{spec['name']}: row {row} does not match {names}")
    return names, rows


def normalise(value):
    """Comparable, engine-agnostic representation of one cell."""
    if value is None:
        return None
    if value is not value or type(value).__name__ in ("NAType", "NaTType"):  # NaN / NaT
        return None
    if value.__class__.__name__ == "NAType":  # pandas.NA
        return None
    if isinstance(value, float) and value != value:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, decimal.Decimal):
        return round(float(value), 6)
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, int):
        return float(value)
    if isinstance(value, dt.datetime):
        return value.replace(microsecond=0).isoformat(sep=" ")
    if isinstance(value, dt.date):
        return value.isoformat()
    if hasattr(value, "isoformat"):  # pandas Timestamp
        return normalise(value.to_pydatetime())
    if hasattr(value, "item"):  # numpy scalar
        return normalise(value.item())
    if isinstance(value, numbers.Integral):  # int, numpy int, duckdb hugeint
        return float(value)
    if isinstance(value, numbers.Real):
        return round(float(value), 6)
    return str(value).strip()


def tidy(frame) -> tuple[list[str], list[tuple]]:
    """(sorted column names, sorted multiset of rows) for a pandas DataFrame."""
    cols = sorted(str(c) for c in frame.columns)
    rows = []
    for _, row in frame.iterrows():
        cells = {str(k): normalise(v) for k, v in row.items()}
        rows.append(tuple(cells.get(c) for c in cols))
    return cols, sorted(rows, key=lambda r: ["" if v is None else str(v) for v in r])


# --------------------------------------------------------------------------- load
def load_spec() -> list[dict]:
    ns: dict = {}
    exec(compile(SPEC.read_text(), str(SPEC), "exec"), ns)  # noqa: S102
    return list(ns["QUESTIONS"])


def load_part(path: pathlib.Path) -> list[dict]:
    """A part is either JSON ({'questions': [...]}) or a .py file defining QUESTIONS = [...]."""
    if path.suffix == ".py":
        ns: dict = {}
        exec(compile(path.read_text(), str(path), "exec"), ns)  # noqa: S102
        return list(ns["QUESTIONS"])
    return list(json.loads(path.read_text())["questions"])


def validate(questions: list[dict]) -> list[str]:
    problems, seen = [], {}
    for q in questions:
        n, tag = q["n"], f"q{q['n']:02d}_"
        for key in ("n", "title", "difficulty", "tags", "prompt", "tables", "sql", "pyspark"):
            if key not in q:
                problems.append(f"Q{n}: missing key {key!r}")
        for t in q.get("tables", []):
            if not t["name"].startswith(tag):
                problems.append(f"Q{n}: table {t['name']} must start with {tag}")
            if t["name"] in seen and seen[t["name"]] != n:
                problems.append(f"Q{n}: table {t['name']} already used by Q{seen[t['name']]}")
            seen[t["name"]] = n
        sql = q.get("sql", "")
        if sql.strip().rstrip(";").count(";") > 0:
            problems.append(f"Q{n}: sql must be a single statement")
        if not re.match(r"\s*(SELECT|WITH)\b", sql, re.I):
            problems.append(f"Q{n}: sql must start with SELECT or WITH")
        if "display(" not in q.get("pyspark", ""):
            problems.append(f"Q{n}: pyspark code must end with display(<dataframe>)")
    return problems


# --------------------------------------------------------------------------- engines
def duckdb_run(con, question: dict) -> tuple[list[str], list[tuple]]:
    for t in question["tables"]:
        names, rows = table_rows(t)
        con.execute(f"CREATE OR REPLACE TEMP TABLE {t['name']} ({t['cols']})")
        if rows:
            marks = ", ".join("?" * len(names))
            con.executemany(f"INSERT INTO {t['name']} VALUES ({marks})", rows)
    rel = con.sql(question["sql"])
    df = rel.df()
    # DuckDB hands a DATE back as midnight datetime64; Spark's toPandas() keeps datetime.date.
    # Coerce DuckDB's DATE columns to python dates so the two engines compare like for like.
    for name, dtype in zip(rel.columns, rel.types):
        if str(dtype).upper() == "DATE" and name in df.columns:
            df[name] = df[name].dt.date
    return tidy(df)


def spark_run(spark, question: dict):
    from pyspark.sql import Window, functions as F  # noqa: N812

    box: dict = {}

    def display(obj, limit: int = 100):  # noqa: ANN001 - mirrors de_helpers.display
        box["df"] = obj
        return obj

    for t in question["tables"]:
        _, rows = table_rows(t)
        spark.createDataFrame(rows, t["cols"]).createOrReplaceTempView(t["name"])
    env = {"spark": spark, "F": F, "Window": Window, "display": display,
           "__name__": f"q{question['n']:02d}"}
    exec(compile(question["pyspark"], f"<q{question['n']:02d} pyspark>", "exec"), env)  # noqa: S102
    if "df" not in box:
        raise AssertionError("display(...) was never called")
    pdf = box["df"].toPandas()
    return tidy(pdf)


# --------------------------------------------------------------------------- main
def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    use_spark = "--no-spark" not in sys.argv
    part_path = None
    if "--part" in sys.argv:
        part_path = pathlib.Path(sys.argv[sys.argv.index("--part") + 1])
        args = [a for a in args if a != str(part_path)]

    questions = load_part(part_path) if part_path else load_spec()
    if args:
        wanted = {int(a.lstrip("qQ")) for a in args}
        questions = [q for q in questions if q["n"] in wanted]
    if not questions:
        print("no questions selected")
        return 1

    problems = validate(questions)
    if problems:
        print("CONTRACT PROBLEMS")
        for p in problems:
            print("  -", p)

    import duckdb

    con = duckdb.connect()  # in-memory: toy tables live only for this verification
    spark = None
    if use_spark:
        try:
            import os

            from pyspark.sql import SparkSession

            # the worker must use the same interpreter as the driver
            os.environ["PYSPARK_PYTHON"] = sys.executable
            os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
            spark = (
                SparkSession.builder.master("local[2]")
                .appName("qbank-verify")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "4")
                # NB: leave spark.sql.session.timeZone at its default (the machine's zone).
                # Naive python datetimes are converted with the local zone and read back in the
                # session zone, so wall-clock values round-trip; forcing UTC here shifts them.
                .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
                .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("ERROR")
        except Exception as exc:  # noqa: BLE001
            print(f"!! Spark unavailable ({type(exc).__name__}: {str(exc).splitlines()[0][:90]})")
            print("!! PySpark half SKIPPED - run this with data/scratch/qbank-venv/bin/python")
            spark = None

    failures, rows_checked = 0, 0
    for q in sorted(questions, key=lambda x: x["n"]):
        label = f"Q{q['n']:02d} {q['title'][:44]:<44}"
        try:
            cols_sql, rows_sql = duckdb_run(con, q)
        except Exception as exc:  # noqa: BLE001
            failures += 1
            print(f"FAIL {label} SQL   {type(exc).__name__}: {str(exc).splitlines()[0][:90]}")
            continue
        if not rows_sql:
            failures += 1
            print(f"FAIL {label} SQL   returned no rows")
            continue
        if spark is None:
            print(f"OK   {label} SQL   {len(rows_sql):>4} rows (spark skipped)")
            continue
        try:
            cols_py, rows_py = spark_run(spark, q)
        except Exception as exc:  # noqa: BLE001
            failures += 1
            print(f"FAIL {label} SPARK {type(exc).__name__}: {str(exc).splitlines()[0][:90]}")
            if "--trace" in sys.argv:
                traceback.print_exc()
            continue
        if cols_sql != cols_py:
            failures += 1
            print(f"FAIL {label} columns differ\n       sql  : {cols_sql}\n       spark: {cols_py}")
            continue
        if rows_sql != rows_py:
            failures += 1
            extra = [r for r in rows_py if r not in rows_sql][:3]
            missing = [r for r in rows_sql if r not in rows_py][:3]
            print(f"FAIL {label} rows differ ({len(rows_sql)} sql vs {len(rows_py)} spark)")
            print(f"       sql only  : {missing}")
            print(f"       spark only: {extra}")
            continue
        rows_checked += len(rows_sql)
        print(f"OK   {label} sql+spark agree on {len(rows_sql):>4} rows")

    print(f"\n{len(questions) - failures}/{len(questions)} questions verified "
          f"({rows_checked} result rows compared){' + contract problems' if problems else ''}")
    return 1 if failures or problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
