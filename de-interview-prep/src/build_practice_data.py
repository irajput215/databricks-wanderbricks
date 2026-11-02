"""
Classic SQL-interview practice tables (customers, orders, employees, departments,
logins, products, purchases).

    .venv/Scripts/python src/build_practice_data.py               # local: CSV + parquet + DuckDB
    .venv/Scripts/python src/build_practice_data.py --databricks  # also load workflows.practice.* in Unity Catalog

Outputs
    data/practice/<table>.csv | .parquet
    data/warehouse/practice.duckdb   (schema main)
    workflows.practice.<table>       (Databricks, with --databricks)

The data is deterministic and deliberately seeded with interview traps:
    customers  - customers with no orders, NULL email, duplicate email (case differs)
    orders     - cancelled/refunded rows, NULL amount, orders on the same day, one orphan customer_id,
                 one NULL customer_id (makes NOT IN return nothing)
    employees  - CEO with NULL manager_id, salary ties per dept, NULL dept_id
    departments- a department with no employees
    logins     - consecutive-day streaks, duplicate logins on the same day, gaps
    products   - a product nobody bought
    purchases  - customers who bought every product, duplicate purchase rows
"""
from __future__ import annotations

import argparse
import random
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd

from paths import DATA, WAREHOUSE

PRACTICE = DATA / "practice"
PRACTICE.mkdir(parents=True, exist_ok=True)
DB_PATH = WAREHOUSE / "practice.duckdb"

CATALOG = "workflows"
SCHEMA = "practice"

rng = random.Random(42)


def build_customers() -> pd.DataFrame:
    rows = [
        (1, "Alice Johnson", "alice@example.com", "New York"),
        (2, "Bob Smith", "bob@example.com", "Chicago"),
        (3, "Carol White", "carol@example.com", "New York"),
        (4, "David Brown", "david@example.com", "Austin"),
        (5, "Eva Green", "eva@example.com", "Seattle"),
        (6, "Frank Moore", None, "Chicago"),                    # NULL email
        (7, "Grace Lee", "grace@example.com", "Austin"),
        (8, "Henry Clark", "ALICE@example.com", "Boston"),      # duplicate email, different case
        (9, "Ivy Lewis", "ivy@example.com", None),              # NULL city
        (10, "Jack Hall", "jack@example.com", "Seattle"),       # never orders
        (11, "Karen Young", "karen@example.com", "New York"),   # never orders
        (12, "Leo King", "leo@example.com", "Boston"),
    ]
    start = datetime(2024, 1, 5, 9, 0)
    return pd.DataFrame(
        [(cid, n, e, c, start + timedelta(days=17 * i, hours=rng.randint(0, 10)))
         for i, (cid, n, e, c) in enumerate(rows)],
        columns=["customer_id", "name", "email", "city", "created_at"],
    )


def build_orders() -> pd.DataFrame:
    buyers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 12]
    statuses = ["completed"] * 7 + ["cancelled", "refunded", "pending"]
    rows, oid = [], 1001
    for d in range(0, 270, 3):
        day = date(2024, 3, 1) + timedelta(days=d)
        for _ in range(rng.choice([1, 1, 2])):
            cid = rng.choice(buyers)
            rows.append((oid, cid, round(rng.uniform(10, 500), 2), day, rng.choice(statuses)))
            oid += 1
    # traps
    rows += [
        (oid, 1, 250.00, date(2024, 6, 15), "completed"),       # same-day orders for one customer
        (oid + 1, 1, 250.00, date(2024, 6, 15), "completed"),
        (oid + 2, 3, None, date(2024, 7, 4), "completed"),       # NULL amount
        (oid + 3, 99, 75.50, date(2024, 8, 1), "completed"),     # orphan customer_id
        (oid + 4, 12, 999.99, date(2025, 1, 2), "completed"),    # next year
        (oid + 5, None, 42.00, date(2024, 9, 9), "completed"),   # NULL customer_id (guest) - breaks NOT IN
    ]
    return pd.DataFrame(rows, columns=["order_id", "customer_id", "amount", "order_date", "status"])


def build_departments() -> pd.DataFrame:
    return pd.DataFrame(
        [(10, "Engineering"), (20, "Sales"), (30, "Marketing"), (40, "Finance"), (50, "Legal")],  # Legal is empty
        columns=["dept_id", "dept_name"],
    )


def build_employees() -> pd.DataFrame:
    rows = [
        (1, "Maria Chen", 10, 250000, None),       # CEO
        (2, "Raj Patel", 10, 180000, 1),
        (3, "Sara Kim", 10, 150000, 2),
        (4, "Tom Davis", 10, 150000, 2),           # tie with Sara
        (5, "Uma Singh", 10, 120000, 2),
        (6, "Victor Ruiz", 20, 160000, 1),
        (7, "Wendy Fox", 20, 95000, 6),
        (8, "Xavier Ng", 20, 95000, 6),            # tie
        (9, "Yara Haddad", 20, 70000, 6),
        (10, "Zane Ortiz", 30, 130000, 1),
        (11, "Amy Brooks", 30, 85000, 10),
        (12, "Ben Carter", 30, 140000, 10),        # earns more than manager
        (13, "Cleo Diaz", 40, 125000, 1),
        (14, "Dan Evans", 40, 90000, 13),
        (15, "Ella Ford", None, 60000, 13),        # NULL dept
    ]
    return pd.DataFrame(rows, columns=["emp_id", "name", "dept_id", "salary", "manager_id"])


def build_logins() -> pd.DataFrame:
    rows = []
    patterns = {
        101: [(date(2024, 5, 1), 7), (date(2024, 5, 20), 3)],   # 7-day streak
        102: [(date(2024, 5, 3), 2), (date(2024, 5, 10), 4)],
        103: [(date(2024, 5, 1), 1), (date(2024, 5, 5), 1), (date(2024, 5, 9), 1)],  # no streak
        104: [(date(2024, 4, 28), 10)],                          # streak crossing month boundary
        105: [(date(2024, 5, 15), 5)],
    }
    for uid, spans in patterns.items():
        for start, n in spans:
            rows += [(uid, start + timedelta(days=i)) for i in range(n)]
    rows += [(101, date(2024, 5, 3)), (105, date(2024, 5, 16))]  # duplicate logins same day
    df = pd.DataFrame(rows, columns=["user_id", "login_date"])
    return df.sort_values(["user_id", "login_date"], kind="stable").reset_index(drop=True)


def build_products() -> pd.DataFrame:
    return pd.DataFrame(
        [(1, "Laptop"), (2, "Mouse"), (3, "Keyboard"), (4, "Monitor"), (5, "Webcam")],  # Webcam never bought
        columns=["product_id", "product_name"],
    )


def build_purchases() -> pd.DataFrame:
    rows = [
        (1, 1), (1, 2), (1, 3), (1, 4), (1, 5),   # bought every product
        (2, 1), (2, 2),
        (3, 1), (3, 2), (3, 3), (3, 4), (3, 5),   # bought every product
        (4, 2), (4, 2),                           # duplicate row
        (5, 3),
        (7, 1), (7, 4),
        (12, 1), (12, 2), (12, 3), (12, 4),       # missing one
    ]
    return pd.DataFrame(rows, columns=["customer_id", "product_id"])


TABLES = {
    "customers": build_customers,
    "orders": build_orders,
    "employees": build_employees,
    "departments": build_departments,
    "logins": build_logins,
    "products": build_products,
    "purchases": build_purchases,
}


def fix_types(name: str, df: pd.DataFrame) -> pd.DataFrame:
    if name == "employees":
        df = df.astype({"dept_id": "Int64", "manager_id": "Int64"})
    if name == "orders":
        df = df.astype({"customer_id": "Int64"})
    for col in ("order_date", "login_date"):
        if col in df:
            df[col] = pd.to_datetime(df[col]).dt.date
    if "created_at" in df:
        df["created_at"] = df["created_at"].astype("datetime64[us]")  # Spark can't read TIMESTAMP_NS parquet
    return df


def write_local(frames: dict[str, pd.DataFrame]) -> None:
    con = duckdb.connect(str(DB_PATH))
    for name, df in frames.items():
        df.to_csv(PRACTICE / f"{name}.csv", index=False)
        con.register("df_view", df)
        con.sql(f"CREATE OR REPLACE TABLE main.{name} AS SELECT * FROM df_view")
        con.sql(f"COPY main.{name} TO '{(PRACTICE / f'{name}.parquet').as_posix()}' (FORMAT parquet)")
        con.unregister("df_view")
        print(f"  {name:<12} {len(df):>4} rows")
    con.close()
    print(f"local: {PRACTICE}  +  {DB_PATH}")


def write_databricks(frames: dict[str, pd.DataFrame]) -> None:
    from de_helpers import get_spark

    spark = get_spark(catalog=CATALOG, schema=SCHEMA)
    if spark is None:
        raise SystemExit("no Databricks session - run: databricks auth login --profile irajput")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    for name, df in frames.items():
        target = f"{CATALOG}.{SCHEMA}.{name}"
        spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        print(f"  {target:<32} {spark.table(target).count():>4} rows")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--databricks", action="store_true", help=f"also load {CATALOG}.{SCHEMA}.* in Unity Catalog")
    args = ap.parse_args()

    frames = {name: fix_types(name, build()) for name, build in TABLES.items()}
    write_local(frames)
    if args.databricks:
        write_databricks(frames)


if __name__ == "__main__":
    main()
