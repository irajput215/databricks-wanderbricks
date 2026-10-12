"""Spec for notebooks/15_interview_question_bank.ipynb - 40 interview-style drills.

Every question carries its own tiny dataset, created in BOTH engines by the `table()` helper in
the setup cell, then answered twice:

    SQL      ->  q('''SELECT ...''')            (DuckDB, read-only connection from de_helpers)
    PySpark  ->  spark.table("q07_...")         (DataFrame API, Databricks Connect / serverless)

Verify before/after editing:

    data/scratch/qbank-venv/bin/python tools/verify_question_bank.py       # all 40
    data/scratch/qbank-venv/bin/python tools/verify_question_bank.py 07    # one question

The QUESTIONS block between the two markers is generated from data/scratch/qbank/part*.json
by tools/_merge_qbank_parts.py - keep it sorted by `n` and keep the markers intact.
"""
TITLE = "17 - Interview Question Bank (40 drills: DuckDB SQL + PySpark)"
PREAMBLE = False  # this notebook brings its own, Spark-tolerant preamble

# --------------------------------------------------------------------------- questions
# >>> QUESTIONS
QUESTIONS: list[dict] = [
    {
        "n": 1,
        "title": 'Property Rental Revenue Analysis',
        "difficulty": 'Hard',
        "tags": ['Inner Joins', 'Aggregate Functions'],
        "prompt": (
            'For every rental property, total the revenue booked in the year and show how much of the '
            "property's available calendar was actually booked. Cancelled bookings earn nothing and do not "
            'count as occupancy.'
        ),
        "returns": 'one row per property: completed bookings, booked nights, revenue, occupancy %',
        "watch_out": (
            'Joining bookings to properties fans out if you aggregate bookings on the wrong grain first; '
            'filter cancelled bookings in the JOIN condition (or a WHERE on the booking side) so properties '
            'with no completed booking still survive a LEFT JOIN.'
        ),
        "tables": [
            {"name": "q01_properties",
             "cols": 'property_id INT, property_name STRING, city STRING, available_nights INT',
             "rows": [
                 (1, 'Sunset Loft', 'Austin', 90),
                 (2, 'Harbor Flat', 'Seattle', 90),
                 (3, 'Desert Casa', 'Phoenix', 90),
                 (4, 'Lake Cabin', 'Denver', 90),
             ]},
            {"name": "q01_bookings",
             "cols": (
                 'booking_id INT, property_id INT, check_in DATE, check_out DATE, nightly_rate DOUBLE, status '
                 'STRING'
             ),
             "rows": [
                 (101, 1, '2024-01-05', '2024-01-10', 200.0, 'completed'),
                 (102, 1, '2024-02-01', '2024-02-08', 200.0, 'completed'),
                 (103, 1, '2024-03-02', '2024-03-04', 220.0, 'cancelled'),
                 (104, 2, '2024-01-12', '2024-01-20', 150.0, 'completed'),
                 (105, 2, '2024-02-14', '2024-02-16', 150.0, 'completed'),
                 (106, 3, '2024-01-03', '2024-01-08', 120.0, 'completed'),
                 (107, 4, '2024-01-04', '2024-01-06', 300.0, 'completed'),
                 (108, 4, '2024-02-09', '2024-02-12', 300.0, 'cancelled'),
             ]},
        ],
        "sql": """
SELECT
    p.property_name,
    p.city,
    COUNT(b.booking_id)                                              AS completed_bookings,
    SUM(date_diff('day', b.check_in, b.check_out))                   AS booked_nights,
    ROUND(SUM(b.nightly_rate * date_diff('day', b.check_in, b.check_out)), 2) AS revenue,
    ROUND(SUM(date_diff('day', b.check_in, b.check_out)) * 100.0
          / NULLIF(p.available_nights, 0), 2)                        AS occupancy_pct
FROM q01_properties p
LEFT JOIN q01_bookings b
       ON b.property_id = p.property_id
      AND b.status = 'completed'
GROUP BY p.property_id, p.property_name, p.city, p.available_nights
ORDER BY revenue DESC, p.property_name
""",
        "pyspark": """
bookings = (
    spark.table("q01_bookings")
    .filter(F.col("status") == "completed")
    .withColumn("nights", F.datediff("check_out", "check_in"))
    .withColumn("revenue", F.col("nightly_rate") * F.col("nights"))
)

result = (
    spark.table("q01_properties")
    .join(bookings, on="property_id", how="left")
    .groupBy("property_name", "city", "available_nights")
    .agg(
        F.count("booking_id").alias("completed_bookings"),
        F.sum("nights").alias("booked_nights"),
        F.round(F.sum("revenue"), 2).alias("revenue"),
    )
    .withColumn("occupancy_pct",
                F.round(F.col("booked_nights") * 100.0 / F.col("available_nights"), 2))
    .select("property_name", "city", "completed_bookings", "booked_nights",
            "revenue", "occupancy_pct")
)
display(result)
""",
    },
    {
        "n": 2,
        "title": 'Manufacturing Defect Rate Analysis',
        "difficulty": 'Hard',
        "tags": ['Rank', 'Partitioning By'],
        "prompt": (
            'Each production line runs several batches. Roll the batches up to a defect rate per line and '
            'rank the lines from worst to best so the quality team knows where to start.'
        ),
        "returns": (
            'one row per production line: units produced, units defective, defect rate %, rank'
        ),
        "watch_out": (
            'Rate = SUM(defective) / SUM(produced), never the average of per-batch rates - averaging rates '
            'gives every batch the same weight regardless of size. Guard the division: a line with zero '
            'production must not produce an error or a fake rank.'
        ),
        "tables": [
            {"name": "q02_production_runs",
             "cols": (
                 'run_id INT, line_id STRING, run_date DATE, units_produced INT, units_defective INT'
             ),
             "rows": [
                 (1, 'L1', '2024-01-02', 1000, 35),
                 (2, 'L1', '2024-01-09', 1200, 60),
                 (3, 'L2', '2024-01-03', 800, 64),
                 (4, 'L2', '2024-01-10', 900, 45),
                 (5, 'L3', '2024-01-04', 1500, 30),
                 (6, 'L3', '2024-01-11', 1500, 45),
                 (7, 'L4', '2024-01-05', 0, 0),
             ]},
        ],
        "sql": """
WITH line_totals AS (
    SELECT
        line_id,
        SUM(units_produced)  AS units_produced,
        SUM(units_defective) AS units_defective
    FROM q02_production_runs
    GROUP BY line_id
)
SELECT
    line_id,
    units_produced,
    units_defective,
    ROUND(units_defective * 100.0 / NULLIF(units_produced, 0), 2) AS defect_rate_pct,
    RANK() OVER (ORDER BY units_defective * 100.0 / NULLIF(units_produced, 0) DESC) AS defect_rank
FROM line_totals
ORDER BY defect_rank, line_id
""",
        "pyspark": """
totals = (
    spark.table("q02_production_runs")
    .groupBy("line_id")
    .agg(F.sum("units_produced").alias("units_produced"),
         F.sum("units_defective").alias("units_defective"))
    .withColumn("defect_rate_pct",
                F.col("units_defective") * 100.0 / F.when(F.col("units_produced") == 0, None)
                 .otherwise(F.col("units_produced")))
)

result = (
    totals
    .withColumn("defect_rank", F.rank().over(Window.orderBy(F.col("defect_rate_pct").desc())))
    .withColumn("defect_rate_pct", F.round("defect_rate_pct", 2))
    .select("line_id", "units_produced", "units_defective", "defect_rate_pct", "defect_rank")
)
display(result)
""",
    },
    {
        "n": 3,
        "title": 'Country GDP Growth Rate',
        "difficulty": 'Hard',
        "tags": ['Window Functions', 'Mathematical Functions'],
        "prompt": (
            "For each country and year, show GDP next to the previous year's GDP and the year-over-year "
            'growth rate. The first year of each country has nothing to compare against, so it should not '
            'appear.'
        ),
        "returns": 'one row per country-year (except the first year): gdp, prior gdp, yoy growth %',
        "watch_out": (
            'LAG() has to be partitioned by country - without the partition you compare Ireland 2021 with '
            'Vietnam 2024. Compare on the same grain: the window runs over years, and the first row of each '
            'partition is NULL.'
        ),
        "tables": [
            {"name": "q03_gdp",
             "cols": 'country STRING, year INT, gdp_usd DOUBLE',
             "rows": [
                 ('Ireland', 2021, 100.0),
                 ('Ireland', 2022, 112.0),
                 ('Ireland', 2023, 108.0),
                 ('Ireland', 2024, 120.0),
                 ('Vietnam', 2021, 80.0),
                 ('Vietnam', 2022, 90.0),
                 ('Vietnam', 2023, 99.0),
                 ('Vietnam', 2024, 112.0),
             ]},
        ],
        "sql": """
WITH yearly AS (
    SELECT
        country,
        year,
        gdp_usd,
        LAG(gdp_usd) OVER (PARTITION BY country ORDER BY year) AS prev_gdp_usd
    FROM q03_gdp
)
SELECT
    country,
    year,
    gdp_usd,
    prev_gdp_usd,
    ROUND((gdp_usd - prev_gdp_usd) * 100.0 / prev_gdp_usd, 2) AS yoy_growth_pct
FROM yearly
WHERE prev_gdp_usd IS NOT NULL
ORDER BY country, year
""",
        "pyspark": """
yearly = Window.partitionBy("country").orderBy("year")

result = (
    spark.table("q03_gdp")
    .withColumn("prev_gdp_usd", F.lag("gdp_usd").over(yearly))
    .filter(F.col("prev_gdp_usd").isNotNull())
    .withColumn("yoy_growth_pct",
                F.round((F.col("gdp_usd") - F.col("prev_gdp_usd")) * 100.0
                        / F.col("prev_gdp_usd"), 2))
    .select("country", "year", "gdp_usd", "prev_gdp_usd", "yoy_growth_pct")
)
display(result)
""",
    },
    {
        "n": 4,
        "title": 'Duplicate Payment Identification',
        "difficulty": 'Hard',
        "tags": ['Order By', 'Aggregate Functions', 'Window Functions'],
        "prompt": (
            'The payments team suspects the gateway retried charges. Flag every payment that shares the same '
            'user, merchant and amount with at least one other payment, and say how many payments are in that '
            'duplicate group.'
        ),
        "returns": 'one row per suspicious payment, with duplicate_count = size of its group',
        "watch_out": (
            'Two different users paying the same merchant the same amount is normal - the duplicate key is '
            '(user, merchant, amount), not (merchant, amount). A window COUNT keeps the individual payment '
            'rows; a GROUP BY would collapse them.'
        ),
        "tables": [
            {"name": "q04_payments",
             "cols": (
                 'payment_id INT, user_id INT, merchant STRING, amount DOUBLE, paid_at TIMESTAMP'
             ),
             "rows": [
                 (1, 101, 'Spotify', 9.99, '2024-03-01 08:00:00'),
                 (2, 101, 'Spotify', 9.99, '2024-03-01 08:04:00'),
                 (3, 101, 'Uber', 24.5, '2024-03-02 18:30:00'),
                 (4, 102, 'Spotify', 9.99, '2024-03-01 09:00:00'),
                 (5, 102, 'Amazon', 54.0, '2024-03-03 12:00:00'),
                 (6, 103, 'Amazon', 54.0, '2024-03-04 12:00:00'),
                 (7, 103, 'Amazon', 54.0, '2024-03-04 12:00:00'),
                 (8, 104, 'Netflix', 15.49, '2024-03-05 20:00:00'),
             ]},
        ],
        "sql": """
WITH flagged AS (
    SELECT
        payment_id,
        user_id,
        merchant,
        amount,
        paid_at,
        COUNT(*) OVER (PARTITION BY user_id, merchant, amount) AS duplicate_count
    FROM q04_payments
)
SELECT payment_id, user_id, merchant, amount, paid_at, duplicate_count
FROM flagged
WHERE duplicate_count > 1
ORDER BY user_id, amount, paid_at, payment_id
""",
        "pyspark": """
result = (
    spark.table("q04_payments")
    .withColumn("duplicate_count",
                F.count("*").over(Window.partitionBy("user_id", "merchant", "amount")))
    .filter(F.col("duplicate_count") > 1)
    .select("payment_id", "user_id", "merchant", "amount", "paid_at", "duplicate_count")
)
display(result)
""",
    },
    {
        "n": 5,
        "title": 'Monthly Active User Retention',
        "difficulty": 'Hard',
        "tags": ['Date/Time Functions'],
        "prompt": (
            'Count monthly active users (distinct users with any action in the month) and show the change '
            'against the previous month, so retention and churn are visible at a glance.'
        ),
        "returns": 'one row per month: activity_month, monthly_active_users, mau_change',
        "watch_out": (
            "COUNT(DISTINCT user_id) per month - counting actions inflates a heavy user into many 'users'. A "
            'month with no rows disappears entirely, so the lag skips it instead of reporting a zero.'
        ),
        "tables": [
            {"name": "q05_user_actions",
             "cols": 'user_id INT, action_date DATE, action STRING',
             "rows": [
                 (1, '2024-01-05', 'login'),
                 (2, '2024-01-06', 'search'),
                 (3, '2024-01-20', 'login'),
                 (1, '2024-02-02', 'login'),
                 (2, '2024-02-14', 'purchase'),
                 (1, '2024-02-28', 'login'),
                 (1, '2024-03-01', 'login'),
                 (2, '2024-03-03', 'login'),
                 (3, '2024-03-09', 'search'),
                 (4, '2024-03-15', 'login'),
             ]},
        ],
        "sql": """
WITH mau AS (
    SELECT
        date_trunc('month', action_date) AS activity_month,
        COUNT(DISTINCT user_id)          AS monthly_active_users
    FROM q05_user_actions
    GROUP BY 1
)
SELECT
    activity_month,
    monthly_active_users,
    monthly_active_users - LAG(monthly_active_users) OVER (ORDER BY activity_month) AS mau_change
FROM mau
ORDER BY activity_month
""",
        "pyspark": """
monthly = (
    spark.table("q05_user_actions")
    .groupBy(F.date_trunc("month", "action_date").alias("activity_month"))
    .agg(F.countDistinct("user_id").alias("monthly_active_users"))
)

result = (
    monthly
    .withColumn("mau_change",
                F.col("monthly_active_users")
                - F.lag("monthly_active_users").over(Window.orderBy("activity_month")))
    .select("activity_month", "monthly_active_users", "mau_change")
)
display(result)
""",
    },
    {
        "n": 6,
        "title": 'User Growth Rate Analysis',
        "difficulty": 'Hard',
        "tags": ['Window Functions'],
        "prompt": (
            'How fast is the user base growing? Count signups per calendar year and show the year-over-year '
            'growth rate, starting from the second year on record.'
        ),
        "returns": 'one row per year (except the first): new users, prior-year users, yoy growth %',
        "watch_out": (
            'Aggregate to the year first, then window over the yearly result. Ordering the window by a raw '
            'signup date would give one row per signup, not per year.'
        ),
        "tables": [
            {"name": "q06_users",
             "cols": 'user_id INT, signup_date DATE',
             "rows": [
                 (1, '2022-01-14'),
                 (2, '2022-03-02'),
                 (3, '2022-07-19'),
                 (4, '2022-11-30'),
                 (5, '2023-01-05'),
                 (6, '2023-02-17'),
                 (7, '2023-04-23'),
                 (8, '2023-06-11'),
                 (9, '2023-09-08'),
                 (10, '2023-12-01'),
                 (11, '2024-01-22'),
                 (12, '2024-03-15'),
                 (13, '2024-06-30'),
                 (14, '2024-08-04'),
                 (15, '2024-10-19'),
             ]},
        ],
        "sql": """
WITH signups AS (
    SELECT
        year(signup_date) AS signup_year,
        COUNT(*)          AS new_users
    FROM q06_users
    GROUP BY 1
),
growth AS (
    SELECT
        signup_year,
        new_users,
        LAG(new_users) OVER (ORDER BY signup_year) AS prev_year_users
    FROM signups
)
SELECT
    signup_year,
    new_users,
    prev_year_users,
    ROUND((new_users - prev_year_users) * 100.0 / prev_year_users, 2) AS yoy_growth_pct
FROM growth
WHERE prev_year_users IS NOT NULL
ORDER BY signup_year
""",
        "pyspark": """
signups = (
    spark.table("q06_users")
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.count("*").alias("new_users"))
)

result = (
    signups
    .withColumn("prev_year_users", F.lag("new_users").over(Window.orderBy("signup_year")))
    .filter(F.col("prev_year_users").isNotNull())
    .withColumn("yoy_growth_pct",
                F.round((F.col("new_users") - F.col("prev_year_users")) * 100.0
                        / F.col("prev_year_users"), 2))
    .select("signup_year", "new_users", "prev_year_users", "yoy_growth_pct")
)
display(result)
""",
    },
    {
        "n": 7,
        "title": 'Warehouse Inventory Optimization',
        "difficulty": 'Hard',
        "tags": ['Order By', 'Subqueries', 'Aggregate Functions'],
        "prompt": (
            'The largest warehouse should take every unit of any item type that fits inside its weight '
            'capacity. Total the inventory by item type, keep the types that fit whole, and show how much of '
            'the capacity each one would consume.'
        ),
        "returns": 'one row per item type that fits: total items, total weight, capacity used %',
        "watch_out": (
            'Weight per type is SUM(quantity * unit_weight_kg) - the type is all-or-nothing, so a type over '
            'capacity is dropped entirely rather than partially stored. The capacity is a scalar subquery, so '
            'it never multiplies the inventory rows.'
        ),
        "tables": [
            {"name": "q07_inventory",
             "cols": 'item_id INT, item_type STRING, quantity INT, unit_weight_kg DOUBLE',
             "rows": [
                 (1, 'bike', 30, 12.5),
                 (2, 'bike', 10, 12.5),
                 (3, 'kayak', 8, 28.0),
                 (4, 'tent', 60, 3.5),
                 (5, 'stove', 45, 6.0),
                 (6, 'dock', 5, 300.0),
             ]},
            {"name": "q07_warehouses",
             "cols": 'warehouse_id INT, city STRING, capacity_kg DOUBLE',
             "rows": [
                 (1, 'Memphis', 500.0),
                 (2, 'Reno', 900.0),
                 (3, 'Allentown', 1200.0),
             ]},
        ],
        "sql": """
WITH type_totals AS (
    SELECT
        item_type,
        SUM(quantity)                  AS total_items,
        SUM(quantity * unit_weight_kg) AS total_weight_kg
    FROM q07_inventory
    GROUP BY item_type
)
SELECT
    item_type,
    total_items,
    ROUND(total_weight_kg, 2) AS total_weight_kg,
    ROUND(total_weight_kg * 100.0
          / (SELECT max(capacity_kg) FROM q07_warehouses), 2) AS capacity_used_pct
FROM type_totals
WHERE total_weight_kg <= (SELECT max(capacity_kg) FROM q07_warehouses)
ORDER BY total_weight_kg DESC
""",
        "pyspark": """
capacity = spark.table("q07_warehouses").agg(F.max("capacity_kg").alias("capacity_kg"))

result = (
    spark.table("q07_inventory")
    .groupBy("item_type")
    .agg(F.sum("quantity").alias("total_items"),
         F.sum(F.col("quantity") * F.col("unit_weight_kg")).alias("total_weight_kg"))
    .crossJoin(capacity)                                   # scalar subquery
    .filter(F.col("total_weight_kg") <= F.col("capacity_kg"))
    .withColumn("capacity_used_pct",
                F.round(F.col("total_weight_kg") * 100.0 / F.col("capacity_kg"), 2))
    .withColumn("total_weight_kg", F.round("total_weight_kg", 2))
    .select("item_type", "total_items", "total_weight_kg", "capacity_used_pct")
)
display(result)
""",
    },
    {
        "n": 8,
        "title": 'Google Search Frequency Median',
        "difficulty": 'Hard',
        "tags": ['Subqueries'],
        "prompt": (
            'How many searches does a typical user make? Aggregate searches per user, then take the median of '
            'those per-user counts - the median is not dragged around by the one user who searches hundreds '
            'of times.'
        ),
        "returns": 'one row: number of users, total searches, median searches per user',
        "watch_out": (
            'Median of a subquery, not median of the raw rows: MEDIAN(searches) over the search log answers a '
            'different question. With an even number of users the median sits between two users (2.5 here), '
            'which percentile_approx() will not give you - use median() (Spark 3.4+/DBR 13.3+).'
        ),
        "tables": [
            {"name": "q08_searches",
             "cols": 'search_id INT, user_id INT, query STRING, searched_at TIMESTAMP',
             "rows": [
                 (1, 201, 'delta lake', '2024-02-01 09:00:00'),
                 (2, 202, 'spark sql', '2024-02-01 09:05:00'),
                 (3, 202, 'duckdb', '2024-02-01 09:10:00'),
                 (4, 203, 'unity catalog', '2024-02-02 10:00:00'),
                 (5, 203, 'medallion', '2024-02-02 10:20:00'),
                 (6, 203, 'z-order', '2024-02-02 10:40:00'),
                 (7, 204, 'parquet', '2024-02-03 11:00:00'),
                 (8, 204, 'orc', '2024-02-03 11:10:00'),
                 (9, 204, 'avro', '2024-02-03 11:20:00'),
                 (10, 204, 'iceberg', '2024-02-03 11:30:00'),
                 (11, 204, 'hudi', '2024-02-03 11:40:00'),
                 (12, 204, 'delta sharing', '2024-02-03 11:50:00'),
             ]},
        ],
        "sql": """
WITH per_user AS (
    SELECT user_id, COUNT(*) AS searches
    FROM q08_searches
    GROUP BY user_id
)
SELECT
    COUNT(*)                        AS users,
    SUM(searches)                   AS total_searches,
    ROUND(median(searches), 2)      AS median_searches_per_user
FROM per_user
""",
        "pyspark": """
per_user = (
    spark.table("q08_searches")
    .groupBy("user_id")
    .agg(F.count("*").alias("searches"))
)

result = per_user.agg(
    F.count("*").alias("users"),
    F.sum("searches").alias("total_searches"),
    F.round(F.median("searches"), 2).alias("median_searches_per_user"),   # Spark 3.4+ / DBR 13.3+
)
display(result)
""",
    },
    {
        "n": 9,
        "title": 'Pizza Topping Cost Calculator',
        "difficulty": 'Hard',
        "tags": ['Inner Joins'],
        "prompt": (
            'Every pizza on the menu carries exactly three different toppings. For each set of three '
            'toppings, work out what that pizza would cost in total, and list each set only once - pepperoni, '
            'mushroom and bacon is the same pizza as bacon, mushroom and pepperoni.'
        ),
        "returns": 'one row per 3-topping combination: the three toppings and the total cost',
        "watch_out": (
            'Joining the topping list to itself three times produces every ordering of the same trio (6 rows '
            'per combination), so keep only the strict ordering a.name < b.name < c.name. Order by name, '
            'never by cost: two pairs of toppings share a price here, and a cost-based ordering both drops '
            'and duplicates combinations.'
        ),
        "tables": [
            {"name": "q09_toppings",
             "cols": 'topping_name STRING, cost DOUBLE',
             "rows": [
                 ('pepperoni', 1.5),
                 ('mushroom', 1.0),
                 ('onion', 1.0),
                 ('sausage', 1.75),
                 ('bacon', 2.25),
                 ('black olives', 2.25),
             ]},
        ],
        "sql": """
SELECT
    a.topping_name                  AS topping_1,
    b.topping_name                  AS topping_2,
    c.topping_name                  AS topping_3,
    ROUND(a.cost + b.cost + c.cost, 2) AS total_cost
FROM q09_toppings a
JOIN q09_toppings b ON b.topping_name > a.topping_name
JOIN q09_toppings c ON c.topping_name > b.topping_name
ORDER BY total_cost DESC, topping_1, topping_2, topping_3
""",
        "pyspark": """
toppings = spark.table("q09_toppings")

first = toppings.select(F.col("topping_name").alias("topping_1"),
                        F.col("cost").alias("cost_1"))
second = toppings.select(F.col("topping_name").alias("topping_2"),
                         F.col("cost").alias("cost_2"))
third = toppings.select(F.col("topping_name").alias("topping_3"),
                        F.col("cost").alias("cost_3"))

result = (
    first
    .join(second, F.col("topping_2") > F.col("topping_1"), "inner")
    .join(third, F.col("topping_3") > F.col("topping_2"), "inner")
    .withColumn("total_cost",
                F.round(F.col("cost_1") + F.col("cost_2") + F.col("cost_3"), 2))
    .select("topping_1", "topping_2", "topping_3", "total_cost")
    .orderBy(F.col("total_cost").desc(), "topping_1", "topping_2", "topping_3")
)
display(result)
""",
    },
    {
        "n": 10,
        "title": 'Advertiser Status Update',
        "difficulty": 'Hard',
        "tags": ['Left Outer Joins', 'Order By'],
        "prompt": (
            'Advertisers are reviewed on their most recent week of activity. Treat the last seven days '
            'present in the spend log as the review window and report every advertiser - including those with '
            'no spend rows at all in that window - with the money they spent there and the status they should '
            'carry: Active at 500 or more, otherwise Inactive. Show the status currently on file next to it '
            'so the correction is visible.'
        ),
        "returns": (
            'one row per advertiser: current status, spend in the last 7 days of the data, and the '
            'recalculated status'
        ),
        "watch_out": (
            'The window is anchored to the latest spend date in the data (2024-03-20), so it covers '
            '2024-03-14..2024-03-20 inclusive - the 2024-03-13 rows are one day earlier and must not count, '
            'which is what flips advertiser 6 from Active to Inactive. The LEFT JOIN leaves NULL spend for '
            'advertisers with no rows, so coalesce to 0 before comparing with 500 or the status comes out '
            'NULL; one advertiser lands exactly on 500 and another on 499.99.'
        ),
        "tables": [
            {"name": "q10_advertisers",
             "cols": 'advertiser_id INT, advertiser_name STRING, status STRING',
             "rows": [
                 (1, 'Acme Ads', 'Active'),
                 (2, 'Bolt Media', 'Inactive'),
                 (3, 'Cobalt Reach', 'Active'),
                 (4, 'Delta Digital', 'Inactive'),
                 (5, 'Echo Group', 'Active'),
                 (6, 'Foxtrot Media', 'Active'),
                 (7, 'Golf Partners', 'Active'),
             ]},
            {"name": "q10_daily_spend",
             "cols": 'advertiser_id INT, spend_date DATE, spend DOUBLE',
             "rows": [
                 (1, '2024-03-16', 400.0),
                 (1, '2024-03-19', 350.0),
                 (2, '2024-03-15', 260.0),
                 (2, '2024-03-20', 300.0),
                 (3, '2024-03-02', 900.0),
                 (3, '2024-03-10', 200.0),
                 (5, '2024-03-14', 500.0),
                 (6, '2024-03-13', 600.0),
                 (6, '2024-03-18', 249.99),
                 (6, '2024-03-20', 250.0),
                 (7, '2024-03-17', 120.0),
                 (7, '2024-03-20', 90.0),
             ]},
        ],
        "sql": """
WITH recent AS (
    SELECT
        advertiser_id,
        SUM(spend) AS recent_spend
    FROM q10_daily_spend
    WHERE spend_date >= (SELECT MAX(spend_date) - 6 FROM q10_daily_spend)
    GROUP BY advertiser_id
)
SELECT
    a.advertiser_id,
    a.advertiser_name,
    a.status                                              AS current_status,
    ROUND(COALESCE(r.recent_spend, 0), 2)                 AS recent_spend,
    CASE WHEN COALESCE(r.recent_spend, 0) >= 500 THEN 'Active' ELSE 'Inactive' END
                                                          AS new_status
FROM q10_advertisers a
LEFT JOIN recent r ON r.advertiser_id = a.advertiser_id
ORDER BY recent_spend DESC, a.advertiser_id
""",
        "pyspark": """
# the review window is anchored to the latest spend date in the data - no driver round-trip
window_start = (
    spark.table("q10_daily_spend")
    .agg(F.date_sub(F.max("spend_date"), 6).alias("window_start"))
)

recent = (
    spark.table("q10_daily_spend")
    .crossJoin(window_start)
    .filter(F.col("spend_date") >= F.col("window_start"))
    .groupBy("advertiser_id")
    .agg(F.sum("spend").alias("recent_spend"))
)

result = (
    spark.table("q10_advertisers")
    .join(recent, on="advertiser_id", how="left")
    .withColumn("new_status",
                F.when(F.coalesce(F.col("recent_spend"), F.lit(0.0)) >= 500,
                       "Active").otherwise("Inactive"))
    .withColumn("recent_spend", F.round(F.coalesce(F.col("recent_spend"), F.lit(0.0)), 2))
    .select("advertiser_id", "advertiser_name",
            F.col("status").alias("current_status"),
            "recent_spend", "new_status")
    .orderBy(F.col("recent_spend").desc(), "advertiser_id")
)
display(result)
""",
    },
    {
        "n": 11,
        "title": 'Cross-Department Salary Comparison',
        "difficulty": 'Hard',
        "tags": ['Inner Joins', 'Subqueries', 'Window Functions'],
        "prompt": (
            'Show every employee next to how their pay compares with their own department and with the '
            "company as a whole. Report the salary, the average salary of the employee's department, the "
            'average salary across the entire company, and how far above or below the department average that '
            'employee sits, as a percentage of that average.'
        ),
        "returns": (
            'one row per employee: salary, department average salary, company average salary, and salary vs '
            'department average (%)'
        ),
        "watch_out": (
            'Both averages have to sit on the same row, and a GROUP BY cannot do that without a self-join or '
            'a scalar subquery - so use window functions: AVG(salary) OVER (PARTITION BY department_id) next '
            'to AVG(salary) OVER (). Compute the percentage from the unrounded averages and round only the '
            "final column, or Support's non-terminating 83333.33 average drifts. An INNER JOIN to departments "
            'is right for the name, and departments with no employees (Legal, Finance) simply do not appear.'
        ),
        "tables": [
            {"name": "q11_employees",
             "cols": 'employee_id INT, employee_name STRING, department_id INT, salary DOUBLE',
             "rows": [
                 (1, 'Ada', 10, 120000.0),
                 (2, 'Brian', 10, 90000.0),
                 (3, 'Chen', 10, 60000.0),
                 (4, 'Dana', 20, 150000.0),
                 (5, 'Eli', 20, 100000.0),
                 (6, 'Fay', 20, 80000.0),
                 (7, 'Gus', 30, 70000.0),
                 (8, 'Hana', 30, 95000.0),
                 (9, 'Ivan', 30, 85000.0),
             ]},
            {"name": "q11_departments",
             "cols": 'department_id INT, department_name STRING',
             "rows": [
                 (10, 'Engineering'),
                 (20, 'Sales'),
                 (30, 'Support'),
                 (40, 'Legal'),
                 (50, 'Finance'),
             ]},
        ],
        "sql": """
WITH compared AS (
    SELECT
        e.employee_id,
        e.employee_name,
        d.department_name,
        e.salary,
        AVG(e.salary) OVER (PARTITION BY e.department_id) AS dept_avg_salary,
        AVG(e.salary) OVER ()                            AS company_avg_salary
    FROM q11_employees e
    JOIN q11_departments d ON d.department_id = e.department_id
)
SELECT
    employee_id,
    employee_name,
    department_name,
    salary,
    ROUND(dept_avg_salary, 2)                                         AS dept_avg_salary,
    ROUND(company_avg_salary, 2)                                      AS company_avg_salary,
    ROUND((salary - dept_avg_salary) * 100.0 / dept_avg_salary, 2)    AS vs_dept_pct
FROM compared
ORDER BY department_name, employee_name
""",
        "pyspark": """
by_department = Window.partitionBy("department_id")
whole_company = Window.partitionBy()

compared = (
    spark.table("q11_employees")
    .join(spark.table("q11_departments"), on="department_id", how="inner")
    .withColumn("dept_avg_salary", F.avg("salary").over(by_department))
    .withColumn("company_avg_salary", F.avg("salary").over(whole_company))
)

result = (
    compared
    .withColumn("vs_dept_pct",
                F.round((F.col("salary") - F.col("dept_avg_salary")) * 100.0
                        / F.col("dept_avg_salary"), 2))
    .withColumn("dept_avg_salary", F.round("dept_avg_salary", 2))
    .withColumn("company_avg_salary", F.round("company_avg_salary", 2))
    .select("employee_id", "employee_name", "department_name", "salary",
            "dept_avg_salary", "company_avg_salary", "vs_dept_pct")
    .orderBy("department_name", "employee_name")
)
display(result)
""",
    },
    {
        "n": 12,
        "title": 'Server Uptime Duration Calculation',
        "difficulty": 'Hard',
        "tags": ['Subqueries', 'Aggregate Functions'],
        "prompt": (
            'Servers report an event when they start and another when they stop, and a few never report the '
            'stop. Add up how long each server was actually up, in days, counting only the start-and-stop '
            'pairs where both events were reported.'
        ),
        "returns": (
            'one row per server that has at least one complete start/stop pair: total uptime in days'
        ),
        "watch_out": (
            'Pair each start with the next event for the same server (LEAD over an ordered row_number, or a '
            'self-join on the next row) and keep the pair only when that next event is a stop: an unclosed '
            'start has NULL next event, or another start. Server 4 only ever started, so it must not appear '
            "at all, and server 2's trailing start must not shorten or extend its 0.5 day. Subtract "
            'timestamps in seconds and divide by 86400 - DAY-granularity differences silently drop the half '
            'days.'
        ),
        "tables": [
            {"name": "q12_server_events",
             "cols": 'server_id INT, event_time TIMESTAMP, event_type STRING',
             "rows": [
                 (1, '2024-04-01 08:00:00', 'start'),
                 (1, '2024-04-01 20:00:00', 'stop'),
                 (1, '2024-04-03 00:00:00', 'start'),
                 (1, '2024-04-04 12:00:00', 'stop'),
                 (2, '2024-04-02 06:00:00', 'start'),
                 (2, '2024-04-02 18:00:00', 'stop'),
                 (2, '2024-04-05 00:00:00', 'start'),
                 (3, '2024-04-01 00:00:00', 'start'),
                 (3, '2024-04-02 06:00:00', 'stop'),
                 (4, '2024-04-06 09:00:00', 'start'),
             ]},
        ],
        "sql": """
WITH paired AS (
    SELECT
        server_id,
        event_time,
        event_type,
        LEAD(event_time) OVER (PARTITION BY server_id ORDER BY event_time) AS next_time,
        LEAD(event_type) OVER (PARTITION BY server_id ORDER BY event_time) AS next_type
    FROM q12_server_events
)
SELECT
    server_id,
    ROUND(SUM(date_diff('second', event_time, next_time)) / 86400.0, 2) AS uptime_days
FROM paired
WHERE event_type = 'start' AND next_type = 'stop'
GROUP BY server_id
ORDER BY server_id
""",
        "pyspark": """
timeline = Window.partitionBy("server_id").orderBy("event_time")

durations = (
    spark.table("q12_server_events")
    .withColumn("next_time", F.lead("event_time").over(timeline))
    .withColumn("next_type", F.lead("event_type").over(timeline))
    .filter((F.col("event_type") == "start") & (F.col("next_type") == "stop"))
    .withColumn("seconds_up",
                F.unix_timestamp("next_time") - F.unix_timestamp("event_time"))
)

result = (
    durations
    .groupBy("server_id")
    .agg(F.round(F.sum("seconds_up") / 86400.0, 2).alias("uptime_days"))
    .orderBy("server_id")
)
display(result)
""",
    },
    {
        "n": 13,
        "title": 'User Popularity Score',
        "difficulty": 'Hard',
        "tags": ['Common Table Expressions', 'Aggregate Functions'],
        "prompt": (
            'Build a popularity score for each user who is actually followed by somebody. For those users '
            'report how many followers they have, how many accounts they follow themselves, and a score of '
            'twice the follower count plus the following count.'
        ),
        "returns": (
            'one row per user who has at least one follower: followers, following, popularity_score'
        ),
        "watch_out": (
            'One pass over the follows table mixes the two roles: GROUP BY follower_id counts who a user '
            'follows, GROUP BY followee_id counts who follows them, and no single grouping gives both. '
            'Aggregate each direction separately and LEFT JOIN them, so a user who is followed but follows '
            'nobody scores with 0 instead of NULL - and users 4, 6 and 7 follow other people yet have no '
            'followers, so they are not in the answer at all. Users 1 and 3 tie on 6, so order by user_id as '
            'the tiebreaker.'
        ),
        "tables": [
            {"name": "q13_follows",
             "cols": 'follower_id INT, followee_id INT',
             "rows": [
                 (1, 2),
                 (1, 3),
                 (4, 2),
                 (5, 2),
                 (2, 3),
                 (6, 1),
                 (7, 1),
                 (3, 5),
                 (2, 5),
                 (3, 8),
             ]},
        ],
        "sql": """
WITH follower_counts AS (
    SELECT followee_id AS user_id, COUNT(*) AS followers
    FROM q13_follows
    GROUP BY followee_id
),
following_counts AS (
    SELECT follower_id AS user_id, COUNT(*) AS following
    FROM q13_follows
    GROUP BY follower_id
)
SELECT
    f.user_id,
    f.followers,
    COALESCE(g.following, 0)                        AS following,
    f.followers * 2 + COALESCE(g.following, 0)      AS popularity_score
FROM follower_counts f
LEFT JOIN following_counts g ON g.user_id = f.user_id
ORDER BY popularity_score DESC, f.user_id
""",
        "pyspark": """
follows = spark.table("q13_follows")

follower_counts = (
    follows.groupBy(F.col("followee_id").alias("user_id"))
    .agg(F.count("*").alias("followers"))
)

following_counts = (
    follows.groupBy(F.col("follower_id").alias("user_id"))
    .agg(F.count("*").alias("following"))
)

result = (
    follower_counts
    .join(following_counts, on="user_id", how="left")
    .withColumn("following", F.coalesce(F.col("following"), F.lit(0)))
    .withColumn("popularity_score", F.col("followers") * 2 + F.col("following"))
    .select("user_id", "followers", "following", "popularity_score")
    .orderBy(F.col("popularity_score").desc(), "user_id")
)
display(result)
""",
    },
    {
        "n": 14,
        "title": 'Month-over-Month Revenue Change',
        "difficulty": 'Hard',
        "tags": ['Order By', 'Common Table Expressions', 'Window Functions'],
        "prompt": (
            'Revenue is reviewed month by month. For every month in the order log, show the revenue booked in '
            'that month, the revenue of the month immediately before it, and the percentage change between '
            'the two, oldest month first.'
        ),
        "returns": (
            'one row per calendar month, ordered by month: revenue, previous month revenue, month-over-month '
            'change %'
        ),
        "watch_out": (
            'The month must come from the order date itself (truncate the timestamp to the month start, so '
            'the column is a real month value) - taking the first seven characters of the date breaks the '
            'moment the data crosses a year. LAG runs over the single global month series with no partition, '
            'and the first month legitimately shows NULL for both the previous revenue and the change. '
            "February repeats January's 1500, so that change is a genuine 0.00%, not NULL, and the division "
            'needs a zero guard.'
        ),
        "tables": [
            {"name": "q14_orders",
             "cols": 'order_id INT, order_date DATE, amount DOUBLE',
             "rows": [
                 (1, '2024-01-05', 1000.0),
                 (2, '2024-01-20', 500.0),
                 (3, '2024-02-03', 800.0),
                 (4, '2024-02-18', 700.0),
                 (5, '2024-03-11', 1200.0),
                 (6, '2024-04-02', 300.0),
                 (7, '2024-04-27', 200.0),
                 (8, '2024-05-09', 900.0),
                 (9, '2024-05-30', 600.0),
                 (10, '2024-06-14', 1100.0),
             ]},
        ],
        "sql": """
WITH monthly AS (
    SELECT
        date_trunc('month', order_date) AS month,
        SUM(amount)                     AS revenue
    FROM q14_orders
    GROUP BY date_trunc('month', order_date)
),
compared AS (
    SELECT
        month,
        revenue,
        LAG(revenue) OVER (ORDER BY month) AS prev_revenue
    FROM monthly
)
SELECT
    month,
    ROUND(revenue, 2)                                            AS revenue,
    ROUND(prev_revenue, 2)                                       AS prev_revenue,
    ROUND((revenue - prev_revenue) * 100.0
          / NULLIF(prev_revenue, 0), 2)                          AS mom_change_pct
FROM compared
ORDER BY month
""",
        "pyspark": """
monthly = (
    spark.table("q14_orders")
    .withColumn("month", F.date_trunc("month", F.col("order_date")))
    .groupBy("month")
    .agg(F.sum("amount").alias("revenue"))
)

result = (
    monthly
    .withColumn("prev_revenue", F.lag("revenue").over(Window.orderBy("month")))
    .withColumn("mom_change_pct",
                F.round((F.col("revenue") - F.col("prev_revenue")) * 100.0
                        / F.when(F.col("prev_revenue") == 0, None)
                         .otherwise(F.col("prev_revenue")), 2))
    .withColumn("revenue", F.round("revenue", 2))
    .withColumn("prev_revenue", F.round("prev_revenue", 2))
    .select("month", "revenue", "prev_revenue", "mom_change_pct")
    .orderBy("month")
)
display(result)
""",
    },
    {
        "n": 15,
        "title": 'Demand Forecasting Simplification',
        "difficulty": 'Hard',
        "tags": ['Common Table Expressions', 'Aggregate Functions'],
        "prompt": (
            'Summarise the trip log by month. For each month show how many trips were taken, the total '
            'distance, the total cost, the average cost of a kilometre, and a running total of cost from the '
            'first month through the current one.'
        ),
        "returns": (
            'one row per month, ordered by month: trips, total distance, total cost, average cost per km, '
            'cumulative cost'
        ),
        "watch_out": (
            'Average cost per kilometre is total cost divided by total distance, never the average of each '
            "trip's own rate: in February those give 3.00 and 3.20, because averaging rates weights a 7.5 km "
            "trip the same as a 12.5 km one. Round only at the end (April's true 2.333... must round from "
            '2.333..., not from a rounded 105 or 45), and the cumulative column is a windowed SUM over the '
            'monthly totals ordered by month, so it cannot be computed in the same aggregation pass as the '
            'GROUP BY.'
        ),
        "tables": [
            {"name": "q15_trips",
             "cols": 'trip_id INT, trip_date DATE, distance_km DOUBLE, cost DOUBLE',
             "rows": [
                 (1, '2024-01-04', 10.0, 25.0),
                 (2, '2024-01-19', 40.0, 60.0),
                 (3, '2024-02-02', 12.5, 30.0),
                 (4, '2024-02-15', 7.5, 30.0),
                 (5, '2024-03-08', 22.0, 55.0),
                 (6, '2024-03-21', 18.0, 45.0),
                 (7, '2024-04-05', 30.0, 66.0),
                 (8, '2024-04-26', 15.0, 39.0),
                 (9, '2024-05-07', 25.0, 50.0),
                 (10, '2024-05-23', 35.0, 80.0),
                 (11, '2024-05-30', 5.0, 15.0),
             ]},
        ],
        "sql": """
WITH monthly AS (
    SELECT
        date_trunc('month', trip_date) AS month,
        COUNT(*)                       AS trips,
        SUM(distance_km)               AS total_distance_km,
        SUM(cost)                      AS total_cost
    FROM q15_trips
    GROUP BY date_trunc('month', trip_date)
)
SELECT
    month,
    trips,
    ROUND(total_distance_km, 2)                                AS total_distance_km,
    ROUND(total_cost, 2)                                       AS total_cost,
    ROUND(total_cost / NULLIF(total_distance_km, 0), 2)        AS avg_cost_per_km,
    ROUND(SUM(total_cost) OVER (ORDER BY month), 2)            AS cumulative_cost
FROM monthly
ORDER BY month
""",
        "pyspark": """
monthly = (
    spark.table("q15_trips")
    .withColumn("month", F.date_trunc("month", F.col("trip_date")))
    .groupBy("month")
    .agg(F.count("*").alias("trips"),
         F.sum("distance_km").alias("total_distance_km"),
         F.sum("cost").alias("total_cost"))
)

result = (
    monthly
    .withColumn("avg_cost_per_km",
                F.round(F.col("total_cost")
                        / F.when(F.col("total_distance_km") == 0, None)
                         .otherwise(F.col("total_distance_km")), 2))
    .withColumn("cumulative_cost",
                F.round(F.sum("total_cost").over(Window.orderBy("month")), 2))
    .withColumn("total_distance_km", F.round("total_distance_km", 2))
    .withColumn("total_cost", F.round("total_cost", 2))
    .select("month", "trips", "total_distance_km", "total_cost",
            "avg_cost_per_km", "cumulative_cost")
    .orderBy("month")
)
display(result)
""",
    },
    {
        "n": 16,
        "title": 'Friday Purchase Patterns in Q1',
        "difficulty": 'Hard',
        "tags": ['Common Table Expressions', 'Date/Time Functions', 'Filter'],
        "prompt": (
            'Friday spending is compared across the first quarter of 2024. For every Friday that falls inside '
            'that quarter, show how many purchases were made, the total spend, and the average spend per '
            'purchase.'
        ),
        "returns": (
            'one row per Friday inside Q1 2024, dated YYYY-MM-DD: purchases, total spend, average spend'
        ),
        "watch_out": (
            'The quarter and the weekday are two independent filters and both have to bite: 2023-12-29 and '
            '2024-04-05 are Fridays but sit outside Q1, so they must be excluded, and the Thursdays and '
            "Saturdays inside Q1 must not survive either. Only three of the quarter's thirteen Fridays have "
            'purchases - the other ten are absent rows, not zero rows. Day-of-week numbering is engine '
            "specific (DuckDB 0=Sunday, Spark 1=Sunday), the average comes from the amounts so 2024-03-15's "
            '111.666... rounds to 111.67, and the reported date is formatted rather than left engine-typed.'
        ),
        "tables": [
            {"name": "q16_purchases",
             "cols": 'purchase_id INT, user_id INT, purchase_date DATE, amount DOUBLE',
             "rows": [
                 (1, 101, '2024-01-04', 40.0),
                 (2, 102, '2024-01-05', 100.0),
                 (3, 103, '2024-01-05', 60.0),
                 (4, 101, '2024-01-06', 25.0),
                 (5, 104, '2024-02-09', 90.0),
                 (6, 105, '2024-02-09', 30.0),
                 (7, 102, '2024-02-10', 55.0),
                 (8, 106, '2024-03-15', 200.0),
                 (9, 107, '2024-03-15', 55.0),
                 (10, 108, '2024-03-15', 80.0),
                 (11, 109, '2023-12-29', 500.0),
                 (12, 110, '2024-04-05', 400.0),
             ]},
        ],
        "sql": """
WITH q1_fridays AS (
    SELECT purchase_date, amount
    FROM q16_purchases
    WHERE purchase_date BETWEEN DATE '2024-01-01' AND DATE '2024-03-31'
      AND dayofweek(purchase_date) = 5
)
SELECT
    strftime(purchase_date, '%Y-%m-%d') AS purchase_date,
    COUNT(*)                            AS purchases,
    ROUND(SUM(amount), 2)               AS total_spend,
    ROUND(AVG(amount), 2)               AS avg_spend
FROM q1_fridays
GROUP BY strftime(purchase_date, '%Y-%m-%d')
ORDER BY purchase_date
""",
        "pyspark": """
q1_fridays = (
    spark.table("q16_purchases")
    .filter(F.col("purchase_date").between(F.lit("2024-01-01").cast("date"),
                                           F.lit("2024-03-31").cast("date")))
    .filter(F.dayofweek("purchase_date") == 6)
)

result = (
    q1_fridays
    .groupBy(F.date_format("purchase_date", "yyyy-MM-dd").alias("purchase_date"))
    .agg(F.count("*").alias("purchases"),
         F.round(F.sum("amount"), 2).alias("total_spend"),
         F.round(F.avg("amount"), 2).alias("avg_spend"))
    .orderBy("purchase_date")
)
display(result)
""",
    },
    {
        "n": 17,
        "title": 'GPS Coordinate Distance Calculation',
        "difficulty": 'Hard',
        "tags": ['Mathematical Functions', 'Order By'],
        "prompt": (
            'Each vehicle reported a handful of GPS pings. For every vehicle, measure how far apart its '
            'northernmost and southernmost ping are, in kilometres, so the fleet team can see which vehicles '
            'actually cover ground.'
        ),
        "returns": 'one row per vehicle: vehicle_id, distance_km',
        "watch_out": (
            'Two pings can share the highest latitude, so the extreme point needs a deterministic tiebreaker '
            '(lowest point_id) or the join fans out; a vehicle with a single ping is a legitimate zero, and '
            'the haversine constant must be 6371 in both engines.'
        ),
        "tables": [
            {"name": "q17_gps_points",
             "cols": 'point_id INT, vehicle_id STRING, recorded_at DATE, lat DOUBLE, lon DOUBLE',
             "rows": [
                 (1, 'V1', '2024-05-01', 40.71, -74.0),
                 (2, 'V1', '2024-05-02', 40.85, -73.95),
                 (3, 'V1', '2024-05-03', 40.7, -74.02),
                 (4, 'V1', '2024-05-04', 40.78, -73.98),
                 (5, 'V2', '2024-05-01', 34.05, -118.25),
                 (6, 'V2', '2024-05-02', 34.05, -118.4),
                 (7, 'V2', '2024-05-03', 33.9, -118.3),
                 (8, 'V3', '2024-05-01', 51.5, -0.12),
             ]},
        ],
        "sql": """
WITH ranked AS (
    SELECT
        vehicle_id,
        lat,
        lon,
        ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY lat DESC, point_id) AS north_rank,
        ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY lat ASC, point_id)  AS south_rank
    FROM q17_gps_points
),
extremes AS (
    SELECT
        n.vehicle_id,
        n.lat AS north_lat,
        n.lon AS north_lon,
        s.lat AS south_lat,
        s.lon AS south_lon
    FROM ranked n
    JOIN ranked s
      ON s.vehicle_id = n.vehicle_id
     AND n.north_rank = 1
     AND s.south_rank = 1
)
SELECT
    vehicle_id,
    ROUND(2 * 6371.0 * asin(sqrt(
        pow(sin(radians((north_lat - south_lat) / 2)), 2)
        + cos(radians(north_lat)) * cos(radians(south_lat))
          * pow(sin(radians((north_lon - south_lon) / 2)), 2)
    )), 2) AS distance_km
FROM extremes
ORDER BY vehicle_id
""",
        "pyspark": """
points = spark.table("q17_gps_points")

north = (
    points
    .withColumn("north_rank",
                F.row_number().over(Window.partitionBy("vehicle_id")
                                    .orderBy(F.col("lat").desc(), F.col("point_id"))))
    .filter(F.col("north_rank") == 1)
    .select("vehicle_id",
            F.col("lat").alias("north_lat"),
            F.col("lon").alias("north_lon"))
)

south = (
    points
    .withColumn("south_rank",
                F.row_number().over(Window.partitionBy("vehicle_id")
                                    .orderBy(F.col("lat"), F.col("point_id"))))
    .filter(F.col("south_rank") == 1)
    .select("vehicle_id",
            F.col("lat").alias("south_lat"),
            F.col("lon").alias("south_lon"))
)

result = (
    north.join(south, on="vehicle_id", how="inner")
    .withColumn("distance_km",
                F.round(2 * 6371.0 * F.asin(F.sqrt(
                    F.pow(F.sin(F.radians((F.col("north_lat") - F.col("south_lat")) / 2)), 2)
                    + F.cos(F.radians(F.col("north_lat"))) * F.cos(F.radians(F.col("south_lat")))
                      * F.pow(F.sin(F.radians((F.col("north_lon") - F.col("south_lon")) / 2)), 2)
                )), 2))
    .select("vehicle_id", "distance_km")
    .orderBy("vehicle_id")
)
display(result)
""",
    },
    {
        "n": 18,
        "title": 'Department Top Salaries',
        "difficulty": 'Hard',
        "tags": ['Mathematical Functions', 'Common Table Expressions', 'Rank'],
        "prompt": (
            'For every department, list the people earning one of the three highest salary levels, and show '
            "what share of the department's payroll each of them takes. The report is meant to expose how "
            'top-heavy each department is.'
        ),
        "returns": (
            'one row per employee holding a top-3 distinct salary: department_name, employee_name, salary, '
            'salary_share_pct'
        ),
        "watch_out": (
            'DENSE_RANK, not ROW_NUMBER: two people on the same salary share one of the three salary levels, '
            "and ROW_NUMBER would silently drop the second of them. The share is that person's salary over "
            "the whole department's payroll, so departments with no staff produce nothing at all."
        ),
        "tables": [
            {"name": "q18_employees",
             "cols": 'employee_id INT, employee_name STRING, department_id INT, salary DOUBLE',
             "rows": [
                 (1, 'Alice', 10, 120.0),
                 (2, 'Bob', 10, 120.0),
                 (3, 'Carol', 10, 100.0),
                 (4, 'Dave', 10, 90.0),
                 (5, 'Eve', 10, 70.0),
                 (6, 'Frank', 20, 95.0),
                 (7, 'Grace', 20, 85.0),
                 (8, 'Heidi', 30, 60.0),
                 (9, 'Ivan', 30, 55.0),
                 (10, 'Judy', 30, 50.0),
                 (11, 'Mallory', 30, 45.0),
             ]},
            {"name": "q18_departments",
             "cols": 'department_id INT, department_name STRING',
             "rows": [
                 (10, 'Engineering'),
                 (20, 'Sales'),
                 (30, 'Support'),
                 (40, 'Legal'),
                 (50, 'Marketing'),
             ]},
        ],
        "sql": """
WITH ranked AS (
    SELECT
        department_id,
        employee_name,
        salary,
        DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rank,
        SUM(salary)  OVER (PARTITION BY department_id)                     AS department_payroll
    FROM q18_employees
)
SELECT
    d.department_name,
    r.employee_name,
    r.salary,
    ROUND(r.salary * 100.0 / r.department_payroll, 2) AS salary_share_pct
FROM ranked r
JOIN q18_departments d ON d.department_id = r.department_id
WHERE r.salary_rank <= 3
ORDER BY d.department_name, r.salary DESC, r.employee_name
""",
        "pyspark": """
by_department = Window.partitionBy("department_id")

ranked = (
    spark.table("q18_employees")
    .withColumn("salary_rank", F.dense_rank().over(by_department.orderBy(F.col("salary").desc())))
    .withColumn("department_payroll", F.sum("salary").over(by_department))
)

result = (
    ranked
    .filter(F.col("salary_rank") <= 3)
    .join(spark.table("q18_departments"), on="department_id", how="inner")
    .withColumn("salary_share_pct",
                F.round(F.col("salary") * 100.0 / F.col("department_payroll"), 2))
    .select("department_name", "employee_name", "salary", "salary_share_pct")
)
display(result)
""",
    },
    {
        "n": 19,
        "title": 'Consecutive Day User Activity',
        "difficulty": 'Hard',
        "tags": ['Lag', 'Common Table Expressions', 'Gaps And Islands'],
        "prompt": (
            'The retention team wants to know which users kept showing up day after day. For every user, find '
            'each unbroken run of three or more consecutive active days and report when that run started, '
            'when it ended and how long it lasted.'
        ),
        "returns": 'one row per qualifying run: user_id, streak_start, streak_end, streak_days',
        "watch_out": (
            'Collapse the raw log to distinct user-days first - a second action on the same day breaks the '
            'date-minus-row-number arithmetic and splits one run into two. A two-day run is not a streak and '
            'must not appear.'
        ),
        "tables": [
            {"name": "q19_user_activity",
             "cols": 'user_id INT, activity_date DATE',
             "rows": [
                 (1, '2024-01-01'),
                 (1, '2024-01-02'),
                 (1, '2024-01-03'),
                 (1, '2024-01-06'),
                 (1, '2024-01-06'),
                 (1, '2024-01-07'),
                 (1, '2024-01-08'),
                 (2, '2024-01-01'),
                 (2, '2024-01-02'),
                 (3, '2024-02-10'),
                 (3, '2024-02-11'),
                 (3, '2024-02-12'),
             ]},
        ],
        "sql": """
WITH daily AS (
    SELECT DISTINCT user_id, activity_date
    FROM q19_user_activity
),
islands AS (
    SELECT
        user_id,
        activity_date,
        date_diff('day', DATE '1970-01-01', activity_date)
            - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date) AS island_key
    FROM daily
)
SELECT
    user_id,
    date_trunc('day', MIN(activity_date)) AS streak_start,
    date_trunc('day', MAX(activity_date)) AS streak_end,
    COUNT(*)                              AS streak_days
FROM islands
GROUP BY user_id, island_key
HAVING COUNT(*) >= 3
ORDER BY user_id, streak_start
""",
        "pyspark": """
daily = spark.table("q19_user_activity").select("user_id", "activity_date").distinct()

islands = (
    daily
    .withColumn("day_number",
                F.datediff("activity_date", F.lit("1970-01-01").cast("date")))
    .withColumn("island_key",
                F.col("day_number")
                - F.row_number().over(Window.partitionBy("user_id").orderBy("activity_date")))
)

result = (
    islands
    .groupBy("user_id", "island_key")
    .agg(F.date_trunc("day", F.min("activity_date")).alias("streak_start"),
         F.date_trunc("day", F.max("activity_date")).alias("streak_end"),
         F.count("*").alias("streak_days"))
    .filter(F.col("streak_days") >= 3)
    .select("user_id", "streak_start", "streak_end", "streak_days")
)
display(result)
""",
    },
    {
        "n": 20,
        "title": 'Daily Order Volume Analysis',
        "difficulty": 'Hard',
        "tags": ['Aggregate Functions', 'Date/Time Functions'],
        "prompt": (
            'How many orders does each user place per day? A simple daily volume table that a support team '
            'can scan for sudden spikes.'
        ),
        "returns": 'one row per user per day that has orders: order_date, user_id, orders',
        "watch_out": (
            'One row per user-day, not per order: the timestamp has to be truncated to the day before '
            'grouping, otherwise every order is its own group.'
        ),
        "tables": [
            {"name": "q20_orders",
             "cols": 'order_id INT, user_id INT, ordered_at TIMESTAMP, amount DOUBLE',
             "rows": [
                 (1, 11, '2024-04-01 08:15:00', 25.0),
                 (2, 11, '2024-04-01 19:40:00', 40.0),
                 (3, 12, '2024-04-01 12:00:00', 15.0),
                 (4, 11, '2024-04-02 09:05:00', 60.0),
                 (5, 13, '2024-04-02 21:30:00', 12.5),
                 (6, 12, '2024-04-03 11:11:00', 33.0),
             ]},
        ],
        "sql": """
SELECT
    date_trunc('day', ordered_at) AS order_date,
    user_id,
    COUNT(*)                      AS orders
FROM q20_orders
GROUP BY 1, 2
ORDER BY 1, 2
""",
        "pyspark": """
result = (
    spark.table("q20_orders")
    .groupBy(F.date_trunc("day", "ordered_at").alias("order_date"), "user_id")
    .agg(F.count("*").alias("orders"))
    .select("order_date", "user_id", "orders")
)
display(result)
""",
    },
    {
        "n": 21,
        "title": 'CRM Order Summary Report',
        "difficulty": 'Medium',
        "tags": ['Inner Joins', 'Filter'],
        "prompt": (
            'The sales team wants one flat report of everything that actually shipped in 2024. For each line '
            'of each completed order, show who bought it, where they are, when the order was placed, what the '
            'product was and what that line was worth.'
        ),
        "returns": (
            'one row per order line on a completed 2024 order: customer_name, region, order_date, '
            'product_name, quantity, line_total'
        ),
        "watch_out": (
            'Inner joins silently drop the orders with no line items, the orphan order whose customer does '
            'not exist and the catalogue product nobody ordered - that is correct here, but filter status and '
            'year on the order itself, not on the item rows, or a cancelled order keeps its lines.'
        ),
        "tables": [
            {"name": "q21_customers",
             "cols": 'customer_id INT, customer_name STRING, region STRING',
             "rows": [
                 (1, 'Acme Corp', 'North'),
                 (2, 'Bravo Ltd', 'South'),
                 (3, 'Cedar GmbH', 'West'),
                 (4, 'Delta SA', 'East'),
                 (6, 'Echo Inc', 'Central'),
             ]},
            {"name": "q21_orders",
             "cols": 'order_id INT, customer_id INT, order_date DATE, status STRING',
             "rows": [
                 (101, 1, '2024-01-15', 'completed'),
                 (102, 1, '2024-03-02', 'cancelled'),
                 (103, 2, '2024-02-20', 'completed'),
                 (104, 3, '2023-12-05', 'completed'),
                 (105, 3, '2024-04-11', 'completed'),
                 (106, 2, '2024-05-01', 'pending'),
                 (107, 5, '2024-06-07', 'completed'),
                 (108, 1, '2024-07-01', 'completed'),
             ]},
            {"name": "q21_order_items",
             "cols": 'order_id INT, product_id INT, quantity INT, unit_price DOUBLE',
             "rows": [
                 (101, 201, 2, 25.0),
                 (101, 202, 1, 120.0),
                 (103, 201, 3, 25.0),
                 (103, 203, 2, 40.5),
                 (104, 202, 1, 110.0),
                 (105, 202, 4, 120.0),
                 (106, 203, 1, 40.5),
                 (107, 201, 5, 25.0),
             ]},
            {"name": "q21_products",
             "cols": 'product_id INT, product_name STRING',
             "rows": [
                 (201, 'Widget'),
                 (202, 'Gadget'),
                 (203, 'Doohickey'),
                 (204, 'Sprocket'),
                 (205, 'Gizmo'),
             ]},
        ],
        "sql": """
SELECT
    c.customer_name,
    c.region,
    date_trunc('day', o.order_date) AS order_date,
    p.product_name,
    i.quantity,
    ROUND(i.quantity * i.unit_price, 2) AS line_total
FROM q21_orders o
JOIN q21_customers   c ON c.customer_id = o.customer_id
JOIN q21_order_items i ON i.order_id    = o.order_id
JOIN q21_products    p ON p.product_id  = i.product_id
WHERE o.status = 'completed'
  AND o.order_date >= DATE '2024-01-01'
  AND o.order_date <  DATE '2025-01-01'
ORDER BY o.order_id, p.product_name
""",
        "pyspark": """
orders = (
    spark.table("q21_orders")
    .filter((F.col("status") == "completed")
            & (F.col("order_date") >= F.lit("2024-01-01").cast("date"))
            & (F.col("order_date") < F.lit("2025-01-01").cast("date")))
)

result = (
    orders
    .join(spark.table("q21_customers"), on="customer_id", how="inner")
    .join(spark.table("q21_order_items"), on="order_id", how="inner")
    .join(spark.table("q21_products"), on="product_id", how="inner")
    .withColumn("order_date", F.date_trunc("day", F.col("order_date")))
    .withColumn("line_total", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .select("customer_name", "region", "order_date", "product_name", "quantity", "line_total")
)
display(result)
""",
    },
    {
        "n": 22,
        "title": 'F&B Product Sales Summary',
        "difficulty": 'Medium',
        "tags": ['Complex Joins', 'Aggregate Functions'],
        "prompt": (
            'The food and beverage team wants one line per catalogue product: how many units it sold, the '
            'revenue it earned, how much stock is left, and whether that leftover is low enough to reorder.'
        ),
        "returns": (
            'one row per product: product_id, product_name, category, units_sold, revenue, stock_remaining, '
            'low_stock'
        ),
        "watch_out": (
            'Keep every product, including the ones with no sales - a plain join drops them and SUM then '
            'arrives as NULL. Stock can also go negative when the shelf count was never updated, and the '
            'reorder flag fires at 20 or below, not below 20.'
        ),
        "tables": [
            {"name": "q22_products",
             "cols": 'product_id INT, product_name STRING, category STRING, stock_on_hand INT',
             "rows": [
                 (1, 'Espresso Beans', 'Coffee', 50),
                 (2, 'Cold Brew', 'Coffee', 30),
                 (3, 'Oat Latte', 'Coffee', 20),
                 (4, 'Croissant', 'Bakery', 40),
                 (5, 'Blueberry Muffin', 'Bakery', 10),
                 (6, 'Matcha Powder', 'Tea', 20),
             ]},
            {"name": "q22_order_items",
             "cols": 'order_id INT, product_id INT, quantity INT, unit_price DOUBLE',
             "rows": [
                 (1001, 1, 10, 12.5),
                 (1001, 4, 6, 3.75),
                 (1002, 2, 8, 4.5),
                 (1002, 5, 4, 3.0),
                 (1003, 1, 5, 12.5),
                 (1003, 3, 12, 5.25),
                 (1004, 2, 4, 4.5),
                 (1005, 3, 9, 5.25),
                 (1006, 5, 9, 3.0),
                 (1007, 9, 2, 10.0),
             ]},
        ],
        "sql": """
WITH sales AS (
    SELECT
        product_id,
        SUM(quantity)              AS units_sold,
        SUM(quantity * unit_price) AS revenue
    FROM q22_order_items
    GROUP BY product_id
)
SELECT
    p.product_id,
    p.product_name,
    p.category,
    COALESCE(s.units_sold, 0)                        AS units_sold,
    ROUND(COALESCE(s.revenue, 0), 2)                 AS revenue,
    p.stock_on_hand - COALESCE(s.units_sold, 0)      AS stock_remaining,
    p.stock_on_hand - COALESCE(s.units_sold, 0) <= 20 AS low_stock
FROM q22_products p
LEFT JOIN sales s ON s.product_id = p.product_id
ORDER BY p.product_id
""",
        "pyspark": """
sales = (
    spark.table("q22_order_items")
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("units_sold"),
         F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"))
)

result = (
    spark.table("q22_products")
    .join(sales, on="product_id", how="left")
    .withColumn("units_sold", F.coalesce(F.col("units_sold"), F.lit(0)))
    .withColumn("revenue", F.round(F.coalesce(F.col("revenue"), F.lit(0.0)), 2))
    .withColumn("stock_remaining", F.col("stock_on_hand") - F.col("units_sold"))
    .withColumn("low_stock", F.col("stock_remaining") <= 20)
    .select("product_id", "product_name", "category", "units_sold", "revenue",
            "stock_remaining", "low_stock")
)
display(result)
""",
    },
    {
        "n": 23,
        "title": 'E-Commerce Date Dimension Analysis',
        "difficulty": 'Medium',
        "tags": ['Date/Time Functions'],
        "prompt": (
            'Build a small calendar dimension out of the order log: one row for every distinct day on which '
            'something was ordered, with the calendar attributes the BI team wants for slicing - year, '
            'quarter, month, week and weekday, plus a weekend flag.'
        ),
        "returns": (
            'one row per distinct order date: order_date, year_num, quarter_num, month_num, month_name, '
            'week_of_year, weekday_name, is_weekend'
        ),
        "watch_out": (
            'The dimension is distinct dates, not orders - two orders on the same day are one calendar row. '
            'Month and weekday names must come from explicit labels: to_char/date_format wording and weekday '
            'numbering differ between engines.'
        ),
        "tables": [
            {"name": "q23_orders",
             "cols": 'order_id INT, order_date DATE, amount DOUBLE',
             "rows": [
                 (1, '2024-01-05', 100.0),
                 (2, '2024-01-05', 250.0),
                 (3, '2024-02-14', 75.5),
                 (4, '2024-03-31', 40.0),
                 (5, '2024-04-01', 60.0),
                 (6, '2024-06-15', 120.0),
                 (7, '2024-06-15', 30.0),
                 (8, '2024-09-02', 90.0),
                 (9, '2024-12-25', 200.0),
                 (10, '2025-01-01', 50.0),
             ]},
        ],
        "sql": """
WITH days AS (
    SELECT DISTINCT date_trunc('day', order_date) AS order_date
    FROM q23_orders
)
SELECT
    order_date,
    EXTRACT(YEAR FROM order_date)    AS year_num,
    EXTRACT(QUARTER FROM order_date) AS quarter_num,
    EXTRACT(MONTH FROM order_date)   AS month_num,
    CASE EXTRACT(MONTH FROM order_date)
        WHEN 1  THEN 'January'   WHEN 2  THEN 'February' WHEN 3  THEN 'March'
        WHEN 4  THEN 'April'     WHEN 5  THEN 'May'      WHEN 6  THEN 'June'
        WHEN 7  THEN 'July'      WHEN 8  THEN 'August'   WHEN 9  THEN 'September'
        WHEN 10 THEN 'October'   WHEN 11 THEN 'November' ELSE 'December'
    END AS month_name,
    week(order_date) AS week_of_year,
    CASE EXTRACT(ISODOW FROM order_date)
        WHEN 1 THEN 'Monday'    WHEN 2 THEN 'Tuesday' WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'  WHEN 5 THEN 'Friday'  WHEN 6 THEN 'Saturday'
        ELSE 'Sunday'
    END AS weekday_name,
    EXTRACT(ISODOW FROM order_date) >= 6 AS is_weekend
FROM days
ORDER BY order_date
""",
        "pyspark": """
days = (
    spark.table("q23_orders")
    .select("order_date")
    .distinct()
    .withColumn("order_date", F.date_trunc("day", F.col("order_date")))
    .withColumn("year_num", F.year("order_date"))
    .withColumn("quarter_num", F.quarter("order_date"))
    .withColumn("month_num", F.month("order_date"))
    .withColumn("month_name",
                F.when(F.col("month_num") == 1, "January")
                 .when(F.col("month_num") == 2, "February")
                 .when(F.col("month_num") == 3, "March")
                 .when(F.col("month_num") == 4, "April")
                 .when(F.col("month_num") == 5, "May")
                 .when(F.col("month_num") == 6, "June")
                 .when(F.col("month_num") == 7, "July")
                 .when(F.col("month_num") == 8, "August")
                 .when(F.col("month_num") == 9, "September")
                 .when(F.col("month_num") == 10, "October")
                 .when(F.col("month_num") == 11, "November")
                 .otherwise("December"))
    .withColumn("week_of_year", F.weekofyear("order_date"))
    .withColumn("weekday_name",
                F.when(F.dayofweek("order_date") == 2, "Monday")
                 .when(F.dayofweek("order_date") == 3, "Tuesday")
                 .when(F.dayofweek("order_date") == 4, "Wednesday")
                 .when(F.dayofweek("order_date") == 5, "Thursday")
                 .when(F.dayofweek("order_date") == 6, "Friday")
                 .when(F.dayofweek("order_date") == 7, "Saturday")
                 .otherwise("Sunday"))
    .withColumn("is_weekend", F.dayofweek("order_date").isin(1, 7))
)

result = days.select("order_date", "year_num", "quarter_num", "month_num", "month_name",
                     "week_of_year", "weekday_name", "is_weekend")
display(result)
""",
    },
    {
        "n": 24,
        "title": 'Venture-Funded Startup Analysis',
        "difficulty": 'Medium',
        "tags": ['Aggregate Functions'],
        "prompt": (
            'The fund wants to know which venture capitalists are writing cheques bigger than the typical '
            'startup raises. Work out the average total raised per startup, then report every VC that has put '
            'more than that into the ecosystem.'
        ),
        "returns": 'one row per qualifying VC: vc_id, rounds, startups_backed, total_invested',
        "watch_out": (
            'The benchmark is the average of per-startup totals, not the average size of a round, so a '
            'startup with several rounds counts once. The test is strictly greater than, so a VC sitting '
            'exactly on the average is out.'
        ),
        "tables": [
            {"name": "q24_funding_rounds",
             "cols": 'round_id INT, startup_id INT, vc_id INT, round_date DATE, amount DOUBLE',
             "rows": [
                 (1, 1, 1, '2023-02-10', 130.0),
                 (2, 1, 4, '2023-06-18', 70.0),
                 (3, 2, 1, '2023-03-05', 100.0),
                 (4, 2, 2, '2023-09-22', 100.0),
                 (5, 3, 2, '2024-01-14', 120.0),
                 (6, 3, 3, '2024-04-02', 110.0),
                 (7, 4, 3, '2024-02-27', 100.0),
                 (8, 4, 4, '2024-07-09', 130.0),
                 (9, 5, 5, '2024-05-21', 140.0),
             ]},
        ],
        "sql": """
WITH startup_totals AS (
    SELECT
        startup_id,
        SUM(amount) AS startup_total
    FROM q24_funding_rounds
    GROUP BY startup_id
),
vc_totals AS (
    SELECT
        vc_id,
        COUNT(*)                   AS rounds,
        COUNT(DISTINCT startup_id) AS startups_backed,
        SUM(amount)                AS total_invested
    FROM q24_funding_rounds
    GROUP BY vc_id
)
SELECT
    vc_id,
    rounds,
    startups_backed,
    ROUND(total_invested, 2) AS total_invested
FROM vc_totals
WHERE total_invested > (SELECT AVG(startup_total) FROM startup_totals)
ORDER BY vc_id
""",
        "pyspark": """
funding = spark.table("q24_funding_rounds")

average_per_startup = (
    funding
    .groupBy("startup_id")
    .agg(F.sum("amount").alias("startup_total"))
    .agg(F.avg("startup_total").alias("average_per_startup"))
)

result = (
    funding
    .groupBy("vc_id")
    .agg(F.count("*").alias("rounds"),
         F.countDistinct("startup_id").alias("startups_backed"),
         F.sum("amount").alias("total_invested"))
    .crossJoin(average_per_startup)
    .filter(F.col("total_invested") > F.col("average_per_startup"))
    .withColumn("total_invested", F.round("total_invested", 2))
    .select("vc_id", "rounds", "startups_backed", "total_invested")
)
display(result)
""",
    },
    {
        "n": 25,
        "title": 'Discount Calculation Error Detection',
        "difficulty": 'Medium',
        "tags": ['Regular Expression'],
        "prompt": (
            'Each order line carries a list price, a discount written as text such as "15%" (sometimes with '
            'stray spaces around it, and occasionally not a percentage at all), and the price actually '
            'charged. Work out what the charged price should have been from the list price and the discount, '
            'and list only the lines where the recorded sale price is more than a cent away from that. Show '
            'the expected price and how far off the recorded price is.'
        ),
        "returns": (
            'one row per suspect line: item_id, list_price, discount, sale_price, expected_price, difference '
            '(sale_price minus expected_price), money to 2dp'
        ),
        "watch_out": (
            'Anchor the pattern at both ends so junk like "12.5" (no percent sign) is rejected instead of '
            'being silently read as 12%, capture the digits on their own so surrounding spaces cannot break '
            'the cast, and compare against the unrounded expected price before rounding for display.'
        ),
        "tables": [
            {"name": "q25_order_items",
             "cols": 'item_id INT, list_price DOUBLE, discount STRING, sale_price DOUBLE',
             "rows": [
                 (1, 100.0, '15%', 85.0),
                 (2, 200.0, ' 20 % ', 150.0),
                 (3, 50.0, '10%', 45.0),
                 (4, 80.0, '25%', 80.0),
                 (5, 120.0, '12.5', 120.0),
                 (6, 200.0, '7.5%', 200.0),
                 (7, 60.0, ' 5 % ', 57.0),
                 (8, 40.0, '0%', 40.0),
             ]},
        ],
        "sql": r"""
WITH parsed AS (
    SELECT
        item_id,
        list_price,
        discount,
        sale_price,
        CAST(regexp_extract(discount, '^\s*([0-9]+(\.[0-9]+)?)\s*%\s*$', 1) AS DOUBLE) AS pct
    FROM q25_order_items
    WHERE regexp_matches(discount, '^\s*([0-9]+(\.[0-9]+)?)\s*%\s*$')
),
checked AS (
    SELECT
        item_id,
        list_price,
        discount,
        sale_price,
        list_price * (1 - pct / 100.0)                  AS raw_expected,
        ROUND(list_price * (1 - pct / 100.0), 2)        AS expected_price
    FROM parsed
)
SELECT
    item_id,
    list_price,
    discount,
    sale_price,
    expected_price,
    ROUND(sale_price - raw_expected, 2) AS difference
FROM checked
WHERE abs(sale_price - raw_expected) > 0.01
ORDER BY item_id
""",
        "pyspark": r"""
PATTERN = r"^\s*([0-9]+(\.[0-9]+)?)\s*%\s*$"

parsed = (
    spark.table("q25_order_items")
    .filter(F.col("discount").rlike(PATTERN))
    .withColumn("pct", F.regexp_extract("discount", PATTERN, 1).cast("double"))
    .withColumn("raw_expected", F.col("list_price") * (1 - F.col("pct") / 100.0))
)

result = (
    parsed
    .filter(F.abs(F.col("sale_price") - F.col("raw_expected")) > 0.01)
    .select(
        "item_id",
        "list_price",
        "discount",
        "sale_price",
        F.round(F.col("raw_expected"), 2).alias("expected_price"),
        F.round(F.col("sale_price") - F.col("raw_expected"), 2).alias("difference"),
    )
    .orderBy("item_id")
)
display(result)
""",
    },
    {
        "n": 26,
        "title": 'Mortgage Rate Comparison',
        "difficulty": 'Medium',
        "tags": ['Complex Joins', 'Aggregate Functions'],
        "prompt": (
            'Lenders quote rates for the same mortgage term, and borrowers want to see the range. For every '
            'term, report the lender offering the lowest rate and the lender offering the highest, the gap '
            "between those two rates in percentage points, and the average rate quoted across that term's "
            'offers. A lender that has not quoted anything should not produce a row.'
        ),
        "returns": (
            'one row per term: term_years, cheapest_lender, cheapest_rate, priciest_lender, priciest_rate, '
            'rate_spread_pct, avg_rate'
        ),
        "watch_out": (
            'Rank the offers inside each term and bring the lender name back twice (two ranked joins, or '
            'conditional aggregation over one ranked set). Break rate ties on offer_id so both engines pick '
            'the same offer, and drive the result from the offers table so a lender with no quotes never '
            'appears.'
        ),
        "tables": [
            {"name": "q26_lenders",
             "cols": 'lender_id INT, lender_name STRING',
             "rows": [
                 (1, 'Summit Bank'),
                 (2, 'Cascade Credit'),
                 (3, 'Harbor Mutual'),
                 (4, 'Prairie Lending'),
                 (5, 'Lakeside Loans'),
             ]},
            {"name": "q26_mortgage_offers",
             "cols": 'offer_id INT, lender_id INT, term_years INT, rate_pct DOUBLE, points DOUBLE',
             "rows": [
                 (1, 1, 15, 5.25, 1.0),
                 (2, 2, 15, 5.1, 0.5),
                 (3, 3, 15, 5.1, 1.25),
                 (4, 4, 15, 5.75, 0.0),
                 (5, 1, 30, 6.1, 0.75),
                 (6, 2, 30, 6.45, 1.5),
                 (7, 3, 30, 5.95, 1.0),
                 (8, 4, 30, 6.45, 0.25),
             ]},
        ],
        "sql": """
WITH joined AS (
    SELECT
        o.term_years,
        o.offer_id,
        o.rate_pct,
        l.lender_name
    FROM q26_mortgage_offers o
    JOIN q26_lenders l ON l.lender_id = o.lender_id
),
ranked AS (
    SELECT
        joined.*,
        ROW_NUMBER() OVER (PARTITION BY term_years
                           ORDER BY rate_pct ASC, offer_id ASC)  AS rn_cheap,
        ROW_NUMBER() OVER (PARTITION BY term_years
                           ORDER BY rate_pct DESC, offer_id ASC) AS rn_pricey
    FROM joined
)
SELECT
    term_years,
    MAX(CASE WHEN rn_cheap = 1 THEN lender_name END)   AS cheapest_lender,
    MAX(CASE WHEN rn_cheap = 1 THEN rate_pct END)      AS cheapest_rate,
    MAX(CASE WHEN rn_pricey = 1 THEN lender_name END)  AS priciest_lender,
    MAX(CASE WHEN rn_pricey = 1 THEN rate_pct END)     AS priciest_rate,
    ROUND(MAX(CASE WHEN rn_pricey = 1 THEN rate_pct END)
          - MAX(CASE WHEN rn_cheap = 1 THEN rate_pct END), 2) AS rate_spread_pct,
    ROUND(AVG(rate_pct), 2)                            AS avg_rate
FROM ranked
GROUP BY term_years
ORDER BY term_years
""",
        "pyspark": """
offers = spark.table("q26_mortgage_offers").join(
    spark.table("q26_lenders"), on="lender_id", how="inner"
)

cheap_window = Window.partitionBy("term_years").orderBy(
    F.col("rate_pct").asc(), F.col("offer_id").asc()
)
pricey_window = Window.partitionBy("term_years").orderBy(
    F.col("rate_pct").desc(), F.col("offer_id").asc()
)

ranked = (
    offers
    .withColumn("rn_cheap", F.row_number().over(cheap_window))
    .withColumn("rn_pricey", F.row_number().over(pricey_window))
)

result = (
    ranked
    .groupBy("term_years")
    .agg(
        F.max(F.when(F.col("rn_cheap") == 1, F.col("lender_name"))).alias("cheapest_lender"),
        F.max(F.when(F.col("rn_cheap") == 1, F.col("rate_pct"))).alias("cheapest_rate"),
        F.max(F.when(F.col("rn_pricey") == 1, F.col("lender_name"))).alias("priciest_lender"),
        F.max(F.when(F.col("rn_pricey") == 1, F.col("rate_pct"))).alias("priciest_rate"),
        F.round(F.avg("rate_pct"), 2).alias("avg_rate"),
    )
    .withColumn("rate_spread_pct",
                F.round(F.col("priciest_rate") - F.col("cheapest_rate"), 2))
    .select("term_years", "cheapest_lender", "cheapest_rate", "priciest_lender",
            "priciest_rate", "rate_spread_pct", "avg_rate")
    .orderBy("term_years")
)
display(result)
""",
    },
    {
        "n": 27,
        "title": 'Regex Pattern Matching',
        "difficulty": 'Medium',
        "tags": ['Mathematical Functions', 'Regular Expression'],
        "prompt": (
            'The expression column holds simple arithmetic written as text, mixed in with junk. Keep only the '
            'entries that are exactly one whole number, one of the four basic operators, and another whole '
            'number, ignoring spaces around the parts, and evaluate each one. A division by zero should come '
            'back with no value instead of failing.'
        ),
        "returns": (
            'one row per valid expression: expression_id, expression, computed_value (2dp, null when dividing '
            'by zero)'
        ),
        "watch_out": (
            "Anchor the pattern at both ends or partial junk such as '10+5 apples' and '2.5+1' will be read "
            'as valid arithmetic; capture the operator so the CASE can branch on it, and check the divisor '
            'for zero before dividing.'
        ),
        "tables": [
            {"name": "q27_expressions",
             "cols": 'expression_id INT, expression STRING',
             "rows": [
                 (1, '12+7'),
                 (2, '3 * 4'),
                 (3, '9/3'),
                 (4, '12+'),
                 (5, '5^2'),
                 (6, ''),
                 (7, ' 8 / 0 '),
                 (8, '20- 6'),
                 (9, '10+5 apples'),
                 (10, '2.5+1'),
                 (11, '7/2'),
                 (12, '10/3'),
             ]},
        ],
        "sql": r"""
WITH parsed AS (
    SELECT
        expression_id,
        expression,
        TRY_CAST(regexp_extract(expression,
                                '^\s*([0-9]+)\s*([+*/-])\s*([0-9]+)\s*$', 1) AS DOUBLE) AS lhs,
        regexp_extract(expression,
                       '^\s*([0-9]+)\s*([+*/-])\s*([0-9]+)\s*$', 2)                  AS operator,
        TRY_CAST(regexp_extract(expression,
                                '^\s*([0-9]+)\s*([+*/-])\s*([0-9]+)\s*$', 3) AS DOUBLE) AS rhs
    FROM q27_expressions
    WHERE regexp_matches(expression, '^\s*([0-9]+)\s*([+*/-])\s*([0-9]+)\s*$')
)
SELECT
    expression_id,
    expression,
    ROUND(
        CASE operator
            WHEN '+' THEN lhs + rhs
            WHEN '-' THEN lhs - rhs
            WHEN '*' THEN lhs * rhs
            WHEN '/' THEN CASE WHEN rhs = 0 THEN NULL ELSE lhs / rhs END
        END, 2) AS computed_value
FROM parsed
ORDER BY expression_id
""",
        "pyspark": r"""
PATTERN = r"^\s*([0-9]+)\s*([+*/-])\s*([0-9]+)\s*$"

parsed = (
    spark.table("q27_expressions")
    .filter(F.col("expression").rlike(PATTERN))
    .withColumn("lhs", F.regexp_extract("expression", PATTERN, 1).cast("double"))
    .withColumn("operator", F.regexp_extract("expression", PATTERN, 2))
    .withColumn("rhs", F.regexp_extract("expression", PATTERN, 3).cast("double"))
)

computed = (
    F.when(F.col("operator") == "+", F.col("lhs") + F.col("rhs"))
    .when(F.col("operator") == "-", F.col("lhs") - F.col("rhs"))
    .when(F.col("operator") == "*", F.col("lhs") * F.col("rhs"))
    .when(
        F.col("operator") == "/",
        F.when(F.col("rhs") == 0, F.lit(None).cast("double"))
        .otherwise(F.col("lhs") / F.col("rhs")),
    )
)

result = (
    parsed
    .withColumn("computed_value", F.round(computed, 2))
    .select("expression_id", "expression", "computed_value")
    .orderBy("expression_id")
)
display(result)
""",
    },
    {
        "n": 28,
        "title": 'Busiest Flight Route Analysis',
        "difficulty": 'Medium',
        "tags": ['Complex Joins'],
        "prompt": (
            'Which city pairs carry the most passengers? A route counts the same whether it is flown A to B '
            'or B to A, so the two directions have to be folded together before ranking.'
        ),
        "returns": 'one row per city pair: route, flights, total passengers (top 3)',
        "watch_out": (
            'Origin/destination are two directions of one route - grouping on the raw pair splits the traffic '
            'in half. Use least()/greatest() (or F.least/F.greatest) on the two airport codes, not string '
            'concatenation of the raw columns.'
        ),
        "tables": [
            {"name": "q28_flights",
             "cols": 'flight_id INT, origin STRING, destination STRING, passengers INT',
             "rows": [
                 (1, 'JFK', 'LAX', 180),
                 (2, 'LAX', 'JFK', 165),
                 (3, 'JFK', 'LAX', 190),
                 (4, 'ORD', 'SFO', 140),
                 (5, 'SFO', 'ORD', 150),
                 (6, 'JFK', 'ORD', 120),
                 (7, 'LAX', 'SEA', 90),
                 (8, 'SEA', 'LAX', 95),
             ]},
        ],
        "sql": """
WITH routes AS (
    SELECT
        least(origin, destination)    AS city_a,
        greatest(origin, destination) AS city_b,
        passengers
    FROM q28_flights
)
SELECT
    city_a || ' <-> ' || city_b AS route,
    COUNT(*)                    AS flights,
    SUM(passengers)             AS total_passengers
FROM routes
GROUP BY city_a, city_b
ORDER BY total_passengers DESC, route
LIMIT 3
""",
        "pyspark": """
routes = (
    spark.table("q28_flights")
    .withColumn("city_a", F.least("origin", "destination"))
    .withColumn("city_b", F.greatest("origin", "destination"))
)

result = (
    routes
    .groupBy("city_a", "city_b")
    .agg(F.count("*").alias("flights"), F.sum("passengers").alias("total_passengers"))
    .withColumn("route", F.concat_ws(" <-> ", "city_a", "city_b"))
    .orderBy(F.col("total_passengers").desc(), "route")
    .limit(3)
    .select("route", "flights", "total_passengers")
)
display(result)
""",
    },
    {
        "n": 29,
        "title": 'Flooring Store Sales Analysis',
        "difficulty": 'Medium',
        "tags": ['String Manipulation'],
        "prompt": (
            'Sales are recorded per product, but the buying team thinks in materials. The material is the '
            'middle segment of the SKU code, which is sometimes typed in lower case, and it should be shown '
            'in title case. Report the units sold and the revenue for every material that actually sold '
            'something.'
        ),
        "returns": 'one row per material sold: material (title-cased), units_sold, revenue (2dp)',
        "watch_out": (
            "Splitting is 1-based in SQL (split_part(sku, '-', 2)) and 0-based in Spark (F.split(sku, "
            "'-')[1]) - take the second segment in both. Title-case portably with upper(substr(x, 1, 1)) || "
            'lower(substr(x, 2, length(x))) (F.upper + F.lower + F.substring in Spark) instead of a '
            'dialect-specific initcap, and join from the order lines so a product that never sold leaves no '
            'row behind.'
        ),
        "tables": [
            {"name": "q29_products",
             "cols": 'product_id INT, sku STRING, unit_price DOUBLE',
             "rows": [
                 (1, 'PRIME-OAK-1200', 4.5),
                 (2, 'classic-oak-600', 3.25),
                 (3, 'PRIME-VINYL-600', 2.8),
                 (4, 'classic-vinyl-900', 3.1),
                 (5, 'PRIME-WALNUT-1400', 6.75),
                 (6, 'classic-walnut-300', 5.2),
                 (7, 'PRIME-BAMBOO-500', 8.0),
             ]},
            {"name": "q29_order_items",
             "cols": 'order_id INT, product_id INT, quantity INT',
             "rows": [
                 (101, 1, 10),
                 (102, 1, 5),
                 (103, 2, 20),
                 (104, 3, 12),
                 (105, 4, 8),
                 (106, 5, 3),
                 (107, 6, 7),
                 (108, 3, 4),
                 (109, 2, 2),
             ]},
        ],
        "sql": """
WITH sku_parts AS (
    SELECT
        product_id,
        unit_price,
        upper(split_part(sku, '-', 2)) AS material
    FROM q29_products
),
materials AS (
    SELECT
        product_id,
        unit_price,
        upper(substr(material, 1, 1))
            || lower(substr(material, 2, length(material))) AS material_name
    FROM sku_parts
)
SELECT
    m.material_name                          AS material,
    SUM(oi.quantity)                         AS units_sold,
    ROUND(SUM(oi.quantity * m.unit_price), 2) AS revenue
FROM materials m
JOIN q29_order_items oi ON oi.product_id = m.product_id
GROUP BY m.material_name
ORDER BY revenue DESC, material
""",
        "pyspark": """
materials = (
    spark.table("q29_products")
    .withColumn("material", F.upper(F.split("sku", "-")[1]))
    .withColumn(
        "material_name",
        F.concat_ws(
            "",
            F.upper(F.substring("material", 1, 1)),
            F.lower(F.substring("material", 2, F.length("material"))),
        ),
    )
)

result = (
    materials
    .join(spark.table("q29_order_items"), on="product_id", how="inner")
    .groupBy("material_name")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("revenue"),
    )
    .select(F.col("material_name").alias("material"), "units_sold", "revenue")
    .orderBy(F.col("revenue").desc(), "material")
)
display(result)
""",
    },
    {
        "n": 30,
        "title": 'Duplicate Product Listing Detection',
        "difficulty": 'Medium',
        "tags": ['Subqueries', 'Aggregate Functions', 'Having'],
        "prompt": (
            'Marketplace vendors sometimes list the same product more than once. For each product, count how '
            'many vendors have more than one listing for it, and report only the products where at least one '
            'vendor does that. Products whose vendors each list them exactly once must not appear.'
        ),
        "returns": (
            'one row per product with at least one repeat vendor: product_name, duplicate_vendors'
        ),
        "watch_out": (
            'A vendor with three listings of the same product counts once, not three times, and a vendor with '
            'a single listing counts zero - collapse to one row per product and vendor (HAVING COUNT(*) > 1) '
            'before counting vendors.'
        ),
        "tables": [
            {"name": "q30_vendor_listings",
             "cols": 'listing_id INT, vendor_id INT, product_name STRING, price DOUBLE',
             "rows": [
                 (1, 101, 'Widget A', 19.99),
                 (2, 101, 'Widget A', 21.5),
                 (3, 102, 'Widget A', 18.75),
                 (4, 102, 'Widget A', 18.95),
                 (5, 103, 'Widget A', 20.0),
                 (6, 101, 'Gadget B', 5.0),
                 (7, 103, 'Gadget B', 4.95),
                 (8, 104, 'Sprocket C', 7.5),
                 (9, 104, 'Sprocket C', 7.75),
                 (10, 104, 'Sprocket C', 8.0),
                 (11, 105, 'Gizmo D', 3.1),
                 (12, 105, 'Gizmo D', 3.2),
             ]},
        ],
        "sql": """
WITH repeat_vendors AS (
    SELECT
        product_name,
        vendor_id
    FROM q30_vendor_listings
    GROUP BY product_name, vendor_id
    HAVING COUNT(*) > 1
)
SELECT
    product_name,
    COUNT(*) AS duplicate_vendors
FROM repeat_vendors
GROUP BY product_name
ORDER BY duplicate_vendors DESC, product_name
""",
        "pyspark": """
repeat_vendors = (
    spark.table("q30_vendor_listings")
    .groupBy("product_name", "vendor_id")
    .agg(F.count("*").alias("listing_count"))
    .filter(F.col("listing_count") > 1)
)

result = (
    repeat_vendors
    .groupBy("product_name")
    .agg(F.count("vendor_id").alias("duplicate_vendors"))
    .orderBy(F.col("duplicate_vendors").desc(), "product_name")
)
display(result)
""",
    },
    {
        "n": 31,
        "title": 'Private Equity Portfolio Valuation',
        "difficulty": 'Medium',
        "tags": ['Aggregate Functions'],
        "prompt": (
            "A private equity firm's positions are marked to market on every date their companies have a "
            'price. For each firm and each trading date, report the total value of the positions priced that '
            'day and how many holdings were priced. A firm whose companies have no price record at all should '
            'not appear.'
        ),
        "returns": (
            'one row per firm per priced trading date: firm_id, price_date, portfolio_value (2dp), '
            'holdings_count'
        ),
        "watch_out": (
            'Join holdings to prices on company_id alone - adding the date or the firm to the join condition '
            'fans the positions out - and let the price table supply the dates so a company with no price on '
            'a date contributes no row rather than a zero.'
        ),
        "tables": [
            {"name": "q31_holdings",
             "cols": 'firm_id INT, company_id INT, shares INT',
             "rows": [
                 (1, 101, 1000),
                 (1, 102, 500),
                 (1, 103, 200),
                 (2, 101, 300),
                 (2, 103, 750),
                 (3, 104, 120),
                 (4, 105, 60),
             ]},
            {"name": "q31_prices",
             "cols": 'company_id INT, price_date DATE, price DOUBLE',
             "rows": [
                 (101, '2024-03-01', 10.0),
                 (101, '2024-03-02', 10.5),
                 (102, '2024-03-01', 25.0),
                 (102, '2024-03-02', 24.0),
                 (103, '2024-03-01', 8.0),
                 (103, '2024-03-02', 8.25),
                 (104, '2024-03-02', 40.0),
                 (106, '2024-03-01', 15.0),
             ]},
        ],
        "sql": """
SELECT
    h.firm_id,
    strftime(p.price_date, '%Y-%m-%d') AS price_date,
    ROUND(SUM(h.shares * p.price), 2)  AS portfolio_value,
    COUNT(*)                           AS holdings_count
FROM q31_holdings h
JOIN q31_prices p ON p.company_id = h.company_id
GROUP BY h.firm_id, p.price_date
ORDER BY p.price_date, h.firm_id
""",
        "pyspark": """
priced = (
    spark.table("q31_holdings")
    .join(spark.table("q31_prices"), on="company_id", how="inner")
    .withColumn("position_value", F.col("shares") * F.col("price"))
)

result = (
    priced
    .groupBy("firm_id", "price_date")
    .agg(
        F.round(F.sum("position_value"), 2).alias("portfolio_value"),
        F.count("*").alias("holdings_count"),
    )
    .select(
        "firm_id",
        F.date_format("price_date", "yyyy-MM-dd").alias("price_date"),
        "portfolio_value",
        "holdings_count",
    )
    .orderBy("price_date", "firm_id")
)
display(result)
""",
    },
    {
        "n": 32,
        "title": 'Trending Tweet Tracker',
        "difficulty": 'Medium',
        "tags": ['Order By', 'Aggregate Functions', 'Window Functions'],
        "prompt": (
            'For each user, show the daily tweet count next to a rolling three-day average made of that day '
            "and the two days before it. The first days of a user's history have fewer than three days behind "
            "them, so their average covers only the days that exist, and one user's window must never pick up "
            "another user's days."
        ),
        "returns": 'one row per user per day: user_id, tweet_date, tweet_count, rolling_avg_3d (2dp)',
        "watch_out": (
            'Use an explicit ROWS BETWEEN 2 PRECEDING AND CURRENT ROW frame rather than the default RANGE '
            'frame, partition by user, and order by the date (tweet_id only as a tiebreaker) - ordering by '
            'the id alone scrambles the window because these ids are not in date order.'
        ),
        "tables": [
            {"name": "q32_tweets",
             "cols": 'tweet_id INT, user_id INT, tweet_date DATE, tweet_count INT',
             "rows": [
                 (1, 101, '2024-01-01', 10),
                 (2, 101, '2024-01-02', 20),
                 (3, 101, '2024-01-03', 30),
                 (4, 101, '2024-01-04', 40),
                 (5, 101, '2024-01-05', 50),
                 (6, 101, '2024-01-06', 60),
                 (11, 102, '2024-01-01', 5),
                 (9, 102, '2024-01-02', 3),
                 (12, 102, '2024-01-03', 8),
                 (8, 102, '2024-01-04', 12),
                 (10, 102, '2024-01-05', 7),
                 (7, 102, '2024-01-06', 9),
             ]},
        ],
        "sql": """
WITH daily AS (
    SELECT
        user_id,
        tweet_date,
        tweet_count,
        ROUND(AVG(tweet_count) OVER (
            PARTITION BY user_id
            ORDER BY tweet_date, tweet_id
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS rolling_avg_3d
    FROM q32_tweets
)
SELECT
    user_id,
    strftime(tweet_date, '%Y-%m-%d') AS tweet_date,
    tweet_count,
    rolling_avg_3d
FROM daily
ORDER BY user_id, tweet_date
""",
        "pyspark": """
daily_window = (
    Window.partitionBy("user_id")
    .orderBy("tweet_date", "tweet_id")
    .rowsBetween(-2, 0)
)

result = (
    spark.table("q32_tweets")
    .withColumn("rolling_avg_3d", F.round(F.avg("tweet_count").over(daily_window), 2))
    .select(
        "user_id",
        F.date_format("tweet_date", "yyyy-MM-dd").alias("tweet_date"),
        "tweet_count",
        "rolling_avg_3d",
    )
    .orderBy("user_id", "tweet_date")
)
display(result)
""",
    },
    {
        "n": 33,
        "title": 'IoT Sensor Data Analysis',
        "difficulty": 'Medium',
        "tags": ['Casting', 'Window Functions'],
        "prompt": (
            'A sensor fleet writes temperatures as text, and a few readings are corrupt. For every sensor, '
            "flag the readings that came in above 1.5x that sensor's own average temperature. Corrupt "
            'readings must not blow up the query and must not drag the average around.'
        ),
        "returns": (
            'one row per anomalous reading: sensor_id, reading_id, reading_ts, reading_value, avg_reading, '
            'anomaly_ratio'
        ),
        "watch_out": (
            "The average is a window over a CAST column, so cast first and average second; a plain CAST('bad' "
            'AS DOUBLE) is not portable - DuckDB raises, and Spark raises too once ANSI mode is on (the '
            'default in Spark 4) - which is exactly why both sides use TRY_CAST / a guarded cast.'
        ),
        "tables": [
            {"name": "q33_sensor_readings",
             "cols": 'reading_id INT, sensor_id STRING, reading_ts TIMESTAMP, reading STRING',
             "rows": [
                 (1, 'S1', '2024-03-01 08:00:00', '10.0'),
                 (2, 'S1', '2024-03-01 09:00:00', '12.0'),
                 (3, 'S1', '2024-03-01 10:00:00', '11.0'),
                 (4, 'S1', '2024-03-01 11:00:00', '40.0'),
                 (5, 'S2', '2024-03-01 08:00:00', '5.0'),
                 (6, 'S2', '2024-03-01 09:00:00', '5.0'),
                 (7, 'S2', '2024-03-01 10:00:00', '6.0'),
                 (8, 'S2', '2024-03-01 11:00:00', '30.0'),
                 (9, 'S3', '2024-03-01 08:00:00', '19.0'),
                 (10, 'S3', '2024-03-01 09:00:00', 'bad'),
                 (11, 'S3', '2024-03-01 10:00:00', '20.0'),
                 (12, 'S3', '2024-03-01 11:00:00', '21.0'),
             ]},
        ],
        "sql": """
WITH parsed AS (
    SELECT
        reading_id,
        sensor_id,
        reading_ts,
        TRY_CAST(reading AS DOUBLE) AS reading_value
    FROM q33_sensor_readings
),
scored AS (
    SELECT
        reading_id,
        sensor_id,
        reading_ts,
        reading_value,
        AVG(reading_value) OVER (PARTITION BY sensor_id) AS sensor_avg
    FROM parsed
)
SELECT
    sensor_id,
    reading_id,
    reading_ts,
    reading_value,
    ROUND(sensor_avg, 2)                 AS avg_reading,
    ROUND(reading_value / sensor_avg, 2) AS anomaly_ratio
FROM scored
WHERE reading_value > 1.5 * sensor_avg
ORDER BY sensor_id, reading_id
""",
        "pyspark": r"""
sensor_window = Window.partitionBy("sensor_id")

result = (
    spark.table("q33_sensor_readings")
    .withColumn(
        "reading_value",
        F.when(
            F.col("reading").rlike(r"^-?\d+(\.\d+)?$"),
            F.col("reading").cast("double"),
        ),
    )
    .withColumn("sensor_avg", F.avg("reading_value").over(sensor_window))
    .filter(F.col("reading_value") > F.lit(1.5) * F.col("sensor_avg"))
    .select(
        "sensor_id",
        "reading_id",
        "reading_ts",
        F.round("reading_value", 2).alias("reading_value"),
        F.round("sensor_avg", 2).alias("avg_reading"),
        F.round(F.col("reading_value") / F.col("sensor_avg"), 2).alias("anomaly_ratio"),
    )
)
display(result)
""",
    },
    {
        "n": 34,
        "title": 'User Third Transaction',
        "difficulty": 'Medium',
        "tags": ['Row_number', 'Partitioning By'],
        "prompt": (
            'A payments team wants to study the third purchase each customer ever made, to see whether spend '
            'jumps once people are invested. Users with one or two transactions are not interesting yet. When '
            'several transactions land on the same day, break the tie by the lower transaction id.'
        ),
        "returns": (
            'one row per user that has a third transaction: user_id, transaction_id, spend, transaction_date'
        ),
        "watch_out": (
            'ROW_NUMBER over a per-user window ordered by date and id - RANK would hand the same number to '
            'same-day transactions and could return several rows for one user, and ordering by date alone '
            'picks whichever tied row the engine happens to see first.'
        ),
        "tables": [
            {"name": "q34_transactions",
             "cols": 'transaction_id INT, user_id INT, spend DOUBLE, transaction_date DATE',
             "rows": [
                 (101, 1, 20.0, '2024-01-05'),
                 (102, 1, 15.0, '2024-01-03'),
                 (103, 1, 30.0, '2024-01-10'),
                 (104, 2, 44.0, '2024-01-08'),
                 (105, 3, 50.0, '2024-02-01'),
                 (106, 3, 25.0, '2024-02-02'),
                 (107, 3, 75.0, '2024-02-03'),
                 (108, 3, 60.0, '2024-02-03'),
                 (109, 4, 10.0, '2024-03-01'),
                 (110, 4, 20.0, '2024-03-02'),
                 (111, 4, 30.0, '2024-03-03'),
                 (112, 5, 99.0, '2024-03-09'),
             ]},
        ],
        "sql": """
WITH ranked AS (
    SELECT
        user_id,
        transaction_id,
        spend,
        transaction_date,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY transaction_date, transaction_id
        ) AS txn_number
    FROM q34_transactions
)
SELECT
    user_id,
    transaction_id,
    spend,
    CAST(transaction_date AS TIMESTAMP) AS transaction_date  -- pin the type: DATE formats differently per engine
FROM ranked
WHERE txn_number = 3
ORDER BY user_id
""",
        "pyspark": """
txn_window = Window.partitionBy("user_id").orderBy("transaction_date", "transaction_id")

result = (
    spark.table("q34_transactions")
    .withColumn("txn_number", F.row_number().over(txn_window))
    .filter(F.col("txn_number") == 3)
    .select(
        "user_id",
        "transaction_id",
        "spend",
        F.col("transaction_date").cast("timestamp").alias("transaction_date"),
    )
)
display(result)
""",
    },
    {
        "n": 35,
        "title": 'Snapchat Activity Breakdown',
        "difficulty": 'Medium',
        "tags": ['Inner Joins', 'Order By', 'Aggregate Functions'],
        "prompt": (
            'Product analytics wants the time each age bucket spends sending versus receiving snaps, with '
            "each side also shown as a share of that bucket's total. Users whose activity cannot be tied to "
            'an age bucket are excluded from the report entirely.'
        ),
        "returns": (
            'one row per age bucket: age_bucket, total_minutes, send_minutes, receive_minutes, '
            'send_percentage, receive_percentage'
        ),
        "watch_out": (
            'An inner join to users silently drops activity rows from users with no age bucket (and buckets '
            'whose users never logged anything), so do not expect a row per bucket in the users table; '
            'percentages must come from the unrounded sums, not from already-rounded minutes.'
        ),
        "tables": [
            {"name": "q35_users",
             "cols": 'user_id INT, age_bucket STRING',
             "rows": [
                 (1, '18-24'),
                 (2, '25-34'),
                 (3, '18-24'),
                 (4, '35-44'),
                 (5, '25-34'),
             ]},
            {"name": "q35_activities",
             "cols": (
                 'activity_id INT, user_id INT, activity_type STRING, time_spent_minutes DOUBLE'
             ),
             "rows": [
                 (1, 1, 'send', 30.0),
                 (2, 1, 'receive', 20.0),
                 (3, 2, 'send', 15.0),
                 (4, 2, 'receive', 45.0),
                 (5, 1, 'send', 10.0),
                 (6, 2, 'send', 5.0),
                 (7, 9, 'send', 100.0),
                 (8, 9, 'receive', 50.0),
                 (9, 3, 'receive', 12.0),
             ]},
        ],
        "sql": """
WITH bucket_totals AS (
    SELECT
        u.age_bucket,
        SUM(a.time_spent_minutes)                                        AS total_minutes,
        SUM(a.time_spent_minutes) FILTER (WHERE a.activity_type = 'send')    AS send_minutes,
        SUM(a.time_spent_minutes) FILTER (WHERE a.activity_type = 'receive') AS receive_minutes
    FROM q35_users AS u
    JOIN q35_activities AS a
        ON a.user_id = u.user_id
    GROUP BY u.age_bucket
)
SELECT
    age_bucket,
    ROUND(total_minutes, 2)                            AS total_minutes,
    ROUND(send_minutes, 2)                             AS send_minutes,
    ROUND(receive_minutes, 2)                          AS receive_minutes,
    ROUND(100.0 * send_minutes / total_minutes, 2)     AS send_percentage,
    ROUND(100.0 * receive_minutes / total_minutes, 2)  AS receive_percentage
FROM bucket_totals
ORDER BY age_bucket
""",
        "pyspark": """
bucket_totals = (
    spark.table("q35_activities").alias("a")
    .join(spark.table("q35_users").alias("u"),
          F.col("a.user_id") == F.col("u.user_id"),
          how="inner")
    .groupBy(F.col("u.age_bucket").alias("age_bucket"))
    .agg(
        F.sum(F.col("a.time_spent_minutes")).alias("total_minutes"),
        F.sum(F.when(F.col("a.activity_type") == "send",
                     F.col("a.time_spent_minutes"))).alias("send_minutes"),
        F.sum(F.when(F.col("a.activity_type") == "receive",
                     F.col("a.time_spent_minutes"))).alias("receive_minutes"),
    )
)

result = bucket_totals.select(
    "age_bucket",
    F.round("total_minutes", 2).alias("total_minutes"),
    F.round("send_minutes", 2).alias("send_minutes"),
    F.round("receive_minutes", 2).alias("receive_minutes"),
    F.round(F.lit(100.0) * F.col("send_minutes") / F.col("total_minutes"), 2).alias("send_percentage"),
    F.round(F.lit(100.0) * F.col("receive_minutes") / F.col("total_minutes"), 2).alias("receive_percentage"),
)
display(result)
""",
    },
    {
        "n": 36,
        "title": 'Top Grossing Product Categories',
        "difficulty": 'Medium',
        "tags": ['Rank'],
        "prompt": (
            'Merchandising wants the two best-selling products in every category, measured by the money those '
            'products actually rang up. Products that tie on revenue both belong in the answer. Products that '
            'never sold are left out.'
        ),
        "returns": (
            'one row per product ranked in the top 2 of its category: category, product_name, total_spend, '
            'spend_rank'
        ),
        "watch_out": (
            'RANK over a per-category window - DENSE_RANK or a global LIMIT 2 gives a different answer, and a '
            'category whose second place is a tie returns three products, which is correct.'
        ),
        "tables": [
            {"name": "q36_products",
             "cols": 'product_id INT, product_name STRING, category STRING',
             "rows": [
                 (1, 'Widget', 'Hardware'),
                 (2, 'Gadget', 'Hardware'),
                 (3, 'Sprocket', 'Hardware'),
                 (4, 'Doohickey', 'Hardware'),
                 (5, 'App', 'Software'),
                 (6, 'Suite', 'Software'),
                 (7, 'Plugin', 'Software'),
                 (8, 'Legacy', 'Hardware'),
             ]},
            {"name": "q36_order_items",
             "cols": 'order_id INT, product_id INT, quantity INT, unit_price DOUBLE',
             "rows": [
                 (201, 1, 3, 4.5),
                 (202, 1, 2, 8.25),
                 (203, 2, 4, 7.5),
                 (204, 3, 2, 10.0),
                 (205, 4, 1, 5.5),
                 (206, 5, 2, 50.0),
                 (207, 6, 4, 12.5),
                 (208, 7, 2, 25.0),
             ]},
        ],
        "sql": """
WITH product_spend AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        SUM(oi.quantity * oi.unit_price) AS total_spend
    FROM q36_products AS p
    JOIN q36_order_items AS oi
        ON oi.product_id = p.product_id
    GROUP BY p.product_id, p.product_name, p.category
),
ranked AS (
    SELECT
        category,
        product_name,
        total_spend,
        RANK() OVER (PARTITION BY category ORDER BY total_spend DESC) AS spend_rank
    FROM product_spend
)
SELECT
    category,
    product_name,
    ROUND(total_spend, 2) AS total_spend,
    spend_rank
FROM ranked
WHERE spend_rank <= 2
ORDER BY category, spend_rank, product_name
""",
        "pyspark": """
spend_window = Window.partitionBy("category").orderBy(F.col("total_spend").desc())

product_spend = (
    spark.table("q36_order_items").alias("oi")
    .join(spark.table("q36_products").alias("p"),
          F.col("oi.product_id") == F.col("p.product_id"),
          how="inner")
    .groupBy(F.col("p.product_id"), F.col("p.category"), F.col("p.product_name"))
    .agg(F.sum(F.col("oi.quantity") * F.col("oi.unit_price")).alias("total_spend"))
)

result = (
    product_spend
    .withColumn("spend_rank", F.rank().over(spend_window))
    .filter(F.col("spend_rank") <= 2)
    .select(
        "category",
        "product_name",
        F.round("total_spend", 2).alias("total_spend"),
        "spend_rank",
    )
)
display(result)
""",
    },
    {
        "n": 37,
        "title": 'High Salary Employee Identification',
        "difficulty": 'Medium',
        "tags": ['Rank'],
        "prompt": (
            'HR wants the three best-paid people in every department, ties included, for a compensation '
            'review.'
        ),
        "returns": (
            'one row per employee ranked in the top 3 of their department: department, employee, salary, '
            'salary_rank'
        ),
        "watch_out": (
            'RANK/DENSE_RANK over a partitioned window - a global ORDER BY salary LIMIT 3 returns the top 3 '
            'of the company, not of each department.'
        ),
        "tables": [
            {"name": "q37_employees",
             "cols": 'employee_id INT, employee_name STRING, department STRING, salary DOUBLE',
             "rows": [
                 (1, 'Ana', 'Data', 145000.0),
                 (2, 'Ben', 'Data', 132000.0),
                 (3, 'Cara', 'Data', 132000.0),
                 (4, 'Dan', 'Data', 118000.0),
                 (5, 'Eve', 'Sales', 99000.0),
                 (6, 'Finn', 'Sales', 121000.0),
                 (7, 'Gus', 'Sales', 87000.0),
                 (8, 'Hana', 'Sales', 121000.0),
             ]},
        ],
        "sql": """
WITH ranked AS (
    SELECT
        department,
        employee_name,
        salary,
        RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank
    FROM q37_employees
)
SELECT department, employee_name, salary, salary_rank
FROM ranked
WHERE salary_rank <= 3
ORDER BY department, salary_rank, employee_name
""",
        "pyspark": """
w = Window.partitionBy("department").orderBy(F.col("salary").desc())

result = (
    spark.table("q37_employees")
    .withColumn("salary_rank", F.rank().over(w))
    .filter(F.col("salary_rank") <= 3)
    .select("department", "employee_name", "salary", "salary_rank")
)
display(result)
""",
    },
    {
        "n": 38,
        "title": 'Top Spotify Artist Rankings',
        "difficulty": 'Medium',
        "tags": ['Inner Joins', 'Subqueries'],
        "prompt": (
            'Each week Spotify ranks its songs by plays and keeps the top ten. Marketing wants the five '
            'artists who appeared in those weekly top tens most often, with names settling any tie on the '
            'count. Songs are ranked within their own week, never across weeks.'
        ),
        "returns": 'one row per artist in the top 5: artist_name, top10_appearances',
        "watch_out": (
            'Two passes over the data are needed - rank inside each week first, then count appearances by '
            'artist - and ROW_NUMBER needs a unique tiebreaker (song_id) because tied play counts decide who '
            'makes the ten.'
        ),
        "tables": [
            {"name": "q38_artists",
             "cols": 'artist_id INT, artist_name STRING',
             "rows": [
                 (1, 'Aurora'),
                 (2, 'Bolt'),
                 (3, 'Cinder'),
                 (4, 'Drift'),
                 (5, 'Ember'),
                 (6, 'Fable'),
                 (7, 'Halo'),
             ]},
            {"name": "q38_songs",
             "cols": 'song_id INT, artist_id INT, song_name STRING',
             "rows": [
                 (1, 1, 'Neon Sky'),
                 (2, 1, 'Paper Moon'),
                 (3, 2, 'Iron Heart'),
                 (4, 2, 'Golden Hour'),
                 (5, 3, 'Velvet Rain'),
                 (6, 4, 'Echo Chamber'),
                 (7, 6, 'Wildfire'),
                 (8, 4, 'Static Bloom'),
                 (9, 3, 'Midnight Run'),
                 (10, 5, 'Hollow Bones'),
                 (11, 6, 'Slow Burn'),
                 (12, 7, 'Afterglow'),
             ]},
            {"name": "q38_weekly_plays",
             "cols": 'song_id INT, week_start DATE, plays INT',
             "rows": [
                 (1, '2024-03-04', 900),
                 (2, '2024-03-04', 700),
                 (3, '2024-03-04', 850),
                 (4, '2024-03-04', 640),
                 (5, '2024-03-04', 610),
                 (6, '2024-03-04', 500),
                 (7, '2024-03-04', 480),
                 (8, '2024-03-04', 300),
                 (9, '2024-03-04', 250),
                 (10, '2024-03-04', 200),
                 (11, '2024-03-04', 200),
                 (12, '2024-03-11', 400),
             ]},
        ],
        "sql": """
WITH weekly AS (
    SELECT
        w.week_start,
        w.song_id,
        s.artist_id,
        ROW_NUMBER() OVER (
            PARTITION BY w.week_start
            ORDER BY w.plays DESC, w.song_id
        ) AS weekly_rank
    FROM q38_weekly_plays AS w
    JOIN q38_songs AS s
        ON s.song_id = w.song_id
),
appearances AS (
    SELECT
        a.artist_name,
        COUNT(*) AS top10_appearances
    FROM weekly AS wk
    JOIN q38_artists AS a
        ON a.artist_id = wk.artist_id
    WHERE wk.weekly_rank <= 10
    GROUP BY a.artist_name
)
SELECT artist_name, top10_appearances
FROM appearances
ORDER BY top10_appearances DESC, artist_name
LIMIT 5
""",
        "pyspark": """
week_window = Window.partitionBy("week_start").orderBy(F.col("plays").desc(), F.col("song_id"))

weekly = (
    spark.table("q38_weekly_plays").alias("w")
    .join(spark.table("q38_songs").alias("s"), on="song_id", how="inner")
    .withColumn("weekly_rank", F.row_number().over(week_window))
    .filter(F.col("weekly_rank") <= 10)
)

result = (
    weekly
    .join(spark.table("q38_artists").alias("a"), on="artist_id", how="inner")
    .groupBy("artist_name")
    .agg(F.count(F.lit(1)).alias("top10_appearances"))
    .orderBy(F.col("top10_appearances").desc(), F.col("artist_name"))
    .limit(5)
)
display(result)
""",
    },
    {
        "n": 39,
        "title": 'Delivery Order Mismatch Correction',
        "difficulty": 'Medium',
        "tags": ['Window Functions'],
        "prompt": (
            "Warehouse scanners occasionally record a delivery's line items with each pair of adjacent "
            'positions swapped - positions 1 and 2 trade places, so do 3 and 4, and so on. Deliveries flagged '
            'as mislabeled need their line-up repaired before the manifest is printed; everything else is '
            'already correct.'
        ),
        "returns": (
            'one row per flagged delivery line: delivery_id, item_position, item_name (corrected)'
        ),
        "watch_out": (
            'LEAD and LAG over the position window repair the pairs, but only for the flagged rows - '
            'correcting unflagged deliveries corrupts them, and a flagged delivery with an odd number of '
            'lines would leave the last line with no partner.'
        ),
        "tables": [
            {"name": "q39_deliveries",
             "cols": (
                 'delivery_id INT, delivery_date DATE, item_name STRING, item_position INT, mislabeled '
                 'BOOLEAN'
             ),
             "rows": [
                 (101, '2024-04-01', 'Notebook', 1, False),
                 (101, '2024-04-01', 'Pen', 2, False),
                 (101, '2024-04-01', 'Eraser', 3, False),
                 (102, '2024-04-02', 'Screwdriver', 1, True),
                 (102, '2024-04-02', 'Hammer', 2, True),
                 (102, '2024-04-02', 'Pliers', 3, True),
                 (102, '2024-04-02', 'Wrench', 4, True),
                 (103, '2024-04-03', 'Mug', 1, False),
                 (103, '2024-04-03', 'Coaster', 2, False),
                 (104, '2024-04-04', 'Cable', 1, True),
                 (104, '2024-04-04', 'Adapter', 2, True),
             ]},
        ],
        "sql": """
WITH windowed AS (
    SELECT
        delivery_id,
        item_position,
        item_name,
        mislabeled,
        LEAD(item_name) OVER (PARTITION BY delivery_id ORDER BY item_position) AS next_item,
        LAG(item_name) OVER (PARTITION BY delivery_id ORDER BY item_position)  AS prev_item
    FROM q39_deliveries
)
SELECT
    delivery_id,
    item_position,
    CASE
        WHEN item_position % 2 = 1 THEN next_item
        ELSE prev_item
    END AS item_name
FROM windowed
WHERE mislabeled
ORDER BY delivery_id, item_position
""",
        "pyspark": """
position_window = Window.partitionBy("delivery_id").orderBy("item_position")

windowed = (
    spark.table("q39_deliveries")
    .withColumn("next_item", F.lead("item_name").over(position_window))
    .withColumn("prev_item", F.lag("item_name").over(position_window))
)

result = (
    windowed
    .filter(F.col("mislabeled"))
    .select(
        "delivery_id",
        "item_position",
        F.when(F.col("item_position") % 2 == 1, F.col("next_item"))
         .otherwise(F.col("prev_item"))
         .alias("item_name"),
    )
)
display(result)
""",
    },
    {
        "n": 40,
        "title": 'User Signup Activation Rate',
        "difficulty": 'Medium',
        "tags": ['Aggregate Functions', 'Date/Time Functions'],
        "prompt": (
            'Growth reports activation by signup month: a user counts as activated when their first '
            'activation event lands no more than 7 days after the signup date, so day 0 through day 7 '
            'qualifies. Users who never activate still belong in the denominator, and later activation events '
            'do not rescue a slow start.'
        ),
        "returns": (
            'one row per signup month: signup_month, signups, activated_users, activation_rate'
        ),
        "watch_out": (
            'A LEFT JOIN to events keeps never-activated users in the count, and the 7-day test belongs on '
            'the FIRST activation date - counting any activation event anywhere inflates the rate for the '
            'months with a late activator.'
        ),
        "tables": [
            {"name": "q40_users",
             "cols": 'user_id INT, signup_date DATE',
             "rows": [
                 (1, '2024-01-03'),
                 (2, '2024-01-15'),
                 (3, '2024-01-20'),
                 (4, '2024-02-05'),
                 (5, '2024-02-18'),
                 (6, '2024-03-02'),
             ]},
            {"name": "q40_events",
             "cols": 'user_id INT, event_date DATE, event_type STRING',
             "rows": [
                 (1, '2024-01-04', 'login'),
                 (1, '2024-01-05', 'activated'),
                 (2, '2024-01-25', 'activated'),
                 (3, '2024-01-22', 'login'),
                 (4, '2024-02-08', 'activated'),
                 (4, '2024-02-20', 'activated'),
                 (5, '2024-02-25', 'activated'),
                 (6, '2024-03-22', 'activated'),
             ]},
        ],
        "sql": """
WITH activation AS (
    SELECT
        u.user_id,
        u.signup_date,
        MIN(e.event_date) AS first_activated_date
    FROM q40_users AS u
    LEFT JOIN q40_events AS e
        ON e.user_id = u.user_id
       AND e.event_type = 'activated'
    GROUP BY u.user_id, u.signup_date
),
monthly AS (
    SELECT
        DATE_TRUNC('month', signup_date) AS signup_month,
        COUNT(*) AS signups,
        COUNT(*) FILTER (
            WHERE DATEDIFF('day', signup_date, first_activated_date) BETWEEN 0 AND 7
        ) AS activated_users
    FROM activation
    GROUP BY DATE_TRUNC('month', signup_date)
)
SELECT
    signup_month,
    signups,
    activated_users,
    ROUND(100.0 * activated_users / signups, 2) AS activation_rate
FROM monthly
ORDER BY signup_month
""",
        "pyspark": """
activation = (
    spark.table("q40_users").alias("u")
    .join(
        spark.table("q40_events")
        .filter(F.col("event_type") == "activated")
        .alias("e"),
        F.col("e.user_id") == F.col("u.user_id"),
        how="left",
    )
    .groupBy(F.col("u.user_id").alias("user_id"), F.col("u.signup_date").alias("signup_date"))
    .agg(F.min(F.col("e.event_date")).alias("first_activated_date"))
)

monthly = (
    activation
    .withColumn("days_to_activate",
                F.datediff(F.col("first_activated_date"), F.col("signup_date")))
    .groupBy(F.date_trunc("month", F.col("signup_date")).alias("signup_month"))
    .agg(
        F.count(F.lit(1)).alias("signups"),
        F.sum(F.when(F.col("days_to_activate").between(0, 7), 1).otherwise(0))
         .alias("activated_users"),
    )
)

result = monthly.select(
    "signup_month",
    "signups",
    "activated_users",
    F.round(F.lit(100.0) * F.col("activated_users") / F.col("signups"), 2).alias("activation_rate"),
)
display(result)
""",
    },
]
# <<< QUESTIONS


# --------------------------------------------------------------------------- cells
INTRO = """# 40 interview drills - answered twice (DuckDB SQL + PySpark)

Forty questions of the kind that show up in data-engineering and analytics screens: 20 Hard and
20 Medium, covering joins, aggregation, window functions, CTEs, date/time maths, regex and
gaps-and-islands. Every question is answered **twice, on the same data**:

| engine | how | why |
|---|---|---|
| DuckDB | `q('''SELECT ...''')` | the fastest way to iterate on SQL; runs locally, no cluster |
| PySpark | `spark.table("q01_...")` + DataFrame API | the same logic expressed the way it ships in a pipeline |

Each question has its own tiny tables (5-12 rows), created in **both** engines by `table()` in the
setup cell, so the two solutions must return the same rows. That agreement is the check that
matters: if SQL and PySpark disagree, the grain or the join is wrong.

**How to practise:** read the prompt and the *Returns* line, write your own query in the cell
above the solution (or just edit the solution), then run it. The patterns to look for:

| signal in the question | pattern |
|---|---|
| "per X, and also the overall Y" | window function / scalar subquery |
| "consecutive", "streak", "3+ days" | gaps and islands (`date - row_number()`) |
| "latest", "top N per group", "third" | `row_number()` / `rank()` over a window |
| "month over month", "year over year" | `lag()` over an ordered window |
| "every combination", "pairs" | self-join with `a.key < b.key` |
| "duplicate", "same details twice" | `group by` on the descriptive key, then `having count(*) > 1` |

**Spark notes.** The PySpark cells use `spark.table(...)`, which reads the temp views created by
`table()`. Spark comes from Databricks Connect (serverless) with the Databricks CLI profile
`irajput`:

```bash
databricks auth login --profile irajput     # only needed when the token has expired
```

If the token is expired, the setup cell prints a warning, `spark` becomes a no-op shim, and the
PySpark cells print `<spark unavailable>` - the DuckDB half of every question still runs.
"""

SETUP_ENGINES = '''# ---------------------------------------------------------------------------
# Engines: DuckDB (q) + Spark (spark.table)
# ---------------------------------------------------------------------------
import datetime as dt
import pathlib
import sys

ROOT = next(p for p in [pathlib.Path.cwd(), *pathlib.Path.cwd().parents]
            if (p / "src" / "de_helpers.py").exists())
sys.path.insert(0, str(ROOT / "src"))

from de_helpers import connect, display, q, q1          # DuckDB: q("SELECT ...") or q(\"\"\"...\"\"\")
from pyspark.sql import Window, functions as F          # PySpark transformations

_con = connect()   # read-only DuckDB connection to data/warehouse/healthcare.duckdb
SPARK_READY = False


class _NoSpark:
    """Stand-in for `spark` when Databricks Connect is not authenticated.

    Every call returns the same object, so the PySpark cells still run top-to-bottom and print
    `<spark unavailable>` instead of raising. Nothing here is part of a solution - it only keeps
    the notebook executable offline (and in CI). Re-authenticate to get real output.
    """

    def __init__(self, reason: str):
        self.reason = reason

    def __getattr__(self, name):                 # every attribute/method call -> self
        if name.startswith("_"):
            raise AttributeError(name)
        return self

    def __call__(self, *args, **kwargs):
        return self

    def __bool__(self):
        return False

    def __eq__(self, other):
        return self

    def __ne__(self, other):
        return self

    __lt__ = __le__ = __gt__ = __ge__ = __eq__
    __add__ = __radd__ = __sub__ = __rsub__ = __eq__
    __hash__ = object.__hash__

    def __getitem__(self, key):                  # df["col"], df[["a", "b"]], collect()[0]
        return self

    def __setitem__(self, key, value):
        pass

    def __iter__(self):                          # for row in df.collect()
        return iter(())

    def __len__(self):
        return 0

    def __contains__(self, item):
        return False

    def __float__(self):
        return 0.0

    def __int__(self):
        return 0

    def __repr__(self):
        return f"<{self.reason}>"

    def __format__(self, spec):
        return f"<{self.reason}>"


try:
    from de_helpers import get_spark

    spark = get_spark()                       # Databricks Connect, serverless, profile irajput
    SPARK_READY = True
    print(f"Spark ready: Databricks Connect {spark.version}")
except Exception as exc:                      # noqa: BLE001
    reason = f"{type(exc).__name__}: {str(exc).splitlines()[0][:80]}"
    spark = _NoSpark(f"spark unavailable - {reason}")
    print(f"!! Spark unavailable: {reason}")
    print("!! run `databricks auth login --profile irajput` and re-run this cell for the PySpark half")
'''

SETUP_HELPER = '''# ---------------------------------------------------------------------------
# table(): create one toy table in BOTH engines, so the two solutions read the same rows
# ---------------------------------------------------------------------------
_TYPES = {"INT": int, "BIGINT": int, "SMALLINT": int, "DOUBLE": float, "FLOAT": float,
          "BOOLEAN": bool, "STRING": str, "VARCHAR": str, "TEXT": str}


def table(name: str, cols: str, rows: list[tuple]) -> None:
    """Create `name` in DuckDB (temp table) and in Spark (temp view).

    cols: 'property_id INT, city STRING, listed_on DATE, rent DOUBLE'
    rows: list of tuples in column order; ISO strings become DATE / TIMESTAMP values.

        table("demo", "id INT, city STRING, d DATE", [(1, "Austin", "2024-01-05")])
        q("SELECT city FROM demo")              # DuckDB
        spark.table("demo")                     # Spark
    """
    spec = [c.strip().split() for c in cols.split(",")]
    names, types = [s[0] for s in spec], [s[1].upper() for s in spec]

    def cast(value, sql_type):
        if value is None:
            return None
        if sql_type == "DATE":
            return dt.date.fromisoformat(str(value))
        if sql_type.startswith("TIMESTAMP"):
            return dt.datetime.fromisoformat(str(value))
        return _TYPES.get(sql_type, str)(value)

    typed = [[cast(v, t) for v, t in zip(row, types)] for row in rows]
    _con.execute(f"CREATE OR REPLACE TEMP TABLE {name} ({cols})")
    if typed:
        _con.executemany(f"INSERT INTO {name} VALUES ({', '.join('?' * len(names))})", typed)
    spark.createDataFrame(typed, cols).createOrReplaceTempView(name)
'''


def _index_table() -> str:
    """The question index that opens the notebook."""
    lines = ["| # | question | level | techniques |", "|---|---|---|---|"]
    for q in sorted(QUESTIONS, key=lambda x: (x["difficulty"] != "Hard", x["n"])):
        tags = " · ".join(f"`{t}`" for t in q["tags"])
        lines.append(f"| {q['n']} | {q['title']} | {q['difficulty']} | {tags} |")
    return "\n".join(lines)


def _header(q: dict) -> str:
    tags = " · ".join(f"`{t}`" for t in q["tags"])
    out = [f"#### Q{q['n']:02d} · {q['title']}", f"**{q['difficulty']}** · {tags}", "",
           f"**Question.** {q['prompt']}"]
    if q.get("returns"):
        out += ["", f"**Returns.** {q['returns']}"]
    if q.get("watch_out"):
        out += ["", f"**Watch out.** {q['watch_out']}"]
    return "\n".join(out)


def _setup_cell(q: dict) -> str:
    lines = ["# data - one TEMP TABLE in DuckDB and one TEMP VIEW in Spark per table"]
    for t in q["tables"]:
        rows = ",\n".join(f"    {tuple(r)!r}" for r in t["rows"])
        lines.append(f'table("{t["name"]}",\n      "{t["cols"]}",\n      [\n{rows},\n      ])')
    return "\n".join(lines)


def _sql_cell(q: dict) -> str:
    return f"# SQL - DuckDB\ndf = q('''\n{q['sql'].strip()}\n''')\ndisplay(df)"


def _spark_cell(q: dict) -> str:
    return f"# PySpark - spark.table()\n{q['pyspark'].strip()}"


def _cells() -> list[tuple[str, str]]:
    cells: list[tuple[str, str]] = [
        ("md", INTRO),
        ("md", "## The index\n\n" + _index_table()),
        ("md", "## Setup - engines and the `table()` loader\n\n"
               "Run these two cells once, then work through the questions in any order."),
        ("code", SETUP_ENGINES),
        ("code", SETUP_HELPER),
    ]
    for q in sorted(QUESTIONS, key=lambda x: x["n"]):
        cells.append(("md", _header(q)))
        cells.append(("code", _setup_cell(q)))
        cells.append(("code", _sql_cell(q)))
        cells.append(("code", _spark_cell(q)))
    return cells


CELLS = _cells()
