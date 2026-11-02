TITLE = "21 - SQL screening drills: customers, orders, employees"
CELLS = [
    ("md", """Runnable versions of the queries in the **SQL Screening Questions Prep** guide, against the
classic interview tables built by `src/build_practice_data.py`.

**How to practise:** read the question, write your own query in a new cell, *then* run the answer cell.
Every answer cell prints a result from the real data, so you can check yours row for row.

The data is small on purpose and seeded with the traps interviewers probe:

| Table | Rows | Traps |
|---|---|---|
| `customers` | 12 | 2 never order, NULL email, `alice@` vs `ALICE@`, NULL city |
| `orders` | 127 | cancelled/refunded/pending, NULL amount, NULL `customer_id`, orphan `customer_id` 99, same-day repeats |
| `employees` | 15 | CEO with NULL manager, salary ties, one earns more than their manager, NULL dept |
| `departments` | 5 | Legal has no employees |
| `logins` | 36 | 7- and 10-day streaks (one crosses a month), duplicate same-day logins |
| `products` / `purchases` | 5 / 21 | Webcam never bought, customers 1 and 3 bought everything, a duplicate purchase row |

The data covers **2024-03 to 2025-01**, so wherever the guide uses `CURRENT_DATE` this notebook uses
a fixed `DATE '2024-11-30'` as "today". Where the guide says `'shipped'`, the data uses `'completed'`.

DuckDB speaks almost all of PostgreSQL's dialect (`DISTINCT ON`, `FILTER`, `generate_series`, `AGE`,
`date - int`), so the guide's Postgres syntax runs as written. The same tables are in Databricks as
`workflows.practice.*`; see the last section."""),
    ("code", """import duckdb

# in-memory connection with the practice file attached read-only:
# temp tables (used by the DELETE drills) go to memory, the file is never modified
pcon = duckdb.connect()
pcon.sql(f"ATTACH '{(ROOT / 'data' / 'warehouse' / 'practice.duckdb').as_posix()}' AS practice (READ_ONLY)")
pcon.sql("USE practice")

def p(sql: str):
    \"\"\"Run SQL against the practice tables and return a DataFrame.\"\"\"
    return pcon.sql(sql).df()

p("SELECT table_name, estimated_size AS rows FROM duckdb_tables() WHERE database_name = 'practice' ORDER BY 1")"""),

    # ------------------------------------------------------------------ 1. joins
    ("md", """## 1. Joins

### Q: Inner join vs left join?
**Say:** "An inner join keeps only rows that match in both tables. A left join keeps every row from the
left table and fills the right side with NULLs where there is no match."

Watch the counts: the left join is bigger by exactly the customers with no orders."""),
    ("code", """p('''
SELECT 'inner' AS join_type, COUNT(*) AS rows, COUNT(DISTINCT c.customer_id) AS customers
FROM customers c INNER JOIN orders o ON c.customer_id = o.customer_id
UNION ALL
SELECT 'left', COUNT(*), COUNT(DISTINCT c.customer_id)
FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id
''')"""),
    ("md", """### Q: Find customers who never placed an order (anti join)
Expect Jack (10) and Karen (11)."""),
    ("code", """p('''
-- Option 1: LEFT JOIN + IS NULL
SELECT c.customer_id, c.name
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL
ORDER BY 1
''')"""),
    ("code", """p('''
-- Option 2: NOT EXISTS (preferred)
SELECT c.customer_id, c.name
FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)
ORDER BY 1
''')"""),
    ("md", """**Gotcha, live:** `orders` has one guest order with a NULL `customer_id`. `NOT IN` compares each
customer against that NULL, gets *unknown*, and returns **zero rows**."""),
    ("code", """p('''
SELECT 'NOT IN'                    AS method, COUNT(*) AS customers_found
FROM customers WHERE customer_id NOT IN (SELECT customer_id FROM orders)
UNION ALL
SELECT 'NOT IN, NULLs filtered', COUNT(*)
FROM customers WHERE customer_id NOT IN (SELECT customer_id FROM orders WHERE customer_id IS NOT NULL)
UNION ALL
SELECT 'NOT EXISTS', COUNT(*)
FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)
''')"""),
    ("md", """The other direction: **orders with no matching customer**. This catches the orphan (99) *and* the NULL
guest order, which a referential-integrity check should flag."""),
    ("code", """p('''
SELECT o.*
FROM orders o
LEFT JOIN customers c ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL
''')"""),
    ("md", """### Q: What is a semi join?
Left rows that **do** have a match, each returned once however many matches there are. Compare the
row counts: the inner join repeats each customer once per order."""),
    ("code", """p('''
SELECT 'EXISTS (semi join)' AS method, COUNT(*) AS rows
FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)
UNION ALL
SELECT 'INNER JOIN', COUNT(*)
FROM customers c JOIN orders o ON o.customer_id = c.customer_id
''')"""),
    ("md", """### Q: A has 3 rows, B has 4. How many rows can an inner join return?
Anywhere from 0 to 12. Fan-out on a repeated key inflates `SUM` silently: joining orders to purchases
on `customer_id` multiplies every order by the customer's purchase count."""),
    ("code", """p('''
SELECT 'orders only'              AS version, ROUND(SUM(amount), 2) AS revenue FROM orders
UNION ALL
SELECT 'orders JOIN purchases',   ROUND(SUM(o.amount), 2)
FROM orders o JOIN purchases pu ON pu.customer_id = o.customer_id
''')"""),
    ("md", """### Q: Does filtering in ON vs WHERE matter for a left join?
Yes. A right-table filter in `WHERE` drops the NULL rows and turns it into an inner join."""),
    ("code", """p('''
SELECT 'filter in WHERE' AS version, COUNT(DISTINCT c.customer_id) AS customers_kept
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.status = 'completed'
UNION ALL
SELECT 'filter in ON', COUNT(DISTINCT c.customer_id)
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.status = 'completed'
''')"""),
    ("md", """### Q: Cross join to build a full grid
Every customer x every product, then left join actual purchases, so you can see what each customer
has *not* bought."""),
    ("code", """p('''
SELECT c.customer_id, pr.product_name,
       COUNT(pu.product_id) > 0 AS bought
FROM customers c
CROSS JOIN products pr
LEFT JOIN purchases pu ON pu.customer_id = c.customer_id AND pu.product_id = pr.product_id
WHERE c.customer_id IN (1, 2)
GROUP BY ALL
ORDER BY 1, 2
''')"""),
    ("md", """### Q: Rows missing from either of two systems (reconciliation)
The two "systems" here are the customers table and the customer ids seen in orders."""),
    ("code", """p('''
WITH system_a AS (SELECT customer_id AS id FROM customers),
     system_b AS (SELECT DISTINCT customer_id AS id FROM orders)
SELECT a.id AS in_customers, b.id AS in_orders
FROM system_a a
FULL OUTER JOIN system_b b ON a.id = b.id
WHERE a.id IS NULL OR b.id IS NULL
''')"""),
    ("md", """The first row is NULL on **both** sides: the guest order's NULL id never matches anything, so it
shows up as "missing" with no id to report. Reconcile NULL keys separately."""),

    # ------------------------------------------------------------------ 2. duplicates
    ("md", """## 2. Duplicates

### Q: Which values are duplicated?
On the raw email the answer is **nothing**, because `alice@` and `ALICE@` differ by case. Normalise first."""),
    ("code", """p('''
SELECT 'raw email' AS key, email, COUNT(*) AS cnt
FROM customers GROUP BY email HAVING COUNT(*) > 1
UNION ALL
SELECT 'LOWER(TRIM(email))', LOWER(TRIM(email)), COUNT(*)
FROM customers GROUP BY LOWER(TRIM(email)) HAVING COUNT(*) > 1
''')"""),
    ("md", """Full-row duplicates: `purchases` has customer 4 buying product 2 twice."""),
    ("code", """p('''
SELECT customer_id, product_id, COUNT(*) AS cnt
FROM purchases
GROUP BY customer_id, product_id
HAVING COUNT(*) > 1
''')"""),
    ("md", """### Q: Show all the duplicate rows, including the original"""),
    ("code", """p('''
SELECT *
FROM (
  SELECT *, COUNT(*) OVER (PARTITION BY LOWER(TRIM(email))) AS cnt
  FROM customers
) t
WHERE cnt > 1
ORDER BY email
''')"""),
    ("md", """### Q: Show only the extra copies (keep one of each)
`ORDER BY created_at DESC` keeps the newest as rn = 1, so the older Alice row is the "extra" one."""),
    ("code", """p('''
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(email)) ORDER BY created_at DESC) AS rn
  FROM customers
  WHERE email IS NOT NULL
) t
WHERE rn > 1
''')"""),
    ("md", """**Gotcha:** without `WHERE email IS NOT NULL`, `PARTITION BY` groups all NULL emails together and
would call them duplicates of each other, even though `NULL = NULL` is not true in a join.

### Q: Delete duplicates
Run on a temp copy so the practice file stays intact. With a primary key:"""),
    ("code", """pcon.sql("CREATE OR REPLACE TEMP TABLE customers_copy AS SELECT * FROM customers")
pcon.sql('''
DELETE FROM customers_copy
WHERE customer_id IN (
  SELECT customer_id FROM (
    SELECT customer_id,
           ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(email)) ORDER BY customer_id) AS rn
    FROM customers_copy
    WHERE email IS NOT NULL
  ) t
  WHERE rn > 1
)
''')
p("SELECT COUNT(*) AS before, (SELECT COUNT(*) FROM customers_copy) AS after FROM customers")"""),
    ("md", """Without any unique column: PostgreSQL uses the hidden `ctid`; DuckDB's equivalent is `rowid`.
`purchases` has no key, which makes it the right table for this."""),
    ("code", """pcon.sql("CREATE OR REPLACE TEMP TABLE purchases_copy AS SELECT * FROM purchases")
pcon.sql('''
DELETE FROM purchases_copy a
USING purchases_copy b
WHERE a.rowid > b.rowid
  AND a.customer_id = b.customer_id
  AND a.product_id  = b.product_id
''')
p("SELECT COUNT(*) AS before, (SELECT COUNT(*) FROM purchases_copy) AS after FROM purchases")"""),
    ("md", """### Q: Return a deduplicated result without deleting"""),
    ("code", """p('''
SELECT DISTINCT ON (LOWER(TRIM(email))) *     -- PostgreSQL and DuckDB
FROM customers
WHERE email IS NOT NULL
ORDER BY LOWER(TRIM(email)), created_at DESC
''')"""),

    # ------------------------------------------------------------------ 3. windows
    ("md", """## 3. Window functions

### Q: ROW_NUMBER vs RANK vs DENSE_RANK?
Engineering has Sara and Tom tied at 150,000; Sales has Wendy and Xavier tied at 95,000."""),
    ("code", """p('''
SELECT dept_id, name, salary,
       ROW_NUMBER() OVER w AS row_number,
       RANK()       OVER w AS rank,
       DENSE_RANK() OVER w AS dense_rank
FROM employees
WHERE dept_id IN (10, 20)
WINDOW w AS (PARTITION BY dept_id ORDER BY salary DESC)
ORDER BY dept_id, salary DESC
''')"""),
    ("md", """### Q: Top 3 earners in each department
With `DENSE_RANK`, Engineering returns 4 people because of the tie. With `ROW_NUMBER` it is exactly 3,
and which tied person is dropped is arbitrary unless you add a tiebreaker."""),
    ("code", """p('''
SELECT *
FROM (
  SELECT e.*, DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rnk
  FROM employees e
) t
WHERE rnk <= 3
ORDER BY dept_id, rnk
''')"""),
    ("md", """### Q: Nth highest salary (N = 3)"""),
    ("code", """p('''
SELECT DISTINCT salary
FROM (
  SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
  FROM employees
) t
WHERE rnk = 3
''')"""),
    ("md", """### Q: Running total of order amounts by date
**Gotcha:** with `ORDER BY order_date` the default frame is `RANGE`, so orders on the same date all get
the same running total (look at 2024-06-15). Add a tiebreaker or `ROWS` for a strictly increasing total."""),
    ("code", """p('''
SELECT order_id, order_date, amount,
       SUM(amount) OVER (ORDER BY order_date)                       AS running_range,
       SUM(amount) OVER (ORDER BY order_date, order_id
                         ROWS UNBOUNDED PRECEDING)                   AS running_rows
FROM orders
WHERE order_date BETWEEN '2024-06-12' AND '2024-06-18'
ORDER BY order_date, order_id
''')"""),
    ("md", """### Q: Month-over-month change (LAG / LEAD)"""),
    ("code", """p('''
WITH monthly AS (
  SELECT DATE_TRUNC('month', order_date) AS month, SUM(amount) AS revenue
  FROM orders
  GROUP BY 1
)
SELECT month, revenue,
       revenue - LAG(revenue)  OVER (ORDER BY month) AS change_vs_prev,
       LEAD(revenue)           OVER (ORDER BY month) AS next_revenue
FROM monthly
ORDER BY month
''')"""),
    ("md", """Note the jump from 2024-11 to 2025-01: December has no orders, so `LAG` compares January to
**November**. That is the missing-month trap from section 14.

### Q: 3-day moving average"""),
    ("code", """p('''
WITH daily AS (SELECT order_date, SUM(amount) AS revenue FROM orders GROUP BY 1)
SELECT order_date, revenue,
       ROUND(AVG(revenue) OVER (ORDER BY order_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ma_3_rows,
       ROUND(AVG(revenue) OVER (ORDER BY order_date
             RANGE BETWEEN INTERVAL 2 DAY PRECEDING AND CURRENT ROW), 2)                      AS ma_3_days
FROM daily
ORDER BY order_date
LIMIT 10
''')"""),
    ("md", """Orders land roughly every 3 days, so "2 preceding rows" reaches back about a week, while the
`RANGE` version usually sees a single day. That's the ROWS-vs-days gotcha the guide calls out."""),

    # ------------------------------------------------------------------ 4. aggregation
    ("md", """## 4. Aggregation and filtering

### Q: WHERE vs HAVING?"""),
    ("code", """p('''
SELECT dept_id, ROUND(AVG(salary)) AS avg_sal, COUNT(*) AS headcount
FROM employees
WHERE salary > 0              -- row filter
GROUP BY dept_id
HAVING AVG(salary) > 100000   -- group filter
ORDER BY dept_id
''')"""),
    ("md", """### Q: COUNT(*) vs COUNT(column) vs COUNT(DISTINCT column)?"""),
    ("code", """p('''
SELECT COUNT(*)                        AS count_star,
       COUNT(email)                    AS count_email,
       COUNT(DISTINCT email)           AS distinct_email,
       COUNT(DISTINCT LOWER(email))    AS distinct_email_ci
FROM customers
''')"""),
    ("md", """### Q: How do aggregates handle NULLs?
One order has a NULL amount. `AVG` skips it; `COALESCE` counts it as zero."""),
    ("code", """p('''
SELECT ROUND(AVG(amount), 4)              AS avg_ignoring_null,
       ROUND(AVG(COALESCE(amount, 0)), 4) AS avg_null_as_zero,
       COUNT(amount)                      AS rows_with_amount,
       COUNT(*)                           AS all_rows
FROM orders
''')"""),
    ("md", """### Q: Departments with more than 3 employees
(The guide uses > 5; this dataset is small.) Drop the `HAVING` and you'll see a NULL `dept_id`
group: `GROUP BY` puts Ella, who has no department, in a group of her own."""),
    ("code", """p('''
SELECT dept_id, COUNT(*) AS headcount
FROM employees
GROUP BY dept_id
HAVING COUNT(*) > 3
''')"""),
    ("md", """### Q: Conditional aggregation (pivot with CASE)"""),
    ("code", """p('''
SELECT customer_id,
       SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_amt,
       SUM(CASE WHEN status = 'cancelled' THEN amount ELSE 0 END) AS cancelled_amt,
       SUM(amount) FILTER (WHERE status = 'refunded')             AS refunded_amt
FROM orders
WHERE customer_id IS NOT NULL
GROUP BY customer_id
ORDER BY customer_id
''')"""),
    ("md", """### Q: Departments with no employees, showing headcount 0
`COUNT(*)` reports Legal as 1, because the left join produces one all-NULL employee row for it."""),
    ("code", """p('''
SELECT d.dept_name,
       COUNT(e.emp_id) AS headcount,
       COUNT(*)        AS wrong_count_star
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.dept_name
ORDER BY headcount
''')"""),

    # ------------------------------------------------------------------ 5. puzzles
    ("md", """## 5. Classic puzzles

### Q: Second highest salary"""),
    ("code", """p('''
SELECT 'subquery' AS method, MAX(salary) AS second_highest
FROM employees WHERE salary < (SELECT MAX(salary) FROM employees)
UNION ALL
SELECT 'offset', (SELECT DISTINCT salary FROM employees ORDER BY salary DESC LIMIT 1 OFFSET 1)
''')"""),
    ("md", """### Q: Employees earning more than their manager (self join)"""),
    ("code", """p('''
SELECT e.name AS employee, e.salary, m.name AS manager, m.salary AS manager_salary
FROM employees e
JOIN employees m ON e.manager_id = m.emp_id
WHERE e.salary > m.salary
''')"""),
    ("md", """Follow-up they often ask: list **every** employee with their manager, including the CEO. That needs a
`LEFT JOIN`, or Maria disappears."""),
    ("code", """p('''
SELECT e.name AS employee, COALESCE(m.name, '(no manager)') AS manager
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.emp_id
ORDER BY e.emp_id
''')"""),
    ("md", """### Q: Highest paid employee in each department
Row-value `IN`. Ties are returned together; Ella (NULL dept) drops out because she has no department."""),
    ("code", """p('''
SELECT d.dept_name, e.name, e.salary
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id
WHERE (e.dept_id, e.salary) IN (SELECT dept_id, MAX(salary) FROM employees GROUP BY dept_id)
ORDER BY d.dept_name
''')"""),
    ("md", """### Q: Employees earning above their department average"""),
    ("code", """p('''
SELECT *
FROM (
  SELECT e.*, ROUND(AVG(salary) OVER (PARTITION BY dept_id)) AS dept_avg
  FROM employees e
) t
WHERE salary > dept_avg
ORDER BY dept_id
''')"""),
    ("md", """### Q: Users who logged in 3 or more consecutive days (gaps and islands)
Subtract a row number from each date: consecutive dates give the same `grp_key`. The `DISTINCT` step
matters, because 101 and 105 each have a duplicate login that would otherwise break the streak."""),
    ("code", """p('''
WITH d AS (
  SELECT DISTINCT user_id, login_date FROM logins
),
grp AS (
  SELECT user_id, login_date,
         login_date - (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date))::int AS grp_key
  FROM d
)
SELECT user_id, MIN(login_date) AS streak_start, MAX(login_date) AS streak_end, COUNT(*) AS streak_days
FROM grp
GROUP BY user_id, grp_key
HAVING COUNT(*) >= 3
ORDER BY user_id, streak_start
''')"""),
    ("md", """Try removing the `DISTINCT` and see which streaks break or shrink.

Simpler alternative for exactly 3 days: `LAG(login_date, 2)` equals `login_date - 2`."""),
    ("code", """p('''
WITH d AS (SELECT DISTINCT user_id, login_date FROM logins)
SELECT DISTINCT user_id
FROM (
  SELECT user_id, login_date,
         LAG(login_date, 2) OVER (PARTITION BY user_id ORDER BY login_date) AS two_back
  FROM d
) t
WHERE two_back = login_date - 2
ORDER BY user_id
''')"""),
    ("md", """### Q: Customers who bought every product (relational division)
`COUNT(DISTINCT ...)` matters: customer 4's duplicate row would otherwise count twice."""),
    ("code", """p('''
SELECT customer_id
FROM purchases
GROUP BY customer_id
HAVING COUNT(DISTINCT product_id) = (SELECT COUNT(*) FROM products)
''')"""),
    ("md", """### Q: First order of each customer
Customer 1 has two orders on 2024-06-15; add `order_id` as a tiebreaker so the result is deterministic."""),
    ("code", """p('''
SELECT *
FROM (
  SELECT o.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date, order_id) AS rn
  FROM orders o
  WHERE customer_id IS NOT NULL
) t
WHERE rn = 1
ORDER BY customer_id
''')"""),
    ("md", """### Q: Percentage of orders that were cancelled
`100` vs `100.0`: DuckDB's `/` is float division, so it doesn't bite here. `//` shows what integer
division would return in PostgreSQL or SQL Server."""),
    ("code", """p('''
SELECT ROUND(100.0 * SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_cancelled,
       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) // COUNT(*) * 100           AS integer_division
FROM orders
''')"""),
    ("md", """### Q: Customers who ordered in June but not in July
(The guide uses January/February 2026; the data covers 2024.)"""),
    ("code", """p('''
SELECT DISTINCT customer_id FROM orders
WHERE order_date >= '2024-06-01' AND order_date < '2024-07-01'
EXCEPT
SELECT DISTINCT customer_id FROM orders
WHERE order_date >= '2024-07-01' AND order_date < '2024-08-01'
''')"""),

    # ------------------------------------------------------------------ 6. order of execution
    ("md", """## 6. Logical order of execution, proved
A `SELECT` alias can't be used in `WHERE` in PostgreSQL. **DuckDB allows it** as an extension, so
don't rely on DuckDB to catch this habit: Postgres, SQL Server and Snowflake behave differently.
`ORDER BY` an alias works everywhere."""),
    ("code", """p('''
SELECT order_id, amount * 1.1 AS amount_with_tax
FROM orders
WHERE amount IS NOT NULL
ORDER BY amount_with_tax DESC
LIMIT 3
''')"""),

    # ------------------------------------------------------------------ 13. dates
    ("md", """## 13. Date functions
"Today" is fixed at `DATE '2024-11-30'` so the date drills return rows.

### Q: Orders in the last 30 days"""),
    ("code", """p('''
SELECT *
FROM orders
WHERE order_date >= DATE '2024-11-30' - INTERVAL '30 days'
  AND order_date <= DATE '2024-11-30'
ORDER BY order_date
''')"""),
    ("md", """### Q: Monthly revenue"""),
    ("code", """p('''
SELECT DATE_TRUNC('month', order_date) AS month,
       ROUND(SUM(amount), 2)           AS revenue,
       COUNT(DISTINCT customer_id)     AS customers
FROM orders
GROUP BY 1
ORDER BY 1
''')"""),
    ("md", """### Q: Month-to-date and year-to-date revenue"""),
    ("code", """p('''
SELECT
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', DATE '2024-11-30') THEN amount ELSE 0 END), 2) AS mtd_revenue,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('year',  DATE '2024-11-30') THEN amount ELSE 0 END), 2) AS ytd_revenue
FROM orders
WHERE order_date <= DATE '2024-11-30'
''')"""),
    ("md", """### Q: Days between a customer's first and second order"""),
    ("code", """p('''
WITH ranked AS (
  SELECT customer_id, order_date,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date, order_id) AS rn
  FROM orders
  WHERE customer_id IS NOT NULL
)
SELECT customer_id,
       MAX(CASE WHEN rn = 2 THEN order_date END) - MAX(CASE WHEN rn = 1 THEN order_date END) AS days_to_second_order
FROM ranked
WHERE rn <= 2
GROUP BY customer_id
HAVING COUNT(*) = 2
ORDER BY customer_id
''')"""),
    ("md", """If a customer's first two orders share a date the answer is 0 days: decide whether that counts as
a "second order" and say so.

### Q: Weekday vs weekend sales"""),
    ("code", """p('''
SELECT CASE WHEN EXTRACT(DOW FROM order_date) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
       COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue
FROM orders
GROUP BY 1
''')"""),
    ("md", """### Q: Show every day, including days with zero sales"""),
    ("code", """p('''
SELECT cal.day::date AS day, COALESCE(SUM(o.amount), 0) AS revenue
FROM generate_series(DATE '2024-10-01', DATE '2024-10-14', INTERVAL '1 day') AS cal(day)
LEFT JOIN orders o ON o.order_date = cal.day::date
GROUP BY cal.day
ORDER BY cal.day
''')"""),
    ("md", """### Q: Customer tenure in days or months"""),
    ("code", """p('''
SELECT customer_id, created_at::date AS created,
       DATE '2024-11-30' - created_at::date AS tenure_days,
       DATE_PART('year',  AGE(DATE '2024-11-30', created_at)) * 12
         + DATE_PART('month', AGE(DATE '2024-11-30', created_at)) AS tenure_months
FROM customers
ORDER BY customer_id
''')"""),
    ("md", """**Gotcha:** `WHERE YEAR(order_date) = 2024` returns the same rows as a range, but it blocks index
use and partition pruning. Prefer `order_date >= '2024-01-01' AND order_date < '2025-01-01'`."""),

    # ------------------------------------------------------------------ 14. business metrics
    ("md", """## 14. Business metrics

### Q: Month-over-month revenue growth"""),
    ("code", """p('''
WITH monthly AS (
  SELECT DATE_TRUNC('month', order_date) AS month, SUM(amount) AS revenue
  FROM orders
  GROUP BY 1
)
SELECT month, ROUND(revenue, 2) AS revenue,
       ROUND(LAG(revenue) OVER (ORDER BY month), 2) AS prev_revenue,
       ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY month))
             / NULLIF(LAG(revenue) OVER (ORDER BY month), 0), 2) AS mom_growth_pct
FROM monthly
ORDER BY month
''')"""),
    ("md", """2025-01 is compared with 2024-11, because December is missing. A calendar spine fixes it:
December appears with 0 revenue, and January's growth is then NULL (0 → x is undefined), not a
misleading figure."""),
    ("code", """p('''
WITH months AS (
  SELECT m::date AS month FROM generate_series(DATE '2024-03-01', DATE '2025-01-01', INTERVAL '1 month') AS t(m)
),
monthly AS (
  SELECT m.month, COALESCE(SUM(o.amount), 0) AS revenue
  FROM months m
  LEFT JOIN orders o ON DATE_TRUNC('month', o.order_date) = m.month
  GROUP BY m.month
)
SELECT month, ROUND(revenue, 2) AS revenue,
       ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY month))
             / NULLIF(LAG(revenue) OVER (ORDER BY month), 0), 2) AS mom_growth_pct
FROM monthly
ORDER BY month
''')"""),
    ("md", """### Q: 7-day moving average of daily revenue
Aggregate to one row per day first, then window."""),
    ("code", """p('''
WITH daily AS (SELECT order_date, SUM(amount) AS revenue FROM orders GROUP BY order_date)
SELECT order_date, ROUND(revenue, 2) AS revenue,
       ROUND(AVG(revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ma_7_rows,
       ROUND(AVG(revenue) OVER (ORDER BY order_date
             RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW), 2)                      AS ma_7_days
FROM daily
ORDER BY order_date
LIMIT 10
''')"""),
    ("md", """### Q: Running total and share of total"""),
    ("code", """p('''
SELECT order_id, order_date, amount,
       SUM(amount) OVER (ORDER BY order_date, order_id)                                   AS running_total,
       SUM(amount) OVER (PARTITION BY DATE_TRUNC('month', order_date) ORDER BY order_date, order_id) AS month_running,
       ROUND(100.0 * amount / SUM(amount) OVER (), 2)                                       AS pct_of_total
FROM orders
ORDER BY order_date, order_id
LIMIT 10
''')"""),
    ("md", """### Q: Monthly churn rate
A customer churned in month M if they ordered in M-1 but not in M. "Today" is 2024-11-30, so the
incomplete-month filter excludes nothing here; it would in production."""),
    ("code", """p('''
WITH active AS (
  SELECT DISTINCT customer_id, DATE_TRUNC('month', order_date) AS month
  FROM orders
  WHERE customer_id IS NOT NULL
)
SELECT prev.month + INTERVAL '1 month' AS month,
       COUNT(*) AS active_last_month,
       COUNT(*) FILTER (WHERE cur.customer_id IS NULL) AS churned,
       ROUND(100.0 * COUNT(*) FILTER (WHERE cur.customer_id IS NULL) / COUNT(*), 2) AS churn_rate_pct
FROM active prev
LEFT JOIN active cur
  ON cur.customer_id = prev.customer_id
 AND cur.month = prev.month + INTERVAL '1 month'
WHERE prev.month + INTERVAL '1 month' <= DATE_TRUNC('month', DATE '2024-11-30')
GROUP BY 1
ORDER BY 1
''')"""),
    ("md", """### Q: Cohort retention
Group customers by first-order month, count how many are active N months later."""),
    ("code", """p('''
WITH first_order AS (
  SELECT customer_id, DATE_TRUNC('month', MIN(order_date)) AS cohort_month
  FROM orders
  WHERE customer_id IS NOT NULL
  GROUP BY customer_id
),
activity AS (
  SELECT DISTINCT customer_id, DATE_TRUNC('month', order_date) AS month
  FROM orders
  WHERE customer_id IS NOT NULL
),
cohort_counts AS (
  SELECT f.cohort_month,
         (EXTRACT(YEAR FROM a.month) - EXTRACT(YEAR FROM f.cohort_month)) * 12
           + (EXTRACT(MONTH FROM a.month) - EXTRACT(MONTH FROM f.cohort_month)) AS month_number,
         COUNT(*) AS active_customers
  FROM first_order f
  JOIN activity a ON a.customer_id = f.customer_id
  GROUP BY 1, 2
)
SELECT cohort_month, month_number, active_customers,
       ROUND(100.0 * active_customers
             / FIRST_VALUE(active_customers) OVER (PARTITION BY cohort_month ORDER BY month_number), 2) AS retention_pct
FROM cohort_counts
ORDER BY cohort_month, month_number
''')"""),
    ("md", """### More metrics from the table: AOV, ARPU, repeat purchase rate"""),
    ("code", """p('''
SELECT ROUND(SUM(amount) / COUNT(*), 2)                         AS aov,
       ROUND(SUM(amount) / COUNT(DISTINCT customer_id), 2)      AS arpu_active,
       ROUND(100.0 * (SELECT COUNT(*) FROM (SELECT customer_id FROM orders WHERE customer_id IS NOT NULL
                                            GROUP BY 1 HAVING COUNT(*) >= 2))
             / (SELECT COUNT(*) FROM customers), 2)             AS repeat_purchase_rate_pct
FROM orders
WHERE status = 'completed'
''')"""),
    ("md", """**Say the definition:** AOV here counts only completed orders, and `SUM(amount)/COUNT(*)` silently
treats the NULL-amount order as an order with no value (sum skips it, count includes it). State
which one you mean.

### Daily and monthly active users from `logins`"""),
    ("code", """p('''
WITH dau AS (
  SELECT login_date, COUNT(DISTINCT user_id) AS dau FROM logins GROUP BY 1
),
mau AS (
  SELECT DATE_TRUNC('month', login_date) AS month, COUNT(DISTINCT user_id) AS mau FROM logins GROUP BY 1
)
SELECT m.month, m.mau, ROUND(AVG(d.dau), 2) AS avg_dau, ROUND(AVG(d.dau) / m.mau, 3) AS stickiness
FROM mau m
JOIN dau d ON DATE_TRUNC('month', d.login_date) = m.month
GROUP BY m.month, m.mau
ORDER BY m.month
''')"""),

    # ------------------------------------------------------------------ spark
    ("md", """## Same tables in Databricks
`build_practice_data.py --databricks` loaded them as `workflows.practice.*`. The PySpark answers from
section 8 of the guide, run against Unity Catalog (skipped if there's no Databricks session)."""),
    ("code", """if spark is not None:
    customers = spark.table("workflows.practice.customers")
    orders = spark.table("workflows.practice.orders")

    # anti join
    customers.join(orders, "customer_id", "left_anti").select("customer_id", "name").show()

    # dedupe keeping the latest per normalised email
    w = Window.partitionBy(F.lower(F.trim("email"))).orderBy(F.col("created_at").desc())
    (customers.filter(F.col("email").isNotNull())
              .withColumn("rn", F.row_number().over(w))
              .filter("rn = 1").drop("rn")
              .select("customer_id", "email", "created_at").show())

    # count nulls in every column
    orders.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in orders.columns]).show()
else:
    print("no Spark session - run: databricks auth login --profile irajput")"""),
    ("code", """if spark is not None:
    spark.sql('''
        SELECT c.customer_id, c.name
        FROM workflows.practice.customers c
        LEFT ANTI JOIN workflows.practice.orders o ON c.customer_id = o.customer_id
    ''').show()"""),
]
