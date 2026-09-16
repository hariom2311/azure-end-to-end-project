# Day 1 — Practice Exercises: Data Pipelines & ETL/ELT

> Use the sample data files in the `data/` folder alongside these exercises.
> Each exercise is self-contained and can be done with Python + pandas or plain SQL.

---

## Exercise 1: Map a Pipeline as a DAG

**Concept:** What is a Data Pipeline

**Scenario:**  
An e-commerce company receives order data from three sources:
1. A MySQL database (`orders` table) — transactional orders
2. A CSV file dropped daily by the logistics team (`shipments.csv`) — shipment tracking
3. A JSON file from the payment gateway (`payments.json`) — payment confirmations

The analytics team needs a single `orders_summary` table that joins all three.

**Tasks:**

1. Draw the pipeline as a DAG (boxes and arrows — pen and paper is fine). Label each node with what it does (Extract, Join, Validate, Load, etc.).

2. Identify the following in your DAG:
   - All **sources** (upstream nodes with no inputs)
   - All **sinks** (downstream nodes with no outputs)
   - Any steps where **fan-in** occurs (multiple inputs merge)

3. Answer these questions:
   - If `payments.json` is delayed by 2 hours, which downstream nodes are blocked?
   - If you want to add a `returns` source later, which part of the DAG changes?

**Sample data:** `data/orders.csv`, `data/shipments.csv`, `data/payments.csv`

---

## Exercise 2: Identify ETL vs. ELT

**Concept:** ETL vs. ELT

**Scenario:**  
Read the two pipeline descriptions below and classify each as ETL or ELT. Justify your answer.

**Pipeline A:**  
> An on-premise Python script reads from an Oracle HR database, calculates each employee's annual bonus using company policy rules, masks the raw salary field, and writes only the final bonus amounts to a reporting database. The raw salary data never leaves the script.

**Pipeline B:**  
> An ADF pipeline copies the raw HR data (including salary, SSN, all columns) into a Snowflake `raw.hr_employees` table. A dbt model then runs inside Snowflake to compute bonus amounts and write to `gold.employee_bonuses`. The raw table is retained for 90 days.

**Tasks:**

1. Classify each as ETL or ELT.
2. For Pipeline A, state one scenario where this design is the right choice over ELT.
3. For Pipeline B, state one risk with storing raw data containing SSN in the warehouse.
4. Rewrite Pipeline B's approach to handle PII correctly while still using ELT.

---

## Exercise 3: Fix a Non-Idempotent Pipeline

**Concept:** Idempotency and Incremental vs. Full Loads

**Scenario:**  
The following Python script runs every day at midnight. It has been running correctly for a week, but today the job failed at 11:45 PM and the team re-ran it at 12:05 AM. The next morning, the analytics team found double-counted revenue.

```python
import pandas as pd
import sqlite3

conn = sqlite3.connect("warehouse.db")

# Load today's new orders from source
new_orders = pd.read_csv("data/orders.csv")
today_orders = new_orders[new_orders["order_date"] == "2024-01-15"]

# Append to the silver table
today_orders.to_sql("orders_silver", conn, if_exists="append", index=False)

# Save watermark
with open("watermark.txt", "w") as f:
    f.write("2024-01-15")

conn.close()
```

**Tasks:**

1. Explain exactly why re-running this script causes duplicate rows.

2. Rewrite the script to make it **idempotent**. The fix must ensure that re-running it any number of times for the same date produces exactly the same result. Use the data in `data/orders.csv`.

3. The team now wants to switch from a full daily load to an **incremental load** by order ID (orders are never updated, only inserted). Rewrite the script to:
   - On the first run: load all orders
   - On subsequent runs: load only orders with `order_id` greater than the last processed `order_id`
   - Save the watermark as the highest `order_id` seen

4. What breaks if an order is inserted with an `order_id` lower than the current watermark? How would you defend against this?

**Sample data:** `data/orders.csv`

---

## Exercise 4: Trace Data Lineage

**Concept:** Data Lineage and Dependency Graphs

**Scenario:**  
A company has the following pipeline (described in plain English). Build the lineage graph and answer the impact analysis questions.

**Pipeline description:**
- `raw.orders` is loaded from the orders API every hour
- `raw.customers` is loaded from the CRM database every night
- `silver.orders` is built from `raw.orders` (cleans nulls, casts types, deduplicates)
- `silver.customers` is built from `raw.customers` (standardises country codes, masks email domain)
- `gold.customer_orders` joins `silver.orders` and `silver.customers` on `customer_id`
- `gold.revenue_by_region` aggregates `gold.customer_orders` grouping by `customer.country`
- The `Revenue Dashboard` reads from `gold.revenue_by_region`
- The `Customer Report` reads from `gold.customer_orders`

**Tasks:**

1. Draw the full lineage graph (DAG). Each dataset and report is a node. Arrows show data flow direction.

2. **Impact analysis — upstream change:** The orders API team says they are renaming the `customer_id` column to `cust_id` in `raw.orders` next week. List every node that will be affected.

3. **Impact analysis — deletion request:** A GDPR deletion request arrives for `customer_id = 4821`. List every table and report that may contain data for this customer, in the order you should process deletions.

4. **Debugging exercise:** The `Revenue Dashboard` is showing incorrect numbers for Germany. Using the lineage graph, write a step-by-step investigation plan (which tables to check, in which order, and what to look for at each step).

---

## Exercise 5: Design a Retry Strategy

**Concept:** Pipeline Failure Modes and Retry Strategies

**Scenario:**  
You are building a pipeline that:
1. Calls an external payments API to fetch the last 24 hours of transactions (rate limit: 100 requests/minute)
2. Transforms the transactions (currency conversion, fee calculation)
3. Writes results to a `payments_silver` table

The API occasionally returns 429 (rate limited) and 503 (service unavailable) errors. The transformation step can fail if a transaction contains an unrecognised currency code.

**Tasks:**

1. What delivery guarantee does a naive "call API → transform → INSERT" pipeline offer? Explain why.

2. Design a retry strategy for the API call step. Write pseudocode that implements:
   - Exponential backoff with jitter for 429/503 errors
   - A maximum of 5 attempts before giving up
   - On final failure: log the error and move to the next batch (do not crash the whole pipeline)

3. What should happen to a transaction with an unrecognised currency code? Design a dead-letter queue approach:
   - Where should rejected records go?
   - What information should be stored alongside the rejected record?
   - How should the team be alerted?

4. The pipeline writes results with `INSERT INTO payments_silver SELECT ...`. Is this idempotent? If not, rewrite the write step using the **write-audit-publish** pattern:
   - Write to `payments_silver_staging`
   - Validate: total row count > 0, no nulls in `transaction_id`, sum of amounts is positive
   - If valid: atomically replace `payments_silver` with `payments_silver_staging`
   - If invalid: alert and abort (leave `payments_silver` unchanged)

Write the validation + swap logic as SQL statements.

---

## Bonus Challenge: End-to-End Mini Pipeline

**All 5 concepts combined**

Using `data/orders.csv`, `data/shipments.csv`, and `data/payments.csv`:

Build a Python script that:

1. **Extracts** data from all three CSV files
2. **Validates** that every `order_id` in `shipments.csv` exists in `orders.csv` (log any orphans to a dead-letter file)
3. **Joins** the three datasets on `order_id`
4. **Transforms**: add a `total_with_shipping` column (`order_amount + shipping_cost`), standardise `status` to lowercase
5. **Loads** the result into a SQLite `orders_summary` table using an idempotent upsert (MERGE by `order_id`)
6. **Saves a watermark** of the highest `order_id` processed
7. **Is re-runnable** — running it twice produces the same result

Print a summary at the end:
```
Orders processed: 50
Shipments matched: 48
Payments matched: 47
Dead-letter records: 2
Watermark saved: order_id = 1050
```

---

## Exercise 6: Schema Evolution — Spot the Break

**Concept:** Data Schemas & Schema Evolution

**Scenario:**  
Your `orders_silver` table was created with this schema:

```sql
CREATE TABLE orders_silver (
    order_id     INTEGER NOT NULL,
    customer_id  VARCHAR(10),
    order_date   DATE,
    order_amount INTEGER,
    status       VARCHAR(20),
    currency     VARCHAR(3)
);
```

The source team sends the following changelog for next Monday's deployment:

```
Change A: Add column `discount_code VARCHAR(50)` — nullable, default NULL
Change B: Remove column `currency` — it will be derived from customer's account
Change C: Rename `customer_id` to `cust_id`
Change D: Change `order_amount` from INTEGER to FLOAT (to support cents)
Change E: Add column `product_category VARCHAR(50)` — NOT NULL, no default
```

**Tasks:**

1. Classify each change (A–E) as **backward compatible**, **forward compatible**, **fully compatible**, or **breaking**. Explain each.

2. Which changes can you apply to `orders_silver` safely without modifying any downstream pipeline? Which ones require downstream pipelines to be updated first?

3. Write the SQL `ALTER TABLE` statements for the safe changes only.

4. For Change B (remove `currency`): design a migration plan that allows the source to remove the column without breaking downstream consumers on the same day. What is the minimum number of deployment steps?

5. Your pipeline uses `SELECT * FROM orders_raw` to load into silver. Which of the five changes would silently break the pipeline without throwing an error? Why is `SELECT *` dangerous in pipelines?

**Sample data:** `data/orders.csv` — note the `currency` and `product_category` columns already present.

---

## Exercise 7: Batch vs. Streaming — Pick the Right Model

**Concept:** Batch vs. Micro-batch vs. Streaming

**Scenario:**  
For each use case below, decide whether to use **batch**, **micro-batch**, or **streaming**. Justify your choice with latency requirement, complexity, and cost reasoning.

| # | Use Case |
|---|---|
| A | Daily sales report emailed to executives every morning at 7 AM |
| B | Fraud detection: flag a transaction as suspicious within 500ms of it occurring |
| C | A recommendation engine that refreshes product suggestions every 15 minutes |
| D | End-of-month invoice generation for 2 million customers |
| E | Real-time dashboard showing orders placed in the last 60 seconds |
| F | Weekly ML model retraining on the last 90 days of clickstream data |

**Tasks:**

1. For each use case (A–F), state your choice (batch / micro-batch / streaming) and a one-sentence justification.

2. For use case C (15-minute recommendations), a colleague suggests using true streaming (event-by-event). What are the trade-offs of streaming vs. micro-batch for this specific case? Which would you recommend?

3. Use case E requires a "last 60 seconds" window. Sketch the approach:
   - What is the window type (tumbling, sliding, session)?
   - What happens to an order event that arrives 90 seconds late due to a network delay?
   - How would you handle this late arrival?

4. Use case B (fraud detection) must process 10,000 transactions per second with sub-500ms latency. What processing model is the only viable option? What makes this hard to implement compared to batch?

---

## Exercise 8: Partitioning — Fix a Slow Query

**Concept:** Data Partitioning & Bucketing

**Scenario:**  
A `orders` table in your data lake holds 3 years of data (≈ 500 million rows, ≈ 2 TB). It is stored as Parquet files in a single flat directory with no partitioning:

```
s3://datalake/orders/
├── part-0001.parquet   (all 500M rows spread across ~2000 files)
├── part-0002.parquet
└── ...
```

The three most common queries run by analysts are:

```sql
-- Query 1: Daily orders report
SELECT order_date, COUNT(*), SUM(order_amount)
FROM orders
WHERE order_date = '2024-01-15';

-- Query 2: Regional breakdown for a date range
SELECT region, SUM(order_amount)
FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'
  AND region = 'APAC';

-- Query 3: Customer lifetime value (joins orders with customers)
SELECT o.customer_id, SUM(o.order_amount)
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY o.customer_id;
```

**Tasks:**

1. With no partitioning, how much data does Query 1 scan? Why is this expensive?

2. Propose a partitioning strategy that optimises Queries 1 and 2. Write the folder structure that results from your strategy using the sample data dates (Jan 15–19, 2024).

3. Would you add a second partition column for `region`? What is the risk if there are 50 distinct regions and data is evenly distributed?

4. For Query 3 (the customer join), partitioning by `order_date` does not help — the query scans all partitions. Propose a **bucketing** strategy that makes this join faster. How many buckets would you choose for a 500M row table?

5. Using `data/orders.csv` (50 rows), simulate partitioning by writing a Python script that reads the CSV and writes separate files per `order_date` into a `partitioned_output/order_date=YYYY-MM-DD/` folder structure.

---

## Exercise 9: Add Observability to a Pipeline

**Concept:** Pipeline Observability & Monitoring

**Scenario:**  
You have inherited a pipeline that runs nightly to load `orders_silver`. It has no monitoring. The only way you find out it failed is when an analyst emails you the next morning. Your job is to add observability.

The pipeline currently looks like this:

```python
import pandas as pd
import sqlite3

def run_pipeline():
    conn = sqlite3.connect("warehouse.db")
    df = pd.read_csv("data/orders.csv")
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["status"] = df["status"].str.lower()
    df.to_sql("orders_silver", conn, if_exists="replace", index=False)
    conn.close()

run_pipeline()
```

**Tasks:**

1. Identify **four observability gaps** in this pipeline (what can go wrong silently without any signal).

2. Add the following checks to the pipeline. The pipeline should **raise an error and stop** if any check fails:
   - Row count is greater than 0
   - `order_id` has no null values
   - `order_amount` has no negative values
   - `status` only contains values from the set: `{completed, pending, cancelled}`

3. Add **freshness monitoring**: after loading, check that the maximum `order_date` in `orders_silver` is no more than 2 days behind today's date. If it is stale, print a warning (do not fail — stale data is better than no data).

4. Add a **pipeline run log**: after every run (success or failure), insert a row into a `pipeline_runs` table with columns: `run_id`, `pipeline_name`, `run_date`, `rows_loaded`, `status` (`success`/`failed`), `error_message`, `duration_seconds`.

5. Using `data/orders.csv`, run your instrumented pipeline and print the final run log entry.

---

## Exercise 10: Design an Orchestrated Pipeline

**Concept:** Orchestration & Dependency Management

**Scenario:**  
You need to orchestrate a daily pipeline that builds the `gold.revenue_summary` table. The full dependency chain is:

```
raw.orders       (loaded by Pipeline A, runs at 01:00)
raw.returns      (loaded by Pipeline B, runs at 01:30)
raw.customers    (loaded by Pipeline C, runs at 00:30)
        |
silver.orders       (depends on raw.orders)
silver.returns      (depends on raw.returns)
silver.customers    (depends on raw.customers)
        |
gold.order_metrics  (depends on silver.orders + silver.returns)
gold.customer_dim   (depends on silver.customers)
        |
gold.revenue_summary (depends on gold.order_metrics + gold.customer_dim)
        |
[Email report sent to executives] (depends on gold.revenue_summary)
                                   SLA: must arrive before 07:00
```

**Tasks:**

1. Draw the full DAG (text diagram is fine). Mark which tasks can run in **parallel** and which must be **sequential**.

2. Identify the **critical path** — the longest chain of sequential dependencies that determines the minimum possible end-to-end runtime. If each task takes 20 minutes, what is the earliest the email can be sent?

3. Pipeline A (raw.orders) is late — it finishes at 02:30 instead of 01:30. Using your DAG, determine:
   - Which tasks are blocked?
   - Which tasks are unaffected and can still run on schedule?
   - Will the 07:00 email SLA be met? Show your working.

4. Write pseudocode for this DAG using an Airflow-like syntax. Define tasks, set dependencies with `>>` operators, and add an SLA of 30 minutes on the `gold.revenue_summary` task.

5. The team wants to **backfill** this pipeline for the last 7 days (the pipeline was broken for a week). List two requirements the pipeline must satisfy for backfill to work correctly. Which concept from earlier in Day 1 is essential here?
