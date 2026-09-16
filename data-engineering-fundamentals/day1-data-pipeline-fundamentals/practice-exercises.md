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
