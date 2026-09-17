# Day 2 — Practice Exercises: Data Storage, File Formats & the Lakehouse

> Use the sample data files in the `data/` folder alongside these exercises.
> Exercises are hands-on — run code where indicated. SQL-style pseudocode is acceptable where a real engine is not available.

---

## Exercise 1: Format Comparison — CSV vs. Parquet

**Concept:** File Formats

**Scenario:**  
You have `data/products.csv` — a dataset of 1,000 product records. Your task is to compare storage size, read performance, and query behaviour between CSV and Parquet.

**Tasks:**

1. Load `data/products.csv` into a pandas DataFrame. Print the shape and column names.

2. Write the DataFrame to Parquet using `df.to_parquet("products.parquet", compression="snappy")`. Compare file sizes:
   - `products.csv` — original size
   - `products.parquet` — Parquet with Snappy
   - Re-save with `compression="gzip"` → `products_gzip.parquet`
   - Re-save with `compression=None` → `products_nocompression.parquet`
   
   Build a table showing file size and compression ratio for each.

3. Benchmark read time for a filtered query: "Find all products in category `Electronics` with `price > 500`":
   - Read from CSV: filter in pandas after loading the full file
   - Read from Parquet: use `pd.read_parquet("products.parquet", filters=[("category", "==", "Electronics"), ("price", ">", 500)])`
   
   Use `time.time()` to measure wall-clock time for each. What is the speedup?

4. Answer: Why is reading from Parquet faster for this query even though the file is smaller? What specific Parquet feature enables this?

5. Which columns in `data/products.csv` would benefit most from dictionary encoding? Which would benefit from run-length encoding? Explain your reasoning.

**Sample data:** `data/products.csv`

---

## Exercise 2: CDC — Trace Changes Through a Transaction Log

**Concept:** Change Data Capture (CDC)

**Scenario:**  
A source PostgreSQL database has an `orders` table. The following sequence of operations happens between 09:00 and 09:05:

```
09:00:01 — INSERT order_id=1051, customer_id='C041', amount=95.00,  status='pending'
09:01:14 — INSERT order_id=1052, customer_id='C042', amount=310.00, status='pending'
09:02:30 — UPDATE order_id=1051 SET status='completed'
09:03:45 — INSERT order_id=1053, customer_id='C043', amount=55.00,  status='pending'
09:04:10 — DELETE order_id=1048  (cancelled and refunded)
09:04:55 — UPDATE order_id=1052 SET amount=285.00  (price adjustment)
```

Your CDC tool (Debezium) captures these and publishes to Kafka with fields: `op`, `before`, `after`, `ts_ms`.

**Tasks:**

1. Write the 6 Kafka CDC event payloads (JSON) for each operation. Use `op` values `c` (insert), `u` (update), `d` (delete). Populate `before` and `after` correctly — `before` is `null` for inserts, `after` is `null` for deletes.

2. Your Silver table `orders_silver` contains the rows from `data/orders.csv`. Write the SQL MERGE statement that applies all 6 CDC events to `orders_silver` in one pass, handling all three operation types.

3. A timestamp-based incremental pipeline uses `WHERE created_at > last_run_timestamp`. Which of the 6 operations above would it miss and why?

4. **Replication lag:** Debezium reports a 45-second lag (events are delayed). Your Spark consumer runs every 60 seconds. What is the maximum staleness of `orders_silver`? Design a monitoring check that alerts if lag exceeds 2 minutes.

5. The team asks: "Why use Debezium when we could add a PostgreSQL trigger that writes changes to a side table, then poll it?" Give two concrete advantages of log-based CDC over trigger-based CDC.

**Sample data:** `data/orders.csv`

---

## Exercise 3: Understand Delta Lake Time Travel

**Concept:** Open Table Formats — Delta Lake

**Scenario:**  
Using `data/orders_delta_log.json` (a simplified Delta Lake transaction log), answer the following questions about what the table looked like at each commit.

The log shows 5 commits:
- Commit 0: Created table, added files A, B, C (100 rows total)
- Commit 1: Added file D (20 new orders), added file E (15 new orders)
- Commit 2: MERGE — removed file B, added file F (updated 30 rows + 10 new rows from B combined)
- Commit 3: DELETE — removed file C, added file G (file C with 5 rows deleted)
- Commit 4: Added file H (25 new orders)

**Tasks:**

1. How many data files represent the **current** (commit 4) state of the table? List which files are active (added but not removed).

2. If you query `VERSION AS OF 2`, which files does the engine read?

3. A bad MERGE was discovered at commit 2 — it accidentally set `status = 'cancelled'` for all orders instead of only those matching the condition. How do you recover? Write the SQL command to restore the table to the state at commit 1.

4. After running `VACUUM RETAIN 0 HOURS` (for testing only — dangerous in production), which files would be physically deleted from storage? Which files are safe from deletion?

5. Commit 3 was a DELETE of 5 rows for GDPR compliance (`customer_id = 9999`). Using the transaction log approach, does this **immediately** free storage? What does `VACUUM` do and when must you run it?

**Sample data:** `data/orders_delta_log.json`

---

## Exercise 4: Database Indexing — Diagnose Slow Queries

**Concept:** Database Indexing & Query Optimisation

**Scenario:**  
You are handed a PostgreSQL table `orders` with 50 million rows and the schema from `data/orders.csv`. The following queries are running in production. Use `EXPLAIN` output hints provided to diagnose and fix each one.

```sql
-- Table: orders (50M rows)
-- Existing indexes: PRIMARY KEY on order_id

-- Query A — runs in 45 seconds
SELECT * FROM orders WHERE customer_id = 'C001';
-- EXPLAIN output: Seq Scan on orders (cost=0..1,250,000 rows=12 width=64)

-- Query B — runs in 12 seconds
SELECT order_date, SUM(order_amount) FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY order_date;
-- EXPLAIN output: Seq Scan on orders (cost=0..1,250,000 rows=1,500,000 width=16)

-- Query C — runs in 30 seconds (called 10,000 times/day)
SELECT customer_id, order_date, order_amount
FROM orders
WHERE customer_id = 'C001'
ORDER BY order_date DESC;
-- EXPLAIN output: Seq Scan → Sort (cost=1,250,000..1,252,000)

-- Query D — runs in 2 seconds but needs to be faster
SELECT COUNT(*) FROM orders WHERE status = 'pending';
-- EXPLAIN output: Index Scan using idx_status (cost=0.56..8,200 rows=1,000,000)
-- Note: 'pending' represents 2% of rows
```

**Tasks:**

1. For Query A: write the `CREATE INDEX` statement that fixes it. What type of scan will `EXPLAIN` show after the index is added?

2. For Query B: a B-tree index on `order_date` exists but `EXPLAIN` still shows a Seq Scan. Why might the planner ignore the index? Under what condition does the planner prefer a Seq Scan over an index scan even when an index exists?

3. For Query C: this is called 10,000 times/day. Write a single **covering index** that makes this query an index-only scan. Explain what columns go in the index and in what order, and why no heap access is needed.

4. For Query D: `status` has only 5 distinct values and 'pending' is 2% of rows. The index exists but is it the right kind? Rewrite the index as a **partial index** that indexes only pending orders. Show the `CREATE INDEX` statement and explain why this is smaller and faster than a full index on `status`.

5. After adding all your indexes, the nightly bulk load of 500,000 new orders slows from 3 minutes to 25 minutes. Why? What is the standard practice to handle indexes during bulk loads?

**Sample data:** `data/orders.csv`

---

## Exercise 5: Build a Data Catalog Entry

**Concept:** Data Catalog & Metadata Management

**Scenario:**  
The `orders_silver` table exists in your data lake but has no documentation. An analyst has emailed asking: "What does `order_status` mean? Is `completed` the same as `fulfilled`? Why are there nulls in `discount_code`? Who owns this table?"

Your job is to write a complete catalog entry for this table.

**Tasks:**

1. Using `data/orders.csv` (from Day 1) as the reference, write a full catalog entry in the format below. Fill in every field:

```
Table: silver.orders
Owner: [team or person]
Description: [1-2 sentences in plain English]
Source: [where does this data come from?]
Update frequency: [how often is it refreshed?]
SLA / freshness: [how stale can this data be before it is a problem?]
Tags: [list tags: pii / financial / certified / etc.]

Schema:
| Column | Type | Nullable | Description | Example |
|---|---|---|---|---|
| order_id | ... | ... | ... | ... |
| customer_id | ... | ... | ... | ... |
| ... (complete all columns) |

Known issues / caveats:
- [Any known data quality issues]
- [Any columns with unusual behaviour]

Downstream consumers:
- [Who or what reads this table]

Contact:
- [Who to call when something breaks]
```

2. The `order_status` column contains values: `completed`, `pending`, `cancelled`. Define what each means in business terms, and what a data consumer should do if they encounter an unexpected value.

3. Write a SQL data quality check that could be run nightly and whose results would be stored in the catalog as the table's quality score:
   - Completeness: % of non-null values in required columns
   - Validity: % of rows where `status` is in the known set
   - Freshness: hours since `MAX(order_date)` vs. current time

4. Describe in 3 sentences how you would use a tool like dbt to auto-generate parts of this catalog entry from the pipeline code itself.

---

## Exercise 6: Storage Lifecycle Simulation

**Concept:** Storage Layout Patterns — Hot, Warm, Cold & Data Lifecycle

**Scenario:**  
Using `data/orders.csv`, simulate a tiered storage lifecycle policy in Python.

**Tasks:**

1. Load `orders.csv`. Add a column `days_since_order` computed as the number of days between `order_date` and today (`2026-09-17`).

2. Apply the following lifecycle rules to classify each order into a storage tier:
   - `days_since_order <= 90` → tier = `hot`
   - `days_since_order <= 365` → tier = `warm`
   - `days_since_order <= 1825` (5 years) → tier = `cold`
   - `days_since_order > 1825` → tier = `archive`

3. Print a summary:
   ```
   hot:     N orders  (X%)
   warm:    N orders  (X%)
   cold:    N orders  (X%)
   archive: N orders  (X%)
   ```

4. The orders in `archive` tier must be available for GDPR deletion requests within 72 hours. However, Glacier Deep Archive has a retrieval time of 12–48 hours. Is this tier appropriate for this data? What tier would you use instead, and what is the monthly cost difference per GB?

5. Write Python code to group the orders by `order_date` and simulate writing them as separate "daily partition files". Print how many files would be created and what the average rows-per-file would be. Is this a healthy file size for a real data lake? What would you do differently for a production pipeline?

---

## Exercise 7: OLTP vs. OLAP Query Analysis

**Concept:** OLTP vs. OLAP Storage Systems

**Scenario:**  
Classify each of the following queries as OLTP or OLAP. Then explain what type of storage system (row-oriented or column-oriented) would execute it most efficiently and why.

**Queries:**

```sql
-- Query A
SELECT * FROM orders WHERE order_id = 1042;

-- Query B
SELECT product_category, SUM(order_amount), COUNT(*) as num_orders
FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY product_category
ORDER BY SUM(order_amount) DESC;

-- Query C
UPDATE orders SET status = 'cancelled' WHERE order_id = 1005;

-- Query D
SELECT customer_id, COUNT(*) as total_orders, AVG(order_amount) as avg_spend
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 3
ORDER BY avg_spend DESC;

-- Query E
INSERT INTO orders VALUES (1051, 'C041', '2024-01-20', 95.00, 'pending', 'USD', 'books');

-- Query F
SELECT order_date, currency, SUM(order_amount)
FROM orders
GROUP BY order_date, currency;
```

**Tasks:**

1. Classify each query as OLTP or OLAP.

2. For each OLAP query, identify which columns the query engine must read. If stored in Parquet (columnar), how many columns are skipped compared to reading all 7 columns?

3. For Query D and F: these are run on the `orders` table stored in Parquet, partitioned by `order_date`. Does the partitioning help? Why or why not?

4. The operations team wants to run Query B every 5 minutes to check for sales spikes. Should this run against the OLTP source database or the OLAP data lake? What are the risks of running it on the source database?

5. Design a storage architecture (at a high level) that satisfies both:
   - Operations team: update/cancel orders in real time (millisecond latency)
   - Analytics team: run complex aggregations across 3 years of orders (no impact on operational systems)

---

## Exercise 8: Replication Lag & Consistency — Diagnose Pipeline Anomalies

**Concept:** Data Replication & Consistency Models

**Scenario:**  
Your pipeline reads from a PostgreSQL **read replica** (not the primary). The replica has an average replication lag of 8 seconds under normal load, spiking to 90 seconds during peak hours (18:00–20:00).

Your incremental pipeline runs every 15 minutes and uses this query:
```sql
SELECT * FROM orders
WHERE created_at > :last_watermark
  AND created_at <= NOW()
```

**Tasks:**

1. An order is created on the primary at 17:59:55. The pipeline's watermark at its 18:00 run is `17:59:45`. During peak hours, replication lag is 90 seconds. Is this order captured in the 18:00 run? Show the timeline with exact timestamps.

2. The pipeline ran at 18:00 and set its watermark to `18:00:00`. At 18:15, the next run starts. The replica's lag is now 90 seconds. What is the latest `created_at` visible on the replica at 18:15? Write the corrected watermark query that adds a lag buffer to avoid missing records.

3. **CAP theorem application:** Your read replica is in the same datacenter as the primary. A network partition occurs between primary and replica. The replica continues serving reads. Is this system CP or AP? What data freshness guarantee does an analyst querying the replica have during the partition?

4. Using `data/orders.csv`, write a Python function `check_replica_lag(primary_max_id, replica_max_id)` that:
   - Takes the max `order_id` from both primary and replica
   - Returns the number of orders the replica is behind
   - Prints `CRITICAL` if behind by more than 100 orders, `WARNING` if more than 10, `OK` otherwise

5. The team proposes switching to **eventual consistency** for the Gold layer: "Let the Gold table be 5 minutes stale — it's fine for dashboards." What scenarios make this acceptable and what scenarios make it dangerous? Give one example of each from an e-commerce context.

**Sample data:** `data/orders.csv`

---

## Exercise 9: Storage Performance Investigation

**Concept:** Storage Performance Optimisation

**Scenario:**  
A query runs against a Silver table with the following Parquet file statistics (each file has row group statistics stored in its footer):

```
File 1: order_date=[2024-01-15 to 2024-01-15], amount=[10.50 to 99.99],  rows=50,000
File 2: order_date=[2024-01-16 to 2024-01-16], amount=[12.00 to 450.00], rows=48,000
File 3: order_date=[2024-01-17 to 2024-01-17], amount=[15.00 to 830.00], rows=52,000
File 4: order_date=[2024-01-18 to 2024-01-18], amount=[18.50 to 720.00], rows=49,000
File 5: order_date=[2024-01-19 to 2024-01-19], amount=[38.00 to 680.00], rows=51,000
```

**Queries to analyse:**

```sql
-- Query 1
SELECT SUM(amount) FROM orders WHERE order_date = '2024-01-16';

-- Query 2
SELECT * FROM orders WHERE amount > 500;

-- Query 3
SELECT COUNT(*) FROM orders WHERE order_date BETWEEN '2024-01-17' AND '2024-01-19'
  AND amount < 100;

-- Query 4
SELECT AVG(amount) FROM orders WHERE order_date = '2024-01-15';
```

**Tasks:**

1. For each query, use the file statistics to determine:
   - Which files can be **pruned** (skipped entirely)?
   - Which files must be **read**?
   - What % of total data is skipped?

2. Query 2 (`amount > 500`) reads files 2–5 but still must scan all rows in those files to find amounts > 500. What additional optimisation (beyond min/max statistics) would allow the engine to skip more rows within those files?

3. After running `OPTIMIZE ... ZORDER BY (order_date, amount)` on this table, all 5 files are rewritten into 2 new files:
   - New File A: order_date=[2024-01-15 to 2024-01-17], amount=[10.50 to 830.00]
   - New File B: order_date=[2024-01-17 to 2024-01-19], amount=[15.00 to 830.00]
   
   Re-analyse Query 1 with the new file layout. Is predicate pushdown more or less effective? Why?

4. The table receives 10,000 new rows per hour written as individual small files. After 7 days, there are 1,680 files averaging 12 KB each. How would you restructure the write and compaction strategy to keep file sizes healthy?

---

## Exercise 10: End-to-End Storage Architecture Design

**All 10 concepts combined**

**Scenario:**  
You are the lead data engineer at a retail company processing 2 million orders per day across 50 countries. Design the complete storage architecture.

**Requirements:**
- Orders arrive as JSON events via a message queue
- Analytics team needs historical queries (3 years) with sub-minute response time
- Finance team needs daily aggregates with 99.9% accuracy (no duplicate counting)
- ML team needs raw feature data with full history for model retraining
- Legal requires 7-year retention of all raw events
- GDPR: any customer's data must be deletable within 72 hours
- Budget: minimise storage cost while meeting all above requirements

**Design tasks:**

1. Choose a file format for each layer (Bronze, Silver, Gold) and justify each choice.

2. Choose a table format (Delta Lake, Iceberg, or Hudi) and justify the choice given the GDPR deletion requirement.

3. Design the partition strategy for the Silver orders table. What column(s) do you partition by and why?

4. Design the lifecycle policy: when does data move from hot → warm → cold in each layer?

5. How does the GDPR 72-hour deletion requirement affect your Bronze layer design? What is the technical challenge with deleting from Parquet files and how does your chosen table format solve it?

6. The Finance team's "no duplicate counting" requirement means the Gold layer must be exactly correct after any pipeline re-run. Which write pattern from Day 1 (Concept 5) satisfies this, and how does the table format support it?

7. Estimate (order of magnitude) the monthly storage cost for the Bronze layer only, given: 2M orders/day × 365 days × avg 500 bytes/order JSON = ~365 GB/year. Use S3 pricing with appropriate lifecycle transitions.
