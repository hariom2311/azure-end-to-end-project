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

## Exercise 2: Diagnose a Data Lake Problem

**Concept:** Data Warehouses vs. Data Lakes vs. Lakehouses

**Scenario:**  
A company has a data lake with the following layout:

```
s3://company-lake/
├── raw/orders/                      ← 2 million files, avg 4 KB each
├── raw/customers/                   ← 500,000 files, avg 8 KB each
├── silver/orders/                   ← No partitioning, 1 large 80 GB Parquet file
├── silver/customers/                ← No partitioning, 1 large 12 GB Parquet file
└── gold/revenue_summary/            ← 3 files, updated by two different Spark jobs simultaneously
```

The team reports three problems:
- **Problem A:** Queries on `silver/orders/` that filter by `order_date` take 45 minutes
- **Problem B:** `gold/revenue_summary/` occasionally has corrupted or duplicate rows after concurrent writes
- **Problem C:** A Spark job that processes `raw/orders/` takes 3 hours just to list the files before it starts reading

**Tasks:**

1. Diagnose the root cause of each problem (A, B, C) using the concepts from Day 2.

2. For Problem A: propose a fix. What partitioning strategy would you apply? Re-sketch the folder structure after your fix.

3. For Problem B: what lakehouse feature directly solves this? Name the specific mechanism (not just the tool).

4. For Problem C: this is the small files problem. Propose two solutions — one immediate fix and one architectural change to prevent it recurring.

5. The team is deciding between Delta Lake and Apache Iceberg for their new lakehouse. They use Spark for ETL, Trino for ad-hoc queries, and Flink for streaming. Which would you recommend and why?

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

## Exercise 4: Design Object Storage Layout

**Concept:** Object Storage & Storage Tiers

**Scenario:**  
Design the S3 folder structure and lifecycle policy for a fintech company with these data assets:

| Dataset | Volume | Access pattern | Retention requirement |
|---|---|---|---|
| Raw transaction events (JSON) | 50 GB/day | Queried in first 7 days for debugging; rarely after | 7 years (regulatory) |
| Silver transactions (Parquet) | 10 GB/day | Daily queries for last 90 days; monthly queries for last 2 years | 3 years |
| Gold daily aggregates | 500 MB/day | Dashboard queries; last 1 year very active | 5 years |
| ML feature store (Parquet) | 2 GB/day | Active for 6 months; archived after | 2 years |
| Audit logs (CSV) | 100 MB/day | Compliance only; 30-day lookback normal; up to 7 years | 7 years |

**Tasks:**

1. Design the full S3 bucket structure (folder hierarchy). Show at least 3 levels of nesting.

2. Write a lifecycle policy (in JSON or pseudocode) for each of the 5 data assets. For each, specify:
   - Day 0: starting storage class
   - Transition dates and target storage classes
   - Whether to enable versioning (and why)

3. The raw transaction events are currently stored as individual JSON files (one file per event = 1,000,000 files/day). Calculate the monthly S3 API cost assuming GET requests cost $0.0004 per 1,000 and the data is queried 10 times/day (each query reads all 1M daily files for the last 7 days). What architectural change would dramatically reduce this cost?

4. The ML feature store needs to be shared with a partner data science team in a different AWS account. How do you grant them read-only access without copying the data?

5. Gold aggregates are queried by Athena. The team is getting billed for scanning the full table even though most queries only look at the last 30 days. What is the fix and how does it work?

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

## Exercise 8: Avro Schema Evolution

**Concept:** Serialisation Formats — Avro & Protobuf

**Scenario:**  
A Kafka topic `orders-events` uses Avro with this schema (v1):

```json
{
  "type": "record",
  "name": "OrderEvent",
  "fields": [
    {"name": "order_id",    "type": "int"},
    {"name": "customer_id", "type": "string"},
    {"name": "amount",      "type": "double"},
    {"name": "status",      "type": "string"}
  ]
}
```

The team wants to make these changes (v2):

```
Change 1: Add field "currency" (string, default "USD")
Change 2: Add field "discount_code" (nullable string, default null)
Change 3: Remove field "status" (they will put status in a separate topic)
Change 4: Rename "amount" to "order_amount"
Change 5: Change "customer_id" from string to int
```

**Tasks:**

1. Classify each change (1–5) as **backward compatible**, **forward compatible**, **fully compatible**, or **breaking** in the context of Avro schema evolution. Explain each.

2. Which changes can be deployed without updating any consumers first?

3. Write the v2 schema JSON for only the safe (non-breaking) changes. Ensure it is Avro-valid.

4. For the breaking changes (3, 4, 5): propose a migration strategy for each that avoids downtime. (Hint: you cannot modify existing messages in Kafka — only future messages.)

5. How does a Confluent Schema Registry prevent incompatible schema changes from being accidentally published? Describe the enforcement mechanism in 3 sentences.

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
