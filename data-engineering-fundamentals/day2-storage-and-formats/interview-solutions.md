# Day 2 — Interview Solutions: Data Storage, File Formats & the Lakehouse

> Complete answers for all 55 questions in `interview-questions.md`.

---

## Concept 1: File Formats

**Q1 — Row-oriented vs. column-oriented formats**

**Row-oriented:** All columns of a single record are stored contiguously on disk.
- Example: CSV — `1001,C001,120.50,completed` is one line; reading any column means reading the full row
- Good for: writing individual records, reading complete rows (OLTP)

**Column-oriented:** All values of a single column are stored contiguously on disk.
- Example: Parquet — all `amount` values are in one file segment; reading `amount` never touches `customer_id`
- Good for: analytical queries that aggregate one or a few columns across millions of rows

**Why columnar is better for analytics:** A query `SELECT SUM(amount) FROM orders` on a 100-column table needs only the `amount` column. Row-oriented storage reads all 100 columns to extract one. Columnar storage reads only 1 column — up to 100× less I/O.

---

**Q2 — Why Parquet dominates modern data lakes**

1. **Columnar reads:** Analytical queries read only the columns they need — most queries touch 2–5 of potentially hundreds of columns
2. **Excellent compression:** Columnar layout exposes repetition (RLE, dictionary encoding) that gives 5–10× compression over raw data
3. **Predicate pushdown:** Row group min/max statistics allow entire blocks to be skipped without decompression
4. **Embedded schema:** No separate schema file needed — the Parquet footer carries the full schema
5. **Splittable:** Row groups can be assigned to different Spark tasks — fully parallel reads of large files
6. **Wide ecosystem support:** Spark, Hive, Presto, Athena, BigQuery, Snowflake external tables, pandas, DuckDB all read Parquet natively

---

**Q3 — Predicate pushdown in Parquet**

Predicate pushdown is the ability to skip reading parts of a file that cannot contain rows matching a query filter.

**How Parquet enables it:**
Parquet files are divided into **row groups** (default 128 MB). Each row group's **footer** stores, for every column:
- Minimum value
- Maximum value
- Null count
- (Optionally) a bloom filter

When the query engine applies a filter (e.g., `WHERE amount > 500`), it reads only the footer first. If a row group's `amount` max is 200, the entire row group is skipped — no decompression, no data transfer.

**Example:**
```
Row group 1: amount min=10, max=99   → skip (max < 500)
Row group 2: amount min=100, max=600 → read (max >= 500, may contain matches)
Row group 3: amount min=700, max=950 → read (min > 500, all rows match)
```
Only row groups 2 and 3 are decompressed.

---

**Q4 — 10 TB Gzip CSV single file problems**

**Problem 1 — Not splittable:** Gzip is a stream codec — the only way to decompress byte 1 billion is to decompress everything before it. A single Gzip CSV cannot be split across Spark workers. All 10 TB must go to one task on one worker → out-of-memory crash or extreme slowness.

**Problem 2 — No partition pruning:** A single flat file has no directory structure. The query `WHERE event_date = '2024-01-15'` still reads all 10 TB and filters in memory.

**Problem 3 — Row-oriented full column scan:** Reading three columns still requires reading all columns in each row and discarding the rest.

**Fix:**
- Convert to Parquet with Snappy or Zstd compression (splittable, columnar)
- Partition by `event_date` — queries on one day read ~2.7 GB instead of 10 TB
- This alone can reduce query time from hours to seconds

---

**Q5 — Dictionary encoding**

Dictionary encoding replaces repeated string values with a small integer code stored in a lookup table (dictionary).

```
Raw status column: [completed, pending, completed, cancelled, completed, pending, completed]
Dictionary:        {0: "completed", 1: "pending", 2: "cancelled"}
Encoded column:    [0, 1, 0, 2, 0, 1, 0]
```

**Benefit:** Integers (1–2 bytes) compress far better than strings (8–12 bytes for "completed"). The dictionary is stored once; the column stores only integer codes.

**Which columns benefit most in e-commerce:**
- `status` (4–5 distinct values across billions of rows) — extreme compression
- `currency` (USD, EUR, GBP — 3 values) — extreme compression
- `country` (200 values) — good compression
- `product_category` (10–50 values) — good compression
- `order_id` (every value unique) — no benefit from dictionary encoding (each value is its own dict entry)

---

## Concept 2: Change Data Capture (CDC)

**Q6 — What CDC solves that timestamp-based incremental load cannot**

A timestamp-based incremental load (`WHERE updated_at > :last_run`) has three critical blind spots:

1. **Hard DELETEs are invisible.** When a row is deleted, `updated_at` no longer exists — the row simply disappears. The downstream table never reflects the deletion.
2. **Clock drift and lag.** If the source database clock is behind the pipeline clock, or if a replica has replication lag, updates that occurred "just before" the watermark are silently skipped.
3. **No `updated_at` on all tables.** Many legacy tables do not have a reliable `updated_at` column, or it is not updated consistently by all write paths.

**CDC captures all three change types — INSERT, UPDATE, DELETE — by reading the database's write-ahead log (WAL) rather than querying the table.** The WAL is the ground truth: every write is recorded there before it is applied to the data pages.

---

**Q7 — How log-based CDC works (WAL mechanics)**

Every production relational database uses a Write-Ahead Log for crash recovery. Before any change is applied to data pages, it is first written to the WAL with full before/after images.

**Why reading the WAL has near-zero production impact:**
- The WAL is always written regardless — CDC just reads it as a secondary consumer
- Reading a log file (sequential I/O) does not lock any table rows
- The database exposes a **replication slot** (PostgreSQL) or **binlog** (MySQL): a stable cursor that CDC tools read at their own pace
- The source database does not execute any additional queries for CDC — it merely ships WAL records to the reader

**Debezium workflow:**
```
MySQL binlog → Debezium connector (runs in Kafka Connect) → Kafka topic (one event per row change) → consumer (Spark Structured Streaming / Flink)
```

Each CDC event contains: `op` (c/u/d), `before` (old row image), `after` (new row image), `ts_ms` (commit timestamp), `pos` (log position for exactly-once resumption).

---

**Q8 — CDC-based replacement for a 6-hour full-load pipeline**

**Current pain:** 200M-row full load takes 6 hours, misses hard DELETEs, and hammers the source DB with a giant SELECT.

**Proposed architecture:**

```
MySQL → Debezium (Kafka Connect) → Kafka topic: orders.cdc
                                         ↓
                              Spark Structured Streaming (or Flink)
                                         ↓
                         Snowflake Silver table (via MERGE)
```

**Steps:**

1. **Initial snapshot (one-time):** Debezium performs a consistent snapshot read of the full `transactions` table and publishes every row as a `c` (create) event. This happens once, during the transition.

2. **Ongoing CDC:** After the snapshot, Debezium tails the MySQL binlog. Only changed rows are published. At 5% daily change rate on 200M rows → 10M events/day vs. 200M rows re-read.

3. **Silver MERGE:** The consumer applies events using MERGE (match on `transaction_id`): INSERT for `op=c`, UPDATE for `op=u`, DELETE for `op=d`.

**Target latency:** < 30 seconds from source commit to Silver table (Kafka buffer + streaming micro-batch).

**DELETEs:** Now fully captured — `op=d` events carry the `before` image so you know exactly which row was deleted and can optionally soft-delete (set `is_deleted = true`) for audit trails.

---

**Q9 — Idempotent MERGE for duplicate CDC events**

**Requirement:** The MERGE must be idempotent — running it twice on the same event must produce the same result as running it once.

For a duplicate `UPDATE` event with the same `after` image: re-applying a MERGE that sets `status = 'completed'` when it is already `'completed'` leaves the row unchanged. This is naturally idempotent for UPDATEs.

For `INSERT` events: if the row already exists (from the first delivery), the MERGE matches on the primary key and updates instead of inserting — no duplicate row.

```sql
MERGE INTO silver.orders AS target
USING (
  SELECT
    after.order_id,
    after.customer_id,
    after.amount,
    after.status,
    after.updated_at,
    op,
    event_ts
  FROM cdc_staging
) AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.op IN ('u', 'r') THEN
  UPDATE SET
    target.customer_id  = source.customer_id,
    target.amount       = source.amount,
    target.status       = source.status,
    target.updated_at   = source.updated_at,
    target._cdc_ts      = source.event_ts
WHEN MATCHED AND source.op = 'd' THEN
  UPDATE SET target._is_deleted = TRUE,
             target._deleted_at  = source.event_ts
WHEN NOT MATCHED AND source.op IN ('c', 'r') THEN
  INSERT (order_id, customer_id, amount, status, updated_at, _cdc_ts)
  VALUES (source.order_id, source.customer_id, source.amount,
          source.status, source.updated_at, source.event_ts);
```

**Key idempotency properties:**
- `WHEN MATCHED ... UPDATE SET` is a pure assignment — applying the same values twice is identical to applying once
- No INSERT when the row already exists — the MATCHED branch fires instead
- To handle out-of-order duplicates, add: `AND source.event_ts > target._cdc_ts` to the MATCHED condition so stale re-deliveries do not overwrite newer data

---

## Concept 3: Data Warehouses vs. Data Lakes vs. Lakehouses

**Q10 — Data warehouse limitations**

A **data warehouse** is a structured, purpose-built analytical database (Snowflake, Redshift, BigQuery) where data is loaded in a predefined schema and queried with SQL.

**Limitation 1 — Cost:** Storage and compute are tightly coupled in classic warehouses. You pay for both even when queries are not running. At petabyte scale, warehouse storage costs become prohibitive.

**Limitation 2 — Rigidity:** All data must conform to a predefined schema before it can be loaded. Unstructured data (images, logs, PDFs), semi-structured data (JSON with variable keys), and raw data cannot be stored in a warehouse without significant pre-processing. This also means you must know how data will be used before you store it.

---

**Q11 — What is a data swamp?**

A data swamp is a data lake that has become unusable because data landed without governance: no schemas, no documentation, no ownership, no quality control.

**Properties that cause it:**
- **No schema enforcement:** Any data is accepted in any format — you cannot query it reliably
- **No cataloging:** Nobody knows what tables exist, what columns mean, or which data is trustworthy
- **No lineage:** Nobody knows where the data came from or which downstream systems depend on it
- **No access control:** Sensitive data is mixed with non-sensitive; anyone can read anything
- **No data quality:** Bad data lands alongside good data with no flagging

The result: analysts distrust the lake, revert to querying source systems directly, and the lake becomes a write-only archive that nobody reads.

---

**Q12 — What is a lakehouse?**

A **lakehouse** combines:
- **Data lake foundation:** Cheap open object storage (S3/GCS/ADLS), open file formats (Parquet), decoupled compute
- **Data warehouse capabilities:** ACID transactions, schema enforcement, SQL support, query performance, governance

The key innovation is an **open table format layer** (Delta Lake, Iceberg, Hudi) that sits between raw Parquet files and the query engine, adding:
- ACID transactions (concurrent writers cannot corrupt)
- Schema enforcement and evolution
- Row-level deletes and updates
- Time travel (query historical versions)
- Statistics for query optimisation

Result: you get warehouse reliability and performance at data lake cost.

---

**Q13 — Startup architecture recommendation**

**Recommendation: Option B (Snowflake) for now, with a clear path to Option C.**

**Why not A (PostgreSQL direct):** At 10 GB today this works, but at 10 TB it will destroy operational performance. Analytical queries lock tables and slow down the application.

**Why B now:** At 10 GB with 3 analysts, the engineering complexity of a lakehouse (Option C) is not justified. Snowflake is fast to set up, requires minimal infrastructure knowledge, and handles 10 TB comfortably. The nightly ETL adds minimal complexity.

**Why not C yet:** A lakehouse requires expertise in Spark, Delta Lake, and orchestration. With 3 analysts and presumably a small data team, the operational overhead is too high relative to benefit at this data volume.

**Switch to C when:**
- ML team needs raw feature data (warehouse alone cannot serve this)
- Data volume > 1 TB/day (warehouse storage cost becomes significant)
- Need for multi-engine access (Spark + SQL + streaming) emerges
- Budget constrains warehouse per-query costs

---

**Q14 — Compute-storage separation**

In a traditional warehouse (classic Redshift, on-premise Teradata), compute nodes store data locally. You cannot add compute without adding storage and vice versa. You pay for the maximum compute you ever need, 24/7.

In a lakehouse:
- **Storage:** Object storage (S3) — scales independently, priced per GB
- **Compute:** Spark clusters / Databricks clusters / Athena — spin up on demand, pay per query

**Cost implication:** A lakehouse cluster can be shut down at 6 PM and restarted at 8 AM. You pay for 13 hours of compute instead of 24. Snowflake has added storage separation in its modern architecture, but still charges for storage at warehouse rates (~5× S3 pricing).

For a company with 10 TB of data that is only queried during business hours, a lakehouse with auto-scaling clusters can cost 70–80% less than a comparable warehouse.

---

## Concept 4: Open Table Formats

**Q15 — What problem do open table formats solve?**

Plain Parquet files on object storage are:
- **Immutable:** Cannot edit a row — must rewrite the entire file
- **Non-transactional:** Two concurrent writers can corrupt a table by simultaneously adding conflicting files
- **Non-versioned:** No history — once a file is overwritten it is gone
- **Schema-blind:** No enforcement — any writer can add any columns

Open table formats (Delta Lake, Iceberg, Hudi) add a **transaction log + metadata layer** on top of Parquet that provides:
- **ACID transactions:** Atomic commits — all files in a write either succeed or fail together
- **Concurrent write safety:** Optimistic concurrency control prevents corruption
- **Updates and deletes:** Mark old files as removed, add new files with the changes
- **Time travel:** Every commit is preserved — query any historical state
- **Schema enforcement:** Reject writes that do not match the declared schema

---

**Q16 — How Delta Lake's transaction log works**

Delta Lake maintains a `_delta_log/` directory containing one JSON file per commit:

```
_delta_log/
├── 00000000000000000000.json  ← commit 0
├── 00000000000000000001.json  ← commit 1
└── 00000000000000000010.checkpoint.parquet  ← checkpoint at every 10 commits
```

**Each commit JSON contains:**
- `add`: list of Parquet files added (with stats: row count, min/max per column)
- `remove`: list of Parquet files that are no longer part of the current table (tombstoned)
- `metaData`: schema at this commit, partition columns, table configuration
- `commitInfo`: operation type (INSERT, UPDATE, DELETE, MERGE), timestamp, user, parameters

**Reading the current table state:**
1. Find the latest checkpoint (avoids reading all commits from 0)
2. Apply all JSON commits after the checkpoint in order
3. Current table = all files in `add` entries that are not in any subsequent `remove` entry

This log is the source of truth — the Parquet files are just data blobs; the log determines which blobs form the table.

---

**Q17 — GDPR DELETE on a 500 GB Delta Lake table**

**What actually happens:**

1. Delta Lake identifies which Parquet files contain rows with `customer_id = 'C9999'`
2. For each affected file, it rewrites the file **without** the deleted rows → new Parquet file
3. The original Parquet file is marked as **removed** in the new `_delta_log` commit entry
4. The new Parquet file (without the deleted rows) is added to the commit

**Does this immediately free disk space? No.**

The original file (containing the deleted rows) still exists on S3. It is merely **tombstoned** in the transaction log — not referenced by the current table version, but physically present.

To actually free disk space, you must run `VACUUM`:
```sql
VACUUM orders RETAIN 0 HOURS  -- dangerous in production; use 7+ days
```
VACUUM deletes all files not referenced by any commit within the retention window. The default retention is 7 days — this allows time travel for 7 days after a delete.

**GDPR implication:** If you need to guarantee the data is irrecoverable, you must run VACUUM after the DELETE, accepting that time travel for the deleted data is no longer possible.

---

**Q18 — Concurrent writes without table format**

**What goes wrong without a table format:**
- Job A reads the table: sees files [F1, F2, F3]
- Job B reads the table: sees files [F1, F2, F3]
- Job A writes F4 (Jan data) and updates metadata → table = [F1, F2, F3, F4]
- Job B writes F5 (Feb data) and updates metadata → table = [F1, F2, F3, F5]

Job B's metadata update overwrites Job A's. File F4 is lost from the table — it exists on disk but is no longer referenced. The January data silently disappears.

**Delta Lake's optimistic concurrency control:**
1. Both jobs read the current commit version (e.g., version 5)
2. Job A commits version 6 (adds F4)
3. Job B tries to commit version 6 — Delta Lake detects the conflict (version 6 already exists)
4. Delta Lake checks if Job A's and Job B's writes conflict (do they touch the same partition or files?)
5. If non-conflicting (different partitions): Job B's commit is automatically retried as version 7
6. If conflicting (same partition): Job B fails with a `ConcurrentModificationException` — the user must retry

---

**Q19 — Delta Lake vs. Apache Iceberg**

**Schema evolution:**
- Delta Lake: supports `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN` (with `delta.columnMapping` enabled), type widening; changes tracked in the log
- Iceberg: first-class schema evolution — all changes tracked in a dedicated schema history; hidden partition evolution (change partition strategy without rewriting data)

**Multi-engine support:**
- Delta Lake: native in Databricks/Spark; other engines (Trino, Flink, Hive) require the Delta connector; Databricks Universal Format (UniForm) adds Iceberg compatibility
- Iceberg: designed for multi-engine from the start; natively supported by Spark, Flink, Trino, Presto, Hive, Dremio, DuckDB, BigQuery, Snowflake

**Hidden partitioning:**
- Delta Lake: partitions are explicit — users must write `WHERE order_date = '2024-01-15'` to get pruning; changing partition strategy requires rewriting data
- Iceberg: partition specs stored in metadata; transforms (date truncation, bucketing) applied transparently; users query `WHERE order_date = '2024-01-15'` regardless of physical layout; partition evolution without data rewrite

**Choose Iceberg when:** You have multiple compute engines (Spark + Trino + Flink); you want vendor independence; you need flexible partition evolution; you are on AWS (Athena supports Iceberg natively).

**Choose Delta Lake when:** You are Databricks-centric; simplicity matters more than multi-engine flexibility; you want the richest Databricks integration (Unity Catalog, Auto Loader, Delta Live Tables).

---

**Q20 — Time travel in Delta Lake**

**Time travel** allows querying a Delta table as it existed at any past version or timestamp:
```sql
SELECT * FROM orders VERSION AS OF 5;
SELECT * FROM orders TIMESTAMP AS OF '2024-01-15 09:00:00';
```

**Real-world scenario 1 — Recover from a bad write:**
A dbt model runs and accidentally sets all `status` values to `cancelled`. Time travel lets you immediately query the previous version to verify what data looked like, then restore:
```sql
INSERT OVERWRITE orders
SELECT * FROM orders VERSION AS OF 99;  -- restore to commit 99 before the bad run
```

**Real-world scenario 2 — Audit / regulatory:**
A regulator asks "what was the revenue reported on January 31st?" Even if the table has been updated since then, time travel retrieves the exact state that existed on that date.

**After VACUUM:**
`VACUUM` deletes Parquet files not referenced by any commit within the retention window. Once vacuumed, time travel before the vacuum cutoff is no longer possible — those files no longer exist on disk. The transaction log entries remain, but they reference files that are gone, so reading them fails.

---

## Concept 5: Database Indexing & Query Optimisation

**Q21 — What is a B-tree index and what queries does it accelerate**

A B-tree (Balanced Tree) index is the default index type in PostgreSQL, MySQL, and most relational databases. It stores a sorted copy of the indexed column(s) in a tree structure where each internal node contains keys and pointers to child nodes.

**How it works:**
- The root node contains keys that split the value space into ranges
- Each level narrows the search: a lookup traverses `O(log N)` nodes from root to leaf
- Leaf nodes contain the actual indexed values and a pointer (heap tuple ID / row pointer) to the physical row

**Queries it accelerates:**
- **Equality:** `WHERE status = 'pending'` — tree traversal to the exact value, then follow heap pointer
- **Range:** `WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'` — find start, scan leaves sequentially (leaves are doubly linked)
- **Prefix search:** `WHERE name LIKE 'John%'` — tree narrows to the 'John' prefix range
- **ORDER BY / GROUP BY** on the indexed column — data is pre-sorted, eliminating sort step
- **MIN / MAX** — the leftmost / rightmost leaf is the answer; no full scan

**Queries it does NOT help:**
- `WHERE name LIKE '%John%'` (leading wildcard — no prefix to use)
- `WHERE amount + tax > 100` (function on the column — value is not stored)
- Full-table aggregates with no filter

---

**Q22 — What is a covering index and how does it eliminate heap access**

A **covering index** is an index that contains all columns required by a query — so the database can answer the query entirely from the index without touching the actual table (heap).

**Normal index lookup (two I/Os per row):**
1. Traverse B-tree → find index entry → get heap tuple ID
2. Follow heap tuple ID → read the actual data page in the table

**Covering index (one step, index only):**
1. Traverse B-tree → all required columns are in the index → return directly

```sql
-- Query that reads order_id, status, amount
SELECT order_id, amount FROM orders WHERE status = 'pending';

-- Regular index on (status) -- reads the index, then the heap for each row
CREATE INDEX idx_orders_status ON orders (status);

-- Covering index -- all three columns in the index; heap is never touched
CREATE INDEX idx_orders_status_covering ON orders (status) INCLUDE (order_id, amount);
-- PostgreSQL syntax; MySQL uses: CREATE INDEX ... ON orders (status, order_id, amount)
```

**Trade-offs:**
- Covering indexes are larger (they store more column data)
- They must be updated on every INSERT/UPDATE/DELETE that touches any of the included columns
- Best for high-frequency, read-heavy queries where the heap I/O is the bottleneck

---

**Q23 — Query planner ignores index on `status`, uses Seq Scan instead**

**Root cause: Low cardinality combined with a non-selective filter.**

If `status = 'pending'` matches 60% of the 80 million rows, the query planner calculates:
- Index path: traverse B-tree, follow 48 million heap tuple pointers (random I/O to 48M rows)
- Seq Scan path: read the entire table sequentially (one pass, sequential I/O)

Sequential I/O is ~10–100× faster than random I/O on spinning disks, and even on SSDs the planner's cost model often prefers Seq Scan when >5–15% of rows match — because random heap lookups generate cache misses.

**Fix with a partial index:**

```sql
-- Index only the rows where status = 'pending' (the selective minority)
CREATE INDEX idx_orders_pending ON orders (order_id)
WHERE status = 'pending';
```

Now the index contains only the ~0.5% of rows that are actually `pending` (not `completed` or `cancelled`). The planner sees that the index covers a small fraction → random I/O for a tiny set → index scan wins.

**When the planner statistics are stale:** Run `ANALYZE orders;` to refresh column statistics. A stale `pg_statistic` estimate that says 5% match when really 60% match will cause wrong plan choices.

---

**Q24 — Composite index `(a, b, c)` vs. three separate indexes**

**Composite index `(a, b, c)`:**
- The sort order is: first by `a`, then by `b` within each `a` group, then by `c` within each `(a, b)` group
- The **left-prefix rule**: the index can be used for queries filtering on `a`, `(a, b)`, or `(a, b, c)`. It cannot be used for `b` alone or `c` alone (no leading `a`)
- One index → one B-tree → compact, fast updates

**Three separate indexes on `(a)`, `(b)`, `(c)`:**
- Each query on `a` alone, `b` alone, or `c` alone can use its respective index
- For a query on `WHERE a = 1 AND b = 2`, the planner might use **bitmap index scan** (combine results from both indexes with AND) — but this is slower than a single composite index for the combined predicate

**Column order matters in a composite index:**

```sql
-- Query 1: WHERE user_id = 123 AND event_date = '2024-01-15'
-- Query 2: WHERE user_id = 123 (no date filter)

-- Good order (user_id first):
CREATE INDEX idx_events_uid_date ON events (user_id, event_date);
-- Supports both Query 1 and Query 2 (left-prefix rule)

-- Bad order (event_date first):
CREATE INDEX idx_events_date_uid ON events (event_date, user_id);
-- Supports Query 1 and queries filtering by event_date alone
-- Does NOT support Query 2 (user_id is not a left-prefix here)
```

**Rule:** Put the most selective column that appears in equality filters first. Put range-filter columns (`BETWEEN`, `>`, `<`) last.

---

**Q25 — Indexing strategy for a 500M-row events table with nightly bulk loads**

**Table:** `events(user_id, event_date, event_type, payload, ...)`
- 10M distinct `user_id` values (high cardinality)
- `event_date`: daily, 3 years of history
- Nightly bulk load: 2M rows inserted

**Index design:**

```sql
-- Primary access pattern: user_id + event_date range queries
CREATE INDEX idx_events_uid_date ON events (user_id, event_date);
-- Supports: WHERE user_id = ? AND event_date >= ? AND event_date <= ?
-- user_id first (equality, high cardinality) → event_date second (range)

-- If event_type queries are also frequent:
CREATE INDEX idx_events_uid_type_date ON events (user_id, event_type, event_date);
-- Supports: WHERE user_id = ? AND event_type = 'click' AND event_date >= ?
```

**Handling the nightly bulk load performance impact:**

Indexes slow INSERT performance because each insert must update every index B-tree. For 2M rows, this is significant.

```sql
-- Approach 1: DROP index before bulk load, recreate after (fastest)
DROP INDEX CONCURRENTLY idx_events_uid_date;
-- ... bulk INSERT 2M rows ...
CREATE INDEX CONCURRENTLY idx_events_uid_date ON events (user_id, event_date);
-- CONCURRENTLY avoids table lock; takes longer but doesn't block reads

-- Approach 2: Disable autovacuum + fill factor tuning (PostgreSQL)
ALTER TABLE events SET (autovacuum_enabled = false);
-- bulk load
ALTER TABLE events SET (autovacuum_enabled = true);
ANALYZE events;

-- Approach 3: Partition by event_date (best long-term)
-- Each nightly partition (one day) is a separate table
-- Only the current day's partition has insert pressure; indexes on older partitions are static
```

**Partition-local indexes:** With range partitioning by `event_date`, each partition has its own smaller index. Nightly inserts only update the current partition's index — the 3 years of historical index B-trees are untouched.

---

## Concept 6: Data Catalog & Metadata Management

**Q26 — What is a data catalog?**

A data catalog is a centralised inventory of all data assets — tables, views, files, dashboards, ML models — with their technical, business, and operational metadata.

**Three types of metadata:**

1. **Technical metadata** (auto-captured): schema, column types, location, partition info, row count, file format, last updated
2. **Business metadata** (manually curated): owner, description in plain English, tags (PII, financial, certified), data classification, SLA
3. **Operational metadata** (from pipeline runs): last successful run, average processing time, quality check history, DQ score

---

**Q27 — Hive Metastore vs. Glue Data Catalog vs. Unity Catalog**

**Hive Metastore:**
- Open-source, runs on-premise or on a cluster
- Stores table definitions (schema, location, partitions) for Hive, Spark, Presto
- No governance, no lineage, no access control beyond OS-level
- Use when: on-premise Hadoop/Spark ecosystem

**AWS Glue Data Catalog:**
- Managed, Hive-compatible metastore hosted by AWS
- Native integration with Athena, EMR, Glue ETL, Redshift Spectrum
- Supports schema versioning, crawlers (auto-discover S3 schemas)
- No column-level access control, limited lineage
- Use when: AWS-native stack; Athena is primary query engine

**Databricks Unity Catalog:**
- Lakehouse-native governance layer for Databricks
- Three-level namespace: `catalog.database.table`
- Fine-grained row/column access control, dynamic data masking
- Automatic lineage tracking across notebooks and jobs
- Covers tables, ML models, files, and dashboards
- Use when: Databricks is the primary platform and governance is a priority

---

**Q28 — Three tables named orders, orders_v2, orders_final**

This is a symptom of: no catalog enforcing naming conventions, no retirement process for deprecated tables, and no single owner responsible for a dataset.

**Prevention:**

1. **Naming convention enforcement:** Pipelines write to `catalog.layer.entity` (e.g., `prod.silver.orders`) — no version suffixes allowed. Deprecate by marking in catalog, not by creating a new name.

2. **Ownership enforcement:** Every table must have a declared owner in the catalog. No owner = pipeline blocked from deploying. Owner is responsible for documentation and decommissioning.

3. **Table certification workflow:** Analysts only trust tables marked `certified` in the catalog. Any new table starts as `draft`. Promotion to `certified` requires schema doc, owner, quality check history. `orders_v2` and `orders_final` would never reach `certified` without going through this.

4. **Deprecation + deletion process:** Old tables are marked `deprecated` in the catalog with a sunset date. Automated job deletes them after 30 days. `orders` → deprecated; `orders_silver` → certified replacement.

---

**Q29 — Technical vs. business vs. operational metadata**

**Technical metadata (auto-captured):**
- Column name: `order_amount`, type: `FLOAT`, nullable: false
- Table location: `s3://datalake/silver/orders/`, format: Parquet, size: 45 GB

**Business metadata (manually curated — hardest to maintain):**
- `order_amount`: "The total value of the order in the customer's billing currency, after discounts, before tax"
- Owner: "Data Platform team — contact @data-platform-team Slack channel"

**Operational metadata (from pipeline runs):**
- Last successful run: 2024-01-15 02:14 AM UTC
- Average daily row count: 48,234 (7-day average)

**Hardest to maintain: business metadata.**
- Requires human input — cannot be automated
- Goes stale as business definitions change (what "completed" means changes, nobody updates the catalog)
- No CI/CD checks to catch staleness (unlike schema, which breaks pipelines)
- Often treated as optional by engineering teams under time pressure

---

**Q30 — What dbt contributes to cataloging**

**What dbt auto-generates:**
- Table/column descriptions from `schema.yml` model documentation — published to dbt Docs as a browsable catalog
- Lineage graph: which models depend on which sources and other models (table-level)
- Column-level tests results: `not_null`, `unique`, `accepted_values` — these become data quality indicators in the catalog
- Source freshness checks: last loaded timestamp vs. expected freshness

**What dbt does NOT capture:**
- Business metadata not written in `schema.yml` (most teams only document 20% of columns)
- Operational context: who runs what query against this table, how frequently
- Access control and PII classification (dbt does not manage permissions)
- Cross-system lineage: if a dashboard in Tableau reads from a dbt model, dbt does not know about the dashboard
- ML model lineage: which features were built from which dbt models

---

## Concept 7: Storage Lifecycle

**Q31 — Hot, Warm, Cold tiers**

| Tier | Access latency | Cost (S3) | Contents |
|---|---|---|---|
| Hot | Milliseconds | $0.023/GB | Active Gold + recent Silver; queried daily |
| Warm | Milliseconds (Standard-IA) or minutes (Glacier IR) | $0.0125/GB | Older Silver, Bronze within 1 year; queried monthly |
| Cold | Minutes to hours | $0.004–$0.0036/GB | Bronze archive; compliance; rarely queried |

The trade-off: colder = cheaper storage but higher retrieval cost per query. For data queried daily, the retrieval cost of Standard-IA exceeds the savings — stay in Standard. For data queried monthly, Standard-IA saves 45% with identical query latency.

---

**Q32 — File compaction**

**Compaction** merges many small files into fewer large files without changing the logical content of the table.

**Why it is necessary for frequent small writes:**
- Streaming or hourly jobs write one Parquet file per micro-batch
- After 30 days of hourly writes: 720 files of ~1 MB each (for a 720 MB/day table)
- Query engines open one task per file: 720 tasks with 1 MB each have more scheduling overhead than 6 tasks with 120 MB each
- S3 LIST operations scale with file count

**How to compact:**
```sql
-- Delta Lake
OPTIMIZE silver.orders;

-- With Z-ORDER for co-location
OPTIMIZE silver.orders ZORDER BY (customer_id);
```

**How often:** Weekly for tables receiving hourly writes; monthly for daily-write tables; trigger by file count threshold (e.g., if avg file size < 32 MB, compact).

---

**Q33 — 8,640 files after 30 days of streaming**

1,000 rows every 5 minutes = 12 files/hour = 288 files/day × 30 days = 8,640 files.

**Performance impact:**
- Each query lists 8,640 files: ~9 LIST API calls (1,000 objects per LIST)
- Spark creates 8,640 tasks for a full scan: scheduling overhead dominates for the tiny data per task
- Poor predicate pushdown: Parquet min/max stats are useful only within a file; with 8,640 tiny files, statistics are spread across too many files to be efficient

**Compaction strategy:**
1. **Nightly OPTIMIZE job:** Every night, compact all files from the past 24 hours into files of ~128 MB
2. **Delta Lake auto-compaction (optional):** Enable `delta.autoOptimize.autoCompact = true` — Delta automatically compacts during writes when small files accumulate
3. **Micro-batch sizing:** Increase the streaming trigger interval from 5 minutes to 30 minutes — 6× fewer files per day with the same data volume
4. **`maxRecordsPerFile` parameter:** For non-streaming batch writes, cap the number of records per file to control file size

---

**Q34 — OPTIMIZE does not free disk space**

`OPTIMIZE` rewrites small files into large ones — the new large files are added and the old small files are **tombstoned** (marked removed in the transaction log).

The old small files still exist on S3 until:
```sql
VACUUM silver.orders RETAIN 168 HOURS;  -- 7 days = 168 hours
```

**Why Delta does not delete immediately:**
- Time travel: deleting files immediately would break queries like `VERSION AS OF 5` which reference the old files
- Concurrent readers: a long-running query that started before OPTIMIZE might still be reading the old files
- Safety: if OPTIMIZE produces a corrupt file, you can restore from the original small files within the retention window

**Safe minimum retention:** The default 7 days is recommended for production. Never set to 0 hours in production — any in-flight query reading old files will fail immediately.

---

## Concept 8: OLTP vs. OLAP

**Q35 — OLTP vs. OLAP**

**OLTP (Online Transactional Processing):** Systems that record individual business events in real time — a new order, a login, a payment. Optimised for high-concurrency reads/writes of individual rows. Row-oriented storage, indexed by primary key.

**OLAP (Online Analytical Processing):** Systems that analyse patterns across large volumes of historical data — monthly revenue, product performance, churn rate. Optimised for complex aggregations across millions of rows with few columns read. Column-oriented storage.

**Fundamental difference in query pattern:**
- OLTP: `SELECT * FROM orders WHERE order_id = 1001` → 1 row, all columns, microseconds
- OLAP: `SELECT region, SUM(amount) FROM orders GROUP BY region` → millions of rows, 2 columns, seconds to minutes

---

**Q36 — Why not run analytics directly on PostgreSQL?**

**Performance:** Aggregating millions of rows with GROUP BY requires full table scans. PostgreSQL's row-oriented B-tree index is optimal for single-row lookups, not for sequential column reads. A query taking 1 second on a dedicated OLAP system may take 10 minutes on PostgreSQL.

**Risks beyond performance:**
1. **Lock contention:** Analytical queries hold read locks. During a heavy aggregation, concurrent write transactions (new orders being inserted) may be blocked or slowed.
2. **Connection exhaustion:** Long-running analytics queries hold database connections. PostgreSQL has a limited connection pool. Analysts blocking connections starves the application.
3. **Query resource exhaustion:** A runaway analytical query (accidental cross join, missing WHERE clause) can spike CPU/memory to 100%, making the operational database unresponsive for users.
4. **Vacuum interference:** PostgreSQL's autovacuum runs to reclaim dead tuples. Heavy read workloads can delay vacuum, causing table bloat.

---

**Q37 — MySQL report locking orders table**

**Solution: Read replica + dedicated analytics connection, then migrate to a warehouse.**

**Immediate fix:** Configure a MySQL read replica. Route the sales report query to the replica. Writes go to the primary; the replica handles analytical reads. Locking on the replica does not affect the application.

**Better long-term fix:** Set up a nightly pipeline (Airbyte, Debezium + Kafka) to sync the `orders` table to a data warehouse (Snowflake, BigQuery) or a Parquet table on S3 + Athena. The report runs on the warehouse — zero impact on MySQL.

**Why this works:** The warehouse is a separate system with dedicated compute for analytics. Even a 15-minute full-table scan causes no impact on the production MySQL database.

---

**Q38 — HTAP**

HTAP (Hybrid Transactional/Analytical Processing) databases handle both OLTP and OLAP workloads in a single system.

**Databases claiming HTAP:** TiDB, SingleStore, CockroachDB (limited), SAP HANA, Oracle In-Memory.

**Is it a replacement for separate OLTP/OLAP systems?** No, for most production use cases.
- HTAP systems make trade-offs: they are not as fast as a dedicated OLTP DB for high-concurrency writes, and not as fast as a dedicated OLAP warehouse for complex analytical queries
- They add operational complexity (tuning for both workloads)

**When HTAP makes sense:**
- Real-time analytics on live operational data (e.g., fraud detection that must query recent transactions during processing)
- Operational reporting that needs sub-minute freshness (cannot wait for a nightly ETL)
- Small-to-medium data volumes where operational + analytical loads do not conflict

---

## Concept 9: Data Replication & Consistency Models

**Q39 — What is replication lag and why does it matter for pipelines reading from a read replica**

**Replication lag** is the delay between a write being committed on the primary database and that write becoming visible on a read replica. It exists because replicas apply changes asynchronously — the primary does not wait for replicas to confirm before acknowledging the write to the application.

**Why it matters for data pipelines:**

A pipeline that reads from a replica using an incremental watermark (`WHERE updated_at > :last_run`) can silently miss records if the lag exceeds the pipeline's run window:

```
Timeline:
  14:00:00  Record updated on primary
  14:00:10  Pipeline runs, queries replica: "give me rows updated after 13:59:00"
  14:00:10  Replica is 30 seconds behind → sees rows only up to 13:59:40
  14:00:10  The 14:00:00 update is NOT YET on the replica → missed
  14:00:30  Replica finally applies the update — but pipeline already moved watermark
  → Record is permanently skipped
```

**Fixes:**
1. Add a **lag buffer** to the watermark: `WHERE updated_at > :last_run AND updated_at < NOW() - INTERVAL '60 seconds'`
2. Read from the **primary** for critical pipelines (at the cost of added load)
3. Monitor `pg_stat_replication.write_lag` / `replay_lag` and alert when lag exceeds threshold

---

**Q40 — CAP theorem: why you cannot have all three properties simultaneously**

The CAP theorem states that a distributed system can guarantee at most two of:
- **C**onsistency — every read returns the most recent write (or an error)
- **A**vailability — every request receives a response (no errors, possibly stale)
- **P**artition tolerance — the system continues operating when network partitions split nodes

**Why all three are impossible:** When a network partition occurs, nodes on either side cannot communicate. To remain Available, each partition must serve responses — but without coordination they may serve stale data, violating Consistency. To remain Consistent, a partitioned node must refuse to serve stale data — but then it is not Available.

Since network partitions are inevitable in any distributed system, the real trade-off is **CP vs AP**:

| | CP example | AP example |
|---|---|---|
| Database | PostgreSQL (primary stops accepting writes if it loses quorum) | Cassandra (every node accepts writes; eventual consistency) |
| Data engineering | HBase (ZooKeeper-coordinated, refuses reads during partition) | DynamoDB (serves stale reads, resolves conflicts later) |
| Message queue | Kafka with `min.insync.replicas=2` (write fails if replicas unavailable) | Kafka with `acks=1` (primary acknowledges, replica may lag) |

**Practical data engineering choice:** Most data lakes and analytics systems choose **AP** — serving a slightly stale dashboard is acceptable; returning an error is not. Transactional systems (payments, inventory) choose **CP** — stale reads cause real-world harm.

---

**Q41 — Replica lag scenario: is the 14:00:00 update captured?**

**Setup:** Read replica, 30-second average lag. Pipeline runs at 14:00:10. Watermark: `WHERE updated_at > :last_run`.

**Answer: No, the update is NOT captured.**

- Record updated on primary at 14:00:00
- Pipeline runs at 14:00:10 → queries replica
- Replica is 30 seconds behind → has applied changes up to ~13:59:40
- The 14:00:00 update does not yet exist on the replica
- Pipeline moves its watermark to 14:00:10
- At 14:00:30 the replica applies the update — but the watermark has already passed → permanently missed

**Fix — lag-aware watermark with buffer:**

```sql
-- Instead of:
WHERE updated_at > :last_run_timestamp

-- Use:
WHERE updated_at > :last_run_timestamp
  AND updated_at < NOW() - INTERVAL '60 seconds'
  -- "don't capture anything from the last 60 seconds — let replication catch up"
```

This creates a 60-second "closed window": the pipeline only captures records that are old enough to have replicated. The next pipeline run will pick up the records that were too recent in the previous run.

**Monitoring the lag:**
```sql
-- PostgreSQL: check current replica lag
SELECT client_addr,
       write_lag,
       flush_lag,
       replay_lag
FROM pg_stat_replication;
-- Alert if replay_lag > 30 seconds before pipeline runs
```

---

**Q42 — ACID vs. BASE: where each model appears in a data platform**

| Property | ACID | BASE |
|---|---|---|
| Full form | Atomicity, Consistency, Isolation, Durability | Basically Available, Soft state, Eventually consistent |
| Guarantee | Every transaction is complete and consistent or fully rolled back | System is available; data may be stale; will converge to consistency eventually |
| Trade-off | Lower throughput, higher latency (locking, coordination) | Higher throughput, lower latency (no cross-node coordination) |

**Where ACID appears in a data platform:**

- **Source OLTP database** (PostgreSQL, MySQL): the system of record — every order, payment, or user action must be fully atomic and durable
- **Delta Lake / Iceberg / Hudi** on the data lake: these table formats add ACID transactions on top of object storage — critical for ensuring a failed write does not leave a corrupted table visible to readers

**Where BASE appears in a data platform:**

- **Message queues** (Kafka): at-least-once delivery — a message may be delivered twice; consumers must handle duplicates (idempotent writes)
- **DynamoDB / Cassandra** as a serving store: eventual consistency is acceptable for leaderboard reads, feature store lookups, and recommendation serving where sub-millisecond latency matters more than perfect freshness
- **S3 Bronze layer** (historically): before December 2020, S3 was eventually consistent — a file might not be immediately visible after a PUT; pipelines had to handle this with retries

**The practical rule for data engineers:** Use ACID for writes that must not be partially applied (Silver/Gold table writes via Delta Lake MERGE). Accept BASE semantics for high-throughput message consumption, and design consumers to be idempotent.

---

**Q43 — S3 eventual consistency before December 2020 and the impact on Delta Lake / Iceberg**

**The historical problem:**

Before December 2020, S3 was eventually consistent for LIST operations and overwrites:
- You write `part-001.parquet` to S3
- A LIST call immediately after might return an empty result (the object had not yet propagated through S3's metadata layer)
- A GET on a newly overwritten object might return the old version for a few seconds

**Why this was dangerous for table formats:**

Delta Lake and Iceberg work by writing new data files and then atomically updating the transaction log / metadata file to point to those files. If S3 did not immediately return the new data file in a LIST response, a reader that validated the file list would see a file referenced by the log but not returned by LIST — causing a read error or silent data skip.

**How Delta Lake worked around eventual consistency (pre-2020):**
1. **Log-based file tracking:** Delta Lake never relied on S3 LIST to discover files — the `_delta_log` was the authoritative file list. Readers only listed the log directory, not the data directory.
2. **Write ordering:** Data files were written first and fully durable before the log commit was written. Even if a reader briefly could not LIST the data file, the log commit guaranteeing its existence would not yet be visible either.
3. **Retry with backoff:** The Delta Lake client retried LIST and GET operations with exponential backoff to handle transient inconsistencies.

**After December 2020 (strong consistency for S3):**
- Every PUT, DELETE, and LIST is immediately consistent — a file is visible in LIST as soon as the PUT returns success
- Delta Lake and Iceberg no longer need eventual-consistency workarounds for S3
- This enabled simpler implementations and removed an entire class of subtle read-after-write bugs

---

## Concept 10: Storage Performance Optimisation

**Q44 — Z-ordering in Delta Lake**

Z-ordering (also called Z-order curve or multi-dimensional clustering) physically re-sorts and co-locates rows in Parquet files so that rows with similar values in the Z-ordered columns are in the same or adjacent files.

```sql
OPTIMIZE orders ZORDER BY (customer_id, order_date);
```

**How it improves performance:**
- After Z-ordering by `customer_id`, all rows for `customer_id = 'C001'` are in the same Parquet file (or adjacent files)
- The file's min/max statistics for `customer_id` become tightly bounded: min=C001, max=C001 for a file containing only C001 data
- Queries `WHERE customer_id = 'C001'` skip all other files via predicate pushdown

**vs. standard partitioning:**
- Partitioning by `customer_id` on a high-cardinality column (millions of customers) creates millions of directories (small files problem)
- Z-ordering on `customer_id` achieves similar locality without creating per-customer directories — the number of files stays manageable

---

**Q45 — Bloom filters**

A **bloom filter** is a probabilistic data structure that answers: "Is this value in this file?"
- **Answer: No** → value is **definitely not** in the file (skip it safely)
- **Answer: Yes** → value is **probably** in the file (read it to confirm — may be a false positive)

**What queries it accelerates:** High-cardinality equality lookups:
```sql
WHERE order_id = 1001     -- needle in a haystack among millions of order_ids
WHERE transaction_id = 'TXN-123456'
WHERE user_uuid = 'abc-def-...'
```
Without a bloom filter, the engine reads every Parquet file and scans for the value. With a bloom filter per file, files that definitely do not contain the value are skipped.

**Limitations:**
- False positives: the engine may read a file that does not contain the value (extra I/O, but no incorrect results)
- Space cost: bloom filters take disk space proportional to the number of distinct values
- Only helps for equality predicates (`=`) — not ranges (`>`, `BETWEEN`)

---

**Q46 — Table partitioned by order_date, frequently queried by customer_id**

The query `WHERE customer_id = 'C001'` scans all partitions because `customer_id` is not the partition column. Options:

**Option 1 — Z-ORDER on customer_id:**
```sql
OPTIMIZE silver.orders ZORDER BY (customer_id);
```
Physically co-locates rows by customer_id within each date partition. Combined with min/max statistics, the engine skips files whose customer_id range does not include 'C001'.

**Option 2 — Bloom filter on customer_id:**
```sql
ALTER TABLE silver.orders SET TBLPROPERTIES (
  'delta.bloomFilter.columns' = 'customer_id'
);
```
Each file records which customer_ids it contains. Files that definitely lack 'C001' are skipped.

**Option 3 — Add customer_id as a second partition:**
Only if cardinality is manageable (<10,000 distinct customers). For millions of customers, this creates the small files problem.

**Option 4 — Pre-compute a customer-oriented Gold table:**
If all analyst queries are by customer, build `gold.customer_orders` partitioned by `customer_id`. Move the expensive computation to the pipeline rather than repeating it at query time.

---

**Q47 — Parquet internal structure**

```
Parquet File
├── Row Group 1 (128 MB default)
│   ├── Column Chunk: order_id
│   │   ├── Page 1 (1 MB, dictionary-encoded)
│   │   └── Page 2 (1 MB)
│   ├── Column Chunk: amount
│   │   └── Page 1 (delta-encoded)
│   └── ... (one column chunk per column)
├── Row Group 2
│   └── ...
└── Footer
    ├── Schema (column names, types)
    ├── Row group statistics (min/max, null count per column per row group)
    └── Column chunk offsets (byte position of each column chunk)
```

**How each level contributes:**

- **Footer (read first):** Contains row group statistics. The engine reads only the footer to decide which row groups to skip (predicate pushdown). For a 1 GB file with 8 row groups: if 6 are skipped, only 25% of the file is read.

- **Row group:** Unit of parallel read. Spark assigns one row group per task. Larger row groups = better compression (more repetition visible), fewer tasks. Smaller = more parallelism, less memory per task.

- **Column chunk:** All values of one column within one row group, stored contiguously. Reading one column requires only seeking to the column chunk offset — skipping other columns entirely.

- **Page (smallest unit):** Within a column chunk, data is split into pages (~1 MB). Each page can have its own encoding (dictionary, RLE, plain). Pages enable fine-grained decompression — decompress only pages that contain matches.

---

**Q48 — 1 TB table in 45 min; 3 GB daily partition in 8 min — slower per GB**

**Expected:** 3 GB / 1 TB = 0.3% of data → should take 0.3% of 45 min = 8 seconds. Actual: 8 minutes = 53× slower per GB than the full table.

**Likely causes:**

1. **Too many small files in the daily partition:** If the daily partition contains 1,000 files of 3 MB each, Spark creates 1,000 tasks. Task scheduling overhead (say 100ms per task) = 100 seconds of overhead before any data is read. The full table query was batched across larger files.

2. **Partition discovery overhead:** Spark's driver must list all files in the partition before assigning tasks. If the partition directory has many files, listing itself takes time.

3. **Data skew:** If the 3 GB is unevenly distributed (one file has 2.5 GB, the rest have 500 MB total), one task processes most of the data while others finish quickly — the stage is bottlenecked by one task.

**Diagnosis:**
- Check Spark UI: task duration distribution (skew?), task count vs. file count
- Check file sizes: `DESCRIBE DETAIL silver.orders PARTITION (order_date='2024-01-15')`
- If files are small: run `OPTIMIZE ZORDER BY (customer_id)` to compact

---

## Mixed / Senior-Level Questions

**Q49 — Always use Parquet vs. CSV/JSON**

**Disagree with the absolute claim.** Parquet is the right default for analytical tables in a data lake, but CSV and JSON have legitimate use cases:

**Use CSV when:**
- Exchanging data with external parties (finance teams, regulators, clients) who cannot read Parquet
- Human inspection is required — Parquet cannot be opened in Excel
- Very small files (< 1 MB) where Parquet overhead is disproportionate

**Use JSON when:**
- Source data is inherently semi-structured with variable schema (clickstream events, API responses with optional fields)
- Streaming into a Kafka topic (Avro is better but JSON is universally understood)
- Landing zone / Bronze layer where you want to preserve the raw API response exactly

**The rule:** Store raw data in its native format in Bronze. Convert to Parquet in Silver. Never store analytical Silver/Gold data as CSV or JSON — the query performance cost is not justified.

---

**Q50 — Schema-on-read technical debt at 2 years / hundreds of tables**

**Technical debt that accumulates:**
- No one knows the schema of legacy tables without reading the raw files and inferring
- Type mismatches: one CSV has `amount` as `12.5`, another from the same source has `amount` as `"12.5"` (string) — queries silently produce nulls or errors
- Column name drift: `customer_id`, `cust_id`, `CustomerID` — same concept, different names across 100 tables
- Stale inferred schemas: Glue Crawler runs monthly; new columns added in week 2 are invisible until the next crawl

**Remediation strategy:**
1. **Schema-first backfill:** Run a schema audit across all tables. Write explicit schema definitions (Avro schemas, `CREATE TABLE` DDL, dbt models with `schema.yml`). Store in version control.
2. **Enforce at write time going forward:** Add schema validation to all new pipelines. Use `mergeSchema = false` in Delta Lake to reject writes that deviate from the declared schema.
3. **Convert legacy tables to Delta Lake:** Migrating from unmanaged Parquet to Delta Lake automatically enforces the schema going forward.
4. **Catalog documentation sprint:** Prioritise the top 20 most-used tables; write business metadata before expanding to all 200.

---

**Q51 — Migrating on-premise Hive/ORC/HDFS to cloud lakehouse**

**Migration strategy:**

**Phase 1 — Evaluate:** Inventory all Hive tables: size, last query date, business criticality. Archive tables not queried in 2 years (cold migration only).

**Phase 2 — Convert format:** ORC → Parquet (columnar to columnar; good compression preservation)
```python
# Spark job to convert ORC to Parquet + Delta Lake
spark.read.orc("hdfs:///data/orders/").write.format("delta").save("s3://datalake/silver/orders/")
```

**Phase 3 — Validate:** Row count match, column value distributions, sample row comparison between HDFS ORC and S3 Parquet.

**Phase 4 — Migrate metadata:** Create Glue/Unity Catalog table definitions pointing to the new S3 locations.

**5-year historical data approach:**
- Do not migrate all 5 years upfront — expensive and risky
- Migrate the last 2 years at full fidelity (actively queried)
- Archive years 2–5 to Glacier in ORC format (or convert to Parquet before archiving)
- Provide a self-service restore process for historical data requests

---

**Q52 — 12 schema versions of raw JSON in Bronze**

**The problem:** Each event contains a schema version indicator (or implicit version based on the fields present). The Silver transformation must handle all 12 versions.

**Approach — Schema normalisation in Silver:**

1. **Version detection:** Read `schema_version` field (if present) or infer from field presence:
```python
def detect_version(event: dict) -> int:
    if "discount_pct" in event:  # added in v5
        if "loyalty_points" in event:  # added in v9
            return 12
        return 5
    return 1
```

2. **Per-version normalisation:** Map each version's fields to the canonical Silver schema:
```python
NORMALIZERS = {
    1: normalise_v1,
    5: normalise_v5,
    ...
    12: normalise_v12,
}
```

3. **Unknown version handling:** Route to dead-letter queue with the raw event for manual inspection.

4. **Canonical Silver schema:** One authoritative schema for all versions post-normalisation. Columns that did not exist in early versions are `NULL` for those records.

**Long-term:** Enforce Avro/Protobuf with a Schema Registry in the mobile app going forward to prevent version 13, 14, 15 from creating this problem again.

---

**Q53 — Real-time ride-sharing analytics storage architecture**

**Requirements:** Active trips (sub-second), revenue last hour (sub-second), trips updated within 30–60 min.

**Architecture:**

```
[Trip events from drivers/riders] → Kafka topic (trip-events)
              ↓                                   ↓
    [Flink streaming job]              [Kafka Streams: active-trips view]
    MERGE into Delta Lake trips         materialised in Redis / ClickHouse
    (Silver, MERGE on trip_id)
              ↓
    [dbt Gold models: hourly revenue]
    (refresh every 1 minute)
              ↓
    [ClickHouse / Apache Druid]
    (OLAP engine optimised for sub-second analytics)
```

**Key storage decisions:**
- **Active trips (sub-second latency):** Redis hash map `{trip_id: status, driver_id, ...}` — updated by Flink on every trip event; queried at millisecond latency. Not a data lake — it is an operational store for live data.
- **Historical trips (Silver):** Delta Lake with MERGE on `trip_id` — handles the 30–60 minute update window; Flink upserts as status changes arrive.
- **Revenue last hour (sub-second):** Pre-aggregated by a 1-minute micro-batch job into ClickHouse (columnar OLAP, purpose-built for sub-second analytics on large time-series). Do not query Delta Lake for this — too slow.

---

**Q54 — GDPR deletion across 5 systems**

**Processing order: most atomic/raw first.**

1. **Raw JSON files (Bronze, object storage):**
   - Hardest if unmanaged. If files are partitioned by date, identify all daily partitions containing the user, rewrite each file without their records. With Delta Lake on Bronze: `DELETE FROM bronze.events WHERE user_id = X` + VACUUM.
   - Challenge: files may be in Glacier (retrieval latency). Request retrieval before the 72-hour SLA starts.

2. **Parquet Silver tables (Delta Lake):**
   - `DELETE FROM silver.users WHERE user_id = X`
   - Delta rewrites affected Parquet files, tombstones originals
   - Run VACUUM to physically delete (ensure GDPR guarantee)

3. **ML feature store (Parquet):**
   - Same as Silver: DELETE + VACUUM
   - Challenge: features may be computed from multiple users' data (aggregated features). If a feature is purely per-user, delete it. If it is a cross-user aggregate (e.g., "average order value in user's neighbourhood"), the user's contribution is baked in — document that aggregates cannot be surgically removed (GDPR allows retention of anonymised statistical data)

4. **Gold aggregate tables:**
   - If aggregated by non-user dimensions: cannot delete one user's contribution without recomputing the aggregate — acceptable under GDPR (aggregate is not personal data)
   - If row-level user data: DELETE + VACUUM

5. **Elasticsearch:**
   - REST API: `DELETE /users/_doc/{user_id}`
   - Verify deletion: `GET /users/_doc/{user_id}` → 404
   - Force segment merge to prevent soft-delete recovery: `POST /users/_forcemerge?only_expunge_deletes=true`

**Hardest system:** Bronze raw JSON files without a table format — requires file-level rewriting with no atomic operation guarantee.

---

**Q55 — TCO comparison: Snowflake vs. pure data lake vs. lakehouse**

**Snowflake wins when:**
- Small-to-medium data volumes (< 10 TB)
- SQL-only workloads, no Spark/ML
- Small data team without infrastructure expertise
- Time-to-value matters more than cost optimisation
- Storage at Snowflake rates (~$46/TB/month) is acceptable

**Pure data lake (Parquet + S3 + Spark) wins when:**
- Very large data volumes (100+ TB) where storage cost dominates (S3 = $23/TB/month)
- Heavy ML and Python workloads
- Multi-engine access already required
- Willingness to manage infrastructure (Spark clusters, orchestration)
- Governance is not yet a priority

**Lakehouse (Delta Lake + S3 + Databricks) wins when:**
- Large data volumes where S3 cost savings matter
- Need for both SQL analytics AND Spark/ML
- ACID reliability required (finance, healthcare)
- GDPR / data deletion requirements (table format makes row-level delete manageable)
- Cost: S3 storage + Databricks compute (DBUs) < Snowflake at scale; breakeven typically around 20–50 TB

**Total cost consideration often missed:** Operational costs. Snowflake requires almost no infrastructure management. A Databricks lakehouse requires tuning cluster sizes, managing Delta table maintenance (VACUUM, OPTIMIZE), and setting up observability. At small scale, Snowflake's higher storage cost is outweighed by lower engineering overhead.
