# Day 2 — Data Storage, File Formats & the Lakehouse Architecture

## Overview

Before a single transformation runs, data must live somewhere — and the choice of where and how to store it determines query speed, cost, scalability, and the kind of analytics that are even possible. Day 2 covers the storage layer from first principles: file formats, storage systems, the evolution from data warehouses to data lakes to lakehouses, and the table formats that make modern data platforms possible. These concepts underpin every architectural decision you will make as a data engineer.

---

## Concept 1: File Formats — CSV, JSON, Parquet, ORC, Avro

Not all files are equal. The format you choose for storing data determines how fast it can be read, how much disk it occupies, and which tools can work with it.

### Row-oriented vs. column-oriented storage

**Row-oriented (CSV, JSON, Avro):** All columns of one row are stored together on disk.
```
Row 1: [order_id=1001, customer_id=C001, amount=120.50, status=completed]
Row 2: [order_id=1002, customer_id=C002, amount=45.00, status=pending]
```
Good for: writing new records (one sequential write per row), reading entire rows (OLTP).  
Bad for: reading one column across millions of rows (must read and skip all other columns).

**Column-oriented (Parquet, ORC):** All values of one column are stored together on disk.
```
order_id column: [1001, 1002, 1003, ...]
amount column:   [120.50, 45.00, 310.00, ...]
status column:   [completed, pending, completed, ...]
```
Good for: analytical queries that aggregate one or a few columns across all rows (`SUM(amount)`).  
Bad for: writing individual rows (requires updating multiple column files).

### Format comparison

| Format | Orientation | Compression | Schema | Splittable | Best for |
|---|---|---|---|---|---|
| CSV | Row | None (plain text) | Inferred | Yes (by line) | Simple exchange, small files, human-readable |
| JSON | Row | None (plain text) | Inferred | Yes (by line) | APIs, semi-structured / nested data |
| Avro | Row | Snappy/Deflate | Embedded in file | Yes | Kafka messages, schema evolution, streaming |
| Parquet | Column | Snappy/Zstd/Gzip | Embedded in file | Yes | Analytical queries, data lakes, Silver/Gold layers |
| ORC | Column | Zlib/Snappy | Embedded in file | Yes | Hive workloads, high compression requirements |

### Why Parquet dominates modern data lakes

1. **Columnar reads:** `SELECT SUM(amount) FROM orders` reads only the `amount` column — skips all others
2. **Compression:** Similar columns compress better together. Numeric columns with repeated values compress 5–10×
3. **Predicate pushdown:** Parquet stores min/max statistics per row group. If `amount` max in a row group is 50 and your query filters `WHERE amount > 100`, the entire row group is skipped without decompression
4. **Schema embedded:** No need for a separate schema file — the Parquet file itself carries the schema
5. **Splittable:** Large Parquet files can be split across multiple workers in Spark/Hive

### Compression codecs

| Codec | Compression ratio | Speed | Use case |
|---|---|---|---|
| Snappy | Medium (2–3×) | Very fast | Default for Parquet — balance of speed and size |
| Gzip | High (4–6×) | Slow | Cold storage where read speed is not critical |
| Zstd | High (4–5×) | Fast | Modern default — better than Snappy at similar speed |
| LZ4 | Low (1.5–2×) | Fastest | Real-time / streaming where CPU is the bottleneck |
| Uncompressed | None | N/A | Development only |

### Splittability matters for distributed processing

A single 100 GB Gzip-compressed CSV cannot be split — all 100 GB must be sent to one worker. A 100 GB Parquet file is made of many row groups (each typically 128 MB), each independently readable — Spark assigns one row group per task, fully parallelising the read.

---

## Concept 2: Change Data Capture (CDC)

### What is CDC?

**Change Data Capture (CDC)** is the practice of identifying and capturing every INSERT, UPDATE, and DELETE made to a source database, then delivering those changes to downstream systems in near-real time.

Without CDC, a pipeline must either:
- **Full load:** Read the entire source table every run (expensive at scale)
- **Timestamp-based incremental:** Read rows where `updated_at > last_watermark` (misses hard deletes, vulnerable to clock skew)

CDC solves both problems by tapping directly into the database's internal change log.

### Why CDC matters

| Problem with alternatives | CDC solution |
|---|---|
| Full load is too slow at >10M rows | CDC captures only changed rows — minimal volume |
| Timestamp watermarks miss hard DELETEs | CDC captures DELETE events explicitly |
| Clock skew causes missed updates | CDC reads commit order from the DB log — authoritative |
| Schema changes break extract queries | CDC captures structural changes too |

### How log-based CDC works

Every production-grade relational database maintains a **write-ahead log (WAL)** — a sequential record of every change committed to the database, used for crash recovery and replication.

CDC tools read this log instead of querying the database tables:

```
[Application writes to PostgreSQL]
          ↓
[PostgreSQL WAL (Write-Ahead Log)]  ← CDC reads here
          ↓
[CDC tool: Debezium / Fivetran / AWS DMS]
          ↓
[Kafka topic: orders-cdc-events]
          ↓
[Spark / Flink reads Kafka → MERGE into Silver table]
```

Each WAL event contains:
- Operation type: `INSERT`, `UPDATE`, `DELETE`
- Table name
- Before image (old row values, for UPDATE and DELETE)
- After image (new row values, for INSERT and UPDATE)
- Commit timestamp and log sequence number (LSN)

### CDC event structure

```json
{
  "op": "u",
  "ts_ms": 1705276800000,
  "before": {"order_id": 1001, "status": "pending", "amount": 120.50},
  "after":  {"order_id": 1001, "status": "completed", "amount": 120.50},
  "source": {"table": "orders", "lsn": 123456789, "db": "prod"}
}
```

Operation codes: `c` = create (INSERT), `u` = update (UPDATE), `d` = delete (DELETE), `r` = read (snapshot).

### Applying CDC events downstream: the MERGE pattern

```sql
-- For each CDC event arriving in the target:
MERGE INTO silver.orders AS target
USING cdc_staging AS source ON target.order_id = source.order_id
WHEN MATCHED AND source.op = 'd' THEN DELETE
WHEN MATCHED AND source.op = 'u' THEN UPDATE SET target.status = source.status, ...
WHEN NOT MATCHED AND source.op = 'c' THEN INSERT VALUES (source.order_id, ...)
```

This keeps the Silver table as a current-state mirror of the source database — every row reflects the latest version.

### CDC tools

| Tool | Type | Source DBs | Target |
|---|---|---|---|
| Debezium | Open-source | PostgreSQL, MySQL, Oracle, MongoDB | Kafka |
| Fivetran | Managed SaaS | 300+ sources | Warehouse / lake |
| AWS DMS | Managed | Most relational DBs | S3, Redshift, RDS |
| Airbyte | Open-source / Cloud | 300+ sources | S3, warehouse |
| Qlik Replicate | Enterprise | Oracle, SAP, DB2 | Warehouse |

### CDC vs. timestamp-based incremental

| Dimension | Timestamp incremental | Log-based CDC |
|---|---|---|
| Hard DELETE visibility | No (row is gone) | Yes (DELETE event captured) |
| Clock skew risk | Yes (late updates missed) | No (log sequence is authoritative) |
| Source DB load | Queries the DB (read impact) | Reads the log (near-zero impact) |
| Setup complexity | Low (just add WHERE clause) | Medium (requires log access, Debezium setup) |
| Latency | Minutes (batch interval) | Sub-second |

### The initial snapshot

When CDC is first set up, you need to backfill all existing data before the log begins. This is the **initial snapshot** (or full-load phase):
1. Take a consistent snapshot of the source table at log position N
2. Load the snapshot to the target
3. Begin consuming CDC events from position N onwards

Debezium handles this automatically in **snapshot mode** before switching to streaming.

---

## Concept 3: Data Warehouses vs. Data Lakes vs. Lakehouses

The architecture of the storage tier has evolved significantly over 30 years. Understanding why each generation emerged explains every architectural decision in modern data platforms.

### Generation 1: Data Warehouse

A **data warehouse** is a structured, purpose-built database for analytical (OLAP) queries.

```
[Operational DBs] → ETL → [Data Warehouse (Redshift, Snowflake, BigQuery)]
                                    ↓
                             [BI Tools / Reports]
```

**Characteristics:**
- Highly structured — data must conform to a predefined schema before loading
- SQL-native — all transformations and queries use SQL
- Expensive compute and storage — tightly coupled (you pay for both even when idle)
- Excellent query performance on structured data
- Poor support for unstructured data (images, logs, PDFs, JSON blobs)

**When warehouses struggled:**
- Machine learning teams need raw data, not curated warehouse tables
- Semi-structured data (JSON, XML) is hard to query in relational schemas
- Storage at petabyte scale is prohibitively expensive in a warehouse
- You must decide the schema before you know how data will be used

### Generation 2: Data Lake

A **data lake** is a flat repository of raw data in any format, stored cheaply in object storage (S3, GCS, ADLS).

```
[Any source] → [Raw dump: S3/GCS/ADLS in any format: CSV, JSON, Parquet, images]
                              ↓
                   [Spark / Hive / Presto queries raw files]
```

**Characteristics:**
- Schema-on-read — store any data, figure out the schema when you query
- Cheap object storage (S3 is ~23× cheaper than warehouse storage)
- Supports all data types (structured, semi-structured, unstructured)
- Decoupled compute and storage — pay only for the compute you use

**Where data lakes failed:**
- No ACID transactions — two writers can corrupt a file; partial writes leave the table in a bad state
- No updates or deletes — object storage is immutable; changing a row means rewriting the whole file
- No schema enforcement — "data swamp": data lands with no structure, nobody knows what is there
- Slow on small files — millions of tiny files kills query performance
- No query optimisation — no statistics, no indexes, full scans only

### Generation 3: Lakehouse

A **lakehouse** combines the cheap open storage of a data lake with the reliability and performance guarantees of a data warehouse.

```
[Any source] → [Raw data in open format (Parquet) on object storage]
                              ↓
                   [Table format layer: Delta Lake / Iceberg / Hudi]
                   (ACID transactions, schema enforcement, time travel)
                              ↓
               [Compute engines: Spark, Trino, Flink, DuckDB, native warehouse]
```

**What the table format layer adds on top of plain Parquet:**
- **ACID transactions:** Multiple writers cannot corrupt a table
- **Schema enforcement:** Bad data is rejected at write time
- **Update and delete support:** Modify individual rows in Parquet files
- **Time travel:** Query the table as it was at any past timestamp
- **Z-ordering / clustering:** Physical data layout optimisation without partitioning
- **Statistics and bloom filters:** Faster predicate pushdown than plain Parquet

### Comparison

| Dimension | Data Warehouse | Data Lake | Lakehouse |
|---|---|---|---|
| Storage format | Proprietary | Any (CSV, JSON, Parquet) | Open (Parquet + table format) |
| Storage cost | High | Low | Low |
| ACID transactions | Yes | No | Yes |
| Schema enforcement | Yes | No | Optional (enforced or flexible) |
| Unstructured data | No | Yes | Yes |
| Update / delete | Yes | No | Yes |
| Query performance | Excellent | Poor–Medium | Good–Excellent |
| Vendor lock-in | High | Low | Low |
| ML / data science | Hard | Easy | Easy |

---

## Concept 4: Open Table Formats — Delta Lake, Apache Iceberg, Apache Hudi

Plain Parquet files on object storage have no concept of transactions, versioning, or updates. Open table formats add a **metadata layer** on top of Parquet to solve these problems. This is the core innovation of the lakehouse.

### The metadata layer problem

When you have 10,000 Parquet files representing a table:
- How do you know which files are part of the current version of the table?
- If two writers add files simultaneously, how do you prevent conflicts?
- If you delete a row, how do you reflect that without rewriting every file?
- How do you roll back a bad write?

The answer: a **transaction log** that tracks every change to the table.

### Delta Lake

Created by Databricks (open-sourced 2019). The most widely used table format in the Databricks ecosystem.

**Transaction log (`_delta_log/`):**
```
table/
├── _delta_log/
│   ├── 00000000000000000000.json  ← commit 0: initial table creation
│   ├── 00000000000000000001.json  ← commit 1: INSERT 1000 rows
│   ├── 00000000000000000002.json  ← commit 2: UPDATE status='cancelled' WHERE ...
│   └── 00000000000000000010.checkpoint.parquet  ← snapshot at commit 10
├── part-0001.parquet
├── part-0002.parquet
└── ...
```

Each JSON entry in `_delta_log` records:
- Which files were **added** (new data)
- Which files were **removed** (deleted or replaced by updates)
- Schema at this commit
- Operation metadata (who wrote, when, how many rows)

**Key features:**
- **Time travel:** `SELECT * FROM orders VERSION AS OF 5` or `TIMESTAMP AS OF '2024-01-15'`
- **MERGE (upsert):** `MERGE INTO target USING source ON key WHEN MATCHED THEN UPDATE ...`
- **Schema evolution:** `ALTER TABLE ADD COLUMN`, `mergeSchema` on write
- **OPTIMIZE:** Compact small files into larger ones
- **VACUUM:** Remove old files no longer referenced by the log (default: 7 day retention)
- **Z-ORDER:** Re-sort data physically by a column to improve predicate pushdown

### Apache Iceberg

Created at Netflix, now an Apache top-level project. Designed for multi-engine interoperability.

**Key differences from Delta Lake:**
- **Snapshot-based** rather than log-based: each write creates a new snapshot pointing to a set of data files
- **Hidden partitioning:** Iceberg tracks partition transforms in metadata — you query without knowing the physical partition layout; Iceberg routes to the right files automatically
- **Row-level deletes:** Iceberg can mark individual rows as deleted without rewriting the data file (using delete files) — crucial for GDPR deletions at scale
- **Multi-engine:** Works natively with Spark, Flink, Trino, Presto, Hive, Dremio, DuckDB without per-engine connectors

### Apache Hudi

Created at Uber. Optimised for streaming upserts at high frequency.

**Key differentiator:** Designed for near-real-time ingestion from CDC (change data capture) streams where records are frequently updated.

**Two table types:**
- **Copy-on-Write (COW):** Rewrites Parquet files on every update — reads are fast, writes are slow
- **Merge-on-Read (MOR):** Writes delta files alongside base Parquet; merges on read — writes are fast, reads are slower until compaction

### Which to choose?

| Use case | Recommended format |
|---|---|
| Databricks-centric platform | Delta Lake |
| Multi-engine (Spark + Trino + Flink) | Apache Iceberg |
| High-frequency CDC upserts (streaming) | Apache Hudi |
| AWS Glue / Athena primary | Apache Iceberg (AWS native) |
| Azure Synapse + Databricks | Delta Lake |

---

## Concept 5: Database Indexing & Query Optimisation

### What is an index?

An **index** is a separate data structure maintained alongside a table that allows the database to locate rows matching a condition without scanning the entire table.

Without an index: find all orders for `customer_id = 'C001'` in a 50M-row table → full table scan → 50M rows read.  
With an index on `customer_id`: → B-tree lookup → 3–4 node reads → directly to matching rows.

Indexes are the single most impactful performance tool in relational databases and appear in almost every senior data engineering interview.

### B-Tree index — the default

Most database indexes are **B-trees** (Balanced Trees). A B-tree maintains sorted keys in a tree structure where every leaf is at the same depth.

```
                    [1000 | 2000]
                   /      |      \
            [500|750]  [1200|1500]  [2200|2500]
           /    |    \
    [300..499][500..749][750..999]   ← leaf pages (actual row pointers)
```

**Queries a B-tree accelerates:**
- Equality: `WHERE customer_id = 'C001'` → O(log n)
- Range: `WHERE amount BETWEEN 100 AND 500` → traverse from 100, scan to 500
- Sort: `ORDER BY customer_id` → already sorted in the B-tree

**Queries a B-tree does NOT help with:**
- Prefix-unanchored wildcard: `WHERE name LIKE '%smith'` → full scan (no leading anchor)
- Inequality on non-indexed column: `WHERE amount > 100` if `amount` is not indexed

### Composite indexes and column order

A composite index covers multiple columns. **Column order is critical** — the index is useful only for queries that provide a leading prefix of the index columns.

```sql
-- Index: (order_date, customer_id, status)

-- Uses index fully:
WHERE order_date = '2024-01-15' AND customer_id = 'C001' AND status = 'completed'

-- Uses index partially (leading prefix):
WHERE order_date = '2024-01-15'
WHERE order_date = '2024-01-15' AND customer_id = 'C001'

-- Does NOT use index (missing leading column):
WHERE customer_id = 'C001'
WHERE status = 'completed'
```

**Rule:** Order composite index columns by selectivity (most selective first) and by query pattern (most common filter first).

### Covering index

A **covering index** includes all columns a query needs — the database engine never touches the actual table rows.

```sql
-- Query:
SELECT customer_id, order_date, amount FROM orders WHERE customer_id = 'C001';

-- Regular index on customer_id:
-- 1. B-tree lookup → row pointers for C001 → 2. fetch each row from heap → 3. return 3 columns
-- Two I/O operations per row

-- Covering index on (customer_id, order_date, amount):
-- 1. B-tree lookup → return all 3 columns directly from the index
-- One I/O operation total — heap never touched
```

Covering indexes are extremely effective for high-frequency read queries (reporting, dashboards).

### Index types beyond B-Tree

| Index type | Best for | Example |
|---|---|---|
| B-Tree | Equality, range, sort | `WHERE order_date BETWEEN x AND y` |
| Hash | Equality only — faster than B-tree for `=` | `WHERE session_id = 'abc123'` |
| GIN (Generalized Inverted Index) | Full-text search, JSONB, arrays | `WHERE tags @> ARRAY['electronics']` |
| BRIN (Block Range Index) | Very large, naturally ordered tables (timestamps, IDs) | `WHERE created_at > '2024-01-01'` on an append-only log table |
| Partial index | Subset of rows | `WHERE status = 'pending'` — index only pending orders |

**Partial index example:**
```sql
-- Only 2% of orders are 'pending' — index just those
CREATE INDEX idx_orders_pending ON orders (created_at)
WHERE status = 'pending';
-- Tiny index, fast lookups for pending-order queries
```

### Query execution plans

The **query planner** (EXPLAIN / EXPLAIN ANALYZE in PostgreSQL) chooses between index scan, index-only scan, and sequential scan based on statistics (row count estimates, column cardinality).

```sql
EXPLAIN ANALYZE
SELECT * FROM orders WHERE customer_id = 'C001';

-- Output:
-- Index Scan using idx_orders_customer_id on orders
--   (cost=0.43..8.45 rows=12 width=64) (actual time=0.032..0.051 rows=12 loops=1)
--   Index Cond: (customer_id = 'C001')
-- Planning Time: 0.2 ms
-- Execution Time: 0.1 ms
```

A **Seq Scan** (sequential scan) on a large table is almost always a sign of a missing or unused index.

### When indexes hurt

- **Write-heavy tables:** Every INSERT/UPDATE/DELETE must also update all indexes → indexes slow down writes
- **Bulk loads:** Disable indexes before bulk insert, rebuild after (much faster than per-row updates)
- **Too many indexes:** Each index uses disk space and write overhead; a table with 15 indexes loads data 15× slower than a table with 1
- **Low selectivity columns:** An index on a `boolean` column (`is_active`: 95% true, 5% false) is useless — the planner prefers a seq scan because most rows match anyway

### Statistics and the query planner

The query planner uses **statistics** (collected by `ANALYZE` in PostgreSQL, `DBMS_STATS` in Oracle) to estimate row counts and choose the best plan:
- Table row count
- Column cardinality (number of distinct values)
- Column histogram (value distribution)
- Null fraction

Stale statistics → bad estimates → wrong query plan → slow queries. Run `ANALYZE` after bulk loads or major data changes.

---

## Concept 6: Data Catalog & Metadata Management

### What is a data catalog?

A **data catalog** is a centralised inventory of all data assets in an organisation — what datasets exist, where they are stored, what their schemas are, who owns them, and how they are used.

Without a catalog, data engineers spend hours searching for the right table, questioning whether it is the authoritative source, and not knowing who to contact when it breaks.

### What a catalog stores

**Technical metadata** (automatically captured):
- Table name, location (S3 path, database)
- Schema: column names, types, descriptions
- Partition information
- Row count, size, last updated timestamp
- Lineage: which tables this one depends on

**Business metadata** (manually curated):
- Owner (team or person responsible)
- Description in plain English
- Tags: `pii`, `financial`, `sensitive`, `raw`, `certified`
- Data classification: public / internal / confidential / restricted
- SLA: expected freshness and availability

**Operational metadata** (from pipeline runs):
- Last successful pipeline run
- Average row count trend
- Quality check results history

### Common catalog tools

| Tool | Type | Notes |
|---|---|---|
| Apache Atlas | Open-source | Integrates with Hadoop/Hive ecosystem |
| AWS Glue Data Catalog | Managed | Native to AWS; integrates with Athena, EMR, Glue |
| Azure Purview / Microsoft Purview | Managed | Native to Azure; scans ADLS, Synapse, SQL |
| Databricks Unity Catalog | Managed | Lakehouse-native; covers Delta tables, ML models |
| dbt | Open-source | Documents models and lineage; integrates with BI tools |
| DataHub (LinkedIn) | Open-source | Federated; pulls metadata from many systems |
| Alation / Collibra | Commercial | Enterprise-grade with governance workflows |

### The Glue Data Catalog (AWS context)

The AWS Glue Data Catalog is a **Hive-compatible metastore** — it stores database and table definitions that Athena, Spark on EMR, Glue ETL jobs, and Redshift Spectrum all read from the same source.

```
S3://datalake/orders/order_date=2024-01-15/part-001.parquet
       ↓
Glue Crawler scans S3 and infers schema
       ↓
Glue Data Catalog: database=silver, table=orders, columns=[order_id INT, ...]
       ↓
Athena: SELECT * FROM silver.orders WHERE order_date='2024-01-15'
   (Athena reads catalog for schema → reads S3 for data)
```

### Unity Catalog (Databricks context)

Unity Catalog is Databricks' governance layer that unifies access control, lineage, and metadata across a Databricks workspace.

Three-level namespace: `catalog.database.table`
```sql
SELECT * FROM prod_catalog.silver.orders
```

Provides:
- Fine-grained row and column-level access control
- Automatic lineage tracking (which notebook or job wrote to this table)
- Data classification tags propagated automatically
- Cross-workspace sharing of tables without copying data

---

## Concept 7: Storage Layout Patterns — Hot, Warm, Cold & Data Lifecycle

### The data lifecycle

Data loses query frequency over time. Orders placed today are queried constantly; orders from 3 years ago are queried only for annual reports. Storage architecture should reflect this access pattern.

```
[Ingest] → Hot storage (fast, expensive) → Warm storage → Cold archive (slow, cheap)
             (Active Silver/Gold)          (Older Silver)  (Raw/Bronze archive)
```

### Hot / Warm / Cold tiers in practice

**Hot tier (active data):**
- Object storage Standard class or SSD-backed database
- Contains: Gold tables, recent Silver (last 90 days)
- Query latency: milliseconds
- Typical retention: 90 days in hot, then move

**Warm tier (recent but less active):**
- Object storage Infrequent-Access class
- Contains: older Silver tables, Bronze (last 1 year)
- Query latency: milliseconds (same as Standard, but charged per retrieval)
- Typical retention: 1–3 years

**Cold tier (archive):**
- Object storage Glacier / Archive class
- Contains: Bronze raw data older than 1 year, compliance records
- Query latency: minutes to hours
- Typical retention: 7 years (regulatory) or indefinite

### Lifecycle policy example (S3)

```json
{
  "Rules": [
    {
      "Prefix": "raw/",
      "Transitions": [
        { "Days": 30,  "StorageClass": "STANDARD_IA" },
        { "Days": 90,  "StorageClass": "GLACIER_IR" },
        { "Days": 365, "StorageClass": "DEEP_ARCHIVE" }
      ]
    },
    {
      "Prefix": "gold/",
      "Transitions": [
        { "Days": 90, "StorageClass": "STANDARD_IA" }
      ]
    }
  ]
}
```

### The Bronze / Silver / Gold retention pattern

| Layer | Retention | Storage class | Why |
|---|---|---|---|
| Bronze (raw) | 7 years | Standard → IA → Glacier over time | Regulatory; source of truth for reprocessing |
| Silver (cleansed) | 2–3 years | Standard → IA | Debugging, trend analysis |
| Gold (curated) | 1–2 years (or indefinite for aggregates) | Standard | Dashboard and BI queries are always on recent data |

### Compaction as a lifecycle step

As data ages in Bronze/Silver, small files accumulate from frequent incremental writes. A periodic **compaction** job rewrites many small files into fewer large ones:

```python
# Delta Lake compaction
spark.sql("OPTIMIZE orders_silver")

# Optionally Z-ORDER by a frequently filtered column
spark.sql("OPTIMIZE orders_silver ZORDER BY (customer_id)")
```

Run compaction weekly or monthly on tables that receive frequent small writes (streaming or hourly incremental loads). This keeps query performance high without changing the data.

---

## Concept 8: OLTP vs. OLAP Storage Systems

### The fundamental distinction

| | OLTP (Online Transaction Processing) | OLAP (Online Analytical Processing) |
|---|---|---|
| Purpose | Record individual business events | Analyse patterns across many events |
| Query pattern | Read/write single rows by primary key | Aggregate millions of rows across few columns |
| Typical query | `SELECT * FROM orders WHERE order_id = 1001` | `SELECT region, SUM(amount) FROM orders GROUP BY region` |
| Optimisation | Row-oriented, indexed by primary key | Column-oriented, partition pruning |
| Database type | PostgreSQL, MySQL, Oracle, SQL Server | Snowflake, BigQuery, Redshift, Spark |
| Data volume | Gigabytes | Terabytes to Petabytes |
| Concurrency | Thousands of short transactions per second | Tens of long-running queries |
| Write pattern | Frequent individual inserts/updates | Bulk loads (less frequent) |

### Why you cannot use the same database for both

An OLTP database optimised for single-row lookups (indexed B-tree) performs poorly when aggregating 10 million rows — it must read the entire row to extract one column. An OLAP database stores data in column files — adding a new order requires writing to multiple column files (slow for single inserts).

This is why operational databases (OLTP) feed into analytical systems (OLAP) through data pipelines. The two systems are separate by design.

### Hybrid approaches

**HTAP (Hybrid Transactional/Analytical Processing):** Some modern databases (TiDB, SingleStore, CockroachDB) claim to handle both. In practice, they make trade-offs — neither as fast as a dedicated OLTP or OLAP system — and are used for special cases (real-time analytics on live operational data) rather than replacing either tier.

---

## Concept 9: Data Replication & Consistency Models

### Why replication exists

**Replication** is the practice of keeping copies of the same data on multiple nodes or systems. Replication serves three purposes:
1. **High availability:** If one node fails, another has the data
2. **Read scalability:** Multiple replicas serve read queries in parallel (read replicas)
3. **Geographic distribution:** Replicas in different regions reduce latency for global users

Every data pipeline that reads from a replica (a very common pattern — never query the primary operational DB directly) is affected by replication behaviour.

### Replication lag

When a write commits on the **primary**, it is not instantly visible on **replicas** — there is a delay while the change is transmitted and applied. This delay is **replication lag**.

```
[User places order on Primary] → order_id=1051 committed
         ↓  (replication lag: 200ms–2s under normal load, minutes under heavy load)
[Read Replica] → order_id=1051 not yet visible

[Pipeline reads from replica] → misses order_id=1051
→ order appears "lost" until lag closes
```

**Consequence for pipelines:** A pipeline reading from a replica with a timestamp watermark can miss recently inserted rows that have not yet replicated. Common fix: add a `lookback_seconds` buffer to the watermark.

### Consistency models

**Strong consistency (linearisability):** Every read reflects the most recent write, regardless of which node serves it. Every node has the same view at the same moment.
- Example: Read from primary only; block reads until replication completes
- Trade-off: Higher latency; primary becomes bottleneck for reads

**Eventual consistency:** After a write, all replicas will eventually converge to the same value — but there is a window where replicas may return stale data.
- Example: Read from replica; replica may lag by milliseconds to seconds
- Trade-off: Lower latency; reads may be stale

**Read-your-own-writes consistency:** A user always sees their own writes, even if other users may see stale data. Implemented by routing a user's reads to the primary (or to the specific replica that received their write) for a short window.

**Monotonic read consistency:** A user never reads data older than what they previously read. If you read version N, subsequent reads return version ≥ N.

### CAP theorem

The **CAP theorem** states that a distributed system can guarantee at most two of three properties simultaneously:

| Property | Meaning |
|---|---|
| **C**onsistency | Every read returns the most recent write (strong consistency) |
| **A**vailability | Every request receives a response (no timeout, even if stale) |
| **P**artition tolerance | The system continues operating even when network partitions (nodes cannot reach each other) occur |

**Network partitions always happen** in distributed systems (cables fail, switches drop packets). Therefore, real systems must choose between CP or AP:

- **CP (Consistency + Partition tolerance):** When partitioned, refuse requests rather than return stale data. Example: HBase, ZooKeeper, etcd — used for coordination where correctness is critical.
- **AP (Availability + Partition tolerance):** When partitioned, continue serving requests with potentially stale data. Example: Cassandra, DynamoDB — used for high-throughput user-facing data where availability matters more than perfect consistency.

**CAP in practice:** Most modern databases are "CA during normal operation, CP or AP during partition" — the partition case is rare but must be explicitly designed for.

### ACID vs. BASE

**ACID** (traditional relational databases):
- **A**tomicity: transactions complete fully or not at all
- **C**onsistency: every transaction takes the database from one valid state to another
- **I**solation: concurrent transactions do not interfere
- **D**urability: committed transactions survive crashes

**BASE** (distributed NoSQL systems, eventual consistency):
- **B**asically **A**vailable: the system is available most of the time
- **S**oft state: the state may change over time without new input (replicas converging)
- **E**ventually consistent: the system will eventually converge to a consistent state

**Relevance to data engineering:**
- Source OLTP databases are typically ACID → reliable for CDC
- Distributed message queues (Kafka) are AP → at-least-once delivery, must handle duplicates
- Data lakes (S3 + table formats) add ACID semantics on top of eventually-consistent object storage

### Replication topologies

**Single-leader (primary-replica):** All writes go to one primary; replicas receive copies. Simple; primary is a bottleneck.

**Multi-leader:** Multiple primaries accept writes; they synchronise with each other. Used for multi-region writes; conflict resolution is complex.

**Leaderless (Dynamo-style):** Any node accepts writes; reads check a quorum of nodes. Used in Cassandra, DynamoDB. Write to W nodes, read from R nodes — if W + R > N (total nodes), reads always see the latest write.

### Practical implications for data engineers

1. **Never query the primary database directly from pipelines** — use read replicas to avoid impacting application performance
2. **Account for replication lag in watermarks** — add a lag buffer (e.g., process data older than 60 seconds to ensure replication has completed)
3. **Understand your source's consistency model** — Kafka delivers at-least-once by default; your MERGE must be idempotent
4. **Eventual consistency in S3** — before Dec 2020, S3 LIST could miss recently written objects; today S3 is strongly consistent, but this is worth verifying when reading from third-party object stores

---

## Concept 10: Storage Performance Optimisation

### The read path in a data lake

When a query engine (Spark, Athena, Trino) reads from a data lake:
```
1. Parse SQL → identify table → look up metadata (catalog)
2. List files in table location (S3 LIST calls)
3. Apply partition pruning → eliminate directories
4. Read file footers → apply predicate pushdown (skip row groups)
5. Decompress and decode remaining data
6. Apply remaining filters in memory
7. Aggregate / join / return results
```

Every optimisation in this section targets one or more of these steps.

### Predicate pushdown

Predicate pushdown is the engine's ability to use metadata inside a file to skip reading parts of it.

**Parquet row group statistics:**
Each Parquet file is divided into row groups (default 128 MB). Each row group stores:
- Min and max value for every column
- Null count per column
- (Optionally) Bloom filter for high-cardinality columns

```
Row group 1: amount min=10.00, max=99.99
Row group 2: amount min=100.00, max=499.99
Row group 3: amount min=500.00, max=999.99

Query: WHERE amount > 200
→ Skip row group 1 entirely (max=99.99 < 200)
→ Read row groups 2 and 3 only
```

### Z-Ordering (Delta Lake) / Sorting

Z-ordering physically co-locates rows with similar values in the same files. After `OPTIMIZE ... ZORDER BY (customer_id)`:
- Rows for `customer_id = C001` are in the same or adjacent files
- A query `WHERE customer_id = 'C001'` reads far fewer files
- Min/max statistics on `customer_id` per file become tightly bounded → better predicate pushdown

### Bloom filters

A bloom filter is a probabilistic data structure that answers "is this value in this file?" with:
- **No → definitely not in this file** (skip it)
- **Yes → probably in this file** (read it to confirm)

Useful for high-cardinality equality filters: `WHERE order_id = 1001` on a table with millions of distinct order IDs.

```sql
-- Delta Lake: add bloom filter on order_id column
ALTER TABLE orders SET TBLPROPERTIES (
  'delta.dataSkippingNumIndexedCols' = '5',
  'delta.bloomFilter.columns' = 'order_id'
);
```

### File sizing targets

| File too small | File too large |
|---|---|
| High metadata overhead | Cannot be parallelised within one file |
| Many S3 LIST/GET API calls | One slow task becomes a bottleneck |
| Poor compression ratio | Must decompress more than needed for a predicate |
| **Target:** 128 MB – 1 GB per file | **Target:** 128 MB – 1 GB per file |

### Caching

**Disk cache (SSD):** Databricks Delta Cache / Spark's disk cache stores decompressed Parquet data on local SSDs attached to compute nodes. Repeat reads of the same data skip S3 entirely. Effective for dashboards with repeated queries.

**Result cache:** Some warehouses (Snowflake, BigQuery) cache the result of a query. If the same query is run again and the underlying data has not changed, the cached result is returned instantly.

---

## Summary

| Concept | One-line summary |
|---|---|
| File Formats | Parquet (columnar) for analytics; Avro for streaming; CSV/JSON for exchange |
| Change Data Capture | Log-based CDC captures every INSERT/UPDATE/DELETE from source DB in near-real time |
| DW vs. Lake vs. Lakehouse | Evolution: structured+expensive → flexible+unreliable → open+reliable |
| Open Table Formats | Delta/Iceberg/Hudi add ACID, time travel, and updates on top of Parquet |
| Database Indexing | B-Tree, composite, covering, partial indexes; EXPLAIN plans; statistics |
| Data Catalog | Central inventory of schema, ownership, lineage, and quality metadata |
| Storage Lifecycle | Hot→Warm→Cold tiers; lifecycle policies automate transitions; compaction keeps files healthy |
| OLTP vs. OLAP | Row-oriented for transactions; columnar for analytics; separate systems by design |
| Replication & Consistency | CAP theorem, ACID vs BASE, replication lag, eventual vs strong consistency |
| Storage Performance | Predicate pushdown, Z-ordering, bloom filters, file sizing, caching |

---

## Further Reading

- [Apache Parquet Documentation](https://parquet.apache.org/documentation/latest/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [AWS S3 Storage Classes](https://aws.amazon.com/s3/storage-classes/)
- [Designing Data-Intensive Applications — Martin Kleppmann, Ch. 3 (Storage Engines)](https://dataintensive.net/)
- [The Delta Lake Paper (VLDB 2020)](https://databricks.com/wp-content/uploads/2020/08/p975-armbrust.pdf)
