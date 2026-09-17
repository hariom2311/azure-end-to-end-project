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

## Concept 2: Data Compression Deep Dive

### Why compress?

- **Storage cost:** 1 TB of raw CSV at $0.023/GB/month = $23.55/month. At 5× compression: $4.71/month
- **Query speed:** Less data to read from disk = faster queries (I/O is usually the bottleneck, not CPU)
- **Network transfer:** Smaller files transfer faster between storage and compute

### Lossless vs. lossy compression

Data engineering exclusively uses **lossless** compression — the original data is perfectly reconstructed. Lossy compression (JPEG, MP3) is for media and has no place in data pipelines.

### How compression works on columnar data

Columnar storage exposes structure that compressors exploit:

**Run-length encoding (RLE):** For repeated values in a column:
```
status column: [completed, completed, completed, pending, pending]
RLE encoded:   [(completed, 3), (pending, 2)]
```
A status column with 10 possible values in 1 billion rows compresses dramatically.

**Dictionary encoding:** Replace repeated string values with integer codes:
```
status column raw:    [completed, pending, completed, cancelled, completed]
dictionary:           {0: completed, 1: pending, 2: cancelled}
encoded:              [0, 1, 0, 2, 0]
```
Integers compress far better than strings and compare faster.

**Delta encoding:** For monotonically increasing values (timestamps, IDs):
```
order_id raw:    [1001, 1002, 1003, 1004, 1005]
delta encoded:   [1001, +1, +1, +1, +1]
```
Deltas are small integers — highly compressible.

### Compression in practice

- **Parquet + Snappy**: default for most data lake Silver/Gold tables — good balance
- **Parquet + Zstd (level 3)**: modern best practice — better compression, similar read speed
- **Parquet + Gzip**: archive/cold storage where query frequency is low
- **Avro + Snappy**: streaming/Kafka pipelines where schema evolution is needed

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

## Concept 5: Object Storage & Storage Tiers

### What is object storage?

**Object storage** (S3, GCS, ADLS Gen2, Azure Blob) stores data as flat objects with a unique key (path), rather than a hierarchical file system. It has no concept of directories — what looks like `s3://bucket/orders/2024/01/15/file.parquet` is just a string key.

**Properties:**
- **Infinite scale:** No capacity limits; storage grows automatically
- **Cheap:** ~$0.023/GB/month (S3 Standard) vs. $0.115+/GB for SSD-backed databases
- **High durability:** 99.999999999% (11 nines) — data replicated across multiple availability zones
- **Eventual consistency (historically):** S3 became strongly consistent in December 2020 — reads now always reflect the latest write
- **Immutable objects:** You cannot edit a file in-place; you must write a new object (this is why table formats maintain a transaction log)

### The object storage cost model

Unlike databases, object storage charges for:
- **Storage:** per GB stored per month
- **API requests:** per PUT, GET, LIST operation (small per-call, but adds up with millions of files)
- **Data transfer (egress):** data leaving the cloud region

This cost model is why the **small files problem** matters even beyond query performance — millions of LIST and GET calls to read millions of tiny files cost money.

### Storage tiers

Cloud providers offer multiple storage classes with different cost/latency trade-offs:

| Tier | Access latency | Cost (S3) | Minimum storage duration | Use case |
|---|---|---|---|---|
| Standard | Milliseconds | $0.023/GB | None | Active data queried frequently |
| Standard-IA (Infrequent Access) | Milliseconds | $0.0125/GB | 30 days | Data queried monthly (compliance, old Silver) |
| Glacier Instant Retrieval | Milliseconds | $0.004/GB | 90 days | Archive data still needing occasional fast access |
| Glacier Flexible Retrieval | Minutes–hours | $0.0036/GB | 90 days | Long-term archive, batch retrieval acceptable |
| Glacier Deep Archive | Hours | $0.00099/GB | 180 days | Regulatory 7-year retention — almost never queried |

**Lifecycle policies** automatically move data between tiers:
```
Raw/Bronze: Standard → Standard-IA after 30 days → Glacier after 90 days
Silver: Standard → Standard-IA after 90 days
Gold: Standard (always active)
```

### Object storage vs. HDFS

Before cloud object storage, Hadoop HDFS (Hadoop Distributed File System) was the standard storage layer for big data. Key differences:

| | HDFS | Object Storage (S3/GCS/ADLS) |
|---|---|---|
| Deployment | Self-managed cluster | Fully managed cloud service |
| Cost | High (servers + ops) | Low (pay per GB) |
| Scalability | Manual (add nodes) | Automatic |
| Compute/storage coupling | Tightly coupled (data on compute nodes) | Fully decoupled |
| Durability | 3× replication | 11 nines |
| Consistency | Strong | Strong (S3 since Dec 2020) |

HDFS is still used in on-premise Hadoop clusters, but new cloud-native architectures exclusively use object storage.

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

## Concept 9: Serialisation Formats — Avro & Protobuf for Streaming

Analytical file formats (Parquet, ORC) are optimised for batch reads. Streaming pipelines need different formats — ones that are fast to serialise/deserialise and carry schema information so that producers and consumers can evolve independently.

### Apache Avro

Avro is a **row-oriented, binary serialisation format** designed for schema evolution in event-driven systems.

**Why Avro for streaming:**
- Schema is embedded in the message header (or in a registry): consumers always know how to parse
- Binary encoding: compact and fast (no field names repeated per record, unlike JSON)
- Rich schema evolution support: add fields with defaults, remove unused fields, rename with aliases

**Avro schema (JSON-defined):**
```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "order_id",   "type": "int"},
    {"name": "customer_id","type": "string"},
    {"name": "amount",     "type": "double"},
    {"name": "status",     "type": "string"},
    {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "discount",   "type": ["null", "double"], "default": null}
  ]
}
```

The `discount` field has type `["null", "double"]` with a null default — adding this field is backward compatible because old readers that do not know about `discount` will use the default.

### Protocol Buffers (Protobuf)

Protobuf is Google's binary serialisation format. More compact than Avro, preferred in high-throughput systems.

```proto
message Order {
  int32  order_id    = 1;
  string customer_id = 2;
  double amount      = 3;
  string status      = 4;
  int64  created_at  = 5;
  optional double discount = 6;
}
```

**Avro vs. Protobuf:**

| | Avro | Protobuf |
|---|---|---|
| Schema language | JSON | Proto IDL |
| Schema evolution | Excellent (named fields + defaults) | Excellent (field numbers stable) |
| Compression | Good | Better (more compact binary) |
| Ecosystem | Kafka-native | gRPC, microservices |
| Self-describing | Yes (schema in file) | No (need .proto file separately) |
| Use in Kafka | Very common | Common (requires Schema Registry) |

### When to use which format

| Scenario | Format |
|---|---|
| Kafka topics / event streaming | Avro (with Schema Registry) |
| REST API response stored to lake | JSON → convert to Parquet in Silver |
| Large analytical table in data lake | Parquet |
| High-throughput microservice messages | Protobuf |
| Simple data exchange with external parties | CSV (universal) or JSON |
| Hive-heavy on-premise analytics | ORC |

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
| Compression | Columnar compression (RLE, dict encoding) gives 5–10× reduction; Snappy/Zstd for Parquet |
| DW vs. Lake vs. Lakehouse | Evolution: structured+expensive → flexible+unreliable → open+reliable |
| Open Table Formats | Delta/Iceberg/Hudi add ACID, time travel, and updates on top of Parquet |
| Object Storage | Cheap, infinite, durable; immutable objects require table format layer for mutations |
| Data Catalog | Central inventory of schema, ownership, lineage, and quality metadata |
| Storage Lifecycle | Hot→Warm→Cold tiers; lifecycle policies automate transitions; compaction keeps files healthy |
| OLTP vs. OLAP | Row-oriented for transactions; columnar for analytics; separate systems by design |
| Avro & Protobuf | Binary serialisation for streaming; schema evolution; works with Schema Registry |
| Storage Performance | Predicate pushdown, Z-ordering, bloom filters, file sizing, caching |

---

## Further Reading

- [Apache Parquet Documentation](https://parquet.apache.org/documentation/latest/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [AWS S3 Storage Classes](https://aws.amazon.com/s3/storage-classes/)
- [Designing Data-Intensive Applications — Martin Kleppmann, Ch. 3 (Storage Engines)](https://dataintensive.net/)
- [The Delta Lake Paper (VLDB 2020)](https://databricks.com/wp-content/uploads/2020/08/p975-armbrust.pdf)
