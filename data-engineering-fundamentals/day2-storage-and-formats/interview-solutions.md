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

## Concept 2: Data Compression

**Q6 — Why columnar storage compresses better**

In row-oriented storage, adjacent bytes on disk alternate between different columns with different types and distributions:
```
Row 1: [1001, C001, 120.50, completed]
Row 2: [1002, C002, 45.00,  pending]
```
The byte stream alternates between integers, strings, floats, strings — no pattern for a compressor to exploit.

In columnar storage, adjacent bytes are all the same type and often similar values:
```
status column: completed, completed, pending, completed, completed, completed, cancelled...
```
Run-length encoding (RLE) replaces `completed, completed, completed, completed, completed` with `(completed, 5)`. The entire column collapses dramatically.

An `order_status` column with 5 possible values across 1 billion rows might compress 50:1 with dictionary + RLE. The equivalent row in a row-oriented file cannot be compressed that way because adjacent bytes belong to different columns.

---

**Q7 — Snappy vs. Gzip vs. Zstd**

| Codec | Compression ratio | Speed | Best use |
|---|---|---|---|
| Snappy | ~2.5× | Very fast (multi-GB/s) | Default for active Parquet tables — balance of speed and size |
| Gzip | ~4–6× | Slow (100–300 MB/s) | Cold storage, external delivery where size matters more than query speed |
| Zstd | ~4–5× | Fast (500 MB/s–1 GB/s, tunable) | Modern best practice — level 3 matches Gzip ratio at near-Snappy speed |

**Rule of thumb:** Use Zstd for new tables. Use Snappy if your toolchain does not yet support Zstd. Use Gzip only for archival/exchange files that are rarely queried.

---

**Q8 — Spark job using only 1 executor with Gzip CSV**

**Root cause:** Gzip is not splittable. Spark cannot divide the file into blocks for parallel processing. Regardless of the number of executors requested, the entire file must be read by a single task on a single executor.

**Fix:** Decompress and convert to Parquet:
```python
df = spark.read.csv("s3://bucket/data.csv.gz", header=True, inferSchema=True)
df.repartition(200).write.parquet("s3://bucket/data_parquet/")
```

Now the Parquet files are independently readable, Spark assigns one file (or row group) per task, and all executors are used.

**Alternative (if you must keep compression):** Use bzip2 — it is splittable (each bzip2 stream within the file is independent). But Parquet+Snappy is still superior for analytics.

---

**Q9 — Gzip (18 GB) vs. Snappy (32 GB) Parquet — which is faster to query?**

**It depends on the I/O vs. CPU trade-off — but for most cloud workloads, Snappy is faster.**

Query: reads only the `status` column.

- **Gzip (18 GB on disk):** Less data to transfer from S3. But Gzip decompression is CPU-intensive and single-threaded per block. After reading the status column chunk (much smaller than 18 GB), you still pay the decompression CPU cost.

- **Snappy (32 GB on disk):** More data to transfer from S3. But Snappy decompression is extremely fast (several GB/s) and parallelises well. The column read is fast end-to-end.

**In cloud environments:** S3 → compute network bandwidth is typically 10–25 Gbps, and compute CPU is plentiful. The bottleneck is usually decompression CPU, not bandwidth. Snappy wins because its fast decompression more than compensates for slightly more data transferred.

**File size on disk does not directly determine query speed** — you must also account for decompression overhead, parallelism, and whether predicate pushdown eliminates whole row groups before any decompression occurs.

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

## Concept 5: Object Storage & Storage Tiers

**Q21 — Object storage vs. traditional file system**

**Traditional file system (POSIX):** Hierarchical directories, in-place file modification, strong consistency, limited to one server (or distributed via NFS). Storage and compute are on the same machine.

**Object storage (S3/GCS/ADLS):**
- Flat namespace: objects are identified by a string key (path is just part of the key)
- Immutable objects: cannot edit in-place; must write a new object with the same key (replaces the old)
- Infinite horizontal scale: no capacity limits
- Globally accessible via HTTP/HTTPS
- Decoupled from compute: Spark running anywhere can read S3
- 99.999999999% durability (11 nines)

**Why it is the data lake foundation:**
- Virtually unlimited capacity at low cost
- Decoupled compute: pay for storage and compute independently
- Any compute engine (Spark, Athena, Trino) can read the same data
- Managed service: no infrastructure to operate

---

**Q22 — S3 storage classes and lifecycle policy for 7-year raw data retention**

| Class | Cost/GB/month | Retrieval latency | Minimum duration |
|---|---|---|---|
| Standard | $0.023 | Milliseconds | None |
| Standard-IA | $0.0125 | Milliseconds | 30 days |
| Glacier Instant Retrieval | $0.004 | Milliseconds | 90 days |
| Glacier Flexible Retrieval | $0.0036 | Minutes–hours | 90 days |
| Deep Archive | $0.00099 | Hours | 180 days |

**Lifecycle policy for raw data (active first 30 days, retained 7 years):**
```
Day 0:   Standard          (active querying during first 30 days)
Day 31:  → Standard-IA     (occasional debugging, millisecond access still needed)
Day 91:  → Glacier IR      (very rare access, still need fast retrieval for compliance)
Day 366: → Glacier Flexible (annual compliance reviews only; hours acceptable)
Day 730: → Deep Archive    (7-year legal hold; almost never accessed)
```

Enable **S3 Object Lock** in compliance mode to prevent deletion before the 7-year mark.

---

**Q23 — 5 million small files, 2-hour Spark job for 50 GB**

**Root cause: The small files problem — metadata overhead dominates compute time.**

With 5 million files at 10 KB each:
1. Spark's driver must **LIST** all files → millions of S3 API calls → minutes just to discover what to read
2. Each Spark task reads one file (10 KB). Task overhead (scheduler, serialisation, task launch) takes ~100ms per task. With 5 million tasks: 5M × 100ms = 139 hours of task overhead (not parallelised — only N executors run at once)
3. After listing, the driver holds metadata for 5M files in memory → potential OOM on the driver

**Options:**
1. **Compact immediately:** Run a one-time Spark job to merge small files into 128 MB–1 GB Parquet files
2. **Use Delta Lake OPTIMIZE:** `OPTIMIZE table_name` rewrites small files into target file sizes
3. **Fix the root cause at write time:** If the small files are produced by a streaming job, configure the writer to write larger files (micro-batch with larger trigger interval, `maxRecordsPerFile` setting)
4. **Use a table format with file tracking:** Delta Lake / Iceberg only LIST the metadata (transaction log), not all S3 files — dramatically faster file discovery

---

**Q24 — S3 API cost with 10 million files**

**Calculation:**
- 100 queries/day, each LIST-ing 10 million files
- LIST returns 1,000 objects per request → 10,000 LIST calls per query
- 100 queries × 10,000 LIST calls = 1,000,000 LIST calls/day
- S3 LIST cost: $0.005 per 1,000 calls
- Daily cost: 1,000,000 / 1,000 × $0.005 = $5/day
- Monthly: ~$150/month just from LIST calls on 10M files

**Plus GET costs:** each query reads many files → millions of GET calls/day.

**How partitioning reduces it:**
- With daily partitioning: each query lists only the relevant partition directory (e.g., one day = 10,000 files instead of 10M)
- LIST calls per query: 10,000/1,000 = 10 LIST calls vs. 10,000
- 1,000× reduction in LIST API costs

**How table formats reduce it further:**
- Delta Lake / Iceberg read a transaction log (a few small files) to discover which data files belong to the table
- No need to LIST S3 at all for file discovery — the log is authoritative
- Only one GET per log file, not one GET per data file

---

**Q25 — Object storage layout for 1 TB/day platform**

```
s3://company-datalake/
├── bronze/
│   ├── orders/
│   │   └── year=2024/month=01/day=15/
│   │       └── part-001.json.gz
│   ├── customers/
│   └── payments/
├── silver/
│   ├── orders/
│   │   └── order_date=2024-01-15/
│   │       └── part-001.parquet
│   └── customers/
│       └── country=AU/year=2024/month=01/
│           └── part-001.parquet
└── gold/
    ├── revenue_summary/
    │   └── report_date=2024-01-15/
    │       └── part-001.parquet
    └── customer_ltv/
        └── part-001.parquet
```

**Lifecycle policy:**
- Bronze: Standard → IA (30d) → Glacier (90d) → Deep Archive (365d)
- Silver: Standard → IA (90d) → Glacier (730d)
- Gold: Standard → IA (365d)

**Access control:**
- Bronze: read only for DE team (IAM role), no analyst access (raw PII)
- Silver: read for analysts + DE; write only for DE pipeline role
- Gold: read for all authenticated users; write only for pipeline role

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

## Concept 9: Serialisation Formats

**Q39 — Why JSON is poor for Kafka at high throughput**

**Problem 1 — Size:** JSON stores field names in every message:
```json
{"order_id": 1001, "customer_id": "C001", "amount": 120.50, "status": "completed"}
```
Field names (`order_id`, `customer_id`, etc.) repeat in every message — for 1 million messages, "order_id" is written 1 million times. In Avro, field names are in the schema (stored once); the message contains only values.

**Problem 2 — No schema enforcement:** Any JSON is valid. A producer typo (`"amout"` instead of `"amount"`) produces a message that parsers silently ignore or crash on.

**Avro advantages over JSON:**
1. **Compact binary encoding:** 3–10× smaller messages; field names not repeated in data
2. **Schema enforcement:** Invalid messages are rejected at the producer before they reach the topic

---

**Q40 — Schema Registry**

A **Schema Registry** (Confluent Schema Registry, AWS Glue Schema Registry) is a centralised service that stores versioned schemas and enforces compatibility rules.

**Problem it solves:** In a distributed system, producers evolve schemas (add fields, remove fields). Without a registry, consumers break silently when they receive a message with an unexpected schema.

**How it works:**
1. Producer registers its schema before first publish — gets a schema ID
2. Each message is prefixed with the schema ID (4 bytes)
3. Consumer fetches the schema for that ID from the registry and deserialises correctly
4. Registry enforces compatibility: if producer tries to register an incompatible schema, the registration is **rejected**

**Without a registry:**
- No enforcement — incompatible schemas publish successfully
- Consumers receive messages they cannot parse → crashes or silent data corruption
- No audit trail of schema changes

---

**Q41 — Consumer breaks when producer removes a field**

**What happens:** Consumer has schema v1 (includes `status` field). Producer now publishes schema v2 (without `status`). The consumer tries to deserialise a v2 message expecting `status` → field not found → depends on configuration: may throw a `DeserializationException` or silently return null.

**How Schema Registry prevents this:**
1. Producer tries to register schema v2 (without `status`) under `BACKWARD` compatibility mode
2. Registry checks: can schema v2 read data written with schema v1? No — v1 has `status`, v2 does not (consumers on v1 cannot read v2 messages)
3. Registry **rejects** the schema registration with a compatibility error
4. The producer cannot publish under v2 until it fixes the compatibility violation (add a default for `status` in v2, or switch to `NONE` mode — which removes enforcement)

---

**Q42 — Avro vs. Protobuf for Python, Java, Go microservices**

**Recommendation: Protobuf** for a multi-language microservice architecture.

**Reasons:**
- First-class code generation for Python, Java, Go, Rust, C++, C#, JavaScript — all from a single `.proto` file
- More compact binary encoding than Avro (~10–30% smaller messages)
- Field numbers (not names) are stable identifiers — renaming a field in the `.proto` is non-breaking as long as the number is unchanged
- gRPC (the dominant inter-service RPC framework) is built on Protobuf — your service APIs and your data serialisation use the same format

**Where Avro wins:**
- Kafka-native ecosystem: the Confluent Schema Registry has deeper Avro support, and most Kafka tooling (Kafka Connect, ksqlDB) defaults to Avro
- Self-describing files: Avro embeds the full schema in the file header — files are readable without a separate schema file; important for data lake landing zones
- If your team already uses Confluent Platform (Kafka + Schema Registry), Avro is the natural choice

---

**Q43 — Avro union type vs. optional field**

In Avro, there is no "optional" keyword. An optional field is represented as a **union type** with `null`:

```json
{
  "name": "discount_pct",
  "type": ["null", "double"],
  "default": null
}
```

- `["null", "double"]` means the field can be either null or a double
- `"default": null` means: if a message is read that does not include this field (backwards compatibility), use `null` as the default
- The `null` type must come **first** in the union when the default is null (Avro requires the default to match the first type in the union)

**Union type for non-null alternatives:**
```json
{
  "name": "order_id",
  "type": ["int", "string"]
}
```
This means `order_id` can be either an int or a string — less common, but valid for heterogeneous sources.

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
