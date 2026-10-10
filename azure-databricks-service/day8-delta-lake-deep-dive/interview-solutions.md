# Day 8 — Interview Solutions: Delta Lake Deep Dive

---

## Delta Lake Fundamentals & Transaction Log

**A1**
Delta Lake is an open-source storage layer built on top of Parquet that adds ACID transactions, versioning, schema enforcement, and time travel to data stored in ADLS. Plain Parquet files have none of these:

| | Plain Parquet in ADLS | Delta Lake |
|---|---|---|
| ACID transactions | No | Yes |
| Concurrent read + write | Corrupts data | Serializable isolation |
| Schema enforcement | No | Yes — rejects wrong-type data |
| Versioned history | No | Yes — `_delta_log/` tracks every version |
| Time travel | No | Yes — `VERSION AS OF N` |
| MERGE (upsert) | Not possible | Built-in |

---

**A2**
`_delta_log/` is the Delta Lake transaction log directory that sits alongside the Parquet data files. It contains one JSON file per commit (version), named `00000000000000000000.json`, `00000000000000000001.json`, etc. Every JSON file records the operation that was performed, which files were added, which were removed, and metadata about the commit (timestamp, user, operation parameters). Every 10 commits, Delta compacts these into a Parquet checkpoint for faster state reconstruction.

---

**A3**

| ACID Property | Meaning | Delta Lake example |
|---|---|---|
| Atomicity | All or nothing — the write either fully succeeds or fully fails | If a MERGE fails halfway, no partial rows are visible — the version is not committed |
| Consistency | Data always satisfies schema and constraints | Schema enforcement rejects a row with a string in an integer column |
| Isolation | Concurrent reads and writes don't interfere | Reader on version 4 sees a stable snapshot even while writer commits version 5 |
| Durability | Once committed, data survives failures | Committed JSON in `_delta_log/` is durable in ADLS — a cluster crash doesn't lose it |

---

**A4**
The statement misses the point. The log is not just metadata — it is the mechanism that enables everything that makes Delta Lake useful: (1) ACID transactions (the log is an atomic commit record — if the JSON is there, the write succeeded; if not, it didn't); (2) snapshot isolation (readers pin to a log version and see exactly that state, unaffected by concurrent writes); (3) time travel (each version maps to a historical snapshot reconstructed from the log); (4) OPTIMIZE, MERGE, VACUUM all work by writing new log entries that redirect reads to different sets of Parquet files. Plain Parquet in ADLS is just files — no ordering, no transaction, no history.

---

**A5**
Job A reads a consistent snapshot. Delta Lake uses snapshot isolation: when Job A starts its read, it pins to the latest committed version at that moment (e.g., version 7). While Job A is reading, Job B commits a write and creates version 8. Job A continues reading from version 7 — it does NOT see Job B's data mid-read. Job A sees a consistent point-in-time snapshot from start to finish. After Job A finishes and runs the same query again, it would see version 8.

---

## DESCRIBE HISTORY, UPDATE, DELETE

**A6**
`DESCRIBE HISTORY` returns one row per version of the Delta table. Key columns:

| Column | Meaning |
|---|---|
| `version` | Sequential version number (0 = initial write, N = Nth change) |
| `timestamp` | When the operation was committed |
| `operation` | What happened — WRITE, UPDATE, DELETE, MERGE, OPTIMIZE, RESTORE |
| `operationParameters` | Details of the operation (e.g., predicate used in DELETE) |
| `operationMetrics` | Stats — numUpdatedRows, numDeletedRows, numFilesAdded, numFilesRemoved |
| `userName` | Who ran the operation |

---

**A7**
Delta Lake does NOT modify the existing Parquet file in place. Instead:
1. Delta reads the Parquet file(s) containing the rows where `id = 1`
2. It writes a NEW Parquet file with the updated row and the unchanged rows from that file
3. The old Parquet file is marked as REMOVED in the new `_delta_log` JSON entry (logically deleted)
4. The new Parquet file is marked as ADDED
5. Future reads follow the log — they read the new file, not the old one

The old file remains physically on disk until VACUUM cleans it up after the retention period.

---

**A8**
Use time travel — read the historical version and insert the missing rows back:

```sql
-- Step 1: find the version before the DELETE
DESCRIBE HISTORY dev_catalog.bronze.orders;

-- Step 2: read deleted rows from the version before the delete
-- (assume version 3 was before the delete)
SELECT * FROM dev_catalog.bronze.orders VERSION AS OF 3
WHERE status = 'test';

-- Step 3: recover by creating a temp view and inserting
CREATE OR REPLACE TEMP VIEW deleted_rows AS
SELECT * FROM dev_catalog.bronze.orders VERSION AS OF 3
WHERE status = 'test';

INSERT INTO dev_catalog.bronze.orders SELECT * FROM deleted_rows;
```

Or if all rows need to be restored: `RESTORE TABLE dev_catalog.bronze.orders TO VERSION AS OF 3`

---

**A9**
The problem is **small file explosion** — the table accumulates hundreds or thousands of small Parquet files over time. Each write produces new files, and old ones linger until VACUUM. Small files cause poor query performance: Spark must open and scan each file separately, creating high overhead for metadata operations.

Fix: `OPTIMIZE dev_catalog.bronze.orders` — this compacts many small Parquet files into fewer, larger files (target ~128 MB each). Adding `ZORDER BY (region)` co-locates related rows within files for better data skipping.

---

## MERGE (Upsert)

**A10**
MERGE is an "upsert" operation — it updates existing rows and inserts new ones in a single atomic operation. It joins the target table against a source dataset on a key column.

- `WHEN MATCHED THEN UPDATE SET ...` — runs when the join key finds a matching row in both target and source; updates specified columns
- `WHEN NOT MATCHED THEN INSERT ...` — runs when the source has a row with no matching key in the target; inserts a new row
- `WHEN MATCHED THEN DELETE` — optional; removes the target row when matched

---

**A11**

```sql
MERGE INTO target AS t
USING source AS s
ON t.id = s.id
WHEN MATCHED AND s.amount <> t.amount THEN
  UPDATE SET t.amount = s.amount
WHEN MATCHED AND s.status = 'deleted' THEN
  DELETE
WHEN NOT MATCHED THEN
  INSERT (id, name, status, amount) VALUES (s.id, s.name, s.status, s.amount)
```

Note: Delta evaluates WHEN MATCHED clauses in order. The first matching clause wins — so put the more specific condition (DELETE) after UPDATE, or the engine will try UPDATE first. If the row matches both UPDATE and DELETE conditions, Delta throws a runtime error (ambiguous match). Use mutually exclusive conditions to avoid this.

---

**A12**
Root cause: MERGE runs every hour, each run produces new small Parquet files. After 6 months (~4,300 runs), there are thousands of small files. Spark opens each file separately, causing high I/O overhead. Queries are slow because there is no data skipping — the engine scans many files.

**Fix 1 — OPTIMIZE:** Run weekly (or after every N merges): `OPTIMIZE dev_catalog.bronze.customers ZORDER BY (customer_id)` — compacts small files into large ones and applies Z-ORDER for data skipping.

**Fix 2 — Enable auto-optimization on the table:**
```sql
ALTER TABLE dev_catalog.bronze.customers
SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',
                   'delta.autoOptimize.autoCompact' = 'true');
```
This tells Databricks to auto-compact during writes so small files never accumulate.

---

**A13**
Yes — and Delta Lake treats this as a runtime error. If the source dataset has two rows with the same key value, the MERGE condition can match the same target row twice (once for each source row). Delta Lake throws: `org.apache.spark.sql.AnalysisException: There is a conflict from the WHEN MATCHED DELETE and WHEN MATCHED UPDATE SET clauses.` The fix: deduplicate the source before merging so each key appears only once, or add mutually exclusive conditions to each WHEN MATCHED clause.

---

## Time Travel

**A14**
Time travel lets you query historical versions of a Delta table — data as it existed at a specific version or timestamp. It works because Delta preserves old Parquet files until VACUUM runs.

```sql
SELECT * FROM dev_catalog.bronze.orders VERSION AS OF 3;
```

---

**A15**

| | `VERSION AS OF N` | `TIMESTAMP AS OF 'ts'` |
|---|---|---|
| Use | You know the exact version number from `DESCRIBE HISTORY` | You know the approximate time (e.g., "before the bad batch ran at 2 PM") |
| Example | `VERSION AS OF 5` | `TIMESTAMP AS OF '2024-10-01 13:00:00'` |
| When to use | After reading DESCRIBE HISTORY and identifying the version number | When you don't know the version but know the time window |

Python equivalents:
```python
# VERSION
spark.read.format("delta").option("versionAsOf", 3).load("abfss://...")

# TIMESTAMP
spark.read.format("delta").option("timestampAsOf", "2024-10-01 13:00:00").load("abfss://...")
```

---

**A16**
```sql
-- Step 1: check history to find the last version before the DELETE
DESCRIBE HISTORY dev_catalog.bronze.sample_people;
-- Suppose version 4 = the DELETE, version 3 = before the DELETE

-- Step 2: restore to version 3
RESTORE TABLE dev_catalog.bronze.sample_people TO VERSION AS OF 3;
```

RESTORE creates a new version in the history (e.g., version 5) that points to the same files as version 3. All rows are recovered atomically. The DELETE is still in history but the table's current state is back to version 3.

---

**A17**
No — they CANNOT time travel to version 2 after VACUUM with `RETAIN 0 HOURS`. VACUUM physically deletes the old Parquet files from ADLS that version 2 referenced. Once those files are gone, Delta can still read the `_delta_log` entry for version 2 (the log JSON is preserved), but when it tries to open the Parquet files listed in that log entry, they are gone — the query fails with a `FileNotFoundException`.

**The default 7-day retention exists precisely to prevent this:** it keeps old files long enough for time travel during the retention window. Short-circuit retention (`RETAIN 0 HOURS`) must first disable the safety check:
```sql
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM dev_catalog.bronze.sample_people RETAIN 0 HOURS;
```
This is explicitly a dev-only workaround — never run in production.

---

## OPTIMIZE and Z-ORDER

**A18**
`OPTIMIZE` compacts many small Parquet files in a Delta table into fewer large files (typically targeting ~128 MB each). Small files accumulate because every INSERT, UPDATE, DELETE, or MERGE appends new Parquet files without removing or combining old ones. Over time, a table that started with 1 file can have thousands of small files. Small files cause Spark to open thousands of individual files during a scan — each with overhead — making queries slow.

---

**A19**
Z-ORDER is a data co-location technique. `ZORDER BY (status)` reorders and rewrites the data inside Parquet files so that rows with the same `status` value are physically located near each other within each file. Delta records the minimum and maximum `status` value for each file in its metadata (statistics).

When you run `SELECT * FROM orders WHERE status = 'pending'`, the Spark engine checks each file's statistics. If a file's min–max range for `status` does not include `'pending'`, Spark skips that file entirely (data skipping). With Z-ORDER applied, only 1–2 files contain `status = 'pending'` instead of all 500 files — far fewer files are opened and scanned.

---

**A20**
After OPTIMIZE, the 500 small files are compacted into a small number of large files. Z-ORDER by `(region, status)` means rows with `region = 'East'` are co-located. Delta records min/max for `region` per file. When the analyst runs `WHERE region = 'East'`, the engine reads file statistics and skips all files whose `region` range doesn't include `'East'`. Instead of scanning all 500 files, it may scan just 1–3. Query time drops dramatically — typically 5–10x faster on large tables.

---

## VACUUM, Schema Evolution, Table Properties, Clone

**A21**
`VACUUM` deletes the old Parquet files from ADLS that are no longer referenced by any current version of the Delta table. These files accumulate because UPDATE, DELETE, MERGE, and OPTIMIZE all write new files while keeping old ones. The default retention is **7 days (168 hours)**. The 7-day window exists to: (1) allow time travel queries up to 7 days into the past; (2) protect concurrent readers — a reader might be holding a reference to an older file while a write is happening. Deleting a file referenced by a concurrent reader would cause a `FileNotFoundException`.

---

**A22**

| | `mergeSchema` | Schema enforcement |
|---|---|---|
| Direction | Additive — allows new columns to be appended to the schema | Protective — blocks writes that don't match the existing schema |
| When it applies | On write, when you pass `.option("mergeSchema", "true")` | Always — Delta checks every write against the registered schema |
| What it allows | A new column in the incoming data is added to the table schema | Throws `AnalysisException` if a column has the wrong type or a required column is missing |
| What it blocks | Wrong data types (a string where an integer is expected) — still rejected | New columns without mergeSchema=true |

---

**A23**
Error without `mergeSchema`:
```
AnalysisException: A schema mismatch detected when writing to the Delta table.
```
Delta enforcement sees a column `promo_code` that doesn't exist in the registered schema and rejects the write.

**Fix:**
```python
df_new.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("abfss://bronze@stadlsdev001.dfs.core.windows.net/orders/")
```
Or via SQL: `SET spark.databricks.delta.schema.autoMerge.enabled = true` before the write. After this, the `promo_code` column is added to the schema. Existing rows have `NULL` for `promo_code`.

---

**A24**

| | SHALLOW CLONE | DEEP CLONE |
|---|---|---|
| What is copied | Only the Delta log (metadata) | Delta log + all Parquet data files |
| Storage cost | Near-zero — data files are shared with the source | Full copy — doubles storage |
| Writes to clone | Clone writes new files; source is unaffected | Fully independent — no shared files |
| Source dependency | Clone READS from the source's files; if source is deleted, clone breaks | Fully independent |
| Best use case | Dev/test — spin up a table for testing queries without copying data | Backup, disaster recovery, migration to another region |

```sql
-- Shallow clone — fast, for dev/test
CREATE TABLE dev_catalog.bronze.orders_test
SHALLOW CLONE dev_catalog.bronze.orders;

-- Deep clone — full backup
CREATE TABLE prod_catalog.bronze.orders_backup
DEEP CLONE dev_catalog.bronze.orders;
```

---

**A25**
Full strategy:

**1. Table Properties (set once at creation or via ALTER TABLE):**
```sql
ALTER TABLE dev_catalog.bronze.orders SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',    -- auto-compact on each write
  'delta.autoOptimize.autoCompact' = 'true',       -- background compaction
  'delta.logRetentionDuration' = 'interval 100 days',  -- keep log 100 days (covers 90-day travel)
  'delta.deletedFileRetentionDuration' = 'interval 100 days'  -- keep data files 100 days
);
```

**2. OPTIMIZE schedule — weekly Databricks job:**
```sql
OPTIMIZE dev_catalog.bronze.orders ZORDER BY (region, order_date);
```
Z-ORDER on `region` and `order_date` because those are the primary filter columns.

**3. VACUUM retention — match to 90-day time travel requirement:**
```sql
VACUUM dev_catalog.bronze.orders RETAIN 2160 HOURS;  -- 90 days * 24 = 2160 hours
```
Run VACUUM after each OPTIMIZE job. With `deletedFileRetentionDuration = 100 days`, VACUUM won't touch files newer than 100 days — time travel to any point in the last 90 days remains possible.

**4. MERGE setup (daily ingestion):**
```sql
MERGE INTO dev_catalog.bronze.orders AS target
USING cdc_feed AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.status <> target.status THEN
  UPDATE SET target.status = source.status, target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT *
```
With `autoOptimize.optimizeWrite = true`, each MERGE auto-compacts its output — small files don't pile up between weekly OPTIMIZE runs.

**5. Audit time travel — confirm it works:**
```sql
-- Query from 85 days ago
SELECT COUNT(*) FROM dev_catalog.bronze.orders
TIMESTAMP AS OF date_sub(current_date(), 85);
```

---
