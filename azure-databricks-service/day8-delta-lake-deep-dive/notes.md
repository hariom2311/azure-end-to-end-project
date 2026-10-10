# Day 8 — Delta Lake Deep Dive

> **Goal:** Master Delta Lake operations beyond basic reads and writes. By the end of this day you will be able to do MERGE (upsert), UPDATE, DELETE, travel back in time to any version, reclaim storage with VACUUM, speed up queries with OPTIMIZE and Z-ORDER, and understand how the Delta transaction log works underneath everything.
>
> **Prerequisite:** Day 5 and Day 6 must be complete — `dev_catalog.bronze.sample_people` external Delta table must exist on `stadlsdev001`.
>
> **Cluster:** attach all notebooks to your all-purpose cluster (`dbw-ev-dev`).

---

## Part 1: How Delta Lake Works — The Transaction Log

Before doing any operations, understand what makes Delta Lake different from plain Parquet.

### 1.1 The `_delta_log` Folder

Every Delta table has a `_delta_log/` folder at its root alongside the data files.

```
abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/
  ├── _delta_log/
  │     ├── 00000000000000000000.json   ← version 0 — CREATE TABLE
  │     ├── 00000000000000000001.json   ← version 1 — first write
  │     ├── 00000000000000000002.json   ← version 2 — UPDATE
  │     └── ...
  ├── part-00000-abc123.snappy.parquet
  └── part-00001-def456.snappy.parquet
```

Each `.json` file in `_delta_log` is one **commit** — it records exactly what changed: which files were added, which were removed, and the schema.

### 1.2 What This Gives You

```
Plain Parquet                    Delta Lake
──────────────────────────────────────────────────────────
No transaction log               _delta_log/ tracks every change
No ACID — partial writes visible Atomic commits — all or nothing
No UPDATE/DELETE                 Full DML: MERGE, UPDATE, DELETE
No time travel                   SELECT ... VERSION AS OF N
No schema enforcement            Schema evolution with guardrails
Manual compaction                OPTIMIZE + Z-ORDER built-in
```

### 1.3 Check the Delta Log in a Notebook

```python
# See the raw commit log entries for your table
log_path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/_delta_log/"
files = dbutils.fs.ls(log_path)
for f in files:
    print(f.name)
```

```python
# See version history with DESCRIBE HISTORY
%sql
DESCRIBE HISTORY dev_catalog.bronze.sample_people
```

Each row = one commit. Columns: `version`, `timestamp`, `operation`, `operationParameters`, `userName`.

---

## Part 2: DESCRIBE HISTORY — Reading the Version Table

Run this at the start of every demo — it tells you which version you are on.

```sql
%sql
DESCRIBE HISTORY dev_catalog.bronze.sample_people
```

The output shows:

| Column | Meaning |
|---|---|
| `version` | Sequential number — starts at 0 |
| `timestamp` | When the commit happened |
| `operation` | `WRITE`, `UPDATE`, `DELETE`, `MERGE`, `OPTIMIZE`, `VACUUM START` |
| `operationParameters` | Extra detail — e.g. the predicate used in UPDATE |
| `operationMetrics` | Row counts — numOutputRows, numUpdatedRows, etc. |

---

## Part 3: UPDATE

UPDATE modifies rows that match a condition. The original Parquet files are NOT edited — Delta writes new files and records which old files to logically delete.

### Step 1 — See Current Data

```sql
%sql
SELECT * FROM dev_catalog.bronze.sample_people ORDER BY id
```

You should see 5 rows: Alice, Bob, Carol, David, Eve with statuses (completed/failed/pending).

### Step 2 — Run UPDATE

```sql
%sql
-- Change all 'pending' status rows to 'completed'
UPDATE dev_catalog.bronze.sample_people
SET status = 'completed', amount = amount + 50.0
WHERE status = 'pending'
```

### Step 3 — Verify the Change

```sql
%sql
SELECT * FROM dev_catalog.bronze.sample_people ORDER BY id
```

David's row should now show `completed` and `amount = 250.0` (was 200.0).

### Step 4 — Check What the Commit Recorded

```sql
%sql
DESCRIBE HISTORY dev_catalog.bronze.sample_people
```

A new row appears at the top with `operation = UPDATE` and `operationParameters` showing the WHERE clause.

### Step 5 — Understand What Happened on Storage

```python
# The original file still exists — Delta only added a new file and marked the old one removed
path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/"
files = dbutils.fs.ls(path)
for f in files:
    print(f.name, f.size)
```

You will see MORE Parquet files than before the UPDATE. Old files are logically deleted (tracked in `_delta_log`) but physically still on disk until VACUUM runs.

---

## Part 4: DELETE

DELETE removes rows that match a condition. Like UPDATE, it writes new files rather than modifying existing ones.

### Step 1 — Run DELETE

```sql
%sql
-- Remove the row where status is 'failed'
DELETE FROM dev_catalog.bronze.sample_people
WHERE status = 'failed'
```

### Step 2 — Verify

```sql
%sql
SELECT * FROM dev_catalog.bronze.sample_people ORDER BY id
-- Bob's row (status='failed') should be gone
```

### Step 3 — Check History

```sql
%sql
DESCRIBE HISTORY dev_catalog.bronze.sample_people
-- New row: operation = DELETE
```

---

## Part 5: MERGE (Upsert)

MERGE is the most powerful Delta operation. It combines INSERT, UPDATE, and DELETE in one statement based on a matching condition. This is the standard pattern for incremental data loads from ADF or streaming pipelines.

### 5.1 What MERGE Does

```
Source table (incoming new/changed records)
  +
Target table (existing Delta table)
  = MERGE on a key column
      ├── WHEN MATCHED AND condition → UPDATE SET ...
      ├── WHEN MATCHED AND other condition → DELETE
      └── WHEN NOT MATCHED → INSERT ...
```

### 5.2 Prepare: Create a Source DataFrame

```python
# Simulate incoming data from a pipeline — 3 scenarios:
# - id=1 (Alice): amount changed → should UPDATE
# - id=2 (Bob): was deleted earlier, now comes back → should INSERT
# - id=6 (Frank): brand new record → should INSERT

new_data = [
    (1, "Alice",   "completed", 300.00),   # existing — amount changed
    (2, "Bob",     "completed", 150.00),   # previously deleted — re-insert
    (6, "Frank",   "completed", 500.00),   # brand new
]
columns = ["id", "name", "status", "amount"]

df_source = spark.createDataFrame(new_data, columns)
df_source.createOrReplaceTempView("source_updates")
df_source.show()
```

### 5.3 Run MERGE

```sql
%sql
MERGE INTO dev_catalog.bronze.sample_people AS target
USING source_updates AS source
ON target.id = source.id

WHEN MATCHED AND source.amount <> target.amount THEN
  UPDATE SET
    target.amount = source.amount,
    target.status = source.status

WHEN NOT MATCHED THEN
  INSERT (id, name, status, amount)
  VALUES (source.id, source.name, source.status, source.amount)
```

### 5.4 Verify All Three Outcomes

```sql
%sql
SELECT * FROM dev_catalog.bronze.sample_people ORDER BY id
```

Expected result:
- id=1 Alice: amount = 300.00 (updated)
- id=2 Bob: back in the table (inserted)
- id=3 Carol: unchanged
- id=4 David: unchanged (was updated to completed in Part 3)
- id=5 Eve: unchanged
- id=6 Frank: new row (inserted)

### 5.5 Check the MERGE Metrics

```sql
%sql
DESCRIBE HISTORY dev_catalog.bronze.sample_people
```

The MERGE commit shows `operationMetrics`:
- `numTargetRowsUpdated` — how many rows were updated
- `numTargetRowsInserted` — how many rows were inserted
- `numTargetRowsDeleted` — how many rows were deleted

### 5.6 MERGE with DELETE Clause

You can also delete rows as part of MERGE:

```sql
%sql
-- If a record arrives with status='remove', delete it from the target
MERGE INTO dev_catalog.bronze.sample_people AS target
USING (SELECT 6 AS id, 'remove' AS status) AS source
ON target.id = source.id

WHEN MATCHED AND source.status = 'remove' THEN DELETE

WHEN NOT MATCHED THEN
  INSERT (id, name, status, amount)
  VALUES (source.id, 'unknown', source.status, 0.0)
```

Frank (id=6) should now be gone.

---

## Part 6: Time Travel

Because every version is preserved in `_delta_log`, you can query any past version of the table.

### 6.1 Query by Version Number

```sql
%sql
-- See the table as it was at version 0 (right after creation)
SELECT * FROM dev_catalog.bronze.sample_people VERSION AS OF 0
```

```sql
%sql
-- See the table before the UPDATE in Part 3 (version 1 = after initial write)
SELECT * FROM dev_catalog.bronze.sample_people VERSION AS OF 1
```

### 6.2 Query by Timestamp

```sql
%sql
-- Use the timestamp from DESCRIBE HISTORY output
SELECT * FROM dev_catalog.bronze.sample_people
TIMESTAMP AS OF '2026-10-10 06:00:00'
```

Replace the timestamp with a value from your `DESCRIBE HISTORY` output.

### 6.3 Time Travel in Python

```python
# Read a specific version as a DataFrame
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(
    "abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/"
)
df_v0.show()
```

```python
# Read as of a specific timestamp
df_ts = spark.read.format("delta").option("timestampAsOf", "2026-10-10 06:00:00").load(
    "abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/"
)
df_ts.show()
```

### 6.4 Restore a Table to a Previous Version

If you accidentally deleted important data, restore the entire table to an earlier version:

```sql
%sql
-- Restore to version 1 (undo all changes after the initial write)
RESTORE TABLE dev_catalog.bronze.sample_people TO VERSION AS OF 1
```

```sql
%sql
-- Verify the restore
SELECT * FROM dev_catalog.bronze.sample_people ORDER BY id
-- Should show the original 5 rows before any UPDATE/DELETE/MERGE
```

```sql
%sql
-- RESTORE itself creates a new version entry
DESCRIBE HISTORY dev_catalog.bronze.sample_people
-- Top row: operation = RESTORE
```

---

## Part 7: OPTIMIZE and Z-ORDER

Over time, Delta tables accumulate many small Parquet files (from streaming writes, frequent small batches, or many UPDATE/DELETE operations). This makes reads slow because Spark has to open hundreds of tiny files. OPTIMIZE compacts them.

### 7.1 See the File Count Before OPTIMIZE

```python
path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/"
files = [f for f in dbutils.fs.ls(path) if f.name.endswith(".parquet")]
print(f"Parquet file count before OPTIMIZE: {len(files)}")
```

### 7.2 Run OPTIMIZE

```sql
%sql
OPTIMIZE dev_catalog.bronze.sample_people
```

Output shows:
- `numFilesAdded` — new compacted files written
- `numFilesRemoved` — old small files marked removed
- `filesAdded.avg` — average size of new files

### 7.3 Z-ORDER — Colocate Related Data

Z-ORDER sorts data within each file by a column so that queries filtering on that column skip irrelevant files entirely (data skipping).

```sql
%sql
-- Optimize and sort data within files by status column
-- Queries like WHERE status = 'completed' will skip files that don't contain 'completed'
OPTIMIZE dev_catalog.bronze.sample_people ZORDER BY (status)
```

```sql
%sql
-- Z-ORDER on multiple columns (most selective column first)
OPTIMIZE dev_catalog.bronze.sample_people ZORDER BY (status, id)
```

> **When to use Z-ORDER:** On columns you frequently filter on in WHERE clauses. For a payments table, `ZORDER BY (payment_date, customer_id)` is typical.

### 7.4 Check File Count After OPTIMIZE

```python
files = [f for f in dbutils.fs.ls(path) if f.name.endswith(".parquet")]
print(f"Parquet file count after OPTIMIZE: {len(files)}")
# Fewer files — but the old ones are still physically present until VACUUM
```

---

## Part 8: VACUUM — Reclaim Storage

VACUUM physically deletes the old Parquet files that Delta has logically removed (from UPDATE, DELETE, MERGE, OPTIMIZE). Until VACUUM runs, old files exist on storage but are invisible to queries.

### 8.1 Default Retention — 7 Days

By default, Delta keeps old files for 7 days (168 hours). This allows time travel back 7 days and protects against long-running readers.

```sql
%sql
-- VACUUM with default 7-day retention (safe for production)
VACUUM dev_catalog.bronze.sample_people
```

Output: lists the files it deleted.

### 8.2 VACUUM DRY RUN — Preview Before Deleting

```sql
%sql
-- See which files WOULD be deleted without actually deleting them
VACUUM dev_catalog.bronze.sample_people DRY RUN
```

### 8.3 Shorter Retention (Dev/Testing Only)

```python
# WARNING: reduces time travel window — do NOT use in production
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
```

```sql
%sql
-- Keep only the last 0 hours — deletes everything not in the current version
-- Only for dev/testing to free up storage immediately
VACUUM dev_catalog.bronze.sample_people RETAIN 0 HOURS
```

```python
# Re-enable the safety check after testing
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
```

### 8.4 After VACUUM — Time Travel is Limited

```sql
%sql
-- This will now fail if version 0 files were vacuumed
SELECT * FROM dev_catalog.bronze.sample_people VERSION AS OF 0
-- Error: version 0 not available — files deleted by VACUUM
```

After VACUUM, time travel only works back to the oldest retained version.

---

## Part 9: Schema Evolution

Delta Lake can handle schema changes — adding new columns, changing nullability — without breaking the table.

### 9.1 Add a New Column via mergeSchema

```python
# New data has an extra column 'region' that the existing table doesn't have
new_data_with_region = [
    (7, "Grace", "completed", 600.00, "North"),
    (8, "Henry", "pending",   400.00, "South"),
]
columns_with_region = ["id", "name", "status", "amount", "region"]

df_new = spark.createDataFrame(new_data_with_region, columns_with_region)

# Without mergeSchema=true this would fail with AnalysisException
df_new.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/")
```

### 9.2 Verify the New Column Exists

```sql
%sql
SELECT * FROM dev_catalog.bronze.sample_people ORDER BY id
-- Grace and Henry have 'region' value; older rows show NULL for region
```

```sql
%sql
DESCRIBE dev_catalog.bronze.sample_people
-- 'region' column now appears in the schema
```

### 9.3 Schema Enforcement — Bad Write Blocked

```python
# Try to write a DataFrame with a column that has the wrong type
bad_data = [(9, "Ivan", "completed", "not-a-number")]  # amount is String, should be Double
bad_df = spark.createDataFrame(bad_data, ["id", "name", "status", "amount"])

try:
    bad_df.write.format("delta").mode("append").save(
        "abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/"
    )
except Exception as e:
    print(f"Schema enforcement blocked the write: {e}")
```

Delta rejects the write and preserves the table's integrity.

---

## Part 10: Table Properties and Statistics

### 10.1 Set Table Properties

```sql
%sql
-- Set Delta-specific properties on the table
ALTER TABLE dev_catalog.bronze.sample_people
SET TBLPROPERTIES (
    'delta.logRetentionDuration' = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
```

| Property | What it does |
|---|---|
| `delta.logRetentionDuration` | How long to keep the `_delta_log` entries (default 30 days) |
| `delta.deletedFileRetentionDuration` | How long deleted files are kept before VACUUM can remove them (default 7 days) |
| `delta.autoOptimize.optimizeWrite` | Automatically writes optimally-sized files — no need to run OPTIMIZE as often |
| `delta.autoOptimize.autoCompact` | After each write, automatically compacts small files in the background |

### 10.2 Check Current Properties

```sql
%sql
DESCRIBE EXTENDED dev_catalog.bronze.sample_people
-- Scroll to the bottom — Table Properties section shows all set values
```

### 10.3 ANALYZE TABLE — Update Statistics

Spark uses column statistics to skip files during reads. ANALYZE updates these statistics:

```sql
%sql
ANALYZE TABLE dev_catalog.bronze.sample_people COMPUTE STATISTICS FOR ALL COLUMNS
```

---

## Part 11: Clone a Delta Table

Delta supports creating copies of tables — useful for testing changes without affecting production.

### 11.1 SHALLOW CLONE — Metadata Only, Shares Files

```sql
%sql
-- Shallow clone: new table entry in catalog, but shares the same data files
-- Fast and cheap — no data is copied
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.sample_people_test
SHALLOW CLONE dev_catalog.bronze.sample_people
```

Changes written to `sample_people_test` create new files — the original is unaffected. But you cannot VACUUM the original without considering the clone.

### 11.2 DEEP CLONE — Full Independent Copy

```sql
%sql
-- Deep clone: copies all data files to a new location
-- Fully independent — changes and VACUUM on either side don't affect the other
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.sample_people_backup
DEEP CLONE dev_catalog.bronze.sample_people
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people_backup/'
```

### 11.3 When to Use Each

| | Shallow Clone | Deep Clone |
|---|---|---|
| Storage cost | Near zero (shares files) | Full copy of all data |
| Independence | Partial — shares data files | Full — completely independent |
| Use for | Dev testing, query testing | Backup, migration, disaster recovery |
| VACUUM impact | Must coordinate with source | Fully independent |

---

## Part 12: Delta Table on a New Path — Full Workflow

Practice creating a brand new Delta table from scratch, running the full lifecycle.

### Step 1 — Create a Larger Dataset

```python
from pyspark.sql.functions import col, when

# Create a larger dataset — 20 rows across different categories
data = [(i,
         f"customer_{i}",
         "electronics" if i % 3 == 0 else ("clothing" if i % 3 == 1 else "food"),
         "completed" if i % 4 != 0 else "pending",
         round(i * 47.5, 2))
        for i in range(1, 21)]

columns = ["id", "customer_name", "category", "status", "amount"]
df = spark.createDataFrame(data, columns)
df.show(5)
print(f"Total rows: {df.count()}")
```

### Step 2 — Write as Delta with Partitioning

```python
orders_path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/orders/"

df.write.format("delta") \
    .partitionBy("category") \
    .mode("overwrite") \
    .save(orders_path)

print(f"Written to: {orders_path}")
```

### Step 3 — Register as External Table

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.orders
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/orders/'
```

### Step 4 — Run All Delta Operations

```sql
%sql
-- 1. Check row count per category
SELECT category, COUNT(*) as count, SUM(amount) as total
FROM dev_catalog.bronze.orders
GROUP BY category ORDER BY category
```

```sql
%sql
-- 2. UPDATE: mark all food items over 200 as 'premium'
UPDATE dev_catalog.bronze.orders
SET status = 'premium'
WHERE category = 'food' AND amount > 200
```

```sql
%sql
-- 3. DELETE: remove all pending electronics orders
DELETE FROM dev_catalog.bronze.orders
WHERE category = 'electronics' AND status = 'pending'
```

```python
# 4. MERGE: bring in 3 updated + 2 new rows
updates = [
    (1,  "customer_1",  "electronics", "completed", 999.99),   # update amount
    (4,  "customer_4",  "clothing",    "completed", 190.00),   # update status
    (21, "customer_21", "electronics", "completed", 350.00),   # new row
    (22, "customer_22", "food",        "pending",   125.00),   # new row
]
df_updates = spark.createDataFrame(updates, columns)
df_updates.createOrReplaceTempView("order_updates")
```

```sql
%sql
MERGE INTO dev_catalog.bronze.orders AS target
USING order_updates AS source
ON target.id = source.id

WHEN MATCHED THEN UPDATE SET *

WHEN NOT MATCHED THEN INSERT *
```

```sql
%sql
-- 5. OPTIMIZE with Z-ORDER on the most-filtered column
OPTIMIZE dev_catalog.bronze.orders ZORDER BY (status)
```

```sql
%sql
-- 6. View full history
DESCRIBE HISTORY dev_catalog.bronze.orders
```

```sql
%sql
-- 7. Time travel to before the UPDATE
SELECT * FROM dev_catalog.bronze.orders VERSION AS OF 0
ORDER BY id
```

```sql
%sql
-- 8. VACUUM — clean up old files (keep last 7 days)
VACUUM dev_catalog.bronze.orders
```

---

## Quick Reference — Day 8 Delta Lake Operations

```
Operation          Syntax                                          Effect
─────────────────────────────────────────────────────────────────────────────────
UPDATE             UPDATE table SET col=val WHERE condition        Modifies matching rows
                                                                   Writes new files

DELETE             DELETE FROM table WHERE condition               Removes matching rows
                                                                   Writes new files

MERGE              MERGE INTO target USING source ON key           Upsert: insert+update+delete
                   WHEN MATCHED THEN UPDATE SET ...                in one atomic operation
                   WHEN NOT MATCHED THEN INSERT ...

DESCRIBE HISTORY   DESCRIBE HISTORY table                          Shows all versions with
                                                                   timestamp and operation

Time travel        SELECT * FROM table VERSION AS OF N             Read any past version
(by version)       SELECT * FROM table TIMESTAMP AS OF 'ts'        by version number or time

RESTORE            RESTORE TABLE table TO VERSION AS OF N          Rolls back the table to
                                                                   a previous version

OPTIMIZE           OPTIMIZE table                                  Compacts small files into
                                                                   larger target-size files

Z-ORDER            OPTIMIZE table ZORDER BY (col1, col2)           Colocates related data
                                                                   within files for data skipping

VACUUM             VACUUM table                                    Physically deletes old files
                   VACUUM table RETAIN N HOURS                     not in current version
                   VACUUM table DRY RUN                            (respects retention window)

mergeSchema        .option("mergeSchema", "true")                  Allows adding new columns
                                                                   when writing a DataFrame

autoOptimize       TBLPROPERTIES                                   Auto-compacts on each write
                   delta.autoOptimize.optimizeWrite=true           and in background

SHALLOW CLONE      CREATE TABLE t2 SHALLOW CLONE t1                Fast copy — shares files
DEEP CLONE         CREATE TABLE t2 DEEP CLONE t1                   Full copy — independent

_delta_log/        Folder inside every Delta table path            Transaction log —
                                                                   one JSON per commit/version
```
