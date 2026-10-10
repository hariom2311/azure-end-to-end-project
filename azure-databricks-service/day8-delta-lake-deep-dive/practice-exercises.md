# Day 8 — Practice Exercises: Delta Lake Deep Dive

> Complete in order. Each exercise builds on the previous one.
> Table used: `dev_catalog.bronze.sample_people` on `stadlsdev001`.

---

## Exercise 1 — Explore the Transaction Log

**Objective:** Understand what `_delta_log` contains and read version history.

1. Run `DESCRIBE HISTORY dev_catalog.bronze.sample_people` — note how many versions exist
2. List files inside `_delta_log/`:
   ```python
   dbutils.fs.ls("abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/_delta_log/")
   ```
3. Read the contents of `00000000000000000000.json` (version 0):
   ```python
   dbutils.fs.head("abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/_delta_log/00000000000000000000.json")
   ```
4. Answer: what operation does version 0 record? What files were added?

**Verify:** You can see the JSON commit entries and understand what each version recorded.

---

## Exercise 2 — UPDATE and Verify

**Objective:** Run an UPDATE and confirm Delta wrote new files instead of editing old ones.

1. Count Parquet files before UPDATE:
   ```python
   files = [f for f in dbutils.fs.ls("abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/") if ".parquet" in f.name]
   print(len(files))
   ```
2. Run: `UPDATE dev_catalog.bronze.sample_people SET amount = amount * 1.1 WHERE status = 'completed'`
3. Count Parquet files after UPDATE — confirm the count increased
4. Run `DESCRIBE HISTORY` — confirm the new version shows `operation = UPDATE`
5. Read the new `_delta_log` JSON file for this version — find `numUpdatedRows` in `operationMetrics`

**Verify:** File count increased. History shows UPDATE. Amounts for completed rows are 10% higher.

---

## Exercise 3 — DELETE and Time Travel Back

**Objective:** Delete rows, then recover them using time travel.

1. Note the current version number from `DESCRIBE HISTORY`
2. Run: `DELETE FROM dev_catalog.bronze.sample_people WHERE id IN (1, 2)`
3. Confirm rows 1 and 2 are gone: `SELECT * FROM dev_catalog.bronze.sample_people ORDER BY id`
4. Time travel to the version BEFORE the delete:
   ```sql
   SELECT * FROM dev_catalog.bronze.sample_people VERSION AS OF <version_before_delete>
   ORDER BY id
   ```
5. Confirm rows 1 and 2 appear in the historical version

**Verify:** Current table has rows 1 and 2 missing. Historical version has them.

---

## Exercise 4 — MERGE (Upsert)

**Objective:** Perform a MERGE that updates existing rows and inserts new ones.

Create this source data:
```python
src = [(2, "Bob",   "completed", 999.00),   # id=2 was deleted — re-insert
       (3, "Carol", "vip",       430.50),   # id=3 exists — update status
       (9, "Ivan",  "new",       100.00)]   # id=9 doesn't exist — insert
df_src = spark.createDataFrame(src, ["id","name","status","amount"])
df_src.createOrReplaceTempView("src")
```

Write a MERGE statement that:
- When `id` matches AND `status` differs → UPDATE the status and amount
- When no match → INSERT the row

After MERGE, verify:
- id=2 Bob is back with amount=999.00
- id=3 Carol has status='vip'
- id=9 Ivan is a new row

**Verify:** `SELECT * FROM dev_catalog.bronze.sample_people ORDER BY id` shows all expected results.

---

## Exercise 5 — OPTIMIZE and Z-ORDER

**Objective:** Compact files and apply Z-ORDER, then measure the difference.

1. Check current file count in the table path
2. Run: `OPTIMIZE dev_catalog.bronze.sample_people ZORDER BY (status, id)`
3. Check file count after — confirm fewer files
4. Run `DESCRIBE HISTORY` — note the OPTIMIZE version entry and `numFilesAdded`/`numFilesRemoved`
5. Run a query with a filter and check how many files were scanned:
   ```sql
   SELECT * FROM dev_catalog.bronze.sample_people WHERE status = 'completed'
   ```
   Look at the query output — Databricks shows "files scanned" in the cell output or Spark UI.

**Verify:** File count reduced. OPTIMIZE appears in history. Filter queries scan fewer files.

---

## Exercise 6 — Schema Evolution with mergeSchema

**Objective:** Add a new column to an existing Delta table without breaking it.

1. Create a DataFrame with an extra column `region`:
   ```python
   new_rows = [(10, "Julia", "pending", 200.0, "East"),
               (11, "Kevin", "completed", 350.0, "West")]
   df_new = spark.createDataFrame(new_rows, ["id","name","status","amount","region"])
   ```
2. Try writing WITHOUT `mergeSchema` — it should fail
3. Write WITH `mergeSchema=true` — it should succeed
4. Run `DESCRIBE dev_catalog.bronze.sample_people` — confirm `region` column now exists
5. Run `SELECT * FROM dev_catalog.bronze.sample_people ORDER BY id` — older rows show `NULL` for `region`

**Verify:** `region` column exists. Old rows have NULL. New rows have East/West.

---

## Exercise 7 — RESTORE, VACUUM, and Clone

**Objective:** Restore the table, vacuum old files, and create a clone.

**Part A — RESTORE:**
1. Run `DESCRIBE HISTORY dev_catalog.bronze.sample_people` — pick version 1
2. Run: `RESTORE TABLE dev_catalog.bronze.sample_people TO VERSION AS OF 1`
3. Confirm the table is back to the state at version 1

**Part B — VACUUM:**
1. Run: `VACUUM dev_catalog.bronze.sample_people DRY RUN` — see which files would be deleted
2. Run: `VACUUM dev_catalog.bronze.sample_people` — delete files older than 7 days
3. Note: most files may not be deleted yet since they are within the 7-day window — this is expected

**Part C — SHALLOW CLONE:**
1. Create a clone:
   ```sql
   CREATE TABLE IF NOT EXISTS dev_catalog.bronze.sample_people_clone
   SHALLOW CLONE dev_catalog.bronze.sample_people
   ```
2. Update a row in the clone: `UPDATE dev_catalog.bronze.sample_people_clone SET amount = 0 WHERE id = 1`
3. Confirm the original table is unaffected: `SELECT amount FROM dev_catalog.bronze.sample_people WHERE id = 1`

**Verify:** RESTORE worked. VACUUM ran. Clone update did not affect the original table.

---
