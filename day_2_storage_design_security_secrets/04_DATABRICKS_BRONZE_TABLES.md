# 04 — Databricks Bronze Delta Tables
**Day 2 | Step 4 of 4**

Create two types of Bronze Delta tables in Databricks:

| Type | What it is | Where data lives |
|---|---|---|
| **External Delta table** | Table definition points to ADLS path — data is in `evdatalakedev` | `abfss://bronze@evdatalakedev.../api/payments/` |
| **Internal (managed) Delta table** | Databricks owns the data — stored in Databricks file system (DBFS) | `/user/hive/warehouse/bronze.db/payments/` |

**Why both?**
- External = production pattern — data lives in ADLS, survives if Databricks workspace is deleted
- Internal = learning — simpler to query, good for understanding Delta basics before worrying about storage paths

Both tables contain the same data. This is purely for education on Day 2 — from Day 3 onwards you will use external only.

---

## Part A — Notebooks to Run

Import both notebooks from `notebooks/` folder into Databricks then run them in order:

1. `notebooks/03_bronze_api_payments.ipynb` — payments: full load + incremental
2. `notebooks/04_bronze_blob_sessions.ipynb` — blob sessions: hourly read

Each notebook does:
- Reads data from the Bronze ADLS Delta path (written by ADF)
- Creates an external table pointing to the ADLS path
- Creates an internal managed table from the same data
- Runs a row count and sample display to verify both

---

## Part B — Create Bronze Database in Databricks

Run this once in any notebook before the table notebooks:

**What it does:** Creates a Hive metastore database called `bronze`. Tables created with `CREATE TABLE ... LOCATION ...` inside this database will be tracked in the metastore, queryable from any notebook.

```sql
-- Run in a SQL cell or use spark.sql()
CREATE DATABASE IF NOT EXISTS bronze
COMMENT 'Bronze layer — raw ingested data, append-only';

SHOW DATABASES;
-- Should list: bronze (and default)
```

Or in Python:
```python
spark.sql("CREATE DATABASE IF NOT EXISTS bronze COMMENT 'Bronze layer — raw ingested data'")
print("Database created — OK")
```

---

## Part C — External Table: Payments

**What is an external Delta table?**
The table definition (schema, name) is registered in the Hive metastore. But the actual Parquet files live in ADLS Gen2. If you `DROP TABLE`, only the metastore entry is removed — the files in ADLS are untouched.

**In Databricks SQL cell:**

```sql
CREATE TABLE IF NOT EXISTS bronze.payments
USING DELTA
LOCATION 'abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/'
COMMENT 'Bronze payments — raw from VoltGrid API, full+incremental load via ADF';
```

**Verify:**
```sql
DESCRIBE EXTENDED bronze.payments;
-- Shows: Location = abfss://bronze@evdatalakedev.../api/payments/

SELECT COUNT(*) FROM bronze.payments;

SELECT * FROM bronze.payments LIMIT 5;
```

---

## Part D — Internal (Managed) Table: Payments

**What is an internal Delta table?**
Databricks manages both the metastore entry AND the data files. Data is written to DBFS (Databricks File System) at `/user/hive/warehouse/bronze.db/payments_internal/`. If you `DROP TABLE`, the files are also deleted.

```python
# Read from ADLS (ADF already wrote data here)
df_payments = spark.read.format("delta").load(
    "abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/"
)

print(f"Rows from ADLS: {df_payments.count():,}")

# Write to internal Delta table (DBFS-backed)
df_payments.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.payments_internal")

print("Internal table created — OK")
```

**Verify:**
```sql
DESCRIBE EXTENDED bronze.payments_internal;
-- Location will show: dbfs:/user/hive/warehouse/bronze.db/payments_internal

SELECT COUNT(*) FROM bronze.payments_internal;
-- Should match external table count
```

**Compare the two:**

| | `bronze.payments` (external) | `bronze.payments_internal` (internal) |
|---|---|---|
| Data location | ADLS Gen2 | DBFS |
| DROP TABLE deletes files? | No | Yes |
| ADF writes directly? | Yes | No — Databricks notebook writes |
| Production use? | Yes | Learning only |
| Works without Databricks? | Yes (any Spark cluster) | No |

---

## Part E — External Table: Charging Sessions

```sql
CREATE TABLE IF NOT EXISTS bronze.charging_sessions
USING DELTA
LOCATION 'abfss://bronze@evdatalakedev.dfs.core.windows.net/blob/iot_sessions/'
COMMENT 'Bronze charging sessions — from source blob, hourly load via ADF'
PARTITIONED BY (ingestion_date, ingestion_hour);
```

**Verify:**
```sql
SHOW PARTITIONS bronze.charging_sessions;
-- Shows: ingestion_date=2026-07-04/ingestion_hour=06 etc.

SELECT COUNT(*) FROM bronze.charging_sessions;

SELECT ingestion_date, ingestion_hour, COUNT(*) as rows
FROM bronze.charging_sessions
GROUP BY ingestion_date, ingestion_hour
ORDER BY ingestion_date, ingestion_hour;
```

---

## Part F — Internal Table: Charging Sessions

```python
df_sessions = spark.read.format("delta").load(
    "abfss://bronze@evdatalakedev.dfs.core.windows.net/blob/iot_sessions/"
)

print(f"Rows from ADLS: {df_sessions.count():,}")

df_sessions.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("ingestion_date", "ingestion_hour") \
    .saveAsTable("bronze.charging_sessions_internal")

print("Internal sessions table created — OK")
```

---

## Part G — Delta Table Basics (Learning)

Now that both tables exist, run these to understand Delta:

### Time Travel
```sql
-- See all versions of the table
DESCRIBE HISTORY bronze.payments;

-- Query version 0 (first load)
SELECT COUNT(*) FROM bronze.payments VERSION AS OF 0;

-- Query data as of a specific timestamp
SELECT COUNT(*) FROM bronze.payments
TIMESTAMP AS OF '2026-07-04T10:00:00';
```

### Transaction Log
```python
# Delta stores every operation in _delta_log/
display(dbutils.fs.ls(abfss("bronze", "api/payments/_delta_log/")))
# Each .json file = one transaction (add files, remove files, schema change, etc.)
```

### Optimize + Z-Order (run after loading data)
```sql
-- Compact small files into larger ones (faster reads)
OPTIMIZE bronze.payments;

-- Z-order by columns you filter on most (e.g. payment_id, updated_at)
OPTIMIZE bronze.payments ZORDER BY (updated_at, status);
```

### Table Stats
```sql
-- Row count, file count, size
DESCRIBE DETAIL bronze.payments;
```

---

## Part H — Incremental Load Verification

After running the payments pipeline a second time (incremental):

```python
# Read from ADLS Delta — should have more rows than first run
df_v2 = spark.read.format("delta").load(abfss("bronze", "api/payments/"))
print(f"Total rows after incremental: {df_v2.count():,}")

# Check history — should show 2 transactions
spark.sql("DESCRIBE HISTORY bronze.payments").show(5, truncate=False)

# Check watermark range — what was the max updated_at loaded?
from pyspark.sql.functions import max as spark_max
df_v2.select(spark_max("updated_at")).show()
```

---

## Part I — Query Both Tables Side by Side

```python
# External (ADLS-backed)
ext_count = spark.sql("SELECT COUNT(*) FROM bronze.payments").collect()[0][0]

# Internal (DBFS-backed)
int_count = spark.sql("SELECT COUNT(*) FROM bronze.payments_internal").collect()[0][0]

print(f"External table rows : {ext_count:,}")
print(f"Internal table rows : {int_count:,}")
print(f"Match: {ext_count == int_count}")
```

---

## End-of-Day Verification

Run all of these — all should pass before calling Day 2 done:

```python
checks = {}

# 1. External payments table exists and has data
try:
    n = spark.sql("SELECT COUNT(*) FROM bronze.payments").collect()[0][0]
    checks["bronze.payments (external)"] = f"OK — {n:,} rows"
except Exception as e:
    checks["bronze.payments (external)"] = f"FAIL — {e}"

# 2. Internal payments table
try:
    n = spark.sql("SELECT COUNT(*) FROM bronze.payments_internal").collect()[0][0]
    checks["bronze.payments_internal"] = f"OK — {n:,} rows"
except Exception as e:
    checks["bronze.payments_internal"] = f"FAIL — {e}"

# 3. External sessions table
try:
    n = spark.sql("SELECT COUNT(*) FROM bronze.charging_sessions").collect()[0][0]
    checks["bronze.charging_sessions (external)"] = f"OK — {n:,} rows"
except Exception as e:
    checks["bronze.charging_sessions (external)"] = f"FAIL — {e}"

# 4. Internal sessions table
try:
    n = spark.sql("SELECT COUNT(*) FROM bronze.charging_sessions_internal").collect()[0][0]
    checks["bronze.charging_sessions_internal"] = f"OK — {n:,} rows"
except Exception as e:
    checks["bronze.charging_sessions_internal"] = f"FAIL — {e}"

# 5. Delta log exists for payments
try:
    dbutils.fs.ls(abfss("bronze", "api/payments/_delta_log/"))
    checks["Delta log (payments)"] = "OK"
except Exception as e:
    checks["Delta log (payments)"] = f"FAIL — {e}"

print("\n=== Day 2 End-of-Day Verification ===\n")
for k, v in checks.items():
    status = "PASS" if v.startswith("OK") else "FAIL"
    print(f"  [{status}] {k:<45} {v}")
```

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `Table not found: bronze.payments` | Database `bronze` not created yet | Run `CREATE DATABASE IF NOT EXISTS bronze` first |
| `Path does not exist` in LOCATION | ADF pipeline has not run yet | Run `pl_bronze_api_payments` in ADF Monitor first |
| `AnalysisException: Delta table not found` | No Delta files at ADLS path | ADF wrote Parquet, not Delta — check sink dataset format is Delta |
| `Permission denied on abfss://` | SP OAuth not configured in this notebook | Run `00b_connect_storage_no_mount` Cells 1–3 first |
| Internal table row count differs | Internal table not refreshed after second ADF run | Re-run Part F with `.mode("overwrite")` |

---

## Summary

| Table | Type | Location | Row source |
|---|---|---|---|
| `bronze.payments` | External Delta | ADLS `bronze/api/payments/` | ADF pipeline (full + incremental) |
| `bronze.payments_internal` | Internal Delta | DBFS warehouse | Databricks notebook copy |
| `bronze.charging_sessions` | External Delta | ADLS `bronze/blob/iot_sessions/` | ADF pipeline (hourly) |
| `bronze.charging_sessions_internal` | Internal Delta | DBFS warehouse | Databricks notebook copy |
