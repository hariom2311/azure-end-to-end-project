# Day 5 — Interview Solutions: Unity Catalog Tables and Volumes

---

## Internal vs External — Concepts

**A1**
The key difference is **who owns the file location and what happens when the table is dropped**.

- **Internal (managed):** No `LOCATION` clause in `CREATE TABLE`. Databricks picks the storage path inside the Unity Catalog managed storage. When you `DROP TABLE`, Databricks **deletes the Delta files**.
- **External:** You provide `LOCATION 'abfss://...'` in `CREATE TABLE`. The files live on your own storage (ADLS, Blob). When you `DROP TABLE`, only the metadata entry is removed from Unity Catalog. **Files on storage are not touched.**

What determines the type: the presence or absence of the `LOCATION` clause in `CREATE TABLE`.

---

**A2**
- **The table entry in Unity Catalog:** Removed permanently. The table no longer appears in the catalog.
- **The Delta files on ADLS:** Not touched. The files at the LOCATION path remain completely intact on ADLS.

An external table drop only removes the catalog metadata — it is like removing a bookmark. The actual page (the data) still exists. You can re-register the same path as a new table and all the data is immediately accessible again.

---

**A3**
The data is deleted. Without a `LOCATION` clause, the table is internal (managed). Databricks owns the file location and deletes the files when the table is dropped.

To prevent data loss, the engineer should have used `LOCATION` to make it external:
```sql
CREATE TABLE dev_catalog.silver.summary
USING DELTA
LOCATION 'abfss://silver@stadlsdev001.dfs.core.windows.net/summary/'
```

With an external table, `DROP TABLE` only removes the metadata — the files on ADLS are safe.

---

**A4**
Run:
```sql
DESCRIBE EXTENDED <catalog>.<schema>.<table>
```

In the output, find two rows:
- `Type` — value is either `MANAGED` (internal) or `EXTERNAL`
- `Location` — for a managed table it shows a Databricks-controlled path; for an external table it shows your own ADLS path

---

**A5**
Yes, the external table will see the new data automatically, **as long as the new files are written in valid Delta format**.

A Delta table works through its `_delta_log/` folder — a transaction log listing all files that belong to the table. When the other team writes Delta files to the same ADLS path using a Delta-aware writer, they append entries to the transaction log. Databricks reads the transaction log on every query, so it automatically discovers the new files.

If the other team writes raw Parquet (not Delta), the external table will NOT see the new files — the Delta log has no record of them.

---

**A6**
The data is not recoverable (from Unity Catalog). The table was created without `LOCATION`, so it is internal (managed). When a managed table is dropped, Databricks deletes the underlying Delta files from the Unity Catalog managed storage. Those files are gone.

The only recovery options are:
- A backup / snapshot made before the drop (e.g. cloned table, separate Delta export)
- Azure Blob soft-delete if enabled on the underlying managed storage account (not guaranteed)

This is one of the most common accidental data-loss scenarios in Databricks — forgetting `LOCATION` on a production table.

---

**A7**
The `_delta_log/` folder contains the **Delta transaction log** — a sequence of JSON files that record every change made to the table: which Parquet files were added, which were removed, what schema changes happened, and what statistics exist per file.

An external table needs it because Unity Catalog relies on this log to know which files belong to the table. Without `_delta_log/`, a folder of Parquet files is just files — not a Delta table. When you register an external table with `LOCATION`, Unity Catalog reads `_delta_log/` at that path to understand the table's current state and schema.

---

## External Tables — Hands-On

**A8**
```sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.transactions
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/transactions/'
```

If the path already has Delta files (with a `_delta_log/`), the table immediately inherits the existing schema and data. If the path is empty, the table is created with no data and no schema until the first write.

---

**A9**
Two possible reasons:

1. **The files at the LOCATION path are not Delta format.** There is no `_delta_log/` folder. Unity Catalog cannot read the table — it may show zero rows or an error.

2. **The path has a `_delta_log/` but all files have been removed.** A previous `DELETE FROM` or file compaction left an empty table. `DESCRIBE EXTENDED` would show a valid table, but `SELECT COUNT(*)` returns 0.

Other possibilities: wrong container name in the path, wrong external location coverage.

---

**A10**
`DESCRIBE EXTENDED` shows full metadata about a Unity Catalog table — not just the schema but also governance information.

The two most important fields when checking internal vs external:
1. **`Type`** — `MANAGED` or `EXTERNAL`
2. **`Location`** — the actual file path where the Delta files are stored

For a managed table, `Location` points to something inside the Unity Catalog metastore storage (a path you did not choose). For an external table, `Location` is the exact `abfss://` path you specified in `CREATE TABLE`.

---

**A11**
No, the data does not need to be re-written. The Delta files are still on ADLS exactly where they were before the drop. Re-registering just creates the metadata entry again:

```sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.payments
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/payments/'
```

After this, all previous data is immediately queryable — same rows, same schema, same Delta time travel history.

---

## External Volumes

**A12**
A Unity Catalog external volume is a catalog object that maps a storage path (ADLS or Blob) to a virtual file system path `/Volumes/<catalog>/<schema>/<volume>/`. It lets notebooks access files on that storage using a consistent path, without needing to know or configure the underlying `abfss://` URL.

Difference from an external table:
- An external table exposes **structured data** with a schema — queryable with `SELECT`
- An external volume exposes **raw files** as a file system path — no schema, accessed with `dbutils.fs` or `spark.read`

---

**A13**
Access in a notebook:

```python
volume_path = "/Volumes/dev_catalog/bronze/blob_files/"

# List files
files = dbutils.fs.ls(volume_path)
for f in files:
    print(f.name, f.size)
```

The `/Volumes/` path works on any cluster in the workspace that has access to the volume. No `abfss://` or auth config is needed — Unity Catalog handles authentication through the external location's storage credential.

---

**A14**
The query fails because `blob_files` is a **volume**, not a table. A volume has no schema — it is a file system, not a queryable relation. `SELECT *` requires a table.

To read data from the volume:

```python
# Step 1 — read the file into a DataFrame
df = spark.read.option("header", "true").csv("/Volumes/dev_catalog/bronze/blob_files/myfile.csv")

# Step 2 — create a temp view to use SQL
df.createOrReplaceTempView("blob_data")
```

```sql
%sql
SELECT * FROM blob_data
```

---

**A15**
Yes, you can write Delta data to a volume path:

```python
df.write.format("delta").save("/Volumes/dev_catalog/bronze/blob_files/my_delta/")
```

To then query it as a SQL table, register it as an external table:

```sql
CREATE TABLE dev_catalog.bronze.my_table
USING DELTA
LOCATION '/Volumes/dev_catalog/bronze/blob_files/my_delta/'
```

Or read it directly without registration:

```python
df = spark.read.format("delta").load("/Volumes/dev_catalog/bronze/blob_files/my_delta/")
```

---

**A16**
The files in `stblobdev001` are **not deleted**. `DROP VOLUME` removes the volume metadata from Unity Catalog — exactly the same behaviour as `DROP TABLE` on an external table. External objects in Unity Catalog are "external" — Databricks does not own the files and will not delete them.

The Blob storage container and all its files remain completely untouched. You can recreate the volume pointing to the same path and the files are immediately accessible again.

---

**A17**
The `/Volumes/` path is a **virtual file system path** managed by Unity Catalog. It is not a real OS-level filesystem path — there is no actual directory called `/Volumes/` on the cluster's disk. Unity Catalog translates it to the underlying `abfss://` path and handles authentication transparently.

Yes, the same `/Volumes/` path works on **any cluster** in the workspace that has access to the volume — a regular cluster, a job cluster, or a SQL warehouse. The user or job running the notebook must have at least `READ VOLUME` privilege on the volume.

---

## Internal Volumes

**A18**
- **Internal volume:** Databricks manages the storage location. Created WITHOUT a `LOCATION` clause:
  ```sql
  CREATE VOLUME dev_catalog.silver.temp_vol
  ```
  Files are stored in the Unity Catalog managed storage. `DROP VOLUME` deletes the files.

- **External volume:** You specify the storage location. Created WITH a `LOCATION` clause:
  ```sql
  CREATE EXTERNAL VOLUME dev_catalog.bronze.blob_files
  LOCATION 'abfss://files@stblobdev001.dfs.core.windows.net/'
  ```
  Files live on your storage. `DROP VOLUME` removes only the metadata — files are kept.

---

**A19**
The files are stored in the Unity Catalog managed storage — a path automatically assigned by Databricks, typically inside the metastore's storage account. You did not choose this path; it looks something like `abfss://unitycatalog@<managed-account>.dfs.core.windows.net/<metastore-id>/...`.

When `DROP VOLUME dev_catalog.silver.temp_files` is run, Databricks **deletes the files** from the managed storage. This is the same behaviour as dropping an internal table — Databricks owns the storage location and cleans it up on drop.

---

**A20**
Choose an internal volume when:
- The files are temporary, intermediate, or only needed during a notebook run
- You do not want to manage the storage location
- The data does not need to be shared with other systems or survive catalog operations

Example: a machine learning training job that downloads a model checkpoint file mid-run and discards it after evaluation. No other system needs this file. Storing it in an internal volume avoids managing a Blob container for a transient artifact.

---

## Decision Making

**A21**

| Use case | Best object |
|---|---|
| Raw CSV files dropped by ADF into Blob Storage | **External Volume** — raw files, no schema yet, files must survive |
| Cleaned salary data that analysts query with SQL | **External Table** — structured, Delta, queryable, should survive catalog changes |
| Temp staging area that only notebooks use | **Internal Volume** or **Internal Table** — Databricks manages it, no sharing needed |
| Delta table on ADLS shared with Synapse Analytics | **External Table** — files on your ADLS, Synapse can also read the same Delta files |

---

**A22**
Steps to migrate from internal to external without data loss:

1. **Read the internal table data into a DataFrame:**
   ```python
   df = spark.read.table("dev_catalog.silver.transactions")
   ```

2. **Write it to the desired external ADLS path as Delta:**
   ```python
   output_path = "abfss://silver@stadlsdev001.dfs.core.windows.net/transactions/"
   df.write.format("delta").mode("overwrite").save(output_path)
   ```

3. **Drop the internal table:**
   ```sql
   DROP TABLE dev_catalog.silver.transactions
   ```

4. **Re-create as an external table pointing to the new path:**
   ```sql
   CREATE TABLE dev_catalog.silver.transactions
   USING DELTA
   LOCATION 'abfss://silver@stadlsdev001.dfs.core.windows.net/transactions/'
   ```

Now the table is external. Future DROP TABLE calls will not delete the data.

---

**A23**
The Medallion landing pattern using volumes and external tables:

```
Step 1 — Raw files land in an External Volume
  ADF drops CSV/JSON files into Blob Storage
  Volume path: /Volumes/dev_catalog/bronze/raw_landing/
  Files are raw, unstructured, append-only

Step 2 — Notebook reads from the volume
  df = spark.read.option("header", "true").csv("/Volumes/.../raw_landing/")
  Applies cleaning, type casting, deduplication

Step 3 — Notebook writes clean data as Delta to ADLS
  df.write.format("delta").mode("append").save("abfss://bronze@stadlsdev001.../cleaned/")

Step 4 — External table registered in Unity Catalog
  CREATE TABLE dev_catalog.bronze.cleaned USING DELTA LOCATION 'abfss://...'

Step 5 — Analysts query with SQL
  SELECT * FROM dev_catalog.bronze.cleaned WHERE date = '2024-01-01'
```

The key: **volumes are for files you cannot control the format of** (raw landing). **Tables are for structured data** you want to query and govern.

---

**A24**
`CREATE TABLE ... LOCATION` succeeds because Unity Catalog does not validate external location coverage at table creation time — it validates at access time (when a query actually reads or writes the path).

When a query runs, Unity Catalog checks: does any external location cover `abfss://silver@stadlsdev001.dfs.core.windows.net/my_table/`? If no external location covers that prefix, the query fails with a permission error even though the table metadata exists.

Fix: create an external location covering the path prefix, or use a path that is already covered by an existing external location.

---

**A25**
Complete strategy:

**Raw JSON files (daily from API via ADF):**
→ **External Volume** at `abfss://files@stblobdev001.dfs.core.windows.net/raw_json/`
- Reason: raw files with no consistent schema, ADF drops them here, must survive catalog changes, archived for 90 days

**Cleaned Delta table (for BI reporting):**
→ **External Table** at `abfss://bronze@stadlsdev001.dfs.core.windows.net/transactions_clean/`
- Reason: structured schema, analysts query with SQL in Databricks SQL or Power BI, must survive catalog changes, supports MERGE/UPDATE for deduplication

**90-day raw archive:**
→ Same **External Volume** or a separate `archive/` folder within the same Blob container
- Files are organised by date: `/raw_json/2024/01/01/batch.json`
- A separate notebook or ADF pipeline moves files older than 90 days to cold storage or deletes them
- An **Internal Volume** would not work here because dropping it would delete the files — the archive must be independent of catalog operations

**Summary:**
```
stblobdev001/raw_json/   → External Volume (landing + 90-day archive)
stadlsdev001/transactions_clean/ → External Table (BI layer)
```
