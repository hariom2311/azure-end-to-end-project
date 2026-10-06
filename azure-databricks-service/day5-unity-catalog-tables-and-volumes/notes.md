# Day 5 — Azure Databricks: Unity Catalog Tables and Volumes

> **Goal:** Understand the difference between internal and external objects in Unity Catalog, then create and work with external Delta tables on ADLS (stadlsdev001) and external volumes on Blob Storage (stblobdev001).
> **Prerequisite:** Day 4 must be complete — Storage Credentials and External Locations must already exist.
> **Storage accounts used:**
> - `stadlsdev001` — ADLS Gen2 → external Delta tables
> - `stblobdev001` — Azure Blob Storage → external volumes

---

## Part 1: Internal vs External — Tables and Volumes

This is a fundamental Unity Catalog concept. Everything in the catalog is either **internal (managed)** or **external**.

### 1.1 The Difference

```
INTERNAL (Managed)
  Data location: Databricks manages it — stored in the Unity Catalog managed storage
  What happens on DROP TABLE: data is DELETED
  You control: the schema and the data, but not the file location
  Use for: data that lives entirely inside Databricks, no sharing with other tools

EXTERNAL
  Data location: you specify — on ADLS, Blob, S3, etc.
  What happens on DROP TABLE: only the metadata is removed, FILES ARE KEPT
  You control: the file location, the schema, and the data
  Use for: data that already exists on storage, shared with ADF, other systems
```

**Analogy:**
- Internal table = a filing cabinet that Databricks owns. Drop the drawer, the files are shredded.
- External table = a filing cabinet you own. Databricks just has a key. Remove Databricks' key, your files are fine.

### 1.2 External Table vs External Volume

```
External TABLE (on stadlsdev001)
  ├── Points to Delta format files on ADLS Gen2
  ├── Queryable with SQL: SELECT * FROM catalog.schema.table_name
  ├── Has a schema (column names and types)
  ├── Supports: ACID transactions, time travel, MERGE, UPDATE, DELETE
  └── Use for: structured data you want to query like a database table

External VOLUME (on stblobdev001)
  ├── Points to a path in Blob Storage (or ADLS)
  ├── Accessed as a file path: /Volumes/catalog/schema/volume_name/
  ├── No schema — raw files (CSV, JSON, images, PDFs, scripts)
  ├── Supports: dbutils.fs operations, spark.read with any format
  └── Use for: raw files, landing zone, binary files, ML training data
```

**Quick comparison:**

| | External Table | External Volume |
|---|---|---|
| Format | Delta (required) | Any file format |
| Access | SQL `SELECT` | File path `/Volumes/...` |
| Storage | `stadlsdev001` (ADLS Gen2) | `stblobdev001` (Blob) |
| Schema | Yes — columns and types | No — just files |
| SQL queryable | Yes | No (read with spark.read) |
| DROP removes files | No — only metadata | No — only metadata |
| Use case | Structured data / analytics | Raw files / landing zone |

---

## Part 2: External Delta Tables on stadlsdev001

An external Delta table points to a Delta-format folder on ADLS. We first write a Delta file to ADLS, then register it as an external table.

> **Prerequisite:** The admin has already created (from Day 4):
> - Storage Credential `sp-stadls-credential` pointing to the Service Principal
> - External Location `ext-loc-stadls` pointing to `abfss://bronze@stadlsdev001.dfs.core.windows.net/`
>
> With that in place, notebooks need **zero auth config** — just use the path directly.

### Step 1 — Verify Access (Unity Catalog way — no spark.conf needed)

Open a new notebook, attach to your cluster, run:

```python
# Unity Catalog handles auth automatically — just use the path
# No spark.conf.set(), no dbutils.secrets.get() for storage access

path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/"

try:
    files = dbutils.fs.ls(path)
    print(f"Access confirmed — {len(files)} items in container")
    for f in files:
        print(f.name)
except Exception as e:
    print(f"Access failed: {e}")
    print("Check: External Location ext-loc-stadls exists and covers this path")
```

If this fails, the admin needs to complete Day 4 Part 5 (Storage Credential + External Location) first.

**Legacy reference only — what the old approach looked like:**
```python
# ❌ Old way — do NOT use with Unity Catalog
# spark.conf.set("fs.azure.account.auth.type.stadlsdev001...", "OAuth")
# spark.conf.set("fs.azure.account.oauth2.client.id...", ...)
# spark.conf.set("fs.azure.account.oauth2.client.secret...", ...)
# Unity Catalog replaces all of this
```

### Step 2 — Write Sample Data as Delta to ADLS

```python
# Create sample data and write it to ADLS as Delta format
data = [
    (1, "Alice",   "completed", 250.00),
    (2, "Bob",     "failed",    100.00),
    (3, "Carol",   "completed", 430.50),
    (4, "David",   "pending",   200.00),
    (5, "Eve",     "completed",  75.00),
]
columns = ["id", "name", "status", "amount"]

df = spark.createDataFrame(data, columns)

# Write to ADLS as Delta
delta_path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/"
df.write.format("delta").mode("overwrite").save(delta_path)

print(f"Delta files written to: {delta_path}")
```

Verify files exist:
```python
dbutils.fs.ls(delta_path)
```

You should see `_delta_log/` folder and `.parquet` files — this is a Delta table on storage.

### Step 3 — Register as External Table in Unity Catalog

Run this SQL in a notebook cell:

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.sample_people
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/'
```

### Step 4 — Query the External Table

```sql
%sql
SELECT * FROM dev_catalog.bronze.sample_people
```

```sql
%sql
SELECT status, COUNT(*) AS total, SUM(amount) AS total_amount
FROM dev_catalog.bronze.sample_people
GROUP BY status
```

### Step 5 — Confirm It Is External

```sql
%sql
DESCRIBE EXTENDED dev_catalog.bronze.sample_people
```

Look for the row `Type` — it will say `EXTERNAL`. Also see `Location` showing the ADLS path.

### Step 6 — Test: DROP Does Not Delete Files

```sql
%sql
-- Drop the table (removes metadata only)
DROP TABLE IF EXISTS dev_catalog.bronze.sample_people
```

Now check if files still exist on ADLS:
```python
dbutils.fs.ls("abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/")
```

Files are still there. The table is gone from the catalog, but the data on storage is untouched.

Re-register it:
```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.sample_people
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/'
```

---

## Part 3: External Volume on stblobdev001

A volume makes a storage path accessible as `/Volumes/<catalog>/<schema>/<volume>/` from any notebook — like mounting a drive.

### Step 1 — Create the External Volume

```sql
%sql
CREATE EXTERNAL VOLUME IF NOT EXISTS dev_catalog.bronze.blob_files
LOCATION 'abfss://files@stblobdev001.dfs.core.windows.net/'
```

> This registers the root of the `files` container in `stblobdev001` as a Unity Catalog volume.

### Step 2 — Access the Volume as a File Path

```python
# The volume is now accessible at this path from any notebook
volume_path = "/Volumes/dev_catalog/bronze/blob_files/"

# List files in the volume
files = dbutils.fs.ls(volume_path)
for f in files:
    print(f.name, f.size)
```

### Step 3 — Write a File to the Volume

```python
# Write a simple text file into the volume (into Blob Storage)
dbutils.fs.put(f"{volume_path}test_file.txt", "Hello from Databricks volume!", overwrite=True)
print("File written to volume")
```

### Step 4 — Read a CSV from the Volume

If you upload a CSV to the Blob container manually (via Azure Portal → Storage account → Upload), you can read it:

```python
# Read a CSV from the volume path
df = spark.read.option("header", "true").csv(f"{volume_path}myfile.csv")
df.show()
```

### Step 5 — Understand: Volume Is Not a Table

You cannot `SELECT * FROM dev_catalog.bronze.blob_files` — a volume is a file system, not a table. To query the data as a table, read it into a DataFrame first:

```python
# Read the CSV from the volume into a DataFrame
df = spark.read.option("header", "true").csv(f"{volume_path}myfile.csv")

# Register as a temp view to query with SQL
df.createOrReplaceTempView("blob_data")
```

```sql
%sql
SELECT * FROM blob_data LIMIT 10
```

---

## Part 4: Internal vs External — Side-by-Side Demo

Run these two examples to clearly see the difference.

### Internal (Managed) Table

```sql
%sql
-- Create an internal table — Databricks manages the file location
CREATE TABLE IF NOT EXISTS dev_catalog.silver.managed_example (
    id     INT,
    name   STRING,
    score  DOUBLE
)
USING DELTA
```

```sql
%sql
INSERT INTO dev_catalog.silver.managed_example VALUES
(1, 'Alice', 95.5),
(2, 'Bob',   82.0),
(3, 'Carol', 91.0)
```

```sql
%sql
-- Where did Databricks store the files?
DESCRIBE EXTENDED dev_catalog.silver.managed_example
```

Look at `Location` — it shows a path inside the Unity Catalog managed storage (not your ADLS). You did not choose this path.

```sql
%sql
-- Drop the internal table — DATA IS DELETED
DROP TABLE dev_catalog.silver.managed_example
```

The files are gone — there is no `Location` to check because Databricks deleted them.

### External Table (recap)

```sql
%sql
-- Create an external table — you specify the location
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.external_example
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/external_example/'
```

```sql
%sql
INSERT INTO dev_catalog.bronze.external_example VALUES
(1, 'Alice', 95.5),
(2, 'Bob',   82.0)
```

```sql
%sql
DROP TABLE dev_catalog.bronze.external_example
-- Files on stadlsdev001 still exist
```

**The rule:** Use external tables when the data is owned by your team, shared with other systems (ADF, Synapse), or must survive a catalog drop. Use internal tables for temporary or intermediate data that only Databricks needs.

---

## Part 5: Internal Volume (Managed)

An internal volume is created without specifying a location — Databricks manages the storage path inside the Unity Catalog managed storage.

### Step 1 — Create an Internal Volume

```sql
%sql
CREATE VOLUME IF NOT EXISTS dev_catalog.silver.managed_volume
```

> No `LOCATION` clause → internal volume. Databricks picks the storage path automatically.

### Step 2 — Write to the Internal Volume

```python
volume_path = "/Volumes/dev_catalog/silver/managed_volume/"

# Write a file
dbutils.fs.put(f"{volume_path}hello.txt", "This file lives in Databricks-managed storage", overwrite=True)
print("File written to internal volume")
```

### Step 3 — Check Where It Is Stored

```sql
%sql
DESCRIBE VOLUME dev_catalog.silver.managed_volume
```

Look at `Storage Location` — Databricks assigned it a path inside the Unity Catalog metastore storage. You did not choose this path.

### Step 4 — Drop Behavior

```sql
%sql
DROP VOLUME dev_catalog.silver.managed_volume
```

Unlike an external volume, dropping an internal volume **deletes the files** from the Databricks-managed storage.

---

## Part 6: Summary — When to Use Each Object Type

```
Decision tree:

Is the data structured with a schema you can query with SQL?
  YES → use a TABLE
    ├── Does the data already exist on ADLS, or must survive Databricks being deleted?
    │     YES → External Table (LOCATION = your ADLS path)
    │     NO  → Internal (Managed) Table (Databricks picks the path)
    └── Use case: analytics, BI, reporting, MERGE/UPDATE operations

Is the data raw files — CSV, JSON, images, scripts, no fixed schema?
  YES → use a VOLUME
    ├── Does the data live on your own storage (Blob/ADLS)?
    │     YES → External Volume (LOCATION = your container path)
    │     NO  → Internal Volume (Databricks manages the storage)
    └── Use case: landing zone, ML training data, binary files, ADF drop zone
```

**Quick rule of thumb:**
- Production data shared with other teams → External Table or External Volume
- Intermediate/temp data only Databricks needs → Internal Table or Internal Volume
- Raw files without structure → Volume (internal or external)
- Structured data with schema → Table (internal or external)

---

## Quick Reference — Day 5 Terminologies

```
Term                      Definition
───────────────────────────────────────────────────────────────────────────────
Internal (Managed) Table  A Delta table whose files are owned by Databricks —
                          stored in UC managed storage, deleted on DROP TABLE

External Table            A Delta table whose files live on your own storage
                          (ADLS) — DROP TABLE removes only metadata, not files

Internal Volume           A volume whose storage is managed by Databricks inside
                          the Unity Catalog managed storage location

External Volume           A Unity Catalog object that exposes a storage path
                          (Blob/ADLS) as /Volumes/catalog/schema/volume/

LOCATION clause           The abfss:// path you specify when creating an external
                          table or volume — omit it for internal/managed objects

_delta_log/               The transaction log folder that makes a folder a Delta
                          table — Unity Catalog reads it on every query

DROP TABLE                Removes the table metadata from Unity Catalog only.
                          For external tables: files on ADLS are kept.
                          For internal tables: files are deleted.

DROP VOLUME               Removes the volume from Unity Catalog only.
                          For external volumes: files on storage are kept.
                          For internal volumes: files are deleted.

DESCRIBE EXTENDED         Shows full table metadata including Type (MANAGED or
                          EXTERNAL) and the storage Location

/Volumes/                 The virtual file system path prefix for Unity Catalog
                          volumes — same path works on any cluster in the workspace
```
