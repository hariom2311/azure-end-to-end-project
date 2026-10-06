# Day 5 — Practice Exercises: Unity Catalog Tables and Volumes

> **Folder for all notebooks:** `Shared/day5-practice`
> **Prerequisites:** Day 4 complete — Storage Credentials and External Locations already created.
> Attempt each exercise before reading the hint.

---

## Exercise 1 — Identify Internal vs External

**Goal:** Run `DESCRIBE EXTENDED` on two existing tables and tell the difference.

**Steps:**

1. Open a notebook in `Shared/day5-practice`, name it `ex1_identify_table_type`
2. Create one internal table and one external table:

```sql
%sql
-- Internal table (no LOCATION)
CREATE TABLE IF NOT EXISTS dev_catalog.silver.internal_demo (
    id    INT,
    label STRING
)
USING DELTA;

-- External table (with LOCATION)
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.external_demo
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/external_demo/'
```

3. Run `DESCRIBE EXTENDED` on both:

```sql
%sql
DESCRIBE EXTENDED dev_catalog.silver.internal_demo
```

```sql
%sql
DESCRIBE EXTENDED dev_catalog.bronze.external_demo
```

4. Find the row `Type` in each output. Write down:
   - What value does `Type` show for the internal table?
   - What value does `Type` show for the external table?
   - What does `Location` show for each?

**Expected result:** `Type = MANAGED` for internal, `Type = EXTERNAL` for external. Location for the external table points to your ADLS path. Location for the internal table points to a Databricks-managed path.

---

## Exercise 2 — DROP Behavior: External Table Keeps Files

**Goal:** Prove that dropping an external table does not delete the files on ADLS.

**Steps:**

1. Create notebook `ex2_drop_external_table`
2. Write some data as Delta to ADLS:

```python
data = [(1, "keep me"), (2, "also keep me"), (3, "do not delete")]
df = spark.createDataFrame(data, ["id", "message"])

delta_path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/drop_test/"
df.write.format("delta").mode("overwrite").save(delta_path)
print("Written to ADLS")
```

3. Register as external table:

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.drop_test
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/drop_test/'
```

4. Verify you can query it:

```sql
%sql
SELECT * FROM dev_catalog.bronze.drop_test
```

5. Drop the table:

```sql
%sql
DROP TABLE dev_catalog.bronze.drop_test
```

6. Verify the files still exist on ADLS:

```python
files = dbutils.fs.ls("abfss://bronze@stadlsdev001.dfs.core.windows.net/drop_test/")
for f in files:
    print(f.name)
```

7. Re-register the table without re-writing the data:

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.drop_test
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/drop_test/'
```

8. Confirm you can query it again — same data, no re-write needed.

**Key takeaway:** External tables store metadata separately from data. DROP TABLE = remove the catalog entry, not the files.

---

## Exercise 3 — DROP Behavior: Internal Table Deletes Files

**Goal:** See what happens when an internal table is dropped.

**Steps:**

1. Create notebook `ex3_drop_internal_table`
2. Create an internal table and insert data:

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.silver.internal_drop_test (
    id      INT,
    message STRING
)
USING DELTA
```

```sql
%sql
INSERT INTO dev_catalog.silver.internal_drop_test VALUES
(1, 'this data will be deleted'),
(2, 'so will this')
```

3. Find where Databricks stored the files:

```sql
%sql
DESCRIBE EXTENDED dev_catalog.silver.internal_drop_test
```

Copy the `Location` value from the output — something like `abfss://unitycatalogstore@...`.

4. Drop the table:

```sql
%sql
DROP TABLE dev_catalog.silver.internal_drop_test
```

5. Try to list the files at the location you copied:

```python
# Paste the location you copied from DESCRIBE EXTENDED
location = "abfss://unitycatalogstore@..."
dbutils.fs.ls(location)
```

**Expected result:** An error — the directory no longer exists. Databricks deleted the files when the table was dropped.

---

## Exercise 4 — Create and Use an External Volume

**Goal:** Create an external volume on Blob Storage and access it as a file path.

**Steps:**

1. Create notebook `ex4_external_volume`

2. Create the external volume:

```sql
%sql
CREATE EXTERNAL VOLUME IF NOT EXISTS dev_catalog.bronze.blob_files
LOCATION 'abfss://files@stblobdev001.dfs.core.windows.net/'
```

3. Write a file to the volume:

```python
volume_path = "/Volumes/dev_catalog/bronze/blob_files/"
dbutils.fs.put(f"{volume_path}exercise4.txt", "Hello from Day 5 exercise!", overwrite=True)
print("File written")
```

4. Read it back:

```python
content = dbutils.fs.head(f"{volume_path}exercise4.txt")
print(content)
```

5. List files in the volume:

```python
for f in dbutils.fs.ls(volume_path):
    print(f.name, f.size)
```

6. Write a CSV to the volume and read it as a DataFrame:

```python
# Write a CSV using Python
csv_content = "id,product,price\n1,Apple,1.50\n2,Banana,0.75\n3,Cherry,3.00"
dbutils.fs.put(f"{volume_path}products.csv", csv_content, overwrite=True)

# Read it back as a DataFrame
df = spark.read.option("header", "true").csv(f"{volume_path}products.csv")
df.show()
```

7. Try to run this and note the error:

```sql
%sql
SELECT * FROM dev_catalog.bronze.blob_files
```

**Expected result:** Error — `blob_files` is a volume, not a table. A volume has no schema.

---

## Exercise 5 — Volume vs Table: Same Data, Different Access

**Goal:** Compare accessing the same data as a volume file vs as a Delta table.

**Steps:**

1. Create notebook `ex5_volume_vs_table`

2. Write sample data as Delta to ADLS:

```python
data = [
    (1, "Engineering", 85000),
    (2, "Marketing",   72000),
    (3, "Sales",       68000),
    (4, "HR",          65000),
]
df = spark.createDataFrame(data, ["id", "department", "salary"])

delta_path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/salary_data/"
df.write.format("delta").mode("overwrite").save(delta_path)
print("Delta written to ADLS")
```

3. Register as external table and query with SQL:

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.salary_data
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/salary_data/'
```

```sql
%sql
SELECT department, salary FROM dev_catalog.bronze.salary_data ORDER BY salary DESC
```

4. Now access the same path using a volume (treating it as raw files):

```python
# Access via volume path
files = dbutils.fs.ls("abfss://bronze@stadlsdev001.dfs.core.windows.net/salary_data/")
for f in files:
    print(f.name)
```

5. Compare the two approaches — fill in this table in your notes:

| | External Table | Volume path |
|---|---|---|
| Access method | SQL `SELECT` | `dbutils.fs.ls()` |
| Needs schema | Yes | No |
| Can filter/aggregate in SQL | Yes | Not directly |
| Use case | Analytics queries | File inspection, raw processing |

---

## Exercise 6 — Internal Volume

**Goal:** Create an internal (managed) volume and observe that Databricks picks the location.

**Steps:**

1. Create notebook `ex6_internal_volume`

2. Create an internal volume (no LOCATION clause):

```sql
%sql
CREATE VOLUME IF NOT EXISTS dev_catalog.silver.managed_vol
```

3. Describe the volume to see where Databricks stored it:

```sql
%sql
DESCRIBE VOLUME dev_catalog.silver.managed_vol
```

Look at `Storage Location` — Databricks assigned a path inside the Unity Catalog metastore storage.

4. Write a file to the internal volume:

```python
vol_path = "/Volumes/dev_catalog/silver/managed_vol/"
dbutils.fs.put(f"{vol_path}test.txt", "Managed volume file", overwrite=True)
print(dbutils.fs.head(f"{vol_path}test.txt"))
```

5. Drop the volume:

```sql
%sql
DROP VOLUME dev_catalog.silver.managed_vol
```

6. Try to access the path — the directory is gone.

**Key takeaway:** Internal volumes are fully managed. Drop the volume = delete the files. External volumes leave the files on your storage.

---

## Exercise 7 — Query Pipeline: Volume → DataFrame → Table

**Goal:** Simulate a real landing zone pattern — raw files land in a volume, you read them into a DataFrame, then save as an external Delta table.

**Steps:**

1. Create notebook `ex7_landing_to_table`

2. Simulate files landing in the volume (as if ADF dropped them):

```python
volume_path = "/Volumes/dev_catalog/bronze/blob_files/"

# Write two "landed" CSVs
dbutils.fs.put(f"{volume_path}batch_001.csv",
    "id,name,amount\n101,Alice,500\n102,Bob,300\n103,Carol,750",
    overwrite=True)

dbutils.fs.put(f"{volume_path}batch_002.csv",
    "id,name,amount\n201,David,420\n202,Eve,610",
    overwrite=True)

print("Files landed in volume")
```

3. Read all CSVs from the volume:

```python
df = spark.read.option("header", "true").option("inferSchema", "true").csv(volume_path)
df.show()
print(f"Total rows: {df.count()}")
```

4. Save as a Delta external table on ADLS:

```python
output_path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/processed_batches/"
df.write.format("delta").mode("overwrite").save(output_path)
print(f"Written to: {output_path}")
```

5. Register as external table:

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.processed_batches
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/processed_batches/'
```

6. Query it:

```sql
%sql
SELECT name, SUM(amount) AS total
FROM dev_catalog.bronze.processed_batches
GROUP BY name
ORDER BY total DESC
```

**This is the standard Medallion landing pattern:** raw files land in a volume (Blob/ADLS) → notebooks process them → output saved as external Delta tables → analysts query the tables with SQL.

---
