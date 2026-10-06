# Day 6 — Practice Exercises: Unity Catalog, Tables, Volumes & ADF

> **Folder for all notebooks:** `Shared/day6-practice`
> **All exercises use only the resources set up in Day 6 notes — no assumptions.**
> Complete them in order — each builds on the previous.

---

## Exercise 1 — Storage Credential + External Location Setup Verification

**Goal:** Confirm that the Storage Credentials and External Locations from the notes are working correctly before writing any code.

**Steps:**

1. In your Databricks workspace, go to **Catalog** (left sidebar)
2. Click **External Data** → **Credentials**
   - Verify `sp-stadls-credential` exists
   - Verify `sp-stblob-credential` exists
   - If missing, follow Day 6 notes Part 2 to create them

3. Click **External Locations**
   - Verify `ext-loc-stadls` exists with URL `abfss://bronze@stadlsdev001.dfs.core.windows.net/`
   - Verify `ext-loc-stblob` exists with URL `abfss://files@stblobdev001.dfs.core.windows.net/`
   - Click each one → **Test connection** → confirm `Connection successful`
   - If missing, follow Day 6 notes Part 3 to create them

4. Create notebook `ex1_verify_access` in `Shared/day6-practice`, attach to cluster, run:

```python
# Test ADLS access (stadlsdev001)
try:
    files = dbutils.fs.ls("abfss://bronze@stadlsdev001.dfs.core.windows.net/")
    print(f"stadlsdev001 access: OK ({len(files)} items)")
except Exception as e:
    print(f"stadlsdev001 access: FAILED — {e}")

# Test Blob access (stblobdev001)
try:
    files = dbutils.fs.ls("abfss://files@stblobdev001.dfs.core.windows.net/")
    print(f"stblobdev001 access: OK ({len(files)} items)")
except Exception as e:
    print(f"stblobdev001 access: FAILED — {e}")
```

**Expected output:**
```
stadlsdev001 access: OK (N items)
stblobdev001 access: OK (N items)
```

Both must succeed before proceeding to Exercise 2.

---

## Exercise 2 — Create and Populate an External Delta Table

**Goal:** Write data to ADLS as Delta, register it as an external table, and query it with SQL.

**Steps:**

1. Create notebook `ex2_external_table` in `Shared/day6-practice`, attach to cluster

2. Write employee data to ADLS as Delta:

```python
data = [
    (101, "Priya",   "Engineering", "Bangalore",  90000),
    (102, "Rahul",   "Marketing",   "Mumbai",     75000),
    (103, "Ananya",  "Engineering", "Hyderabad",  88000),
    (104, "Vikram",  "Sales",       "Delhi",      65000),
    (105, "Sneha",   "HR",          "Bangalore",  58000),
    (106, "Arjun",   "Engineering", "Pune",       92000),
    (107, "Meera",   "Finance",     "Mumbai",     78000),
]
columns = ["emp_id", "name", "department", "city", "salary"]

df = spark.createDataFrame(data, columns)

delta_path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/employees/"
df.write.format("delta").mode("overwrite").save(delta_path)
print(f"Written to: {delta_path}")
```

3. Verify the Delta files exist:

```python
for f in dbutils.fs.ls(delta_path):
    print(f.name, "-", f.size, "bytes")
```

4. Register as external table:

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.employees
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/employees/'
```

5. Query with SQL:

```sql
%sql
SELECT department, COUNT(*) AS headcount, AVG(salary) AS avg_salary
FROM dev_catalog.bronze.employees
GROUP BY department
ORDER BY avg_salary DESC
```

6. Run `DESCRIBE EXTENDED` and confirm `Type = EXTERNAL` and `Location = abfss://bronze@stadlsdev001.dfs.core.windows.net/employees/`

7. Drop the table and confirm the files still exist:

```sql
%sql
DROP TABLE dev_catalog.bronze.employees
```

```python
files = dbutils.fs.ls("abfss://bronze@stadlsdev001.dfs.core.windows.net/employees/")
print(f"Files on ADLS after DROP TABLE: {len(files)} items — data is safe")
```

8. Re-register and query again — data comes back instantly:

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.employees
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/employees/'
```

```sql
%sql
SELECT * FROM dev_catalog.bronze.employees ORDER BY emp_id
```

---

## Exercise 3 — Internal Table: Observe File Management

**Goal:** Create an internal table, find where Databricks stored the files, drop it, and confirm files are deleted.

**Steps:**

1. Create notebook `ex3_internal_table` in `Shared/day6-practice`

2. Create an internal Delta table:

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.silver.dept_summary (
    department STRING,
    headcount  INT,
    avg_salary DOUBLE
)
USING DELTA
```

3. Insert data:

```sql
%sql
INSERT INTO dev_catalog.silver.dept_summary VALUES
('Engineering', 3, 90000.0),
('Marketing',   1, 75000.0),
('Sales',       1, 65000.0)
```

4. Find the storage location:

```sql
%sql
DESCRIBE EXTENDED dev_catalog.silver.dept_summary
```

Copy the value from the `Location` row — it will be a Databricks-managed path like:
`abfss://unitycatalog@<account>.dfs.core.windows.net/<guid>/dev_catalog/silver/dept_summary`

5. Drop the table:

```sql
%sql
DROP TABLE dev_catalog.silver.dept_summary
```

6. Try to access the location you copied — confirm files are gone:

```python
managed_location = "paste-the-location-you-copied-from-DESCRIBE-EXTENDED"

try:
    files = dbutils.fs.ls(managed_location)
    print(f"UNEXPECTED: {len(files)} files still exist")
except Exception as e:
    print("CONFIRMED: Files are deleted — internal table drop removed the data")
    print(f"Error: {e}")
```

**Key observation:** The internal table's files at the managed location are gone — Databricks deleted them. This is why internal tables should NEVER be used for production data that must be preserved.

---

## Exercise 4 — External Volume: Land, Read, Process

**Goal:** Use an external volume as a landing zone, read raw files, and process them.

**Steps:**

1. Create notebook `ex4_volume_landing` in `Shared/day6-practice`

2. Ensure the external volume exists:

```sql
%sql
CREATE EXTERNAL VOLUME IF NOT EXISTS dev_catalog.bronze.blob_files
LOCATION 'abfss://files@stblobdev001.dfs.core.windows.net/'
```

3. Write three "landed" JSON files to the volume:

```python
volume_path = "/Volumes/dev_catalog/bronze/blob_files/"

# Day 1 data
dbutils.fs.put(f"{volume_path}sensors_day1.json",
    '[{"sensor_id": "S001", "temp": 72.5, "humidity": 65},'
    ' {"sensor_id": "S002", "temp": 68.1, "humidity": 70}]',
    overwrite=True)

# Day 2 data
dbutils.fs.put(f"{volume_path}sensors_day2.json",
    '[{"sensor_id": "S001", "temp": 74.2, "humidity": 62},'
    ' {"sensor_id": "S003", "temp": 80.0, "humidity": 55}]',
    overwrite=True)

print("JSON files landed:")
for f in dbutils.fs.ls(volume_path):
    if f.name.endswith(".json"):
        print(f"  {f.name}  ({f.size} bytes)")
```

4. Read all JSON files from the volume:

```python
df = spark.read \
    .option("multiline", "true") \
    .json(volume_path + "sensors_day*.json")

print(f"Total records: {df.count()}")
df.show()
df.printSchema()
```

5. Find the maximum temperature per sensor:

```python
from pyspark.sql.functions import max as spark_max

df_summary = df.groupBy("sensor_id") \
    .agg(spark_max("temp").alias("max_temp"), spark_max("humidity").alias("max_humidity")) \
    .orderBy("sensor_id")

df_summary.show()
```

6. Save the summary as an external Delta table on ADLS:

```python
summary_path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/sensor_summary/"
df_summary.write.format("delta").mode("overwrite").save(summary_path)
```

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.sensor_summary
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/sensor_summary/'
```

```sql
%sql
SELECT * FROM dev_catalog.bronze.sensor_summary
```

---

## Exercise 5 — Internal Volume: Temp Files That Should Not Persist

**Goal:** Use an internal volume for temporary processing files, then clean up.

**Steps:**

1. Create notebook `ex5_internal_volume` in `Shared/day6-practice`

2. Create an internal volume:

```sql
%sql
CREATE VOLUME IF NOT EXISTS dev_catalog.silver.temp_processing
```

3. Describe it to see the managed storage location:

```sql
%sql
DESCRIBE VOLUME dev_catalog.silver.temp_processing
```

4. Write a temporary file:

```python
temp_path = "/Volumes/dev_catalog/silver/temp_processing/"
dbutils.fs.put(f"{temp_path}temp_checkpoint.txt",
    "Processing state: step=3, last_id=1005, status=running",
    overwrite=True)

content = dbutils.fs.head(f"{temp_path}temp_checkpoint.txt")
print(f"Checkpoint written: {content}")
```

5. List files:

```python
for f in dbutils.fs.ls(temp_path):
    print(f.name, f.size, "bytes")
```

6. Clean up — drop the volume (this deletes the temp files):

```sql
%sql
DROP VOLUME dev_catalog.silver.temp_processing
```

7. Confirm the path is gone:

```python
try:
    dbutils.fs.ls("/Volumes/dev_catalog/silver/temp_processing/")
except Exception as e:
    print("Volume and files cleaned up successfully")
```

**Use case:** Internal volumes are ideal for temporary checkpoint files, intermediate processing artifacts, or scratch space — data that should not persist after the job completes.

---

## Exercise 6 — ADF Notebook Activity (Manual Trigger + Output Capture)

**Goal:** Set up the Databricks Linked Service in ADF, create a pipeline, trigger it, and see the notebook output in ADF.

**Steps:**

**Part A — Prepare the notebook in Databricks**

1. Open or create notebook `adf_triggered_notebook` in `Shared/day6-practice`
2. Verify it contains (from Day 6 notes Part 9.2):
   - Cell 1: `dbutils.widgets.text(...)` for `adf_pipeline_name`, `adf_run_id`, `env`
   - Cell 2: Creates a DataFrame, shows it, prints row count
   - Cell 3: `dbutils.notebook.exit(f"SUCCESS: ...")`

3. Generate a PAT token (if you do not already have one):
   - Top right of Databricks workspace → click your username → **User Settings**
   - Left menu → **Developer** → **Access tokens**
   - Click **Generate new token**
   - Comment: `adf-day6`
   - Lifetime: `90`
   - Click **Generate**
   - Copy the token — shown only once

**Part B — Create the Linked Service in ADF**

1. Azure Portal → **Data factories** → `adf-ev-dev` → **Launch studio**
2. Left sidebar → **Manage** → **Linked services** → **+ New**
3. Search `Databricks` → **Azure Databricks** → **Continue**

Fill in:

| Field | Value |
|---|---|
| Name | `ls_databricks_dev` |
| Azure subscription | your subscription |
| Databricks workspace | `dbw-ev-dev` |
| Select cluster | `New job cluster` |
| Runtime version | `15.4 LTS` |
| Worker node type | `Standard_D4s_v3` |
| Workers | `1` |
| Authentication type | `Access token` |
| Access token | paste your PAT token |

4. Click **Test connection** → `Connection successful`
5. Click **Apply**

**Part C — Create the pipeline**

1. Left sidebar → **Author** → **+** → **New pipeline**
2. Name: `pl_day6_ex6_notebook`
3. Activities panel → **Databricks** → drag **Notebook** onto canvas

Configure the activity:

- **General tab:** Name = `Run Day6 Test Notebook`
- **Azure Databricks tab:** Linked service = `ls_databricks_dev`
- **Settings tab:**
  - Notebook path = `/Shared/day6-practice/adf_triggered_notebook`
  - Base parameters:
    - `adf_pipeline_name` → `@pipeline().Pipeline`
    - `adf_run_id` → `@pipeline().RunId`
    - `env` → `dev`

**Part D — Run and observe**

1. Click **Debug** → **OK**
2. Watch the Output tab at the bottom — the activity will show blue (running)
3. Wait 5–8 minutes (cluster startup + notebook execution)
4. When green tick appears, click the **Output** icon on the activity row
5. In the JSON output, find `runOutput` — it should show:
   ```
   "runOutput": "SUCCESS: 5 records processed in dev"
   ```

6. Verify in Databricks:
   - Left sidebar → **Workflows** → **Job runs**
   - See a run with source `ADF`
   - Click it → **Logs** → see cell output

---

## Exercise 7 — End-to-End Pipeline: Volume → Transform → External Table → ADF

**Goal:** Build a complete pipeline: ADF drops CSV to a volume, ADF triggers a Databricks notebook that reads the CSV, transforms it, and writes to an external Delta table.

**Part A — Create the transformation notebook in Databricks**

1. `Shared/day6-practice` → Create → Notebook
2. Name: `transform_orders`
3. Attach to cluster

```python
# Cell 1 — Parameters
dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("input_volume_path",  "/Volumes/dev_catalog/bronze/blob_files/", "Input Volume Path")
dbutils.widgets.text("output_table", "dev_catalog.bronze.orders_final", "Output Table")

env              = dbutils.widgets.get("env")
input_path       = dbutils.widgets.get("input_volume_path")
output_table     = dbutils.widgets.get("output_table")
output_adls_path = f"abfss://bronze@stadlsdev001.dfs.core.windows.net/orders_final_{env}/"

print(f"env:          {env}")
print(f"input_path:   {input_path}")
print(f"output_table: {output_table}")
```

```python
# Cell 2 — Read CSVs from the volume
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path + "*.csv")

print(f"Raw rows: {df_raw.count()}")
df_raw.show()
```

```python
# Cell 3 — Transform
from pyspark.sql.functions import col, round as spark_round, upper

df_clean = df_raw \
    .withColumn("total_value", spark_round(col("quantity").cast("double") * col("price").cast("double"), 2)) \
    .withColumn("customer", upper(col("customer")))

df_clean.show()
```

```python
# Cell 4 — Write as Delta to ADLS
df_clean.write.format("delta").mode("overwrite").save(output_adls_path)
print(f"Written to: {output_adls_path}")
```

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.orders_final
USING DELTA
LOCATION '${output_adls_path}'
```

```python
# Cell 5 — Return result to ADF
row_count = df_clean.count()
dbutils.notebook.exit(f"SUCCESS: {row_count} rows written to {output_table}")
```

> Note: The `%sql` cell with `${output_adls_path}` widget substitution may not work — use Python instead:
>
> ```python
> spark.sql(f"""
>     CREATE TABLE IF NOT EXISTS {output_table}
>     USING DELTA
>     LOCATION '{output_adls_path}'
> """)
> ```

**Part B — Set up CSV data in the volume (simulate ADF Copy Activity)**

Run this in any notebook:

```python
volume_path = "/Volumes/dev_catalog/bronze/blob_files/"

dbutils.fs.put(f"{volume_path}orders_jan.csv",
    "order_id,customer,product,quantity,price\n"
    "2001,alice,Laptop,1,75000\n2002,bob,Mouse,3,1500\n2003,carol,Keyboard,2,3000",
    overwrite=True)

dbutils.fs.put(f"{volume_path}orders_feb.csv",
    "order_id,customer,product,quantity,price\n"
    "2004,david,Monitor,1,22000\n2005,eve,Laptop,1,75000\n2006,frank,Chair,4,8500",
    overwrite=True)

print("CSV files ready in volume")
```

**Part C — Create ADF pipeline with the notebook activity**

1. ADF Studio → **Author** → **+** → **New pipeline**
2. Name: `pl_day6_ex7_full_pipeline`
3. Add **Notebook** activity with:
   - Linked service: `ls_databricks_dev`
   - Notebook path: `/Shared/day6-practice/transform_orders`
   - Base parameters:
     - `env` → `dev`
     - `input_volume_path` → `/Volumes/dev_catalog/bronze/blob_files/`
     - `output_table` → `dev_catalog.bronze.orders_final`

4. Click **Debug** → wait for completion

5. When succeeded, check the output table in Databricks:

```sql
%sql
SELECT customer, product, quantity, price, total_value
FROM dev_catalog.bronze.orders_final
ORDER BY total_value DESC
```

---
