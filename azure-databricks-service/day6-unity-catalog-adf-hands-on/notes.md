# Day 6 — Azure Databricks: Unity Catalog, Tables, Volumes & ADF — Full Hands-On

> **Goal:** End-to-end hands-on session. Starting from zero Unity Catalog setup, you will create Storage Credentials, External Locations, a Catalog, Schemas, External Delta Tables, External Volumes, Internal Tables, Internal Volumes — and wire a Databricks notebook into an ADF pipeline. Every step is shown exactly as it appears in the Azure portal and Databricks UI.
>
> **Storage accounts in this project:**
> - `stadlsdev001` — ADLS Gen2 with Hierarchical Namespace (HNS) enabled → used for Delta tables
> - `stblobdev001` — Azure Blob Storage → used for volumes
>
> **Cluster needed:** An all-purpose cluster already running in your workspace (`dbw-ev-dev`). If none is running, go to Compute → Create cluster before starting.

---

## Part 1: Unity Catalog Hierarchy — What You Are Building

Before touching any UI, understand what you are going to create and why.

```
Azure Databricks Account (accounts.azuredatabricks.net)
  └── Unity Catalog Metastore  (one per Azure region — already exists)
        ├── Storage Credential: sp-stadls-credential   ← holds SP identity for stadlsdev001
        ├── Storage Credential: sp-stblob-credential   ← holds SP identity for stblobdev001
        ├── External Location:  ext-loc-stadls          ← maps abfss://bronze@stadlsdev001...
        ├── External Location:  ext-loc-stblob          ← maps abfss://files@stblobdev001...
        └── Catalog: dev_catalog
              ├── Schema: bronze
              │     ├── External Table: sample_people  ← Delta files on stadlsdev001
              │     └── External Volume: blob_files    ← raw files on stblobdev001
              └── Schema: silver
                    ├── Internal Table: managed_example ← Databricks owns the files
                    └── Internal Volume: managed_volume ← Databricks owns the files
```

**Why this order matters:**
- Storage Credential must exist before External Location (External Location points to a credential)
- External Location must exist before External Table/Volume (tables and volumes need a covered path)
- Catalog and Schema must exist before Table/Volume (tables live inside schemas)

---

## Part 2: Create Storage Credentials

A storage credential holds the Service Principal details — Client ID, Client Secret, Tenant ID — so Unity Catalog can authenticate to Azure storage on your behalf.

**You need two credentials** — one per storage account (or one shared if the same SP has access to both).

### 2.1 Where to Create Storage Credentials

There are two places you can create a storage credential:

**Option A — Databricks Account Console (recommended for admins)**
1. Open a browser and navigate to `https://accounts.azuredatabricks.net`
2. Sign in with your Azure account
3. Left sidebar → click **Catalog**
4. Top tab bar → click **External locations**
5. Left sub-tab → click **Credentials**
6. Click **+ Add a credential** (top right)

**Option B — Workspace UI**
1. Open your Databricks workspace: `https://adb-<workspace-id>.azuredatabricks.net`
2. Left sidebar → click **Catalog** (grid icon)
3. Left panel → click **External Data** (folder icon with a chain)
4. Click **Credentials** tab at the top
5. Click **Create credential** (top right)

Both options open the same form. Use whichever you have access to.

---

### 2.2 Create Credential for stadlsdev001

Fill in the form exactly as shown:

| Field | Value |
|---|---|
| Credential name | `sp-stadls-credential` |
| Authentication type | `Azure Service Principal` |
| Directory (tenant) ID | `c8fe40ce-7c95-4958-8992-21dfb0ea6c3c` |
| Application (client) ID | `e7bedfb8-e1c8-4b5f-89a2-ad9be09a7ac1` |
| Client secret | (the value from Key Vault secret `sp-client-secret`) |

> **Where to find these values:**
> - Tenant ID and Client ID → Azure Portal → **Azure Active Directory** → **App registrations** → search `sp-databricks-dev` → **Overview** tab
> - Client Secret → Azure Portal → **Key vaults** → `kv-ev-intelligence-dev` → **Secrets** → `sp-client-secret` → click the secret → **Show Secret Value**

Click **Create**.

You will see `sp-stadls-credential` appear in the Credentials list.

---

### 2.3 Create Credential for stblobdev001

If the same Service Principal has access to both storage accounts (which it does in this project — it has `Storage Blob Data Contributor` on both), you can either:
- Reuse `sp-stadls-credential` for the Blob External Location (one credential, two locations)
- Create a separate credential with the same SP details

For clarity, create a separate credential:

| Field | Value |
|---|---|
| Credential name | `sp-stblob-credential` |
| Authentication type | `Azure Service Principal` |
| Directory (tenant) ID | `c8fe40ce-7c95-4958-8992-21dfb0ea6c3c` |
| Application (client) ID | `e7bedfb8-e1c8-4b5f-89a2-ad9be09a7ac1` |
| Client secret | (same value as above — same SP) |

Click **Create**.

---

### 2.4 Verify Both Credentials Exist

In the Credentials list, you should see:
```
sp-stadls-credential   Azure Service Principal   Created by: you
sp-stblob-credential   Azure Service Principal   Created by: you
```

If you see an error "You do not have permission to create credentials" — you need **Account Admin** or **Metastore Admin** role in Databricks. Contact your workspace admin.

---

## Part 3: Create External Locations

An external location maps a storage path prefix to a storage credential. Unity Catalog checks external locations when any notebook tries to read or write an `abfss://` path.

**Rule:** The `abfss://` path in your table or volume LOCATION must be covered by (start with the URL of) an external location.

### 3.1 Open External Locations

**In the Workspace UI:**
1. Left sidebar → **Catalog** (grid icon)
2. Left panel → **External Data**
3. Click **External Locations** tab
4. Click **+ Create location** (top right)

---

### 3.2 Create External Location for stadlsdev001

Fill in the form:

| Field | Value |
|---|---|
| External location name | `ext-loc-stadls` |
| URL | `abfss://bronze@stadlsdev001.dfs.core.windows.net/` |
| Storage credential | `sp-stadls-credential` |

> **Important about the URL:**
> - `bronze` is the container name inside `stadlsdev001`
> - The URL ends with `/` — this is the root of the container
> - Any path that starts with `abfss://bronze@stadlsdev001.dfs.core.windows.net/` is automatically covered
> - Example covered: `abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/`

Click **Create**.

**Step — Test the connection immediately:**
1. In the External Locations list, click `ext-loc-stadls`
2. Click **Test connection** (top right of the detail page)
3. Wait 5–10 seconds
4. You should see: `Connection successful`

If you see an error:
- `Authentication failed` → the Client Secret in the credential is wrong or expired
- `Authorization failed` → the Service Principal does not have `Storage Blob Data Contributor` on `stadlsdev001`
- `Resource not found` → check the container name (`bronze`) — it must exist in the storage account

---

### 3.3 Create External Location for stblobdev001

**Important note about stblobdev001:**
Azure Blob Storage without Hierarchical Namespace (HNS) uses a flat namespace. The `abfss://` scheme requires HNS to be enabled.

**Check if HNS is enabled on stblobdev001:**
1. Azure Portal → **Storage accounts** → `stblobdev001`
2. Left menu → **Overview**
3. Look for **Hierarchical namespace** in the properties panel
4. It should show **Enabled**

If HNS is NOT enabled:
- You cannot use `abfss://` for this account
- Use `wasbs://files@stblobdev001.blob.core.windows.net/` instead in the External Location URL
- Note: `wasbs://` is the older scheme for plain Blob Storage without HNS

Assuming HNS is enabled, fill in:

| Field | Value |
|---|---|
| External location name | `ext-loc-stblob` |
| URL | `abfss://files@stblobdev001.dfs.core.windows.net/` |
| Storage credential | `sp-stblob-credential` |

> `files` is the container name in `stblobdev001`. If your container has a different name, use that name instead.

Click **Create** → Click **Test connection** → `Connection successful`.

---

### 3.4 Verify Both External Locations

External Locations list should show:
```
ext-loc-stadls   abfss://bronze@stadlsdev001.dfs.core.windows.net/   sp-stadls-credential
ext-loc-stblob   abfss://files@stblobdev001.dfs.core.windows.net/    sp-stblob-credential
```

---

## Part 4: Create a Catalog and Schemas

A **catalog** is the top-level namespace in Unity Catalog — like a database server. A **schema** is a database inside a catalog — it holds tables and volumes.

### 4.1 Create the Catalog

1. Left sidebar → **Catalog** (grid icon)
2. In the Catalog Explorer panel, click **+** (Add) at the top
3. Select **Add a catalog**

Fill in:

| Field | Value |
|---|---|
| Catalog name | `dev_catalog` |
| Type | `Standard` |
| Storage location | Leave blank (uses Unity Catalog managed storage by default) |

Click **Create**.

> If you see "Catalog already exists" — the catalog was already created. Click `dev_catalog` in the left panel to open it and move to Step 4.2.

---

### 4.2 Create the bronze Schema

1. In the Catalog Explorer, click `dev_catalog` to expand it
2. Click **+** (Add) next to `dev_catalog`
3. Select **Add a schema**

Fill in:

| Field | Value |
|---|---|
| Schema name | `bronze` |
| Storage location | Leave blank (inherits from catalog) |

Click **Create**.

---

### 4.3 Create the silver Schema

1. Click `dev_catalog` → **+** → **Add a schema**

| Field | Value |
|---|---|
| Schema name | `silver` |

Click **Create**.

After both schemas are created, the Catalog Explorer shows:
```
dev_catalog
  ├── bronze
  └── silver
```

---

## Part 5: External Delta Table on stadlsdev001

An external Delta table stores its files on your ADLS storage. Databricks reads and writes through the External Location — no auth code needed in notebooks.

### Step 1 — Open a Notebook

1. Left sidebar → **Workspace**
2. Navigate to **Shared**
3. Click **⋮** (three dots) next to Shared → **Create** → **Folder**
4. Name: `day6-practice`
5. Click inside `day6-practice` → **⋮** → **Create** → **Notebook**
6. Name: `01_external_delta_table`
7. Default language: `Python`
8. Click **Create**

---

### Step 2 — Attach to a Cluster

At the top of the notebook, look for the cluster dropdown (shows `Detached` or a cluster name).

1. Click the dropdown
2. Select your running all-purpose cluster (e.g. `cluster-dev`)
3. If no cluster is running → click **Start** on one, or create one:
   - Left sidebar → **Compute** → **Create compute**
   - Runtime: `15.4 LTS (Scala 2.12, Spark 3.5.0)`
   - Node type: `Standard_D4s_v3`
   - Terminate after: `60 minutes`
   - Click **Create compute**
   - Wait 3–5 minutes for it to start, then attach it to the notebook

---

### Step 3 — Verify Storage Access

Paste in Cell 1 and run (`Shift + Enter`):

```python
# Test that Unity Catalog gives us access to stadlsdev001
# No spark.conf.set() needed — the External Location handles auth

path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/"

try:
    files = dbutils.fs.ls(path)
    print(f"SUCCESS — {len(files)} items found in container")
    for f in files[:5]:
        print(f"  {f.name}")
except Exception as e:
    print(f"FAILED: {e}")
    print("")
    print("Checklist:")
    print("  1. External Location ext-loc-stadls exists? (Catalog → External Data → External Locations)")
    print("  2. Storage Credential sp-stadls-credential is valid?")
    print("  3. SP has Storage Blob Data Contributor on stadlsdev001?")
    print("  4. Cluster is Single User or Shared access mode (not No Isolation Shared)?")
```

**Expected output:**
```
SUCCESS — 0 items found in container
```
(Zero items is fine — the container is empty. The important thing is no error.)

---

### Step 4 — Write Sample Data as Delta

Add Cell 2:

```python
# Create a sample DataFrame
data = [
    (1, "Alice",   "completed", 250.00),
    (2, "Bob",     "failed",    100.00),
    (3, "Carol",   "completed", 430.50),
    (4, "David",   "pending",   200.00),
    (5, "Eve",     "completed",  75.00),
]
columns = ["id", "name", "status", "amount"]

df = spark.createDataFrame(data, columns)
df.show()
```

Add Cell 3:

```python
# Write as Delta format to ADLS
# Unity Catalog handles auth — just use the abfss:// path directly

delta_path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/"

df.write.format("delta").mode("overwrite").save(delta_path)

print(f"Delta written to: {delta_path}")
```

Add Cell 4 — verify the files landed:

```python
# List files written to ADLS
for f in dbutils.fs.ls(delta_path):
    print(f.name, f"-", f.size, "bytes")
```

**Expected output — you should see:**
```
_delta_log/  - 0 bytes
part-00000-....parquet - 12345 bytes
```

The `_delta_log/` folder is what makes this a Delta table (not just a folder of Parquet files).

---

### Step 5 — Register as External Table in Unity Catalog

Add Cell 5:

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.sample_people
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/'
```

**What this does:**
- Creates metadata entry in Unity Catalog pointing to the Delta files
- Does NOT copy or move any data
- Schema is inferred from the existing Delta files at that path

**Expected output:** `OK` with no rows returned.

---

### Step 6 — Query the External Table

Add Cell 6:

```sql
%sql
SELECT * FROM dev_catalog.bronze.sample_people
```

Add Cell 7:

```sql
%sql
SELECT status, COUNT(*) AS total, SUM(amount) AS total_amount
FROM dev_catalog.bronze.sample_people
GROUP BY status
ORDER BY total_amount DESC
```

---

### Step 7 — Confirm It Is External

Add Cell 8:

```sql
%sql
DESCRIBE EXTENDED dev_catalog.bronze.sample_people
```

In the output table, find these two rows:

| col_name | data_type |
|---|---|
| Type | EXTERNAL |
| Location | abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/ |

If `Type` shows `MANAGED`, the table was not created with a `LOCATION` clause — delete it and re-run Step 5.

---

### Step 8 — Prove DROP Does Not Delete Files

Add Cell 9:

```sql
%sql
DROP TABLE IF EXISTS dev_catalog.bronze.sample_people
```

Add Cell 10 — check the files on ADLS are still there:

```python
files = dbutils.fs.ls("abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/")
print(f"Files still on ADLS: {len(files)} items")
for f in files:
    print(f"  {f.name}")
```

**Expected output:** Files are still there — `_delta_log/` and the parquet file(s).

Add Cell 11 — re-register the table (data comes back instantly, no re-write needed):

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.sample_people
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/'
```

Add Cell 12 — verify data is back:

```sql
%sql
SELECT COUNT(*) AS row_count FROM dev_catalog.bronze.sample_people
```

---

## Part 6: External Volume on stblobdev001

A volume makes a Blob Storage path accessible as `/Volumes/<catalog>/<schema>/<volume>/`. No auth code, no mounting — Unity Catalog handles it.

### Step 1 — Create a New Notebook

1. In `Shared/day6-practice` → **⋮** → **Create** → **Notebook**
2. Name: `02_external_volume`
3. Attach to the same cluster

---

### Step 2 — Create the External Volume

Add Cell 1:

```sql
%sql
CREATE EXTERNAL VOLUME IF NOT EXISTS dev_catalog.bronze.blob_files
LOCATION 'abfss://files@stblobdev001.dfs.core.windows.net/'
```

> This tells Unity Catalog: "the path `/Volumes/dev_catalog/bronze/blob_files/` maps to `abfss://files@stblobdev001.dfs.core.windows.net/`". No files are created or moved.

**Expected output:** `OK`

---

### Step 3 — Confirm the Volume Exists in the Catalog

Add Cell 2:

```sql
%sql
SHOW VOLUMES IN dev_catalog.bronze
```

You should see `blob_files` in the list with `Volume Type = EXTERNAL`.

---

### Step 4 — List Files via the Volume Path

Add Cell 3:

```python
volume_path = "/Volumes/dev_catalog/bronze/blob_files/"

try:
    files = dbutils.fs.ls(volume_path)
    print(f"Volume accessible — {len(files)} items")
    for f in files[:10]:
        print(f"  {f.name}  ({f.size} bytes)")
except Exception as e:
    print(f"Error: {e}")
    print("Check: External Location ext-loc-stblob covers abfss://files@stblobdev001...")
```

---

### Step 5 — Write a Text File to the Volume

Add Cell 4:

```python
# Write a file — this physically writes to stblobdev001 Blob Storage
dbutils.fs.put(
    f"{volume_path}day6_test.txt",
    "Hello from Databricks! Written via Unity Catalog volume.",
    overwrite=True
)
print("File written to volume (= written to stblobdev001 Blob Storage)")
```

---

### Step 6 — Read the File Back

Add Cell 5:

```python
content = dbutils.fs.head(f"{volume_path}day6_test.txt")
print(f"File content: {content}")
```

---

### Step 7 — Write a CSV and Read it as a DataFrame

Add Cell 6:

```python
# Write a CSV manually
csv_content = """id,product,category,price
1,Laptop,Electronics,75000
2,Desk,Furniture,12000
3,Notebook,Stationery,150
4,Monitor,Electronics,22000
5,Chair,Furniture,8500"""

dbutils.fs.put(f"{volume_path}products.csv", csv_content, overwrite=True)
print("CSV written to volume")
```

Add Cell 7:

```python
# Read the CSV from the volume path using Spark
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{volume_path}products.csv")

df.show()
print(f"Schema: {df.dtypes}")
```

---

### Step 8 — Understand: Volume Is Not a Table

Add Cell 8:

```sql
%sql
-- This will FAIL — a volume is not a table
SELECT * FROM dev_catalog.bronze.blob_files
```

**Expected error:** `[DELTA_MISSING_TRANSACTION_LOG] or similar — blob_files is not a Delta table.`

To query the CSV data with SQL, use a temp view:

```python
df.createOrReplaceTempView("products_view")
```

```sql
%sql
SELECT category, COUNT(*) AS count, SUM(price) AS total_value
FROM products_view
GROUP BY category
ORDER BY total_value DESC
```

---

### Step 9 — Verify DROP VOLUME Keeps Files

Add Cell 9:

```sql
%sql
DROP VOLUME dev_catalog.bronze.blob_files
```

Add Cell 10:

```python
# Try to access via volume path — this will fail (volume is gone)
try:
    dbutils.fs.ls(volume_path)
except Exception as e:
    print(f"Volume path no longer accessible: {e}")

# But the files still exist on stblobdev001 via the direct abfss path
files = dbutils.fs.ls("abfss://files@stblobdev001.dfs.core.windows.net/")
print(f"\nFiles still on Blob Storage: {len(files)} items")
for f in files:
    print(f"  {f.name}")
```

Add Cell 11 — re-create the volume:

```sql
%sql
CREATE EXTERNAL VOLUME IF NOT EXISTS dev_catalog.bronze.blob_files
LOCATION 'abfss://files@stblobdev001.dfs.core.windows.net/'
```

---

## Part 7: Internal (Managed) Table vs External Table — Side-by-Side

### Step 1 — Create a New Notebook

1. In `Shared/day6-practice` → Create → Notebook
2. Name: `03_internal_vs_external`
3. Attach to the cluster

---

### Step 2 — Create an Internal Table

Add Cell 1:

```sql
%sql
-- Internal table — NO LOCATION clause
-- Databricks picks the storage path inside UC managed storage
CREATE TABLE IF NOT EXISTS dev_catalog.silver.managed_example (
    id     INT,
    name   STRING,
    score  DOUBLE
)
USING DELTA
```

Add Cell 2:

```sql
%sql
INSERT INTO dev_catalog.silver.managed_example VALUES
(1, 'Alice', 95.5),
(2, 'Bob',   82.0),
(3, 'Carol', 91.0)
```

Add Cell 3:

```sql
%sql
DESCRIBE EXTENDED dev_catalog.silver.managed_example
```

Find the rows:
- `Type` → `MANAGED`
- `Location` → something like `abfss://unitycatalog@<uc-managed-account>.dfs.core.windows.net/<guid>/dev_catalog/silver/managed_example`

You did **not** choose this path — Databricks assigned it automatically inside its own managed storage.

---

### Step 3 — Create an External Table

Add Cell 4:

```sql
%sql
-- External table — WITH LOCATION clause pointing to YOUR storage
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.external_example
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/external_example/'
```

Add Cell 5:

```sql
%sql
INSERT INTO dev_catalog.bronze.external_example VALUES
(1, 'Alice', 95.5),
(2, 'Bob',   82.0)
```

Add Cell 6:

```sql
%sql
DESCRIBE EXTENDED dev_catalog.bronze.external_example
```

Find the rows:
- `Type` → `EXTERNAL`
- `Location` → `abfss://bronze@stadlsdev001.dfs.core.windows.net/external_example/`

---

### Step 4 — Drop Both and Compare

Add Cell 7:

```sql
%sql
-- Drop internal table → DATA IS DELETED
DROP TABLE dev_catalog.silver.managed_example
```

Add Cell 8:

```python
# Try to access the managed location — files are gone
# (You can only verify this if you copied the Location from DESCRIBE EXTENDED)
# The path is gone — Databricks deleted it when the table was dropped
print("Internal table dropped — Databricks deleted the Delta files from managed storage")
print("There is no Location to check — the files are gone")
```

Add Cell 9:

```sql
%sql
-- Drop external table → ONLY METADATA REMOVED, FILES KEPT
DROP TABLE dev_catalog.bronze.external_example
```

Add Cell 10:

```python
# Files are still on ADLS even though the table is dropped
files = dbutils.fs.ls("abfss://bronze@stadlsdev001.dfs.core.windows.net/external_example/")
print(f"External table dropped — files still on ADLS: {len(files)} items")
for f in files:
    print(f"  {f.name}")
```

---

### Step 5 — Comparison Summary

Add Cell 11 — run this to print a clear comparison:

```python
print("""
+--------------------+---------------------------+---------------------------+
|                    | Internal (Managed) Table  | External Table            |
+--------------------+---------------------------+---------------------------+
| LOCATION clause    | Not specified             | Required (abfss://)       |
| File location      | Databricks managed storage| Your ADLS path            |
| DROP TABLE effect  | Deletes Delta files       | Removes metadata only     |
| Files recoverable  | No (gone permanently)     | Yes (still on ADLS)       |
| Use case           | Temp / intermediate data  | Production / shared data  |
+--------------------+---------------------------+---------------------------+
""")
```

---

## Part 8: Internal Volume vs External Volume — Side-by-Side

### Step 1 — Create a New Notebook

1. `Shared/day6-practice` → Create → Notebook
2. Name: `04_internal_vs_external_volume`
3. Attach to the cluster

---

### Step 2 — Create an Internal Volume

Add Cell 1:

```sql
%sql
-- Internal volume — NO LOCATION clause
-- Databricks picks the storage path
CREATE VOLUME IF NOT EXISTS dev_catalog.silver.managed_volume
```

Add Cell 2:

```sql
%sql
DESCRIBE VOLUME dev_catalog.silver.managed_volume
```

Look at `Storage Location` — Databricks assigned a path in its managed storage. You did not choose this.

Add Cell 3:

```python
# Write to the internal volume
vol_path = "/Volumes/dev_catalog/silver/managed_volume/"
dbutils.fs.put(f"{vol_path}test.txt", "Written to internal volume", overwrite=True)

content = dbutils.fs.head(f"{vol_path}test.txt")
print(f"Read back: {content}")
```

---

### Step 3 — Create an External Volume

Add Cell 4:

```sql
%sql
-- External volume — WITH LOCATION pointing to YOUR storage
CREATE EXTERNAL VOLUME IF NOT EXISTS dev_catalog.bronze.blob_files
LOCATION 'abfss://files@stblobdev001.dfs.core.windows.net/'
```

Add Cell 5:

```python
# Write to the external volume — goes to stblobdev001
ext_vol_path = "/Volumes/dev_catalog/bronze/blob_files/"
dbutils.fs.put(f"{ext_vol_path}test.txt", "Written to external volume (= stblobdev001)", overwrite=True)

content = dbutils.fs.head(f"{ext_vol_path}test.txt")
print(f"Read back: {content}")
```

---

### Step 4 — Drop Both and Compare

Add Cell 6:

```sql
%sql
-- Drop internal volume → FILES DELETED
DROP VOLUME dev_catalog.silver.managed_volume
```

Add Cell 7:

```python
# Internal volume files are gone
try:
    dbutils.fs.ls("/Volumes/dev_catalog/silver/managed_volume/")
    print("UNEXPECTED: still accessible")
except:
    print("Internal volume dropped — path no longer accessible and files are deleted")
```

Add Cell 8:

```sql
%sql
-- Drop external volume → ONLY METADATA REMOVED
DROP VOLUME dev_catalog.bronze.blob_files
```

Add Cell 9:

```python
# External volume files still exist on stblobdev001
files = dbutils.fs.ls("abfss://files@stblobdev001.dfs.core.windows.net/")
print(f"External volume dropped — files still on stblobdev001: {len(files)} items")
for f in files:
    print(f"  {f.name}")
```

Add Cell 10:

```python
print("""
+--------------------+---------------------------+---------------------------+
|                    | Internal Volume           | External Volume           |
+--------------------+---------------------------+---------------------------+
| LOCATION clause    | Not specified             | Required (abfss://)       |
| File location      | Databricks managed storage| Your Blob/ADLS path       |
| DROP VOLUME effect | Deletes files             | Removes metadata only     |
| Files recoverable  | No                        | Yes (still on storage)    |
| Access path        | /Volumes/catalog/schema/  | /Volumes/catalog/schema/  |
| Use case           | Temp files, staging       | Raw landing zone, archive |
+--------------------+---------------------------+---------------------------+
""")
```

---

## Part 9: ADF — Running a Databricks Notebook Activity

Azure Data Factory can trigger a Databricks notebook as a step in an ADF pipeline. ADF starts the notebook, waits for it to finish, and captures the exit value.

### 9.1 What You Need

```
Azure Data Factory (adf-ev-dev)
  └── Pipeline
        └── Databricks Notebook Activity
              ├── Linked Service  → points to Databricks workspace (dbw-ev-dev)
              ├── Notebook path   → Shared/day6-practice/adf_triggered_notebook
              ├── Cluster config  → New job cluster (auto-provision on each run)
              └── Base parameters → key-value pairs the notebook reads as widgets
```

---

### 9.2 Step 1 — Create the Notebook in Databricks

First create the notebook that ADF will call.

1. **Databricks workspace** → left sidebar → **Workspace**
2. Navigate to `Shared/day6-practice`
3. Click **⋮** → **Create** → **Notebook**
4. Name: `adf_triggered_notebook`
5. Default language: `Python`
6. Click **Create**
7. Attach the notebook to your cluster (just for editing — ADF will use a job cluster)

Add Cell 1:

```python
# Read parameters that ADF passes in as Base Parameters
# dbutils.widgets.text("key", "default_value", "label shown in UI")

dbutils.widgets.text("adf_pipeline_name", "unknown", "ADF Pipeline Name")
dbutils.widgets.text("adf_run_id",        "unknown", "ADF Run ID")
dbutils.widgets.text("env",               "dev",     "Environment")

pipeline_name = dbutils.widgets.get("adf_pipeline_name")
run_id        = dbutils.widgets.get("adf_run_id")
env           = dbutils.widgets.get("env")

print(f"Triggered by: {pipeline_name}")
print(f"Run ID:       {run_id}")
print(f"Environment:  {env}")
```

Add Cell 2:

```python
# Simulate some processing work
data = [(i, f"record_{i}", i * 100) for i in range(1, 6)]
df = spark.createDataFrame(data, ["id", "label", "value"])
df.show()

row_count = df.count()
print(f"Processed {row_count} records in {env} environment")
```

Add Cell 3:

```python
# Return a result string back to ADF
# ADF captures this as activity output: runOutput
result = f"SUCCESS: {row_count} records processed in {env}"
dbutils.notebook.exit(result)
```

Note the exact notebook path — you need it in ADF:
`/Shared/day6-practice/adf_triggered_notebook`

---

### 9.3 Step 2 — Create a Databricks Linked Service in ADF

A Linked Service is the connection object in ADF that tells ADF how to reach Databricks.

1. Open **Azure Data Factory Studio**: Azure Portal → **Data factories** → `adf-ev-dev` → **Launch studio**
2. Left sidebar → click **Manage** (toolbox wrench icon)
3. Under **Connections** section → click **Linked services**
4. Click **+ New** (top left)
5. In the search box type `Databricks` → select **Azure Databricks** → click **Continue**

Fill in the form:

**General section:**

| Field | Value |
|---|---|
| Name | `ls_databricks_dev` |
| Description | `Databricks workspace for dev environment` |

**Connect via integration runtime:**
- Leave as `AutoResolveIntegrationRuntime` (default)

**Account selection method:**
- Select `From Azure subscription`

| Field | Value |
|---|---|
| Azure subscription | select your subscription |
| Databricks workspace | `dbw-ev-dev` |

**Select cluster:**
- Select `New job cluster`

This means ADF will auto-provision a new cluster each time it triggers the notebook. The cluster is deleted after the notebook finishes.

| Field | Value |
|---|---|
| Databricks runtime version | `15.4 LTS (Scala 2.12, Spark 3.5.0)` |
| Worker node type | `Standard_D4s_v3` |
| Driver node type | `Standard_D4s_v3` (same as worker for small clusters) |
| Workers | `1` (min for a small job) |
| Python version | `3` |

**Authentication:**

| Field | Value |
|---|---|
| Authentication type | `Access token` |
| Access token | Paste your Databricks PAT token |

> **Where to get the PAT token:**
> - Databricks workspace → top right → click your username → **User Settings**
> - Left menu → **Developer** → **Access tokens**
> - Click **Generate new token**
> - Comment: `adf-linked-service`
> - Lifetime: `90` (days)
> - Click **Generate**
> - Copy the token — it is shown only once
> - Paste it in the ADF Linked Service form

6. Click **Test connection** → wait for `Connection successful`
7. Click **Apply** (top right)

---

### 9.4 Step 3 — Create or Open an ADF Pipeline

1. Left sidebar → **Author** (pencil icon)
2. Under **Pipelines** → click **+** → **New pipeline**
3. Name the pipeline: `pl_day6_notebook_demo`

---

### 9.5 Step 4 — Add the Databricks Notebook Activity

1. In the pipeline canvas, look at the **Activities** panel on the left
2. Expand **Databricks** section
3. Drag **Notebook** onto the canvas

Click the Notebook activity to select it. Configure the three tabs at the bottom:

**General tab:**

| Field | Value |
|---|---|
| Name | `Run Day6 Notebook` |
| Timeout | `00:30:00` (30 minutes) |
| Retry | `1` |

**Azure Databricks tab:**

| Field | Value |
|---|---|
| Databricks linked service | `ls_databricks_dev` (select from dropdown) |

**Settings tab:**

| Field | Value |
|---|---|
| Notebook path | `/Shared/day6-practice/adf_triggered_notebook` |

Click **+ New** under **Base parameters** to add each parameter:

| Name | Value |
|---|---|
| `adf_pipeline_name` | `@pipeline().Pipeline` |
| `adf_run_id` | `@pipeline().RunId` |
| `env` | `dev` |

> `@pipeline().Pipeline` and `@pipeline().RunId` are ADF system variables — they inject the actual pipeline name and run ID automatically at runtime.

---

### 9.6 Step 5 — Test the Pipeline (Debug Run)

1. Click **Debug** in the pipeline toolbar (triangle with a bug icon)
2. A dialog may appear — click **OK** to start
3. At the bottom, the **Output** tab shows pipeline progress
4. Watch the `Run Day6 Notebook` activity:
   - Blue circle = running (waiting for cluster to start, then notebook running)
   - Green tick = succeeded
   - Red X = failed

> **Note on timing:** "New job cluster" takes 3–5 minutes to provision before the notebook starts. This is normal. The notebook itself runs in seconds. Total time per ADF run: ~5–8 minutes.

5. When it succeeds, click the **Output** icon (glasses icon) on the activity row

You will see:
```json
{
  "runOutput": "SUCCESS: 5 records processed in dev",
  "effectiveIntegrationRuntime": "AutoResolveIntegrationRuntime",
  "executionDuration": 312,
  "durationInQueue": { ... }
}
```

The `runOutput` value is exactly what the notebook returned with `dbutils.notebook.exit(result)`.

---

### 9.7 Step 6 — Verify the Run in Databricks

1. Databricks workspace → left sidebar → **Workflows**
2. Click **Job runs** tab (not the Jobs tab)
3. You will see a run with source `ADF` — it shows which ADF pipeline triggered it
4. Click the run → click **Logs** to see all notebook cell outputs

---

### 9.8 Step 7 — Add a Dependency (Copy → Notebook)

If you want the notebook to run only after a Copy activity succeeds:

1. In the pipeline canvas, add a **Copy data** activity (from the Activities panel → Move & Transform)
2. Draw the **green arrow** from the Copy activity → Notebook activity
   - Green arrow = run Notebook only if Copy **succeeded**
   - Red arrow = run Notebook only if Copy **failed** (for error handling)
   - Blue arrow = run regardless of Copy result (always)
3. If Copy fails, the Notebook activity is skipped and the pipeline shows as failed

---

## Part 10: Cluster Access Modes and Notebook Permissions

### 10.1 Cluster Access Modes

When you create a cluster, you choose an access mode. This determines what Unity Catalog features are available.

**To check or set the access mode:**
1. Left sidebar → **Compute**
2. Click your cluster → **Edit**
3. Scroll to **Advanced options** → expand it
4. Look for **Access mode** dropdown

| Access Mode | Unity Catalog | Multiple users | Languages | Use case |
|---|---|---|---|---|
| Single User | Full support | No — only 1 user | Python, SQL, Scala, R | Default for most workloads |
| Shared | Full support | Yes — isolated sessions | Python, SQL only | Team cluster, shared access |
| No Isolation Shared | Not supported | Yes — no isolation | All | Legacy only — avoid |

**For this project:** Use `Single User` for job clusters triggered by ADF. Use `Shared` for a team all-purpose cluster.

---

### 10.2 Notebook Permissions

Who can view, run, or edit a notebook is controlled by notebook-level permissions.

**To set permissions on a notebook:**
1. Navigate to the notebook in **Workspace**
2. Right-click the notebook → **Permissions**
3. Click **+ Add**
4. Search for a user, group, or service principal
5. Choose the permission level:

| Level | Can do |
|---|---|
| Can View | Read the code, see cell outputs — cannot run |
| Can Run | Run the notebook — cannot edit the code |
| Can Edit | Run and edit — cannot delete or change permissions |
| Can Manage | Full control — edit, run, delete, change permissions |

**For ADF-triggered notebooks:**
ADF runs the notebook as the **Service Principal** defined in the Linked Service — not as your personal login. The Service Principal must have at minimum `Can Run` permission on the notebook, OR the notebook must be in a folder where the Service Principal has access.

**Simplest setup:** Put ADF-triggered notebooks in `Shared/` folder. Service Principals have access to Shared by default.

---

## Part 11: Landing Zone Pattern — Full Pipeline Demo

This combines everything: files land in a volume (Blob Storage), a notebook reads them, transforms them, and saves as an external Delta table on ADLS.

### Step 1 — Create the Notebook

1. `Shared/day6-practice` → Create → Notebook
2. Name: `05_landing_to_table_pipeline`
3. Attach to cluster

---

### Step 2 — Simulate Files Landing in the Volume

Add Cell 1:

```python
# Re-create the volume if it was dropped earlier
```

```sql
%sql
CREATE EXTERNAL VOLUME IF NOT EXISTS dev_catalog.bronze.blob_files
LOCATION 'abfss://files@stblobdev001.dfs.core.windows.net/'
```

Add Cell 2:

```python
# Simulate ADF dropping CSV files into Blob Storage (the volume)
volume_path = "/Volumes/dev_catalog/bronze/blob_files/"

# Batch 1
dbutils.fs.put(f"{volume_path}batch_2024_01.csv",
    "order_id,customer,product,quantity,price\n"
    "1001,Alice,Laptop,1,75000\n"
    "1002,Bob,Mouse,2,1500\n"
    "1003,Carol,Keyboard,1,3000",
    overwrite=True)

# Batch 2
dbutils.fs.put(f"{volume_path}batch_2024_02.csv",
    "order_id,customer,product,quantity,price\n"
    "1004,David,Monitor,1,22000\n"
    "1005,Eve,Laptop,2,150000",
    overwrite=True)

print("Batch files landed in volume:")
for f in dbutils.fs.ls(volume_path):
    if f.name.endswith(".csv"):
        print(f"  {f.name}  ({f.size} bytes)")
```

---

### Step 3 — Read and Transform

Add Cell 3:

```python
# Read all CSV files from the volume
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(volume_path + "*.csv")

print(f"Raw rows read: {df_raw.count()}")
df_raw.show()
```

Add Cell 4:

```python
from pyspark.sql.functions import col, round as spark_round

# Transform: calculate total_value per order
df_clean = df_raw \
    .withColumn("total_value", spark_round(col("quantity") * col("price"), 2)) \
    .select("order_id", "customer", "product", "quantity", "price", "total_value")

print("Transformed data:")
df_clean.show()
```

---

### Step 4 — Write as External Delta Table on ADLS

Add Cell 5:

```python
# Write cleaned data as Delta to stadlsdev001
output_path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/orders_cleaned/"

df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

print(f"Written to: {output_path}")
```

Add Cell 6:

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.orders_cleaned
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/orders_cleaned/'
```

---

### Step 5 — Query the Final Table

Add Cell 7:

```sql
%sql
SELECT * FROM dev_catalog.bronze.orders_cleaned
ORDER BY total_value DESC
```

Add Cell 8:

```sql
%sql
SELECT customer, SUM(total_value) AS total_spend
FROM dev_catalog.bronze.orders_cleaned
GROUP BY customer
ORDER BY total_spend DESC
```

Add Cell 9 — return a summary to ADF (useful when this notebook is called from ADF):

```python
row_count = spark.table("dev_catalog.bronze.orders_cleaned").count()
dbutils.notebook.exit(f"SUCCESS: {row_count} orders written to dev_catalog.bronze.orders_cleaned")
```

---

## Quick Reference — Day 6 Terminologies

```
Term                         Definition
──────────────────────────────────────────────────────────────────────────────────
Storage Credential           Unity Catalog object holding SP auth details
                             (tenant ID, client ID, client secret). Created once
                             per SP in Account Console or Workspace Catalog UI.

External Location            Unity Catalog object that maps an abfss:// path
                             prefix to a storage credential. Any table or volume
                             under that prefix gets access via the credential.

Internal (Managed) Table     Delta table with no LOCATION clause. Databricks
                             owns the file path. DROP TABLE deletes files.

External Table               Delta table with LOCATION = your abfss:// path.
                             DROP TABLE removes metadata only. Files stay.

Internal Volume              Volume with no LOCATION clause. Databricks owns
                             the storage. DROP VOLUME deletes files.

External Volume              Volume with LOCATION = your abfss:// path.
                             Exposed as /Volumes/catalog/schema/volume/.
                             DROP VOLUME removes metadata only. Files stay.

ADF Linked Service           ADF connection object pointing to a Databricks
                             workspace — uses PAT or Service Principal auth.

ADF Notebook Activity        ADF activity that triggers a Databricks notebook,
                             waits for it to complete, captures runOutput.

Base Parameters              Key-value pairs passed from ADF into a Databricks
                             notebook — read with dbutils.widgets.get("key").

runOutput                    Value from dbutils.notebook.exit("...") — returned
                             to ADF and visible in the activity Output tab.

New job cluster              A cluster provisioned by ADF for each notebook run
                             and terminated when the run finishes. Adds 3-5 min
                             startup overhead per run.

Personal Access Token (PAT)  A token generated in Databricks User Settings used
                             by ADF Linked Service to authenticate to Databricks.

dbutils.widgets.text()       Creates a widget (parameter) that ADF or a user
                             can set before running the notebook.

dbutils.notebook.exit()      Ends the notebook and passes a string back to the
                             caller (ADF or dbutils.notebook.run()).
```
