# Day 4 — Practice Exercises: Access Control, Secrets, Catalog & ADF Integration

> **Goal:** Set up a secret scope backed by Azure Key Vault, connect to real storage accounts, create external tables and volumes in Unity Catalog, and trigger a Databricks notebook from ADF.
> Storage accounts: `stadlsdev001` (ADLS Gen2 for Delta tables), `stblobdev001` (Blob for volumes)

---

## Before You Start

You need:
- Databricks workspace running (`dbw-ev-dev`)
- Azure Key Vault (`key-vault-session-ded`) with these secrets:
  - `sp-client-id`
  - `sp-client-secret`
  - `sp-tenant-id`
- Service Principal with `Storage Blob Data Contributor` on both storage accounts
- Admin access to the Databricks workspace (for secret scope and catalog setup)
- ADF Studio access (for Exercise 6)

---

## Exercise 1 — Create an AKV-Backed Secret Scope

**Goal:** Connect Databricks to Azure Key Vault so notebooks can read secrets securely.

### Steps

**Step 1 — Open the secret scope creation URL**

In your browser, navigate to:
```
https://<your-workspace-url>/#secrets/createScope
```

Example:
```
https://adb-1234567890123456.7.azuredatabricks.net/#secrets/createScope
```

> You must type this URL manually — there is no button in the workspace UI to reach it.

**Step 2 — Fill in the form**

| Field | Value |
|---|---|
| Scope name | `kv-scope` |
| Manage Principal | `All Users` |
| DNS Name | `https://key-vault-session-ded.vault.azure.net/` |
| Resource ID | Copy from: Azure Portal → Key Vault → Properties → Resource ID |

**Step 3 — Click Create**

You should see: `Secret scope kv-scope has been created.`

**Step 4 — Verify in a notebook**

Open your Databricks workspace → create a notebook `Shared/day4-practice/ex1_secret_scope` → run:

```python
# List all secrets in the scope — shows key names only, never values
secrets = dbutils.secrets.list(scope="kv-scope")
for s in secrets:
    print(s.key)
```

Expected output: the secret names from your Key Vault (e.g. `sp-client-id`, `sp-client-secret`, `sp-tenant-id`)

**Step 5 — Read a secret**

```python
# Read a secret — value is [REDACTED] in output, but real value is in the variable
client_id = dbutils.secrets.get(scope="kv-scope", key="sp-client-id")
print("Client ID:", client_id)        # prints: Client ID: [REDACTED]
print("Length of client ID:", len(client_id))   # prints the actual length
```

**What to verify:** Secret names are listed. `len(client_id)` returns a non-zero number (proving the value was read, even though it shows [REDACTED] when printed).

---

## Exercise 2 — Verify Storage Access via Unity Catalog

**Goal:** Confirm that Unity Catalog's External Location gives the notebook access to `stadlsdev001` — with zero auth code in the notebook.

> **Prerequisite:** Admin must have completed these steps first (notes.md Part 5):
> 1. Created Storage Credential `sp-stadls-credential` (Service Principal details in Unity Catalog)
> 2. Created External Location `ext-loc-stadls` → `abfss://bronze@stadlsdev001.dfs.core.windows.net/`
>
> If not done yet — ask your admin. Without the External Location, this exercise will fail with an access error.

### Steps

**Step 1 — Create a notebook**

`Shared/day4-practice/ex2_adls_connect`

**Step 2 — Test access using Unity Catalog (no auth code needed)**

```python
# Cell 1 — Unity Catalog handles auth — just use the path directly
# No spark.conf.set(), no dbutils.secrets.get() for storage access

path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/"

try:
    files = dbutils.fs.ls(path)
    print(f"Access confirmed — {len(files)} items found")
    for f in files:
        print(f"  {f.name}")
except Exception as e:
    print(f"Access failed: {e}")
    print("Ask admin to check: Storage Credential + External Location for stadlsdev001")
```

**Step 3 — Also read using Spark directly (same — no config)**

```python
# Cell 2 — Spark read also works without any spark.conf setup
# Unity Catalog intercepts the path and injects the Storage Credential automatically

delta_test_path = "abfss://bronze@stadlsdev001.dfs.core.windows.net/"

try:
    # List via spark (alternative to dbutils.fs)
    spark_files = spark.read.format("binaryFile").load(delta_test_path)
    print(f"Spark access confirmed — {spark_files.count()} files visible")
except Exception as e:
    print(f"Spark access check: {e}")
```

**Step 4 — Understand what is happening under the hood**

```python
# Cell 3 — show what Unity Catalog resolved for this path
# (This is for learning — not needed in production)
print("How Unity Catalog handled this access:")
print("  1. Notebook used path: abfss://bronze@stadlsdev001.dfs.core.windows.net/")
print("  2. Unity Catalog matched it to External Location: ext-loc-stadls")
print("  3. Unity Catalog injected Storage Credential: sp-stadls-credential")
print("  4. Storage Credential authenticated to Azure AD using Service Principal")
print("  5. Azure AD returned OAuth token — ADLS granted access")
print("  6. Data returned to notebook — zero credential code written by developer")
```

**Legacy reference — what the old approach looked like (do NOT use):**

```python
# ❌ Legacy pattern — shown for reference only, not for use
# client_id     = dbutils.secrets.get(scope="kv-scope", key="sp-client-id")
# client_secret = dbutils.secrets.get(scope="kv-scope", key="sp-client-secret")
# tenant_id     = dbutils.secrets.get(scope="kv-scope", key="sp-tenant-id")
# spark.conf.set("fs.azure.account.auth.type.stadlsdev001...", "OAuth")
# spark.conf.set("fs.azure.account.oauth2.client.id...", client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret...", client_secret)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint...", ...)
# — Unity Catalog replaces ALL of this
```

**What to verify:** `dbutils.fs.ls()` succeeds with no auth code. Files or folders are listed. If it fails, the External Location is missing — not a notebook problem.

---

## Exercise 3 — Create an External Delta Table on stadlsdev001

**Goal:** Write Delta files to ADLS and register them as an external table in Unity Catalog.

### Steps

> Prerequisite: External Location `ext-loc-stadls` must exist (notes.md Part 5). With Unity Catalog, the notebook needs no auth setup — just use the storage path directly.

**Step 1 — Create notebook `ex3_external_table`**

**Step 2 — Create sample data and write as Delta**

```python
# Cell 2 — create a small DataFrame
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
print("DataFrame created")
```

```python
# Cell 3 — write to ADLS as Delta format
delta_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/sample_people/"
df.write.format("delta").mode("overwrite").save(delta_path)
print(f"Delta written to: {delta_path}")
```

```python
# Cell 4 — verify files exist on ADLS
files = dbutils.fs.ls(delta_path)
for f in files:
    print(f.name)
```

Expected: you see `_delta_log/` (the transaction log folder) and one or more `.parquet` files.

**Step 3 — Register as external table in Unity Catalog**

```sql
%sql
-- Create a catalog and schema first if they don't exist
CREATE CATALOG IF NOT EXISTS dev_catalog;
CREATE SCHEMA IF NOT EXISTS dev_catalog.bronze;

-- Register the Delta files as an external table
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.sample_people
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/'
```

**Step 4 — Query the external table**

```sql
%sql
SELECT * FROM dev_catalog.bronze.sample_people
```

```sql
%sql
SELECT status, COUNT(*) AS cnt, SUM(amount) AS total
FROM dev_catalog.bronze.sample_people
GROUP BY status
ORDER BY cnt DESC
```

**Step 5 — Confirm it is EXTERNAL**

```sql
%sql
DESCRIBE EXTENDED dev_catalog.bronze.sample_people
```

Scroll through the output — find the row where `col_name = 'Type'`. It should say `EXTERNAL`. Also find `Location` — it should show your ADLS path.

**Step 6 — Test: drop table, files still exist**

```sql
%sql
DROP TABLE IF EXISTS dev_catalog.bronze.sample_people
```

```python
# Files should still be on ADLS even though the table is gone from catalog
files = dbutils.fs.ls(delta_path)
print(f"Files on ADLS after DROP: {len(files)} items")
for f in files:
    print(f.name)
```

Expected: files are still there.

```sql
%sql
-- Re-register (because we still need the table)
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.sample_people
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/'
```

**What to verify:** Table is queryable via SQL. DESCRIBE EXTENDED shows `Type = EXTERNAL`. After DROP, ADLS files still exist.

---

## Exercise 4 — Create an External Volume on stblobdev001

**Goal:** Register the Blob storage container as a Unity Catalog external volume and access it as a file path.

### Steps

> Prerequisite: external location `ext-loc-stblob` created by admin (see notes.md Part 5.3). If not done — ask admin to create it pointing to `abfss://files@stblobdev001.dfs.core.windows.net/`.

**Step 1 — Create a notebook `ex4_external_volume`**

**Step 2 — Create the catalog and schema (if not already done)**

```sql
%sql
CREATE CATALOG IF NOT EXISTS dev_catalog;
CREATE SCHEMA IF NOT EXISTS dev_catalog.bronze;
```

**Step 3 — Create the external volume**

```sql
%sql
CREATE EXTERNAL VOLUME IF NOT EXISTS dev_catalog.bronze.blob_files
LOCATION 'abfss://files@stblobdev001.dfs.core.windows.net/'
```

**Step 4 — Access the volume as a file path**

```python
# Volume is now accessible at this path — no spark.conf needed
volume_path = "/Volumes/dev_catalog/bronze/blob_files/"

# List contents
try:
    files = dbutils.fs.ls(volume_path)
    print(f"Volume accessible — {len(files)} items")
    for f in files:
        print(f.name)
except Exception as e:
    print(f"Error accessing volume: {e}")
```

**Step 5 — Write a file to the volume**

```python
# Write a text file into the volume (goes to Blob Storage)
test_file = f"{volume_path}day4_test.txt"
dbutils.fs.put(test_file, "Hello from Databricks volume! Day 4 exercise.", overwrite=True)
print("File written to volume")

# Read it back
content = dbutils.fs.head(test_file)
print("Content:", content)
```

**Step 6 — Write a CSV to the volume using Spark**

```python
# Create a small DataFrame and write as CSV into the volume
data = [(1, "Apple", 1.50), (2, "Banana", 0.80), (3, "Mango", 2.00)]
df = spark.createDataFrame(data, ["id", "fruit", "price"])

# Write as CSV to the volume
csv_path = f"{volume_path}fruits.csv"
df.coalesce(1).write.option("header", "true").mode("overwrite").csv(csv_path)
print("CSV written to volume")
```

**Step 7 — Read the CSV back from the volume**

```python
# Read CSV from volume path
df_read = spark.read.option("header", "true").csv(f"{volume_path}fruits.csv/")
df_read.show()
```

**Step 8 — Understand the difference from an external table**

```python
# This works — reading as a DataFrame from the volume path
df_read = spark.read.option("header", "true").csv(f"{volume_path}fruits.csv/")
df_read.show()
```

```sql
%sql
-- This does NOT work — volumes are not tables
-- SELECT * FROM dev_catalog.bronze.blob_files   -- this would fail
-- A volume is a file system, not a table

-- To query: first read into a temp view
```

```python
df_read.createOrReplaceTempView("fruits_view")
```

```sql
%sql
SELECT * FROM fruits_view
```

**What to verify:** Volume is accessible at `/Volumes/...` path. Files written via `dbutils.fs.put` appear in Blob Storage (check in Azure Portal). Volume cannot be directly selected with SQL — must be read as a DataFrame first.

---

## Exercise 5 — Internal vs External Comparison

**Goal:** Create both an internal and external table, drop both, and observe the different outcomes.

### Steps

**Step 1 — Create an internal (managed) table**

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.internal_example (
    id     INT,
    city   STRING,
    score  DOUBLE
)
USING DELTA
```

```sql
%sql
INSERT INTO dev_catalog.bronze.internal_example VALUES
(1, 'Mumbai',    88.5),
(2, 'Bangalore', 92.0),
(3, 'Delhi',     79.5)
```

```sql
%sql
-- Check where Databricks stored the files
DESCRIBE EXTENDED dev_catalog.bronze.internal_example
```

Note the `Location` value — it is inside Unity Catalog managed storage (not your ADLS).

**Step 2 — Create an external table**

```sql
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.external_example
USING DELTA
LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/external_example/'
```

```sql
%sql
INSERT INTO dev_catalog.bronze.external_example VALUES
(1, 'Mumbai',    88.5),
(2, 'Bangalore', 92.0),
(3, 'Delhi',     79.5)
```

```sql
%sql
DESCRIBE EXTENDED dev_catalog.bronze.external_example
```

Note the `Location` value — it is your `stadlsdev001` ADLS path.

**Step 3 — Drop both and observe**

```sql
%sql
DROP TABLE dev_catalog.bronze.internal_example
```

```python
# Try to find internal table files — they are gone
# (Databricks deleted them from its managed storage)
print("Internal table dropped — files are deleted by Databricks")
```

```sql
%sql
DROP TABLE dev_catalog.bronze.external_example
```

```python
# External table files are still on ADLS
files = dbutils.fs.ls("abfss://bronze@stadlsdev001.dfs.core.windows.net/external_example/")
print(f"External table dropped — files still exist: {len(files)} items")
```

**What to verify:**

| | Internal table | External table |
|---|---|---|
| Location after CREATE | Databricks managed path | Your ADLS path |
| After DROP | Data deleted | Data still on ADLS |

---

## Exercise 6 — Run a Databricks Notebook from ADF

**Goal:** Create a notebook in Databricks, configure a Linked Service in ADF, and trigger the notebook from an ADF pipeline.

### Steps

**Step 1 — Create the notebook in Databricks**

`Workspace` → `Shared/day4-practice` → Create → Notebook → name: `adf_triggered_notebook`

```python
# Cell 1 — read parameters from ADF
dbutils.widgets.text("adf_pipeline", "unknown", "ADF Pipeline Name")
dbutils.widgets.text("env",          "dev",     "Environment")

pipeline = dbutils.widgets.get("adf_pipeline")
env      = dbutils.widgets.get("env")

print(f"Notebook triggered by pipeline: {pipeline}")
print(f"Environment: {env}")
```

```python
# Cell 2 — simple work (no external data needed)
data = [(i, f"item_{i}", i * 100) for i in range(1, 6)]
df = spark.createDataFrame(data, ["id", "name", "value"])
print("Data processed:")
df.show()
```

```python
# Cell 3 — return a result to ADF
dbutils.notebook.exit(f"SUCCESS from {pipeline} in {env}")
```

Note the full notebook path: `/Shared/day4-practice/adf_triggered_notebook`

**Step 2 — Create ADF Linked Service for Databricks**

1. Open **ADF Studio** → **Manage** → **Linked services** → **+ New**
2. Search `Azure Databricks` → **Continue**
3. Configure:

   | Field | Value |
   |---|---|
   | Name | `ls_databricks_dev` |
   | Azure subscription | your subscription |
   | Databricks workspace | `dbw-ev-dev` |
   | Select cluster | `New job cluster` |
   | Runtime version | `15.4 LTS` |
   | Node type | `Standard_D4s_v3` |
   | Python version | `3` |

4. Authentication — choose **Access token**:
   - Get a PAT from Databricks: Settings → Developer → Access tokens → Generate new token
   - Paste the token into the `Access token` field in ADF

5. Click **Test connection** → should show **Connection successful**

6. Click **Apply**

**Step 3 — Add Databricks Notebook Activity to an ADF Pipeline**

1. **ADF Studio** → **Author** → open any pipeline or create a new one: `pl_day4_notebook_test`
2. **Activities panel** → search `Databricks` → drag **Notebook** onto the canvas
3. Click the activity → configure each tab:

**General tab:**
- Name: `Run Day4 Notebook`

**Azure Databricks tab:**
- Databricks linked service: `ls_databricks_dev`

**Settings tab:**
- Notebook path: `/Shared/day4-practice/adf_triggered_notebook`
- Base parameters → click **+ New** for each:

  | Name | Value |
  |---|---|
  | `adf_pipeline` | `@pipeline().Pipeline` |
  | `env` | `dev` |

**Step 4 — Debug (test) the pipeline**

1. Click **Debug** in the toolbar
2. Watch the activity — it goes:
   - **Queued** (waiting for cluster to start — ~3–5 min on first run)
   - **In Progress** (notebook is running)
   - **Succeeded** (notebook completed)
3. Click the activity → **Output** tab → look for `runOutput`:
   ```json
   {
     "runOutput": "SUCCESS from pl_day4_notebook_test in dev"
   }
   ```

**Step 5 — Verify in Databricks**

1. Databricks → **Workflows** (left sidebar) → top tabs → **Job runs**
2. You will see the ADF-triggered run with source shown as the ADF linked service
3. Click the run → **Logs** tab → see both cell outputs

**What to verify:** ADF activity shows SUCCEEDED. `runOutput` in ADF shows the value from `dbutils.notebook.exit()`. Databricks job run history shows the run.

---

## Exercise 7 — Chain Notebook Activity After a Copy Activity

**Goal:** Make the Databricks notebook run only after a Copy Activity succeeds.

### Steps

1. Open `pl_day4_notebook_test` (from Exercise 6)

2. Drag a **Copy Data** activity onto the canvas (configure any simple source — even the Lookup array trick from Day 5 ADF works)

3. Draw a connection arrow: **Copy Activity → Notebook Activity**
   - Drag from the **green tick** (success) side of the Copy Activity to the Notebook Activity

4. The dependency is now: Notebook runs ONLY if Copy succeeds

5. Click **Debug** and watch both activities execute in order:
   - Copy Activity runs first
   - When it turns green, the Notebook Activity starts automatically

6. If the Copy Activity fails (red X), the Notebook Activity will be skipped (grey)

**What to verify:** The two activities run in sequence. Notebook activity shows the dependency arrow in the canvas.

---

## Final Summary — What You Set Up

```
Azure Key Vault (key-vault-session-ded)
  ├── sp-client-id        (Service Principal client ID)
  ├── sp-client-secret    (Service Principal client secret)
  └── sp-tenant-id        (Azure AD tenant ID)

Databricks Secret Scope: kv-scope
  └── points to key-vault-session-ded

Unity Catalog: dev_catalog
  └── Schema: bronze
        ├── sample_people   (EXTERNAL table → stadlsdev001/bronze/sample_people/)
        ├── blob_files      (EXTERNAL volume → stblobdev001/files/)
        └── internal_example (INTERNAL/managed — deleted when dropped)

ADF:
  └── ls_databricks_dev   (Linked Service → dbw-ev-dev)
  └── pl_day4_notebook_test (Pipeline with Notebook Activity)

Notebooks:
  └── Shared/day4-practice/
        ├── ex1_secret_scope
        ├── ex2_adls_connect
        ├── ex3_external_table
        ├── ex4_external_volume
        └── adf_triggered_notebook
```

---

## Quick Verification Checklist

| Task | How to verify |
|---|---|
| Secret scope created | Navigate to `#secrets/createScope` → scope `kv-scope` exists |
| Secrets readable | `dbutils.secrets.list("kv-scope")` → shows key names |
| ADLS connection works | `dbutils.fs.ls("abfss://bronze@stadlsdev001...")` → no auth error |
| Delta written to ADLS | `dbutils.fs.ls(delta_path)` → shows `_delta_log/` and `.parquet` files |
| External table queryable | `SELECT * FROM dev_catalog.bronze.sample_people` → returns rows |
| External table type confirmed | `DESCRIBE EXTENDED` → `Type = EXTERNAL` |
| DROP keeps files | After `DROP TABLE`, ADLS files still exist |
| External volume accessible | `dbutils.fs.ls("/Volumes/dev_catalog/bronze/blob_files/")` → no error |
| Volume write works | `dbutils.fs.put(...)` → file appears in Azure Portal Blob container |
| Internal table vs external | DROP internal = data gone; DROP external = data stays on ADLS |
| ADF linked service works | ADF → Manage → ls_databricks_dev → Test connection → Succeeded |
| ADF triggers notebook | ADF Debug → Notebook Activity → Succeeded → runOutput has exit value |
