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
        ├── Storage Credential: mi-stadls-credential   ← Managed Identity for stadlsdev001
        ├── Storage Credential: mi-stblob-credential   ← Managed Identity for stblobdev001
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

### 2.1 What the "Create a new credential" form looks like

When you click **Create credential** in the Workspace UI, you see two radio button options at the top:

```
Create a new credential
─────────────────────────────────────────────────────
  ○ Storage Credential    ● Service Credential

  Credential name*
  ┌──────────────────────────────────────┐
  │                                      │
  └──────────────────────────────────────┘

  Access connector ID  Learn more *
  ┌──────────────────────────────────────┐
  │                                      │
  └──────────────────────────────────────┘

  User assigned managed identity ID (optional)
  ┌──────────────────────────────────────┐
  │                                      │
  └──────────────────────────────────────┘

  Comment
  ┌──────────────────────────────────────┐
  │                                      │
  └──────────────────────────────────────┘

                         [ Cancel ]  [ Create ]
```

**Both radio buttons (Storage Credential and Service Credential) show the same fields:**
- **Access connector ID** — the Azure resource ID of the Access Connector
- **User assigned managed identity ID** — optional, only if using a user-assigned MI instead of system-assigned

**There is NO client ID / client secret / tenant ID field anywhere in this UI.** On Azure, Unity Catalog credentials always use **Managed Identity via Access Connector** — not a Service Principal password.

| Radio Button | Purpose | Auth method |
|---|---|---|
| Storage Credential | Access ADLS/Blob storage → used for External Locations | Access Connector (Managed Identity) |
| Service Credential | Access cloud services (Azure OpenAI, etc.) | Access Connector (Managed Identity) |

**For creating External Locations to access stadlsdev001 and stblobdev001 → select `Storage Credential`.**

---

### 2.2 What is an Access Connector?

An **Azure Databricks Access Connector** is an Azure resource (like a VM or storage account) that has a **system-assigned managed identity**. You grant this managed identity access to your storage account, then give its resource ID to Unity Catalog as the credential.

```
Flow:
  Unity Catalog
    → uses Access Connector's managed identity
    → managed identity has "Storage Blob Data Contributor" on stadlsdev001
    → access granted — no password, no secret, no rotation needed
```

**Why Managed Identity is preferred over Service Principal:**
- No client secret to rotate or leak
- Azure manages the identity automatically
- Access Connector resource lives in your Azure subscription

---

### 2.3 Step 1 — Create the Access Connector in Azure Portal (do this first)

Before creating the credential in Databricks, you need an **Azure Databricks Access Connector** resource in Azure.

1. Open **Azure Portal** → search `Access Connector for Azure Databricks` in the top search bar
2. Click **+ Create**
3. Fill in:

   | Field | Value |
   |---|---|
   | Subscription | your subscription |
   | Resource group | `data-engineering-daily-grp` |
   | Name | `ac-databricks-dev` |
   | Region | same region as your Databricks workspace |

4. Click **Review + create** → **Create**
5. Wait for deployment to complete (~1 minute)
6. Click **Go to resource**
7. On the Access Connector overview page, copy the **Resource ID** — it looks like:
   ```
   /subscriptions/81dd57e1-876a-4fcc-8778-e06f68c13228/resourceGroups/data-engineering-daily-grp/providers/Microsoft.Databricks/accessConnectors/ac-databricks-dev
   ```
   You need this for the Databricks credential form.

---

### 2.4 Step 2 — Grant the Access Connector Access to Storage Accounts

The Access Connector's managed identity must have `Storage Blob Data Contributor` role on each storage account.

**For stadlsdev001:**
1. Azure Portal → **Storage accounts** → `stadlsdev001`
2. Left menu → **Access Control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. **Role tab:** search `Storage Blob Data Contributor` → select it → **Next**
5. **Members tab:**
   - Assign access to: `Managed identity`
   - Click **+ Select members**
   - Managed identity type: `Access Connector for Azure Databricks`
   - Select: `ac-databricks-dev`
   - Click **Select** → **Next** → **Review + assign**

**For stblobdev001** — repeat the exact same steps:
1. Azure Portal → **Storage accounts** → `stblobdev001`
2. Left menu → **Access Control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. Role: `Storage Blob Data Contributor`
5. Managed identity: `ac-databricks-dev`
6. Click **Review + assign**

> Wait 1–2 minutes after role assignment before testing — Azure RBAC propagation takes time.

---

### 2.5 Step 3 — Create the Storage Credential in Databricks (for stadlsdev001)

Now go back to Databricks:

1. Left sidebar → **Catalog** (grid icon)
2. Left panel → **External Data**
3. Click **Credentials** tab
4. Click **Create credential** (top right)

The form opens. Fill in exactly:

| Field | Value |
|---|---|
| Radio button | `Storage Credential` (left option — already selected) |
| Credential Type | `Azure Managed Identity` (already selected by default — leave it) |
| Credential name | `mi-stadls-credential` |
| Access connector ID | paste the full Resource ID copied in Step 2.3 |
| User assigned managed identity ID | leave blank (we are using system-assigned) |
| Comment | `Managed identity credential for stadlsdev001` |

Click **Create**.

You will see `mi-stadls-credential` appear in the Credentials list.

---

### 2.6 Step 4 — Create the Storage Credential for stblobdev001

Click **Create credential** again. Fill in:

| Field | Value |
|---|---|
| Radio button | `Storage Credential` |
| Credential Type | `Azure Managed Identity` |
| Credential name | `mi-stblob-credential` |
| Access connector ID | same Resource ID as above (same Access Connector) |
| User assigned managed identity ID | leave blank |
| Comment | `Managed identity credential for stblobdev001` |

Click **Create**.

> One Access Connector can be used for multiple credentials pointing to different storage accounts — as long as the managed identity has been granted access to each storage account (which we did in Step 2.4).

---

### 2.7 Verify Both Credentials Exist

Credentials list should show:
```
mi-stadls-credential   Azure Managed Identity   Created by: you
mi-stblob-credential   Azure Managed Identity   Created by: you
```

If you see `You do not have permission to create credentials` — you need **Metastore Admin** or **Account Admin** role in Databricks.

---

## Part 3: Create External Locations

### 3.1 What the "Create a new external location" form looks like

```
Create a new external location
─────────────────────────────────────────────────────
  External location name*
  ┌──────────────────────────────────────┐
  │                                      │
  └──────────────────────────────────────┘

  Storage type*
  ┌─────────────────────────────────────┐
  │ Azure Data Lake Storage         ▼  │  ← default
  └─────────────────────────────────────┘

  URL*  ⓘ
  Enter the bucket path that you want to use as the external location.
  Note: This must be an ADLS Gen2 storage account with a hierarchical namespace
  ┌──────────────────────────────────────────────────────┐ ┌──────────────────┐
  │  abfss://<container_name>@<storage_account_name>...  │ │ Copy from DBFS ▼ │
  └──────────────────────────────────────────────────────┘ └──────────────────┘

  Storage credential*  Learn more
  Provide a storage credential capable of accessing the URL
  ┌─────────────────────────────────────┐
  │ Select storage credential       ▼  │
  └─────────────────────────────────────┘

  Comment
  ┌──────────────────────────────────────┐
  │                                      │
  └──────────────────────────────────────┘

  > Advanced Options

                         [ Cancel ]  [ Create ]
```

**Key fields explained:**

| Field | What to do |
|---|---|
| External location name | Type a name — no spaces, use hyphens |
| Storage type | Leave as `Azure Data Lake Storage` (default) |
| URL | Type the `abfss://` path manually — do NOT use the "Copy from DBFS" dropdown (explained below) |
| Storage credential | Select from the dropdown — shows credentials you created in Part 2 |
| Comment | Optional — leave blank |

> **"Copy from DBFS" dropdown — ignore it**
> When you click the URL field, a dropdown appears showing paths like `/databricks-datasets`, `/Volumes`, `/databricks/mlflow-tracking`, `/databricks/mlflow-registry`, `/databricks-results`, `/Volume`, `/volumes`, `/volume`, `/`. These are internal Databricks filesystem (DBFS) paths — they are NOT Azure storage paths. **Do not select any of them.** Just type the `abfss://` URL directly into the URL field and ignore the dropdown.

---

### 3.2 Open External Locations

1. Left sidebar → **Catalog** (grid icon)
2. Left panel → **External Data**
3. Click **External Locations** tab
4. Click **+ Create location** (top right)

---

### 3.3 Create External Location for stadlsdev001

The URL field shows a placeholder: `abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>`

**How to fill in the URL — replace each part:**

```
abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>
         ↓                  ↓                                          ↓
        bronze           stadlsdev001                              (leave empty)

Result: abfss://bronze@stadlsdev001.dfs.core.windows.net/
```

Fill in the full form:

| Field | Value |
|---|---|
| External location name | `ext-loc-stadls` |
| Storage type | `Azure Data Lake Storage` (leave default) |
| URL | `abfss://bronze@stadlsdev001.dfs.core.windows.net/` |
| Storage credential | `mi-stadls-credential` (select from dropdown) |
| Comment | leave blank |

Click **Create**.

---

### 3.4 Test the Connection — Understanding the Results

After clicking Create, click **Test connection**. You will see a result like this:

```
Location Type: Directory

  ✅ Success  - Read
  ✅ Success  - List
  ✅ Success  - Write
  ✅ Success  - Delete
  ✅ Success  - Path Exists
  ✅ Success  - Hierarchical Namespace Enabled
  ❌ Failed   - File Events Resource Provision
  ❌ Failed   - File Events Resource Teardown

⚠️ File Events Permissions Not Verified
Your storage credential can read and write to this location,
but file events permissions could not be verified.
File events are optional but recommended; they improve
ingestion performance and reduce cloud storage listing costs.
```

**What this means:**

| Check | Result | Action needed |
|---|---|---|
| Read / List / Write / Delete | ✅ Success | External Location works — notebooks can access the storage |
| Hierarchical Namespace Enabled | ✅ Success | HNS is on — `abfss://` URI works correctly |
| File Events Resource Provision | ❌ Failed | **Optional** — no action needed to proceed |
| File Events Resource Teardown | ❌ Failed | **Optional** — no action needed to proceed |

**The External Location is working correctly.** The File Events failure is a warning, not a blocker. File Events are used for event-driven auto-refresh of tables (like streaming ingestion) — they are NOT needed for regular Delta table reads and writes.

**If you want to fix the File Events warning** (optional — can be done later):
Assign these additional roles to the Access Connector managed identity on `stadlsdev001`:
1. `Storage Account Contributor`
2. `EventGrid EventSubscription Contributor`
3. `Storage Queue Data Contributor`

Azure Portal → `stadlsdev001` → **Access Control (IAM)** → **+ Add role assignment** → repeat for each role, selecting `ac-databricks-dev` managed identity each time.

**If the core checks (Read/List/Write) are failing instead:**
- `Authorization failed` → managed identity does not have `Storage Blob Data Contributor` on `stadlsdev001` — re-check Part 2.4
- `Path not found` → the container `bronze` does not exist — create it in Azure Portal → `stadlsdev001` → **Containers** → **+ Container**

---

### 3.5 Create External Location for stblobdev001

**First — confirm HNS is enabled on stblobdev001:**
1. Azure Portal → **Storage accounts** → `stblobdev001`
2. Left menu → **Overview**
3. Look for **Hierarchical namespace** in the Properties panel on the right
4. Must show **Enabled** — otherwise `abfss://` will not work

**Fill in the URL — replace each part:**

```
abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>
         ↓                  ↓                                          ↓
       (your container)  stblobdev001                              (leave empty)

Example: abfss://files@stblobdev001.dfs.core.windows.net/
```

> To find the container name: Azure Portal → `stblobdev001` → **Containers** → note the container name listed there. Use that name in the URL.

Fill in the form:

| Field | Value |
|---|---|
| External location name | `ext-loc-stblob` |
| Storage type | `Azure Data Lake Storage` (leave default) |
| URL | `abfss://<your-container-name>@stblobdev001.dfs.core.windows.net/` |
| Storage credential | `mi-stblob-credential` (select from dropdown) |

Click **Create** → **Test connection**.

Same result expected — Read/List/Write/Delete/HNS will be ✅. File Events may be ❌ (optional warning, not a blocker).

---

### 3.6 Verify Both External Locations

External Locations list should show:
```
ext-loc-stadls   abfss://bronze@stadlsdev001.dfs.core.windows.net/      mi-stadls-credential
ext-loc-stblob   abfss://<container>@stblobdev001.dfs.core.windows.net/ mi-stblob-credential
```

---

## Part 4: Create a Catalog and Schemas

A **catalog** is the top-level namespace in Unity Catalog — like a database server. A **schema** is a database inside a catalog — it holds tables and volumes.

### 4.1 What the "Create a new catalog" form looks like

```
Create a new catalog
─────────────────────────────────────────────────────────────────
  Catalog name*
  ┌──────────────────────────────────────────────────────────┐
  │ dev_catalog                                              │
  └──────────────────────────────────────────────────────────┘

  Type*
  ┌──────────────────────────────────────────────────────────┐
  │ Standard                                             ▼  │
  └──────────────────────────────────────────────────────────┘

  Storage location
  Cloud storage location used for managed tables and volumes
  in this catalog. If not specified, it defaults to the
  metastore root location.
  ┌────────────────────────────┐  ┌────────────────────┐
  │ Select external location ▼ │  │ sub/path           │
  └────────────────────────────┘  └────────────────────┘
  Create a new external location ↗

                             [ Cancel ]  [ Create ]
```

**The Storage location field is required in this project** — see the error explanation below.

---

### 4.2 Common Error: "Metastore storage root URL does not exist"

When you click **Create** without filling in the Storage location, you will see:

```
⚠️ Metastore storage root URL does not exist. Default Storage is
enabled in your account. You can use the UI to create a new catalog
using Default Storage, or please provide a storage location for the
catalog (for example 'CREATE CATALOG myCatalog MANAGED LOCATION
'<location-path>').
```

**Why this happens:**

The Unity Catalog Metastore was created without a default root storage account. This is common when Databricks was set up without linking a managed storage account. Because there is no default storage, Databricks does not know where to store internal/managed tables — so you must tell it explicitly.

**Two ways to fix this — choose one:**

---

**Fix Option 1 — UI: Select an External Location as catalog storage**

In the Create catalog form, fill in the Storage location:

| Field | Value |
|---|---|
| Catalog name | `dev_catalog` |
| Type | `Standard` |
| Select external location | `ext-loc-stadls` (select from dropdown) |
| sub/path | `dev_catalog` |

This means internal/managed tables in `dev_catalog` will be stored at:
`abfss://bronze@stadlsdev001.dfs.core.windows.net/dev_catalog/`

Click **Create**.

---

**Fix Option 2 — SQL: Run in a notebook (quickest)**

Open any notebook, attach to your cluster, run:

```sql
%sql
CREATE CATALOG IF NOT EXISTS dev_catalog
MANAGED LOCATION 'abfss://bronze@stadlsdev001.dfs.core.windows.net/dev_catalog/'
```

This is exactly what the error message suggests. The `MANAGED LOCATION` tells Databricks where to store internal/managed tables for this catalog.

**After the catalog is created with either option, verify it exists:**

```sql
%sql
SHOW CATALOGS
```

You should see `dev_catalog` in the list.

> If you see "Catalog already exists" — it was already created. Click `dev_catalog` in the left Catalog Explorer panel and move to Step 4.3.

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

---

> **ADF — Databricks Notebook Activity** has moved to **Day 7**.
> Day 7 covers all three Linked Service authentication methods (Access Token, System-assigned Managed Identity, User-assigned Managed Identity), cluster options, Base Parameters, runOutput, pipeline dependencies, scheduling, and monitoring.

---

## Part 9: Cluster Access Modes and Notebook Permissions

### 9.1 Cluster Access Modes

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

### 9.2 Notebook Permissions

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

## Part 10: Landing Zone Pattern — Full Pipeline Demo

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
