# Day 4 — Azure Databricks: Access Control, Secrets, Catalog & ADF Integration

> **Goal:** Learn how to securely connect Databricks to Azure storage, manage secrets with Key Vault, register external tables and volumes in Unity Catalog, run notebooks from ADF, and understand internal vs external data objects.
> **Storage accounts used in this project:**
> - `stadlsdev001` — ADLS Gen2 (used for Delta tables / external tables)
> - `stblobdev001` — Azure Blob Storage (used as external volume)

---

## Part 1: How Databricks Accesses Azure Storage — Two Approaches

Databricks needs two things to read from Azure storage:

```
1. Authentication   → prove who Databricks is (Service Principal or Managed Identity)
2. Authorisation    → that identity has the correct role on the storage account
```

There are two approaches to set this up — the **legacy approach** (still works, widely seen in older projects) and the **Unity Catalog approach** (current standard). You need to understand both.

---

### Approach 1 — Legacy: spark.conf.set() per Notebook

> Used before Unity Catalog. Still seen in many projects and documentation. **Do not use for new workspaces with Unity Catalog.**

Each notebook manually sets OAuth credentials in the Spark config before accessing storage:

```python
# ❌ Legacy — credentials configured inside every notebook
storage_account = "stadlsdev001"
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
               dbutils.secrets.get(scope="kv-scope", key="sp-client-id"))
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
               dbutils.secrets.get(scope="kv-scope", key="sp-client-secret"))
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/<tenant-id>/oauth2/token")

# Only then can you read
df = spark.read.format("delta").load(f"abfss://bronze@{storage_account}.dfs.core.windows.net/data/")
```

**Problems with the legacy approach:**
- Every notebook must repeat these 5 lines per storage account
- Credentials are visible in cluster Spark config and logs
- If a secret rotates, every notebook that hardcodes the key name must still be checked
- No centralised governance — anyone with notebook access can read any path

---

### Approach 2 — Unity Catalog: Storage Credential + External Location

> Current standard. Admin sets up credentials once. Notebooks access storage with no config at all.

```
Admin sets up (once):
  Storage Credential  →  holds Service Principal details in Unity Catalog
        │
  External Location   →  maps abfss://bronze@stadlsdev001...  to that credential
        │
  Grant USAGE + READ FILES on the External Location to data engineers

Developer notebook (no credentials, no spark.conf):
  ✅ spark.read.format("delta").load("abfss://bronze@stadlsdev001.dfs.core.windows.net/data/")
  ✅ SELECT * FROM dev_catalog.bronze.sample_people
  — Unity Catalog resolves the path → finds the external location → injects the credential automatically
```

**Advantages:**
- Zero credential code in notebooks
- One credential update propagates to all tables/volumes instantly
- Full RBAC: grant SELECT on a table without giving access to the raw storage path
- Complete audit trail: who accessed which table, when

**Side-by-side comparison:**

| | Legacy (spark.conf.set) | Unity Catalog |
|---|---|---|
| Where credentials live | Each notebook + cluster config | Storage Credential (one place) |
| Who configures auth | Every developer | Admin once |
| Notebook code for auth | 5+ lines per storage account | Zero lines |
| Credential rotation | Must update secrets + notebooks | Update Storage Credential only |
| Governance | None — path-level only | Table/volume/column-level RBAC |
| Audit logs | Spark logs (noisy) | Unity Catalog audit (clean) |
| Use in new projects | ❌ Avoid | ✅ Use this |

**In this day we use the Unity Catalog approach throughout.** The legacy pattern is shown here so you recognise it when you see it in older code or documentation — not to use it.

---

## Part 2: Prerequisites — What Must Exist Before Day 4 Steps

Check these exist in your Azure environment before starting:

```
Azure Resources needed:
  ├── Resource Group: rg-ev-dev
  ├── Databricks Workspace: dbw-ev-dev  (Day 2)
  ├── Key Vault: key-vault-session-ded  (or your KV name)
  ├── ADLS Gen2 Storage Account: stadlsdev001
  │     └── Container: bronze  (or any container)
  ├── Blob Storage Account: stblobdev001
  │     └── Container: files   (or any container)
  └── Service Principal (App Registration) in Azure AD
        ├── Client ID    (stored in Key Vault)
        ├── Client Secret (stored in Key Vault)
        └── Tenant ID    (stored in Key Vault or noted separately)
```

**Key Vault secrets needed (names used in this day):**

| Secret name in KV | What it holds |
|---|---|
| `sp-client-id` | Application (client) ID of the Service Principal |
| `sp-client-secret` | Client secret of the Service Principal |
| `sp-tenant-id` | Azure AD Tenant ID |

If these do not exist in your Key Vault yet — add them:
1. Azure Portal → Key Vaults → `key-vault-session-ded` → **Secrets** → **+ Generate/Import**
2. Add each secret by name and value

---

## Part 3: Azure Key Vault — Reading Secrets in Databricks

This is the most important security pattern in Databricks. **Never hardcode credentials in a notebook.**

### 3.1 What Is a Secret Scope

A **Secret Scope** is a named collection in Databricks that points to a secret store. You create it once, then notebooks use `dbutils.secrets.get(scope, key)` to read values without ever printing or logging them.

```
Two types:
  ├── Databricks-managed scope
  │     Secrets stored inside Databricks' own encrypted store
  │     Created via CLI or REST API
  │
  └── Azure Key Vault-backed scope  ← recommended
        Secrets stored in Azure Key Vault
        Databricks reads them via the scope at runtime
        If you rotate a secret in KV, Databricks picks up the new value automatically
```

### 3.2 Create an AKV-Backed Secret Scope (Step by Step)

> Do this once per workspace. Requires workspace admin access.

**Step 1 — Open the secret scope creation page**

Navigate to this URL (replace with your workspace URL):
```
https://<your-workspace-url>#secrets/createScope
```

Example:
```
https://adb-1234567890123456.7.azuredatabricks.net/#secrets/createScope
```

> This page is NOT accessible from the normal workspace UI — you must type the URL directly.

**Step 2 — Fill in the form**

| Field | Value | Where to find it |
|---|---|---|
| Scope name | `kv-scope` | Any name — you will use this in `dbutils.secrets.get(scope="kv-scope", ...)` |
| Manage Principal | `All Users` (dev) or `Creators` (prod) | Creators = only the scope creator can manage it |
| DNS Name | `https://key-vault-session-ded.vault.azure.net/` | Azure Portal → Key Vault → Overview → Vault URI |
| Resource ID | `/subscriptions/<sub-id>/resourceGroups/rg-ev-dev/providers/Microsoft.KeyVault/vaults/key-vault-session-ded` | Azure Portal → Key Vault → Properties → Resource ID |

**Step 3 — Click Create**

You will see: `Secret scope kv-scope has been created.`

**Step 4 — Verify in a notebook**

Open any notebook → run:
```python
# List all secret keys in the scope (shows names only, never values)
secrets = dbutils.secrets.list(scope="kv-scope")
for s in secrets:
    print(s.key)
```

Expected output: the key names from your Key Vault (`sp-client-id`, `sp-client-secret`, `sp-tenant-id`, etc.)

### 3.3 Reading a Secret in a Notebook

```python
# Read credentials from Key Vault via the scope
client_id     = dbutils.secrets.get(scope="kv-scope", key="sp-client-id")
client_secret = dbutils.secrets.get(scope="kv-scope", key="sp-client-secret")
tenant_id     = dbutils.secrets.get(scope="kv-scope", key="sp-tenant-id")

# Safe to use in code — if you print these, Databricks shows [REDACTED]
print("Client ID:", client_id)      # prints: Client ID: [REDACTED]
print("Secrets loaded successfully")
```

> **Important:** `dbutils.secrets.get()` returns the real string value — you can pass it to Spark config or JDBC URLs. The `[REDACTED]` only appears in notebook output and logs, not in the actual variable value.

### 3.4 Where Secrets Are Still Used With Unity Catalog

With Unity Catalog, notebooks do **not** use `dbutils.secrets.get()` to access ADLS or Blob Storage — Unity Catalog's Storage Credential handles that transparently. Secrets are still used in two specific places:

**Place 1 — Creating the Storage Credential (admin, done once in UI)**

When an admin creates a Storage Credential in Unity Catalog, they paste the Service Principal values directly into the Unity Catalog UI form. They get those values from Key Vault manually — there is no notebook code involved.

**Place 2 — Non-storage secrets in notebooks (API keys, DB passwords, etc.)**

If a notebook needs an API key, a database password, or any secret that is not a storage credential, it still reads from Key Vault via the secret scope:

```python
# ✅ Still valid — for non-storage secrets like API keys, DB passwords
api_key    = dbutils.secrets.get(scope="kv-scope", key="voltgrid-api-key")
db_password = dbutils.secrets.get(scope="kv-scope", key="sql-db-password")

print("Secrets loaded")
# Use in code — shown as [REDACTED] in output
```

**What you do NOT need to do with Unity Catalog (legacy pattern — for reference only):**

```python
# ❌ Legacy — do not use this pattern in a Unity Catalog workspace
# spark.conf.set("fs.azure.account.auth.type.stadlsdev001...", "OAuth")
# spark.conf.set("fs.azure.account.oauth2.client.id...", client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret...", client_secret)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint...", endpoint)
# — Unity Catalog replaces ALL of this
```

**With Unity Catalog, after the admin sets up the Storage Credential and External Location, a notebook accesses storage like this:**

```python
# ✅ Unity Catalog way — just use the path, no auth code needed
files = dbutils.fs.ls("abfss://bronze@stadlsdev001.dfs.core.windows.net/")
for f in files:
    print(f.name)
```

Or read a Delta table directly:

```python
# ✅ Unity Catalog way — read from ADLS path, no spark.conf needed
df = spark.read.format("delta").load("abfss://bronze@stadlsdev001.dfs.core.windows.net/sample_people/")
df.show()
```

Unity Catalog intercepts the path, matches it to the External Location, and injects the Storage Credential automatically.

---

## Part 4: Internal vs External — Tables and Volumes

This is a fundamental Unity Catalog concept. Everything in the catalog is either **internal (managed)** or **external**.

### 4.1 The Difference

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

### 4.2 External Table vs External Volume

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

## Part 5: Unity Catalog Setup — Prerequisites

Before creating external tables and volumes, Unity Catalog needs a **storage credential** and an **external location**. These are admin-level operations done once.

```
Unity Catalog objects (top to bottom):
  Metastore              ← account-level, one per region
    └── Catalog          ← like a database server
          └── Schema     ← like a database
                ├── Table     ← structured data (internal or external)
                └── Volume    ← file storage (internal or external)
```

### 5.1 Create a Storage Credential (Admin — once per Service Principal)

A storage credential stores the Service Principal details so Unity Catalog can authenticate to storage on your behalf.

**Step 1 — Open Unity Catalog in the Account Console**

1. Navigate to `https://accounts.azuredatabricks.net`
2. Left sidebar → **Catalog** → **External locations** → **Credentials** tab
3. Click **+ Add a credential**

**Step 2 — Fill in the credential form**

| Field | Value |
|---|---|
| Credential name | `sp-stadls-credential` |
| Authentication type | `Service Principal` |
| Directory (tenant) ID | your Azure AD Tenant ID |
| Application (client) ID | your Service Principal Client ID |
| Client secret | your Service Principal Client Secret |

Click **Create**

> Alternatively, create the credential directly in the workspace:
> Catalog → External Data → Credentials → Create credential

### 5.2 Create External Location for ADLS (stadlsdev001)

An external location maps a Unity Catalog path prefix to a real storage path. Any table or volume under this path inherits access automatically.

**Step 1 — Workspace → Catalog → External Data → External Locations → + Create location**

**Step 2 — Fill in**

| Field | Value |
|---|---|
| External location name | `ext-loc-stadls` |
| URL | `abfss://bronze@stadlsdev001.dfs.core.windows.net/` |
| Storage credential | `sp-stadls-credential` |

Click **Create**

**Step 3 — Test the location**

Click `ext-loc-stadls` → **Test connection** → should show `Connection successful`

### 5.3 Create External Location for Blob Storage (stblobdev001)

**Step 1 — Create a second storage credential for Blob**

| Field | Value |
|---|---|
| Credential name | `sp-stblob-credential` |
| Authentication type | `Service Principal` |
| (same tenant, client ID, secret as above if same SP) | |

**Step 2 — Create the external location**

| Field | Value |
|---|---|
| External location name | `ext-loc-stblob` |
| URL | `abfss://files@stblobdev001.dfs.core.windows.net/` |
| Storage credential | `sp-stblob-credential` |

> Note: Even Blob Storage uses `abfss://` with the hierarchical namespace driver. Make sure HNS (Hierarchical Namespace) is enabled on `stblobdev001` OR use the wasbs:// scheme for plain Blob without HNS.

Click **Create** → **Test connection**

---

## Part 6: Creating a Catalog and Schema

### Step 1 — Create a Catalog

A catalog is the top-level namespace. Create one for the dev environment.

1. **Workspace → Catalog** (left sidebar) → **+ Create catalog**
2. Fill in:

   | Field | Value |
   |---|---|
   | Catalog name | `dev_catalog` |
   | Storage | Default (Unity Catalog managed storage) |

3. Click **Create**

### Step 2 — Create Schemas

Inside `dev_catalog`, create two schemas:

1. Click `dev_catalog` → **+ Create schema**
   - Schema name: `bronze`
   - Click **Create**

2. Repeat → **+ Create schema**
   - Schema name: `silver`
   - Click **Create**

---

## Part 7: External Delta Tables on stadlsdev001

An external Delta table points to a Delta-format folder on ADLS. We first write a Delta file to ADLS, then register it as an external table.

> **Prerequisite:** The admin has already created:
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

If this fails, the admin needs to complete Part 5 (Storage Credential + External Location) first.

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

## Part 8: External Volume on stblobdev001

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

## Part 9: Internal vs External — Side-by-Side Demo

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

## Part 10: ADF — Running a Databricks Notebook Activity

Azure Data Factory can trigger a Databricks notebook as a step in an ADF pipeline. The notebook runs on a Databricks cluster and ADF waits for it to complete.

### 10.1 What You Need in ADF

```
ADF pipeline
  └── Databricks Notebook Activity
        ├── Linked Service → connection to your Databricks workspace
        ├── Notebook path  → which notebook to run
        ├── Cluster config → new job cluster or existing cluster
        └── Base parameters → key-value pairs passed as widgets
```

### 10.2 Step 1 — Create a Databricks Linked Service in ADF

1. **ADF Studio** → **Manage** (toolbox icon, left sidebar) → **Linked services** → **+ New**
2. Search `Databricks` → select **Azure Databricks** → **Continue**
3. Fill in:

   | Field | Value |
   |---|---|
   | Name | `ls_databricks_dev` |
   | Azure subscription | your subscription |
   | Databricks workspace | `dbw-ev-dev` |
   | Select cluster | `New job cluster` |
   | Databricks runtime version | `15.4 LTS` |
   | Node type | `Standard_D4s_v3` |
   | Python version | `3` |

4. **Authentication** section:
   - Method: `Access token` (use a PAT) OR `Service Principal`
   - If using PAT: paste the token you generated in Day 3 Exercise 1
   - If using Service Principal: enter client ID and secret from Key Vault

5. Click **Test connection** → should show **Connection successful**
6. Click **Apply**

### 10.3 Step 2 — Create a Notebook to Run from ADF

In your Databricks workspace, create a notebook that ADF will call:

1. **Workspace** → `Shared/day4-practice` → **Create** → **Notebook**
2. Name: `adf_triggered_notebook`

Paste this content:

```python
# Cell 1 — Read parameters passed by ADF
dbutils.widgets.text("adf_pipeline_name", "unknown", "ADF Pipeline")
dbutils.widgets.text("adf_run_id",        "unknown", "ADF Run ID")
dbutils.widgets.text("env",               "dev",     "Environment")

pipeline_name = dbutils.widgets.get("adf_pipeline_name")
run_id        = dbutils.widgets.get("adf_run_id")
env           = dbutils.widgets.get("env")

print(f"Triggered by ADF pipeline: {pipeline_name}")
print(f"ADF Run ID: {run_id}")
print(f"Environment: {env}")
```

```python
# Cell 2 — Do some simple work
data = [(i, f"record_{i}", i * 10) for i in range(1, 6)]
df = spark.createDataFrame(data, ["id", "name", "value"])
df.show()
print(f"Processed {df.count()} records")
```

```python
# Cell 3 — Return a result to ADF
result = f"SUCCESS: processed in {env} environment"
dbutils.notebook.exit(result)
```

Note the notebook path: `Shared/day4-practice/adf_triggered_notebook` — you will need this in ADF.

### 10.4 Step 3 — Add the Databricks Notebook Activity to an ADF Pipeline

1. **ADF Studio** → **Author** → open your pipeline (e.g. `pl_bronze_api_payments`) OR create a new pipeline
2. From the **Activities** panel → **Databricks** section → drag **Notebook** onto the canvas
3. Click the Notebook activity → configure the tabs:

**General tab:**
| Field | Value |
|---|---|
| Name | `Run Silver Transform` |

**Azure Databricks tab:**
| Field | Value |
|---|---|
| Databricks linked service | `ls_databricks_dev` |

**Settings tab:**
| Field | Value |
|---|---|
| Notebook path | `/Shared/day4-practice/adf_triggered_notebook` |
| Base parameters | (see below) |

**Base parameters (click + New for each):**

| Name | Value |
|---|---|
| `adf_pipeline_name` | `@pipeline().Pipeline` |
| `adf_run_id` | `@pipeline().RunId` |
| `env` | `dev` |

`@pipeline().Pipeline` and `@pipeline().RunId` are ADF system variables — they inject the actual pipeline name and run ID automatically.

### 10.5 Step 4 — Connect the Activity in the Pipeline

If you have a Copy Activity before the Notebook Activity:
- Drag the green arrow from the Copy Activity → Notebook Activity
- This means: Notebook runs ONLY if the Copy succeeds

### 10.6 Step 5 — Test the Activity

1. Click **Debug** in the ADF pipeline toolbar
2. The pipeline runs — watch the Notebook Activity turn from blue (running) to green (succeeded)
3. Click the activity → **Output** → you will see the notebook's exit value:
   ```json
   {
     "runOutput": "SUCCESS: processed in dev environment"
   }
   ```

### 10.7 Step 6 — Verify in Databricks

1. Databricks → **Workflows** → **Job runs** (not Jobs — Job runs is the raw run history)
2. You will see a run triggered by ADF — it shows `Run by: ADF` in the source column
3. Click the run → **Logs** → see all notebook cell outputs

---

## Part 11: Cluster Access Modes and Notebook Permissions

### 11.1 Who Can Run a Notebook

```
Notebook permission levels:
  Can View    → read the code, cannot run
  Can Run     → run without editing
  Can Edit    → edit and run
  Can Manage  → edit, run, delete, change permissions

Set via: right-click notebook → Permissions → Add user/group
```

### 11.2 Access Mode and What It Affects

When a notebook runs on a cluster, the cluster's access mode determines what the notebook can do:

```
Single User cluster:
  ├── Full access to cluster resources
  ├── Supports Python, SQL, Scala, R
  ├── Can install libraries with %pip
  ├── Unity Catalog: full support
  └── Only ONE user at a time

Shared cluster:
  ├── Multiple users simultaneously
  ├── Supports Python and SQL only (no Scala, no R)
  ├── %pip install is restricted (admin must approve)
  ├── Unity Catalog: full support
  └── User sessions are isolated from each other
```

**For ADF-triggered notebooks:** ADF runs notebooks as the **Service Principal** identity defined in the Linked Service. The notebook runs on a job cluster (Single User mode, service principal as the user). It does NOT use your personal login.

---

## Quick Reference — Day 4 Terminologies

```
Term                      Definition
───────────────────────────────────────────────────────────────────────────────
Secret Scope              A named collection in Databricks that points to a
                          secret store (Databricks-managed or AKV-backed)

AKV-backed scope          Secret scope where secrets live in Azure Key Vault —
                          Databricks reads them via OAuth at runtime

dbutils.secrets.get()     Python function to read a secret value — shows
                          [REDACTED] in notebook output, real value in memory

Service Principal         An Azure AD app identity used by Databricks to
                          authenticate to storage without a user login

External Location         A Unity Catalog object that maps a storage path prefix
                          to a storage credential — enables access to that path

Storage Credential        Unity Catalog object that holds the auth details
                          (Service Principal) for accessing storage

External Table            A Delta table whose files live on your own storage
                          (ADLS) — DROP removes metadata, not files

Internal (Managed) Table  A table whose files are managed by Databricks —
                          DROP removes both metadata and files

External Volume           A Unity Catalog object that exposes a storage path
                          (Blob/ADLS) as /Volumes/catalog/schema/volume/

Internal Volume           A volume whose storage is managed by Databricks
                          inside the Unity Catalog managed storage location

abfss://                  URI scheme for ADLS Gen2 and Blob (with HNS) access
                          Format: abfss://container@account.dfs.core.windows.net/

Unity Catalog             Databricks' centralised governance layer — manages
                          catalogs, schemas, tables, volumes, and permissions

Metastore                 Account-level Unity Catalog store — one per region,
                          shared across all workspaces in the account

ADF Databricks Linked     ADF connection object that points to a Databricks
Service                   workspace using PAT or Service Principal auth

Databricks Notebook       ADF activity that triggers a notebook on Databricks
Activity                  and waits for it to complete — result available in ADF

Base Parameters           Key-value pairs passed from ADF into a Databricks
                          notebook — read with dbutils.widgets.get()

runOutput                 The value from dbutils.notebook.exit() — returned to
                          ADF and visible in the activity output JSON
```
