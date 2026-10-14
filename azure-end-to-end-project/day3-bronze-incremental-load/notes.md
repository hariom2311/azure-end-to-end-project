# Day 3 — Bronze Incremental Load: Control Table + Full Pagination

**Duration:** 3 hours | **Goal:** Replace the Day 2 single-page pipeline with a production-grade incremental loader that reads every page of the payments API, stores state in a Delta control table, and writes to a partitioned Bronze path — all driven by a control table, not hardcoded values.

> **Dependency order:** Parts 1–3 are Databricks setup (Unity Catalog, control table). Part 4 onward is ADF pipeline rebuild. Do not start ADF until the control table exists.

---

## What We Are Building

```
Day 2 (fixed):                     Day 3 (production):
─────────────────────────────      ─────────────────────────────────────────────────
page=1, page_size=100              All pages (1 to total_pages), page_size=1000
Fixed sink path                    Partitioned sink from control table
No history                         ingestion_date=YYYY-MM-DD/page_NNN.json
Overwrites every run               Idempotent — same run = same files, safe to rerun
No watermark                       Incremental: updated_after = last_successful_run
No control table                   Delta control table drives everything
```

### Control Table — What It Drives

```
bronze/ctl/pipeline_control (Delta table)
  ├── endpoint          →  which API path to call
  ├── sink_base_path    →  where in Bronze to land the files
  ├── file_prefix       →  stable file name prefix (e.g. "payments")
  ├── page_size         →  records per API call (1000)
  ├── last_watermark    →  updated_after value for next incremental run
  └── load_type         →  "full" or "incremental"

ADF reads this at pipeline start → uses values for all subsequent activities
```

### Incremental Logic (Idempotent)

```
First run (full load):
  updated_after = 1900-01-01T00:00:00Z    ← fetch everything
  pages: 1 to N (where N = total_pages from API page 1 response)
  sink:  bronze/api/payments/ingestion_date=2026-10-14/page_001.json ... page_NNN.json

Second run (incremental):
  updated_after = last_watermark (timestamp of first run)
  pages: 1 to M (where M = total_pages for records changed since last run)
  sink:  bronze/api/payments/ingestion_date=2026-10-15/page_001.json ... page_MMM.json

Re-run same day (idempotent):
  Same ingestion_date → same files → overwrite is safe
  Same updated_after → same data → no duplicates
  Delta table MERGE on session_id handles any re-processing downstream
```

---

## Architecture: Full Flow

```
[Databricks Unity Catalog — one-time setup]
  Access Connector (MI) → Storage Blob Data Contributor → evdatalakedev
  Storage Credential: mi-ev-credential
  External Location:  ext-loc-ev-bronze  (abfss://bronze@evdatalakedev...)
  Catalog: ev_catalog | Schema: ctl
  Delta Table: ev_catalog.ctl.pipeline_control

[ADF Pipeline: pl_bronze_incremental_load]
  Step 1: Read control table (Databricks notebook activity)
            → returns endpoint, sink_base_path, file_prefix, page_size, last_watermark
  Step 2: Get auth token from VoltGrid API (Web Activities)
  Step 3: Fetch page 1 → extract total_pages
  Step 4: Until loop (current_page <= total_pages)
            → Copy Activity: GET /api/db/payments/?page=N&page_size=1000&updated_after=<watermark>
            → Sink: bronze/api/payments/ingestion_date=YYYY-MM-DD/page_NNN.json
  Step 5: Update watermark in control table (Databricks notebook activity)
```

---

## Part 1 — Databricks Account Setup (Unity Catalog Prerequisites)

Before creating the control table, the Unity Catalog infrastructure must exist. This is a one-time setup.

### 1.1 Prerequisites Check

Verify in Azure Portal:
- Databricks workspace exists (`dbw-ev-intelligence-dev` in `rg-ev-intelligence-dev`)
- ADLS Gen2 `evdatalakedev` has `bronze` container
- `MicrosoftDatabricks` resource provider is registered

```bash
az provider show --namespace Microsoft.Databricks --query registrationState -o tsv
# Expected: Registered
```

### 1.2 Create Access Connector for Azure Databricks

The Access Connector is an Azure resource whose Managed Identity Unity Catalog uses to access storage — no password, no rotation.

**Via Portal:**
1. Portal → search `Access Connector for Azure Databricks` → **+ Create**
2. Fill:

   | Field | Value |
   |---|---|
   | Subscription | yours |
   | Resource group | `rg-ev-intelligence-dev` |
   | Name | `ac-ev-databricks` |
   | Region | `Central India` |

3. **Review + Create** → **Create** (~1 minute)
4. Go to resource → **Overview** → copy the **Resource ID**:
   ```
   /subscriptions/<sub-id>/resourceGroups/rg-ev-intelligence-dev/providers/Microsoft.Databricks/accessConnectors/ac-ev-databricks
   ```
   Save this — you paste it into Databricks in Part 1.4.

**Via CLI:**
```bash
az databricks access-connector create \
  --resource-group rg-ev-intelligence-dev \
  --name ac-ev-databricks \
  --location centralindia \
  --identity-type SystemAssigned
```

### 1.3 Grant Access Connector Permission on ADLS Gen2

The Access Connector's Managed Identity must have `Storage Blob Data Contributor` on `evdatalakedev`.

**Via Portal:**
1. Portal → **Storage accounts** → `evdatalakedev` → **Access Control (IAM)**
2. **+ Add** → **Add role assignment**
3. Role: `Storage Blob Data Contributor` → **Next**
4. Members: **Managed identity** → **+ Select members**
   - Managed identity type: `Access Connector for Azure Databricks`
   - Select: `ac-ev-databricks`
5. **Review + assign**

**Via CLI:**
```bash
STORAGE_ID=$(az storage account show \
  --name evdatalakedev \
  --resource-group rg-ev-intelligence-dev \
  --query id -o tsv)

AC_PRINCIPAL=$(az resource show \
  --resource-group rg-ev-intelligence-dev \
  --resource-type "Microsoft.Databricks/accessConnectors" \
  --name ac-ev-databricks \
  --query "identity.principalId" -o tsv)

az role assignment create \
  --assignee-object-id $AC_PRINCIPAL \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ID
```

Wait 2 minutes after assignment before proceeding.

---

## Part 2 — Unity Catalog Setup in Databricks

### 2.1 Create Storage Credential

The Storage Credential tells Unity Catalog how to authenticate to storage — it holds the Access Connector reference.

1. Open Databricks workspace → left sidebar → **Catalog** (grid icon)
2. Left panel → **External Data** → **Credentials** tab
3. **Create credential** (top right)

Fill in:

| Field | Value |
|---|---|
| Radio button | `Storage Credential` (left option) |
| Credential Type | `Azure Managed Identity` |
| Credential name | `mi-ev-credential` |
| Access connector ID | paste the Resource ID from Part 1.2 |
| User assigned managed identity ID | leave blank |
| Comment | `MI credential for evdatalakedev` |

Click **Create** → `mi-ev-credential` appears in the list.

> If you see `You do not have permission to create credentials` → you need **Metastore Admin** or **Account Admin** role in Databricks. Ask your workspace admin, or assign yourself via `https://accounts.azuredatabricks.net` → **Account** → **Users** → your user → **Roles**.

### 2.2 Create External Location

The External Location maps an `abfss://` path to the Storage Credential.

1. **External Data** → **External Locations** tab → **+ Create location**

Fill in:

| Field | Value |
|---|---|
| External location name | `ext-loc-ev-bronze` |
| Storage type | `Azure Data Lake Storage` (default) |
| URL | `abfss://bronze@evdatalakedev.dfs.core.windows.net/` |
| Storage credential | `mi-ev-credential` |

Click **Create** → **Test connection**

Expected result:
```
✅ Read    ✅ List    ✅ Write    ✅ Delete    ✅ Hierarchical Namespace Enabled
❌ File Events Resource Provision   ← optional warning, not a blocker
❌ File Events Resource Teardown    ← optional warning, not a blocker
```

If Read/List/Write fail with `Authorization failed` → the Access Connector MI doesn't have `Storage Blob Data Contributor` yet — re-check Part 1.3, wait 2 more minutes.

> **What Unity Catalog provides:** Once this External Location exists, any notebook that reads/writes to `abfss://bronze@evdatalakedev.dfs.core.windows.net/` using a path covered by this location will have credentials injected automatically. No `spark.conf.set`, no account key, no mount.

### 2.3 Create Catalog and Schema

A **catalog** is the top-level namespace. A **schema** holds the control tables.

```sql
-- Run in a Databricks notebook (SQL cell)
CREATE CATALOG IF NOT EXISTS ev_catalog;
USE CATALOG ev_catalog;

CREATE SCHEMA IF NOT EXISTS ctl
COMMENT 'Control tables — pipeline state, watermarks, config';
```

Or via UI:
1. **Catalog** → **+ Create catalog** → name: `ev_catalog` → **Create**
2. Click `ev_catalog` → **+ Create schema** → name: `ctl` → **Create**

---

## Part 3 — Create the Control Table

The control table is a Delta table in `bronze/ctl/` that drives the entire pipeline. ADF reads it at the start of every run.

### 3.1 Control Table Schema

```
ev_catalog.ctl.pipeline_control
┌──────────────────┬───────────────┬────────────────────────────────────────────────────────┐
│ Column           │ Type          │ Purpose                                                │
├──────────────────┼───────────────┼────────────────────────────────────────────────────────┤
│ source_name      │ STRING        │ Unique key — e.g. "payments", "sessions"               │
│ endpoint         │ STRING        │ API relative path — "/api/db/payments/"                │
│ sink_base_path   │ STRING        │ Bronze directory — "api/payments"                      │
│ file_prefix      │ STRING        │ File name stem — "payments"                            │
│ page_size        │ INT           │ Records per API call — 1000                            │
│ load_type        │ STRING        │ "full" or "incremental"                                │
│ last_watermark   │ STRING        │ ISO 8601 UTC — used as updated_after in next run       │
│ last_run_ts      │ TIMESTAMP     │ When this row was last updated                         │
│ is_active        │ BOOLEAN       │ false = skip this source                               │
└──────────────────┴───────────────┴────────────────────────────────────────────────────────┘
```

### 3.2 Create the Table

Run in a Databricks SQL cell or notebook:

```sql
CREATE TABLE IF NOT EXISTS ev_catalog.ctl.pipeline_control (
    source_name    STRING       NOT NULL,
    endpoint       STRING       NOT NULL,
    sink_base_path STRING       NOT NULL,
    file_prefix    STRING       NOT NULL,
    page_size      INT          NOT NULL,
    load_type      STRING       NOT NULL DEFAULT 'full',
    last_watermark STRING       NOT NULL DEFAULT '1900-01-01T00:00:00Z',
    last_run_ts    TIMESTAMP,
    is_active      BOOLEAN      NOT NULL DEFAULT true,
    CONSTRAINT pk_source_name PRIMARY KEY (source_name)
)
USING DELTA
LOCATION 'abfss://bronze@evdatalakedev.dfs.core.windows.net/ctl/pipeline_control'
COMMENT 'ADF ingestion control table — drives endpoint, path, watermark per source';
```

> `LOCATION` makes this an **External Delta table** — the files live inside the External Location at `bronze/ctl/pipeline_control/`. Unity Catalog manages the metadata; the data lives in your own ADLS.

### 3.3 Seed the Payments Row

```sql
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('payments', '/api/db/payments/', 'api/payments', 'payments', 1000, 'full', '1900-01-01T00:00:00Z', true);
```

Verify:
```sql
SELECT * FROM ev_catalog.ctl.pipeline_control;
```

Expected:
```
source_name | endpoint           | sink_base_path | file_prefix | page_size | load_type | last_watermark          | last_run_ts | is_active
payments    | /api/db/payments/  | api/payments   | payments    | 1000      | full      | 1900-01-01T00:00:00Z    | null        | true
```

### 3.4 Create a Databricks Notebook to Read the Control Table

ADF will call this notebook as an activity to retrieve control values. Create `nb_read_control_table` in Databricks:

```python
# nb_read_control_table
# Parameters passed by ADF via Databricks job parameters:
#   source_name  (string) — which row to read

dbutils.widgets.text("source_name", "payments")
source_name = dbutils.widgets.get("source_name")

row = spark.sql(f"""
    SELECT endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark
    FROM ev_catalog.ctl.pipeline_control
    WHERE source_name = '{source_name}'
      AND is_active = true
""").collect()

if not row:
    raise Exception(f"No active control row for source_name='{source_name}'")

r = row[0]

# Return values to ADF via notebook exit value (JSON string)
import json
result = {
    "endpoint":       r["endpoint"],
    "sink_base_path": r["sink_base_path"],
    "file_prefix":    r["file_prefix"],
    "page_size":      int(r["page_size"]),
    "load_type":      r["load_type"],
    "last_watermark": r["last_watermark"]
}

dbutils.notebook.exit(json.dumps(result))
```

> ADF's Databricks notebook activity returns `dbutils.notebook.exit(value)` as `activity('act_read_ctl').output.runOutput`. The JSON string is then parsed in ADF expressions.

### 3.5 Create a Notebook to Update the Watermark

ADF calls this after all pages are successfully loaded.

```python
# nb_update_watermark
# Parameters passed by ADF:
#   source_name    (string)
#   new_watermark  (string) — ISO 8601 UTC timestamp

dbutils.widgets.text("source_name", "payments")
dbutils.widgets.text("new_watermark", "1900-01-01T00:00:00Z")

source_name   = dbutils.widgets.get("source_name")
new_watermark = dbutils.widgets.get("new_watermark")

spark.sql(f"""
    UPDATE ev_catalog.ctl.pipeline_control
    SET
        last_watermark = '{new_watermark}',
        load_type      = 'incremental',
        last_run_ts    = current_timestamp()
    WHERE source_name = '{source_name}'
""")

print(f"Watermark updated: {source_name} → {new_watermark}")
dbutils.notebook.exit("OK")
```

---

## Part 4 — ADF: Linked Service for Databricks

ADF needs a new linked service to call Databricks notebooks.

### 4.1 Create Linked Service `ls_databricks`

**Via ADF Studio → Manage → Linked services → + New:**
1. Search `Azure Databricks` → select **Azure Databricks** (under **Compute** category — NOT "Azure Databricks Delta Lake") → **Continue**

2. Fill:

   | Field | Value |
   |---|---|
   | Name | `ls_databricks` |
   | Azure subscription | yours |
   | Databricks workspace | `dbw-ev-intelligence-dev` |
   | Select cluster | **New job cluster** |
   | Job cluster version | `15.4 LTS (includes Apache Spark 3.5.0, Scala 2.12)` |
   | Node type | `Standard_DS3_v2` |
   | Authentication type | `System Assigned Managed Identity` |

3. **Test connection** → green → **Create**

> **New job cluster** means ADF spins up a fresh cluster per pipeline run and terminates it when done. No running cluster to forget to stop — cost only during execution (~5 minutes for a full payments load).

### 4.2 Grant ADF MI Access to Databricks Workspace

ADF's Managed Identity needs permission to trigger Databricks jobs.

**Via Portal:**
1. Portal → **Azure Databricks** → `dbw-ev-intelligence-dev` → **Access Control (IAM)**
2. **+ Add** → **Add role assignment**
3. Role: `Contributor` → **Next**
4. Members: **Managed identity** → `adf-ev-intelligence-dev`
5. **Review + assign**

---

## Part 5 — ADF: Rebuild the Pipeline

### 5.1 New Pipeline Variables and Parameters

Create a new pipeline: `pl_bronze_incremental_load`

**Parameters** (caller-supplied):

| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `p_source_name` | String | `payments` | Which source to load — reads the right control row |
| `p_load_type_override` | String | `` (empty) | Force `full` for backfill; empty = use control table value |

**Variables** (pipeline-managed):

| Variable | Type | Purpose |
|---|---|---|
| `v_token` | String | VoltGrid API auth token |
| `v_endpoint` | String | API endpoint from control table |
| `v_sink_base_path` | String | Bronze base path from control table |
| `v_file_prefix` | String | File name prefix from control table |
| `v_page_size` | String | Page size from control table |
| `v_load_type` | String | `full` or `incremental` |
| `v_last_watermark` | String | `updated_after` value for this run |
| `v_ingestion_date` | String | `YYYY-MM-DD` for sink partition |
| `v_current_page` | Integer | Loop counter |
| `v_total_pages` | Integer | Max pages from API page 1 response |
| `v_run_ts` | String | ISO 8601 UTC — written back as new watermark |

### 5.2 Pipeline Activity Flow

```
act_read_ctl (Databricks notebook)
    ↓
act_set_vars_from_ctl (Set Variable ×4 — endpoint, sink_base_path, file_prefix, page_size)
    ↓
act_set_watermark (Set Variable — v_last_watermark)
    ↓
act_set_ingestion_date (Set Variable — v_ingestion_date)
    ↓
act_set_run_ts (Set Variable — v_run_ts)
    ↓
act_get_username ─┐
act_get_password  ├─→ act_api_login → act_set_token
                  ↓
act_fetch_page1 (Copy Activity — page 1 only, capture total_pages from response)
    ↓
act_set_total_pages (Set Variable — v_total_pages from page 1 output)
    ↓
act_set_current_page_2 (Set Variable — v_current_page = 2, page 1 already done)
    ↓
Until: @greaterOrEquals(variables('v_current_page'), variables('v_total_pages'))
    │
    ├─ act_copy_page (Copy Activity — page N)
    └─ act_increment_page (Set Variable — v_current_page + 1)
    ↓
act_update_watermark (Databricks notebook — writes v_run_ts back to control table)
    ↓ (on failure from any copy)
act_alert_failure (Web Activity — Teams/email webhook)
```

### 5.3 Build Step by Step

#### Step 1 — Read Control Table

Drag **Databricks Notebook** activity → rename `act_read_ctl`

**Azure Databricks** tab:
- Linked service: `ls_databricks`
- Notebook path: `/Workspace/ev-project/nb_read_control_table`

**Settings** tab → **Base parameters**:
- `source_name` → **Add dynamic content**: `@pipeline().parameters.p_source_name`

#### Step 2 — Set Variables from Control Table Output

Add **4 Set Variable** activities after `act_read_ctl` (connect all 4 in parallel from `act_read_ctl`):

> The notebook returns a JSON string via `dbutils.notebook.exit(json.dumps(result))`. Access it with `json()` function in ADF expressions to parse the JSON string, then dot-notation to access fields.

| Activity | Variable | Value expression |
|---|---|---|
| `act_set_endpoint` | `v_endpoint` | `@json(activity('act_read_ctl').output.runOutput).endpoint` |
| `act_set_sink_path` | `v_sink_base_path` | `@json(activity('act_read_ctl').output.runOutput).sink_base_path` |
| `act_set_file_prefix` | `v_file_prefix` | `@json(activity('act_read_ctl').output.runOutput).file_prefix` |
| `act_set_page_size` | `v_page_size` | `@string(json(activity('act_read_ctl').output.runOutput).page_size)` |

#### Step 3 — Set Watermark, Date, and Run Timestamp

Add 3 more **Set Variable** activities (after all 4 above complete):

**act_set_watermark:**
```
Variable: v_last_watermark
Value:
  @if(
    and(
      not(empty(pipeline().parameters.p_load_type_override)),
      equals(pipeline().parameters.p_load_type_override, 'full')
    ),
    '1900-01-01T00:00:00Z',
    json(activity('act_read_ctl').output.runOutput).last_watermark
  )
```

> Logic: if caller forced `full` load → use epoch watermark (fetch everything). Otherwise → use the last saved watermark (incremental).

**act_set_ingestion_date:**
```
Variable: v_ingestion_date
Value: @formatDateTime(utcnow(), 'yyyy-MM-dd')
```

**act_set_run_ts:**
```
Variable: v_run_ts
Value: @utcnow()
```

> `v_run_ts` is the watermark value written back to the control table after success. It represents "everything up to this moment has been loaded."

#### Step 4 — Authenticate to VoltGrid API

Add **act_get_username** and **act_get_password** (parallel, same as Day 2) then **act_api_login** and **act_set_token** — identical to Day 2 pipeline. See Day 2 notes Part 7.3 for exact settings.

#### Step 5 — Fetch Page 1 (Capture total_pages)

This Copy Activity fetches page 1 AND captures the `total_pages` value from the response pagination block.

**Source dataset:** `ds_voltgrid_payments_src` with parameters:
- `p_page`: `1`
- `p_page_size`: `@variables('v_page_size')`
- The dynamic URL resolves to: `/api/db/payments/?page=1&page_size=1000&updated_after=1900-01-01T00:00:00Z`

However — ADF's Copy Activity doesn't expose `total_pages` from the source. Use a **Web Activity** instead for page 1 to capture the pagination block:

**act_fetch_page1_meta** (Web Activity):
- URL → **Add dynamic content**:
  ```
  @concat(
    'https://ev-project-navy-mu.vercel.app',
    variables('v_endpoint'),
    '?page=1&page_size=',
    variables('v_page_size'),
    '&updated_after=',
    encodeUriComponent(variables('v_last_watermark'))
  )
  ```
- Method: `GET`
- Headers: `Authorization` = `@concat('Token ', variables('v_token'))`

This returns the full API response including `pagination.total_pages`. Also write this page's `data` to Bronze using a Copy Activity in parallel or use this output directly.

**Simplest approach — separate page 1 Web Activity + Copy:**
1. Web Activity `act_fetch_page1_meta` — GET page 1, capture `total_pages`
2. Set Variable `act_set_total_pages`:
   ```
   Variable: v_total_pages
   Value: @int(activity('act_fetch_page1_meta').output.pagination.total_pages)
   ```
3. Copy Activity `act_copy_page1` — write page 1 data to Bronze sink

**Sink path for page 1:**
- Container: `bronze`
- Folder: `@concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'))`
- File: `@concat(variables('v_file_prefix'), '_page_', padLeft('1', 3, '0'), '.json')`

> File name: `payments_page_001.json` — zero-padded 3-digit page number gives lexicographic sort up to 999 pages. For >999 pages use 4-digit padding.

#### Step 6 — Set Current Page to 2

```
Variable: v_current_page
Value: 2
```

Page 1 is already written. The Until loop starts at page 2.

#### Step 7 — Until Loop (Pages 2 to total_pages)

Drag **Until** activity onto canvas. Connect `act_set_current_page_2` → Until.

**Until** settings:
- Expression → **Add dynamic content**:
  ```
  @greaterOrEquals(variables('v_current_page'), add(variables('v_total_pages'), 1))
  ```
  > Loop runs while `v_current_page <= v_total_pages`. Exits when page exceeds total.

Inside the Until loop, add 2 activities:

**act_copy_page** (Copy Activity):
- Source dataset: `ds_voltgrid_payments_src`
  - `p_page`: `@variables('v_current_page')`
  - `p_page_size`: `@variables('v_page_size')`
  - Additional headers: `Authorization` = `@concat('Token ', variables('v_token'))`
  - The dataset URL resolves to: `/api/db/payments/?page=N&page_size=1000&updated_after=<watermark>`
- Sink dataset: `ds_bronze_payments_sink` (parameterised)
  - Folder: `@concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'))`
  - File: `@concat(variables('v_file_prefix'), '_page_', padLeft(string(variables('v_current_page')), 3, '0'), '.json')`

**act_increment_page** (Set Variable) — connect after `act_copy_page`:
```
Variable: v_current_page
Value: @add(variables('v_current_page'), 1)
```

> **Important:** Inside an Until loop, activities run sequentially — `act_copy_page` completes before `act_increment_page` runs. The loop re-evaluates the Until expression after both activities complete.

#### Step 8 — Update Watermark

After the Until loop completes (On Success), add Databricks notebook activity:

**act_update_watermark** (Databricks Notebook):
- Linked service: `ls_databricks`
- Notebook path: `/Workspace/ev-project/nb_update_watermark`
- Base parameters:
  - `source_name`: `@pipeline().parameters.p_source_name`
  - `new_watermark`: `@variables('v_run_ts')`

#### Step 9 — Failure Alert

Connect **On Failure** from both `act_copy_page` and `act_copy_page1` to a Web Activity `act_alert_failure`. This triggers if any page fails.

---

## Part 6 — Sink Path Convention and File Naming

### 6.1 Resolved Paths

For a payments run on 2026-10-14 with 321 total pages:

```
bronze/
└── api/
    └── payments/
        └── ingestion_date=2026-10-14/
            ├── payments_page_001.json    ← page 1 (1,000 records)
            ├── payments_page_002.json    ← page 2 (1,000 records)
            ├── payments_page_003.json    ← page 3 (1,000 records)
            │   ...
            └── payments_page_321.json   ← page 321 (~217 records — last page)
```

Control table for the next session path:
```
bronze/
└── ctl/
    └── pipeline_control/
        └── _delta_log/
        └── part-00000-*.parquet
```

### 6.2 Why This Naming Convention Is Reliable

| Property | How achieved |
|---|---|
| **Lexicographic sort = correct order** | Zero-padded page number (`_page_001`, `_page_002`) |
| **Idempotent re-runs** | Same `ingestion_date` + same page → same file name → overwrites are safe |
| **Source-type isolation** | `file_prefix` comes from control table (`payments_page_001.json` vs `sessions_page_001.json`) |
| **No hardcoded paths in ADF** | Sink base path from control table — add a new source by inserting a row, not changing pipeline |
| **Date partitioning** | `ingestion_date=YYYY-MM-DD` → Databricks auto-discovers partitions when reading |
| **Long-lasting** | Works for 1 to 999 pages without naming conflicts |

### 6.3 How ADF Expressions Resolve

| Expression | Example value |
|---|---|
| `variables('v_sink_base_path')` | `api/payments` |
| `variables('v_ingestion_date')` | `2026-10-14` |
| `variables('v_file_prefix')` | `payments` |
| `variables('v_current_page')` | `42` |
| Full folder: `@concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'))` | `api/payments/ingestion_date=2026-10-14` |
| Full file: `@concat(variables('v_file_prefix'), '_page_', padLeft(string(variables('v_current_page')), 3, '0'), '.json')` | `payments_page_042.json` |

---

## Part 7 — Updated Source Dataset

The Day 2 source dataset needs a new parameter for `updated_after`:

**Modify `ds_voltgrid_payments_src`:**
1. **Author** → **Datasets** → `ds_voltgrid_payments_src`
2. **Parameters** tab → add:
   - `p_updated_after` — String — default: `1900-01-01T00:00:00Z`
3. **Connection** tab → Relative URL → **Edit dynamic content**:
   ```
   @concat(
     dataset().p_endpoint,
     '?page=',
     string(dataset().p_page),
     '&page_size=',
     string(dataset().p_page_size),
     '&updated_after=',
     encodeUriComponent(dataset().p_updated_after)
   )
   ```

> `p_endpoint` is also a new dataset parameter now — the endpoint comes from the control table, so the dataset should accept it as a parameter too. This makes `ds_voltgrid_payments_src` reusable for all 6 API endpoints.

**Full updated dataset parameters:**

| Parameter | Type | Default |
|---|---|---|
| `p_endpoint` | String | `/api/db/payments/` |
| `p_page` | Int | `1` |
| `p_page_size` | Int | `1000` |
| `p_updated_after` | String | `1900-01-01T00:00:00Z` |

---

## Part 8 — Trigger Setup

Use a **Tumbling Window Trigger** — it tracks every day's window and guarantees no day is silently missed.

1. ADF Studio → **Manage** → **Triggers** → **+ New**
2. Type: `Tumbling window`
3. Name: `trg_payments_incremental`
4. Start time: `2026-10-14T00:00:00Z` *(today — first day)*
5. Recurrence: `1 Day`
6. Max concurrency: `1`
7. Retry: `2` times, `30` minutes interval
8. Pipeline parameters:
   - `p_source_name`: `payments`
   - `p_load_type_override`: `` *(empty — use control table)*
9. **OK** → **Publish all** → **Activate**

**For backfill (loading historical data):**
- Set start date to how far back you need (e.g. `2026-01-01T00:00:00Z`)
- ADF queues all windows from that date to today automatically
- Each window uses the corresponding `ingestion_date` partition

---

## Part 9 — Adding a New Source (Zero Pipeline Changes)

Once the pipeline is built, adding any new API endpoint is a single SQL statement:

```sql
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('sessions', '/api/db/sessions/', 'api/sessions', 'sessions', 1000, 'full', '1900-01-01T00:00:00Z', true);
```

Then trigger the pipeline with `p_source_name = 'sessions'` — no ADF changes required.

The sink path resolves automatically:
```
bronze/api/sessions/ingestion_date=2026-10-14/sessions_page_001.json
```

---

## Verify: End-to-End Checklist

**Databricks / Unity Catalog:**
- [ ] Access Connector `ac-ev-databricks` created
- [ ] Access Connector MI has `Storage Blob Data Contributor` on `evdatalakedev`
- [ ] Storage Credential `mi-ev-credential` created in Databricks
- [ ] External Location `ext-loc-ev-bronze` created — Read/Write test green
- [ ] Catalog `ev_catalog` and schema `ctl` created
- [ ] Table `ev_catalog.ctl.pipeline_control` created at `bronze/ctl/pipeline_control`
- [ ] Payments row inserted — `last_watermark = 1900-01-01T00:00:00Z`, `load_type = full`
- [ ] Notebook `nb_read_control_table` created and tested manually
- [ ] Notebook `nb_update_watermark` created and tested manually

**ADF:**
- [ ] Linked service `ls_databricks` created — New job cluster, test connection green
- [ ] ADF MI has `Contributor` on Databricks workspace
- [ ] Dataset `ds_voltgrid_payments_src` updated — 4 parameters including `p_endpoint` and `p_updated_after`
- [ ] Pipeline `pl_bronze_incremental_load` created with all variables and parameters
- [ ] `act_read_ctl` returns correct JSON from control table
- [ ] `act_fetch_page1_meta` returns `total_pages` from API
- [ ] Until loop runs pages 2 to N — each page writes `payments_page_NNN.json`
- [ ] `act_update_watermark` updates `last_watermark` in control table
- [ ] Trigger `trg_payments_incremental` created and active

**Post-run verification:**
- [ ] `az storage blob list --account-name evdatalakedev --container-name bronze --prefix "api/payments/" --auth-mode login` shows multiple files
- [ ] Control table shows `load_type = incremental` and `last_watermark` updated
- [ ] Second run fetches only records changed since first run

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `act_read_ctl` fails — notebook not found | Notebook path wrong in linked service | Check exact path in Databricks Workspace — case-sensitive |
| `act_read_ctl` fails — `You do not have permission` | ADF MI missing Contributor on Databricks workspace | Portal → Databricks workspace → IAM → assign Contributor |
| `json()` returns null in Set Variable | `dbutils.notebook.exit()` not called — notebook failed silently | Check job run logs in Databricks → Jobs (triggered by ADF) |
| `total_pages` is 0 | `updated_after` filter returned no records — watermark too recent | Force full load: set `p_load_type_override = full` in Trigger Now |
| Until loop runs forever | `v_current_page` increment not wired correctly | Verify `act_increment_page` is inside the Until body, not outside |
| File overwrites from previous day | `ingestion_date` using `utcnow()` in a trigger run | Replace `utcnow()` with `trigger().outputs.windowStartTime` once Tumbling Window is attached |
| `encodeUriComponent` not resolving | ADF function name case mismatch | Use lowercase exactly: `encodeUriComponent` |
| `Authorization failed` on External Location test | Access Connector MI role not propagated | Wait 2 more minutes after RBAC assignment |
| `Credential not found` in Databricks | Storage credential was deleted or renamed | Re-create `mi-ev-credential` in Databricks → Catalog → External Data → Credentials |

---

## Quick Reference

```
Control table : ev_catalog.ctl.pipeline_control
                Location: abfss://bronze@evdatalakedev.dfs.core.windows.net/ctl/pipeline_control

Sink pattern  : bronze/<sink_base_path>/ingestion_date=YYYY-MM-DD/<file_prefix>_page_NNN.json
Example       : bronze/api/payments/ingestion_date=2026-10-14/payments_page_001.json

Access chain (Databricks):
  ac-ev-databricks (MI)  →  Storage Blob Data Contributor  →  evdatalakedev
  mi-ev-credential       →  references ac-ev-databricks
  ext-loc-ev-bronze      →  abfss://bronze@evdatalakedev.dfs.core.windows.net/

Access chain (ADF):
  adf-ev-intelligence-dev MI  →  Key Vault Secrets User        →  kv-ev-intelligence-dev
  adf-ev-intelligence-dev MI  →  Storage Blob Data Contributor →  evdatalakedev
  adf-ev-intelligence-dev MI  →  Contributor                   →  dbw-ev-intelligence-dev

API pagination:
  page_size = 1000
  total_pages from: response.pagination.total_pages  (e.g. 321 for payments)
  updated_after = last_watermark from control table

Watermark update: written after all pages succeed — v_run_ts (utcnow() at pipeline start)
```
