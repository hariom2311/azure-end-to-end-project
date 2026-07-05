# 03 — Build v3 Payments Pipeline: Step by Step in ADF Studio
**Day 3 | pl_bronze_api_payments_v3**

This guide walks you through creating the v3 pipeline from scratch in ADF Studio UI — no JSON pasting required. v3 upgrades v2 by reading the watermark automatically from a `pipeline_audit` Delta table (no manual input) and writing an audit row via a Databricks notebook after every run.

> **Prerequisite:** Day 2 linked services must exist — `ls_keyvault`, `ls_voltgrid_api`, `ls_adls_bronze`.
> **Also required:** Upload `notebooks/nb_write_audit.py` to Databricks and create `ls_databricks_cluster` in Part A below.
> **If you prefer JSON paste:** use the `.json` files in this folder + `PIPELINE_NOTES_V3.md`.

> **Note on SQL Warehouses:** ADF's Azure Databricks Delta Lake linked service supports **Spark clusters only** — it does not list SQL Warehouses in the dropdown. Use your existing `dev-cluster` (All-Purpose cluster) for both the Lookup Activity and the Notebook Activity.

---

## What You Will Build

| Artifact | Name | Purpose |
|---|---|---|
| Notebook | `nb_write_audit` | Databricks notebook — writes audit row to `pipeline_audit` Delta table |
| Linked Service | `ls_databricks_cluster` | Connects ADF to `dev-cluster` — used by Lookup + Notebook activities |
| Dataset | `ds_voltgrid_payments_src_v3` | Source — VoltGrid API with page + watermark parameters |
| Dataset | `ds_bronze_payments_sink_v3` | Sink — ADLS Bronze, partitioned by date and page |
| Dataset | `ds_pipeline_audit_src` | Lookup source — reads `pipeline_audit` Delta table |
| Pipeline | `pl_bronze_api_payments_v3` | Full + incremental, auto watermark, audit trail |

**Pipeline parameter:**

| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `p_load_type` | String | `incremental` | `full` or `incremental` — no manual watermark needed |

**Pipeline variables:**

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `v_token` | String | — | API bearer token |
| `v_watermark` | String | `1900-01-01T00:00:00Z` | Resolved from audit table automatically |
| `v_ingestion_date` | String | — | Today's date — Bronze partition folder |
| `v_current_page` | Integer | `1` | Current loop page |
| `v_temp_page` | Integer | `1` | Intermediate for page increment |
| `v_total_pages` | Integer | `1` | Total pages from API |
| `v_status` | String | `started` | `succeeded` or `failed` — written to audit |

---

## Part A — Upload Notebook + Create Linked Service

### Step A1 — Upload `nb_write_audit` to Databricks

The audit notebook runs on `dev-cluster` after every pipeline run and writes one row to the `pipeline_audit` Delta table.

1. Open your Databricks workspace
2. Left sidebar → **Workspace** → **Shared**
3. Click the **⋮** menu → **Create** → **Folder** → name it `adf_pipelines`
4. Inside `adf_pipelines` → click **⋮** → **Import**
5. Select **File** → upload `notebooks/nb_write_audit.py` from this directory
6. Confirm the notebook appears at: `/Shared/adf_pipelines/nb_write_audit`

> This path is hardcoded in `pl_bronze_api_payments_v3.json`. If you use a different path, update the `notebookPath` field in the pipeline JSON.

---

### Step A2 — Create `ls_databricks_cluster` Linked Service

This linked service connects ADF to your `dev-cluster` (All-Purpose Spark cluster). It is used by:
- The **Lookup Activity** (`act_get_watermark`) — reads the `pipeline_audit` Delta table
- The **Notebook Activity** (`act_write_audit`) — runs `nb_write_audit`

> **Why not SQL Warehouse?** ADF's Azure Databricks linked service only works with Spark clusters. SQL Warehouses are for BI tools (Power BI, JDBC). All ADF pipeline activities require a Spark cluster.

1. ADF Studio → **Manage** → **Linked services** → **+ New**
2. Search `Azure Databricks` → select **Azure Databricks** *(not "Delta Lake")* → **Continue**
3. Fill in:
   - **Name:** `ls_databricks_cluster`
   - **Account selection method:** From Azure subscription
   - **Azure subscription:** select yours
   - **Databricks workspace:** `dbw-ev-intelligence-dev`
   - **Select cluster:** `Existing interactive cluster`
   - **Existing cluster ID:** select `dev-cluster` from the dropdown
   - **Authentication:** Access token → click **Azure Key Vault** tab → **AKV linked service:** `ls_keyvault` → **Secret name:** `databricks-pat-token`
4. Click **Test connection** → **Connection successful**
5. Click **Create** → **Publish all**

> If `dev-cluster` does not appear in the dropdown, make sure the cluster is running in Databricks → Compute. Start it, wait 1 minute, then refresh the ADF linked service form.

---

## Part B — Create Dataset 1: `ds_voltgrid_payments_src_v3`

Same as `ds_voltgrid_payments_src_v2` — three parameters, dynamic URL. Create a separate v3 version to keep it independent.

### Steps

1. **Author** → **Datasets** → **+** → **New dataset**
2. Search `REST` → select **REST** → **Continue**
3. Fill in:
   - **Name:** `ds_voltgrid_payments_src_v3`
   - **Linked service:** `ls_voltgrid_api`
   - **Relative URL:** leave blank
4. Click **OK**

### Add Parameters

5. **Parameters** tab → **+ New** → add:

   | Name | Type | Default value |
   |---|---|---|
   | `p_page` | Int | `1` |
   | `p_page_size` | Int | `100` |
   | `p_updated_after` | String | `1900-01-01T00:00:00Z` |

### Set the Dynamic URL

6. **Connection** tab → **Relative URL** → **Add dynamic content**:
   ```
   @concat('/api/db/payments/?page=', string(dataset().p_page), '&page_size=', string(dataset().p_page_size), '&updated_after=', dataset().p_updated_after)
   ```
   Click **OK**

7. **Publish all** → **Publish**

---

## Part C — Create Dataset 2: `ds_bronze_payments_sink_v3`

Same structure as v2 sink — partitioned Bronze path.

### Steps

1. **Datasets** → **+** → **New dataset**
2. Search `Azure Data Lake Storage Gen2` → **Continue**
3. Select **JSON** → **Continue**
4. Fill in:
   - **Name:** `ds_bronze_payments_sink_v3`
   - **Linked service:** `ls_adls_bronze`
   - **File system:** `bronze`
   - Leave directory and file blank
5. Click **OK**

### Add Parameters

6. **Parameters** tab → **+ New** → add:

   | Name | Type |
   |---|---|
   | `p_ingestion_date` | String |
   | `p_page` | Int |

### Set Dynamic Path

7. **Connection** tab:
   - **Directory** → **Add dynamic content**:
     ```
     @concat('api/payments/raw/ingestion_date=', dataset().p_ingestion_date)
     ```
     Click **OK**
   - **File** → **Add dynamic content**:
     ```
     @concat('page_', string(dataset().p_page), '.json')
     ```
     Click **OK**

8. **Publish all** → **Publish**

---

## Part D — Create Dataset 3: `ds_pipeline_audit_src`

This dataset points to the `pipeline_audit` Delta table in Unity Catalog. It is used only by the **Lookup Activity** (`act_get_watermark`) to read the watermark. The audit write is now done by the notebook — no sink dataset needed.

### Steps

1. **Datasets** → **+** → **New dataset**
2. Search `Databricks` → select **Azure Databricks Delta Lake** → **Continue**
3. Fill in:
   - **Name:** `ds_pipeline_audit_src`
   - **Linked service:** `ls_databricks_cluster`
4. Click **OK**

### Set the Table

5. **Connection** tab:
   - **Catalog:** `dbw_ev_intelligence_dev`
   - **Database:** `default`
   - **Table:** `pipeline_audit`

6. **Publish all** → **Publish**

> The table will be created automatically by `nb_write_audit` on the first pipeline run. You do not need to create it in Databricks first.

---

## Part F — Create Pipeline: `pl_bronze_api_payments_v3`

### Step 1 — Create the Pipeline Shell

1. **Author** → **Pipelines** → **+** → **New pipeline**
2. **Name:** `pl_bronze_api_payments_v3`

### Step 2 — Add Parameter

3. Bottom panel → **Parameters** tab → **+ New**:

   | Name | Type | Default |
   |---|---|---|
   | `p_load_type` | String | `incremental` |

### Step 3 — Add Variables

4. **Variables** tab → **+ New** → add all seven:

   | Name | Type | Default |
   |---|---|---|
   | `v_token` | String | *(blank)* |
   | `v_watermark` | String | `1900-01-01T00:00:00Z` |
   | `v_ingestion_date` | String | *(blank)* |
   | `v_current_page` | Integer | `1` |
   | `v_temp_page` | Integer | `1` |
   | `v_total_pages` | Integer | `1` |
   | `v_status` | String | `started` |

---

### Step 4 — Activity 1: `act_get_username`

**Type:** Web Activity

1. Drag **Web** onto the canvas → **Name:** `act_get_username`
2. **Settings:**
   - **URL:** `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-username/?api-version=7.0`
   - **Method:** GET
   - **Authentication:** System Assigned Managed Identity
   - **Resource:** `https://vault.azure.net`

---

### Step 5 — Activity 2: `act_get_password`

**Type:** Web Activity

1. Drag **Web** → **Name:** `act_get_password`
2. Connect success arrow: `act_get_username` → `act_get_password`
3. **Settings:**
   - **URL:** `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-password/?api-version=7.0`
   - **Method:** GET
   - **Authentication:** System Assigned Managed Identity
   - **Resource:** `https://vault.azure.net`

---

### Step 6 — Activity 3: `act_api_login`

**Type:** Web Activity

1. Drag **Web** → **Name:** `act_api_login`
2. Connect: `act_get_password` → `act_api_login`
3. **Settings:**
   - **URL:** `https://ev-project-navy-mu.vercel.app/api/auth/login/`
   - **Method:** POST
   - **Headers:** `Content-Type` = `application/json`
   - **Body:** **Add dynamic content**:
     ```
     @concat('{"username":"', activity('act_get_username').output.value, '","password":"', activity('act_get_password').output.value, '"}')
     ```
     Click **OK**

---

### Step 7 — Activity 4: `act_set_token`

**Type:** Set Variable

1. Drag **Set variable** → **Name:** `act_set_token`
2. Connect: `act_api_login` → `act_set_token`
3. **Settings:**
   - **Variable:** `v_token`
   - **Value:** **Add dynamic content** → `@activity('act_api_login').output.token` → OK

---

### Step 8 — Activity 5: `act_set_ingestion_date`

**Type:** Set Variable

1. Drag **Set variable** → **Name:** `act_set_ingestion_date`
2. Connect: `act_set_token` → `act_set_ingestion_date`
3. **Settings:**
   - **Variable:** `v_ingestion_date`
   - **Value:** **Add dynamic content** → `@formatDateTime(utcNow(), 'yyyy-MM-dd')` → OK

---

### Step 9 — Activity 6: `act_get_watermark` ⭐ *New in v3*

**Type:** Lookup Activity — queries `pipeline_audit` Delta table via Databricks SQL Warehouse to get the watermark automatically. No manual input needed.

1. In the Activities panel → expand **General** → drag **Lookup** onto the canvas
2. **Name:** `act_get_watermark`
3. Connect: `act_set_ingestion_date` → `act_get_watermark`

#### Settings tab:

4. **Source dataset:** `ds_pipeline_audit_src`
5. **Use query:** select **Query** (not Table)
6. **Query:** click **Add dynamic content** and paste this expression:

   ```
   @if(
     equals(pipeline().parameters.p_load_type, 'full'),
     'SELECT ''1900-01-01T00:00:00Z'' AS last_watermark',
     concat(
       'SELECT COALESCE(MAX(watermark_value), ''1900-01-01T00:00:00Z'') AS last_watermark FROM dbw_ev_intelligence_dev.default.pipeline_audit WHERE pipeline_name = ''pl_bronze_api_payments_v3'' AND status = ''succeeded'''
     )
   )
   ```
   Click **OK**

7. **First row only:** toggle **ON**

> **What this does:**
> - Full load → returns the constant `1900-01-01T00:00:00Z` (no table read needed)
> - Incremental → reads `MAX(watermark_value)` from the last succeeded run
> - Output accessed as: `activity('act_get_watermark').output.firstRow.last_watermark`

---

### Step 10 — Activity 7: `act_set_watermark`

**Type:** Set Variable — stores the watermark returned by the Lookup.

1. Drag **Set variable** → **Name:** `act_set_watermark`
2. Connect: `act_get_watermark` → `act_set_watermark`
3. **Settings:**
   - **Variable:** `v_watermark`
   - **Value:** **Add dynamic content**:
     ```
     @activity('act_get_watermark').output.firstRow.last_watermark
     ```
     Click **OK**

---

### Step 11 — Activity 8: `act_get_total_pages`

**Type:** Web Activity — calls page 1 to read total pages before starting the loop.

1. Drag **Web** → **Name:** `act_get_total_pages`
2. Connect: `act_set_watermark` → `act_get_total_pages`
3. **Settings:**
   - **URL:** **Add dynamic content**:
     ```
     @concat('https://ev-project-navy-mu.vercel.app/api/db/payments/?page=1&page_size=100&updated_after=', variables('v_watermark'))
     ```
     Click **OK**
   - **Method:** GET
   - **Headers:** `Authorization` → **Add dynamic content** → `@concat('Token ', variables('v_token'))` → OK

---

### Step 12 — Activity 9: `act_set_total_pages`

**Type:** Set Variable

1. Drag **Set variable** → **Name:** `act_set_total_pages`
2. Connect: `act_get_total_pages` → `act_set_total_pages`
3. **Settings:**
   - **Variable:** `v_total_pages`
   - **Value:** **Add dynamic content**:
     ```
     @activity('act_get_total_pages').output.pagination.total_pages
     ```
     Click **OK**

---

### Step 13 — Activity 10: `act_paginate` (Until Loop)

1. Drag **Until** → **Name:** `act_paginate`
2. Connect: `act_set_total_pages` → `act_paginate`
3. **Settings:**
   - **Expression:** **Add dynamic content**:
     ```
     @greater(variables('v_current_page'), variables('v_total_pages'))
     ```
     Click **OK**
   - **Timeout:** `0.12:00:00`

#### Open loop canvas — click the pencil icon inside the Until box

---

### Step 14 — Inside Loop: `act_copy_payments_page`

**Type:** Copy Activity

1. Drag **Copy data** → **Name:** `act_copy_payments_page`

#### Source tab:
- **Source dataset:** `ds_voltgrid_payments_src_v3`
- **Dataset parameters:**
  - `p_page` → **Add dynamic content** → `@variables('v_current_page')` → OK
  - `p_page_size` → `100`
  - `p_updated_after` → **Add dynamic content** → `@variables('v_watermark')` → OK
- **Additional headers:** `Authorization` → **Add dynamic content** → `@concat('Token ', variables('v_token'))` → OK
- **Request method:** GET

#### Sink tab:
- **Sink dataset:** `ds_bronze_payments_sink_v3`
- **Dataset parameters:**
  - `p_ingestion_date` → **Add dynamic content** → `@variables('v_ingestion_date')` → OK
  - `p_page` → **Add dynamic content** → `@variables('v_current_page')` → OK

---

### Step 15 — Inside Loop: `act_set_temp_page`

1. Drag **Set variable** → **Name:** `act_set_temp_page`
2. Connect: `act_copy_payments_page` → `act_set_temp_page`
3. **Settings:**
   - **Variable:** `v_temp_page`
   - **Value:** **Add dynamic content** → `@add(variables('v_current_page'), 1)` → OK

---

### Step 16 — Inside Loop: `act_increment_page`

1. Drag **Set variable** → **Name:** `act_increment_page`
2. Connect: `act_set_temp_page` → `act_increment_page`
3. **Settings:**
   - **Variable:** `v_current_page`
   - **Value:** **Add dynamic content** → `@variables('v_temp_page')` → OK

#### Exit the loop — click the pipeline name in the breadcrumb to return to the main canvas

---

### Step 17 — Activity 11: `act_set_status_success` ⭐ *New in v3*

**Type:** Set Variable — marks the run as succeeded after the loop completes normally.

1. Drag **Set variable** → **Name:** `act_set_status_success`
2. Connect arrow from `act_paginate` → `act_set_status_success`
   - **Dependency condition:** **Success** (default)
3. **Settings:**
   - **Variable:** `v_status`
   - **Value:** type `succeeded` directly (no dynamic content needed)

---

### Step 18 — Activity 12: `act_set_status_failed` ⭐ *New in v3*

**Type:** Set Variable — marks the run as failed if the loop fails.

1. Drag **Set variable** → **Name:** `act_set_status_failed`
2. Connect arrow from `act_paginate` → `act_set_status_failed`
   - Click the arrow → change **Dependency condition** to **Failure**
3. **Settings:**
   - **Variable:** `v_status`
   - **Value:** type `failed` directly

---

### Step 19 — Activity 13: `act_write_audit` ⭐ *New in v3*

**Type:** Notebook Activity — runs `nb_write_audit` on `dev-cluster`. Writes one audit row to `pipeline_audit` Delta table. Always runs after both status activities — success path and failure path.

1. In the Activities panel → expand **Databricks** → drag **Notebook** onto the canvas
2. **Name:** `act_write_audit`
3. Connect **two** arrows into `act_write_audit`:
   - `act_set_status_success` → `act_write_audit` (condition: **Success**)
   - `act_set_status_failed` → `act_write_audit` (condition: **Success**)

   > Both use "Success" condition — meaning "this activity completed running", not "the overall pipeline succeeded". This ensures the audit row is always written regardless of whether the loop succeeded or failed.

#### Azure Databricks tab:

4. **Databricks linked service:** `ls_databricks_cluster`
5. **Notebook path:** `/Shared/adf_pipelines/nb_write_audit`
   *(click the folder icon to browse, or type the path directly)*

#### Settings tab:

6. **Base parameters** → click **+ New** for each:

   | Name | Value (click Add dynamic content for each) |
   |---|---|
   | `pipeline_name` | `pl_bronze_api_payments_v3` *(type directly)* |
   | `load_type` | `@pipeline().parameters.p_load_type` |
   | `watermark_value` | `@variables('v_watermark')` |
   | `ingestion_date` | `@variables('v_ingestion_date')` |
   | `total_pages` | `@string(variables('v_total_pages'))` |
   | `status` | `@variables('v_status')` |
   | `pipeline_run_id` | `@pipeline().RunId` |

> These parameters are read inside the notebook via `dbutils.widgets.get()` — each widget name must match exactly.

---

### Step 20 — Publish the Pipeline

1. **Publish all** → **Publish**
2. Wait for "Published successfully"

---

## Part G — Trigger and Verify

### First run — Full load (run once manually)

1. `pl_bronze_api_payments_v3` → **Add trigger** → **Trigger now**
2. Parameters:
   - `p_load_type`: `full`
3. Click **OK**
4. **Monitor** → **Pipeline runs** → all 13 activities should turn green

### Check the audit table after full load

In a Databricks notebook or SQL editor:
```sql
SELECT pipeline_name, load_type, watermark_value, ingestion_date, total_pages, status, run_timestamp
FROM dbw_ev_intelligence_dev.default.pipeline_audit
ORDER BY run_timestamp DESC
LIMIT 5;
```

You should see one row with `load_type = full` and `status = succeeded`.

### Advance the watermark after full load

The full load wrote `watermark_value = 1900-01-01T00:00:00Z`. For the next incremental run to fetch only new records, update the audit table once with the actual max timestamp from your Bronze data:

```sql
UPDATE dbw_ev_intelligence_dev.default.pipeline_audit
SET watermark_value = (
  SELECT MAX(updated_at)
  FROM delta.`abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/`
  LATERAL VIEW explode(data) t AS updated_at
  WHERE ingestion_date = '<your ingestion_date folder>'
)
WHERE pipeline_name = 'pl_bronze_api_payments_v3'
  AND status = 'succeeded'
  AND load_type = 'full';
```

> Day 8 (Orchestration) will automate this step — a separate pipeline reads `MAX(updated_at)` from Bronze and updates the audit record automatically.

### Daily scheduled run — Incremental

Set up a daily trigger so the pipeline runs automatically:

1. `pl_bronze_api_payments_v3` → **Add trigger** → **New/Edit**
2. **+ New**:
   - **Name:** `trg_payments_daily`
   - **Type:** Schedule
   - **Recurrence:** Every 1 Day at `01:00 UTC`
3. Click **Next**
4. **Parameters:**
   - `p_load_type`: `incremental`
5. Click **Finish** → **Publish all**

Each run automatically reads the watermark from the last succeeded row in `pipeline_audit` — no manual input ever again.

### Verify Bronze output

```python
# List all partitions
display(dbutils.fs.ls("abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/"))

# Read one page
df = spark.read.option("multiLine", "true").json(
    "abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/ingestion_date=2026-07-05/page_1.json"
)
display(df.limit(5))
```

---

## How the Watermark Advances Automatically

```
Run 1  (full)
  act_get_watermark  → SELECT '1900-01-01T00:00:00Z' AS last_watermark
  v_watermark        = 1900-01-01T00:00:00Z
  API fetches ALL records
  act_write_audit    → watermark_value = 1900-01-01T00:00:00Z, status = succeeded

  [Manual step once: UPDATE audit SET watermark_value = MAX(updated_at) from Bronze]

Run 2  (incremental)
  act_get_watermark  → SELECT COALESCE(MAX(watermark_value),...) → returns 2026-07-04T09:43:00Z
  v_watermark        = 2026-07-04T09:43:00Z
  API fetches only records updated after that timestamp
  act_write_audit    → watermark_value = 2026-07-04T09:43:00Z, status = succeeded

Run 3  (incremental — next day)
  act_get_watermark  → returns 2026-07-04T09:43:00Z (last succeeded watermark)
  API fetches only new records since Run 2
  ...and so on
```

---

## Audit Table Schema

**Table:** `dbw_ev_intelligence_dev.default.pipeline_audit`
Auto-created on first `act_write_audit` run.

| Column | Type | Description |
|---|---|---|
| `pipeline_name` | STRING | `pl_bronze_api_payments_v3` |
| `load_type` | STRING | `full` or `incremental` |
| `watermark_value` | STRING | `updated_after` value used this run |
| `ingestion_date` | STRING | Bronze partition date |
| `total_pages` | INT | Pages fetched this run |
| `status` | STRING | `succeeded` or `failed` |
| `pipeline_run_id` | STRING | ADF RunId — links to ADF Monitor |
| `run_timestamp` | TIMESTAMP | UTC time this row was written |

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `act_get_watermark` fails: `LinkedService not found` | `ls_databricks_cluster` not created | Part A Step A2 — create the linked service first |
| `act_get_watermark` fails: `Table not found` | `pipeline_audit` table does not exist yet | Run a full load first — `nb_write_audit` creates the table on first run |
| `act_get_watermark` returns no rows | Table exists but has no succeeded rows | Check audit table in Databricks — if all rows show `failed`, fix the pipeline and re-run |
| `act_write_audit` fails: notebook not found | Notebook not uploaded | Upload `nb_write_audit.py` to `/Shared/adf_pipelines/` in Databricks (Part A Step A1) |
| `act_write_audit` fails: cluster terminated | `dev-cluster` auto-terminated | Databricks → Compute → start `dev-cluster` before triggering |
| `act_get_username` 403 | ADF MI missing `Key Vault Secrets User` | Portal → Key Vault → IAM → assign role, wait 2 min |
| `act_api_login` 401 | Wrong credentials | Check `voltgrid-username` and `voltgrid-password` in Key Vault |
| Until loop runs only once | `v_total_pages` stayed at 1 | Monitor → `act_get_total_pages` output → confirm `pagination.total_pages` key |
| `act_copy_payments_page` 403 | ADF MI missing `Storage Blob Data Contributor` | Portal → Storage → IAM → assign role |
| Incremental fetches all records | Watermark not updated after full load | Run the UPDATE SQL above after the first full load |
| `act_set_status_failed` never runs | Wrong dependency condition | Click the arrow from `act_paginate` to this activity → ensure condition is **Failure** |
| `act_write_audit` not always running | Missing second dependency | Ensure both `act_set_status_success` and `act_set_status_failed` connect to `act_write_audit` with Success condition |
