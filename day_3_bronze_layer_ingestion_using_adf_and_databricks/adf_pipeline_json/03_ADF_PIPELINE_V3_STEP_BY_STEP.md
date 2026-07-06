# 03 — Build v3 Payments Pipeline: Step by Step in ADF Studio
**Day 3 | pl_bronze_api_payments_v3**

v3 upgrades v2 by:
1. Reading the watermark automatically from `pipeline_audit.csv` in Bronze — no manual input needed.
2. Writing an audit row to that same CSV after every run (success or failure).

**No new linked services.** Everything reuses `ls_adls_bronze` from Day 2.

> **Prerequisite:** Day 2 linked services must exist — `ls_keyvault`, `ls_voltgrid_api`, `ls_adls_bronze`.
> **If you prefer JSON paste:** use the `.json` files in this folder + `PIPELINE_NOTES_V3.md`.

---

## What You Will Build

| Artifact | Name | Purpose |
|---|---|---|
| CSV file | `bronze/audit/pipeline_audit.csv` | Audit trail + watermark store — lives in Bronze ADLS |
| Dataset | `ds_voltgrid_payments_src_v3` | Source — VoltGrid API (same as v2) |
| Dataset | `ds_bronze_payments_sink_v3` | Sink — ADLS Bronze, partitioned by date and page |
| Dataset | `ds_pipeline_audit_csv` | Read/write `pipeline_audit.csv` via `ls_adls_bronze` |
| Pipeline | `pl_bronze_api_payments_v3` | Full + incremental, auto watermark, CSV audit trail |

**Pipeline parameter:**

| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `p_load_type` | String | `incremental` | `full` or `incremental` — no manual watermark needed |

**Pipeline variables:**

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `v_token` | String | *(blank)* | API bearer token |
| `v_watermark` | String | `1900-01-01T00:00:00Z` | Resolved from audit CSV automatically |
| `v_ingestion_date` | String | *(blank)* | Today's date — Bronze partition folder |
| `v_current_page` | Integer | `1` | Current loop page |
| `v_temp_page` | Integer | `1` | Intermediate for page increment |
| `v_total_pages` | Integer | `1` | Total pages from API |
| `v_status` | String | `started` | `succeeded` or `failed` — written to audit |

---

## Part A — One-Time Setup: Create `pipeline_audit.csv` in Bronze

This is done **once before the first pipeline run**. The file must exist with a header row so the Lookup Activity can parse column names.

### Step A1 — Create the file via Azure Portal

1. Go to [portal.azure.com](https://portal.azure.com) → your storage account → **Containers** → `bronze`
2. Click **+ Add directory** → type `audit` → click **Save**
3. Click into the `audit` folder
4. On your local machine, create a file named `pipeline_audit.csv` with exactly this content (one line only):
   ```
   pipeline_name,load_type,watermark_value,ingestion_date,total_pages,status,pipeline_run_id,run_timestamp
   ```
5. In the Portal, click **Upload** → select your `pipeline_audit.csv` → **Upload**
6. Confirm the file appears at: `bronze/audit/pipeline_audit.csv`

> After the first full-load pipeline run, this file will have one data row appended below the header. You can open it in the Portal or Storage Explorer at any time to inspect or manually edit the watermark.

---

## Part B — Create Dataset 1: `ds_voltgrid_payments_src_v3`

Same as `ds_voltgrid_payments_src_v2` — three parameters, dynamic URL. Create a separate v3 copy.

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

### Set Dynamic URL

6. **Connection** tab → **Relative URL** → **Add dynamic content**:
   ```
   @concat('/api/db/payments/?page=', string(dataset().p_page), '&page_size=', string(dataset().p_page_size), '&updated_after=', dataset().p_updated_after)
   ```
   Click **OK**

7. **Publish all** → **Publish**

---

## Part C — Create Dataset 2: `ds_bronze_payments_sink_v3`

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
   - **File** → **Add dynamic content**:
     ```
     @concat('page_', string(dataset().p_page), '.json')
     ```

8. **Publish all** → **Publish**

---

## Part D — Create Dataset 3: `ds_pipeline_audit_csv`

This single dataset is used for **both reading** (Lookup Activity) and **writing** (Copy Activity sink) the audit CSV. It points to `bronze/audit/pipeline_audit.csv` using `ls_adls_bronze` — no new linked service needed.

1. **Datasets** → **+** → **New dataset**
2. Search `Azure Data Lake Storage Gen2` → **Continue**
3. Select **DelimitedText** → **Continue**
4. Fill in:
   - **Name:** `ds_pipeline_audit_csv`
   - **Linked service:** `ls_adls_bronze`
   - **File system:** `bronze`
   - **Directory:** `audit`
   - **File:** `pipeline_audit.csv`
   - **First row as header:** toggle **ON**
5. Click **OK**

6. **Connection** tab — verify:
   - **Column delimiter:** Comma (`,`)
   - **Row delimiter:** Default (`\n`)
   - **First row as header:** checked

7. **Publish all** → **Publish**

---

## Part E — Create Pipeline: `pl_bronze_api_payments_v3`

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
2. Connect: `act_get_username` → `act_get_password`
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
   - **Body** → **Add dynamic content**:
     ```
     @concat('{"username":"', activity('act_get_username').output.value, '","password":"', activity('act_get_password').output.value, '"}')
     ```

---

### Step 7 — Activity 4: `act_set_token`

**Type:** Set Variable

1. Drag **Set variable** → **Name:** `act_set_token`
2. Connect: `act_api_login` → `act_set_token`
3. **Settings:**
   - **Variable:** `v_token`
   - **Value:** `@activity('act_api_login').output.token`

---

### Step 8 — Activity 5: `act_set_ingestion_date`

**Type:** Set Variable

1. Drag **Set variable** → **Name:** `act_set_ingestion_date`
2. Connect: `act_set_token` → `act_set_ingestion_date`
3. **Settings:**
   - **Variable:** `v_ingestion_date`
   - **Value:** `@formatDateTime(utcNow(), 'yyyy-MM-dd')`

---

### Step 9 — Activity 6: `act_get_watermark` ⭐ *New in v3*

**Type:** Lookup Activity — reads `pipeline_audit.csv` from Bronze to get the watermark automatically.

> For a **full load** the pipeline ignores the Lookup output and uses the `v_watermark` default (`1900-01-01T00:00:00Z`). For an **incremental load** it reads `watermark_value` from the first data row of the CSV.

1. Activities panel → expand **General** → drag **Lookup** → **Name:** `act_get_watermark`
2. Connect: `act_set_ingestion_date` → `act_get_watermark`

#### Settings tab:

3. **Source dataset:** `ds_pipeline_audit_csv`
4. **Use query:** select **Dataset** (reads all rows; first row only will be taken)
5. **First row only:** toggle **ON**

> This returns `activity('act_get_watermark').output.firstRow.watermark_value` — the `watermark_value` column from the first data row of the CSV.

---

### Step 10 — Activity 7: `act_set_watermark`

**Type:** Set Variable — chooses the watermark based on load type.

1. Drag **Set variable** → **Name:** `act_set_watermark`
2. Connect: `act_get_watermark` → `act_set_watermark`
3. **Settings:**
   - **Variable:** `v_watermark`
   - **Value** → **Add dynamic content**:
     ```
     @if(
       equals(pipeline().parameters.p_load_type, 'full'),
       '1900-01-01T00:00:00Z',
       activity('act_get_watermark').output.firstRow.watermark_value
     )
     ```
     Click **OK**

> Full load always uses the epoch constant regardless of what is in the CSV. Incremental reads from the CSV.

---

### Step 11 — Activity 8: `act_get_total_pages`

**Type:** Web Activity

1. Drag **Web** → **Name:** `act_get_total_pages`
2. Connect: `act_set_watermark` → `act_get_total_pages`
3. **Settings:**
   - **URL** → **Add dynamic content**:
     ```
     @concat('https://ev-project-navy-mu.vercel.app/api/db/payments/?page=1&page_size=100&updated_after=', variables('v_watermark'))
     ```
   - **Method:** GET
   - **Headers:** `Authorization` → `@concat('Token ', variables('v_token'))`

---

### Step 12 — Activity 9: `act_set_total_pages`

**Type:** Set Variable

1. Drag **Set variable** → **Name:** `act_set_total_pages`
2. Connect: `act_get_total_pages` → `act_set_total_pages`
3. **Settings:**
   - **Variable:** `v_total_pages`
   - **Value:** `@activity('act_get_total_pages').output.pagination.total_pages`

---

### Step 13 — Activity 10: `act_paginate` (Until Loop)

1. Drag **Until** → **Name:** `act_paginate`
2. Connect: `act_set_total_pages` → `act_paginate`
3. **Settings:**
   - **Expression:** `@greater(variables('v_current_page'), variables('v_total_pages'))`
   - **Timeout:** `0.12:00:00`

#### Open loop canvas — click the pencil icon inside the Until box

---

### Step 14 — Inside Loop: `act_copy_payments_page`

**Type:** Copy Activity

1. Drag **Copy data** → **Name:** `act_copy_payments_page`

#### Source tab:
- **Source dataset:** `ds_voltgrid_payments_src_v3`
- **Dataset parameters:**
  - `p_page` → `@variables('v_current_page')`
  - `p_page_size` → `100`
  - `p_updated_after` → `@variables('v_watermark')`
- **Additional headers:** `Authorization` → `@concat('Token ', variables('v_token'))`
- **Request method:** GET

#### Sink tab:
- **Sink dataset:** `ds_bronze_payments_sink_v3`
- **Dataset parameters:**
  - `p_ingestion_date` → `@variables('v_ingestion_date')`
  - `p_page` → `@variables('v_current_page')`

---

### Step 15 — Inside Loop: `act_set_temp_page`

1. Drag **Set variable** → **Name:** `act_set_temp_page`
2. Connect: `act_copy_payments_page` → `act_set_temp_page`
3. **Settings:**
   - **Variable:** `v_temp_page`
   - **Value:** `@add(variables('v_current_page'), 1)`

---

### Step 16 — Inside Loop: `act_increment_page`

1. Drag **Set variable** → **Name:** `act_increment_page`
2. Connect: `act_set_temp_page` → `act_increment_page`
3. **Settings:**
   - **Variable:** `v_current_page`
   - **Value:** `@variables('v_temp_page')`

#### Exit the loop — click the pipeline name in the breadcrumb

---

### Step 17 — Activity 11: `act_set_status_success` ⭐ *New in v3*

**Type:** Set Variable

1. Drag **Set variable** → **Name:** `act_set_status_success`
2. Connect: `act_paginate` → `act_set_status_success`
   - **Dependency condition:** **Success** (default)
3. **Settings:**
   - **Variable:** `v_status`
   - **Value:** type `succeeded` directly (no dynamic content)

---

### Step 18 — Activity 12: `act_set_status_failed` ⭐ *New in v3*

**Type:** Set Variable

1. Drag **Set variable** → **Name:** `act_set_status_failed`
2. Connect: `act_paginate` → `act_set_status_failed`
   - Click the arrow → change **Dependency condition** to **Failure**
3. **Settings:**
   - **Variable:** `v_status`
   - **Value:** type `failed` directly

---

### Step 19 — Activity 13: `act_write_audit` ⭐ *New in v3*

**Type:** Copy Activity — appends one CSV row to `pipeline_audit.csv` in Bronze. Uses an **inline dataset** as source (no separate file needed) and `ds_pipeline_audit_csv` as sink.

Always runs after both status activities — regardless of whether the loop succeeded or failed.

1. Drag **Copy data** → **Name:** `act_write_audit`
2. Connect **two** arrows into `act_write_audit`:
   - `act_set_status_success` → `act_write_audit` (condition: **Success**)
   - `act_set_status_failed` → `act_write_audit` (condition: **Success**)

   > Both use "Success" condition — meaning "this Set Variable activity completed running", not "the pipeline succeeded". This ensures the audit row is always written.

#### Source tab:

3. **Source type:** select **DelimitedText**
4. Click **Use inline dataset**
5. **Linked service:** `ls_adls_bronze`

   Under **Inline dataset** → set these to blank/default so we override with dynamic content:
   - **File system:** `bronze`
   - **Directory:** leave blank
   - **File name:** leave blank

6. **File path type:** select **Wildcard file path** — but we will supply the content via the **Additional columns** approach instead.

   > **Simpler approach:** Rather than an inline file, use the Copy Activity's **Additional columns** feature to inject dynamic values into a static source file. Here is the actual pattern:

   **Revised Source setup:**
   - **Source dataset:** `ds_pipeline_audit_csv` (same dataset, reuse it)
   - **Additional columns** → **+ New** for each audit field:

     | Name | Value (Add dynamic content) |
     |---|---|
     | `pipeline_name` | `pl_bronze_api_payments_v3` |
     | `load_type` | `@pipeline().parameters.p_load_type` |
     | `watermark_value` | `@variables('v_watermark')` |
     | `ingestion_date` | `@variables('v_ingestion_date')` |
     | `total_pages` | `@string(variables('v_total_pages'))` |
     | `status` | `@variables('v_status')` |
     | `pipeline_run_id` | `@pipeline().RunId` |
     | `run_timestamp` | `@utcNow()` |

   > This copies the existing header row, injects the dynamic values as a new row, and writes it to the sink. The sink uses **append** mode so each run adds a row.

   > **Alternative (simpler, paste JSON approach):** Paste `pl_bronze_api_payments_v3.json` which uses a **Web Activity** to construct the CSV row as a string and a Copy Activity with a binary inline source. See the JSON file for the exact definition — it is easier to paste than to configure the inline dataset via UI.

#### Sink tab:

7. **Sink dataset:** `ds_pipeline_audit_csv`
8. **Copy behavior:** select **Append dynamic content** — or leave default; ADF writes to the same file in append mode because the sink dataset points to an existing file

---

### Step 20 — Publish the Pipeline

1. **Publish all** → **Publish**
2. Wait for "Published successfully"

---

## Part F — Trigger and Verify

### First run — Full load

1. `pl_bronze_api_payments_v3` → **Add trigger** → **Trigger now**
2. Parameters:
   - `p_load_type`: `full`
3. Click **OK**
4. **Monitor** → **Pipeline runs** → all 13 activities should turn green

### Check the audit CSV after full load

1. Portal → your storage account → **Containers** → `bronze` → `audit` → click `pipeline_audit.csv`
2. Click **Edit** (or download)
3. You should see two rows: the header + one data row with `load_type = full` and `status = succeeded`

### Update the watermark after full load (one-time manual step)

The full load wrote `watermark_value = 1900-01-01T00:00:00Z`. Edit `pipeline_audit.csv` to set this to the actual latest `updated_at` timestamp from your Bronze payment data:

1. Download `pipeline_audit.csv` from Portal
2. Change `watermark_value` in the data row to the max `updated_at` value (e.g. `2026-07-05T23:59:00Z`)
3. Re-upload to `bronze/audit/pipeline_audit.csv` (overwrite)

> Day 8 (Orchestration) will automate this step.

### Verify Bronze payment output

```python
# In a Databricks notebook or ADF Data Flow preview:
display(dbutils.fs.ls("abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/"))

df = spark.read.option("multiLine", "true").json(
    "abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/ingestion_date=2026-07-06/page_1.json"
)
display(df.limit(5))
```

### Daily scheduled run — Incremental

1. `pl_bronze_api_payments_v3` → **Add trigger** → **New/Edit** → **+ New**
   - **Name:** `trg_payments_daily`
   - **Type:** Schedule
   - **Recurrence:** Every 1 Day at `01:00 UTC`
2. Click **Next**
3. **Parameters:** `p_load_type` = `incremental`
4. Click **Finish** → **Publish all**

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `act_get_watermark` fails: file not found | `pipeline_audit.csv` not in `bronze/audit/` | Upload the CSV with header row (Part A above) |
| `act_get_watermark` returns `firstRow` with empty `watermark_value` | CSV has only the header row, no data yet | Run a full load first to write the first data row |
| Incremental run fetches all records | Watermark still shows `1900-01-01T00:00:00Z` | Edit `pipeline_audit.csv` and set `watermark_value` to the actual max timestamp (see Part F above) |
| `act_write_audit` not always running | Missing second dependency arrow | Ensure both `act_set_status_success` and `act_set_status_failed` connect to `act_write_audit` with **Success** condition |
| `act_write_audit` permission denied | ADF MI missing `Storage Blob Data Contributor` | Portal → Storage Account → IAM → assign `Storage Blob Data Contributor` to ADF managed identity |
| `act_get_username` 403 | ADF MI missing `Key Vault Secrets User` | Portal → Key Vault → IAM → assign role, wait 2 min |
| `act_api_login` 401 | Wrong credentials | Check `voltgrid-username` and `voltgrid-password` in Key Vault |
| Until loop runs only once | `v_total_pages` stayed at 1 | Monitor → `act_get_total_pages` output → confirm `pagination.total_pages` key exists |
