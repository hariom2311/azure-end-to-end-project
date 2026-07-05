# 02 — Build v2 Payments Pipeline: Step by Step in ADF Studio
**Day 3 | pl_bronze_api_payments_v2**

This guide walks you through creating everything from scratch in the ADF Studio UI — no JSON pasting required. By the end you will have a working full + incremental payments pipeline with pagination.

> **Prerequisite:** Day 2 linked services must already exist — `ls_keyvault`, `ls_voltgrid_api`, `ls_adls_bronze`.
> **If you prefer JSON paste:** use the `.json` files in this folder + `PIPELINE_V2_EXPLAINED.md`.

---

## What You Will Build

| Artifact | Name | Purpose |
|---|---|---|
| Dataset | `ds_voltgrid_payments_src_v2` | Source — VoltGrid API with page + watermark parameters |
| Dataset | `ds_bronze_payments_sink_v2` | Sink — ADLS Bronze, partitioned by date and page |
| Pipeline | `pl_bronze_api_payments_v2` | Full + incremental load, paginates all pages |

**Pipeline parameters:**

| Parameter | Type | Purpose |
|---|---|---|
| `p_load_type` | String | `full` or `incremental` |
| `p_watermark` | String | ISO 8601 datetime — only used when `p_load_type = incremental` |

**Pipeline variables:**

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `v_token` | String | — | API bearer token set after login |
| `v_watermark` | String | — | Resolved watermark used in API calls |
| `v_ingestion_date` | String | — | Today's date — Bronze partition folder name |
| `v_current_page` | Integer | 1 | Current loop page number |
| `v_temp_page` | Integer | 1 | Intermediate variable for page increment |
| `v_total_pages` | Integer | 1 | Total pages from API response |

---

## Part A — Create Dataset 1: `ds_voltgrid_payments_src_v2`

This dataset points to the VoltGrid payments endpoint. It has three parameters so the pipeline can control which page to fetch and which watermark to apply.

### Steps

1. ADF Studio → **Author** (pencil icon) → **Datasets** section → click **+** → **New dataset**
2. In the search box type `REST` → select **REST** → click **Continue**
3. Fill in the basics:
   - **Name:** `ds_voltgrid_payments_src_v2`
   - **Linked service:** `ls_voltgrid_api`
   - **Relative URL:** leave blank for now
4. Click **OK**

### Add Parameters

5. In the dataset editor → click the **Parameters** tab (bottom panel)
6. Click **+ New** and add these three parameters:

   | Name | Type | Default value |
   |---|---|---|
   | `p_page` | Int | `1` |
   | `p_page_size` | Int | `100` |
   | `p_updated_after` | String | `1900-01-01T00:00:00Z` |

### Set the Dynamic Relative URL

7. Click the **Connection** tab
8. In the **Relative URL** field → click **Add dynamic content** (the blue link below the field)
9. Paste this expression:
   ```
   @concat('/api/db/payments/?page=', string(dataset().p_page), '&page_size=', string(dataset().p_page_size), '&updated_after=', dataset().p_updated_after)
   ```
10. Click **OK**

### Publish

11. Click **Publish all** → **Publish**

> **What this expression does:** builds the full query string at runtime — e.g. `/api/db/payments/?page=3&page_size=100&updated_after=2026-07-04T00:00:00Z`

---

## Part B — Create Dataset 2: `ds_bronze_payments_sink_v2`

This dataset points to the Bronze container in ADLS Gen2. The path is dynamic — each run creates a dated folder and each page gets its own file.

### Steps

1. **Datasets** → **+** → **New dataset**
2. Search `Azure Data Lake Storage Gen2` → select it → click **Continue**
3. Select **JSON** as the format → click **Continue**
4. Fill in:
   - **Name:** `ds_bronze_payments_sink_v2`
   - **Linked service:** `ls_adls_bronze`
   - **File system:** `bronze`
   - Leave **Directory** and **File** blank for now
5. Click **OK**

### Add Parameters

6. **Parameters** tab → **+ New** → add:

   | Name | Type |
   |---|---|
   | `p_ingestion_date` | String |
   | `p_page` | Int |

### Set the Dynamic Path

7. **Connection** tab
8. **Directory** field → click **Add dynamic content**:
   ```
   @concat('api/payments/raw/ingestion_date=', dataset().p_ingestion_date)
   ```
   Click **OK**

9. **File** field → click **Add dynamic content**:
   ```
   @concat('page_', string(dataset().p_page), '.json')
   ```
   Click **OK**

### Publish

10. **Publish all** → **Publish**

> **Result path example:** `bronze/api/payments/raw/ingestion_date=2026-07-05/page_3.json`

---

## Part C — Create Pipeline: `pl_bronze_api_payments_v2`

### Step 1 — Create the Pipeline Shell

1. **Author** → **Pipelines** section → click **+** → **New pipeline**
2. **Name:** `pl_bronze_api_payments_v2`
3. Click somewhere on the blank canvas to deselect, then look at the **bottom panel**

### Step 2 — Add Parameters

4. Bottom panel → **Parameters** tab → **+ New** → add:

   | Name | Type | Default value |
   |---|---|---|
   | `p_load_type` | String | `incremental` |
   | `p_watermark` | String | *(leave blank)* |

### Step 3 — Add Variables

5. Bottom panel → **Variables** tab → **+ New** → add:

   | Name | Type | Default value |
   |---|---|---|
   | `v_token` | String | *(blank)* |
   | `v_watermark` | String | *(blank)* |
   | `v_ingestion_date` | String | *(blank)* |
   | `v_current_page` | Integer | `1` |
   | `v_temp_page` | Integer | `1` |
   | `v_total_pages` | Integer | `1` |

---

### Step 4 — Activity 1: `act_get_username`

**Type:** Web Activity — reads VoltGrid username from Key Vault.

1. In the **Activities** panel (left side) → expand **General** → drag **Web** onto the canvas
2. Click the activity to select it → in the bottom panel:
   - **General** tab → **Name:** `act_get_username`
3. **Settings** tab:
   - **URL:** `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-username/?api-version=7.0`
   - **Method:** GET
   - **Authentication:** System Assigned Managed Identity
   - **Resource:** `https://vault.azure.net`

---

### Step 5 — Activity 2: `act_get_password`

**Type:** Web Activity — reads VoltGrid password from Key Vault.

1. Drag another **Web** activity onto the canvas
2. **Name:** `act_get_password`
3. Draw a connection: hover over `act_get_username` → green arrow appears on the right edge → drag it to `act_get_password` → select **Success**
4. **Settings** tab:
   - **URL:** `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-password/?api-version=7.0`
   - **Method:** GET
   - **Authentication:** System Assigned Managed Identity
   - **Resource:** `https://vault.azure.net`

---

### Step 6 — Activity 3: `act_api_login`

**Type:** Web Activity — POSTs credentials to VoltGrid login, gets back a token.

1. Drag another **Web** activity → **Name:** `act_api_login`
2. Connect success arrow: `act_get_password` → `act_api_login`
3. **Settings** tab:
   - **URL:** `https://ev-project-navy-mu.vercel.app/api/auth/login/`
   - **Method:** POST
   - **Headers:** click **+ New header**
     - Name: `Content-Type` | Value: `application/json`
   - **Body:** click **Add dynamic content**:
     ```
     @concat('{"username":"', activity('act_get_username').output.value, '","password":"', activity('act_get_password').output.value, '"}')
     ```
     Click **OK**

> **What this returns:** `{ "token": "abc123xyz..." }`

---

### Step 7 — Activity 4: `act_set_token`

**Type:** Set Variable — stores the login token for use by all downstream activities.

1. In the Activities panel → expand **General** → drag **Set variable** onto the canvas
2. **Name:** `act_set_token`
3. Connect success arrow: `act_api_login` → `act_set_token`
4. **Settings** tab:
   - **Variable:** `v_token`
   - **Value:** click **Add dynamic content**:
     ```
     @activity('act_api_login').output.token
     ```
     Click **OK**

---

### Step 8 — Activity 5: `act_set_ingestion_date`

**Type:** Set Variable — captures today's UTC date once for use as the Bronze partition folder.

1. Drag **Set variable** → **Name:** `act_set_ingestion_date`
2. Connect: `act_set_token` → `act_set_ingestion_date`
3. **Settings:**
   - **Variable:** `v_ingestion_date`
   - **Value:** click **Add dynamic content**:
     ```
     @formatDateTime(utcNow(), 'yyyy-MM-dd')
     ```
     Click **OK**

---

### Step 9 — Activity 6: `act_set_watermark`

**Type:** Set Variable — resolves the watermark: epoch date for full load, the `p_watermark` parameter for incremental.

1. Drag **Set variable** → **Name:** `act_set_watermark`
2. Connect: `act_set_ingestion_date` → `act_set_watermark`
3. **Settings:**
   - **Variable:** `v_watermark`
   - **Value:** click **Add dynamic content**:
     ```
     @if(equals(pipeline().parameters.p_load_type, 'full'), '1900-01-01T00:00:00Z', pipeline().parameters.p_watermark)
     ```
     Click **OK**

> **What this does:**
> - `p_load_type = full` → `v_watermark = 1900-01-01T00:00:00Z` (fetches all records)
> - `p_load_type = incremental` → `v_watermark = p_watermark` (fetches only recent records)

---

### Step 10 — Activity 7: `act_get_total_pages`

**Type:** Web Activity — calls page 1 of the API to read `pagination.total_pages` before starting the loop.

1. Drag **Web** → **Name:** `act_get_total_pages`
2. Connect: `act_set_watermark` → `act_get_total_pages`
3. **Settings:**
   - **URL:** click **Add dynamic content**:
     ```
     @concat('https://ev-project-navy-mu.vercel.app/api/db/payments/?page=1&page_size=100&updated_after=', variables('v_watermark'))
     ```
     Click **OK**
   - **Method:** GET
   - **Headers:** click **+ New header**
     - Name: `Authorization` | Value: click **Add dynamic content** → `@concat('Token ', variables('v_token'))` → **OK**

---

### Step 11 — Activity 8: `act_set_total_pages`

**Type:** Set Variable — stores the total page count from the API response.

1. Drag **Set variable** → **Name:** `act_set_total_pages`
2. Connect: `act_get_total_pages` → `act_set_total_pages`
3. **Settings:**
   - **Variable:** `v_total_pages`
   - **Value:** click **Add dynamic content**:
     ```
     @activity('act_get_total_pages').output.pagination.total_pages
     ```
     Click **OK**

---

### Step 12 — Activity 9: `act_paginate` (Until Loop)

**Type:** Until — loops through all pages until `v_current_page` exceeds `v_total_pages`.

#### Create the Until loop

1. In the Activities panel → expand **Iteration & conditionals** → drag **Until** onto the canvas
2. **Name:** `act_paginate`
3. Connect: `act_set_total_pages` → `act_paginate`
4. **Settings** tab:
   - **Expression:** click **Add dynamic content**:
     ```
     @greater(variables('v_current_page'), variables('v_total_pages'))
     ```
     Click **OK**
   - **Timeout:** `0.12:00:00` (12 hours — safe upper limit)

#### Open the loop canvas

5. Click the **pencil icon** (Edit activities) inside the Until box — this opens the loop's inner canvas

---

### Step 13 — Inside the Loop: `act_copy_payments_page`

**Type:** Copy Activity — fetches one page from VoltGrid API and writes it to Bronze.

1. Inside the Until canvas → drag **Copy data** → **Name:** `act_copy_payments_page`

#### Source tab:
- **Source dataset:** `ds_voltgrid_payments_src_v2`
- **Dataset parameters** (click **+ New** for each):
  - `p_page` → value: click **Add dynamic content** → `@variables('v_current_page')` → OK
  - `p_page_size` → value: `100`
  - `p_updated_after` → value: click **Add dynamic content** → `@variables('v_watermark')` → OK
- **Additional headers:** click **+ New**
  - Name: `Authorization` | Value: click **Add dynamic content** → `@concat('Token ', variables('v_token'))` → OK
- **Request method:** GET

#### Sink tab:
- **Sink dataset:** `ds_bronze_payments_sink_v2`
- **Dataset parameters:**
  - `p_ingestion_date` → click **Add dynamic content** → `@variables('v_ingestion_date')` → OK
  - `p_page` → click **Add dynamic content** → `@variables('v_current_page')` → OK

#### Settings tab:
- Leave all defaults

---

### Step 14 — Inside the Loop: `act_set_temp_page`

**Type:** Set Variable — calculates `current_page + 1` into a temp variable.

> **Why a temp variable?** ADF does not allow a variable to reference itself: `v_current_page = v_current_page + 1` throws a self-reference error. The workaround is two steps: write to temp, then copy temp into current.

1. Drag **Set variable** → **Name:** `act_set_temp_page`
2. Connect success arrow: `act_copy_payments_page` → `act_set_temp_page`
3. **Settings:**
   - **Variable:** `v_temp_page`
   - **Value:** click **Add dynamic content**:
     ```
     @add(variables('v_current_page'), 1)
     ```
     Click **OK**

---

### Step 15 — Inside the Loop: `act_increment_page`

**Type:** Set Variable — copies `v_temp_page` into `v_current_page`.

1. Drag **Set variable** → **Name:** `act_increment_page`
2. Connect: `act_set_temp_page` → `act_increment_page`
3. **Settings:**
   - **Variable:** `v_current_page`
   - **Value:** click **Add dynamic content**:
     ```
     @variables('v_temp_page')
     ```
     Click **OK**

#### Exit the loop canvas

4. Click the breadcrumb at the top of the canvas (pipeline name) to go back to the main canvas

---

### Step 16 — Publish the Pipeline

1. Click **Publish all** → **Publish**
2. Wait for "Published successfully"

---

## Part D — Trigger and Verify

### First run — Full load

1. `pl_bronze_api_payments_v2` → **Add trigger** → **Trigger now**
2. Parameters:
   - `p_load_type`: `full`
   - `p_watermark`: *(leave blank)*
3. Click **OK**
4. **Monitor** → **Pipeline runs** → watch all activities turn green

**Expected Bronze output after full load:**
```
bronze/api/payments/raw/
└── ingestion_date=2026-07-05/
    ├── page_1.json
    ├── page_2.json
    └── page_N.json    ← one file per page
```

### Daily run — Incremental

1. **Add trigger** → **Trigger now**
2. Parameters:
   - `p_load_type`: `incremental`
   - `p_watermark`: `2026-07-05T00:00:00Z` ← start of the previous day, ISO 8601
3. Click **OK**

> **Important:** `p_watermark` must always be in ISO 8601 format: `YYYY-MM-DDTHH:MM:SSZ`. ADF `equals()` is case-sensitive — always pass `full` in lowercase.

### Verify in Databricks

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

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `act_get_username` fails with 403 | ADF MI missing `Key Vault Secrets User` role | Portal → Key Vault → IAM → assign role, wait 2 min |
| `act_api_login` fails with 401 | Wrong credentials in Key Vault | Check `voltgrid-username` and `voltgrid-password` values |
| `act_copy_payments_page` fails with 401 | Token not in header | Confirm `act_set_token` succeeded and `v_token` is not empty |
| Until loop runs only once | `v_total_pages` is 1 | Check `act_get_total_pages` output in Monitor → confirm `pagination.total_pages` key exists |
| Incremental returns all records | `p_watermark` left blank | Always pass watermark date when `p_load_type = incremental` |
| Self-reference error on page increment | Direct `v_current_page = v_current_page + 1` | Use two-variable pattern: `v_temp_page` then `v_current_page = v_temp_page` |
| `act_copy_payments_page` fails with 403 | ADF MI missing `Storage Blob Data Contributor` on `evdatalakedev` | Portal → Storage → IAM → assign role, wait 2 min |

---

## What v3 Adds

v2 requires you to manually pass `p_watermark` each incremental run. v3 removes that — it reads the watermark automatically from the `pipeline_audit` Delta table and writes an audit row after every run.

→ See `03_ADF_PIPELINE_V3_STEP_BY_STEP.md` to build v3.
