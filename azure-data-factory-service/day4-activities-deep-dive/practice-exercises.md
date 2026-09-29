# Day 4 — Practice Exercises: ADF Activities

> All exercises use `adf-datalake-dev-ded`. Open ADF Studio before starting.
> Most exercises extend the existing `pl_bronze_api_payments` pipeline — no new pipeline needed.

---

## Exercise 1 — Get Metadata + If Condition: Verify Bronze File Before Silver (20 min)

**Goal:** After copying payments to Bronze, check the file exists and is not empty before declaring success.

### Task 1.1 — Create a dataset pointing to the Bronze output

We already have `ds_bronze_payments_sink`. Create a second dataset that points to the folder (not the file) so Get Metadata can check the child items.

1. **Author** → **Datasets** → **+ New dataset** → **Azure Data Lake Storage Gen2** → **JSON**
2. Name: `ds_bronze_payments_folder`
3. Linked service: `ls_adls_bronze`
4. File path:
   - Container: `bronze`
   - Directory → **Add dynamic content**: `api/payments/ingestion_date=@{dataset().p_run_date}`
   - File: *(leave blank — we point to the folder)*
5. **Parameters** tab → add `p_run_date` (String)
6. **Publish all**

### Task 1.2 — Add Get Metadata after the Copy Activity

1. Open `pl_bronze_api_payments`
2. Drag **Get Metadata** from Activities panel → canvas → rename to `act_check_bronze`
3. Connect `act_copy_payments` → `act_check_bronze` (On Success)
4. **Settings** tab:
   - Dataset: `ds_bronze_payments_folder`
   - Dataset properties: `p_run_date` = `@variables('v_ingestion_date')`
   - **Field list** → **+ New** for each:
     - `exists`
     - `childItems`
5. **Publish all** → **Debug** → after run, click `act_check_bronze` → **Output** tab

**Expected output:**
```json
{
  "exists": true,
  "childItems": [
    { "name": "page_1.json", "type": "File" }
  ]
}
```

### Task 1.3 — Add If Condition to check the file exists

1. Drag **If Condition** → rename to `act_verify_file`
2. Connect `act_check_bronze` → `act_verify_file`
3. **Settings** tab → Expression → **Add dynamic content**:
   ```
   @activity('act_check_bronze').output.exists
   ```
4. Click **Edit True branch** (pencil icon):
   - Drag **Wait** activity → rename to `act_success_wait` → Wait time: `1` second
   - This represents "continue to next step" — in a real pipeline this connects to Databricks
5. Click **Edit False branch**:
   - Drag **Fail** activity → rename to `act_fail_missing`
   - Message: `Bronze file missing for ingestion_date=@{variables('v_ingestion_date')}`
   - Error code: `BRONZE_MISSING`
6. **Publish all** → **Debug**

**Test both branches:**
- Normal run → True branch executes → Wait succeeds
- To test False: temporarily break the sink dataset path → run → False branch fires → Monitor shows Fail message

---

## Exercise 2 — Append Variable: Collect Row Counts (15 min)

**Goal:** Build an array of row counts across multiple runs — demonstrates Append Variable behaviour.

### Task 2.1 — Add an Array variable to the pipeline

1. Open `pl_bronze_api_payments` → canvas (deselect all) → **Variables** tab
2. **+ New**: Name `v_row_counts` | Type `Array`

### Task 2.2 — Add Append Variable after the Copy Activity

1. Drag **Append Variable** → rename to `act_record_row_count`
2. Connect `act_copy_payments` → `act_record_row_count` (parallel with `act_check_bronze` — both succeed arrows from `act_copy_payments`)
3. **Settings** tab:
   - Variable: `v_row_counts`
   - Value → **Add dynamic content**:
     ```
     @activity('act_copy_payments').output.rowsCopied
     ```
4. **Publish all** → **Debug** with `p_page=1`

**After the run:** Monitor → `act_record_row_count` → Output → `v_row_counts` = `[100]`

Run Debug again → Output → `v_row_counts` = `[100]` *(resets each run — variables reset per pipeline run)*

**Key insight:** Each pipeline run starts with `v_row_counts = []`. Within a single run, Append Variable builds the array. Across runs, it resets — because variables are per-run, not persisted.

---

## Exercise 3 — Lookup + ForEach: Loop Over Multiple Endpoints (30 min)

**Goal:** Replace hardcoded endpoint logic with a config-driven ForEach that loops over all VoltGrid endpoints.

### Task 3.1 — Create the config file in ADLS Gen2

1. Create a file locally named `endpoint_config.json`:
```json
[
  { "table_name": "payments",  "endpoint": "/api/db/payments/"  },
  { "table_name": "sessions",  "endpoint": "/api/db/sessions/"  }
]
```
2. Portal → **Storage accounts** → `evdatalakedev` → **Containers** → `bronze`
3. Create folder `config` → Upload `endpoint_config.json`

### Task 3.2 — Create a dataset for the config file

1. **Author** → **Datasets** → **+ New dataset** → **Azure Data Lake Storage Gen2** → **JSON**
2. Name: `ds_endpoint_config`
3. Linked service: `ls_adls_bronze`
4. File path: `bronze` / `config` / `endpoint_config.json`
5. **Publish all**

### Task 3.3 — Create a new pipeline for multi-endpoint ingestion

Rather than modifying `pl_bronze_api_payments`, create a new pipeline:

1. **Author** → **Pipelines** → **+** → **New pipeline**
2. Name: `pl_bronze_all_endpoints`
3. **Variables** tab → add `v_token` (String), `v_ingestion_date` (String)

### Task 3.4 — Add Lookup Activity

1. Drag **Lookup** → rename to `act_lookup_endpoints`
2. **Settings** tab:
   - Source dataset: `ds_endpoint_config`
   - First row only: **No** (we want all rows)
3. Connect it first in the pipeline (no dependency)

### Task 3.5 — Add login activities (reuse pattern from pl_bronze_api_payments)

After Lookup:
1. `act_get_username` → `act_get_password` → `act_api_login` → `act_set_token` → `act_set_ingestion_date`

(Same configuration as `pl_bronze_api_payments` — copy the settings)

### Task 3.6 — Add ForEach Activity

1. Drag **ForEach** → rename to `act_loop_endpoints`
2. Connect `act_set_ingestion_date` → `act_loop_endpoints`
3. **Settings** tab:
   - Items → **Add dynamic content**:
     ```
     @activity('act_lookup_endpoints').output.value
     ```
   - Is Sequential: `false`
   - Batch count: `2` (start with 2 — safer for API)

### Task 3.7 — Add Copy Activity inside the ForEach

1. Click the **pencil icon** on `act_loop_endpoints` to open the inner canvas
2. Drag **Copy data** → rename to `act_copy_endpoint`
3. **Source** tab:
   - Source dataset: `ds_voltgrid_payments_src` (reuse existing REST dataset)
   - Relative URL → **Add dynamic content**:
     ```
     @concat(item().endpoint, '?page=1&page_size=10')
     ```
   - Additional headers → `Authorization`: `@concat('Token ', variables('v_token'))`
4. **Sink** tab:
   - Sink dataset: `ds_bronze_payments_sink`
   - `p_run_date` = `@variables('v_ingestion_date')`

   > Note: the sink path will collide since both endpoints write to the same dataset. For a real pipeline, create a generic sink dataset with a `table_name` parameter. For this exercise, the goal is to observe the ForEach mechanism.

5. **Publish all** → **Debug**
6. Monitor → observe 2 Copy Activities running in parallel (one per endpoint)

**Expected Monitor — Activity runs:**
```
act_lookup_endpoints     Succeeded
act_get_username         Succeeded
act_get_password         Succeeded
act_api_login            Succeeded
act_set_token            Succeeded
act_set_ingestion_date   Succeeded
act_loop_endpoints       Succeeded
  └─ act_copy_endpoint (payments)   Succeeded   ← parallel
  └─ act_copy_endpoint (sessions)   Succeeded   ← parallel
```

---

## Exercise 4 — If Condition: Full vs Incremental Load Switch (20 min)

**Goal:** Add a parameter `p_load_type` to `pl_bronze_api_payments` and use If Condition to set the `updated_after` watermark accordingly.

### Task 4.1 — Add pipeline parameter and variable

1. Open `pl_bronze_api_payments` → **Parameters** tab → **+ New**:
   - Name: `p_load_type` | Type: `String` | Default: `full`
2. **Variables** tab → **+ New**:
   - Name: `v_watermark` | Type: `String`

### Task 4.2 — Add If Condition activity

1. Drag **If Condition** → rename to `act_set_load_type`
2. Place it after `act_set_ingestion_date`, before `act_copy_payments`
3. **Settings** → Expression → **Add dynamic content**:
   ```
   @equals(pipeline().parameters.p_load_type, 'full')
   ```
4. **Edit True branch:**
   - Drag **Set Variable** → rename to `act_set_full_watermark`
   - Variable: `v_watermark`
   - Value: `1900-01-01T00:00:00Z`
5. **Edit False branch:**
   - Drag **Set Variable** → rename to `act_set_incr_watermark`
   - Variable: `v_watermark`
   - Value → **Add dynamic content**: `@addDays(utcnow(), -1)`
   - *(Yesterday's date as fallback — in production this comes from a pipeline_audit table)*
6. Connect `act_set_load_type` → `act_copy_payments`

### Task 4.3 — Update Copy Activity source URL to use watermark

1. Click `act_copy_payments` → **Source** tab
2. The source dataset `ds_voltgrid_payments_src` builds its URL from `p_page` and `p_page_size`
3. We need to add `updated_after` — go to **Additional headers** or update the dataset relative URL

Update `ds_voltgrid_payments_src` **Connection** tab → Relative URL → **Edit dynamic content**:
```
/api/db/payments/?page=@{dataset().p_page}&page_size=@{dataset().p_page_size}&updated_after=@{dataset().p_watermark}
```
Add dataset parameter `p_watermark` (String, default: `1900-01-01T00:00:00Z`).

In `act_copy_payments` Source → Dataset properties: `p_watermark` = `@variables('v_watermark')`

### Task 4.4 — Test both branches

**Full load:** Debug → `p_load_type = full` → Monitor → `act_set_full_watermark` runs (True branch)

**Incremental load:** Debug → `p_load_type = incremental` → Monitor → `act_set_incr_watermark` runs (False branch)

Check `act_copy_payments` → Input tab → confirm the URL contains the correct `updated_after` value.

---

## Exercise 5 — Wait + Fail: Rate Limiting and Explicit Failure (10 min)

**Goal:** See Wait and Fail activities in action.

### Task 5.1 — Add Wait between two Web Activities

1. In `pl_bronze_api_payments`, insert a **Wait** between `act_api_login` and `act_set_token`
2. Name: `act_wait_rate_limit`
3. Wait time: `2` seconds
4. Re-wire: `act_api_login` → `act_wait_rate_limit` → `act_set_token`
5. **Debug** — observe the 2-second pause in the Monitor timeline

### Task 5.2 — Add Fail if login response has no token

After `act_api_login`, before the Wait, add an **If Condition**:

1. Drag **If Condition** → name `act_check_token`
2. Connect `act_api_login` → `act_check_token` → `act_wait_rate_limit`
3. Expression:
   ```
   @empty(activity('act_api_login').output.token)
   ```
4. True branch (token is empty → fail):
   - **Fail** activity → Message: `Login succeeded but returned no token` | Error code: `NO_TOKEN`
5. False branch: leave empty (token is present → continue)
6. **Publish all** → **Debug** → confirm it runs through successfully (token is present, `empty()` returns false → False branch → nothing → continues)

**To test the True branch:** Temporarily change the login URL to return a different JSON shape — the `token` key won't be present → `empty()` returns true → Fail fires.

---

## Exercise 6 — Switch Activity: Route by Environment (10 min)

**Goal:** Use Switch to demonstrate multi-branch routing.

### Task 6.1 — Create a minimal Switch demo pipeline

1. **+ New pipeline** → name `pl_switch_demo`
2. **Parameters** tab → add `p_env` (String, default `dev`)
3. **Variables** tab → add `v_api_url` (String)

### Task 6.2 — Add Switch Activity

1. Drag **Switch** → name `act_route_env`
2. **Settings** → Expression: `@pipeline().parameters.p_env`
3. **Cases** → **+ Add case** three times:

   **Case `dev`:**
   - Set Variable → `v_api_url` = `https://ev-project-navy-mu.vercel.app`

   **Case `staging`:**
   - Set Variable → `v_api_url` = `https://ev-staging.vercel.app`

   **Case `prod`:**
   - Set Variable → `v_api_url` = `https://ev-prod.vercel.app`

   **Default:**
   - Fail → Message: `Unknown environment: @{pipeline().parameters.p_env}` | Error code: `BAD_ENV`

4. **Publish all**

### Task 6.3 — Test each branch

- Debug → `p_env = dev` → `v_api_url` = dev URL
- Debug → `p_env = prod` → `v_api_url` = prod URL
- Debug → `p_env = uat` → Default branch fires → Fail message in Monitor

**Observe:** Switch is cleaner than three nested If Conditions when you have 3+ branches on the same expression.
