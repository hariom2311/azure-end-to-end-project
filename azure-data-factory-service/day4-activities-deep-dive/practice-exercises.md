# Day 4 — Activity Implementation Guide

> **Base pipeline:** `pl_bronze_api_payments` in `adf-datalake-dev-ded`
> Each section covers one activity with exact steps: where to drag it, how to wire it, what to configure, and what to verify in Monitor.
> Work through them in order — later activities build on earlier ones.

---

## Before You Start — Current Pipeline State

Open `pl_bronze_api_payments`. It currently has 5 activities:

```
act_get_username ──┐
                   ├──► act_api_login ──► act_set_token ──► act_copy_payments
act_get_password ──┘
```

Each section below adds or extends one activity and tells you exactly where it fits.

---

## Activity 1 — Copy Data (Settings Deep-Dive)

> The pipeline already has `act_copy_payments`. This section walks through every important tab so you understand what each setting controls.

### Steps

1. Open `pl_bronze_api_payments` → click `act_copy_payments`
2. **Source tab** — review:
   - **Source dataset:** `ds_voltgrid_payments_src`
   - **Dataset properties:** `p_page` and `p_page_size` passed from pipeline parameters
   - **Request method:** GET
   - **Additional headers:** `Authorization` = `@concat('Token ', variables('v_token'))`
   - **Pagination rules:** `supportRFC5988: true` — ADF follows the `Link: <next>` header and fetches all pages automatically until no next link is returned
3. **Sink tab** — review:
   - **Sink dataset:** `ds_bronze_payments_sink`
   - **File pattern:** `setOfObjects` — writes all records as a single JSON array
4. **Mapping tab** — leave blank for REST-to-JSON (schema passes through as-is)
5. **Settings tab** — check:
   - **Fault tolerance:** Skip incompatible rows → OFF (hard failure on bad data)
   - **Logging:** Enable only if you want skipped rows written to ADLS
6. **Publish all** → **Debug** → after run, click `act_copy_payments` → **Output** tab

**Expected Output:**
```json
{
  "dataRead": 45230,
  "dataWritten": 45230,
  "rowsCopied": 100,
  "rowsRead": 100,
  "copyDuration": 8,
  "throughput": 5.6
}
```

`rowsCopied` is the value you will reference in later activities to confirm the copy worked.

---

## Activity 2 — Web Activity

> The pipeline already uses Web Activity three times (Key Vault reads + login). This section adds a fourth: a health check before doing anything else.

### Steps

1. Open `pl_bronze_api_payments` → Activities panel → search **Web** → drag to canvas, place it to the left of `act_get_username`
2. Rename to `act_health_check`
3. **Settings tab:**
   - **URL:** `https://ev-project-navy-mu.vercel.app/api/health/`
   - **Method:** GET
   - **Authentication:** None
4. **Wire it:** draw On Success arrows from `act_health_check` to both `act_get_username` and `act_get_password`
5. **Publish all** → **Debug**
6. Monitor → click `act_health_check` → **Output** tab — confirm status 200:
   ```json
   {
     "statusCode": 200,
     "body": { "status": "ok" }
   }
   ```

**What this achieves:** If the API is down, `act_health_check` fails and the pipeline stops before wasting time on Key Vault calls or the login POST.

---

## Activity 3 — Set Variable

> `act_set_token` already stores the auth token. This section adds a second Set Variable to stamp today's ingestion date.

### Steps

1. Open `pl_bronze_api_payments` → click the canvas background → **Variables** tab
2. If `v_ingestion_date` is not present, add it: Name `v_ingestion_date` | Type `String`
3. Activities panel → drag **Set Variable** → place between `act_set_token` and `act_copy_payments`
4. Rename to `act_set_ingestion_date`
5. **Settings tab:**
   - **Variable:** `v_ingestion_date`
   - **Value** → **Add dynamic content**:
     ```
     @formatDateTime(utcnow(), 'yyyy-MM-dd')
     ```
6. **Wire it:**
   - `act_set_token` → `act_set_ingestion_date` (On Success)
   - `act_set_ingestion_date` → `act_copy_payments` (On Success)
   - Remove the old direct arrow from `act_set_token` → `act_copy_payments`
7. **Publish all** → **Debug**
8. Monitor → click `act_set_ingestion_date` → **Output** tab:
   ```json
   { "variableName": "v_ingestion_date", "value": "2026-09-29" }
   ```

**How this is used:** Pass `@variables('v_ingestion_date')` to the sink dataset parameter `p_run_date` so each run writes to a dated folder like `api/payments/ingestion_date=2026-09-29/payments.json`.

---

## Activity 4 — Append Variable

> Append Variable adds one item to an array variable — useful for collecting per-iteration results within a single run.

### Steps

1. Open `pl_bronze_api_payments` → **Variables** tab → **+ New**:
   - Name: `v_row_counts` | Type: `Array`
2. Drag **Append Variable** → place after `act_copy_payments`
3. Rename to `act_record_row_count`
4. **Wire it:** `act_copy_payments` → `act_record_row_count` (On Success)
5. **Settings tab:**
   - **Variable:** `v_row_counts`
   - **Value** → **Add dynamic content**:
     ```
     @activity('act_copy_payments').output.rowsCopied
     ```
6. **Publish all** → **Debug**
7. Monitor → click `act_record_row_count` → **Output** tab:
   ```json
   { "variableName": "v_row_counts", "value": [100] }
   ```

Run Debug a second time → still `[100]` — variables reset per pipeline run.

**Set Variable vs Append Variable:**
- **Set Variable** → replaces the whole value each call
- **Append Variable** → adds one element to the array; used inside ForEach to collect one result per iteration

---

## Activity 5 — Get Metadata

> Get Metadata reads properties (existence, child items, size) of a file or folder in ADLS Gen2 without moving any data.

### Steps

#### 5.1 — Create a folder-level dataset

1. **Author** → **Datasets** → **+ New dataset** → **Azure Data Lake Storage Gen2** → **JSON**
2. Name: `ds_bronze_payments_folder`
3. Linked service: `ls_adls_bronze`
4. File path:
   - Container: `bronze`
   - Directory: `api/payments`
   - File: *(leave blank — pointing at the folder)*
5. **Publish all**

#### 5.2 — Add Get Metadata to the pipeline

1. Drag **Get Metadata** → place after `act_record_row_count`
2. Rename to `act_check_bronze_folder`
3. **Wire it:** `act_record_row_count` → `act_check_bronze_folder` (On Success)
4. **Settings tab:**
   - **Dataset:** `ds_bronze_payments_folder`
   - **Field list** → **+ New** three times: `exists`, `childItems`, `lastModified`
5. **Publish all** → **Debug**
6. Monitor → click `act_check_bronze_folder` → **Output** tab:
   ```json
   {
     "exists": true,
     "childItems": [
       { "name": "ingestion_date=2026-09-29", "type": "Folder" }
     ],
     "lastModified": "2026-09-29T08:12:00Z"
   }
   ```

Feed `exists` into an If Condition (next section) to decide whether to proceed or fail.

---

## Activity 6 — If Condition

> If Condition routes to a True or False branch based on a boolean expression. Use it to guard downstream activities behind a file-exists check.

### Steps

1. Drag **If Condition** → place after `act_check_bronze_folder`
2. Rename to `act_verify_bronze_exists`
3. **Wire it:** `act_check_bronze_folder` → `act_verify_bronze_exists` (On Success)
4. **Settings** → **Expression** → **Add dynamic content**:
   ```
   @activity('act_check_bronze_folder').output.exists
   ```
5. **Edit True branch** (click the pencil icon on True):
   - Drag **Wait** → rename to `act_success_marker` → Wait time: `1`
   - In production this connects to a Databricks notebook or the next pipeline stage
6. **Edit False branch** (click the pencil icon on False):
   - Drag **Fail** → rename to `act_fail_no_bronze`
   - **Message** → **Add dynamic content**:
     ```
     @concat('Bronze folder missing after copy. Run date: ', variables('v_ingestion_date'))
     ```
   - **Error code:** `BRONZE_MISSING`
7. Click the canvas background to exit branch editing
8. **Publish all** → **Debug**
9. Monitor → expand `act_verify_bronze_exists` → True branch ran → `act_success_marker` Succeeded

**To test the False branch:** Temporarily change the sink dataset path to a non-existent container → Debug → `act_fail_no_bronze` fires → Monitor shows the custom error message.

---

## Activity 7 — Fail

> Fail explicitly stops the pipeline with a custom error message. Use it to surface the real problem instead of a confusing downstream failure.

### Steps

1. Drag **If Condition** → place between `act_api_login` and `act_set_token`
2. Rename to `act_guard_token`
3. **Wire it:** `act_api_login` → `act_guard_token` → `act_set_token`
4. **Settings** → Expression → **Add dynamic content**:
   ```
   @empty(activity('act_api_login').output.token)
   ```
5. **Edit True branch** (token IS empty — this is the problem case):
   - Drag **Fail** → rename to `act_fail_no_token`
   - **Message:** `Login API returned 200 but token field is missing or empty`
   - **Error code:** `NO_TOKEN`
6. **Edit False branch:** leave empty (token present → continue)
7. **Publish all** → **Debug** → normal run → False branch → pipeline continues

**Monitor view when Fail fires:**
```
act_api_login          Succeeded
act_guard_token        Failed
  └─ act_fail_no_token   Failed  ← "Login API returned 200 but token field is missing"
act_set_token          Skipped
act_copy_payments      Skipped
```

Without this guard, `act_copy_payments` would fail with a cryptic auth error. The Fail activity surfaces the real cause immediately.

---

## Activity 8 — Wait

> Wait pauses the pipeline for a fixed number of seconds. Use it to respect API rate limits before the first data call.

### Steps

1. Drag **Wait** → place between `act_set_token` and `act_set_ingestion_date`
2. Rename to `act_wait_api_cooldown`
3. **Wire it:** `act_set_token` → `act_wait_api_cooldown` → `act_set_ingestion_date`
4. **Settings tab:**
   - **Wait time in seconds:** `3`
5. **Publish all** → **Debug**
6. Monitor → click `act_wait_api_cooldown` → observe 3-second duration in the run timeline
7. **Output** tab:
   ```json
   { "waitTimeInSeconds": 3 }
   ```

**When to use Wait:**
- API enforces a rate limit (e.g., 5 req/s) → place Wait inside a ForEach between iterations
- Upstream system needs time to flush data before you read it
- Testing: simulate a delay to verify timeout policy behaviour

Maximum Wait time: 604,800 seconds (7 days). For long waits use the activity-level `Timeout` policy instead.

---

## Activity 9 — Delete

> Delete removes files or folders from ADLS Gen2. Use it to clean up a previous run's output before writing fresh data.

### Steps

#### 9.1 — Create a cleanup dataset

1. **Author** → **Datasets** → **+ New dataset** → **Azure Data Lake Storage Gen2** → **JSON**
2. Name: `ds_bronze_payments_cleanup`
3. Linked service: `ls_adls_bronze`
4. File path: Container `bronze`, Directory `api/payments`, File `payments.json`
5. **Publish all**

#### 9.2 — Add Delete at the start of the pipeline

1. Drag **Delete** → place as the first activity (before the health check)
2. Rename to `act_cleanup_old_file`
3. **Wire it:** `act_cleanup_old_file` → `act_health_check` (On Success)
4. **Settings tab:**
   - **Dataset:** `ds_bronze_payments_cleanup`
   - **Recursive:** OFF (delete only the specified file, not subfolders)
   - **Max concurrent connections:** 1
   - **Enable logging:** ON → configure a log path to track what was deleted
5. **Publish all** → **Debug**
6. Monitor → click `act_cleanup_old_file` → **Output** tab:
   ```json
   { "filesDeleted": 1, "filesSkipped": 0 }
   ```
   If the file did not exist: `filesDeleted: 0, filesSkipped: 1` — the activity still **Succeeded**.

**Warning:** Delete is irreversible in ADLS Gen2 unless soft delete is enabled on the storage account. Enable it before using this activity in production.

---

## Activity 10 — Lookup

> Lookup reads all rows from a source and returns them as an in-memory array. Use it to load a config file that drives the rest of the pipeline.

### Steps

#### 10.1 — Create the config file

1. Create `endpoint_config.json` locally:
   ```json
   [
     { "table_name": "payments",  "endpoint": "/api/db/payments/",  "active": true },
     { "table_name": "sessions",  "endpoint": "/api/db/sessions/",  "active": true },
     { "table_name": "customers", "endpoint": "/api/db/customers/", "active": false }
   ]
   ```
2. Portal → **Storage accounts** → your ADLS account → **Containers** → `bronze`
3. Create folder `config` → upload `endpoint_config.json`

#### 10.2 — Create a dataset for the config file

1. **Author** → **Datasets** → **+ New dataset** → **Azure Data Lake Storage Gen2** → **JSON**
2. Name: `ds_endpoint_config`
3. Linked service: `ls_adls_bronze`
4. File path: `bronze` / `config` / `endpoint_config.json`
5. **Publish all**

#### 10.3 — Add Lookup to the pipeline

1. Drag **Lookup** → place at the very start (no dependency — it can run in parallel with the Key Vault reads)
2. Rename to `act_lookup_endpoints`
3. **Settings tab:**
   - **Source dataset:** `ds_endpoint_config`
   - **First row only:** **No** (return all rows)
4. **Publish all** → **Debug**
5. Monitor → click `act_lookup_endpoints` → **Output** tab:
   ```json
   {
     "count": 3,
     "value": [
       { "table_name": "payments",  "endpoint": "/api/db/payments/",  "active": true },
       { "table_name": "sessions",  "endpoint": "/api/db/sessions/",  "active": true },
       { "table_name": "customers", "endpoint": "/api/db/customers/", "active": false }
     ]
   }
   ```

**Referencing Lookup results downstream:**
- All rows: `@activity('act_lookup_endpoints').output.value`
- Row at index 1: `@activity('act_lookup_endpoints').output.value[1].endpoint`
- First-row-only mode: `@activity('act_lookup_endpoints').output.firstRow`

---

## Activity 11 — Execute Pipeline

> Execute Pipeline calls another pipeline as a child. Use it to keep the main pipeline lean and make sub-flows reusable and independently testable.

### Steps

#### 11.1 — Create the child pipeline

1. **Author** → **Pipelines** → **+** → **New pipeline**
2. Name: `pl_notify_success`
3. **Parameters** tab → add `p_run_date` (String)
4. Drag **Web Activity** → rename to `act_log_to_api`
5. Settings: URL = `https://ev-project-navy-mu.vercel.app/api/health/`, Method = GET
6. **Publish all**

#### 11.2 — Add Execute Pipeline to the main pipeline

1. Open `pl_bronze_api_payments` → drag **Execute Pipeline** → place after `act_verify_bronze_exists`
2. Rename to `act_notify_success`
3. **Wire it:** `act_verify_bronze_exists` → `act_notify_success` (On Success)
4. **Settings tab:**
   - **Invoked pipeline:** `pl_notify_success`
   - **Wait on completion:** **Yes**
   - **Parameters** → `p_run_date` = `@variables('v_ingestion_date')`
5. **Publish all** → **Debug**
6. Monitor → click `act_notify_success` → **Input** tab to see parameters; click the child pipeline run link to see it execute

**Wait on completion: Yes vs No:**

| Setting | Behaviour | Use when |
|---|---|---|
| Yes | Parent waits for child to finish | Child writes audit data the parent depends on |
| No | Parent fires child and moves on | Child sends a notification the parent does not need |

---

## Activity 12 — Filter

> Filter takes an array and returns only elements where a condition is true. Use it after Lookup to skip inactive endpoints before ForEach.

### Steps

1. Drag **Filter** → place after `act_lookup_endpoints`
2. Rename to `act_filter_active`
3. **Wire it:** `act_lookup_endpoints` → `act_filter_active` (On Success)
4. **Settings tab:**
   - **Items** → **Add dynamic content**:
     ```
     @activity('act_lookup_endpoints').output.value
     ```
   - **Condition** → **Add dynamic content**:
     ```
     @equals(item().active, true)
     ```
5. **Publish all** → **Debug**
6. Monitor → click `act_filter_active` → **Output** tab:
   ```json
   {
     "value": [
       { "table_name": "payments",  "endpoint": "/api/db/payments/",  "active": true },
       { "table_name": "sessions",  "endpoint": "/api/db/sessions/",  "active": true }
     ],
     "filterCount": 2
   }
   ```
   The customer endpoint (`active: false`) is excluded.

**Passing the filtered result to ForEach:**
```
@activity('act_filter_active').output.value
```

---

## Activity 13 — ForEach

> ForEach loops over an array and runs inner activities for each element — sequentially or in parallel.

### Steps

1. Drag **ForEach** → place after `act_filter_active`
2. Rename to `act_loop_endpoints`
3. **Wire it:** `act_filter_active` → `act_loop_endpoints` (On Success)
4. **Settings tab:**
   - **Items** → **Add dynamic content**:
     ```
     @activity('act_filter_active').output.value
     ```
   - **Is Sequential:** `false`
   - **Batch count:** `2` (2 endpoints copy in parallel at a time)

#### 13.1 — Build the ForEach body

1. Click the **pencil icon** on `act_loop_endpoints` to open the inner canvas
2. Drag **Copy data** → rename to `act_copy_endpoint`
3. **Source tab:**
   - **Source dataset:** `ds_voltgrid_payments_src`
   - **Dataset properties:** `p_page` = `1`, `p_page_size` = `100`
   - Open the source dataset → **Connection** tab → Relative URL → update to:
     ```
     @concat(dataset().p_endpoint, '?page=', dataset().p_page, '&page_size=', dataset().p_page_size)
     ```
   - Add dataset parameter `p_endpoint` (String) → back in Copy source, set `p_endpoint` = `@item().endpoint`
   - **Additional headers** → `Authorization` = `@concat('Token ', variables('v_token'))`
4. **Sink tab:** `ds_bronze_payments_sink`
5. **Publish all** → **Debug**

**Monitor view:**
```
act_lookup_endpoints              Succeeded
act_filter_active                 Succeeded
act_loop_endpoints                Succeeded
  └─ act_copy_endpoint (payments) Succeeded  ← parallel
  └─ act_copy_endpoint (sessions) Succeeded  ← parallel
```

**Inside ForEach, `@item()` = the current element:**
- Iteration 1: `@item().table_name` = `"payments"`, `@item().endpoint` = `"/api/db/payments/"`
- Iteration 2: `@item().table_name` = `"sessions"`, `@item().endpoint` = `"/api/db/sessions/"`

---

## Activity 14 — Until

> Until loops until a boolean condition becomes true. Use it for pagination when you do not know the total page count upfront.

### Steps

#### 14.1 — Add pagination variables

1. Open `pl_bronze_api_payments` → **Variables** tab → add:
   - `v_current_page` | Type: `String` | Default: `1`
   - `v_temp_page` | Type: `String`
   - `v_has_more` | Type: `Boolean` | Default: `true`

#### 14.2 — Initialize the counter

1. Drag **Set Variable** → place after `act_set_token` → rename to `act_init_page`
2. **Settings:** Variable = `v_current_page`, Value = `1`
3. **Wire it:** `act_set_token` → `act_init_page`

#### 14.3 — Add the Until activity

1. Drag **Until** → place after `act_init_page` → rename to `act_paginate`
2. **Wire it:** `act_init_page` → `act_paginate`
3. **Settings** → **Expression** → **Add dynamic content**:
   ```
   @not(variables('v_has_more'))
   ```
   Loop stops when `v_has_more` becomes `false`.

#### 14.4 — Build the Until body

Click the pencil on `act_paginate`. Add these activities inside:

**A — Copy one page:**
- Drag **Copy data** → rename to `act_copy_page`
- Source: `ds_voltgrid_payments_src`, `p_page` = `@variables('v_current_page')`, `p_page_size` = `100`
- Source additional headers: `Authorization` = `@concat('Token ', variables('v_token'))`
- Sink: `ds_bronze_payments_sink`

**B — Check if more pages exist:**
- Drag **If Condition** → rename to `act_check_more` → depends on `act_copy_page`
- Expression:
  ```
  @greater(activity('act_copy_page').output.rowsCopied, 0)
  ```
- **True branch** (rows copied — increment page):
  - **Set Variable** → rename to `act_set_temp` → Variable: `v_temp_page` → Value:
    ```
    @string(add(int(variables('v_current_page')), 1))
    ```
  - **Set Variable** → rename to `act_inc_page` → depends on `act_set_temp` → Variable: `v_current_page` → Value:
    ```
    @variables('v_temp_page')
    ```
- **False branch** (0 rows — no more data):
  - **Set Variable** → rename to `act_stop_loop` → Variable: `v_has_more` → Value: `false`

5. **Publish all** → **Debug** (use `p_page_size = 200` so it finishes in 1–2 iterations)
6. Monitor → expand `act_paginate` → see each iteration listed

**Why two Set Variable activities for the counter?**
ADF throws a runtime error if you read and write the same variable in one expression (`v_current_page = add(variables('v_current_page'), 1)`). Fix: write the incremented value to `v_temp_page` first, then copy it to `v_current_page`.

---

## Activity 15 — Switch

> Switch routes to one of N named branches based on a string expression. Cleaner than nested If Conditions when you have 3+ cases on the same value.

### Steps

#### 15.1 — Add a pipeline parameter

1. Open `pl_bronze_api_payments` → **Parameters** tab → **+ New**:
   - Name: `p_load_type` | Type: `String` | Default: `full`

#### 15.2 — Add the Switch activity

1. Drag **Switch** → place after `act_set_ingestion_date`, before `act_copy_payments`
2. Rename to `act_route_load_type`
3. **Wire it:** `act_set_ingestion_date` → `act_route_load_type` → `act_copy_payments`
4. **Settings** → **On** → **Add dynamic content**:
   ```
   @pipeline().parameters.p_load_type
   ```

#### 15.3 — Configure the cases

Click **+ Add case** twice, then configure Default:

**Case `full`:**
- Click pencil → drag **Set Variable** → rename to `act_set_full_date`
- Variable: `v_ingestion_date` | Value: `@formatDateTime(utcnow(), 'yyyy-MM-dd')`

**Case `incremental`:**
- Click pencil → drag **Set Variable** → rename to `act_set_incr_date`
- Variable: `v_ingestion_date` | Value: `@formatDateTime(addDays(utcnow(), -1), 'yyyy-MM-dd')`

**Default:**
- Drag **Fail** → rename to `act_fail_bad_load_type`
- Message:
  ```
  @concat('Unknown load type: ', pipeline().parameters.p_load_type, '. Expected: full or incremental')
  ```
- Error code: `BAD_LOAD_TYPE`

#### 15.4 — Test all three cases

- Debug → `p_load_type = full` → `full` case runs → `v_ingestion_date` = today
- Debug → `p_load_type = incremental` → `incremental` case runs → `v_ingestion_date` = yesterday
- Debug → `p_load_type = historical` → Default fires → Fail with `BAD_LOAD_TYPE` in Monitor

**Monitor view for a `full` run:**
```
act_set_ingestion_date    Succeeded
act_route_load_type       Succeeded
  └─ act_set_full_date    Succeeded   ← 'full' branch
act_copy_payments         Succeeded
```

---

## Final Pipeline Shape (All Activities Combined)

After working through all 15 sections, the conceptual flow is:

```
act_cleanup_old_file
        │
act_health_check
        │
act_lookup_endpoints ──► act_filter_active ──► act_loop_endpoints (ForEach per endpoint)
        │
act_get_username ──┐
                   ├──► act_api_login ──► act_guard_token ──► act_set_token
act_get_password ──┘                                               │
                                                       act_wait_api_cooldown
                                                               │
                                                       act_init_page ──► act_paginate (Until per page)
                                                               │                └─ act_copy_page
                                                       act_set_ingestion_date
                                                               │
                                                       act_route_load_type (Switch)
                                                               │
                                                       act_copy_payments
                                                               │
                                                       act_record_row_count (Append Variable)
                                                               │
                                                       act_check_bronze_folder (Get Metadata)
                                                               │
                                                       act_verify_bronze_exists (If Condition)
                                                               │
                                                       act_notify_success (Execute Pipeline)
```

> In a real pipeline you would not add every activity at once. Build and test each section individually, then keep what your use case needs.

---

## Quick Verification Checklist

After each Debug run, open Monitor and check:

| Activity | What to verify |
|---|---|
| Copy Data | `rowsCopied > 0` in Output tab |
| Web Activity | `statusCode: 200` in Output tab |
| Set Variable | Correct value shown in Output tab |
| Append Variable | Array has one new element added |
| Get Metadata | `exists: true`, `childItems` not empty |
| If Condition | Correct branch (True or False) ran |
| Fail | Custom message and error code visible in Monitor |
| Wait | Duration matches the configured seconds |
| Delete | `filesDeleted: 1` in Output tab |
| Lookup | `count` matches config file row count |
| Execute Pipeline | Child pipeline status = Succeeded |
| Filter | `filterCount` = number of active items only |
| ForEach | One child activity run listed per array element |
| Until | Loop stopped when condition became true (not timed out) |
| Switch | Only the matching case branch ran |
