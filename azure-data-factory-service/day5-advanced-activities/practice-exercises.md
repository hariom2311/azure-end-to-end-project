# Day 5 — Practice Exercises: Remaining ADF Activities

> **No external API required** — all exercises use ADLS Gen2 files, inline arrays, or Azure SQL.
> Work through each section independently. Each one demonstrates one activity with exact steps.

---

## Before You Start

You need:
- Access to `adf-datalake-dev-ded` (ADF Studio → Author view)
- A `bronze` container in your ADLS Gen2 account
- Optionally: an Azure SQL Database for Script and WebHook exercises

---

## Exercise 1 — Lookup: Load a Config List from ADLS

**Goal:** Create a JSON config file in ADLS, read it with a Lookup Activity, and verify the output in Monitor.

### Step 1.1 — Upload the config file to ADLS

1. Create `tables.json` locally:
   ```json
   [
     { "table": "payments",  "active": true  },
     { "table": "sessions",  "active": true  },
     { "table": "customers", "active": false }
   ]
   ```
2. Azure Portal → **Storage accounts** → your ADLS account → **Containers** → `bronze`
3. Create folder `config` → upload `tables.json`

### Step 1.2 — Create a dataset pointing to the config file

1. **Author** → **Datasets** → **+ New dataset** → **Azure Data Lake Storage Gen2** → **JSON**
2. Name: `ds_tables_config`
3. Linked service: `ls_adls_bronze`
4. File path: Container `bronze`, Directory `config`, File `tables.json`
5. **Publish all**

### Step 1.3 — Create a demo pipeline and add Lookup

1. **Author** → **Pipelines** → **+** → **New pipeline** → name `pl_day5_demo`
2. Activities panel → **General** → drag **Lookup** → rename to `act_lookup_tables`
3. **Settings tab:**
   - **Source dataset:** `ds_tables_config`
   - **First row only:** **No** (return all rows)
4. **Publish all** → **Debug**
5. Monitor → click `act_lookup_tables` → **Output** tab:
   ```json
   {
     "count": 3,
     "value": [
       { "table": "payments",  "active": true  },
       { "table": "sessions",  "active": true  },
       { "table": "customers", "active": false }
     ],
     "firstRow": { "table": "payments", "active": true }
   }
   ```

**Try First row only: Yes** → Output changes to just `{ "firstRow": { "table": "payments", "active": true } }`

**Key expressions:**
```
All rows:    @activity('act_lookup_tables').output.value
Row count:   @activity('act_lookup_tables').output.count
Row 2:       @activity('act_lookup_tables').output.value[1].table   → "sessions"
```

---

## Exercise 2 — Filter: Keep Only Active Tables

**Goal:** Pass the Lookup result through a Filter to remove inactive entries.

### Steps

1. In `pl_day5_demo` → drag **Filter** → place after `act_lookup_tables`
2. Rename to `act_filter_active`
3. **Wire it:** `act_lookup_tables` → `act_filter_active` (On Success)
4. **Settings tab:**
   - **Items** → **Add dynamic content**:
     ```
     @activity('act_lookup_tables').output.value
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
       { "table": "payments", "active": true },
       { "table": "sessions", "active": true }
     ],
     "filterCount": 2
   }
   ```
   `customers` (active: false) is gone.

**What to verify:** `filterCount` = 2, not 3. The downstream ForEach will only process 2 tables.

---

## Exercise 3 — Switch: Route by Environment

**Goal:** Use Switch to set a schema variable based on a pipeline parameter — no API, no ADLS needed.

### Step 3.1 — Add a parameter and variable

1. Open `pl_day5_demo` → **Parameters** tab → **+ New**: Name `p_env` | Type `String` | Default `dev`
2. **Variables** tab → **+ New**: Name `v_schema` | Type `String`

### Step 3.2 — Add Switch Activity

1. Drag **Switch** → place after `act_filter_active`
2. Rename to `act_route_env`
3. **Wire it:** `act_filter_active` → `act_route_env` (On Success)
4. **Settings** → **On** → **Add dynamic content**:
   ```
   @pipeline().parameters.p_env
   ```

### Step 3.3 — Configure cases

Click **+ Add case** twice, then configure Default:

**Case `dev`:**
- Click pencil → drag **Set Variable** → rename to `act_set_dev_schema`
- Variable: `v_schema` | Value: `stg`

**Case `prod`:**
- Click pencil → drag **Set Variable** → rename to `act_set_prod_schema`
- Variable: `v_schema` | Value: `dbo`

**Default:**
- Drag **Fail** → rename to `act_fail_bad_env`
- Message: `@concat('Unknown environment: ', pipeline().parameters.p_env)`
- Error code: `BAD_ENV`

### Step 3.4 — Test all cases

- **Debug** → `p_env = dev` → Monitor → `act_set_dev_schema` ran → `v_schema = stg`
- **Debug** → `p_env = prod` → Monitor → `act_set_prod_schema` ran → `v_schema = dbo`
- **Debug** → `p_env = uat` → Monitor → `act_fail_bad_env` fired → pipeline failed with `BAD_ENV`

---

## Exercise 4 — ForEach: Loop Over the Filtered List

**Goal:** Loop over the two active tables from the Filter output and log each table name using a Set Variable per iteration (no Copy Activity needed — just observe the loop).

### Steps

1. In `pl_day5_demo` → **Variables** tab → **+ New**: Name `v_last_table` | Type `String`
2. Drag **ForEach** → place after `act_route_env`
3. Rename to `act_loop_tables`
4. **Wire it:** `act_route_env` → `act_loop_tables` (On Success)
5. **Settings tab:**
   - **Items** → **Add dynamic content**:
     ```
     @activity('act_filter_active').output.value
     ```
   - **Is Sequential:** `true` (process one table at a time)
6. Click the **pencil icon** on `act_loop_tables` → inner canvas opens
7. Drag **Set Variable** → rename to `act_track_table`
8. **Settings:**
   - Variable: `v_last_table`
   - Value → **Add dynamic content**: `@item().table`
9. **Publish all** → **Debug**
10. Monitor → expand `act_loop_tables` → two iterations appear:
    ```
    Iteration 1:  act_track_table   Succeeded   (item: payments)
    Iteration 2:  act_track_table   Succeeded   (item: sessions)
    ```
11. Click each iteration → **Input** tab → confirm `item()` value is correct per iteration

**Try changing Is Sequential to `false` and batch count to `2`** → both iterations run simultaneously.

---

## Exercise 5 — Wait: Pause Between Iterations

**Goal:** Add a 3-second Wait inside the ForEach to simulate a delay between processing each table.

### Steps

1. Click the pencil on `act_loop_tables` to re-enter the inner canvas
2. Drag **Wait** → place after `act_track_table`
3. Rename to `act_pause`
4. **Wire it:** `act_track_table` → `act_pause` (On Success)
5. **Settings:** Wait time in seconds: `3`
6. **Publish all** → **Debug**
7. Monitor → expand `act_loop_tables` → each iteration now shows:
   ```
   act_track_table   Succeeded   ~0s
   act_pause         Succeeded   3s
   ```
   Total pipeline time increases by 3s × number of iterations (6s for 2 tables).

---

## Exercise 6 — Script: Run Inline SQL

**Goal:** After the ForEach, write the current run date to an Azure SQL table using inline SQL — no stored procedure needed.

> Skip this exercise if you don't have an Azure SQL Database. The concept is the same as Stored Procedure but with inline SQL.

### Step 6.1 — Prepare Azure SQL

Run in your Azure SQL database:
```sql
CREATE TABLE run_log (
    id           INT IDENTITY PRIMARY KEY,
    pipeline     VARCHAR(200),
    run_date     DATE,
    logged_at    DATETIME DEFAULT GETDATE()
);
```

### Step 6.2 — Add Script Activity

1. In `pl_day5_demo` → drag **Script** → place after `act_loop_tables`
2. Rename to `act_log_run`
3. **Wire it:** `act_loop_tables` → `act_log_run` (On Success)
4. **Settings tab:**
   - **Linked service:** `ls_azure_sql` (create one if not existing: Manage → Linked services → Azure SQL Database)
   - **Script type:** `NonQuery`
   - **Script** → **Add dynamic content**:
     ```
     INSERT INTO run_log (pipeline, run_date)
     VALUES ('@{pipeline().pipelineName}', '@{formatDateTime(utcnow(), 'yyyy-MM-dd')}')
     ```
5. **Publish all** → **Debug**
6. Monitor → `act_log_run` → **Output** tab:
   ```json
   { "resultSetCount": 0, "recordsAffected": 1 }
   ```
7. Verify in Azure SQL:
   ```sql
   SELECT * FROM run_log ORDER BY logged_at DESC;
   ```

**Try Script type: Query to read a value:**

Change Script to:
```sql
SELECT COUNT(*) AS total_runs FROM run_log
```
Output:
```json
{ "resultSets": [ [ { "total_runs": 1 } ] ] }
```
Reference downstream: `@activity('act_log_run').output.resultSets[0][0].total_runs`

---

## Exercise 7 — Until: Loop Until a Condition Is Met

**Goal:** Build a simple counter loop using Until — loop 3 times then stop. No API, no storage needed.

### Step 7.1 — Add counter variables

1. Open `pl_day5_demo` → **Variables** tab → **+ New** for each:
   - `v_count` | Type: `String` | Default: `0`
   - `v_temp`  | Type: `String`

### Step 7.2 — Add Until Activity

1. Drag **Until** → place after `act_log_run` (or after `act_loop_tables` if skipping Exercise 6)
2. Rename to `act_count_to_3`
3. **Wire it:** previous activity → `act_count_to_3` (On Success)
4. **Settings** → **Expression** → **Add dynamic content**:
   ```
   @greaterOrEquals(int(variables('v_count')), 3)
   ```
   Loop stops when `v_count` reaches 3.
5. **Timeout:** `0.00:01:00` (1 minute safety cap)

### Step 7.3 — Build the Until body

Click the pencil on `act_count_to_3`:

**Activity A — Write to temp (increment):**
- Drag **Set Variable** → rename to `act_set_temp`
- Variable: `v_temp` | Value → **Add dynamic content**:
  ```
  @string(add(int(variables('v_count')), 1))
  ```

**Activity B — Copy temp to counter:**
- Drag **Set Variable** → rename to `act_set_count` → depends on `act_set_temp`
- Variable: `v_count` | Value → **Add dynamic content**:
  ```
  @variables('v_temp')
  ```

6. **Publish all** → **Debug**
7. Monitor → expand `act_count_to_3` → 3 iterations appear:
   ```
   Iteration 1: act_set_temp → act_set_count   (v_count becomes 1)
   Iteration 2: act_set_temp → act_set_count   (v_count becomes 2)
   Iteration 3: act_set_temp → act_set_count   (v_count becomes 3 → condition true → loop exits)
   ```

**Why not `v_count = add(variables('v_count'), 1)` directly?**
ADF prohibits reading and writing the same variable in one expression at runtime. The two-variable workaround (`v_temp` then `v_count`) is the standard pattern.

---

## Exercise 8 — Delete: Clean Up a File After Processing

**Goal:** After the pipeline finishes, delete a file from ADLS to simulate post-processing cleanup.

### Step 8.1 — Upload a test file to ADLS

1. Create an empty file called `processed_payments.json` with content `[]`
2. Portal → `bronze` container → create folder `landing` → upload `processed_payments.json`

### Step 8.2 — Create a dataset for the file to delete

1. **Author** → **Datasets** → **+ New dataset** → **Azure Data Lake Storage Gen2** → **JSON**
2. Name: `ds_landing_cleanup`
3. Linked service: `ls_adls_bronze`
4. File path: Container `bronze`, Directory `landing`, File `processed_payments.json`
5. **Publish all**

### Step 8.3 — Add Delete Activity

1. In `pl_day5_demo` → drag **Delete** → place at the end of the pipeline
2. Rename to `act_cleanup_landing`
3. **Wire it:** last activity → `act_cleanup_landing` (On Success)
4. **Settings tab:**
   - **Dataset:** `ds_landing_cleanup`
   - **Recursive:** OFF
5. **Publish all** → **Debug**
6. Monitor → `act_cleanup_landing` → **Output** tab:
   ```json
   { "filesDeleted": 1, "filesSkipped": 0 }
   ```
7. Portal → ADLS → `bronze/landing/` → confirm `processed_payments.json` is gone

**Run Debug again** → Output: `filesDeleted: 0, filesSkipped: 1` — Delete succeeded even though file was missing (no error thrown).

---

## Exercise 9 — WebHook: Fire an Async Call and Wait for Callback (Concept Demo)

> WebHook requires an external service that accepts the call and POSTs back to ADF's callback URL. This exercise shows the configuration steps — test it if you have an external endpoint, or just review the settings.

### Steps (configuration walkthrough)

1. In `pl_day5_demo` → Activities panel → **General** → drag **WebHook** → rename to `act_webhook_notify`
2. **Wire it:** `act_cleanup_landing` → `act_webhook_notify` (On Success)
3. **Settings tab:**
   - **URL:** your external service endpoint (e.g., `https://your-service.com/api/start`)
   - **Method:** POST
   - **Body** → **Add dynamic content**:
     ```
     @concat('{"pipeline":"', pipeline().pipelineName,
              '","callback_url":"', activity('act_webhook_notify').output.callBackUri, '"}')
     ```
   - **Timeout:** `0.00:05:00` (5 minutes)
   - **Report status on callback body:** ON (external service can signal success/failure in its callback)

**What happens at runtime:**
1. ADF POSTs to your URL with the body above
2. Pipeline pauses — `act_webhook_notify` shows "In Progress" in Monitor
3. Your external service does its work
4. Your service POSTs back to `callBackUri` with `{ "statusCode": "200", "Output": { "result": "done" } }`
5. Pipeline resumes — next activity runs

**Testing without a real external service:**
Copy the `callBackUri` from the Monitor output tab while the activity is paused → use Postman or curl to POST back manually:
```
POST <callBackUri>
Body: { "statusCode": "200", "Output": { "result": "done" } }
```
The pipeline will resume immediately.

---

## Final Pipeline Structure (All 8 Activities)

```
act_lookup_tables      (Lookup — reads tables.json)
        │
act_filter_active      (Filter — keeps active:true only)
        │
act_route_env          (Switch — set schema by p_env)
        │
act_loop_tables        (ForEach — sequential loop over 2 tables)
  └─ act_track_table   (Set Variable — @item().table)
  └─ act_pause         (Wait — 3 seconds)
        │
act_log_run            (Script — INSERT into run_log)
        │
act_count_to_3         (Until — loops 3 times using counter)
        │
act_cleanup_landing    (Delete — removes landing file)
        │
act_webhook_notify     (WebHook — fires async callback)
```

---

## Quick Verification Checklist

| Activity | What to check in Monitor |
|---|---|
| Lookup | `count` matches number of rows in config file |
| Filter | `filterCount` = number of rows where condition is true |
| Switch | Only the matching case branch ran; Default fires on unknown value |
| ForEach | One iteration row per array element; sequential = one at a time |
| Wait | Duration matches configured seconds |
| Script | `recordsAffected: 1` for NonQuery; `resultSets` present for Query |
| Until | Correct number of iterations; loop stopped when condition became true |
| Delete | `filesDeleted: 1` on first run; `filesSkipped: 1` if file already gone |
| WebHook | Shows "In Progress" while waiting; resumes after callback |
