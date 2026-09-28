# Day 3 — Practice Exercises: Pipeline Control, Triggers & Monitoring

> All exercises continue directly from the `pl_bronze_api_payments` pipeline built in Day 2. Open ADF Studio → `adf-datalake-dev-ded` before starting.

---

## Exercise 1 — Add Variables and Dynamic Sink Path (25 min)

**Goal:** Replace the fixed `payments.json` sink with a date-partitioned path using variables and dynamic content.

### Task 1.1 — Add variable to the pipeline

1. **Author** → **Pipelines** → `pl_bronze_api_payments`
2. Click anywhere on the empty canvas (deselect all activities)
3. Bottom panel → **Variables** tab
4. **+ New** → Name: `v_ingestion_date` | Type: `String`

You should now have two variables: `v_token` (from Day 2) and `v_ingestion_date`.

### Task 1.2 — Add Set Variable activity for the date

1. From the Activities panel → **General** → drag **Set Variable** onto the canvas
2. Rename it to `act_set_ingestion_date`
3. Connect `act_set_token` → `act_set_ingestion_date` (green On Success arrow)
4. **Settings** tab:
   - Variable name: `v_ingestion_date`
   - Value → **Add dynamic content**:
     ```
     @formatDateTime(utcnow(), 'yyyy-MM-dd')
     ```
5. Click **Debug** on just this activity to check — the variable should get today's date string

### Task 1.3 — Add a parameter to the sink dataset

1. **Author** → **Datasets** → `ds_bronze_payments_sink`
2. **Parameters** tab → **+ New**:
   - Name: `p_run_date` | Type: `String` | Default: leave blank
3. **Connection** tab → **File path** section:
   - Folder: click the folder field → **Add dynamic content**:
     ```
     api/payments/ingestion_date=@{dataset().p_run_date}
     ```
   - File: **Add dynamic content**:
     ```
     page_@{pipeline().parameters.p_page}.json
     ```
4. **Publish all**

### Task 1.4 — Pass the variable to the sink dataset

1. Go back to `pl_bronze_api_payments`
2. Click `act_copy_payments`
3. **Sink** tab → **Dataset properties** (appears after selecting the dataset):
   - `p_run_date` → **Add dynamic content**: `@variables('v_ingestion_date')`
4. Reconnect the dependency: `act_set_ingestion_date` → `act_copy_payments`

### Task 1.5 — Debug and verify

1. Click **Debug** → `p_page = 1`, `p_page_size = 10` → **OK**
2. All 6 activities should succeed
3. Go to Portal → `evdatalakedev` → `bronze` container → navigate to:
   `api/payments/ingestion_date=<today's date>/`
4. Confirm `page_1.json` exists in that folder

**Expected ADLS structure after this exercise:**
```
bronze/
└── api/
    └── payments/
        └── ingestion_date=2026-09-28/
            └── page_1.json
```

---

## Exercise 2 — Explore Dynamic Content Expressions (20 min)

**Goal:** Practice writing ADF expressions using the dynamic content editor. Use the existing pipeline to test them.

### Task 2.1 — Explore system variables

On the `act_set_ingestion_date` Set Variable activity, temporarily change the Value to each of these one at a time. Click **Debug** after each to see the result in the activity output.

| Expression | What you expect to see |
|---|---|
| `@utcnow()` | Full UTC datetime string |
| `@formatDateTime(utcnow(), 'yyyy-MM-dd')` | Just the date: `2026-09-28` |
| `@formatDateTime(utcnow(), 'yyyyMMdd')` | Compact date: `20260928` |
| `@formatDateTime(addDays(utcnow(), -1), 'yyyy-MM-dd')` | Yesterday's date |
| `@pipeline().pipelineName` | `pl_bronze_api_payments` |
| `@pipeline().runId` | A long GUID string |
| `@string(pipeline().parameters.p_page)` | `"1"` (string form of the int parameter) |

After each Debug run: Monitor → Activity runs → `act_set_ingestion_date` → Output tab — the variable value is shown.

### Task 2.2 — Build the Authorization header expression

On `act_copy_payments` → Source tab → Additional headers → `Authorization` value, examine the expression:
```
@concat('Token ', variables('v_token'))
```

Now answer these questions by modifying the expression temporarily and observing Debug output:
1. What happens if you use `'Bearer '` instead of `'Token '`? (Try it — expect 401 from the API)
2. What does `@length(variables('v_token'))` return? (How long is the token?)
3. What does `@substring(variables('v_token'), 0, 8)` return? (First 8 characters)

Restore the correct expression before moving on.

### Task 2.3 — If condition expression

Add a new Set Variable activity `act_set_watermark`:
- Variable: `v_ingestion_date` (reuse it temporarily)
- Value → **Add dynamic content**:
  ```
  @if(equals(pipeline().parameters.p_load_type, 'full'), '1900-01-01T00:00:00Z', utcnow())
  ```
- Add a new pipeline parameter `p_load_type` (String, default `full`) first

Debug with `p_load_type = full` → variable should be `1900-01-01T00:00:00Z`
Debug with `p_load_type = incremental` → variable should be today's UTC datetime

This is the pattern used to implement full vs incremental load switching.

---

## Exercise 3 — Create a Schedule Trigger (15 min)

**Goal:** Set up a daily Schedule Trigger at 01:00 UTC so the pipeline runs automatically.

### Task 3.1 — Create the trigger

1. ADF Studio → **Manage** → **Triggers** → **+ New**
2. Fill in:
   - Name: `trg_payments_daily`
   - Type: `Schedule`
   - Description: `Runs payments ingestion daily at 01:00 UTC`
   - Start date: today's date
   - Start time: `01:00 AM`
   - Time zone: `(UTC) Coordinated Universal Time`
   - Recurrence: Every `1` `Day`
3. Click **Next** → Pipeline parameters:
   - `p_page`: `1`
   - `p_page_size`: `100`
4. Click **OK** → **Publish all**

### Task 3.2 — Activate and verify

1. Manage → Triggers → find `trg_payments_daily` → toggle it to **Active**
2. **Publish all** again (triggers must be published to take effect)

**Verify trigger is active:**
- Manage → Triggers → status column shows **Started**
- Monitor → Trigger runs → filter by `trg_payments_daily` → you'll see it listed (fires at 01:00 UTC)

### Task 3.3 — Trigger Now with trigger parameters

Instead of waiting for 01:00 UTC, test the trigger behaviour manually:
1. Author → `pl_bronze_api_payments` → **Add trigger** → **Trigger now**
2. In the parameters form — note that `p_page`, `p_page_size` are pre-filled from the trigger defaults
3. Change `p_page_size` to `5` for a quick test → **OK**
4. Monitor → watch the run complete
5. Portal → ADLS Gen2 → confirm a new `page_1.json` appeared under today's `ingestion_date=` folder

---

## Exercise 4 — Create a Tumbling Window Trigger (20 min)

**Goal:** Replace the Schedule Trigger with a Tumbling Window Trigger to guarantee every day's data is ingested even if a run fails.

### Task 4.1 — Deactivate the Schedule Trigger

1. Manage → Triggers → `trg_payments_daily` → toggle to **Inactive** → **Publish all**

### Task 4.2 — Create the Tumbling Window Trigger

1. Manage → Triggers → **+ New**
2. Fill in:
   - Name: `trg_payments_tumbling`
   - Type: `Tumbling window`
   - Start: `2026-09-25T01:00:00Z` (3 days ago — ADF will backfill Sep 25, 26, 27)
   - Recurrence: Every `1 Day`
   - Max concurrency: `1`
   - Retry: count `2`, interval `30 minutes`
3. Click **Next** → Parameters:
   - `p_page`: `1`
   - `p_page_size`: `10` (keep small for testing)
   - Add a new parameter `p_run_date`:
     - Value → **Add dynamic content**: `@formatDateTime(trigger().outputs.windowStartTime, 'yyyy-MM-dd')`
4. **OK** → **Publish all** → activate

### Task 4.3 — Observe backfill

1. Monitor → **Trigger runs** → filter by `trg_payments_tumbling`
2. You should see 3 trigger runs queued/running (Sep 25, 26, 27) — all firing automatically because the start date was in the past
3. Monitor → **Pipeline runs** → watch them complete one by one (concurrency = 1)
4. Portal → ADLS Gen2 → bronze container → `api/payments/` → confirm folders for `ingestion_date=2026-09-25`, `2026-09-26`, `2026-09-27` all exist

> If `p_run_date` was passed to the pipeline, each dated folder is correct for its window. This is the power of Tumbling Window — automatic backfill with correct dates.

---

## Exercise 5 — Monitor, Diagnose, and Rerun (15 min)

**Goal:** Practice using the Monitor panel to diagnose a failure and rerun a specific pipeline run.

### Task 5.1 — Force a failure (controlled break)

1. Go to `ls_voltgrid_api` → edit → change Base URL to `https://ev-project-navy-mu-BROKEN.vercel.app`
2. **Publish all**
3. **Trigger Now** on `pl_bronze_api_payments` with `p_page=1`, `p_page_size=5`
4. Watch the run fail

### Task 5.2 — Diagnose in Monitor

1. Monitor → Pipeline runs → find the failed run (red X)
2. Click it → Activity runs
3. Find the first failed activity — note which one it is
4. Click the failed activity → **Error** tab → read the full error message
5. Click the failed activity → **Input** tab → confirm the URL being called

**Questions to answer:**
- Which activity failed first?
- What was the HTTP status code?
- Was the token obtained before the failure, or did the failure happen before login?

### Task 5.3 — Fix and rerun

1. Go back to `ls_voltgrid_api` → restore correct URL: `https://ev-project-navy-mu.vercel.app`
2. **Publish all**
3. Monitor → Pipeline runs → find the failed run → click **Rerun**
4. Confirm it succeeds this time

---

## Exercise 6 — Set Up Email Alert on Pipeline Failure (20 min)

**Goal:** Receive an email whenever any ADF pipeline in `adf-datalake-dev-ded` fails.

### Task 6.1 — Create an Action Group

1. Portal → **Monitor** (search in top bar) → left menu → **Alerts** → **Action groups**
2. **+ Create**
3. Fill in:
   - Subscription: yours
   - Resource group: `rg-ev-intelligence-dev`
   - Action group name: `ag-adf-failures`
   - Display name: `ADF Failures`
4. **Notifications** tab → **+ Add notification**:
   - Notification type: `Email/SMS message/Push/Voice`
   - Name: `email-team`
   - Click the pencil → check **Email** → enter your email → **OK**
5. **Review + Create** → **Create**

### Task 6.2 — Create an Alert Rule

1. Portal → **Monitor** → **Alerts** → **+ Create** → **Alert rule**
2. **Scope** tab → **Select scope**:
   - Search `adf-datalake-dev-ded` → select it → **Done**
3. **Condition** tab → **+ Add condition**:
   - Search and select: `Failed pipeline runs count`
   - Threshold value: `0`
   - Operator: `Greater than`
   - Aggregation type: `Total`
   - Check every: `5 minutes`
   - Lookback period: `5 minutes`
4. **Actions** tab → **+ Select action groups** → select `ag-adf-failures` → **Select**
5. **Details** tab:
   - Alert rule name: `alert-adf-pipeline-failure`
   - Severity: `2 - Warning`
   - Enable upon creation: checked
6. **Review + Create** → **Create**

### Task 6.3 — Test the alert

1. Repeat the controlled break from Exercise 5.1 (wrong URL → Trigger Now → let it fail)
2. Wait up to 5 minutes
3. Check your email inbox for the Azure Monitor alert notification
4. Fix the URL → Publish all
5. Go to Portal → Monitor → Alerts → **Alert rules** → `alert-adf-pipeline-failure` → **Edit** to confirm it's active

### Task 6.4 (Bonus) — Web Activity On Failure inside the pipeline

1. Open `pl_bronze_api_payments`
2. Drag a **Web Activity** onto the canvas → name it `act_alert_failure`
3. Connect `act_copy_payments` → `act_alert_failure`:
   - Hover over `act_copy_payments` → drag the arrow out → drop on `act_alert_failure`
   - In the dialog → select **Failure** → **OK** (arrow should turn red)
4. **Settings** tab:
   - Method: `POST`
   - URL: `https://ev-project-navy-mu.vercel.app/api/auth/login/` ← placeholder (replace with real Teams webhook if available)
   - Headers: `Content-Type` = `application/json`
   - Body:
     ```
     @concat('{"text":"Pipeline FAILED: ', pipeline().pipelineName, ' | RunId: ', pipeline().runId, ' | Time: ', utcnow(), '"}')
     ```
5. **Publish all**

**Observe:** Run the pipeline with the broken URL again. The `act_copy_payments` fails → `act_alert_failure` fires (the POST goes to the placeholder — it will get a response but that's fine for demonstration). In Monitor you'll see `act_alert_failure` ran under the `On Failure` condition even though the pipeline overall failed.
