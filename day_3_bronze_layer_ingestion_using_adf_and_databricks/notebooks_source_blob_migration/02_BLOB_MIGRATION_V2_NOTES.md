# Blob Migration v2 Notes — Hourly Scheduled Job
**Day 3 | Source Blob → Bronze Volume | Databricks Job every hour**

---

## What Changed from v1 → v2

| | v1 | v2 |
|---|---|---|
| **Trigger** | Manual — run notebook by hand | Databricks Job — cron `0 * * * *` (top of every hour) |
| **Partition selection** | You edit `LOAD_YEAR`, `LOAD_MONTH`, `LOAD_DAY`, `LOAD_HOUR` manually | Auto-computed from `datetime.now(UTC)` at runtime |
| **Full load** | `LOAD_MODE = "full"` in Cell 2 | `FULL_LOAD_OVERRIDE = True` in Cell 2 (one-off, then set back to False) |
| **Bronze folder creation** | Auto-created by `dbutils.fs.cp` when file is copied | Same — `YYYY/MM/DD/HH/` hierarchy is created automatically on Bronze Volume during copy |
| **Missing source hour** | Crashes with `Path does not exist` if source folder absent | Checks source folder exists first — logs warning and exits cleanly if source has no data for that hour |
| **Idempotency** | Overwrites if re-run same hour | Same — overwrite is safe, re-running produces identical result |
| **Failure alerting** | No | Job marks run Failed + sends email alert if copy errors occur |

---

## How the Hourly Schedule Works

The Job fires at the **top of every hour** (cron `0 * * * *`). At that point the previous hour has fully completed — the source system finishes writing its CSV before the next hour boundary. The notebook therefore targets the **previous hour**, not the current one.

### 3-Hour Look-Back Window

Instead of loading only the previous hour, each run checks the **3 most recently completed hours**. This handles late-arriving data automatically:

```
Job fires at 09:00 UTC → look-back window = [08, 07, 06]

For each hour in the window:
  ┌─ Check 1: Is it already in Bronze?
  │     YES → SKIP (already loaded on a previous run)
  │     NO  → continue
  └─ Check 2: Does the source folder exist?
        NO  → SKIP (data not arrived yet — will retry on next run's window)
        YES → COPY to Bronze

Example:
  Hour 08 → source exists, Bronze empty  → COPY   ← primary target (prev hour)
  Hour 07 → source exists, Bronze empty  → COPY   ← late data catch-up
  Hour 06 → Bronze already has data      → SKIP
```

**Late arriving data scenario:**
```
Run at 08:00 → checks [07, 06, 05]
  Hour 07 source not found yet (late) → SKIP

Run at 09:00 → checks [08, 07, 06]
  Hour 07 source now available        → COPY  ← automatically caught up
```

Each run handles 1–3 hours (only what's actually missing). 24 runs per day.

---

## Prerequisites

Same as v1 — these must exist before scheduling the Job:

| Requirement | Where set up |
|---|---|
| `kv-ev-scope` Databricks secret scope | Day 1 Part 6.5 |
| `source-storage-account` secret in KV | Day 1 — blob storage account name |
| `source-container` secret in KV | Day 1 — container name (`source`) |
| `source-sas-token` secret in KV | Day 1 — SAS token with `sp=rl` (read + list) |
| Bronze Volume exists | Day 2 — `05_UNITY_CATALOG_EXTERNAL_LOCATIONS.md` Part 5 |
| `dev-cluster` running and attached to Unity Catalog | Databricks Compute |

---

## Part A — Upload the Notebook to Databricks

1. Open your Databricks workspace
2. Left sidebar → **Workspace** → **Shared**
3. Click **⋮** → **Create** → **Folder** → name it `bronze_ingestion` (or open existing `adf_pipelines` folder)
4. Inside the folder → click **⋮** → **Import**
5. Select **File** → upload `02_bronze_blob_charging_sessions_v2.ipynb`
6. Confirm notebook appears at: `/Shared/bronze_ingestion/02_bronze_blob_charging_sessions_v2`

> The Job will reference this path. If you upload to a different location, update the notebook path in Part B Step 5.

---

## Part B — Create the Databricks Job

### Step 1 — Open Workflows

1. Left sidebar → **Workflows**
2. Click **+ Create job**

---

### Step 2 — Name the Job

3. At the top, click the default name (`New job`) → rename it to:
   ```
   job_bronze_charging_sessions_hourly
   ```

---

### Step 3 — Configure Task 1

You land on the **Tasks** tab with one task pre-created.

4. **Task name:** `task_copy_hourly`
5. **Type:** select **Notebook**
6. **Source:** select **Workspace**
7. **Path:** browse to `/Shared/bronze_ingestion/02_bronze_blob_charging_sessions_v2`
   *(click the folder icon → navigate and select)*
8. **Cluster:** select your existing `dev-cluster` (All-Purpose cluster)
   > Do NOT use a Job cluster here unless you want cold-start delays every hour. All-Purpose cluster is already warm.

---

### Step 4 — Set the Schedule

9. Click the **Schedules & Triggers** tab (top of the job configuration panel)
10. Click **+ Add schedule**
11. Fill in:
    - **Trigger type:** `Scheduled`
    - **Schedule:** select **Custom cron**
    - **Cron expression:** `0 * * * *`

    > `0 * * * *` = at minute 0 of every hour = 00:00, 01:00, 02:00, ... 23:00 UTC every day.

    - **Timezone:** `UTC` ← important — source folder names use UTC hours
12. Click **Save**

---

### Step 5 — Configure Email Alerts (Recommended)

13. Click the **Notifications** tab
14. Under **On failure** → **+ Add notification** → enter your email address
15. Under **On success** → optionally add email (useful for first few runs to confirm it's working)
16. Click **Save**

> You will receive an email if any run fails (copy errors, assertion mismatch, authentication failure).

---

### Step 6 — Save and Activate

17. Click **Save job** (top right)
18. Toggle the job status from **Paused** to **Active**
    - Look for the status toggle at the top of the job page
    - Active = job will fire on schedule
    - Paused = job is saved but will not fire

---

### Step 7 — Verify the Schedule

19. On the job page, under **Schedules & Triggers**, confirm:
    - Status: **Active**
    - Next run time shown (e.g. `2026-07-06 10:00:00 UTC`)

---

## Part C — Run a Full Load First (One-Time)

Before the scheduled hourly runs begin, you need to copy all historical data into Bronze.

### Step 1 — Open the notebook in Databricks

1. Workspace → `/Shared/bronze_ingestion/02_bronze_blob_charging_sessions_v2`

### Step 2 — Set full load override

2. In **Cell 2**, change:
   ```python
   FULL_LOAD_OVERRIDE = False
   ```
   to:
   ```python
   FULL_LOAD_OVERRIDE = True
   ```

### Step 3 — Run all cells

3. Click **Run all** (top toolbar)
4. Wait for all cells to complete — this may take several minutes depending on total file count

### Step 4 — Verify output

5. Cell 6 will assert that Bronze file count matches source file count
6. Cell 8 prints a summary — check `Files copied` count matches what you expect

### Step 5 — Reset to incremental for the Job

7. In **Cell 2**, set back to:
   ```python
   FULL_LOAD_OVERRIDE = False
   ```
8. **Do not run the notebook again** — the Job will handle it from here

> If you forget to reset `FULL_LOAD_OVERRIDE` and the Job fires with it set to `True`, it will re-copy all historical files — safe but slow. Reset and the next run will be correct.

---

## Part D — Trigger a Manual Test Run

Before waiting for the next scheduled hour, trigger a run manually to confirm the Job is configured correctly.

1. Databricks → **Workflows** → `job_bronze_charging_sessions_hourly`
2. Click **Run now** (top right)
3. Click into the run that appears under **Active runs** or **Completed runs**
4. Watch Cell by Cell output — all cells should show green checkmarks
5. Confirm Cell 8 summary shows:
   - `Files copied: 1` (or however many files exist for the current hour)
   - `Files failed: 0`

> If the current hour folder does not exist yet in the source blob, Cell 3 will exit with:
> `WARNING: Source folder not found — ... Data may not have arrived yet. Exiting.`
> This is expected and the run is marked Succeeded. Wait until the source system writes the file, then run again.

---

## Part E — Monitor Scheduled Runs

### View run history

1. Databricks → **Workflows** → `job_bronze_charging_sessions_hourly`
2. Click **Run history** tab
3. Each row = one Job run — click into any row to see cell-by-cell output

### What a healthy run looks like (all 3 hours need copying)

```
Cell 2: Job fire time (UTC): 2026-07-06 09:00:12 UTC
        Look-back window (3 hours): [08, 07, 06]

Cell 3:   QUEUE for copy : INCREMENTAL — 2026/07/06/08
          QUEUE for copy : INCREMENTAL — 2026/07/06/07
          QUEUE for copy : INCREMENTAL — 2026/07/06/06
        Hours queued: 3

Cell 5: --- INCREMENTAL — 2026/07/06/08 ---
          COPIED  sessions_20260706_0800.csv  → Result: 1 copied, 0 failed
        --- INCREMENTAL — 2026/07/06/07 ---
          COPIED  sessions_20260706_0700.csv  → Result: 1 copied, 0 failed
        --- INCREMENTAL — 2026/07/06/06 ---
          COPIED  sessions_20260706_0600.csv  → Result: 1 copied, 0 failed
        Total: 3 copied, 0 failed

Cell 8: Hours copied: 3 | Hours skipped: 0 | Files copied: 3
          [COPIED] INCREMENTAL — 2026/07/06/08
          [COPIED] INCREMENTAL — 2026/07/06/07
          [COPIED] INCREMENTAL — 2026/07/06/06
```

### What a normal steady-state run looks like (prev hour already loaded, one late)

```
Cell 3:   SKIP (already in Bronze) : INCREMENTAL — 2026/07/06/08
          QUEUE for copy           : INCREMENTAL — 2026/07/06/07  ← late data caught up
          SKIP (already in Bronze) : INCREMENTAL — 2026/07/06/06
        Hours queued: 1

Cell 8: Hours copied: 1 | Hours skipped: 2 | Files copied: 1
          [SKIPPED] INCREMENTAL — 2026/07/06/08
          [COPIED]  INCREMENTAL — 2026/07/06/07
          [SKIPPED] INCREMENTAL — 2026/07/06/06
```

### What a nothing-to-do exit looks like (not a failure)

```
Cell 3:   SKIP (already in Bronze) : INCREMENTAL — 2026/07/06/08
          SKIP (source not found)  : INCREMENTAL — 2026/07/06/07
          SKIP (already in Bronze) : INCREMENTAL — 2026/07/06/06
        Hours queued: 0
        INFO: Nothing to copy — all hours in window already loaded or source data not yet available.
Run status: Succeeded
```

---

## Part F — Verify Bronze Volume Contents

From any Databricks notebook or the SQL editor:

```python
# List all hours loaded today
display(dbutils.fs.ls(
    "/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/2026/07/06/"
))

# Read the latest hour's CSV
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/2026/07/06/09/")

display(df.limit(10))
```

---

## Notebook Cell Reference

| Cell | What it does | Needed for scheduled Job? |
|---|---|---|
| Cell 1 | Authenticate to source blob via Key Vault secrets | Yes — always |
| Cell 2 | Build 3-hour look-back window from `datetime.now(UTC) - 1h` | Yes — always |
| Cell 3 | Filter window: skip hours already in Bronze OR missing from source | Yes — always |
| Cell 4 | List source files for each queued hour | Yes — always |
| Cell 5 | Copy files to Bronze for each queued hour (auto-creates `YYYY/MM/DD/HH/` folders) | Yes — main operation |
| Cell 6 | Assert Bronze file count matches source per hour | Yes — failure triggers Job alert |
| Cell 7 | Read sample CSV from first copied hour, print schema | Optional — safe to keep |
| Cell 8 | Print run summary: hours copied vs skipped, total file counts | Yes — visible in Job run output |

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| Cell 1: `Secret does not exist` | Secret name wrong or scope not configured | Check `source-storage-account`, `source-container`, `source-sas-token` in Key Vault and `kv-ev-scope` scope exists |
| Cell 1: `Secret scope not found: kv-ev-scope` | Scope missing on this cluster | Re-create scope in Databricks Settings → Secrets |
| Cell 3: `WARNING: Source folder not found` | Data not yet written by source system | Normal — run will exit cleanly. Wait for next hour or trigger manually after data arrives |
| Cell 5: `FAILED` lines appear | Blob read error or Volume write permission | Check SAS token has `sp=rl`, check ADF MI / cluster service principal has `Storage Blob Data Contributor` on Bronze |
| Cell 6: assertion fails | Partial copy | Check Cell 5 for which files failed, fix permission, re-run |
| Wrong hour loaded | Cluster timezone not UTC | Confirm `timezone.utc` is used in Cell 2 — the code uses `datetime.now(timezone.utc)` explicitly |
| Job not firing | Job status is Paused | Workflows → job → toggle to **Active** |
| Job fires but wrong hour | Cron expression wrong | Should be `0 * * * *` — minute 0, every hour |

---

## Cron Expression Reference

| Expression | Meaning |
|---|---|
| `0 * * * *` | Top of every hour (00:00, 01:00, ... 23:00) |
| `0 */2 * * *` | Every 2 hours |
| `0 9 * * *` | Once a day at 09:00 UTC |
| `30 * * * *` | 30 minutes past every hour |

> For this job use `0 * * * *` — the source system writes one file per hour and the folder is named by the hour boundary.

---

## What Comes Next

Hourly raw CSVs now land in Bronze automatically. The Silver layer notebook (Day 7) will:

1. Read all CSVs from `/Volumes/.../bronze-volume/realtime/charging_sessions/`
2. Apply explicit schema (cast string columns to correct types)
3. Deduplicate by `session_id`
4. Write as Delta table to the Silver Volume

No changes needed to this notebook before that — Silver reads from the same Bronze Volume path.
