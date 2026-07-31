# Day 14 — Databricks Job Setup: Bronze Stream
**Notebook:** `01_stream_vehicle_battery_to_bronze`  
**Job role:** Reads from Azure Event Hubs every 30 seconds → writes raw JSON to Bronze Delta

---

## Why a Job Instead of Running Manually

| Manual run | Databricks Job |
|---|---|
| Stream stops when you close the browser tab | Stream runs 24/7 headlessly |
| No retry if cluster crashes | Auto-restarts from checkpoint on failure |
| No failure notification | Email alert on failure |
| You must be logged in | Fully unattended |

---

## How the Notebook Stays Running

The Bronze notebook's Cell 8 contains:
```python
while streaming_query.isActive:
    time.sleep(30)
```

This loop keeps the notebook (and the Databricks Job) running indefinitely.  
The job never "completes" — it runs until you stop it manually or the stream fails.  
On restart, the streaming query resumes from the checkpoint in ADLS — no events lost.

---

## Step 1 — Create the Job

1. Databricks workspace → left sidebar → **Workflows**
2. Click **+ Create job** (top right)
3. A new job opens with one empty task

---

## Step 2 — Configure the Bronze Task

Fill in the task form:

| Field | Value |
|---|---|
| **Task name** | `bronze-stream` |
| **Type** | Notebook |
| **Source** | Workspace |
| **Path** | Click browse → navigate to your `01_stream_vehicle_battery_to_bronze` notebook |
| **Cluster** | Select existing → `dev-cluster` |
| **Depends on** | *(leave empty)* |

Click **Create task**.

---

## Step 3 — Name the Job

At the top of the page, click the job name field (shows "New job") and rename it:
```
job-ev-streaming-pipeline
```

This is the parent job that will hold all 3 tasks (Bronze + Silver + Gold).  
**Do not create separate jobs for Silver and Gold** — add them as tasks in this same job (done in Day 15 and Day 16 job setup docs).

---

## Step 4 — Configure Job-Level Settings

Click the **Settings** tab on the job page:

### Email notifications
| Event | Email |
|---|---|
| On failure | your email address |
| On start | *(optional)* |

### Trigger
- **Trigger type:** None (manual)  
  *(We run it manually once — the `while` loop inside the notebook keeps it alive forever)*

---

## Step 5 — Set Cluster to Never Auto-Terminate

By default `dev-cluster` terminates after 15 minutes of inactivity.  
A running streaming job IS active — it won't hit the 15-min threshold.  
But to be safe, confirm:

1. Databricks → **Compute** → `dev-cluster` → **Edit**
2. **Terminate after** → set to `120` minutes (or uncheck entirely for production)
3. **Save**

---

## Step 6 — Run the Job

> **Before running the job:** Make sure `send_vehicle_battery_events.py` is running on your local machine sending events to Event Hubs. The Bronze notebook needs events to consume.

1. On the job page → click **Run now** (top right)
2. A new run appears under **Runs** tab
3. Click the run → click the `bronze-stream` task → watch the notebook output

**Expected output after 30 seconds:**
```
Secrets loaded successfully.
ADLS OAuth configured for: evdatalakedev
Bronze path: abfss://bronze@evdatalakedev.dfs.core.windows.net/event-stream/vehicle_battery_live/
Streaming query started. ID: 90838724-f9dc-4f06-8b92-ec206c4ffbdc
Stream is live. Printing progress every 30 seconds.

[Batch 1] Rows this batch: 225 | Input rate: 7.5 rows/sec | Processing rate: 84.7 rows/sec
[Batch 2] Rows this batch: 225 | Input rate: 7.5 rows/sec | Processing rate: 41.6 rows/sec
```

---

## Step 7 — Verify Bronze Data in ADLS

Azure Portal → `evdatalakedev` → Storage browser → `bronze` → `event-stream` → `vehicle_battery_live`

You should see:
```
vehicle_battery_live/
├── _delta_log/
└── event_date=2026-07-31/
    ├── part-00000-abc.snappy.parquet
    └── part-00001-def.snappy.parquet   ← new file every 30 seconds
```

---

## Job Status Reference

| Status | Meaning |
|---|---|
| **Running** | Stream is active, batches processing normally |
| **Succeeded** | Notebook exited cleanly (only happens if you stop the stream manually) |
| **Failed** | Notebook threw an unhandled exception — check logs, fix, re-run |
| **Cancelled** | You stopped it manually |

---

## How to Stop the Bronze Stream

**Option A — Stop just the task:**
Workflows → `job-ev-streaming-pipeline` → Runs → click active run → **Cancel**

**Option B — Stop from inside the notebook:**
Open the notebook → run in any cell:
```python
streaming_query.stop()
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Job fails immediately | Check Cell 1 — Key Vault secret names must match exactly |
| `ClassNotFoundException: EventHubsSourceProvider` | Maven library not installed — see DAY14_STREAMING_EVENTHUB_SETUP.md Part 5 |
| `Py4JError: encrypt does not exist` | Maven library installed but cluster needs restart |
| No rows in Bronze after 2 minutes | Confirm `send_vehicle_battery_events.py` is running locally |
| Job keeps restarting every 15 min | Cluster auto-termination is set too low — increase to 120 min |
