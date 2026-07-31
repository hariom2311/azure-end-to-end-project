# Day 16 — Databricks Job Setup: Gold Stream
**Notebook:** `01_gold_charging_progress_live_mart`  
**Job role:** Reads Silver Delta every 120 seconds → 5-min window aggregation → MERGE → Gold Delta

---

## Prerequisites

- `bronze-stream` and `silver-stream` tasks already in `job-ev-streaming-pipeline`
- `gold` container exists in `evdatalakedev` ADLS (create if missing — see below)
- Silver has at least one batch of data in `silver/sl_vehicle_battery_live/`

---

## Complete 3-Task Job Layout

After Day 16, the job looks like this:

```
job-ev-streaming-pipeline
│
├── bronze-stream   [runs forever]   Bronze writes every 30s
├── silver-stream   [runs forever]   Silver reads Bronze every 60s
└── gold-stream     [runs forever]   Gold reads Silver every 120s

All 3 start simultaneously. No dependencies between tasks.
```

---

## Step 1 — Ensure Gold Container Exists

1. Azure Portal → `evdatalakedev` → **Storage browser** → **Blob containers**
2. Check if `gold` container exists
3. If not: **+ Add container** → name: `gold` → **Create**

---

## Step 2 — Open the Existing Job

1. Databricks → **Workflows** → `job-ev-streaming-pipeline`
2. Click **Tasks** tab
3. You should see `bronze-stream` and `silver-stream` already present

---

## Step 3 — Add the Gold Task

1. Click **+ Add task** → **Notebook**
2. Fill in:

| Field | Value |
|---|---|
| **Task name** | `gold-stream` |
| **Type** | Notebook |
| **Source** | Workspace |
| **Path** | browse to `01_gold_charging_progress_live_mart` |
| **Cluster** | `dev-cluster` |
| **Depends on** | *(leave empty — runs in parallel)* |

3. Click **Save task**

---

## Step 4 — Final Job Graph

Tasks tab should show all 3 tasks with **no arrows** between them:

```
[bronze-stream]    [silver-stream]    [gold-stream]
       ↓                  ↓                 ↓
  (no arrows — all independent, all start together)
```

If any dependency arrows exist, remove them by clicking the task → **Depends on** → clear all.

---

## Step 5 — How the Gold Notebook Stays Running

Cell 7 contains:
```python
while gold_query.isActive:
    time.sleep(120)
```

Gold triggers every 120 seconds. On restart it resumes from:  
`gold/_checkpoints/charging-progress-live/`

The MERGE on `vehicle_id + station_id + window_start` ensures replaying a batch  
produces the same result — no duplicate 5-minute windows.

---

## Step 6 — Run the Complete 3-Task Job

1. Job page → **Run now**
2. All 3 tasks start simultaneously
3. Expected start sequence:
   - `bronze-stream` → first batch at ~30 seconds
   - `silver-stream` → first batch at ~60 seconds (reads what Bronze just wrote)
   - `gold-stream` → first batch at ~120 seconds (reads what Silver just wrote)

**Gold task expected output after 120 seconds:**
```
Secrets loaded successfully.
ADLS OAuth configured: evdatalakedev
Silver:     abfss://silver@evdatalakedev.dfs.core.windows.net/sl_vehicle_battery_live/
Gold mart:  abfss://gold@evdatalakedev.dfs.core.windows.net/mart_charging_progress_live/
Imports done.
build_gold_mart() defined.
Gold stream started. ID: ...
Trigger: 120s | Silver -> 5-min window aggregation -> MERGE -> Gold mart

[Batch 0] Silver rows in: 900 | 5-min windows upserted: 10
[Batch 1] Silver rows in: 900 | 5-min windows upserted: 10
```

10 windows = 10 vehicles (1 active 5-min window per vehicle at any given time).

---

## Step 7 — Verify Gold Mart in ADLS

Azure Portal → `evdatalakedev` → Storage browser → `gold` → `mart_charging_progress_live`

```
mart_charging_progress_live/
├── _delta_log/
└── vehicle_id=VH-0001/
│   └── part-00000-abc.snappy.parquet
├── vehicle_id=VH-0002/
...
└── vehicle_id=VH-0010/
```

---

## Step 8 — Verify via Notebook (optional)

Open a new notebook attached to `dev-cluster`:

```python
STORAGE_ACCOUNT = dbutils.secrets.get(scope='kv-ev-scope', key='adls-account-name')
GOLD_PATH = f'abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/mart_charging_progress_live/'

from pyspark.sql.functions import col
from pyspark.sql.window import Window
import pyspark.sql.functions as F

gold_df = spark.read.format('delta').load(GOLD_PATH)
print(f'Total 5-min windows: {gold_df.count():,}')

# Latest battery % per vehicle
latest_w = Window.partitionBy('vehicle_id').orderBy(col('window_start').desc())
(
    gold_df
    .withColumn('_rn', F.row_number().over(latest_w))
    .filter(col('_rn') == 1)
    .select('vehicle_id', 'station_id', 'window_start',
            'max_battery_pct', 'avg_charging_rate_kw',
            'overtemp_flag', 'min_est_minutes_to_full')
    .orderBy('vehicle_id')
    .show(truncate=False)
)
```

---

## Complete Pipeline: End-to-End Data Flow

```
Local machine
  send_vehicle_battery_events.py
  └── 10 events/sec → Azure Event Hubs
                              │
                              ▼  every 30s
                       Bronze Delta
                       bronze/event-stream/vehicle_battery_live/
                              │
                              ▼  every 60s
                       Silver Delta  (DQ + dedup + MERGE)
                       silver/sl_vehicle_battery_live/
                       silver/quarantine/vehicle_battery_invalid/
                              │
                              ▼  every 120s
                       Gold Delta  (5-min windows + MERGE)
                       gold/mart_charging_progress_live/
                              │
                              ├──▶ Power BI Live Charging dashboard
                              └──▶ Cosmos DB session_live collection
                                   (mobile app <2 sec reads)
```

---

## Monitoring the Running Job

**Workflows → `job-ev-streaming-pipeline` → Runs tab**

```
Run #1  Started: 2026-07-31 22:04  Status: Running
  ├── bronze-stream   ● Running   Duration: 3h 42m
  ├── silver-stream   ● Running   Duration: 3h 41m
  └── gold-stream     ● Running   Duration: 3h 40m
```

Click any task → **Spark UI** → see live streaming query stats  
Click any task → **Logs** → see `[Batch N]` output lines

---

## How to Stop the Pipeline

**Stop all 3 streams:**
Workflows → `job-ev-streaming-pipeline` → active run → **Cancel run**

This cancels all 3 tasks. Each streaming query stops cleanly, checkpoint is preserved.  
Next **Run now** resumes all 3 from their checkpoints — no data loss, no duplicates.

---

## Cost for This Setup

| Resource | Cost |
|---|---|
| `dev-cluster` running 24/7 (Standard_D4ds_v4) | ~₹500–600/day |
| Event Hub namespace (Basic 1 TU) | ~₹0.37/day |
| ADLS Gen2 storage (Bronze + Silver + Gold) | ~₹1–2/day |
| **Total per day (all 3 streams running)** | **~₹500–600/day** |

> For a learning project, only run the pipeline when actively testing.  
> Cancel the job and terminate `dev-cluster` when done to avoid cost.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `[Batch 0] Empty — skipping` for many minutes | Silver hasn't written yet — wait for Silver to complete its first batch (60s after start) |
| `gold container not found` | Create `gold` container in ADLS portal |
| Gold row count stays at 0 | Check Silver has rows: query `silver/sl_vehicle_battery_live/` in a separate notebook |
| `window_start` null in Gold rows | `event_ts` format mismatch — confirm Bronze events have `event_ts` in ISO-8601 format |
| MERGE fails `AnalysisException` | Schema mismatch between batch and existing Gold table — delete Gold path and checkpoint, restart |
| Gold shows same 10 rows, never grows | Expected — MERGE updates existing windows. Count grows only when new 5-min windows open |
