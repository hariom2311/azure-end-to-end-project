# Day 15 — Databricks Job Setup: Silver Stream
**Notebook:** `01_silver_vehicle_battery_transformation`  
**Job role:** Reads Bronze Delta every 60 seconds → DQ + dedup + MERGE → Silver Delta

---

## Prerequisites

- Day 14 job task `bronze-stream` already added to `job-ev-streaming-pipeline`
- `silver` container exists in `evdatalakedev` ADLS (create if missing — see DAY15_SILVER_STREAMING_SETUP.md Step 1)

---

## How This Task Fits in the Pipeline

```
job-ev-streaming-pipeline
├── bronze-stream   (Day 14 — already added)   ← Bronze writes every 30s
├── silver-stream   (Day 15 — add now)         ← Silver reads Bronze every 60s
└── gold-stream     (Day 16 — add in Day 16)
```

All 3 tasks run **in parallel** — Silver does not wait for Bronze to finish  
(Bronze never finishes — it runs forever). Silver reads from the Bronze Delta  
table independently; it sees whatever Bronze has written so far.

---

## Step 1 — Open the Existing Job

1. Databricks → **Workflows**
2. Click on `job-ev-streaming-pipeline` (created in Day 14)
3. Click the **Tasks** tab

You should see `bronze-stream` already there.

---

## Step 2 — Add the Silver Task

1. Click **+ Add task** → **Notebook**
2. Fill in:

| Field | Value |
|---|---|
| **Task name** | `silver-stream` |
| **Type** | Notebook |
| **Source** | Workspace |
| **Path** | browse to `01_silver_vehicle_battery_transformation` |
| **Cluster** | `dev-cluster` |
| **Depends on** | *(leave empty — runs in parallel with bronze-stream)* |

3. Click **Save task**

---

## Step 3 — Confirm Parallel Layout

On the Tasks tab the graph should look like:

```
[bronze-stream]     [silver-stream]
      ↓                   ↓
   (no arrows between them — both start at the same time)
```

If you see an arrow from `bronze-stream` → `silver-stream`, remove the dependency:  
click `silver-stream` task → **Depends on** → remove `bronze-stream`.

---

## Step 4 — How the Silver Notebook Stays Running

The Silver notebook's Cell 7 contains:
```python
while silver_query.isActive:
    time.sleep(60)
```

This keeps the job task running indefinitely — same pattern as Bronze.  
On cluster restart, Silver resumes from its checkpoint:  
`silver/_checkpoints/vehicle-battery-live/`

---

## Step 5 — Run the Updated Job

1. Job page → **Run now**
2. Both `bronze-stream` and `silver-stream` tasks start simultaneously
3. Click `silver-stream` task → watch output

**Expected output after 60 seconds (first batch):**
```
Secrets loaded successfully.
ADLS OAuth configured: evdatalakedev
Bronze:     abfss://bronze@evdatalakedev.dfs.core.windows.net/event-stream/vehicle_battery_live/
Silver:     abfss://silver@evdatalakedev.dfs.core.windows.net/sl_vehicle_battery_live/
Quarantine: abfss://silver@evdatalakedev.dfs.core.windows.net/quarantine/vehicle_battery_invalid/
Imports done.
transform_to_silver() defined.
Silver stream started. ID: ...
Trigger: 60s | Bronze -> DQ (9 rules) + dedup + MERGE -> Silver

[Batch 0] In: 450 | Clean merged: 450 | Quarantined: 0
[Batch 1] In: 450 | Clean merged: 450 | Quarantined: 0
```

---

## Step 6 — Verify Silver in ADLS

Azure Portal → `evdatalakedev` → Storage browser → `silver` → `sl_vehicle_battery_live`

```
sl_vehicle_battery_live/
├── _delta_log/
│   ├── 00000000000000000000.json    ← table creation
│   └── 00000000000000000001.json    ← first MERGE
└── event_date=2026-07-31/
    └── part-00000-abc.snappy.parquet
```

Also check quarantine:
```
silver/quarantine/vehicle_battery_invalid/   ← should be empty (all events pass DQ)
```

---

## Step 7 — Verify via Notebook (optional)

Open a new notebook attached to `dev-cluster` and run:

```python
STORAGE_ACCOUNT = dbutils.secrets.get(scope='kv-ev-scope', key='adls-account-name')
SILVER_PATH = f'abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/sl_vehicle_battery_live/'

df = spark.read.format('delta').load(SILVER_PATH)
print(f'Silver rows: {df.count():,}')
df.groupBy('vehicle_id').count().orderBy('vehicle_id').show()
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `[Batch 0] Empty batch — skipping` for many batches | Bronze hasn't written yet — wait 30s for Bronze to land its first batch |
| `AnalysisException: Path does not exist` on Bronze path | Bronze stream hasn't started — start Bronze task first, wait 30s, then start Silver |
| `silver container not found` | Create `silver` container in ADLS — Azure Portal → `evdatalakedev` → Storage browser → + Add container |
| All rows going to quarantine | Check DQ rules — likely `_is_corrupt = True` means Bronze JSON schema mismatch |
| Silver row count not growing | Check checkpoint — if corrupted: `dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)` then restart |
| MERGE fails with `ConcurrentAppendException` | Two Silver tasks running simultaneously — ensure only one Silver task in the job |
