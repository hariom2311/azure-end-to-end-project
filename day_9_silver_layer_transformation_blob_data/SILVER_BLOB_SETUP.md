# Day 9 — Silver Layer: Blob Realtime Data Transformation

## Three Notebooks — Learning Progression

| Notebook | Scope | Purpose |
|---|---|---|
| `01_silver_charging_sessions_simple_v1.ipynb` | charging_sessions only | Every step written explicitly — no functions, no loops. Full overwrite. |
| `02_silver_blob_all_entities_forloop_v2.ipynb` | charging_sessions + maintenance_events | Same logic wrapped in a for loop. Full overwrite. |
| `03_silver_blob_all_entities_job_params_v3.ipynb` | charging_sessions + maintenance_events | Production — job parameters only, data quality pipeline, Delta MERGE, attaches to existing Databricks job. |

**Teach in order: v1 → v2 → v3**

---

## Source → Silver data flow

```
Bronze Volume (CSV, hourly partitions)
  realtime/charging_sessions/YYYY/MM/DD/HH/sessions_YYYYMMDD_HHMM.csv
  realtime/maintenance_events/YYYY/MM/DD/HH/maintenance_YYYYMMDD_HHMM.csv

      PySpark reads CSV → cast types → data quality → dedup → Delta MERGE

Silver Volume (Delta tables)
  realtime/charging_sessions/    (Delta — MERGE upsert on session_id)
  realtime/maintenance_events/   (Delta — MERGE upsert on event_id)

Silver Volume (Quarantine)
  quarantine/realtime/charging_sessions/    (rejected rows with reject_reason)
  quarantine/realtime/maintenance_events/
```

---

## Key difference from Day 8 (API data)

| | Day 8 (API JSON) | Day 9 (Blob CSV) |
|---|---|---|
| Bronze format | JSON with `data[]` array wrapper | CSV with header row |
| Read step | `spark.read.json()` + `explode(data[])` | `spark.read.csv(header=true)` |
| Partition structure | `ingestion_date=YYYY-MM-DD/` (flat) | `YYYY/MM/DD/HH/` (hierarchical) |
| Job parameters | `load_type`, `ingestion_date` | `load_type`, `ingestion_year`, `ingestion_month`, `ingestion_day`, `ingestion_hour` |
| No explode needed | No — CSV is already flat rows |

---

## Part A — Upload v3 notebook to Databricks

1. Databricks → **Workspace** → **Shared** → `silver_transformation` folder
2. **Import** → select `03_silver_blob_all_entities_job_params_v3.ipynb`
3. Confirm path:
   ```
   /Shared/silver_transformation/03_silver_blob_all_entities_job_params_v3
   ```

---

## Part B — Attach to existing Databricks Job (job_bronze_realtime_hourly)

The notebook attaches as a **second task** in the existing `job_bronze_realtime_hourly` job so Silver runs automatically after Bronze completes each hour.

### Step 1 — Open the existing Bronze job

1. Databricks → left sidebar → **Workflows** → **Jobs**
2. Click `job_bronze_realtime_hourly`
3. Click **Edit** (or the task canvas area)

### Step 2 — Add Silver as a new task

1. On the task canvas, click **+ Add task** → **Notebook**
2. A new task box appears — connect it after the existing Bronze task with a dependency arrow

### Step 3 — Configure the Silver task

| Field | Value |
|---|---|
| Task name | `silver_blob_transform` |
| Type | Notebook |
| Source | Workspace |
| Path | `/Shared/silver_transformation/03_silver_blob_all_entities_job_params_v3` |
| Cluster | Select your `dev-cluster` |
| Depends on | `<your existing bronze task name>` |
| Timeout | 3600 seconds (1 hour) |
| Retries | 1 |

### Step 4 — Add task parameters

In the task configuration → **Parameters** section → click **+ Add**:

| Key | Value |
|---|---|
| `load_type` | `incremental` |
| `ingestion_year` | `{{job.start_time.iso_date \| date_format: 'yyyy'}}` |
| `ingestion_month` | `{{job.start_time.iso_date \| date_format: 'MM'}}` |
| `ingestion_day` | `{{job.start_time.iso_date \| date_format: 'dd'}}` |
| `ingestion_hour` | `{{job.start_time.iso_date \| date_format: 'HH'}}` |

> These dynamic values inject the job's scheduled run time as the partition to process. Bronze writes `YYYY/MM/DD/HH/` and Silver reads the same partition automatically.

### Step 5 — Save and verify job structure

After saving, the job task graph should look like:

```
job_bronze_realtime_hourly
  │
  ├── task: bronze_ingest    (existing — reads from blob, writes Bronze CSV)
  │
  └── task: silver_blob_transform    (NEW — reads Bronze CSV, writes Silver Delta)
        dependsOn: bronze_ingest [Success]
        Parameters:
          load_type       = incremental
          ingestion_year  = {{run_time.year}}
          ingestion_month = {{run_time.month}}
          ingestion_day   = {{run_time.day}}
          ingestion_hour  = {{run_time.hour}}
```

### Step 6 — Test run

1. Click **Run now** on the job
2. Monitor: Workflows → Job Runs → expand the run
   - Bronze task: Succeeded
   - Silver task: Succeeded
3. Verify Silver output:
   ```python
   SILVER_REALTIME = "/Volumes/dbw_ev_intelligence_dev/default/silver-volume/realtime"
   for entity in ["charging_sessions", "maintenance_events"]:
       df = spark.read.format("delta").load(f"{SILVER_REALTIME}/{entity}")
       print(f"{entity:<25} rows={df.count()}")
   ```

---

## Part C — Run Silver independently (backfill)

If you need to reprocess a specific hour without re-running Bronze:

1. Databricks → **Workflows** → open `job_bronze_realtime_hourly` → **Run now with different parameters**
2. Or trigger just the `silver_blob_transform` task
3. Set parameters:
   | Key | Value |
   |---|---|
   | `load_type` | `incremental` |
   | `ingestion_year` | `2026` |
   | `ingestion_month` | `07` |
   | `ingestion_day` | `15` |
   | `ingestion_hour` | `06` |

---

## Job schedule reference

| Job | Cron | What runs |
|---|---|---|
| `job_bronze_realtime_hourly` | `0 0 * * * ?` (every hour on the hour) | Bronze task: blob CSV → Bronze Volume |
| Silver task (attached) | Runs after Bronze task succeeds | Silver task: Bronze CSV → Silver Delta |

Bronze runs at :00, Silver starts immediately after Bronze completes (~:05–:10 each hour).

---

## Silver Delta table reference

| Entity | Natural Key | CDC Field | Silver Path |
|---|---|---|---|
| charging_sessions | `session_id` | `updated_at` | `.../realtime/charging_sessions/` |
| maintenance_events | `event_id` | `updated_at` | `.../realtime/maintenance_events/` |

---

## Common errors

| Error | Cause | Fix |
|---|---|---|
| `Parameter 'load_type' was not provided` | Notebook run directly without job params | Run via Databricks Job only (v3 is production) |
| `No Bronze CSV files found for given partition` | Bronze task hasn't run yet or partition path wrong | Check: `dbutils.fs.ls(".../bronze-volume/realtime/charging_sessions/YYYY/MM/DD/HH/")` |
| `AnalysisException: Path does not exist` | Silver Volume not created | Create `silver-volume` Volume under `dbw_ev_intelligence_dev.default` (same as Day 8) |
| `silver=0` for numeric entities | Corrupt check was firing on legitimate NULLs | v3 uses pre-cast sentinel fix — this should not happen |
| Silver task shows as Skipped | Bronze task failed | Fix Bronze task failure first — Silver depends on Bronze success |
