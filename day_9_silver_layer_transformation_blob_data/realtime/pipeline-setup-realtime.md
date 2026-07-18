# Pipeline Setup — Silver Realtime Blob Entities

**Notebook:** `day_9_silver_layer_transformation_blob_data/realtime/03_silver_blob_all_entities_job_params_v3.ipynb`

---

## What this pipeline does

Reads Bronze CSV files for two realtime entities written by IoT devices:

| Entity | Natural key | CDC field | Bronze path pattern |
|--------|-------------|-----------|---------------------|
| `charging_sessions` | `session_id` | `updated_at` | `bronze-volume/realtime/charging_sessions/YYYY/MM/DD/HH/` |
| `maintenance_events` | `event_id` | `updated_at` | `bronze-volume/realtime/maintenance_events/YYYY/MM/DD/HH/` |

Steps per entity: read CSV → derive `updated_at` from file path → trim → NULL PK quarantine → NULL CDC quarantine → ANSI-safe cast → negative value quarantine → dedup → Delta MERGE (incremental) or overwrite (full).

---

## Parameter design

Only **one parameter** is passed from ADF. Date/hour default to the **previous UTC hour** in the notebook — current hour's files are still being appended when the job triggers.

| Parameter | Source | Required | Default | Example override |
|-----------|--------|----------|---------|-----------------|
| `load_type` | ADF `baseParameters` | Yes | `incremental` | `full` |
| `ingestion_year` | ADF `baseParameters` | No | previous UTC hour's year | `2026` |
| `ingestion_month` | ADF `baseParameters` | No | previous UTC hour's month | `06` |
| `ingestion_day` | ADF `baseParameters` | No | previous UTC hour's day | `14` |
| `ingestion_hour` | ADF `baseParameters` | No | previous UTC hour | `09` |

**Why previous hour?** When the job fires at e.g. 10:15 UTC, the `HH=10` partition is still receiving device events. The `HH=09` partition is complete and safe to process.

For a **regular scheduled run**: pass only `load_type`. The notebook picks up the completed prior hour automatically.

For a **backfill**: pass all five parameters to target a specific historical partition.

---

## Databricks Job setup

### Step 1 — Create a Databricks Job

1. Go to **Databricks workspace → Workflows → Jobs → Create job**
2. Job name: `job_silver_blob_realtime`

### Step 2 — Add a Task

| Field | Value |
|-------|-------|
| Task name | `silver_realtime` |
| Type | `Notebook` |
| Source | `Workspace` |
| Path | `/Repos/<your-repo>/day_9_silver_layer_transformation_blob_data/realtime/03_silver_blob_all_entities_job_params_v3` |
| Cluster | Serverless (recommended) or existing cluster |

### Step 3 — Add Parameters

Click **+ Add** under Parameters:

| Key | Value |
|-----|-------|
| `load_type` | `incremental` |

That is the only parameter needed for regular runs. Leave year/month/day/hour empty — the notebook computes previous hour automatically.

### Step 4 — Schedule

- Trigger: **Scheduled**
- Frequency: **Hourly** (e.g. at minute 15 past each hour — gives devices 15 min to finish writing the prior hour's files)
- Timezone: UTC

---

## ADF pipeline setup

### Step 1 — Create the ADF pipeline

1. Open **Azure Data Factory → Author → Pipelines → New pipeline**
2. Name: `pl_silver_blob_realtime`

### Step 2 — Add a Databricks Notebook activity

Drag **Databricks → Notebook** onto the canvas.

**Settings tab:**

| Field | Value |
|-------|-------|
| Azure Databricks linked service | your Databricks linked service |
| Notebook path | `/Repos/<your-repo>/day_9_silver_layer_transformation_blob_data/realtime/03_silver_blob_all_entities_job_params_v3` |

**Base parameters tab — click + New:**

| Name | Value |
|------|-------|
| `load_type` | `incremental` |

Do NOT add `ingestion_year`, `ingestion_month`, `ingestion_day`, `ingestion_hour` — the notebook defaults to the previous UTC hour automatically.

### Step 3 — Add a Trigger

1. **Add trigger → New/Edit → New**
2. Type: **Schedule**
3. Recurrence: Every **1 Hour**
4. Start time: set to next `HH:15:00` UTC (15 minutes past the hour)
5. Timezone: **(UTC) Coordinated Universal Time**

### Step 4 — Full load (one-time or reset)

To run a full overwrite of all Silver data, trigger the pipeline manually with parameter override:

1. **Trigger now → Parameters**
2. Set `load_type` = `full`
3. Leave all other parameters blank

---

## Backfill a specific hour

Trigger manually with:

| Name | Value |
|------|-------|
| `load_type` | `incremental` |
| `ingestion_year` | `2026` |
| `ingestion_month` | `07` |
| `ingestion_day` | `10` |
| `ingestion_hour` | `14` |

---

## Silver output locations

| Entity | Silver Delta path |
|--------|------------------|
| `charging_sessions` | `silver-volume/realtime/charging_sessions/` |
| `maintenance_events` | `silver-volume/realtime/maintenance_events/` |
| Quarantine | `silver-volume/quarantine/realtime/<entity>/` |
