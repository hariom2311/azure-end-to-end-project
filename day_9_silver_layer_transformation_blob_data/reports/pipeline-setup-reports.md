# Pipeline Setup — Silver Monthly Reports

**Notebook:** `day_9_silver_layer_transformation_blob_data/reports/03_silver_blob_reports_job_params_v3.ipynb`

---

## What this pipeline does

Reads three monthly report JSON files from Bronze, flattens their nested schemas, applies data quality checks, and writes to three separate Silver Delta tables.

| Report | Bronze file | Silver path | MERGE key |
|--------|-------------|-------------|-----------|
| `kpi_report` | `kpi_report_YYYYMM.json` | `silver-volume/reports/kpi_report/` | `report_period` |
| `sla_report` | `sla_report_YYYYMM.json` | `silver-volume/reports/sla_report/` | `report_period` |
| `state_breakdown` | `state_breakdown_YYYYMM.json` | `silver-volume/reports/state_breakdown/` | `report_period` + `state_code` |

Bronze source pattern: `bronze-volume/reports/YYYY/MM/<filename>.json`

> **Note:** Reports are monthly — one run per month is the normal cadence. The notebook processes all three report files in a single execution.

---

## Parameter design

Only **one parameter** is passed from ADF. Year and month default to the **previous UTC month** in the notebook — monthly report files are generated at month-end for the prior month, so the job always processes last month's data.

| Parameter | Source | Required | Default | Example override |
|-----------|--------|----------|---------|-----------------|
| `load_type` | ADF `baseParameters` | Yes | `incremental` | `full` |
| `ingestion_year` | ADF `baseParameters` | No | previous UTC month's year | `2026` |
| `ingestion_month` | ADF `baseParameters` | No | previous UTC month | `06` |

**Why previous month?** When the job runs on e.g. July 1st, the July report files don't exist yet — only the June report files are complete and available in Bronze. The notebook automatically computes the prior month using `now.replace(day=1) - 1 day`.

For a **regular monthly run**: pass only `load_type`. The notebook picks up the previous month automatically.

For a **backfill**: pass `ingestion_year` and `ingestion_month` to reprocess a specific historical month.

---

## Databricks Job setup

### Step 1 — Create a Databricks Job

1. Go to **Databricks workspace → Workflows → Jobs → Create job**
2. Job name: `job_silver_blob_reports`

### Step 2 — Add a Task

| Field | Value |
|-------|-------|
| Task name | `silver_reports` |
| Type | `Notebook` |
| Source | `Workspace` |
| Path | `/Repos/<your-repo>/day_9_silver_layer_transformation_blob_data/reports/03_silver_blob_reports_job_params_v3` |
| Cluster | Serverless (recommended) or existing cluster |

### Step 3 — Add Parameters

Click **+ Add** under Parameters:

| Key | Value |
|-----|-------|
| `load_type` | `incremental` |

That is the only parameter needed for regular runs. Leave year/month blank — the notebook computes the previous UTC month automatically.

### Step 4 — Schedule

- Trigger: **Scheduled**
- Frequency: **Monthly** — 1st of each month at 03:00 UTC (after Bronze report generation is complete)
- Timezone: UTC

---

## ADF pipeline setup

### Step 1 — Create the ADF pipeline

1. Open **Azure Data Factory → Author → Pipelines → New pipeline**
2. Name: `pl_silver_blob_reports`

### Step 2 — Add a Databricks Notebook activity

Drag **Databricks → Notebook** onto the canvas.

**Settings tab:**

| Field | Value |
|-------|-------|
| Azure Databricks linked service | your Databricks linked service |
| Notebook path | `/Repos/<your-repo>/day_9_silver_layer_transformation_blob_data/reports/03_silver_blob_reports_job_params_v3` |

**Base parameters tab — click + New:**

| Name | Value |
|------|-------|
| `load_type` | `incremental` |

Do NOT add `ingestion_year` or `ingestion_month` — the notebook defaults to the previous UTC month automatically.

### Step 3 — Add a Trigger

1. **Add trigger → New/Edit → New**
2. Type: **Schedule**
3. Recurrence: Every **1 Month** on the **1st day**
4. Start time: `03:00 AM UTC`
5. Timezone: **(UTC) Coordinated Universal Time**

### Step 4 — Dependency on Bronze

The Bronze report files (`kpi_report_YYYYMM.json`, `sla_report_YYYYMM.json`, `state_breakdown_YYYYMM.json`) must exist before this pipeline runs. The notebook does a pre-flight check and raises an error immediately if any file is missing.

Recommended order:
```
pl_bronze_blob_reports   →  pl_silver_blob_reports
(runs at 02:00 UTC, 1st)      (runs at 03:00 UTC, 1st)
```

Or chain them sequentially in a single ADF pipeline.

---

## What happens if a Bronze file is missing

Cell 7 of the notebook checks all three source files before processing any. If any is missing the run fails immediately with no partial writes:

```
Exception: 1 report file(s) missing in Bronze.
Run day_6/05_bronze_blob_reports_json.ipynb first for 2026/06.
```

---

## Full load (one-time or reset)

Trigger the pipeline manually with parameter override:

1. **Trigger now → Parameters**
2. Set `load_type` = `full`
3. Leave year/month blank (processes previous month's files)

This overwrites all three Silver report tables completely.

---

## Backfill a specific month

Trigger manually with:

| Name | Value |
|------|-------|
| `load_type` | `incremental` |
| `ingestion_year` | `2026` |
| `ingestion_month` | `06` |

The notebook will look for `kpi_report_202606.json`, `sla_report_202606.json`, `state_breakdown_202606.json` in `bronze-volume/reports/2026/06/`.

---

## Silver output locations

| Table | Delta path |
|-------|-----------|
| KPI report | `silver-volume/reports/kpi_report/` |
| SLA report | `silver-volume/reports/sla_report/` |
| State breakdown | `silver-volume/reports/state_breakdown/` |
| Quarantine (KPI) | `silver-volume/quarantine/reports/kpi_report/` |
| Quarantine (SLA) | `silver-volume/quarantine/reports/sla_report/` |
| Quarantine (states) | `silver-volume/quarantine/reports/state_breakdown/` |
