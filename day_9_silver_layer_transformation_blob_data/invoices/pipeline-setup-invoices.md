# Pipeline Setup — Silver Invoice Metadata

**Notebook:** `day_9_silver_layer_transformation_blob_data/invoices/03_silver_blob_invoices_job_params_v3.ipynb`

---

## What this pipeline does

Reads the Bronze invoice metadata Delta table (written by `day_6/04_bronze_blob_invoices_pdf.ipynb`), casts types, builds `invoice_date` and `invoice_number`, applies data quality checks, and upserts into the Silver invoice Delta table.

Bronze source: `bronze-volume/invoices/_metadata/` (Delta table, partitioned by `year` / `month` / `day`)

Silver sink: `silver-volume/invoices/` (Delta — MERGE on `invoice_id`)

> **Note:** PDF content (amounts, line items) is NOT parsed here. Silver stores only metadata: `invoice_id`, `invoice_date`, `invoice_number`, `file_size_kb`, `bronze_path`.

---

## Parameter design

Only **one parameter** is passed from ADF. Date params default to **yesterday's UTC date** in the notebook — today's invoices are still being generated when the daily job triggers.

| Parameter | Source | Required | Default | Example override |
|-----------|--------|----------|---------|-----------------|
| `load_type` | ADF `baseParameters` | Yes | `incremental` | `full` |
| `ingestion_year` | ADF `baseParameters` | No | yesterday's UTC year | `2026` |
| `ingestion_month` | ADF `baseParameters` | No | yesterday's UTC month | `06` |
| `ingestion_day` | ADF `baseParameters` | No | yesterday's UTC day | `14` |

**Why yesterday?** When the daily job fires at e.g. 02:00 UTC, today's invoice batch is still being processed. Yesterday's invoices are fully landed in Bronze and safe to process.

For a **regular daily run**: pass only `load_type`. The notebook picks up yesterday's date automatically.

For a **backfill**: pass `ingestion_year`, `ingestion_month`, and `ingestion_day` to target a specific historical day.

---

## Databricks Job setup

### Step 1 — Create a Databricks Job

1. Go to **Databricks workspace → Workflows → Jobs → Create job**
2. Job name: `job_silver_blob_invoices`

### Step 2 — Add a Task

| Field | Value |
|-------|-------|
| Task name | `silver_invoices` |
| Type | `Notebook` |
| Source | `Workspace` |
| Path | `/Repos/<your-repo>/day_9_silver_layer_transformation_blob_data/invoices/03_silver_blob_invoices_job_params_v3` |
| Cluster | Serverless (recommended) or existing cluster |

### Step 3 — Add Parameters

Click **+ Add** under Parameters:

| Key | Value |
|-----|-------|
| `load_type` | `incremental` |

That is the only parameter needed for regular runs. Leave year/month/day blank — the notebook computes yesterday's date automatically.

### Step 4 — Schedule

- Trigger: **Scheduled**
- Frequency: **Daily** at 02:00 UTC — after the Bronze invoice job completes for the prior day
- Timezone: UTC

---

## ADF pipeline setup

### Step 1 — Create the ADF pipeline

1. Open **Azure Data Factory → Author → Pipelines → New pipeline**
2. Name: `pl_silver_blob_invoices`

### Step 2 — Add a Databricks Notebook activity

Drag **Databricks → Notebook** onto the canvas.

**Settings tab:**

| Field | Value |
|-------|-------|
| Azure Databricks linked service | your Databricks linked service |
| Notebook path | `/Repos/<your-repo>/day_9_silver_layer_transformation_blob_data/invoices/03_silver_blob_invoices_job_params_v3` |

**Base parameters tab — click + New:**

| Name | Value |
|------|-------|
| `load_type` | `incremental` |

Do NOT add `ingestion_year`, `ingestion_month`, `ingestion_day` — the notebook defaults to yesterday's UTC date automatically.

### Step 3 — Add a Trigger

1. **Add trigger → New/Edit → New**
2. Type: **Schedule**
3. Recurrence: Every **1 Day**
4. Start time: `02:00 AM UTC` (after Bronze invoice job completes)
5. Timezone: **(UTC) Coordinated Universal Time**

### Step 4 — Dependency on Bronze

Make sure the Bronze invoice ingestion pipeline runs **before** this pipeline so yesterday's metadata is in the Bronze Delta table.

Recommended order:
```
pl_bronze_blob_invoices  →  pl_silver_blob_invoices
(runs at 01:00 UTC)           (runs at 02:00 UTC)
```

Or chain them sequentially in a single ADF pipeline.

---

## Full load (one-time or reset)

Trigger the pipeline manually with parameter override:

1. **Trigger now → Parameters**
2. Set `load_type` = `full`
3. Leave year/month/day blank

This overwrites the entire Silver invoice table with all Bronze metadata records.

---

## Backfill a specific day

Trigger manually with:

| Name | Value |
|------|-------|
| `load_type` | `incremental` |
| `ingestion_year` | `2026` |
| `ingestion_month` | `07` |
| `ingestion_day` | `10` |

---

## Silver output locations

| Table | Delta path |
|-------|-----------|
| Silver invoices | `silver-volume/invoices/` |
| Quarantine | `silver-volume/quarantine/invoices/` |
