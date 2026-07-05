# ADF Pipeline Notes — v3 Payments API → Bronze
**Day 3 | Auto Watermark via Delta Lake Lookup + Notebook Audit Write**

---

## Important — ADF Databricks Linked Service Limitation

> ADF's **Azure Databricks Delta Lake** linked service works with **Spark clusters only** (All-Purpose or Job clusters).
> It does **NOT** support SQL Warehouses — the SQL Warehouse dropdown is not shown in this linked service type.
>
> **Correct architecture for v3:**
> - Watermark read → **Lookup Activity** using `ls_databricks_cluster` (Delta Lake linked service → `dev-cluster`)
> - Audit write → **Notebook Activity** running `nb_write_audit` on `dev-cluster`
>
> SQL Warehouses are for BI tools (Power BI, Tableau) and JDBC/ODBC connections — not for ADF pipeline activities.

---

## What Changed from v2 → v3

| | v2 | v3 |
|---|---|---|
| Watermark input | Manual `p_watermark` parameter every incremental run | Automatic — Lookup Activity queries `pipeline_audit` Delta table |
| Audit write | None | Notebook Activity (`nb_write_audit`) writes one row after every run |
| Notebooks required | 0 | 1 (`nb_write_audit`) uploaded to Databricks workspace |
| Extra linked service | None | `ls_databricks_cluster` — Delta Lake linked service pointing to `dev-cluster` |
| Parameters | `p_load_type` + `p_watermark` | `p_load_type` only |

---

## Files to Set Up

### Step 1 — Upload the audit notebook to Databricks

1. Open your Databricks workspace
2. Left sidebar → **Workspace** → **Shared** → create folder `adf_pipelines`
3. Click **Import** → upload `notebooks/nb_write_audit.py` from this directory
4. Confirm notebook is at path: `/Shared/adf_pipelines/nb_write_audit`

### Step 2 — Create `ls_databricks_cluster` linked service in ADF

1. ADF Studio → **Manage** → **Linked services** → **+ New**
2. Search `Azure Databricks` → select **Azure Databricks** (not Delta Lake) → **Continue**
3. Fill in:
   - **Name:** `ls_databricks_cluster`
   - **Azure subscription:** select yours
   - **Databricks workspace:** `dbw-ev-intelligence-dev`
   - **Select cluster:** `Existing interactive cluster`
   - **Existing cluster ID:** select `dev-cluster` from the dropdown
   - **Authentication:** Access token → Azure Key Vault → `ls_keyvault` → secret `databricks-pat-token`
4. **Test connection** → **Connection successful**
5. **Create** → **Publish all**

### Step 3 — Paste datasets and pipeline into ADF

**Paste order:**

| Step | File | Paste into |
|---|---|---|
| 1 | `ds_voltgrid_payments_src_v3.json` | Author → Datasets |
| 2 | `ds_bronze_payments_sink_v3.json` | Author → Datasets |
| 3 | `ds_pipeline_audit_src.json` | Author → Datasets |
| 4 | `pl_bronze_api_payments_v3.json` | Author → Pipelines |

> ADF Studio: Author → Dataset or Pipeline → `{ }` Code button → select all → paste → OK → Publish all

> `ds_pipeline_audit_sink.json` is no longer needed — audit write is now done by the notebook, not a Copy Activity.

---

## Pipeline Flow — v3

```
pl_bronze_api_payments_v3
│
│  Parameter: p_load_type  ("full" | "incremental")
│
├── act_get_username        WebActivity  — Key Vault → voltgrid-username (MSI)
├── act_get_password        WebActivity  — Key Vault → voltgrid-password (MSI)
├── act_api_login           WebActivity  — POST /api/auth/login/ → token
├── act_set_token           SetVariable  — v_token = token
├── act_set_ingestion_date  SetVariable  — v_ingestion_date = today (yyyy-MM-dd)
│
├── act_get_watermark       Lookup Activity  ← reads pipeline_audit Delta table
│     Linked service : ls_databricks_cluster  (dev-cluster, Delta Lake type)
│     Dataset        : ds_pipeline_audit_src
│     Query (full):
│       SELECT '1900-01-01T00:00:00Z' AS last_watermark
│     Query (incremental):
│       SELECT COALESCE(MAX(watermark_value), '1900-01-01T00:00:00Z') AS last_watermark
│       FROM dbw_ev_intelligence_dev.default.pipeline_audit
│       WHERE pipeline_name = 'pl_bronze_api_payments_v3'
│         AND status = 'succeeded'
│     Output: activity('act_get_watermark').output.firstRow.last_watermark
│
├── act_set_watermark       SetVariable  — v_watermark = firstRow.last_watermark
│
├── act_get_total_pages     WebActivity  — GET /api/db/payments/?page=1&updated_after={v_watermark}
├── act_set_total_pages     SetVariable  — v_total_pages = pagination.total_pages
│
├── act_paginate            Until loop (exits when v_current_page > v_total_pages)
│     ├── act_copy_payments_page   Copy Activity
│     │     Source: ds_voltgrid_payments_src_v3
│     │     Sink:   ds_bronze_payments_sink_v3
│     │             bronze/api/payments/raw/ingestion_date={date}/page_{n}.json
│     ├── act_set_temp_page        SetVariable — v_temp_page = v_current_page + 1
│     └── act_increment_page       SetVariable — v_current_page = v_temp_page
│
├── act_set_status_success  SetVariable — v_status = "succeeded"  (on loop Succeeded)
├── act_set_status_failed   SetVariable — v_status = "failed"     (on loop Failed)
│
└── act_write_audit         Notebook Activity  ← always runs (success AND failure)
      Linked service : ls_databricks_cluster  (dev-cluster)
      Notebook path  : /Shared/adf_pipelines/nb_write_audit
      Parameters passed:
        pipeline_name   = "pl_bronze_api_payments_v3"
        load_type       = p_load_type
        watermark_value = v_watermark
        ingestion_date  = v_ingestion_date
        total_pages     = v_total_pages
        status          = v_status
        pipeline_run_id = pipeline().RunId
```

---

## Audit Table

**Table:** `dbw_ev_intelligence_dev.default.pipeline_audit`
**Created by:** `nb_write_audit` on first run (`saveAsTable` with `append` mode auto-creates it)

**Schema:**

| Column | Type | Description |
|---|---|---|
| `pipeline_name` | STRING | `pl_bronze_api_payments_v3` |
| `load_type` | STRING | `full` or `incremental` |
| `watermark_value` | STRING | `updated_after` value used this run |
| `ingestion_date` | STRING | Bronze partition date (`yyyy-MM-dd`) |
| `total_pages` | INT | Pages fetched this run |
| `status` | STRING | `succeeded` or `failed` |
| `pipeline_run_id` | STRING | ADF RunId GUID — links to ADF Monitor |
| `run_timestamp` | TIMESTAMP | UTC time this row was written |

**Query from Databricks:**
```sql
SELECT load_type, watermark_value, ingestion_date, total_pages, status, run_timestamp
FROM dbw_ev_intelligence_dev.default.pipeline_audit
WHERE pipeline_name = 'pl_bronze_api_payments_v3'
ORDER BY run_timestamp DESC
LIMIT 20;
```

---

## How Incremental Load Advances Automatically

```
Run 1 — Full load  (p_load_type = full)
  act_get_watermark  → SELECT '1900-01-01T00:00:00Z' AS last_watermark
  v_watermark        = '1900-01-01T00:00:00Z'
  API fetches ALL records
  nb_write_audit     → writes row: watermark_value='1900-01-01T00:00:00Z', status='succeeded'

  [Manual step once after full load — update watermark to actual max timestamp:]
  UPDATE dbw_ev_intelligence_dev.default.pipeline_audit
  SET    watermark_value = (
    SELECT MAX(updated_at) FROM delta.`abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/`
    LATERAL VIEW explode(data) AS updated_at
    WHERE ingestion_date = '<your ingestion_date>'
  )
  WHERE  pipeline_name = 'pl_bronze_api_payments_v3'
    AND  status = 'succeeded' AND load_type = 'full';

Run 2 — Incremental
  act_get_watermark  → returns '2026-07-04T09:43:00Z'  (from audit table)
  API fetches only records updated after that timestamp
  nb_write_audit     → writes row: watermark_value='2026-07-04T09:43:00Z', status='succeeded'

Run 3+ — Each run picks up exactly where the last succeeded run left off
```

> Day 8 (Orchestration) will automate the watermark update step.

---

## Trigger Setup

### First run — Full load (manual)
| Parameter | Value |
|---|---|
| `p_load_type` | `full` |

### Daily scheduled run — Incremental
```
ADF Studio → pl_bronze_api_payments_v3 → Add trigger → New/Edit
  Type       : Schedule
  Recurrence : Every 1 Day at 01:00 UTC
  Parameters : p_load_type = incremental
```

---

## Linked Services Required

| Linked Service | Type | Used by |
|---|---|---|
| `ls_keyvault` | Azure Key Vault | KV Web Activities (from Day 2) |
| `ls_voltgrid_api` | REST | Source dataset (from Day 2) |
| `ls_adls_bronze` | ADLS Gen2 | Sink dataset (from Day 2) |
| `ls_databricks_cluster` | Azure Databricks | `act_get_watermark` Lookup + `act_write_audit` Notebook |

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `act_get_watermark` fails: linked service not found | `ls_databricks_cluster` not created | Create it in ADF Manage → Linked services (Step 2 above) |
| `act_get_watermark` fails: table not found | `pipeline_audit` table does not exist yet | Run a full load first — `nb_write_audit` creates the table on first run |
| `act_write_audit` fails: notebook not found | Notebook not uploaded to Databricks | Upload `nb_write_audit.py` to `/Shared/adf_pipelines/` in Databricks workspace |
| `act_write_audit` fails: cluster terminated | `dev-cluster` auto-terminated | Databricks → Compute → start `dev-cluster` before triggering, or set cluster to not auto-terminate |
| `act_get_username` 403 | ADF MI missing `Key Vault Secrets User` | Portal → Key Vault → IAM → assign role, wait 2 min |
| `act_api_login` 401 | Wrong credentials | Check `voltgrid-username` and `voltgrid-password` in Key Vault |
| Until loop runs only once | `v_total_pages` stayed at 1 | Monitor → `act_get_total_pages` output → confirm `pagination.total_pages` key exists |
| Incremental fetches all records | Watermark not updated after full load | Run the UPDATE SQL above after full load completes |
