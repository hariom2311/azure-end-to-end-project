# ADF Pipeline Notes — v3 Payments API → Bronze
**Day 3 | Auto Watermark via Databricks SQL Warehouse — Zero Notebooks**

---

## What Changed from v2 → v3

| | v2 | v3 |
|---|---|---|
| Watermark input | Manual `p_watermark` parameter every incremental run | Automatic — Lookup Activity queries `pipeline_audit` Delta table |
| Audit write | None | Copy Activity writes one row to `pipeline_audit` after every run |
| Notebooks required | 0 | 0 |
| Extra Azure resources | None | None — uses existing Databricks SQL Warehouse |
| Parameters | `p_load_type` + `p_watermark` | `p_load_type` only |

---

## Files to Paste into ADF

**Paste order — linked service first, then datasets, then pipeline:**

| Step | File | Paste into |
|---|---|---|
| 1 | `ls_databricks_sql` — create manually in ADF Manage tab | Manage → Linked services |
| 2 | `ds_voltgrid_payments_src_v3.json` | Author → Datasets |
| 3 | `ds_bronze_payments_sink_v3.json` | Author → Datasets |
| 4 | `ds_pipeline_audit_src.json` | Author → Datasets |
| 5 | `ds_pipeline_audit_sink.json` | Author → Datasets |
| 6 | `pl_bronze_api_payments_v3.json` | Author → Pipelines |

> ADF Studio: Author → Dataset or Pipeline → `{ }` Code button (top right) → select all → delete → paste → OK → Publish all

---

## Step 1 — Create `ls_databricks_sql` Linked Service

This is the only manual step. ADF needs a linked service that connects to your Databricks SQL Warehouse endpoint.

### In ADF Studio (UI)

1. ADF Studio → **Manage** → **Linked services** → **+ New**
2. Search `Azure Databricks Delta Lake` → **Continue**
3. Fill in:
   - **Name:** `ls_databricks_sql`
   - **Domain:** your Databricks workspace URL — `https://adb-XXXXXXXXXXXXXXXX.X.azuredatabricks.net`
   - **Cluster type:** Existing interactive cluster OR SQL Warehouse
   - **SQL Warehouse / Cluster ID:**
     - For SQL Warehouse: Databricks → SQL Warehouses → your warehouse → Connection details → HTTP Path (e.g. `/sql/1.0/warehouses/abc123`)
     - For existing cluster: Databricks → Compute → your cluster → Advanced → Tags → Cluster ID
   - **Authentication:** Managed Identity (recommended) OR Access Token
     - Managed Identity: ADF MI must have `Contributor` or `Can Restart` permission on Databricks workspace
     - Access Token: Databricks → User Settings → Developer → Access tokens → Generate → store in Key Vault → reference here
4. Click **Test connection** → **Connection successful**
5. Click **Create**

### How to find your SQL Warehouse HTTP Path

```
Databricks workspace
  → SQL Warehouses (left sidebar)
  → Select your warehouse (or create one — Serverless tier is cheapest)
  → Connection details tab
  → Copy "HTTP path"  e.g.  /sql/1.0/warehouses/abc1234567890abc
```

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
├── act_get_watermark       Lookup Activity  ← reads from Delta table via SQL Warehouse
│     Dataset : ds_pipeline_audit_src
│     Query (full load):
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
│                                          reads pagination.total_pages
├── act_set_total_pages     SetVariable  — v_total_pages = total_pages
│
├── act_paginate            Until loop (exits when v_current_page > v_total_pages)
│     ├── act_copy_payments_page   Copy Activity
│     │     Source: ds_voltgrid_payments_src_v3
│     │             GET /api/db/payments/?page={n}&page_size=100&updated_after={v_watermark}
│     │             Authorization: Token {v_token}
│     │     Sink:   ds_bronze_payments_sink_v3
│     │             bronze/api/payments/raw/ingestion_date={v_ingestion_date}/page_{n}.json
│     ├── act_set_temp_page        SetVariable — v_temp_page = v_current_page + 1
│     └── act_increment_page       SetVariable — v_current_page = v_temp_page
│
├── act_set_status_success  SetVariable — v_status = "succeeded"  (on loop success)
├── act_set_status_failed   SetVariable — v_status = "failed"     (on loop failure)
│
└── act_write_audit         Copy Activity  ← writes to Delta table via SQL Warehouse
      Source: ds_pipeline_audit_src (inline SELECT query builds the audit row)
        SELECT
          'pl_bronze_api_payments_v3' AS pipeline_name,
          '{p_load_type}'             AS load_type,
          '{v_watermark}'             AS watermark_value,
          '{v_ingestion_date}'        AS ingestion_date,
           {v_total_pages}            AS total_pages,
          '{v_status}'                AS status,
          '{pipeline().RunId}'        AS pipeline_run_id,
          current_timestamp()         AS run_timestamp
      Sink:   ds_pipeline_audit_sink (Delta table INSERT)
      Always runs — on success AND failure
```

---

## Audit Table

**Table:** `dbw_ev_intelligence_dev.default.pipeline_audit`
**Type:** Delta table — auto-created by Databricks SQL Warehouse on first `act_write_audit` run

> The first time `act_write_audit` runs, if the table does not exist, the Delta Lake sink in ADF will create it automatically using the schema of the source SELECT.

**Schema:**

| Column | Type | Description |
|---|---|---|
| `pipeline_name` | STRING | `pl_bronze_api_payments_v3` |
| `load_type` | STRING | `full` or `incremental` |
| `watermark_value` | STRING | `updated_after` value used this run — `act_get_watermark` reads this next time |
| `ingestion_date` | STRING | Bronze partition date (`yyyy-MM-dd`) |
| `total_pages` | INT | Pages fetched this run |
| `status` | STRING | `succeeded` or `failed` |
| `pipeline_run_id` | STRING | ADF RunId GUID — links to ADF Monitor |
| `run_timestamp` | TIMESTAMP | UTC time this row was written |

**Query from Databricks SQL or notebook:**
```sql
SELECT
    load_type,
    watermark_value,
    ingestion_date,
    total_pages,
    status,
    run_timestamp
FROM dbw_ev_intelligence_dev.default.pipeline_audit
WHERE pipeline_name = 'pl_bronze_api_payments_v3'
ORDER BY run_timestamp DESC
LIMIT 20;
```

---

## How Incremental Load Advances Automatically

```
Run 1 — Full load  (p_load_type = full)
  act_get_watermark  → query: SELECT '1900-01-01T00:00:00Z' AS last_watermark
  v_watermark        = '1900-01-01T00:00:00Z'
  API fetches all records
  act_write_audit    → watermark_value = '1900-01-01T00:00:00Z', status = succeeded

Run 2 — Incremental  (p_load_type = incremental)
  act_get_watermark  → query: SELECT COALESCE(MAX(watermark_value),...) ...
                       returns '1900-01-01T00:00:00Z'  (last succeeded watermark)
  API fetches records updated_after '1900-01-01T00:00:00Z'
  act_write_audit    → watermark_value = '1900-01-01T00:00:00Z', status = succeeded

  NOTE: To advance the watermark after Run 1, update the audit table with
  MAX(updated_at) from the Bronze data. Run this in Databricks once after full load:

  UPDATE dbw_ev_intelligence_dev.default.pipeline_audit
  SET    watermark_value = (
    SELECT MAX(p.updated_at)
    FROM   delta.`abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/`
           LATERAL VIEW explode(data) AS p
    WHERE  ingestion_date = '<your ingestion_date folder>'
  )
  WHERE  pipeline_name = 'pl_bronze_api_payments_v3'
    AND  status        = 'succeeded'
    AND  load_type     = 'full';

Run 3 — Incremental  (after watermark updated)
  act_get_watermark  → returns '2026-07-04T09:43:00Z'
  API fetches only records updated after that timestamp
  act_write_audit    → watermark_value = '2026-07-04T09:43:00Z', status = succeeded

Run 4+ — Each incremental run picks up exactly where the last succeeded run left off.
```

> Day 8 (Orchestration) will automate the watermark update step — a separate pipeline reads `MAX(updated_at)` from the Bronze Delta table and updates the audit record. For now, do it manually once after the full load.

---

## Trigger Setup

### First run — Full load (run once manually)
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
| `ls_keyvault` | Azure Key Vault | KV Web Activities (already exists from Day 2) |
| `ls_voltgrid_api` | REST | Source dataset (already exists from Day 2) |
| `ls_adls_bronze` | ADLS Gen2 | Sink dataset (already exists from Day 2) |
| `ls_databricks_sql` | Azure Databricks Delta Lake | `act_get_watermark` Lookup + `act_write_audit` Copy |

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `act_get_watermark` fails with `LinkedService not found` | `ls_databricks_sql` not created | Create it in ADF Manage → Linked services (Step 1 above) |
| `act_get_watermark` fails with `Table not found` | `pipeline_audit` table does not exist yet | Run `act_write_audit` once via a full load run — it creates the table |
| `act_get_watermark` Lookup returns no rows | Table exists but has no succeeded rows | Check audit table — if all rows are failed, fix the pipeline and re-run |
| `act_write_audit` fails | SQL Warehouse stopped | Start the SQL Warehouse in Databricks before triggering, or use auto-start |
| `act_get_username` 403 | ADF MI missing `Key Vault Secrets User` role | Portal → Key Vault → IAM → assign role, wait 2 min |
| `act_api_login` 401 | Wrong credentials in Key Vault | Check `voltgrid-username` and `voltgrid-password` |
| Until loop runs only once | `v_total_pages` stayed at 1 | Check `act_get_total_pages` output in Monitor → confirm `pagination.total_pages` key exists |
| `act_copy_payments_page` 403 | ADF MI missing `Storage Blob Data Contributor` on `evdatalakedev` | Portal → Storage → IAM → assign role |
| Incremental fetches all records | `watermark_value` in audit is still `1900-01-01T00:00:00Z` | Run the UPDATE SQL above to set correct watermark after full load |
