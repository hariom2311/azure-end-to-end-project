# ADF Pipeline Notes — v3 Payments API → Bronze
**Day 3 | Auto Watermark + Audit via CSV in Bronze ADLS**

---

## What Changed from v2 → v3

| | v2 | v3 |
|---|---|---|
| Watermark input | Manual `p_watermark` parameter every incremental run | Automatic — Lookup Activity reads `pipeline_audit.csv` from Bronze |
| Audit write | None | Copy Activity appends one row to `pipeline_audit.csv` after every run |
| New linked services | None | None — reuses `ls_adls_bronze` from Day 2 |
| New datasets | None | `ds_pipeline_audit_csv` (DelimitedText on ADLS) |
| Parameters | `p_load_type` + `p_watermark` | `p_load_type` only |

> **Why CSV in Bronze?** It is the simplest possible audit store — no Databricks cluster, no Delta Lake linked service, no notebooks to upload. The same `ls_adls_bronze` linked service used for payment data handles both the audit read and write. You can open and inspect the file directly in Azure Storage Explorer or the Portal.

---

## Audit File Location

```
bronze/
└── audit/
    └── pipeline_audit.csv
```

**File:** `abfss://bronze@<your-storage>.dfs.core.windows.net/audit/pipeline_audit.csv`

**Schema (CSV header row — must exist before first incremental run):**

```
pipeline_name,load_type,watermark_value,ingestion_date,total_pages,status,pipeline_run_id,run_timestamp
```

**Example rows:**
```
pl_bronze_api_payments_v3,full,1900-01-01T00:00:00Z,2026-07-06,12,succeeded,abc123,2026-07-06T01:05:00Z
pl_bronze_api_payments_v3,incremental,2026-07-05T23:59:00Z,2026-07-07,3,succeeded,def456,2026-07-07T01:03:00Z
```

---

## Files to Set Up

### Step 1 — Create the audit CSV with a header row (one-time setup)

Before running the pipeline for the first time, upload an empty CSV with just the header to Bronze.

**Option A — Azure Portal:**
1. Portal → your storage account → **Containers** → `bronze`
2. Click **+ Add Directory** → name it `audit`
3. Inside `audit` → click **Upload**
4. Create a local file `pipeline_audit.csv` with this content (one line, no trailing newline):
   ```
   pipeline_name,load_type,watermark_value,ingestion_date,total_pages,status,pipeline_run_id,run_timestamp
   ```
5. Upload it

**Option B — Azure Storage Explorer:**
1. Connect to your storage account
2. Navigate to `bronze` container → create folder `audit`
3. Upload `pipeline_audit.csv` with the header line above

> The first pipeline run (full load) will append a data row below this header. Without the header row, the Lookup Activity on the second run will fail to parse the watermark column.

---

### Step 2 — Paste datasets and pipeline into ADF

No new linked services needed — everything uses `ls_adls_bronze`.

**Paste order:**

| Step | File | Paste into |
|---|---|---|
| 1 | `ds_voltgrid_payments_src_v3.json` | Author → Datasets |
| 2 | `ds_bronze_payments_sink_v3.json` | Author → Datasets |
| 3 | `ds_pipeline_audit_csv.json` | Author → Datasets |
| 4 | `pl_bronze_api_payments_v3.json` | Author → Pipelines |

**How to paste JSON in ADF Studio:**
1. Author → Datasets (or Pipelines)
2. Click **+** → **New dataset** (or Pipeline)
3. After it opens → click the **`{ }` Code** button (top right)
4. Select all → paste the JSON → click **OK**
5. **Publish all** after each paste

---

## Pipeline Flow — v3

```
pl_bronze_api_payments_v3
│
│  Parameter : p_load_type  ("full" | "incremental")
│
├── act_get_username        WebActivity  — Key Vault → voltgrid-username (MSI)
├── act_get_password        WebActivity  — Key Vault → voltgrid-password (MSI)
├── act_api_login           WebActivity  — POST /api/auth/login/ → token
├── act_set_token           SetVariable  — v_token = token
├── act_set_ingestion_date  SetVariable  — v_ingestion_date = today (yyyy-MM-dd)
│
├── act_get_watermark       Lookup Activity  ← reads pipeline_audit.csv from Bronze
│     Linked service : ls_adls_bronze  (same as payment data — no new linked service!)
│     Dataset        : ds_pipeline_audit_csv
│     Query type     : Query (not Table)
│     Logic:
│       full load       → skip CSV read, use constant '1900-01-01T00:00:00Z'
│       incremental     → first row only = last succeeded row in CSV
│     Output: activity('act_get_watermark').output.firstRow.watermark_value
│
├── act_set_watermark       SetVariable  — v_watermark = watermark from Lookup
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
└── act_write_audit         Copy Activity  ← always runs (success AND failure paths)
      Source: inline dataset — one-row CSV string built from pipeline variables
      Sink:   ds_pipeline_audit_csv  (append mode, no header)
      Writes one row: pipeline_name, load_type, watermark_value, ingestion_date,
                      total_pages, status, pipeline_run_id, run_timestamp
```

---

## How the Watermark Lookup Works

The Lookup Activity reads `pipeline_audit.csv` using `ds_pipeline_audit_csv`. ADF reads the file as a table and returns `firstRowOnly = true`.

**Full load behaviour:**
The `act_get_watermark` Lookup still runs but the pipeline uses the `v_watermark` default value (`1900-01-01T00:00:00Z`) — set by `act_set_watermark` which ignores the Lookup output when `p_load_type = full`.

**Incremental behaviour:**
`act_set_watermark` reads `activity('act_get_watermark').output.firstRow.watermark_value` from the CSV. This returns the **last row** appended (= last run's watermark).

> **Important:** ADF Lookup with `firstRowOnly = true` on a CSV returns the first data row, not the last. The CSV is therefore written in **newest-first order** — `act_write_audit` prepends each new row using a workaround described in the step-by-step guide. Alternatively, treat the CSV as append-only and use a separate Databricks/Logic App to read MAX — but for simplicity the step-by-step guide uses a single-row "current watermark" file instead. See `03_ADF_PIPELINE_V3_STEP_BY_STEP.md` Part D for the exact implementation.

---

## Audit CSV — Column Reference

| Column | Example value | Source |
|---|---|---|
| `pipeline_name` | `pl_bronze_api_payments_v3` | hardcoded string |
| `load_type` | `full` or `incremental` | `pipeline().parameters.p_load_type` |
| `watermark_value` | `1900-01-01T00:00:00Z` | `variables('v_watermark')` |
| `ingestion_date` | `2026-07-06` | `variables('v_ingestion_date')` |
| `total_pages` | `12` | `variables('v_total_pages')` |
| `status` | `succeeded` or `failed` | `variables('v_status')` |
| `pipeline_run_id` | `abc-123-...` | `pipeline().RunId` |
| `run_timestamp` | `2026-07-06T01:05:00Z` | `utcNow()` |

---

## How Incremental Load Advances Automatically

```
One-time setup
  Upload pipeline_audit.csv to bronze/audit/ with header row only.

Run 1 — Full load  (p_load_type = full)
  act_set_watermark  → v_watermark = '1900-01-01T00:00:00Z'  (default, not from CSV)
  API fetches ALL records → all pages written to bronze/api/payments/raw/
  act_write_audit    → appends row to pipeline_audit.csv:
                       watermark_value = '1900-01-01T00:00:00Z', status = 'succeeded'

  [Manual step once after full load:]
  Open pipeline_audit.csv in Portal/Storage Explorer.
  Edit the watermark_value to the actual MAX(updated_at) from your Bronze payment data.
  Save. This is the watermark the next incremental run will use.

Run 2 — Incremental  (p_load_type = incremental)
  act_get_watermark  → reads pipeline_audit.csv → watermark_value = '2026-07-05T23:59:00Z'
  act_set_watermark  → v_watermark = '2026-07-05T23:59:00Z'
  API fetches only records updated after that timestamp
  act_write_audit    → appends row: watermark_value = '2026-07-05T23:59:00Z', status = 'succeeded'

Run 3+
  Same pattern — each run picks up exactly where the last succeeded run left off.
```

> Day 8 (Orchestration) will automate the watermark update step after full load.

---

## Trigger Setup

### First run — Full load (manual, one-time)
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

| Linked Service | Type | Used by | From |
|---|---|---|---|
| `ls_keyvault` | Azure Key Vault | KV Web Activities | Day 2 |
| `ls_voltgrid_api` | REST | Source dataset | Day 2 |
| `ls_adls_bronze` | ADLS Gen2 | Payment sink + audit CSV read/write | Day 2 |

**No new linked services needed for v3.**

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `act_get_watermark` fails: file not found | `pipeline_audit.csv` not uploaded | Upload the CSV with header row to `bronze/audit/` (Step 1 above) |
| `act_get_watermark` returns no rows | CSV has only the header, no data rows | Run a full load first to create the first data row |
| Incremental fetches all records | Watermark not updated after full load | Manually edit `pipeline_audit.csv` and set `watermark_value` to the max timestamp from your Bronze payment data |
| `act_write_audit` fails: permission denied | ADF MI missing `Storage Blob Data Contributor` on Bronze | Portal → Storage → IAM → assign `Storage Blob Data Contributor` to ADF managed identity |
| `act_get_username` 403 | ADF MI missing `Key Vault Secrets User` | Portal → Key Vault → IAM → assign role, wait 2 min |
| `act_api_login` 401 | Wrong credentials in Key Vault | Check `voltgrid-username` and `voltgrid-password` secrets |
| Until loop runs only once | `v_total_pages` stayed at 1 | Monitor → `act_get_total_pages` output → confirm `pagination.total_pages` key exists |
| `act_write_audit` not always running | Wrong dependency condition | Both `act_set_status_success` and `act_set_status_failed` must connect to `act_write_audit` with **Success** condition |
