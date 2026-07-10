# Day 5 — Metadata-Driven ADF Pipeline (v4)
**All 17 EV API entities ingested by a single parameterized pipeline pair**

---

## What changed from v3 to v4

| | v3 | v4 |
|---|---|---|
| Pipelines | 1 (payments only) | 2 (master + child) |
| Entities covered | 1 | 17 |
| Source datasets | 1 per entity | 1 generic (`ds_voltgrid_api_src_v4`) |
| Sink datasets | 1 per entity | 1 generic (`ds_bronze_api_sink_v4`) |
| Adding a new entity | New pipeline + 2 datasets | Add 1 row to config JSON |
| Parallel execution | No | Yes — all 17 entities run simultaneously |
| Watermark tracking | shared CSV (buggy) | per-entity CSV file (correct) |
| Bronze path | `bronze/<entity>/` | `bronze/api/<entity>/` |

---

## Files in this directory

```
day_5_metadata_driven_pipeline/adf_pipeline_json/
├── pipeline_metadata_config.json       ← Config: 17 entities, upload to bronze/config/
├── pipeline_audit_v4.csv               ← Seed audit CSV, upload to bronze/audit/
├── ds_pipeline_metadata_config.json    ← Dataset: reads config JSON from ADLS
├── ds_voltgrid_api_src_v4.json         ← Dataset: generic REST source (parameterized)
├── ds_bronze_api_sink_v4.json          ← Dataset: generic JSON sink (parameterized)
├── ds_pipeline_audit_entity_csv.json   ← Dataset: per-entity watermark CSV (parameterized)
├── ds_audit_template_csv.json          ← Dataset: single-newline source for audit/watermark writes
├── pl_bronze_api_master_v4.json        ← Master pipeline: reads config, ForEach entity
└── pl_bronze_api_ingest_v4.json        ← Child pipeline: auth → watermark → copy → audit
```

Datasets reused from Day 3 (already in your ADF):
- `ds_pipeline_audit_csv` — shared audit history CSV with header (audit write sink)

---

## Prerequisites

Before starting, confirm these are already set up from Day 3:

- [ ] ADF instance: `adf-ev-intelligence-dev`
- [ ] Linked service `ls_voltgrid_api` — REST, base URL `https://ev-project-navy-mu.vercel.app`
- [ ] Linked service `ls_adls_bronze` — ADLS Gen2, storage account `evdatalakedev`, MSI auth
- [ ] Key Vault `kv-ev-intelligence-dev` with secrets `voltgrid-username` and `voltgrid-password`
- [ ] ADF Managed Identity has `Key Vault Secrets User` role on the Key Vault
- [ ] ADF Managed Identity has `Storage Blob Data Contributor` role on `evdatalakedev`
- [ ] Datasets `ds_pipeline_audit_csv` and `ds_pipeline_audit_csv_noheader` imported from Day 3

---

## Step 1 — Upload files to ADLS Bronze

### 1a. Upload the metadata config

Go to **Azure Portal → Storage Accounts → evdatalakedev → Containers → bronze**

1. Create a folder called `config` if it does not exist
2. Upload `pipeline_metadata_config.json` into `bronze/config/`

Final path:
```
abfss://bronze@evdatalakedev.dfs.core.windows.net/config/pipeline_metadata_config.json
```

### 1b. Upload the audit CSV seed file

1. Navigate to the `audit` folder inside the `bronze` container (create it if missing)
2. Upload `pipeline_audit_v4.csv` and **rename it to `pipeline_audit.csv`** after uploading

> If you already have a `pipeline_audit.csv` from Day 3, add a header column `entity_name`
> between `pipeline_name` and `load_type`. The v4 audit CSV has one extra column vs v3.

Final path:
```
abfss://bronze@evdatalakedev.dfs.core.windows.net/audit/pipeline_audit.csv
```

### 1c. Upload per-entity watermark seed files

The child pipeline reads `bronze/audit/watermark_<entity_name>.csv` to get the
incremental watermark for each entity. These files must exist before the first
incremental run.

> **If you ran `full_load_bronze.py` locally**: these files were already created
> automatically in ADLS at `2026-07-10T00:00:00Z`. Skip this step.

If you skipped the Python full load, run this once in a Python environment
(requires `.env` to be configured):

```bash
python -c "
import os, datetime
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()
cred = ClientSecretCredential(os.getenv('AZURE_TENANT_ID'), os.getenv('AZURE_PROJECT_CLIENT_ID'), os.getenv('AZURE_PROJECT_CLIENT_SECRET'))
client = DataLakeServiceClient('https://evdatalakedev.dfs.core.windows.net', credential=cred)
fs = client.get_file_system_client('bronze')

WATERMARK = '2026-07-10T00:00:00Z'
ENTITIES = ['payments','sessions','customers','fleet','chargers','vehicles',
            'stations','complaints','maintenance_events','energy_prices','tariffs',
            'charge_cards','employees','partners','cities','states','weather']

header = 'watermark_value,entity_name,updated_at\n'
for entity in ENTITIES:
    content = (header + f'{WATERMARK},{entity},{WATERMARK}\n').encode('utf-8')
    fc = fs.get_file_client(f'audit/watermark_{entity}.csv')
    fc.upload_data(content, overwrite=True, length=len(content))
    print(f'Created: watermark_{entity}.csv')
"
```

This creates 17 files in `bronze/audit/`:
```
bronze/audit/watermark_payments.csv
bronze/audit/watermark_sessions.csv
... (one per entity)
```

---

## Step 2 — Import the four new datasets into ADF

Go to **Azure Data Factory Studio → Author tab → Datasets**

Import each dataset JSON:

### ds_pipeline_metadata_config
1. Click **+** → **Import from ARM template** → paste contents of `ds_pipeline_metadata_config.json`
2. Linked service: `ls_adls_bronze`
3. Verify: **Preview data** should show the 17-row entity array

### ds_voltgrid_api_src_v4
1. Click **+** → **Import from ARM template** → paste contents of `ds_voltgrid_api_src_v4.json`
2. Linked service: `ls_voltgrid_api`
3. Has 4 parameters: `p_api_path`, `p_page`, `p_page_size`, `p_updated_after`

### ds_bronze_api_sink_v4
1. Click **+** → **Import from ARM template** → paste contents of `ds_bronze_api_sink_v4.json`
2. Linked service: `ls_adls_bronze`
3. Has 3 parameters: `p_entity_name`, `p_ingestion_date`, `p_page`

### ds_pipeline_audit_entity_csv
1. Click **+** → **Import from ARM template** → paste contents of `ds_pipeline_audit_entity_csv.json`
2. Linked service: `ls_adls_bronze`
3. Has 1 parameter: `p_entity_name` — used to read/write `bronze/audit/watermark_<entity>.csv`
4. This is the key fix for per-entity watermark tracking

### ds_audit_template_csv
1. Click **+** → **Import from ARM template** → paste contents of `ds_audit_template_csv.json`
2. Linked service: `ls_adls_bronze`
3. Points to `bronze/audit/audit_template.csv` — a single-newline file with no columns
4. Used as the **source** in both `act_write_audit` and `act_write_watermark`
5. Why needed: ADF Copy requires a source dataset even when all output columns come from `additionalColumns`. Using a blank no-column file with `firstRowAsHeader: false` prevents the `QuoteAllText` error and `Prop_0..Prop_N` ghost column corruption that occurs when source and sink header modes differ

Click **Publish all** after importing all five datasets.

---

## Step 3 — Import the child pipeline

Go to **Author tab → Pipelines**

1. Click **+** → **Import from ARM template** → paste contents of `pl_bronze_api_ingest_v4.json`
2. Pipeline name: `pl_bronze_api_ingest_v4`
3. Has 4 parameters: `p_entity_name`, `p_api_path`, `p_page_size`, `p_load_type`
4. Do NOT add a trigger — called only by the master pipeline
5. Click **Publish all**

### What this pipeline does (per entity)

```
act_get_username        → Key Vault: read voltgrid-username (MSI auth)
act_get_password        → Key Vault: read voltgrid-password (MSI auth)
act_api_login           → POST /api/auth/login/ → get token
act_set_token           → store token in v_token
act_set_ingestion_date  → capture today's date as partition folder name
act_get_watermark       → Lookup: read watermark_<entity_name>.csv (per-entity)
act_set_watermark       → full: epoch | incremental: value from watermark file
act_get_total_pages     → GET page 1 of entity API → read pagination.total_pages
act_set_total_pages     → store total_pages in v_total_pages
act_paginate            → Until loop: copy each page to Bronze ADLS
  └── act_copy_entity_page  → REST source → JSON sink
                              bronze/api/<entity>/ingestion_date=<date>/page_N.json
  └── act_set_temp_page     → v_temp_page = v_current_page + 1
  └── act_increment_page    → v_current_page = v_temp_page
act_set_status_success  → v_status = "succeeded" (if loop succeeded)
act_set_status_failed   → v_status = "failed"    (if loop failed)
act_write_audit         → append 1 row to pipeline_audit.csv (always runs)
act_write_watermark     → overwrite watermark_<entity>.csv with utcNow()
                          (only runs on success — failed runs do NOT advance watermark)
```

Bronze output path:
```
bronze/api/<entity_name>/ingestion_date=<yyyy-MM-dd>/page_<N>.json
```

---

## Step 4 — Import the master pipeline

1. Click **+** → **Import from ARM template** → paste contents of `pl_bronze_api_master_v4.json`
2. Pipeline name: `pl_bronze_api_master_v4`
3. Has 1 parameter: `p_load_type` (default: `incremental`)
4. Click **Publish all**

### What this pipeline does

```
act_read_metadata    → Lookup: reads pipeline_metadata_config.json
                       returns all 17 entity rows as an array
act_foreach_entity   → ForEach (parallel, max 20): iterates over the array
  └── act_ingest_entity → ExecutePipeline: calls pl_bronze_api_ingest_v4
                          passes entity_name, api_path, page_size, load_type
```

All 17 entities run **in parallel**. If one entity fails, the others continue.

---

## Step 5 — Run the full load (first time only)

> **Recommended**: Run `full_load_bronze.py` locally instead of using ADF for
> the full load. It's faster, doesn't consume ADF pipeline minutes, and handles
> the 600+ page entities better. See `FULL_LOAD_GUIDE.md`.

If you prefer ADF for full load:

1. Go to **pl_bronze_api_master_v4 → Debug or Trigger now**
2. Set `p_load_type` = `full`
3. Click **OK**

Monitor: **Monitor tab → Pipeline runs → pl_bronze_api_master_v4**
Click into the run → `act_foreach_entity` → see all 17 child runs in parallel.

---

## Step 6 — Run incremental load (every subsequent run)

1. Go to **pl_bronze_api_master_v4 → Trigger now**
2. Set `p_load_type` = `incremental`
3. Each child pipeline reads `watermark_<entity>.csv` for the correct per-entity
   watermark and fetches only records updated after that timestamp

To automate with a **Schedule trigger**:
1. **Manage tab → Triggers → New**
2. Type: Schedule, Recurrence: every 2 hours
3. Pipeline: `pl_bronze_api_master_v4`
4. Parameter `p_load_type` = `incremental`

---

## Step 7 — Verify the run

### Check Bronze ADLS
Go to **Portal → evdatalakedev → bronze → api**. One folder per entity, JSON files
partitioned by ingestion date.

### Check the watermark files
Go to **bronze → audit**. After a successful incremental run you should see:
- `pipeline_audit.csv` — new rows appended (one per entity)
- `watermark_payments.csv`, `watermark_sessions.csv` etc. — timestamp updated to now

### Check in ADF Monitor
- **Monitor → Pipeline runs** — filter by `pl_bronze_api_master_v4`
- Drill into the ForEach to see each entity's child run duration and status
- Incremental runs should take minutes not hours — if they still take hours,
  check that the watermark files exist and have a recent timestamp

---

## How to add a new entity later

1. Add one row to `pipeline_metadata_config.json`
2. Upload updated config to `bronze/config/pipeline_metadata_config.json`
3. Create the watermark seed file for the new entity in `bronze/audit/`:
   ```
   watermark_value,entity_name,updated_at
   1900-01-01T00:00:00Z,new_entity,1900-01-01T00:00:00Z
   ```
   (use epoch so the first run is a full load for the new entity)
4. Run `pl_bronze_api_master_v4` — new entity is picked up automatically

---

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `Lookup activity returned no rows` | `pipeline_audit.csv` missing | Upload seed `pipeline_audit_v4.csv` as `pipeline_audit.csv` to `bronze/audit/` |
| `Resource not found` on metadata Lookup | Config JSON not uploaded | Upload `pipeline_metadata_config.json` to `bronze/config/` |
| `Resource not found` on watermark Lookup | `watermark_<entity>.csv` missing | Run the seed script in Step 1c |
| Incremental still fetches all pages | Watermark file has epoch value | Check watermark file content — should be a recent date not `1900-01-01` |
| `401 Unauthorized` on API call | Key Vault MSI permission missing | Grant ADF MI `Key Vault Secrets User` on `kv-ev-intelligence-dev` |
| `403 Forbidden` on ADLS write | Storage permission missing | Grant ADF MI `Storage Blob Data Contributor` on `evdatalakedev` |
| `dataset() parameter not found` | Old dataset imported without parameters | Re-import the v4 datasets from JSON files |
| Child pipeline not found | Master imported before child | Import `pl_bronze_api_ingest_v4` first, then master |
| `QuoteAllText cannot set to false` | Source has `firstRowAsHeader: false`, sink has `true` — ADF conflict | Import `ds_audit_template_csv` and set it as source in `act_write_audit` and `act_write_watermark` |
| `Prop_0, Prop_1...` columns in audit CSV | Source CSV had blank comma-separated columns — each comma became a `Prop_N` column | Use `audit_template.csv` (single newline only) as source — zero commas means zero ghost columns |
