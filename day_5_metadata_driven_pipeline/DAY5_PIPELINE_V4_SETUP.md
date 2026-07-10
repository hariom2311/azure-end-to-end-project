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
| Audit | per pipeline | per entity — watermark tracked per entity |

---

## Files in this directory

```
day_5_metadata_driven_pipeline/adf_pipeline_json/
├── pipeline_metadata_config.json     ← Config: 17 entities, upload to ADLS bronze/config/
├── pipeline_audit_v4.csv             ← Audit CSV seed file, upload to ADLS bronze/audit/
├── ds_pipeline_metadata_config.json  ← Dataset: reads config JSON from ADLS
├── ds_voltgrid_api_src_v4.json       ← Dataset: generic REST source (parameterized)
├── ds_bronze_api_sink_v4.json        ← Dataset: generic JSON sink (parameterized)
├── pl_bronze_api_master_v4.json      ← Master pipeline: reads config, ForEach entity
└── pl_bronze_api_ingest_v4.json      ← Child pipeline: auth → watermark → copy → audit
```

Datasets reused from Day 3 (already in your ADF):
- `ds_pipeline_audit_csv` — reads audit CSV with header (watermark lookup)
- `ds_pipeline_audit_csv_noheader` — audit CSV without header (audit write source)

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

You need to upload two files to your ADLS storage account before ADF can run.

### 1a. Upload the metadata config

Go to **Azure Portal → Storage Accounts → evdatalakedev → Containers → bronze**

1. Create a folder called `config` if it does not exist
2. Upload `pipeline_metadata_config.json` into `bronze/config/`

Final path must be:
```
abfss://bronze@evdatalakedev.dfs.core.windows.net/config/pipeline_metadata_config.json
```

### 1b. Upload the audit CSV seed file

1. Navigate to the `audit` folder inside the `bronze` container (create it if missing)
2. Upload `pipeline_audit_v4.csv` and **rename it to `pipeline_audit.csv`** after uploading

> If you already have a `pipeline_audit.csv` from Day 3, open it and add a header column `entity_name` between `pipeline_name` and `load_type`. The v4 audit CSV has one extra column compared to v3.

Final path must be:
```
abfss://bronze@evdatalakedev.dfs.core.windows.net/audit/pipeline_audit.csv
```

---

## Step 2 — Import the three new datasets into ADF

Go to **Azure Data Factory Studio → Author tab → Datasets**

Import each dataset JSON one at a time:

### ds_pipeline_metadata_config
1. Click **+** → **Import from ARM template** → paste the contents of `ds_pipeline_metadata_config.json`
2. Or: Click **+** → **New dataset** → JSON → `ls_adls_bronze` → set path to `bronze/config/pipeline_metadata_config.json`
3. Verify: **Preview data** should show the 17-row entity array

### ds_voltgrid_api_src_v4
1. Click **+** → **Import from ARM template** → paste contents of `ds_voltgrid_api_src_v4.json`
2. Linked service: `ls_voltgrid_api`
3. This dataset has 4 parameters: `p_api_path`, `p_page`, `p_page_size`, `p_updated_after` — all filled at runtime by the child pipeline

### ds_bronze_api_sink_v4
1. Click **+** → **Import from ARM template** → paste contents of `ds_bronze_api_sink_v4.json`
2. Linked service: `ls_adls_bronze`
3. This dataset has 3 parameters: `p_entity_name`, `p_ingestion_date`, `p_page` — all filled at runtime

Click **Publish all** after importing all three datasets.

---

## Step 3 — Import the child pipeline

Go to **Author tab → Pipelines**

1. Click **+** → **Import from ARM template** → paste contents of `pl_bronze_api_ingest_v4.json`
2. Pipeline name: `pl_bronze_api_ingest_v4`
3. It has 4 parameters: `p_entity_name`, `p_api_path`, `p_page_size`, `p_load_type`
4. Do NOT add a trigger to this pipeline — it is called only by the master pipeline
5. Click **Publish all**

### What this pipeline does (per entity)

```
act_get_username       → Key Vault: read voltgrid-username
act_get_password       → Key Vault: read voltgrid-password
act_api_login          → POST /api/auth/login/ → get token
act_set_token          → store token in v_token
act_set_ingestion_date → capture today's date as partition folder name
act_get_watermark      → Lookup: read last watermark from pipeline_audit.csv
act_set_watermark      → full load: use epoch | incremental: use CSV value
act_get_total_pages    → GET page 1 of entity API → read pagination.total_pages
act_set_total_pages    → store total_pages in v_total_pages
act_paginate           → Until loop: copy each page to Bronze ADLS
  └── act_copy_entity_page  → REST source → JSON sink per page
  └── act_set_temp_page     → v_temp_page = v_current_page + 1
  └── act_increment_page    → v_current_page = v_temp_page
act_set_status_success → v_status = "succeeded" (if loop succeeded)
act_set_status_failed  → v_status = "failed"    (if loop failed)
act_write_audit        → append 1 row to pipeline_audit.csv
```

Bronze output path per entity per page:
```
bronze/<entity_name>/ingestion_date=<yyyy-MM-dd>/page_<N>.json
```
Example:
```
bronze/payments/ingestion_date=2026-07-10/page_1.json
bronze/sessions/ingestion_date=2026-07-10/page_1.json
```

---

## Step 4 — Import the master pipeline

1. Click **+** → **Import from ARM template** → paste contents of `pl_bronze_api_master_v4.json`
2. Pipeline name: `pl_bronze_api_master_v4`
3. It has 1 parameter: `p_load_type` (default: `incremental`)
4. Click **Publish all**

### What this pipeline does

```
act_read_metadata    → Lookup: reads pipeline_metadata_config.json from bronze/config/
                       returns all 17 entity rows as an array
act_foreach_entity   → ForEach (parallel, max 20): iterates over the array
  └── act_ingest_entity → ExecutePipeline: calls pl_bronze_api_ingest_v4
                          passes entity_name, api_path, page_size, load_type
```

All 17 entities run **in parallel**. Each entity's child pipeline runs independently — if one entity fails, the others continue.

---

## Step 5 — Run the full load (first time only)

1. Go to **Author tab → Pipelines → pl_bronze_api_master_v4**
2. Click **Debug** or **Add trigger → Trigger now**
3. In the parameters panel set:
   - `p_load_type` = `full`
4. Click **OK**

Monitor progress under **Monitor tab → Pipeline runs → pl_bronze_api_master_v4**

Click into the run → click `act_foreach_entity` → you will see 17 child pipeline runs, one per entity, all running in parallel.

Expected Bronze output after full load:
```
bronze/
├── config/
│   └── pipeline_metadata_config.json
├── audit/
│   └── pipeline_audit.csv         ← 17 new rows appended (one per entity)
├── payments/ingestion_date=2026-07-10/
│   ├── page_1.json
│   ├── page_2.json
│   └── ...
├── sessions/ingestion_date=2026-07-10/
│   └── ...
├── customers/...
└── (one folder per entity)
```

---

## Step 6 — Run incremental load (every subsequent run)

1. Go to **pl_bronze_api_master_v4 → Trigger now**
2. Set `p_load_type` = `incremental`
3. Each child pipeline reads the latest `watermark_value` from `pipeline_audit.csv` for its entity and fetches only records updated after that timestamp

To automate, add a **Schedule trigger**:
1. **Manage tab → Triggers → New**
2. Type: Schedule
3. Recurrence: every 2 hours (or as needed)
4. Pipeline: `pl_bronze_api_master_v4`
5. Parameter `p_load_type` = `incremental`

---

## Step 7 — Verify the run

### Check Bronze ADLS
Go to **Portal → evdatalakedev → bronze container**. You should see one folder per entity with JSON files partitioned by date.

### Check the audit CSV
Open `bronze/audit/pipeline_audit.csv`. You should see 17 new rows — one per entity — each with `status = succeeded`.

### Check in ADF Monitor
- **Monitor → Pipeline runs** — filter by `pl_bronze_api_master_v4`
- Drill into the ForEach to see each entity's child run duration and status
- Any failed entity shows in red — the others still complete (parallel, isolated)

---

## How to add a new entity later

No pipeline changes needed. Just:

1. Open `pipeline_metadata_config.json`
2. Add one new object to the array:
```json
{
  "entity_name":  "your_new_entity",
  "api_path":     "/api/db/your_new_entity/",
  "natural_key":  "entity_id",
  "cdc_field":    "updated_at",
  "page_size":    500,
  "enabled":      true
}
```
3. Upload the updated file to `bronze/config/pipeline_metadata_config.json` (overwrite)
4. Run `pl_bronze_api_master_v4` — the new entity is picked up automatically

To temporarily disable an entity without deleting it, set `"enabled": false`. The ForEach still iterates it but `act_ingest_entity` receives it — note: the current master pipeline does not filter on `enabled` in the ForEach expression. If you want filtering, change the ForEach items expression to:
```
@json(string(activity('act_read_metadata').output.value))
```
and add a filter step, or simply remove the row from the config.

---

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `Lookup activity returned no rows` | `pipeline_audit.csv` missing or empty | Upload the seed `pipeline_audit_v4.csv` as `pipeline_audit.csv` to `bronze/audit/` |
| `Resource not found` on metadata Lookup | Config JSON not uploaded | Upload `pipeline_metadata_config.json` to `bronze/config/` |
| `401 Unauthorized` on API call | Key Vault MSI permission missing | Grant ADF MI `Key Vault Secrets User` on `kv-ev-intelligence-dev` |
| `403 Forbidden` on ADLS write | Storage permission missing | Grant ADF MI `Storage Blob Data Contributor` on `evdatalakedev` |
| `dataset() parameter not found` | Old dataset imported without parameters | Re-import `ds_voltgrid_api_src_v4.json` and `ds_bronze_api_sink_v4.json` |
| Child pipeline not found | Master imported before child | Import `pl_bronze_api_ingest_v4` first, then `pl_bronze_api_master_v4` |
