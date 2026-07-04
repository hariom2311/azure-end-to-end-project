# Day 2 — Bronze Layer Ingestion: ADF Setup for API + Blob Sources
**Duration: ~2.5 hours | Cost: ~₹60–70**

Connect Azure Data Factory to VoltGrid API and source blob storage.
Load one API endpoint (payments) and one blob source (charging_sessions) into the Bronze layer as Delta tables in ADLS Gen2 — plus a mirrored Databricks internal Delta table for learning.

---

## What You Will Have at the End of Day 2

- ADF linked services for VoltGrid API and source blob storage
- ADF pipeline: payments API → Bronze Delta table (full load first run, incremental on re-runs)
- ADF pipeline: charging_sessions blob → Bronze Delta table (partitioned by hour)
- Bronze Delta tables in ADLS: `bronze/api/payments/` and `bronze/blob/iot_sessions/`
- Databricks internal Delta tables: `bronze_payments` and `bronze_charging_sessions` (same data, different storage — for comparison)
- Pipeline audit log entry in `bronze/api/pipeline_audit/` after every run
- All credentials in Key Vault — nothing hardcoded in ADF or notebooks

---

## Architecture for Today

```
VoltGrid API
  POST /api/auth/login/    →  token (in memory, pipeline variable)
  GET  /api/db/payments/   →  paginated JSON  →  ADF Copy Activity
                                                      ↓
                                              Bronze Delta Table
                                    abfss://bronze@evdatalakedev.../api/payments/
                                              ↓
                                    Databricks Delta Table (internal)
                                    hive_metastore.bronze.payments

Source Blob (dataenggdailystorage)
  wasbs://source@.../realtime/charging_sessions/YYYY/MM/DD/HH/*.csv
                                              ↓
                                              ADF Copy Activity
                                              ↓
                                    Bronze Delta Table
                                    abfss://bronze@evdatalakedev.../blob/iot_sessions/
                                              ↓
                                    Databricks Delta Table (internal)
                                    hive_metastore.bronze.charging_sessions
```

---

## Load Strategy

| Source | First Run | Subsequent Runs |
|---|---|---|
| API (payments) | Full load — all pages | Incremental — `updated_after={last_watermark}` |
| Blob (charging_sessions) | All files for today | Files for current hour only |

**Watermark for API:** After each run, store `max(updated_at)` from the response in the `pipeline_audit` table. Next run reads this value and passes it as `?updated_after=<value>`.

**Partitioning for Blob:** Files are at `charging_sessions/YYYY/MM/DD/HH/`. ADF reads the current hour's folder each run. Delta table partitioned by `ingestion_date` and `ingestion_hour`.

---

## Separate Config Files — What to Read

| File | What it covers |
|---|---|
| `01_ADF_LINKED_SERVICES.md` | Create linked services for API + blob in ADF UI and CLI |
| `02_ADF_PIPELINE_API_PAYMENTS.md` | ADF pipeline for payments API → Bronze Delta (full + incremental) |
| `03_ADF_PIPELINE_BLOB_SESSIONS.md` | ADF pipeline for blob charging_sessions → Bronze Delta (hourly) |
| `04_DATABRICKS_BRONZE_TABLES.md` | Databricks notebooks: create internal Delta tables mirroring the ADF-loaded data |
| `notebooks/03_bronze_api_payments.ipynb` | Databricks notebook: full+incremental load for payments |
| `notebooks/04_bronze_blob_sessions.ipynb` | Databricks notebook: hourly blob load for charging_sessions |

---

## Prerequisites (all from Day 1)

- [ ] ADF instance exists: `adf-ev-intelligence-dev`
- [ ] ADLS Gen2 exists: `evdatalakedev` with containers: bronze, silver, gold, source
- [ ] Key Vault: `kv-ev-intelligence-dev` with secret scope `kv-ev-scope`
- [ ] Secrets in Key Vault:
  - `voltgrid-api-base-url`
  - `voltgrid-username`
  - `voltgrid-password`
  - `adls-account-name`
  - `sp-client-id`, `sp-client-secret`, `sp-tenant-id`
  - `source-storage-account` (`dataenggdailystorage`)
  - `source-sas-token`
- [ ] Bronze folder structure created (run `01_create_folder_structure.ipynb`)
- [ ] ADF Managed Identity has `Storage Blob Data Contributor` role on `evdatalakedev`

---

## Day 2 Checklist

### ADF Setup
- [ ] Key Vault linked service created in ADF
- [ ] VoltGrid API linked service created (REST)
- [ ] Source blob linked service created (Azure Blob Storage + SAS)
- [ ] ADLS Gen2 linked service created (Managed Identity)

### API Pipeline
- [ ] Dataset: VoltGrid payments REST source
- [ ] Dataset: Bronze payments Delta sink
- [ ] Pipeline: `pl_bronze_api_payments` created
- [ ] Web activity: login → token stored in variable
- [ ] ForEach + Copy activity: paginate all pages to Delta
- [ ] Watermark logic: read last run → pass as updated_after → write new watermark
- [ ] Pipeline ran successfully (full load)
- [ ] Pipeline re-ran successfully (incremental load — fewer rows)

### Blob Pipeline
- [ ] Dataset: charging_sessions blob source (parameterised path)
- [ ] Dataset: Bronze sessions Delta sink
- [ ] Pipeline: `pl_bronze_blob_sessions` created
- [ ] Pipeline ran successfully for current hour

### Databricks Tables
- [ ] `hive_metastore.bronze.payments` created and queryable
- [ ] `hive_metastore.bronze.charging_sessions` created and queryable
- [ ] Both tables show same row counts as ADLS Delta files

---

## Cost for Day 2

| Resource | Cost |
|---|---|
| ADF pipeline runs (~5 test runs) | ~₹5–8 |
| Databricks cluster (2 hours) | ~₹40–45 |
| ADLS storage (small write) | ~₹1 |
| **Total** | **~₹46–54** |

---

## Reading Order

1. `01_ADF_LINKED_SERVICES.md` — set up all 4 linked services first
2. `02_ADF_PIPELINE_API_PAYMENTS.md` — build the payments pipeline
3. `03_ADF_PIPELINE_BLOB_SESSIONS.md` — build the blob pipeline
4. `04_DATABRICKS_BRONZE_TABLES.md` — create internal Delta tables in Databricks
5. Run `notebooks/03_bronze_api_payments.ipynb` — verify payments data in Delta
6. Run `notebooks/04_bronze_blob_sessions.ipynb` — verify sessions data in Delta
