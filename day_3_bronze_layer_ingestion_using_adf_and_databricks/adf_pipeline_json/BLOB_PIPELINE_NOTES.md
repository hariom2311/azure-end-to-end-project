# ADF Pipeline Notes — Bronze Blob Charging Sessions
**Day 3 | pl_bronze_blob_charging_sessions**

---

## Files in This Directory (Blob Pipeline)

| File | Paste target in ADF |
|---|---|
| `pl_bronze_blob_charging_sessions.json` | Pipelines → `{ }` Code button |
| `ds_charging_sessions_src.json` | Datasets → `{ }` Code button |
| `ds_bronze_sessions_sink.json` | Datasets → `{ }` Code button |

**Paste order:** datasets first, then pipeline.

---

## Pipeline Architecture

```
pl_bronze_blob_charging_sessions
│
│  Parameter: p_load_type  ("full" | "incremental")
│
├── act_check_load_type  [IfCondition]
│     │
│     ├── TRUE (incremental)
│     │     ├── act_get_last_partition   [DatabricksNotebook]
│     │     │     Runs: nb_get_last_partition
│     │     │     Returns: {"year":"2026","month":"06","day":"01","hour":"07"}
│     │     │
│     │     ├── act_set_year   [SetVariable]  v_year  = json(output).year
│     │     ├── act_set_month  [SetVariable]  v_month = json(output).month
│     │     ├── act_set_day    [SetVariable]  v_day   = json(output).day
│     │     └── act_set_hour   [SetVariable]  v_hour  = json(output).hour
│     │
│     └── FALSE (full)
│           ├── act_set_year_full   [SetVariable]  v_year  = "*"
│           ├── act_set_month_full  [SetVariable]  v_month = "*"
│           ├── act_set_day_full    [SetVariable]  v_day   = "*"
│           └── act_set_hour_full   [SetVariable]  v_hour  = "*"
│
├── act_copy_sessions  [Copy]
│     Source : ds_charging_sessions_src
│               wasbs://source@dataenggdailystorage.blob.core.windows.net/
│               realtime/charging_sessions/{year}/{month}/{day}/{hour}/*.csv
│     Sink   : ds_bronze_sessions_sink
│               abfss://bronze@evdatalakedev.dfs.core.windows.net/
│               realtime/charging_sessions/{year}/{month}/{day}/{hour}/
│
├── act_set_files_copied  [SetVariable]  v_files_copied = filesWritten
│
├── act_set_status_success  [SetVariable]  v_status = "succeeded"  (on Copy success)
├── act_set_status_failed   [SetVariable]  v_status = "failed"     (on Copy failure)
│
└── act_write_audit  [DatabricksNotebook]   ← always runs (success AND failure)
      Runs: nb_write_audit
      Writes 1 row to: dbw_ev_intelligence_dev.default.bronze_blob_audit
```

---

## How Dynamic Incremental Load Works

This is the production pattern — no hardcoded dates anywhere in the pipeline.

**First run (no audit record yet):**
```
nb_get_last_partition
  → audit table does not exist or has no succeeded rows
  → returns FALLBACK_PARTITION = {"year":"2026","month":"06","day":"01","hour":"06"}

act_copy_sessions copies: realtime/charging_sessions/2026/06/01/06/

nb_write_audit writes:
  partition_copied = 2026/06/01/06, status = succeeded
```

**Second run (audit record exists):**
```
nb_get_last_partition
  → reads audit table: last succeeded row = 2026/06/01/06
  → advances by 1 hour → returns {"year":"2026","month":"06","day":"01","hour":"07"}

act_copy_sessions copies: realtime/charging_sessions/2026/06/01/07/

nb_write_audit writes:
  partition_copied = 2026/06/01/07, status = succeeded
```

**Every subsequent run:** automatically picks up from where the last run left off. No manual date entry. Hour rollover (23 → 00 next day), month/year boundaries — all handled by `datetime + timedelta(hours=1)` in `nb_get_last_partition`.

---

## Audit Table

**Location:** `dbw_ev_intelligence_dev.default.bronze_blob_audit`
(Managed Delta table — created automatically on first run by `nb_write_audit`)

**Schema:**

| Column | Type | Example |
|---|---|---|
| `pipeline_name` | STRING | `pl_bronze_blob_charging_sessions` |
| `load_type` | STRING | `incremental` |
| `p_year` | STRING | `2026` |
| `p_month` | STRING | `06` |
| `p_day` | STRING | `01` |
| `p_hour` | STRING | `06` |
| `files_copied` | INT | `1` |
| `status` | STRING | `succeeded` |
| `pipeline_run_id` | STRING | `a1b2c3d4-...` (ADF run GUID) |
| `run_timestamp` | TIMESTAMP | `2026-07-05 10:30:00` |

**Query the audit table from Databricks:**
```sql
SELECT
    concat(p_year, '/', p_month, '/', p_day, '/', p_hour) AS partition_copied,
    files_copied,
    status,
    run_timestamp
FROM dbw_ev_intelligence_dev.default.bronze_blob_audit
WHERE pipeline_name = 'pl_bronze_blob_charging_sessions'
ORDER BY run_timestamp DESC
LIMIT 20;
```

---

## Databricks Notebooks — Upload Locations

Upload these two notebooks to Databricks Workspace before running the pipeline:

| Notebook file | Upload to Workspace path |
|---|---|
| `nb_get_last_partition.ipynb` | `/Shared/ev_intelligence/bronze/nb_get_last_partition` |
| `nb_write_audit.ipynb` | `/Shared/ev_intelligence/bronze/nb_write_audit` |

**How to upload:**
1. Databricks → left sidebar → **Workspace**
2. Navigate to `/Shared/ev_intelligence/bronze/` (create folders if needed)
3. Click the `⋮` menu → **Import** → upload the `.ipynb` file

The paths must match exactly what is set in `pl_bronze_blob_charging_sessions.json` under `notebookPath`.

---

## Linked Services Required

| Linked Service | Used by |
|---|---|
| `ls_source_blob` | `ds_charging_sessions_src` — source blob SAS auth |
| `ls_adls_bronze` | `ds_bronze_sessions_sink` — ADLS Gen2 Managed Identity |
| `ls_databricks` | `act_get_last_partition`, `act_write_audit` — Databricks cluster |

`ls_databricks` must be created in ADF if not already present:
- Type: Azure Databricks
- Authentication: Managed Identity (or PAT token stored in Key Vault)
- Cluster: existing cluster or job cluster

---

## Trigger Setup

**Incremental — daily schedule (copy one hour per day):**
```
Trigger type : Schedule
Recurrence   : Every 1 hour  (or every day depending on source file frequency)
Parameters   : p_load_type = incremental
```

**Full load — run once manually:**
```
Add trigger → Trigger now
Parameters  : p_load_type = full
```

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `act_get_last_partition` fails | `ls_databricks` linked service not created | Create Databricks linked service in ADF Manage tab |
| `act_get_last_partition` returns empty | Fallback partition format wrong in notebook | Check `FALLBACK_PARTITION` in `nb_get_last_partition` Cell 1 |
| `act_copy_sessions` 403 on source | `ls_source_blob` SAS token expired | Regenerate SAS, update `source-sas-token` in Key Vault |
| `act_copy_sessions` 403 on sink | ADF MI missing `Storage Blob Data Contributor` on `evdatalakedev` | Day 2 Part 2 — assign role, wait 2 min |
| `act_write_audit` fails but copy succeeded | Databricks cluster not running | Use job cluster in `ls_databricks`, or ensure cluster is running |
| `act_write_audit` writes `status=failed` | Copy activity failed | Check `act_copy_sessions` error in Monitor tab, fix source/sink config |
| Pipeline keeps copying same partition | `nb_get_last_partition` not reading audit table correctly | Run notebook manually in Databricks and check output |
