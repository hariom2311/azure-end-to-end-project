# Day 5 — Metadata-Driven Pipeline v4 — Full Flow
**pl_bronze_api_master_v4 → pl_bronze_api_ingest_v4**

---

## Big Picture

```
You (trigger)
    │
    ▼
pl_bronze_api_master_v4          ← Master pipeline — runs ONCE
    │
    ├── reads pipeline_metadata_config.json from ADLS
    │       (17 entity rows: entity_name, api_path, page_size)
    │
    └── ForEach entity (all 17 in parallel, max 20 at a time)
            │
            ├── pl_bronze_api_ingest_v4 [payments]
            ├── pl_bronze_api_ingest_v4 [sessions]
            ├── pl_bronze_api_ingest_v4 [customers]
            ├── pl_bronze_api_ingest_v4 [fleet]
            ├── pl_bronze_api_ingest_v4 [chargers]
            ├── pl_bronze_api_ingest_v4 [vehicles]
            ├── pl_bronze_api_ingest_v4 [stations]
            ├── pl_bronze_api_ingest_v4 [complaints]
            ├── pl_bronze_api_ingest_v4 [maintenance_events]
            ├── pl_bronze_api_ingest_v4 [energy_prices]
            ├── pl_bronze_api_ingest_v4 [tariffs]
            ├── pl_bronze_api_ingest_v4 [charge_cards]
            ├── pl_bronze_api_ingest_v4 [employees]
            ├── pl_bronze_api_ingest_v4 [partners]
            ├── pl_bronze_api_ingest_v4 [cities]
            ├── pl_bronze_api_ingest_v4 [states]
            └── pl_bronze_api_ingest_v4 [weather]
```

Each child pipeline runs **independently and in parallel**.
If one entity fails, the other 16 continue unaffected.

---

## Master Pipeline — pl_bronze_api_master_v4

```
TRIGGER (manual or schedule)
│   Parameter: p_load_type = "full" | "incremental"
│
▼
┌─────────────────────────────────────────────────────┐
│ act_read_metadata                                   │
│ Type: Lookup                                        │
│ Reads: bronze/config/pipeline_metadata_config.json  │
│ Returns: array of 17 entity objects                 │
│                                                     │
│ output.value = [                                    │
│   { entity_name: "payments",                        │
│     api_path: "/api/db/payments/",                  │
│     page_size: 500, enabled: true },                │
│   { entity_name: "sessions", ... },                 │
│   ...17 rows total                                  │
│ ]                                                   │
└──────────────────────┬──────────────────────────────┘
                       │ Succeeded
                       ▼
┌─────────────────────────────────────────────────────┐
│ act_foreach_entity                                  │
│ Type: ForEach                                       │
│ Items: @activity('act_read_metadata').output.value  │
│ isSequential: false  (ALL entities run in parallel) │
│ batchCount: 20                                      │
│                                                     │
│ For each item in the array:                         │
│   └── act_ingest_entity                             │
│       Type: ExecutePipeline                         │
│       Calls: pl_bronze_api_ingest_v4                │
│       Parameters passed:                            │
│         p_entity_name ← item().entity_name          │
│         p_api_path    ← item().api_path             │
│         p_page_size   ← item().page_size            │
│         p_load_type   ← pipeline().parameters       │
│                          .p_load_type               │
└─────────────────────────────────────────────────────┘
```

---

## Child Pipeline — pl_bronze_api_ingest_v4

One copy of this pipeline runs **per entity**. All activity names and
variables are the same — what changes is the parameter values injected
by the master ForEach.

```
Parameters received from master:
  p_entity_name  = "payments"           (changes per entity)
  p_api_path     = "/api/db/payments/"  (changes per entity)
  p_page_size    = 500
  p_load_type    = "full" | "incremental"
```

### Full Activity Flow

```
┌──────────────────────────────────────────────────────────────────┐
│ act_get_username                                                 │
│ Type: Web Activity (GET)                                         │
│ URL : https://kv-ev-intelligence-dev.vault.azure.net/            │
│       secrets/voltgrid-username/?api-version=7.0                 │
│ Auth: Managed Identity (MSI)                                     │
│ Returns: { value: "voltgrid_demo" }                              │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_get_password                                                 │
│ Type: Web Activity (GET)                                         │
│ URL : https://kv-ev-intelligence-dev.vault.azure.net/            │
│       secrets/voltgrid-password/?api-version=7.0                 │
│ Auth: Managed Identity (MSI)                                     │
│ Returns: { value: "EVcharge@AU2025" }                            │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_api_login                                                    │
│ Type: Web Activity (POST)                                        │
│ URL : https://ev-project-navy-mu.vercel.app/api/auth/login/      │
│ Body: { username: <from KV>, password: <from KV> }              │
│ Returns: { token: "abc123..." }                                  │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_set_token                                                    │
│ Type: SetVariable                                                │
│ v_token ← activity('act_api_login').output.token                 │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_set_ingestion_date                                           │
│ Type: SetVariable                                                │
│ v_ingestion_date ← formatDateTime(utcNow(), 'yyyy-MM-dd')        │
│ Example: "2026-07-10"                                            │
│ Used as the Bronze partition folder name for every page          │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_get_watermark                                                │
│ Type: Lookup                                                     │
│ Dataset: ds_pipeline_audit_csv                                   │
│ Reads: bronze/audit/pipeline_audit.csv (first row only)         │
│ Returns: { watermark_value: "2026-07-09T00:00:00Z", ... }       │
│                                                                  │
│ NOTE: For full load this value is ignored in the next step.      │
│       For incremental it becomes the updated_after filter.       │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_set_watermark                                                │
│ Type: SetVariable                                                │
│                                                                  │
│ IF p_load_type == "full"                                         │
│   v_watermark = "1900-01-01T00:00:00Z"  ← fetch ALL records     │
│ ELSE (incremental)                                               │
│   v_watermark = firstRow.watermark_value ← fetch only new ones  │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_get_total_pages                                              │
│ Type: Web Activity (GET)                                         │
│ URL : https://ev-project-navy-mu.vercel.app                      │
│       + p_api_path                                               │
│       + ?page=1&page_size=500&updated_after=v_watermark          │
│ Example for payments full load:                                  │
│   /api/db/payments/?page=1&page_size=500                         │
│   &updated_after=1900-01-01T00:00:00Z                            │
│ Header: Authorization: Token <v_token>                           │
│ Returns: { pagination: { total_pages: 641, total: 320035 } }    │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_set_total_pages                                              │
│ Type: SetVariable                                                │
│ v_total_pages ← activity('act_get_total_pages')                  │
│                  .output.pagination.total_pages                  │
│ Example: v_total_pages = 641                                     │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_paginate                                                     │
│ Type: Until Loop                                                 │
│ Condition: v_current_page > v_total_pages                        │
│ Timeout: 12 hours                                                │
│                                                                  │
│  Loop iteration (runs once per page):                            │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ act_copy_entity_page                                       │  │
│  │ Type: Copy Activity                                        │  │
│  │                                                            │  │
│  │ SOURCE (ds_voltgrid_api_src_v4):                           │  │
│  │   REST GET https://ev-project-navy-mu.vercel.app           │  │
│  │     + p_api_path                                           │  │
│  │     + ?page=v_current_page                                 │  │
│  │       &page_size=p_page_size                               │  │
│  │       &updated_after=v_watermark                           │  │
│  │   Header: Authorization: Token <v_token>                   │  │
│  │                                                            │  │
│  │ SINK (ds_bronze_api_sink_v4):                              │  │
│  │   ADLS Gen2 → bronze container                             │  │
│  │   Path: api/<entity_name>/                                 │  │
│  │         ingestion_date=<v_ingestion_date>/                 │  │
│  │         page_<v_current_page>.json                         │  │
│  │   Example:                                                 │  │
│  │     bronze/api/payments/                                   │  │
│  │     ingestion_date=2026-07-10/page_1.json                  │  │
│  └───────────────────────────┬────────────────────────────────┘  │
│                              │ Succeeded                         │
│                              ▼                                   │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ act_set_temp_page                                          │  │
│  │ Type: SetVariable                                          │  │
│  │ v_temp_page = v_current_page + 1                           │  │
│  │ (ADF cannot self-assign a variable — needs temp workaround) │  │
│  └───────────────────────────┬────────────────────────────────┘  │
│                              │ Succeeded                         │
│                              ▼                                   │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ act_increment_page                                         │  │
│  │ Type: SetVariable                                          │  │
│  │ v_current_page = v_temp_page                               │  │
│  │ (now v_current_page is page + 1, loop re-checks condition) │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ← loop back to condition check →                                │
│    if v_current_page > v_total_pages: EXIT                       │
│    else: run next iteration                                      │
└──────────┬──────────────────────────┬───────────────────────────┘
           │ Succeeded                │ Failed
           ▼                          ▼
┌──────────────────────┐   ┌──────────────────────────┐
│ act_set_status_success│   │ act_set_status_failed    │
│ Type: SetVariable     │   │ Type: SetVariable        │
│ v_status = "succeeded"│   │ v_status = "failed"      │
└──────────┬────────────┘   └────────────┬─────────────┘
           │ Succeeded/Skipped           │ Succeeded/Skipped
           └──────────────┬─────────────┘
                          │ (BOTH must be Succeeded OR Skipped)
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_write_audit                                                  │
│ Type: Copy Activity                                              │
│ Always runs — whether entity succeeded or failed                 │
│                                                                  │
│ SOURCE (ds_pipeline_audit_csv_noheader):                         │
│   bronze/audit/pipeline_audit.csv (no header schema)            │
│   additionalColumns inject dynamic values:                       │
│     pipeline_name   = "pl_bronze_api_master_v4"                 │
│     entity_name     = p_entity_name                              │
│     load_type       = p_load_type                                │
│     watermark_value = v_watermark                                │
│     ingestion_date  = v_ingestion_date                           │
│     total_pages     = v_total_pages                              │
│     status          = v_status  ("succeeded" or "failed")       │
│     pipeline_run_id = pipeline().RunId                           │
│     run_timestamp   = utcNow()                                   │
│                                                                  │
│ SINK (ds_pipeline_audit_csv):                                    │
│   Appends one row to bronze/audit/pipeline_audit.csv             │
│                                                                  │
│ Result row example:                                              │
│   pl_bronze_api_master_v4, payments, incremental,               │
│   2026-07-09T00:00:00Z, 2026-07-10, 641, succeeded,             │
│   <run-id>, 2026-07-10T06:00:00Z                                 │
└──────────────────────────────────────────────────────────────────┘
```

---

## Variable Lifecycle (per child pipeline run)

| Variable | Initial | Set by | Used by |
|---|---|---|---|
| `v_token` | "" | `act_set_token` | All API calls (Authorization header) |
| `v_ingestion_date` | "" | `act_set_ingestion_date` | Sink folder path + audit row |
| `v_watermark` | "1900-01-01T00:00:00Z" | `act_set_watermark` | API `updated_after` param + audit row |
| `v_total_pages` | 1 | `act_set_total_pages` | Until loop exit condition + audit row |
| `v_current_page` | 1 | `act_increment_page` | REST URL page param + sink file name |
| `v_temp_page` | 1 | `act_set_temp_page` | Intermediate for incrementing `v_current_page` |
| `v_status` | "started" | `act_set_status_success` / `act_set_status_failed` | Audit row |

---

## Why Two SetVariable Activities to Increment a Page?

ADF does not allow assigning a variable to itself in a single step:

```
❌  v_current_page = v_current_page + 1   ← not allowed in ADF
```

Workaround using a temp variable:

```
✅  Step 1: v_temp_page    = v_current_page + 1
    Step 2: v_current_page = v_temp_page
```

This is a known ADF limitation — not a bug in the pipeline design.

---

## Why Two Status Activities?

`act_set_status_success` and `act_set_status_failed` are **mutually exclusive**:

```
act_paginate
    ├── Succeeded → act_set_status_success runs, act_set_status_failed SKIPPED
    └── Failed    → act_set_status_failed  runs, act_set_status_success SKIPPED
```

`act_write_audit` depends on BOTH with conditions `["Succeeded", "Skipped"]`:

```
act_write_audit dependsOn:
  act_set_status_success → Succeeded OR Skipped  ✅
  act_set_status_failed  → Succeeded OR Skipped  ✅
```

This guarantees `act_write_audit` **always runs** — whether the entity
ingestion succeeded or failed — so the audit log is never missing a row.

---

## Bronze Output Structure

After a successful full load run, Bronze ADLS looks like:

```
bronze/
├── config/
│   └── pipeline_metadata_config.json     ← metadata read by master
├── audit/
│   └── pipeline_audit.csv                ← one row per entity per run
└── api/
    ├── payments/
    │   └── ingestion_date=2026-07-10/
    │       ├── page_1.json               ← 500 records
    │       ├── page_2.json               ← 500 records
    │       └── page_641.json             ← remaining records
    ├── sessions/
    │   └── ingestion_date=2026-07-10/
    │       ├── page_1.json
    │       └── ...
    ├── customers/  ...
    ├── fleet/      ...
    ├── chargers/   ...
    ├── vehicles/   ...
    ├── stations/   ...
    ├── complaints/ ...
    ├── maintenance_events/ ...
    ├── energy_prices/      ...
    ├── tariffs/    ...
    ├── charge_cards/ ...
    ├── employees/  ...
    ├── partners/   ...
    ├── cities/     ...
    ├── states/     ...
    └── weather/    ...
```

Each `page_N.json` file contains the full API response:

```json
{
  "pagination": {
    "page": 1,
    "page_size": 500,
    "total": 320035,
    "total_pages": 641
  },
  "data": [
    { "payment_id": "PAY-001", "amount_aud": "443.73", ... },
    { "payment_id": "PAY-002", "amount_aud": "94.03",  ... }
  ]
}
```

---

## Full vs Incremental — What Changes

| | Full Load | Incremental Load |
|---|---|---|
| `p_load_type` parameter | `full` | `incremental` |
| `v_watermark` value | `1900-01-01T00:00:00Z` | Last `watermark_value` from audit CSV |
| `updated_after` in API call | epoch (all records) | timestamp of last run |
| Pages fetched | all pages | only pages with new/changed records |
| Bronze output | full history | delta since last run |
| Audit CSV | new row written | new row written (watermark advances) |

---

## Audit CSV — How Watermark Advances

After each run, `pipeline_audit.csv` gets one new row per entity:

```
pipeline_name,          entity_name, load_type,    watermark_value,       ingestion_date, total_pages, status,    pipeline_run_id,  run_timestamp
pl_bronze_api_master_v4,payments,    full,          1900-01-01T00:00:00Z,  2026-07-10,     641,         succeeded, <run-id>,         2026-07-10T06:00:00Z
pl_bronze_api_master_v4,sessions,    full,          1900-01-01T00:00:00Z,  2026-07-10,     401,         succeeded, <run-id>,         2026-07-10T06:00:00Z
```

On the **next incremental run**, `act_get_watermark` reads the **first row**
of this CSV. The `watermark_value` from that row becomes `updated_after`
in the API call — so only records updated after the last run are fetched.

> Important: The current v4 pipeline reads `firstRowOnly: true` from the
> audit CSV which always returns the header row's next record — for
> per-entity watermark tracking, a Databricks notebook lookup or filtered
> query would be more precise. This is improved in Day 6+.

---

## Dataset Roles

| Dataset | Used in | Role |
|---|---|---|
| `ds_pipeline_metadata_config` | Master — `act_read_metadata` | Read entity config JSON from ADLS |
| `ds_voltgrid_api_src_v4` | Child — `act_copy_entity_page` source | Parameterized REST source — one dataset for all 17 entities |
| `ds_bronze_api_sink_v4` | Child — `act_copy_entity_page` sink | Parameterized JSON sink — one dataset for all 17 entities |
| `ds_pipeline_audit_csv` | Child — `act_get_watermark` + `act_write_audit` sink | Read/write audit CSV with header |
| `ds_pipeline_audit_csv_noheader` | Child — `act_write_audit` source | Same CSV without header — prevents duplicate column name error in additionalColumns |
