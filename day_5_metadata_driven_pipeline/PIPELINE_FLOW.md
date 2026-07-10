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
│ Dataset: ds_pipeline_metadata_config                │
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

### Full Activity Flow (14 activities total)

```
┌──────────────────────────────────────────────────────────────────┐
│ act_get_username                                                 │
│ Type: Web Activity (GET)                                         │
│ Dataset: none — direct HTTPS call                                │
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
│ Dataset: none — direct HTTPS call                                │
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
│ Dataset: none — direct HTTPS call                                │
│ URL : https://ev-project-navy-mu.vercel.app/api/auth/login/      │
│ Body: { username: <from KV>, password: <from KV> }              │
│ Returns: { token: "abc123..." }                                  │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_set_token                                                    │
│ Type: SetVariable (no dataset)                                   │
│ v_token ← activity('act_api_login').output.token                 │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_set_ingestion_date                                           │
│ Type: SetVariable (no dataset)                                   │
│ v_ingestion_date ← formatDateTime(utcNow(), 'yyyy-MM-dd')        │
│ Example: "2026-07-10"                                            │
│ Used as the Bronze partition folder name for every page          │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_get_watermark                                                │
│ Type: Lookup                                                     │
│ Dataset: ds_pipeline_audit_entity_csv (p_entity_name passed in) │
│ Reads: bronze/audit/watermark_<entity_name>.csv                  │
│ Returns: { watermark_value: "2026-07-10T00:00:00Z", ... }       │
│                                                                  │
│ WHY per-entity file: old shared pipeline_audit.csv with          │
│ firstRowOnly always returned row 1 (epoch) for ALL entities —    │
│ making every incremental run a full load (4+ hours)             │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_set_watermark                                                │
│ Type: SetVariable (no dataset)                                   │
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
│ Type: Web Activity (GET) — no dataset, inline URL expression     │
│ URL : concat(base_url, p_api_path,                               │
│              ?page=1&page_size=500&updated_after=v_watermark)    │
│ Header: Authorization: Token <v_token>                           │
│ Returns: { pagination: { total_pages: 3, total: 1200 } }        │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_set_total_pages                                              │
│ Type: SetVariable (no dataset)                                   │
│ v_total_pages ← output.pagination.total_pages                    │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_paginate  (Until Loop)                                       │
│ Exit condition: v_current_page > v_total_pages                   │
│ Timeout: 12 hours                                                │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ act_copy_entity_page                                       │  │
│  │ Type: Copy                                                 │  │
│  │ Source: ds_voltgrid_api_src_v4  (REST, parameterized)      │  │
│  │ Sink:   ds_bronze_api_sink_v4   (JSON, parameterized)      │  │
│  │                                                            │  │
│  │ Fetches: api_path?page=N&page_size=500&updated_after=wm    │  │
│  │ Writes:  bronze/api/<entity>/ingestion_date=<date>/        │  │
│  │          page_<N>.json                                     │  │
│  └───────────────────────────┬────────────────────────────────┘  │
│                              │ Succeeded                         │
│  ┌───────────────────────────▼────────────────────────────────┐  │
│  │ act_set_temp_page   v_temp_page = v_current_page + 1       │  │
│  └───────────────────────────┬────────────────────────────────┘  │
│                              │ Succeeded                         │
│  ┌───────────────────────────▼────────────────────────────────┐  │
│  │ act_increment_page  v_current_page = v_temp_page           │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────┬──────────────────────────┬───────────────────────────┘
           │ Succeeded                │ Failed
           ▼                          ▼
┌──────────────────────┐   ┌──────────────────────────┐
│ act_set_status_success│   │ act_set_status_failed    │
│ v_status="succeeded"  │   │ v_status="failed"        │
└──────────┬────────────┘   └────────────┬─────────────┘
           │ Succeeded/Skipped           │ Succeeded/Skipped
           └──────────────┬─────────────┘
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_write_audit  (always runs — success or failure)              │
│ Type: Copy                                                       │
│ Source: ds_audit_template_csv  ← single-newline, no columns     │
│ Sink:   ds_pipeline_audit_csv  ← append-only history CSV        │
│                                                                  │
│ additionalColumns inject 9 columns from pipeline context:        │
│   pipeline_name, entity_name, load_type, watermark_value,        │
│   ingestion_date, total_pages, status, pipeline_run_id,          │
│   run_timestamp                                                  │
│                                                                  │
│ Appends one row to bronze/audit/pipeline_audit.csv               │
└───────────────────────────┬──────────────────────────────────────┘
                            │ Succeeded + act_set_status_success Succeeded
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ act_write_watermark  (only runs on success)                      │
│ Type: Copy                                                       │
│ Source: ds_audit_template_csv       ← same single-newline file   │
│ Sink:   ds_pipeline_audit_entity_csv ← per-entity watermark CSV  │
│                                                                  │
│ additionalColumns inject 3 columns:                              │
│   watermark_value = utcNow()  ← new watermark for next run      │
│   entity_name     = p_entity_name                                │
│   updated_at      = utcNow()                                     │
│                                                                  │
│ OVERWRITES bronze/audit/watermark_<entity_name>.csv              │
│ Failed runs skip this → watermark stays at previous value        │
└──────────────────────────────────────────────────────────────────┘
```

---

## Dataset Reference Guide

Every dataset used in v4 — what it is, where it points, and which activity uses it.

### ds_pipeline_metadata_config
| Property | Value |
|---|---|
| Type | JSON (ADLS Gen2) |
| Path | `bronze/config/pipeline_metadata_config.json` |
| Direction | Read only |
| Used by | Master — `act_read_metadata` (Lookup) |
| What it contains | Array of 17 entity objects: `entity_name`, `api_path`, `page_size`, `enabled` |
| Why it exists | Single config file drives the entire ForEach — add a new entity here, no pipeline changes needed |

### ds_voltgrid_api_src_v4
| Property | Value |
|---|---|
| Type | REST (linked service: `ls_voltgrid_api`) |
| Base URL | `https://ev-project-navy-mu.vercel.app` |
| Direction | Read (source) |
| Used by | Child — `act_copy_entity_page` (Copy source) |
| Parameters | `p_api_path`, `p_page`, `p_page_size`, `p_updated_after` |
| Dynamic URL | `concat(p_api_path, '?page=', p_page, '&page_size=', p_page_size, '&updated_after=', p_updated_after)` |
| Why it exists | One generic REST dataset replaces 17 entity-specific datasets |

### ds_bronze_api_sink_v4
| Property | Value |
|---|---|
| Type | JSON (ADLS Gen2, linked service: `ls_adls_bronze`) |
| Container | `bronze` |
| Dynamic path | `api/<p_entity_name>/ingestion_date=<p_ingestion_date>/page_<p_page>.json` |
| Direction | Write (sink) |
| Used by | Child — `act_copy_entity_page` (Copy sink) |
| Parameters | `p_entity_name`, `p_ingestion_date`, `p_page` |
| Why it exists | One generic JSON sink replaces 17 entity-specific datasets |

### ds_pipeline_audit_entity_csv
| Property | Value |
|---|---|
| Type | CSV (ADLS Gen2) |
| Container | `bronze` |
| Dynamic path | `audit/watermark_<p_entity_name>.csv` |
| Direction | Read (Lookup) + Write (Copy sink) |
| Used by | `act_get_watermark` (read) + `act_write_watermark` (write) |
| Parameters | `p_entity_name` |
| firstRowAsHeader | `true` |
| What it contains | Single data row per file: `watermark_value`, `entity_name`, `updated_at` |
| Why it exists | Per-entity watermark. Fixes the shared CSV bug where `firstRowOnly` returned epoch for all entities, causing every incremental run to be a full load |

### ds_pipeline_audit_csv
| Property | Value |
|---|---|
| Type | CSV (ADLS Gen2) |
| Container | `bronze` |
| Fixed path | `audit/pipeline_audit.csv` |
| Direction | Write (sink, append) |
| Used by | Child — `act_write_audit` (Copy sink) |
| firstRowAsHeader | `true` |
| What it contains | Append-only history: `pipeline_name`, `entity_name`, `load_type`, `watermark_value`, `ingestion_date`, `total_pages`, `status`, `pipeline_run_id`, `run_timestamp` |
| Why it exists | Audit trail — never overwritten, one row per entity per run |

### ds_audit_template_csv
| Property | Value |
|---|---|
| Type | CSV (ADLS Gen2) |
| Container | `bronze` |
| Fixed path | `audit/audit_template.csv` |
| Direction | Read (source only) |
| Used by | `act_write_audit` source + `act_write_watermark` source |
| firstRowAsHeader | `false` |
| What it contains | A single newline — no columns, no data |
| Why it exists | ADF Copy needs a source dataset even when all output columns come from `additionalColumns`. A blank file with `firstRowAsHeader: false` makes ADF read 1 empty row with zero source columns. This prevents `Prop_0..Prop_N` ghost columns appearing in the output and prevents the `QuoteAllText` conflict error that occurs when source header mode mismatches sink header mode. |

---

## Variable Lifecycle

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

## Watermark Design

### The bug this fixed
`firstRowOnly: true` on a shared `pipeline_audit.csv` always returned **row 1** (epoch) for ALL entities — every incremental run fetched all pages (4+ hours).

### The fix
One `watermark_<entity>.csv` per entity. Single-row file always returns the correct value for that entity.

### How watermark advances
```
Run N   → act_get_watermark reads  watermark_payments.csv → "2026-07-10T00:00:00Z"
        → fetch only pages updated after that timestamp
        → act_write_watermark overwrites watermark_payments.csv → "2026-07-10T14:40:16Z"

Run N+1 → act_get_watermark reads  watermark_payments.csv → "2026-07-10T14:40:16Z"
        → fetch only records updated after 14:40
```

Failed runs skip `act_write_watermark` — watermark stays at last successful value.

---

## Bronze Output Structure

```
bronze/
├── config/
│   └── pipeline_metadata_config.json
├── audit/
│   ├── pipeline_audit.csv          ← append-only history
│   ├── audit_template.csv          ← single newline, Copy source template
│   ├── watermark_payments.csv
│   ├── watermark_sessions.csv
│   └── watermark_<entity>.csv × 17
└── api/
    ├── payments/
    │   ├── ingestion_date=2026-07-10/
    │   │   ├── page_1.json
    │   │   └── page_N.json
    │   └── ingestion_date=2026-07-11/
    │       └── page_1.json
    └── <entity>/ × 17
```

---

## Full vs Incremental

| | Full Load | Incremental Load |
|---|---|---|
| `p_load_type` | `full` | `incremental` |
| `v_watermark` | `1900-01-01T00:00:00Z` | value from `watermark_<entity>.csv` |
| API `updated_after` | epoch — all records | timestamp of last successful run |
| Pages fetched | all | only new/changed |
| `watermark_<entity>.csv` | NOT updated | overwritten with `utcNow()` |
| `pipeline_audit.csv` | new row appended | new row appended |
