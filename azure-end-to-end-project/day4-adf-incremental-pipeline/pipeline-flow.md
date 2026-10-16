# pl_bronze_incremental_load — Pipeline Flow

**Pipeline:** `pl_bronze_incremental_load`
**Purpose:** Incrementally load payments data from VoltGrid API → Bronze ADLS Gen2, page by page, then update the watermark in the Delta control table.

---

## Quick Summary

```
STEP 1  Fetch credentials from Key Vault (parallel with Step 2)
STEP 2  Read config from control table via Databricks notebook (parallel with Step 1)
STEP 3  Login to VoltGrid API → get token
STEP 4  Fetch page 1 via Web Activity → read total_pages from response
STEP 5  Copy page 1 data → Bronze ADLS  (Copy data1)
STEP 6  Loop pages 2 → N → copy each → Bronze ADLS  (Until loop)
STEP 7  Update watermark in control table via Databricks notebook
```

---

## Full Pipeline Flow Diagram

```
╔══════════════════════════════════════════════════════════════════════════╗
║                        PIPELINE START                                    ║
║                   (3 activities run in parallel)                         ║
╚══════════════════════════════════════════════════════════════════════════╝
         │                    │                    │
         ▼                    ▼                    ▼
  ┌─────────────┐    ┌──────────────┐    ┌──────────────────┐
  │act_get_     │    │act_get_      │    │  act_read_ctl    │
  │username     │    │password      │    │  (Databricks     │
  │(Web→KV)     │    │(Web→KV)      │    │   Notebook)      │
  └──────┬──────┘    └──────┬───────┘    └────────┬─────────┘
         │                  │                      │
         └────────┬─────────┘           ┌──────────┴──────────────────────┐
                  │                     │ (all 7 run in parallel)          │
                  ▼                     ▼                                  │
          ┌──────────────┐    ┌──────────────────┐                         │
          │ act_api_login│    │ act_set_endpoint  │                         │
          │ (Web POST    │    │ act_set_sink_path │                         │
          │  /auth/login)│    │ act_set_file_     │                         │
          └──────┬───────┘    │   prefix          │                         │
                 │            │ act_set_page_size │                         │
                 ▼            │ act_set_watermark │                         │
          ┌──────────────┐    │ act_set_ingestion │                         │
          │ act_set_token│    │   _date           │                         │
          │ (v_token =   │    │ act_set_run_ts    │                         │
          │  API token)  │    └──────────┬────────┘                         │
          └──────┬───────┘              │                                   │
                 │                      │                                   │
                 └──────────┬───────────┘                                   │
                            │  ◄── JOIN POINT 1                             │
                            ▼                                               │
                  ┌─────────────────────┐                                   │
                  │  act_fetch_page1_   │                                   │
                  │  meta               │                                   │
                  │  (Web GET page 1    │                                   │
                  │   → read           │                                   │
                  │   total_pages)      │                                   │
                  └──────────┬──────────┘                                   │
                             │                                              │
                             ▼                                              │
                  ┌─────────────────────┐                                   │
                  │ act_set_total_pages │                                   │
                  │ (v_total_pages = N) │                                   │
                  └──────────┬──────────┘                                   │
                             │                                              │
                             └──────────────────────┬───────────────────────┘
                                                    │  ◄── JOIN POINT 2
                                                    ▼
                                         ┌─────────────────────┐
                                         │     Copy data1       │
                                         │  (Copy Activity)     │
                                         │  Source: REST API    │
                                         │    page=1            │
                                         │    Authorization:    │
                                         │    Token v_token     │
                                         │  Sink: ADLS Bronze   │
                                         │  payments_page_1.json│
                                         └──────────┬──────────┘
                                                    │
                                                    ▼
                                         ┌─────────────────────┐
                                         │ act_set_current_    │
                                         │ page_2              │
                                         │ (v_current_page = 2)│
                                         └──────────┬──────────┘
                                                    │
                                                    ▼
                             ┌──────────────────────────────────────────┐
                             │              Until Loop                   │
                             │  Exit: v_current_page > v_total_pages    │
                             │                                           │
                             │  ┌─────────────────────────────────────┐ │
                             │  │  act_copy_page  (Copy Activity)     │ │
                             │  │  Source: REST API page=v_current_   │ │
                             │  │    page  Authorization: Token       │ │
                             │  │    v_token                          │ │
                             │  │  Sink: payments_page_N.json         │ │
                             │  └──────────────┬──────────────────────┘ │
                             │                 │                         │
                             │                 ▼                         │
                             │  ┌─────────────────────────────────────┐ │
                             │  │  act_increment_page                 │ │
                             │  │  (v_next_page = v_current_page + 1) │ │
                             │  └──────────────┬──────────────────────┘ │
                             │                 │                         │
                             │                 ▼                         │
                             │  ┌─────────────────────────────────────┐ │
                             │  │  act_set_current_page               │ │
                             │  │  (v_current_page = v_next_page)     │ │
                             │  └─────────────────────────────────────┘ │
                             │         ↑ loop back ↑                     │
                             └──────────────────────────────────────────┘
                                                    │
                                                    │  (loop exits when
                                                    │   v_current_page > N)
                                                    ▼
                                         ┌─────────────────────┐
                                         │  act_update_        │
                                         │  watermark          │
                                         │  (Databricks        │
                                         │   Notebook)         │
                                         │  writes v_run_ts    │
                                         │  → control table    │
                                         └─────────────────────┘
                                                    │
                                                    ▼
                                              PIPELINE END
```

---

## Activity Reference Table

| # | Activity Name | Type | Depends On | What It Does |
|---|---|---|---|---|
| 1 | `act_get_username` | Web GET | — | Fetches username from Key Vault |
| 2 | `act_get_password` | Web GET | — | Fetches password from Key Vault |
| 3 | `act_read_ctl` | Databricks Notebook | — | Reads config row from control table |
| 4 | `act_api_login` | Web POST | 1, 2 | Logs in to VoltGrid API, gets token |
| 5 | `act_set_token` | SetVariable | 4 | Stores token in `v_token` |
| 6 | `act_set_endpoint` | SetVariable | 3 | Stores API endpoint in `v_endpoint` |
| 7 | `act_set_sink_path` | SetVariable | 3 | Stores Bronze base path in `v_sink_base_path` |
| 8 | `act_set_file_prefix` | SetVariable | 3 | Stores file prefix in `v_file_prefix` |
| 9 | `act_set_page_size` | SetVariable | 3 | Stores page size in `v_page_size` |
| 10 | `act_set_watermark` | SetVariable | 3 | Sets `v_last_watermark` (full or incremental) |
| 11 | `act_set_ingestion_date` | SetVariable | 3 | Sets `v_ingestion_date` = today |
| 12 | `act_set_run_ts` | SetVariable | 3 | Sets `v_run_ts` = pipeline start UTC |
| 13 | `act_fetch_page1_meta` | Web GET | 5, 6, 9, 10 | Fetches page 1, reads `total_pages` |
| 14 | `act_set_total_pages` | SetVariable | 13 | Stores `total_pages` in `v_total_pages` |
| 15 | `Copy data1` | Copy | 5–12, 14 | Copies page 1 data → Bronze ADLS |
| 16 | `act_set_current_page_2` | SetVariable | 15 | Initialises loop counter to 2 |
| 17 | `Until` loop | Until | 16 | Loops pages 2 → N |
| ↳ | `act_copy_page` | Copy (inside loop) | — | Copies page N → Bronze ADLS |
| ↳ | `act_increment_page` | SetVariable (inside loop) | `act_copy_page` | `v_next_page = N + 1` |
| ↳ | `act_set_current_page` | SetVariable (inside loop) | `act_increment_page` | `v_current_page = v_next_page` |
| 18 | `act_update_watermark` | Databricks Notebook | 17 | Writes `v_run_ts` → control table |

---

## Variables Reference

| Variable | Type | Set By | Used By | Example Value |
|---|---|---|---|---|
| `v_token` | String | `act_set_token` | `Copy data1`, `act_copy_page`, `act_fetch_page1_meta` | `abc123xyz` |
| `v_endpoint` | String | `act_set_endpoint` | `act_fetch_page1_meta`, `Copy data1`, `act_copy_page` | `/api/db/payments/` |
| `v_sink_base_path` | String | `act_set_sink_path` | `Copy data1`, `act_copy_page` | `api/payments` |
| `v_file_prefix` | String | `act_set_file_prefix` | `Copy data1`, `act_copy_page` | `payments` |
| `v_page_size` | String | `act_set_page_size` | `act_fetch_page1_meta`, `Copy data1`, `act_copy_page` | `1000` |
| `v_last_watermark` | String | `act_set_watermark` | `act_fetch_page1_meta`, `Copy data1`, `act_copy_page` | `2026-10-14T02:00:00Z` |
| `v_ingestion_date` | String | `act_set_ingestion_date` | `Copy data1`, `act_copy_page` | `2026-10-16` |
| `v_run_ts` | String | `act_set_run_ts` | `act_update_watermark` | `2026-10-16T02:00:00Z` |
| `v_total_pages` | Integer | `act_set_total_pages` | Until exit condition | `321` |
| `v_current_page` | Integer | `act_set_current_page_2`, `act_set_current_page` | `act_copy_page`, Until condition | `2`, `3`, ... `321` |
| `v_next_page` | Integer | `act_increment_page` | `act_set_current_page` | `3`, `4`, ... `322` |

> **Why `v_next_page` exists:** ADF does not allow a SetVariable activity to read and write the same variable in one step. Writing `v_current_page = v_current_page + 1` throws a self-reference error. The workaround: write the incremented value to `v_next_page` first, then copy it to `v_current_page` in a second activity.

---

## Authorization: Why Two Different Approaches

| Activity | Auth Method | Why |
|---|---|---|
| `act_fetch_page1_meta` (Web Activity) | `headers: { authorization: "@concat('Token ', variables('v_token'))" }` | Web Activity headers evaluate ADF expressions |
| `Copy data1` (Copy Activity) | `additionalHeaders` in source block with `@concat(...)` | Copy Activity source `additionalHeaders` also evaluates expressions |
| `act_copy_page` (Copy Activity inside loop) | Same as `Copy data1` — `additionalHeaders` in source | Same reason |

> **Important:** Dataset `additionalHeaders` and linked service `Auth headers` do NOT reliably evaluate ADF expressions for dynamic tokens. The working solution is to put the `Authorization` header directly in the Copy Activity source's `additionalHeaders` block.

---

## Sink Output Pattern

```
bronze/
└── api/
    └── payments/
        └── ingestion_date=2026-10-16/
            ├── payments_page_1.json      ← Copy data1 (hardcoded page 1)
            ├── payments_page_2.json      ← Until loop iteration 1
            ├── payments_page_3.json      ← Until loop iteration 2
            │   ...
            └── payments_page_321.json   ← Until loop iteration 320
```

**Expressions that build file names:**

| Activity | p_folder | p_file |
|---|---|---|
| `Copy data1` | `@concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'))` | `@concat(variables('v_file_prefix'), '_page_1.json')` |
| `act_copy_page` | same | `@concat(variables('v_file_prefix'), '_page_', string(variables('v_current_page')), '.json')` |

---

## Watermark Logic

```
Pipeline triggered
       │
       ▼
p_load_type_override == 'full'?
  ├── YES → v_last_watermark = '1900-01-01T00:00:00Z'  (fetch ALL records ever)
  └── NO  → v_last_watermark = last_watermark from control table
                               (fetch only records updated since last run)
       │
       ▼
API called with ?updated_after=v_last_watermark
       │
       ▼
All pages written to Bronze successfully?
  ├── YES → act_update_watermark writes v_run_ts → control table
  │          (next run will use this as the new starting point)
  └── NO  → watermark NOT updated → next run retries from same watermark
```

> `v_run_ts` is captured at pipeline **start** (not end) so any records updated during the run are caught in the next incremental fetch. No data is missed.

---

## Two Join Points Explained

**Join Point 1 — `act_fetch_page1_meta`**

This activity cannot run until BOTH parallel branches finish:
- Token branch must complete: `act_get_username` + `act_get_password` → `act_api_login` → `act_set_token`
- Config branch must complete: `act_read_ctl` → `act_set_endpoint`, `act_set_page_size`, `act_set_watermark`

It needs the token to authenticate and the endpoint/params to build the URL.

**Join Point 2 — `Copy data1`**

This activity cannot run until ALL variables are set:
- From Join Point 1: `act_set_total_pages`
- From config branch: `act_set_endpoint`, `act_set_sink_path`, `act_set_file_prefix`, `act_set_page_size`, `act_set_watermark`, `act_set_ingestion_date`, `act_set_run_ts`
- From auth branch: `act_set_token` (via the chain through `act_fetch_page1_meta`)
