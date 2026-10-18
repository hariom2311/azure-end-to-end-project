# pl_bronze_incremental_load_v2 — Pipeline Flow

**Pipeline:** `pl_bronze_incremental_load_v2`
**Purpose:** Hardened version of the Day 4 pipeline. Handles zero-page and single-page API responses without 404, and refreshes the auth token before every page copy to prevent expiry on long runs.

---

## What Changed from Day 4

| Area | Day 4 | Day 5 (v2) |
|---|---|---|
| Page 1 copy | Separate `Copy data1` activity — always ran, even when `total_pages=0` | Removed. Loop starts at page 1 |
| Loop start | `v_current_page = 2` | `v_current_page = 1` |
| Loop exit | `v_current_page >= total_pages + 1` (body ran at least once) | `v_current_page > total_pages` (checked BEFORE body) |
| Zero pages | `Copy data1` fetched page 1 → 404 | Loop exits immediately: `1 > 0 = true` |
| One page | Loop body ran once for page 2 → 404 | Loop copies page 1, then exits: `2 > 1 = true` |
| Token refresh | None — same token for entire run | `act_relogin` + `act_set_loop_token` run first each iteration |

---

## Quick Summary

```
STEP 1  Fetch credentials from Key Vault  (parallel)
STEP 2  Read config from control table via Databricks  (parallel)
STEP 3  Login to VoltGrid API → get token
STEP 4  Fetch page 1 via Web Activity → read total_pages from response
STEP 5  Init loop counter: v_current_page = 1
STEP 6  Until loop (exits BEFORE body when v_current_page > total_pages)
          └─ Re-login → get fresh token every iteration
          └─ Copy page v_current_page → Bronze ADLS
          └─ Increment counter
STEP 7  Update watermark (always runs, even if 0 pages copied)
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
                  │   → read            │                                   │
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
                                         │ act_set_current_    │
                                         │ page_1              │
                                         │ (v_current_page=1)  │
                                         └──────────┬──────────┘
                                                    │
                                                    ▼
                             ┌──────────────────────────────────────────┐
                             │              Until Loop                   │
                             │                                           │
                             │  ┌─ Exit condition checked FIRST ───────┐ │
                             │  │  v_current_page > v_total_pages      │ │
                             │  │                                       │ │
                             │  │  total=0: 1>0=true  → exits (0 files)│ │
                             │  │  total=1: 1>1=false → runs once      │ │
                             │  │  total=N: runs N times               │ │
                             │  └───────────────────────────────────────┘ │
                             │                                           │
                             │  ┌─────────────────────────────────────┐ │
                             │  │  act_relogin (Web POST /auth/login/) │ │
                             │  │  Fresh token every iteration         │ │
                             │  └──────────────┬──────────────────────┘ │
                             │                 │                         │
                             │                 ▼                         │
                             │  ┌─────────────────────────────────────┐ │
                             │  │  act_set_loop_token                 │ │
                             │  │  (v_token = relogin.output.token)   │ │
                             │  └──────────────┬──────────────────────┘ │
                             │                 │                         │
                             │                 ▼                         │
                             │  ┌─────────────────────────────────────┐ │
                             │  │  act_copy_page  (Copy Activity)     │ │
                             │  │  Source: page=v_current_page        │ │
                             │  │  Authorization: Token v_token       │ │
                             │  │  Sink: <prefix>_page_N.json         │ │
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
| 7 | `act_set_sink_path` | SetVariable | 3 | Stores Bronze base path |
| 8 | `act_set_file_prefix` | SetVariable | 3 | Stores file prefix |
| 9 | `act_set_page_size` | SetVariable | 3 | Stores page size |
| 10 | `act_set_watermark` | SetVariable | 3 | Sets `v_last_watermark` |
| 11 | `act_set_ingestion_date` | SetVariable | 3 | Sets `v_ingestion_date` = today |
| 12 | `act_set_run_ts` | SetVariable | 3 | Sets `v_run_ts` = pipeline start UTC |
| 13 | `act_fetch_page1_meta` | Web GET | 5, 6, 9, 10 | Fetches page 1, reads `total_pages` |
| 14 | `act_set_total_pages` | SetVariable | 13 | Stores `total_pages` in `v_total_pages` |
| 15 | `act_set_current_page_1` | SetVariable | 14 + 5–12 | **Inits loop at page 1** (replaces `Copy data1` + `act_set_current_page_2`) |
| 16 | `Until` loop | Until | 15 | Loops all pages 1 → N, exits immediately if N=0 |
| ↳ | `act_relogin` | Web POST (inside loop) | — | **Fresh login every iteration** |
| ↳ | `act_set_loop_token` | SetVariable (inside loop) | `act_relogin` | Updates `v_token` with fresh token |
| ↳ | `act_copy_page` | Copy (inside loop) | `act_set_loop_token` | Copies page `v_current_page` → Bronze |
| ↳ | `act_increment_page` | SetVariable (inside loop) | `act_copy_page` | `v_next_page = current + 1` |
| ↳ | `act_set_current_page` | SetVariable (inside loop) | `act_increment_page` | `v_current_page = v_next_page` |
| 17 | `act_update_watermark` | Databricks Notebook | Until | Writes `v_run_ts` → control table |

---

## Variables Reference

| Variable | Type | Set By | Purpose |
|---|---|---|---|
| `v_token` | String | `act_set_token`, `act_set_loop_token` | Auth token — refreshed every iteration |
| `v_endpoint` | String | `act_set_endpoint` | API endpoint path |
| `v_sink_base_path` | String | `act_set_sink_path` | Bronze folder base path |
| `v_file_prefix` | String | `act_set_file_prefix` | File name prefix |
| `v_page_size` | String | `act_set_page_size` | Records per page |
| `v_last_watermark` | String | `act_set_watermark` | `updated_after` filter value |
| `v_ingestion_date` | String | `act_set_ingestion_date` | Partition folder date |
| `v_run_ts` | String | `act_set_run_ts` | Pipeline start UTC — becomes new watermark |
| `v_total_pages` | Integer | `act_set_total_pages` | Total pages from API |
| `v_current_page` | Integer | `act_set_current_page_1`, `act_set_current_page` | Loop counter — starts at 1 |
| `v_next_page` | Integer | `act_increment_page` | Temp for increment (self-reference rule) |

---

## Zero / One / Many Pages — How the Exit Condition Works

ADF evaluates the Until exit condition **before** each iteration (including the very first). This is what makes starting at page 1 safe:

```
v_current_page = 1   (set by act_set_current_page_1)

Iteration 0 — check condition FIRST:
  total_pages=0 → 1 > 0 = true  → EXIT  (loop body never runs, 0 files)
  total_pages=1 → 1 > 1 = false → RUN body (copy page 1)
  total_pages=N → 1 > N = false → RUN body (copy page 1)

After iteration for page 1:
  v_current_page = 2

Iteration 1 — check condition FIRST:
  total_pages=1 → 2 > 1 = true  → EXIT  (only page 1 was copied)
  total_pages=N → 2 > N = false → RUN body (copy page 2)

...continues until v_current_page = N+1 → N+1 > N = true → EXIT
```

---

## Sink Output Pattern

```
bronze/
└── api/
    └── payments/              ← or fleet/ or weather/
        └── ingestion_date=2026-10-18/
            ├── payments_page_1.json   ← loop iteration 1
            ├── payments_page_2.json   ← loop iteration 2
            │   ...
            └── payments_page_N.json  ← loop iteration N
```

> Page 1 is now named `payments_page_1.json` (via `act_copy_page` with `v_current_page=1`), consistent with all other pages. In Day 4, page 1 was also named `payments_page_1.json` but written by `Copy data1`.

---

## ADF Container Nesting Rule

> ADF allows **one level** of container nesting (Until, IfCondition, ForEach, Switch).
> Placing an IfCondition inside an Until that is already inside an IfCondition = two levels = error:
> `Container activity cannot include another container activity`

This is why:
- The single-page guard uses the **exit condition** (not an IfCondition wrapper)
- The token refresh uses **per-iteration relogin** (not a conditional check inside the loop)
