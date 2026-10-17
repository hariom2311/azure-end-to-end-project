# Day 5 — Pipeline Hardening: Single-Page Guard + Token Refresh

**Duration:** 1.5 hours | **Goal:** Fix two production bugs discovered during Day 4 testing — 404 error when data has only one page, and token expiry on long multi-hour runs.

> **Prerequisites:** Day 4 complete — `pl_bronze_incremental_load` running and writing to Bronze. Day 5 introduces `pl_bronze_incremental_load_v2`, a hardened replacement with both fixes applied.

---

## BEFORE YOU START — Student Customization Guide

Same values as Day 4. Replace all instructor-specific values before publishing.

| Where | What to Change | Instructor's Value |
|---|---|---|
| `act_read_ctl` notebook path | Your Databricks notebook path | `/Users/hariomsuryawanshi68258@gmail.com/nb_read_control_table` |
| `act_update_watermark` notebook path | Your Databricks notebook path | `/Users/hariomsuryawanshi68258@gmail.com/nb_update_watermark` |
| `act_get_username` URL | Your Key Vault name | `kv-ev-intelligence-dev02` |
| `act_get_password` URL | Your Key Vault name | `kv-ev-intelligence-dev02` |
| `act_api_login` + `act_relogin` URL | API base URL | `https://ev-project-navy-mu.vercel.app` |
| `act_fetch_page1_meta` URL | API base URL | `https://ev-project-navy-mu.vercel.app` |
| Linked service names | Names you created in Day 2/4 | `ls_databricks`, `ls_voltgrid_api`, `ls_adls_bronze` |

---

## What Changed vs Day 4

```
Day 4 pipeline (pl_bronze_incremental_load):
  Copy data1 (page 1)           ← BUG: always runs even when total_pages = 0
  act_set_current_page_2  (v_current_page = 2)
  Until  ← exit: v_current_page >= total_pages + 1
    act_copy_page               ← BUG: ran when total_pages = 1 (page 2 = 404)
    act_increment_page
    act_set_current_page
  act_update_watermark

Day 5 pipeline (pl_bronze_incremental_load_v2):
  act_set_current_page_1  (v_current_page = 1)  ← replaces Copy data1 on main canvas
  Until  ← exit: v_current_page > total_pages   ← FIX 1: exits BEFORE body if total=0
    act_relogin            ← FIX 2: fresh token every iteration (no nesting needed)
    act_set_loop_token     ← update v_token from relogin
    act_copy_page          ← copies page v_current_page (1, 2, 3 ... N)
    act_increment_page
    act_set_current_page
  act_update_watermark

Key design change: page 1 is now copied INSIDE the loop (not by a separate Copy data1).
Loop starts at page 1, handles all pages uniformly, exits before running when total=0.
```

---

## Bug 1 — 404 When Data Has Zero or One Page

### What Happened

The error `{"detail":"Invalid page."}` with HTTP 404 occurs when the API is asked for a page that doesn't exist. Two scenarios trigger this:

- **Zero pages** (`total_pages = 0`): No records match the watermark filter. Day 4's `Copy data1` always ran page 1 → 404 because page 1 doesn't exist.
- **One page** (`total_pages = 1`): Day 4's Until loop always started at page 2 → 404 because only page 1 existed.

### Root Cause

Day 4 had `Copy data1` (always copies page 1) as a separate activity BEFORE the Until loop. This meant page 1 was always fetched regardless of `total_pages`. When `total_pages = 0`, even page 1 returns 404.

### Fix — Move Page 1 Inside the Loop, Start at Page 1

Instead of a separate `Copy data1`, the loop now starts at `v_current_page = 1` and handles all pages (including page 1) uniformly.

**Exit condition:**
```
@greater(variables('v_current_page'), variables('v_total_pages'))
```

ADF evaluates this condition **before** each iteration (including the very first). So:

| total_pages | v_current_page at start | Condition | Result |
|---|---|---|---|
| `0` | `1` | `1 > 0 = true` | Loop exits immediately — nothing copied |
| `1` | `1` | `1 > 1 = false` | Copies page 1, increments to 2, `2 > 1 = true` → exits |
| `321` | `1` | `1 > 321 = false` | Copies pages 1–321, exits when `322 > 321` |

No IfCondition wrapper, no extra activities — just the loop handles every case.

> **Why not IfCondition around the loop?** ADF does not allow a container activity nested inside another container activity. Wrapping Until inside IfCondition causes: `Container activity cannot include another container activity`.

---

## Bug 2 — Token Expiry on Long Runs

### What Happened

The VoltGrid API token fetched at pipeline start has a limited lifetime. For large datasets with hundreds of pages, the token expires mid-loop and `act_copy_page` gets `401 Unauthorized`.

### Root Cause

Token was fetched once (`act_api_login` → `act_set_token`) and never refreshed. `v_token` stayed unchanged for the entire run.

### Fix — Re-login Every Iteration

The simplest correct fix: call `act_relogin` (POST to login) at the **start of every loop iteration**, before `act_copy_page`. This guarantees a fresh token for every page, regardless of how long the run takes.

**Inside the Until loop (each iteration):**
```
act_relogin           ← POST /api/auth/login/ → fresh token  (no dependency, runs first)
act_set_loop_token    ← v_token = act_relogin.output.token   (depends on act_relogin)
act_copy_page         ← uses the just-refreshed v_token       (depends on act_set_loop_token)
act_increment_page    ← v_next_page = v_current_page + 1
act_set_current_page  ← v_current_page = v_next_page
```

**Why re-login every page and not every N pages?**

The counter-based approach (`v_pages_since_refresh >= N`) requires an IfCondition inside the Until loop to conditionally call `act_relogin`. ADF does not allow IfCondition nested inside Until (both are container activities) — this triggers the same nesting error. The per-iteration approach avoids any nesting and is equally correct:

- One login POST ≈ 100–200ms overhead per page
- For 300 pages: ~30–60 seconds total extra time — negligible
- Token is guaranteed fresh for every Copy call regardless of API token lifetime
- Zero complexity, zero nesting, zero new variables needed

> `act_relogin` reuses `activity('act_get_username').output.value` and `activity('act_get_password').output.value` — outputs from the pipeline start activities. ADF makes main-canvas activity outputs available inside nested containers (Until, IfCondition). No second Key Vault call is needed.

---

## Full Pipeline Activity Graph

```
╔══════════════════════════════════════════════════════════════════╗
║                      PIPELINE START                              ║
╚══════════════════════════════════════════════════════════════════╝
    │                    │                    │
    ▼                    ▼                    ▼
act_get_username    act_get_password     act_read_ctl
(Web→KV)            (Web→KV)             (Databricks)
    │                    │                    │
    └──────┬─────────────┘      ┌─────────────┴──────────────────┐
           ▼                    ▼  (7 SetVars run in parallel)
      act_api_login      act_set_endpoint
           │             act_set_sink_path
           ▼             act_set_file_prefix
      act_set_token      act_set_page_size
           │             act_set_watermark
           └──────────┬──act_set_ingestion_date
                      │  act_set_run_ts
                      ▼  ◄── JOIN POINT 1
               act_fetch_page1_meta
                      │
                      ▼
               act_set_total_pages
                      │
                      └──────────────────────┬─────────────────────┘
                                             ▼  ◄── JOIN POINT 2
                                  act_set_current_page_1
                                  (v_current_page = 1)
                                             │
                                             ▼
                              ┌──── Until ──────────────────────────┐
                              │  Exit BEFORE body:                  │
                              │  v_current_page > v_total_pages     │
                              │                                     │
                              │  total=0: 1>0=true → exits, 0 files │
                              │  total=1: 1>1=false → copies pg 1,  │
                              │           2>1=true → exits, 1 file  │
                              │  total=N: copies pg 1..N, exits     │
                              │                                     │
                              │  act_relogin  ← fresh login POST    │
                              │  act_set_loop_token ← update v_token│
                              │  act_copy_page ← copy v_current_page│
                              │  act_increment_page                 │
                              │  act_set_current_page               │
                              └──────────── ↑ loop back ────────────┘
                                             │
                                             ▼
                                    act_update_watermark
                                    (Databricks — always runs
                                     after Until, even if loop
                                     exited with 0 iterations)
                                             │
                                             ▼
                                        PIPELINE END
```

---

## Activity Reference Table

| # | Activity | Type | Changed in v2? | Purpose |
|---|---|---|---|---|
| 1 | `act_get_username` | Web GET | No | Fetch username from KV |
| 2 | `act_get_password` | Web GET | No | Fetch password from KV |
| 3 | `act_read_ctl` | Databricks | No | Read control table config |
| 4–10 | `act_set_*` (7 SetVars) | SetVariable | No | Store config in variables |
| 11 | `act_api_login` | Web POST | No | Login at pipeline start |
| 12 | `act_set_token` | SetVariable | No | Store initial token |
| 13 | `act_fetch_page1_meta` | Web GET | No | Get total_pages from page 1 response |
| 14 | `act_set_total_pages` | SetVariable | No | Store total_pages |
| — | ~~`Copy data1`~~ | ~~Copy~~ | **REMOVED** | Moved inside loop as `act_copy_page` |
| 15 | `act_set_current_page_1` | SetVariable | **NEW (replaces act_set_current_page_2)** | Init loop counter to **1** (not 2) |
| 16 | `Until` | Until | **YES — exit condition + starts at page 1** | Loop ALL pages 1..N |
| ↳ | `act_relogin` | Web POST | **NEW** | Re-login every iteration — fresh token |
| ↳ | `act_set_loop_token` | SetVariable | **NEW** | Update v_token from relogin |
| ↳ | `act_copy_page` | Copy | **YES — now copies page 1 too** | Copy v_current_page → Bronze |
| ↳ | `act_increment_page` | SetVariable | No | v_next_page = current + 1 |
| ↳ | `act_set_current_page` | SetVariable | No | v_current_page = v_next_page |
| 17 | `act_update_watermark` | Databricks | No | Write new watermark |

---

## Variables Reference

| Variable | Type | Set By | Used By |
|---|---|---|---|
| `v_token` | String | `act_set_token`, `act_set_loop_token` | `act_fetch_page1_meta`, `Copy data1`, `act_copy_page` |
| `v_endpoint` | String | `act_set_endpoint` | `act_fetch_page1_meta`, Copy activities |
| `v_sink_base_path` | String | `act_set_sink_path` | Copy activities |
| `v_file_prefix` | String | `act_set_file_prefix` | Copy activities |
| `v_page_size` | String | `act_set_page_size` | `act_fetch_page1_meta`, Copy activities |
| `v_last_watermark` | String | `act_set_watermark` | `act_fetch_page1_meta`, Copy activities |
| `v_ingestion_date` | String | `act_set_ingestion_date` | Copy activities |
| `v_run_ts` | String | `act_set_run_ts` | `act_update_watermark` |
| `v_total_pages` | Integer | `act_set_total_pages` | Until exit condition |
| `v_current_page` | Integer | `act_set_current_page_2`, `act_set_current_page` | `act_copy_page`, Until condition |
| `v_next_page` | Integer | `act_increment_page` | `act_set_current_page` |

> `v_pages_since_refresh` and `v_next_refresh_counter` from the first Day 5 draft have been removed. The per-iteration relogin approach makes them unnecessary.

---

## ADF Container Nesting Rule

> **ADF allows only one level of container nesting.**
> Container activities: `Until`, `IfCondition`, `ForEach`, `Switch`
> You CAN put an Until inside an IfCondition (or vice versa) — that is one level.
> You CANNOT put an IfCondition inside an Until that is already inside an IfCondition — that is two levels and ADF rejects it with: `Container activity cannot include another container activity`.

This is why the counter-based token refresh (which needed `IfCondition` inside `Until`) was replaced with per-iteration relogin (no nesting at all).

---

## Verify: Day 5 Checklist

**Single-page / zero-page guard:**
- [ ] `Copy data1` removed from main canvas
- [ ] `act_set_current_page_1` sets `v_current_page = 1` (not 2)
- [ ] Until exit condition is `@greater(variables('v_current_page'), variables('v_total_pages'))`
- [ ] `act_copy_page` inside loop uses `@string(variables('v_current_page'))` for `p_page` — copies pages 1, 2, 3 … N
- [ ] Test zero pages: set watermark to `utcnow()` — pipeline completes with 0 files copied, no 404
- [ ] Test one page: set watermark so only a handful of records exist — pipeline copies exactly 1 file, no 404

**Token refresh:**
- [ ] `act_relogin` is first activity inside Until (no dependency)
- [ ] `act_set_loop_token` depends on `act_relogin`, sets `v_token`
- [ ] `act_copy_page` depends on `act_set_loop_token`
- [ ] `act_relogin` body uses `activity('act_get_username').output.value` and `activity('act_get_password').output.value`

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `Container activity cannot include another container activity` | IfCondition nested inside Until (or Until inside IfCondition inside Until) | ADF allows only one container level. Remove inner container — use exit condition logic or per-iteration calls instead |
| `404 Not Found` on page 1 (total=0) or page 2 (total=1) | Day 4 `Copy data1` always ran page 1; loop always started at 2 | v2: start loop at page 1, exit condition `@greater(v_current_page, v_total_pages)` — handles 0 and 1 page with no copies |
| `401 Unauthorized` after many pages | Token expired mid-run | `act_relogin` at the start of each loop iteration guarantees a fresh token per page |
| `act_relogin` fails — activity output not found | ADF cannot resolve `activity('act_get_username')` inside Until | This should work — ADF propagates main-canvas activity outputs into nested containers. Verify the activity name spelling matches exactly |
