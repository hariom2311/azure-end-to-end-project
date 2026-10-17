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
  Copy data1 (page 1)
  act_set_current_page_2  (v_current_page = 2)
  Until  ← exit: v_current_page >= total_pages + 1  ← BUG: body always runs once
    act_copy_page
    act_increment_page
    act_set_current_page
  act_update_watermark

Day 5 pipeline (pl_bronze_incremental_load_v2):
  Copy data1 (page 1)
  act_set_current_page_2  (v_current_page = 2)
  Until  ← exit: v_current_page > total_pages   ← FIX 1: exits before body when total=1
    act_relogin            ← FIX 2: fresh token every iteration (no nesting needed)
    act_set_loop_token     ← update v_token from relogin
    act_copy_page          ← depends on act_set_loop_token (uses fresh token)
    act_increment_page
    act_set_current_page
  act_update_watermark

Removed: p_token_refresh_every parameter, v_pages_since_refresh, v_next_refresh_counter
         (counter-based approach replaced by per-iteration relogin — simpler, no nesting)
```

---

## Bug 1 — 404 When Data Has Only One Page

### What Happened

When the API returns only 1 page of data, the Day 4 loop tried to fetch page 2 → 404 Not Found → pipeline failed.

### Root Cause

The Day 4 Until exit condition was:
```
@greaterOrEquals(variables('v_current_page'), add(variables('v_total_pages'), 1))
```

ADF evaluates the Until condition **before each iteration**. With `total_pages = 1` and `v_current_page = 2`:
- Condition: `2 >= (1 + 1)` → `2 >= 2` → `true` → loop should exit

However — ADF runs the loop body **at least once** before the first condition check. So `act_copy_page` for page 2 always ran, regardless of `total_pages`.

### Fix — Change the Exit Condition

The new exit condition is:
```
@greater(variables('v_current_page'), variables('v_total_pages'))
```

With `total_pages = 1` and `v_current_page = 2`:
- Condition: `2 > 1` → `true` → loop exits **before** the first iteration runs

No IfCondition wrapper needed. The Until loop itself handles the single-page case cleanly.

> **Why not IfCondition around the loop?** ADF does not allow a container activity (IfCondition, Until, ForEach) to be nested inside another container activity. Wrapping the Until inside an IfCondition causes the error: `Container activity cannot include another container activity`. The exit condition fix is the correct ADF-native approach.

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
                                         Copy data1
                                         (page 1, always)
                                             │
                                             ▼
                                  act_set_current_page_2
                                  (v_current_page = 2)
                                             │
                                             ▼
                              ┌──── Until ──────────────────────────┐
                              │  Exit BEFORE body:                  │
                              │  v_current_page > v_total_pages     │
                              │  (exits immediately if total = 1)   │
                              │                                     │
                              │  act_relogin  ← fresh login POST    │
                              │  act_set_loop_token ← update v_token│
                              │  act_copy_page ← copy page N        │
                              │  act_increment_page                 │
                              │  act_set_current_page               │
                              └──────────── ↑ loop back ────────────┘
                                             │
                                             ▼
                                    act_update_watermark
                                    (Databricks — always runs
                                     after Until, even if it
                                     exited immediately)
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
| 13 | `act_fetch_page1_meta` | Web GET | No | Get total_pages from page 1 |
| 14 | `act_set_total_pages` | SetVariable | No | Store total_pages |
| 15 | `Copy data1` | Copy | No | Copy page 1 → Bronze |
| 16 | `act_set_current_page_2` | SetVariable | No | Init loop counter to 2 |
| 17 | `Until` | Until | **YES — exit condition fixed** | Loop pages 2..N |
| ↳ | `act_relogin` | Web POST | **NEW** | Re-login every iteration |
| ↳ | `act_set_loop_token` | SetVariable | **NEW** | Update v_token from relogin |
| ↳ | `act_copy_page` | Copy | **YES — depends on act_set_loop_token** | Copy page N → Bronze |
| ↳ | `act_increment_page` | SetVariable | No | v_next_page = N + 1 |
| ↳ | `act_set_current_page` | SetVariable | No | v_current_page = v_next_page |
| 18 | `act_update_watermark` | Databricks | No | Write new watermark |

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

**Single-page guard:**
- [ ] Until exit condition is `@greater(variables('v_current_page'), variables('v_total_pages'))` (not `greaterOrEquals(..., add(..., 1))`)
- [ ] Test: set watermark to a recent timestamp so API returns 0 or 1 pages → pipeline must complete without 404

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
| `404 Not Found` on page 2 | Old exit condition `greaterOrEquals(..., add(...,1))` — loop body still ran when total=1 | Use `@greater(v_current_page, v_total_pages)` — exits before the first iteration when total=1 |
| `401 Unauthorized` after many pages | Token expired mid-run | `act_relogin` at the start of each loop iteration guarantees a fresh token per page |
| `act_relogin` fails — activity output not found | ADF cannot resolve `activity('act_get_username')` inside Until | This should work — ADF propagates main-canvas activity outputs into nested containers. Verify the activity name spelling matches exactly |
