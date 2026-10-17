# Day 5 — Pipeline Hardening: Single-Page Guard + Token Refresh

**Duration:** 1.5 hours | **Goal:** Fix two production bugs discovered during Day 4 testing — 404 error when data has only one page, and token expiry on long multi-hour runs.

> **Prerequisites:** Day 4 complete — `pl_bronze_incremental_load` running and writing to Bronze. Day 5 is a drop-in replacement of the same pipeline with two targeted fixes applied.

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
Day 4 pipeline:                    Day 5 changes:
───────────────────────────────    ──────────────────────────────────────────
Copy data1 (page 1)                unchanged
├── act_set_current_page_2         MOVED inside IfCondition true branch
└── Until (always ran)             WRAPPED in act_check_has_more_pages
    └── act_copy_page              act_check_token_refresh added before it
    └── act_increment_page         unchanged
    └── act_set_current_page       unchanged
                                   act_increment_refresh_counter  (new)
                                   act_set_refresh_counter        (new)
                                   act_check_token_refresh        (new — IfCondition)
                                     act_relogin                  (new — inside If true)
                                     act_set_refreshed_token      (new — inside If true)
                                     act_reset_refresh_counter    (new — inside If true)

New pipeline parameter: p_token_refresh_every (int, default 50)
New variables: v_pages_since_refresh (Integer), v_next_refresh_counter (Integer)
```

---

## Bug 1 — 404 When Data Has Only One Page

### What Happened

The Day 4 Until loop always started at page 2, regardless of `v_total_pages`. When the API returns only 1 page of data (e.g. after a recent incremental run with very few new records), the loop tried to fetch page 2 → the API returned 404 Not Found → pipeline failed.

### Root Cause

```
Day 4 flow:
  Copy data1 (page 1)  ← OK
  act_set_current_page_2 (v_current_page = 2)
  Until (exit when v_current_page > v_total_pages)
    act_copy_page(page=2)  ← 404 if total_pages = 1
```

The exit condition `v_current_page >= v_total_pages + 1` was evaluated at the TOP of each iteration. When `total_pages = 1` and `v_current_page = 2`, the condition is `2 >= 2` = true — BUT ADF evaluates the Until exit condition AFTER the first iteration completes, not before it starts. So page 2 was always fetched at least once.

### Fix — Wrap the loop in an IfCondition

```
Day 5 flow:
  Copy data1 (page 1)  ← always runs
  act_check_has_more_pages  (IfCondition: v_total_pages > 1?)
    TRUE branch:
      act_set_current_page_2
      act_init_pages_since_refresh
      Until_pages
        act_check_token_refresh
        act_copy_page (page 2..N)
        act_increment_page
        act_set_current_page
        act_increment_refresh_counter
        act_set_refresh_counter
    FALSE branch: (empty — nothing to do)
  act_update_watermark  ← always runs on success
```

**How it works:** If `total_pages = 1`, the IfCondition evaluates `1 > 1 = false` → the entire Until loop is skipped. Pipeline writes page 1, updates watermark, done. No 404.

**IfCondition expression:**
```
@greater(variables('v_total_pages'), 1)
```

---

## Bug 2 — Token Expiry on Long Runs

### What Happened

The VoltGrid API token issued at pipeline start has a limited lifetime (typically 1–2 hours). For sources with many pages (300+ pages × ~2-3 seconds each = 10–15 minutes minimum, but slower networks or API throttling can push this to hours), the token expires mid-loop. Subsequent `act_copy_page` calls get `401 Unauthorized` and the pipeline fails.

### Root Cause

Token was fetched once at pipeline start (`act_api_login` → `act_set_token`) and never refreshed. The same `v_token` was used for all pages throughout the entire run.

### Fix — Periodic Token Refresh Inside the Loop

Every `p_token_refresh_every` pages (default: 50), the pipeline re-logs in and updates `v_token` before the next Copy. The counter `v_pages_since_refresh` tracks how many pages have been copied since the last login.

**New pipeline parameter:**

| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `p_token_refresh_every` | int | `50` | Re-login after this many pages. Lower = more frequent refresh (safer but slower). |

**New variables:**

| Variable | Type | Purpose |
|---|---|---|
| `v_pages_since_refresh` | Integer | Pages copied since last token refresh |
| `v_next_refresh_counter` | Integer | Temp variable for counter increment (same self-reference rule as `v_next_page`) |

**Token refresh flow inside Until loop (each iteration):**

```
act_check_token_refresh  (IfCondition)
  Condition: v_pages_since_refresh >= p_token_refresh_every
  TRUE branch:
    act_relogin                  ← POST /api/auth/login/ with KV credentials
    act_set_refreshed_token      ← v_token = act_relogin.output.token
    act_reset_refresh_counter    ← v_pages_since_refresh = 0
  FALSE branch: (empty — token still valid, skip)

act_copy_page  ← runs after token check (uses fresh v_token if refreshed)

act_increment_page            ← v_next_page = v_current_page + 1
act_set_current_page          ← v_current_page = v_next_page

act_increment_refresh_counter ← v_next_refresh_counter = v_pages_since_refresh + 1
act_set_refresh_counter       ← v_pages_since_refresh = v_next_refresh_counter
```

**Key detail:** `act_relogin` reuses the credentials already fetched from Key Vault by `act_get_username` and `act_get_password` at pipeline start (`activity('act_get_username').output.value`). There is no second Key Vault call — credentials are cached in activity output for the lifetime of the pipeline run.

**Why 50 pages as default?** At 1,000 records/page and ~2-3 seconds per copy, 50 pages ≈ 2-3 minutes of loop time. If the token lasts 1 hour, refreshing every 50 pages means ~72 refreshes for a 3,600-page run — negligible overhead. Lower it to 30 for APIs with shorter token lifetimes.

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
           ▼                    ▼  (7 SetVars run in parallel)    │
      act_api_login      act_set_endpoint                         │
           │             act_set_sink_path                        │
           ▼             act_set_file_prefix                      │
      act_set_token      act_set_page_size                        │
           │             act_set_watermark                        │
           └──────────┬──act_set_ingestion_date                   │
                      │  act_set_run_ts                           │
                      ▼  ◄── JOIN POINT 1                         │
               act_fetch_page1_meta                               │
                      │                                           │
                      ▼                                           │
               act_set_total_pages                                │
                      │                                           │
                      └──────────────────────┬────────────────────┘
                                             ▼  ◄── JOIN POINT 2
                                         Copy data1
                                         (page 1)
                                             │
                                             ▼
                                  act_check_has_more_pages
                                  (v_total_pages > 1 ?)
                                     │           │
                                   TRUE         FALSE
                                     │           │ (skip loop entirely)
                                     ▼           │
                              act_set_current_page_2 (=2)
                                     │
                                     ▼
                              act_init_pages_since_refresh (=1)
                                     │
                                     ▼
                              ┌──── Until_pages ──────────────────┐
                              │  Exit: v_current_page >           │
                              │        v_total_pages              │
                              │                                   │
                              │  act_check_token_refresh          │
                              │  (v_pages_since_refresh >=        │
                              │   p_token_refresh_every?)         │
                              │    TRUE → act_relogin             │
                              │           act_set_refreshed_token │
                              │           act_reset_refresh_      │
                              │           counter (=0)            │
                              │    FALSE → (skip)                 │
                              │                                   │
                              │  act_copy_page (page N)           │
                              │  act_increment_page               │
                              │  act_set_current_page             │
                              │  act_increment_refresh_counter    │
                              │  act_set_refresh_counter          │
                              └────────── ↑ loop back ────────────┘
                                             │
                                   ◄─────────┘
                                             │
                                             ▼
                                    act_update_watermark
                                    (Databricks — always runs
                                     after IfCondition, whether
                                     TRUE or FALSE branch ran)
                                             │
                                             ▼
                                        PIPELINE END
```

---

## Activity Reference Table

| # | Activity | Type | New in Day 5? | Purpose |
|---|---|---|---|---|
| 1 | `act_get_username` | Web GET | No | Fetch username from KV |
| 2 | `act_get_password` | Web GET | No | Fetch password from KV |
| 3 | `act_read_ctl` | Databricks | No | Read control table config |
| 4–10 | `act_set_*` (7 SetVars) | SetVariable | No | Store config in variables |
| 11 | `act_api_login` | Web POST | No | Login to VoltGrid API |
| 12 | `act_set_token` | SetVariable | No | Store token |
| 13 | `act_fetch_page1_meta` | Web GET | No | Get total_pages from page 1 |
| 14 | `act_set_total_pages` | SetVariable | No | Store total_pages |
| 15 | `Copy data1` | Copy | No | Copy page 1 → Bronze |
| 16 | `act_check_has_more_pages` | **IfCondition** | **YES** | Skip loop if total_pages = 1 |
| ↳ | `act_set_current_page_2` | SetVariable | No (moved inside If) | Init loop counter to 2 |
| ↳ | `act_init_pages_since_refresh` | SetVariable | **YES** | Init refresh counter to 1 |
| ↳ | `Until_pages` | Until | No (renamed from Until) | Loop pages 2..N |
| ↳↳ | `act_check_token_refresh` | **IfCondition** | **YES** | Re-login if counter threshold hit |
| ↳↳↳ | `act_relogin` | Web POST | **YES** | Fresh login call |
| ↳↳↳ | `act_set_refreshed_token` | SetVariable | **YES** | Update v_token |
| ↳↳↳ | `act_reset_refresh_counter` | SetVariable | **YES** | Reset counter to 0 |
| ↳↳ | `act_copy_page` | Copy | No | Copy page N → Bronze |
| ↳↳ | `act_increment_page` | SetVariable | No | v_next_page = N + 1 |
| ↳↳ | `act_set_current_page` | SetVariable | No | v_current_page = v_next_page |
| ↳↳ | `act_increment_refresh_counter` | SetVariable | **YES** | v_next_refresh_counter = counter + 1 |
| ↳↳ | `act_set_refresh_counter` | SetVariable | **YES** | v_pages_since_refresh = v_next_refresh_counter |
| 17 | `act_update_watermark` | Databricks | No | Write new watermark |

---

## Variables Reference

| Variable | Type | New? | Set By | Used By |
|---|---|---|---|---|
| `v_token` | String | No | `act_set_token`, `act_set_refreshed_token` | All Copy + Web activities |
| `v_endpoint` | String | No | `act_set_endpoint` | `act_fetch_page1_meta`, Copy activities |
| `v_sink_base_path` | String | No | `act_set_sink_path` | Copy activities |
| `v_file_prefix` | String | No | `act_set_file_prefix` | Copy activities |
| `v_page_size` | String | No | `act_set_page_size` | `act_fetch_page1_meta`, Copy activities |
| `v_last_watermark` | String | No | `act_set_watermark` | `act_fetch_page1_meta`, Copy activities |
| `v_ingestion_date` | String | No | `act_set_ingestion_date` | Copy activities |
| `v_run_ts` | String | No | `act_set_run_ts` | `act_update_watermark` |
| `v_total_pages` | Integer | No | `act_set_total_pages` | Until exit condition, `act_check_has_more_pages` |
| `v_current_page` | Integer | No | `act_set_current_page_2`, `act_set_current_page` | `act_copy_page`, Until condition |
| `v_next_page` | Integer | No | `act_increment_page` | `act_set_current_page` |
| `v_pages_since_refresh` | Integer | **YES** | `act_init_pages_since_refresh`, `act_set_refresh_counter`, `act_reset_refresh_counter` | `act_check_token_refresh`, `act_increment_refresh_counter` |
| `v_next_refresh_counter` | Integer | **YES** | `act_increment_refresh_counter` | `act_set_refresh_counter` |

---

## Parameters Reference

| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `p_source_name` | String | `payments` | Control table row to read |
| `p_load_type_override` | String | `` | Force `full` for backfill |
| `p_token_refresh_every` | **int** | `50` | Refresh token after this many pages |

> `p_token_refresh_every` type is `int` (not String) so the comparison `v_pages_since_refresh >= p_token_refresh_every` works without casting.

---

## How to Apply Day 5 Changes in ADF Studio

1. **Open** `pl_bronze_incremental_load` in ADF Studio → Author
2. **Add new parameter:** `p_token_refresh_every` — type `int` — default `50`
3. **Add new variables:** `v_pages_since_refresh` (Integer), `v_next_refresh_counter` (Integer)
4. **Delete** the connection between `Copy data1` and the existing Until loop
5. **Delete** `act_set_current_page_2` from the main canvas
6. **Add** an **IfCondition** activity after `Copy data1` → name it `act_check_has_more_pages`
   - Expression: `@greater(variables('v_total_pages'), 1)`
7. **Click** `act_check_has_more_pages` → **Edit (pencil)** → go into the **True** canvas
8. **Inside the True canvas**, add:
   - `act_set_current_page_2` (SetVariable: `v_current_page = 2`)
   - `act_init_pages_since_refresh` (SetVariable: `v_pages_since_refresh = 1`, depends on `act_set_current_page_2`)
   - The existing `Until` loop (depends on `act_init_pages_since_refresh`)
9. **Inside the Until loop** → Edit (pencil):
   - **Add** `act_check_token_refresh` (IfCondition, no dependency — first activity each iteration)
     - Expression: `@greaterOrEquals(variables('v_pages_since_refresh'), pipeline().parameters.p_token_refresh_every)`
     - True canvas: `act_relogin` → `act_set_refreshed_token` → `act_reset_refresh_counter`
   - **Connect** `act_check_token_refresh` → `act_copy_page` (add dependency)
   - **Add** `act_increment_refresh_counter` (depends on `act_set_current_page`)
     - Value: `@add(variables('v_pages_since_refresh'), 1)` → variable: `v_next_refresh_counter`
   - **Add** `act_set_refresh_counter` (depends on `act_increment_refresh_counter`)
     - Value: `@variables('v_next_refresh_counter')` → variable: `v_pages_since_refresh`
10. **Back on main canvas:** connect `act_check_has_more_pages` → `act_update_watermark`
11. **Publish All**

> **Tip:** It is easier to delete the old pipeline and paste the Day 5 JSON directly from `pipelines/pl_bronze_incremental_load.json` in this repo.

---

## Verify: Day 5 Checklist

**Single-page guard:**
- [ ] `act_check_has_more_pages` IfCondition exists on main canvas after `Copy data1`
- [ ] Condition: `@greater(variables('v_total_pages'), 1)`
- [ ] `act_set_current_page_2` and Until loop are inside the **True branch** of the IfCondition
- [ ] `act_update_watermark` depends on `act_check_has_more_pages` (not directly on Until)
- [ ] Test: trigger with `p_load_type_override = full` after watermark is recent (few/no new records) — pipeline should complete without 404

**Token refresh:**
- [ ] `p_token_refresh_every` parameter added (int, default 50)
- [ ] `v_pages_since_refresh` variable added (Integer)
- [ ] `v_next_refresh_counter` variable added (Integer)
- [ ] `act_init_pages_since_refresh` sets `v_pages_since_refresh = 1` at loop start
- [ ] `act_check_token_refresh` is first activity inside Until (no dependency)
- [ ] `act_relogin` uses `activity('act_get_username').output.value` (reuses cached KV output — no second KV call)
- [ ] `act_copy_page` depends on `act_check_token_refresh`
- [ ] `act_increment_refresh_counter` and `act_set_refresh_counter` run after `act_set_current_page`

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `404 Not Found` on page 2 | `total_pages = 1`, Until loop still ran | Ensure `act_check_has_more_pages` IfCondition wraps the Until loop |
| `401 Unauthorized` after many pages | Token expired mid-run | Confirm `act_check_token_refresh` is wired correctly and `p_token_refresh_every` is set low enough |
| `act_relogin` fails — username/password not found | `act_get_username`/`act_get_password` output not accessible inside Until (nested scope) | ADF activity outputs from the main canvas ARE accessible inside Until/IfCondition — this should work. If it fails, verify the activity names match exactly |
| Refresh counter never resets | `act_reset_refresh_counter` missing or not in If true branch | Check the `act_check_token_refresh` True canvas has all three: `act_relogin` → `act_set_refreshed_token` → `act_reset_refresh_counter` |
| Pipeline skips loop even when there are multiple pages | `v_total_pages` is 0 or 1 but API has more | Watermark too recent — no new records match the filter. Trigger with `p_load_type_override = full` to confirm. If it works with full, the issue is the watermark, not the guard |
