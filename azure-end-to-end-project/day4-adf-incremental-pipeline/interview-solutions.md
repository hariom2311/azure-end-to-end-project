# Day 4 — Interview Solutions: ADF Incremental Pipeline

---

## ADF + Databricks Integration

**A1**
`Contributor` on the Databricks workspace is required because ADF's MI needs to:
- Create and submit Databricks jobs (job runs API)
- Read job status to determine when the notebook completed
- Access workspace configuration

There is no built-in Azure RBAC role that grants only "trigger jobs" without broader workspace access. The Databricks-native `Can Restart` cluster permission is a separate system inside Databricks — it doesn't control the Azure-level ARM API calls ADF makes to submit jobs. `Contributor` is the minimum Azure RBAC role that covers those ARM operations.

`Reader` is insufficient — it allows reading the workspace configuration but not submitting job runs.

---

**A2**
**New job cluster:**
- **Cost:** ADF pays for compute only during the pipeline run (~5 minutes for a full 321-page load). The cluster starts when the notebook activity begins and terminates when it ends. No idle cost between runs.
- **Operational benefit:** No risk of forgetting to stop a cluster. No shared state between runs — each pipeline run gets a clean Spark context, no accumulated session data or cached DataFrames from a previous run that could affect results.

**Interactive cluster (existing cluster):**
- Cheaper per-minute but must run 24/7 to guarantee availability → much higher monthly cost for a daily pipeline that only runs 10 minutes per day
- If the cluster is stopped when the pipeline triggers, the job fails

For a production pipeline running once daily, New job cluster is the correct choice.

---

**A3**
Two distinct causes:

**Cause 1 — `dbutils.notebook.exit()` returned an empty string or was not called.**
Diagnosis: In ADF Monitor → click `act_read_ctl` → **Output** → inspect `runOutput` field. If it is `""` or missing, the notebook did not call `exit()`. Check the Databricks job run linked in the activity output → view the notebook run logs.

**Cause 2 — `@json()` parsed successfully but the field name is wrong.**
Diagnosis: If `runOutput` contains the expected JSON string but `act_set_endpoint` returns null, the field name in the expression doesn't match the key in the JSON. For example, the notebook returns `"api_endpoint"` but the expression reads `.endpoint`. Fix: compare the exact field names in the notebook's `result` dict against the ADF expression.

---

**A4**
`activity('act_read_ctl').output.runOutput` is a **string**, not a JSON object. It contains:
```
{"endpoint": "/api/db/payments/", "sink_base_path": "api/payments", ...}
```

Without `@json()`:
```
@activity('act_read_ctl').output.runOutput.endpoint
```
ADF tries to access `.endpoint` on a string type — strings don't have named properties. The expression returns `null`.

With `@json()`:
```
@json(activity('act_read_ctl').output.runOutput).endpoint
```
`json()` parses the string into an ADF object, then `.endpoint` accesses the field correctly. This is always required when a Databricks notebook exits with `json.dumps(result)`.

---

**A5**
Three reasons parameterisation is correct from day one:

1. **Adding a second source costs nothing:** When sessions is added, the trigger fires with `p_source_name = 'sessions'` and the same pipeline handles it — no code change, no duplicate pipeline.

2. **Testing is clean:** You can test with `p_source_name = 'payments_test'` (a test control row) without touching the production payments row or triggering a real load.

3. **Hardcoding is never free:** Removing a hardcoded value later requires finding every place it appears, testing the change, and deploying — all under production pressure. Parameterising takes 5 minutes now and saves hours of refactoring later.

The rule: if a value might differ per invocation, it should be a parameter. `source_name` clearly will differ — there are 6+ API endpoints in this project.

---

## Pipeline Variables and Watermark Logic

**A6**
Capturing at **start** is correct. Capturing at end creates a data gap:

```
Pipeline starts: 02:00 UTC
API has records updated at 02:10 UTC (during the run)
Pipeline ends:   02:25 UTC

If watermark = 02:25 (end):
  Next incremental runs with updated_after = 02:25
  Records updated at 02:10 are in the range [02:00, 02:25]
  They are MISSED — never fetched ❌

If watermark = 02:00 (start):
  Next incremental runs with updated_after = 02:00
  Records updated at 02:10 are fetched in the next run ✅
  Some overlap with the previous run, but Silver's MERGE on primary key handles that
```

Start timestamp creates safe overlap. End timestamp creates a gap. Overlap is always preferable to a gap in incremental loads.

---

**A7**
Trace with `p_load_type_override = ''` (empty string):

1. `empty('')` = `true`
2. `not(true)` = `false`
3. `and(false, ...)` = `false` (short-circuits — second condition not evaluated)
4. `@if(false, '1900-01-01T00:00:00Z', <else>)` → takes the else branch
5. Else: `json(activity('act_read_ctl').output.runOutput).last_watermark`

Result: `v_last_watermark` = the `last_watermark` value from the control table row — whatever the last successful run wrote. If this is the second run, it might be `'2026-10-14T02:00:00Z'`. ADF sends `updated_after=2026-10-14T02%3A00%3A00Z` to the API — fetching only records changed since the last run.

---

**A8**
The pipeline sends `updated_after = '2026-10-14T02:00:00Z'` (the stored watermark from 6 days ago).

Yes — all 6 days of data are captured in this single run. The VoltGrid API filters by `updated_at > updated_after`, regardless of how old the watermark is. All records updated in the 6-day window return in the response. The `total_pages` will be larger than a normal daily incremental run (more records changed over 6 days than 1 day).

After the run completes, the watermark is updated to `2026-10-20T<pipeline-start-time>Z`. The next daily run only fetches records changed since that new watermark — back to normal.

This is the self-healing property of watermark-based incremental loads: a missed run doesn't cause permanent data loss, it just means a larger-than-usual incremental on the catch-up run.

---

**A9**
**Readability and reuse:** If every downstream activity references the full `@json(activity('act_read_ctl').output.runOutput).endpoint` expression, any change to the control table field name or the notebook's JSON key requires updating every activity. With Set Variable, only one expression changes.

**Expression length:** ADF dynamic content expressions have a length limit. The full `@json(...)` expression is long — embedding it in a Copy Activity's dataset parameter mapping, a URL concat, and a folder path expression simultaneously risks hitting that limit or making expressions unreadable.

**Debug clarity:** In Monitor, Set Variable activities show their resolved value in the output. If `v_endpoint` shows the wrong value, you know exactly where to look. If you're embedding the full expression inline, you have to run the pipeline again with debug logging to see what resolved.

---

**A10**
`v_page_size` is String because ADF pipeline variables support only String, Boolean, Array, Object, Integer, and Float — and Integer variables cannot hold values read from a JSON string via `@json(...).page_size` without explicit conversion.

The control table returns `page_size` as a JSON number. `@string(json(...).page_size)` converts it: `1000` (number) → `"1000"` (string) → stored in `v_page_size`.

In the dataset, `p_page_size` is typed as Int. When ADF passes `@variables('v_page_size')` (a string `"1000"`) to an Int parameter, ADF automatically coerces the string to integer at assignment time — no explicit `@int()` wrapper needed in the activity mapping. The coercion is implicit and type-safe within ADF's expression engine.

---

## Pagination and Until Loop

**A11**
ADF's Copy Activity is designed to write data — it reads from source and writes to sink. The full source response body (including `pagination.total_pages`) is not exposed as an activity output; only metadata like `rowsCopied` and `dataWritten` is available.

A Web Activity performs an HTTP call and exposes the **full response body** as `activity('act_fetch_page1_meta').output`. This allows `activity('act_fetch_page1_meta').output.pagination.total_pages` to be read and stored in `v_total_pages` — which then drives the Until loop exit condition.

Without `total_pages`, the Until loop has no bound and cannot be constructed.

---

**A12**
Final iteration trace when `v_current_page = 321` and `v_total_pages = 321`:

1. Until checks: `@greaterOrEquals(321, add(321, 1))` = `@greaterOrEquals(321, 322)` = `false` → loop body runs
2. `act_copy_page` fetches page 321 → writes `payments_page_321.json`
3. `act_increment_page` sets `v_current_page = @add(321, 1)` = `322`
4. Until checks: `@greaterOrEquals(322, add(321, 1))` = `@greaterOrEquals(322, 322)` = `true` → loop exits

Page 321 is correctly written before the loop exits. If the condition was `@greaterOrEquals(v_current_page, v_total_pages)` (without `+1`): when `v_current_page = 321`, the check `321 >= 321` = `true` → the loop exits before page 321 runs. The last page is missed.

---

**A13**
What happened: `act_increment_page` was on the main canvas. It ran once before the Until loop started (as part of the pipeline's sequential flow), incrementing `v_current_page` from 2 to 3. Inside the Until loop, `v_current_page` is never incremented — it stays at 3 forever because the increment activity is outside the loop body.

Actually — the loop started at `v_current_page = 2` (set by `act_set_current_page_2`). With no increment inside the loop, the loop condition `@greaterOrEquals(2, 322)` is always false. The loop runs `act_copy_page` with `p_page = 2` infinitely (until timeout).

Fix: In ADF, click the Until activity → **Edit** (pencil icon) → the inner canvas opens. Drag `act_increment_page` onto this inner canvas and wire `act_copy_page → act_increment_page`. Click the main canvas breadcrumb to go back and verify the outer canvas no longer shows `act_increment_page`.

---

**A14**
If `act_copy_page` and `act_increment_page` ran in parallel:
- `act_increment_page` reads `v_current_page` (e.g. 5) and immediately sets it to 6
- `act_copy_page` is still fetching page 5 from the API
- When the next iteration starts, `v_current_page = 6` — page 5 copy might still be in progress

This creates a race condition: the counter advances before the copy confirms success. If the copy fails, the pipeline retries — but with `v_current_page` already at 6, it retries page 6 instead of page 5. Page 5 data is never written. Worse, the until condition may be checked while the increment is mid-flight.

Sequential dependency (`act_copy_page` → `act_increment_page`) ensures: page N succeeds → then N+1 starts. Each iteration is atomic: copy OR nothing. No race.

---

**A15**
`total_pages = 0` means the API returned zero pages. The Until condition at the start:

```
@greaterOrEquals(variables('v_current_page'), add(variables('v_total_pages'), 1))
= @greaterOrEquals(2, add(0, 1))
= @greaterOrEquals(2, 1)
= true
```

The Until loop exits immediately without running a single iteration. The pipeline continues to `act_update_watermark` — which updates the watermark to `v_run_ts` (today's timestamp).

The pipeline **succeeds silently** — no data written to Bronze (correct, since there's nothing to write), but the watermark advances. This is correct behaviour: the next run will use today's timestamp as `updated_after` and still find no new records (assuming still no updates) — normal incremental state.

**Improvement:** Add an `act_check_total_pages` If Condition activity — if `v_total_pages = 0`, skip the Until loop and optionally log a warning. This makes the "zero pages" case explicit in the pipeline monitor rather than invisible.

---

## Incremental Load and Idempotency

**A16**
Incremental load means: instead of fetching all records every run, fetch only records that changed since the last successful run. `updated_after` is a VoltGrid API query parameter — `?updated_after=2026-10-14T02:00:00Z` returns only records where `updated_at > '2026-10-14T02:00:00Z'`.

On the second run: `v_last_watermark` comes from `ev_catalog.ctl.pipeline_control.last_watermark`, which was written by `act_update_watermark` at the end of the first run. The pipeline reads this value from the control table via `nb_read_control_table`, sets it as `v_last_watermark`, and passes it as `updated_after` to every API call.

---

**A17**
Idempotent: running the pipeline N times with the same inputs produces the same output — no duplicates, no corruption.

Concrete example for 2026-10-14 with `updated_after = 1900-01-01T00:00:00Z`:

**Run 1:** 321 pages → 321 files in `bronze/api/payments/ingestion_date=2026-10-14/`

**Run 2 (same day, same watermark):**
- Same `updated_after = 1900-01-01T00:00:00Z` → same 321 pages from the API → same JSON content
- Sink file names are identical: `payments_page_001.json` ... `payments_page_321.json`
- ADF's Copy Activity overwrites each file with identical content
- ADLS ends up with exactly the same 321 files as after Run 1

No duplicates because files overwrite (not append). No corruption because same query → same data.

---

**A18**
The pipeline re-trigger at 03:00 UTC uses `last_watermark` from the control table. Since `act_update_watermark` failed on the first run, the watermark was NOT updated — it still holds the value from before the first run (e.g. `2026-10-13T02:00:00Z`).

So the 03:00 re-trigger uses the same `updated_after = 2026-10-13T02:00:00Z` → same 5 pages → same 5 file names → overwrites the existing 5 files with identical content.

Result: the 5 pages in Bronze are correct and complete. The watermark updates successfully this time. No data loss, no duplicates.

---

**A19**
Trace:

1. Trigger fires with `p_load_type_override = ''` (empty — not set)
2. `act_read_ctl` reads the control table: `last_watermark = '2026-10-14T02:00:00Z'` (from yesterday's full load)
3. `act_set_watermark`:
   - `empty('')` = `true` → `not(true)` = `false` → condition is `false`
   - Takes else branch: `json(activity('act_read_ctl').output.runOutput).last_watermark`
   - Result: `v_last_watermark = '2026-10-14T02:00:00Z'`
4. API call: `?updated_after=2026-10-14T02%3A00%3A00Z`
5. Returns only records updated after 2026-10-14 02:00 UTC — yesterday's records to now

---

**A20**
Not a data problem. The re-run overwrites pages 1–199 with identical data (same `updated_after` → same records → same content), then writes pages 200–321 for the first time. Bronze ends with all 321 files — complete and correct.

**Downstream in Silver:** Silver uses a MERGE on `payment_id` (primary key). Re-processing pages 1–199 from Bronze in Silver:
- For each record in pages 1–199, MERGE matches on `payment_id`
- The record already exists in Silver → UPDATE with the same field values (no actual change)
- No phantom duplicates, no incorrect updates

The Medallion Architecture's MERGE at Silver is specifically designed to make Bronze reruns safe. This is the correct outcome — Bronze is meant to be replayable.

---

## File Naming, Path Design, and Trigger

**A21**
No padding — files are named `payments_page_1.json`, `payments_page_2.json`, ..., `payments_page_321.json`.

**Loop pages (`act_copy_page`) — `p_file` expression:**
```
@concat(variables('v_file_prefix'), '_page_', string(variables('v_current_page')), '.json')
```

Results:
- `v_current_page = 2` → `payments_page_2.json`
- `v_current_page = 42` → `payments_page_42.json`
- `v_current_page = 321` → `payments_page_321.json`

**Page 1 (`Copy data1`) — `p_file` expression:**
```
@concat(variables('v_file_prefix'), '_page_1.json')
```

`string()` converts the Integer variable to a string for concatenation. Without it, ADF throws a type error because `concat` expects all string arguments.

---

**A22**
This is **Hive-style partition format** — `key=value` pairs in directory names. It is a convention from Apache Hive, adopted by Spark, Delta Lake, and Databricks.

When Databricks reads:
```python
spark.read.format("json").load("abfss://bronze@evdatalakedev2.dfs.core.windows.net/api/payments/")
```
Spark automatically discovers all `ingestion_date=*/` subdirectories and:
1. Makes `ingestion_date` available as a column in the resulting DataFrame (no schema definition needed)
2. Enables **partition pruning**: `WHERE ingestion_date = '2026-10-14'` reads only that one folder, skipping all others — critical performance optimisation at scale

Without the `ingestion_date=` prefix (just `2026-10-14/`), Spark reads the folder as a sub-path but does NOT expose it as a queryable partition column without explicit manual configuration.

---

**A23**
Complete change list:

**Control table — 1 SQL INSERT (the only code change required):**
```sql
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('sessions', '/api/db/sessions/', 'api/sessions', 'sessions', 1000, 'full', '1900-01-01T00:00:00Z', true);
```

**ADF pipeline — no changes.** `pl_bronze_incremental_load` reads all config from the control table. It works for any `source_name` row.

**ADF trigger — create a new trigger** (or reuse the existing one with `p_source_name = 'sessions'` if running ad-hoc).

**Databricks notebooks — no changes.** Both notebooks use `source_name` as a parameter and work for any value.

**Datasets — no changes.** `ds_voltgrid_payments_src` accepts `p_endpoint` as a parameter — the sessions endpoint flows through automatically.

The pipeline resolves sink path to: `bronze/api/sessions/ingestion_date=YYYY-MM-DD/sessions_page_NNN.json`

---

**A24**
**Tumbling Window trigger:**
- Tracks every time window from the start date to now
- If a run fails, the window stays in "failed" state — ADF retries it (per retry policy) and does NOT skip to the next window
- If the system is down for 3 days, on recovery ADF queues 3 pending windows and runs them in order (with max concurrency = 1, sequentially)
- Guarantees: no window is ever silently skipped

**Schedule trigger:**
- Fires at calendar times regardless of previous run state
- If the 02:00 run fails, the 02:00 window is "lost" — ADF fires the next 02:00 trigger tomorrow with no backfill
- No guarantee of coverage

For a daily incremental pipeline where missing a day means a watermark gap and potentially missing a full day of payments data: Tumbling Window is the only correct choice.

---

**A25**
Design using **ForEach** with `batchCount`:

```
act_read_ctl → act_set_vars → ... → act_fetch_page1_meta
    ↓
act_set_total_pages
act_copy_page1 (page 1 already done)
    ↓
act_generate_page_list (Set Variable):
  v_page_list = @range(2, add(variables('v_total_pages'), 1))
  → produces [2, 3, 4, ..., 321]
    ↓
ForEach (items: @variables('v_page_list'), batchCount: 5, isSequential: false)
  └── act_copy_page (p_page = @item())
    ↓
act_update_watermark
```

**Wall-clock time:** 320 pages ÷ 5 parallel = 64 batches × ~4 seconds = ~4 minutes (vs 20 minutes sequential).

**Why idempotent:** Same page number → same `updated_after` → same records → same file name. Whether 5 pages run in parallel or sequentially, `payments_page_007.json` contains the same data.

**Constraints and risks:**

| Risk | Mitigation |
|---|---|
| API rate limiting (429) | Retry on inner activity: 3 retries, 60s interval |
| ADF concurrent activity cap (~50 per pipeline) | Keep `batchCount` ≤ 10 to leave headroom |
| Partial batch failure (3/5 succeed, 2 fail) | ForEach iteration fails → watermark NOT updated → safe to rerun entire ForEach from scratch |
| VoltGrid token expiry mid-run | Token valid for ~1 hour; 320 pages at 5 parallel = ~4 min total — well within window |
| `v_page_list` variable type | ADF Array variable — `@range()` returns array; `@item()` inside ForEach gives the current page integer |

The watermark update only runs after ForEach succeeds — any failure leaves `last_watermark` unchanged, making a full ForEach rerun safe and correct.

---
