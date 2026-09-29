# Day 4 — Interview Solutions: ADF Activities

---

## General Activities

**Q1 — Web Activity vs Copy Activity**

| | Web Activity | Copy Activity |
|---|---|---|
| Purpose | General HTTP call — get a response | Move data from source to sink dataset |
| Returns | JSON response body | Nothing returned — writes to a file/table |
| Data volume | Small — one response payload | Any size — bytes to terabytes |
| Output used | `activity('x').output.field` in expressions | `activity('x').output.rowsCopied` for stats only |

**In `pl_bronze_api_payments`:**
- `act_get_username` — Web Activity — GET Key Vault → returns `{"value": "voltgrid_demo", ...}`
- `act_get_password` — Web Activity — GET Key Vault → returns `{"value": "EVcharge@AU2025", ...}`
- `act_api_login` — Web Activity — POST login endpoint → returns `{"token": "abc123..."}`
- `act_copy_payments` — Copy Activity — GET payments API → writes JSON to ADLS Gen2

---

**Q2 — Set Variable vs Append Variable**

**Set Variable:** replaces the entire variable value. Works with String, Int, Bool, Array.

**Append Variable:** adds one item to the end of an Array variable. Does not replace — accumulates.

```
Set Variable: v_token = "abc123"   → v_token is now "abc123"
Set Variable: v_token = "xyz789"   → v_token is now "xyz789" (replaced)

Append Variable: v_counts += 100   → v_counts is now [100]
Append Variable: v_counts += 87    → v_counts is now [100, 87]
Append Variable: v_counts += 53    → v_counts is now [100, 87, 53]
```

**Use Append Variable when:** building a list across loop iterations — row counts per page, filenames processed, error messages from ForEach iterations.

---

**Q3 — Four Get Metadata fields with use cases**

| Field | Returns | Real use case |
|---|---|---|
| `exists` | `true` / `false` | Check if a Bronze file landed before running Silver transform |
| `size` | File size in bytes | Verify file is not empty (`size > 0`) before processing |
| `lastModified` | Timestamp | Check a file wasn't modified hours ago — detect stale data |
| `childItems` | Array of `{name, type}` objects | List all JSON files in a dated folder to process each one in a ForEach |

---

**Q4 — Copy succeeded but Get Metadata shows empty folder**

The Copy Activity wrote the file but to a **different path** than where Get Metadata is looking.

Most likely cause: the `v_ingestion_date` variable was updated between the Copy Activity completing and the Get Metadata running — or the dataset parameter `p_run_date` was not passed correctly to the Get Metadata dataset, so it checked a different `ingestion_date=` folder (possibly today's date vs yesterday's).

Also possible: eventual consistency in ADLS Gen2 — very rare but a listing operation immediately after a write may not yet show the new file. Adding a Wait Activity (2–3 seconds) before Get Metadata resolves this.

**Diagnose:** Click `act_copy_payments` → Output → `dataWritten` (confirm bytes > 0) and `filesWritten` (confirm = 1). Click `act_check_bronze` → Input → confirm the dataset path being checked matches the Copy Activity output path.

---

**Q5 — Extract token and expiry from Web Activity output**

Login response: `{"token": "abc123", "expires_in": 3600}`

**(a) Token value:**
```
@activity('act_api_login').output.token
```
Result: `"abc123"`

**(b) Expiry in minutes (divide seconds by 60):**
```
@div(activity('act_api_login').output.expires_in, 60)
```
Result: `60` (integer division — `3600 / 60 = 60`)

---

**Q6 — Execute Pipeline: Wait vs No Wait**

**Wait on completion: Yes**
The parent pipeline pauses at the Execute Pipeline activity until the child pipeline finishes (success or failure). The child's status determines whether the parent continues or stops.

Use case: Pipeline A must complete before Pipeline B starts. `pl_bronze_api_payments` must succeed before `pl_silver_payments` transforms the Bronze data.

```
Execute pl_bronze_api_payments (wait: yes) ─On Success─►
Execute pl_silver_payments     (wait: yes) ─On Success─►
Execute pl_gold_payments       (wait: yes)
```

**Wait on completion: No**
The parent fires the child and immediately moves to the next activity — does not wait. The child runs independently.

Use case: Send a notification pipeline asynchronously. The orchestrator fires all 5 endpoint pipelines simultaneously and moves on — a monitoring activity checks results later.

```
Execute pl_ingest_payments  (wait: no) ─┐
Execute pl_ingest_sessions  (wait: no) ─┤─ all fire simultaneously
Execute pl_ingest_customers (wait: no) ─┘
Wait 20 minutes
Check audit table for failures
```

---

**Q7 — Verify Bronze file before Databricks**

**Step 1: Get Metadata Activity**
- Points to the Bronze output folder
- Field list: `exists`, `size`
- Output: `{ "exists": true, "size": 45320 }`

**Step 2: If Condition Activity**
- Expression:
  ```
  @and(activity('act_check_bronze').output.exists,
       greater(activity('act_check_bronze').output.size, 1024))
  ```
- True branch → Databricks Notebook Activity
- False branch → Fail Activity ("Bronze file missing or smaller than 1KB")

---

**Q8 — Why not just overwrite instead of Delete**

Overwriting a file in ADLS Gen2 replaces the content. But there are two problems:

1. **Partial write race condition:** If the new write fails halfway through, the file now contains partial data from the new write mixed with nothing — the original data is gone. A Delete + Write keeps the old file intact until the new write succeeds.

2. **Audit trail:** In a compliant data platform, every file in the landing zone is a record of a delivery. Deleting it after ingestion (and archiving it to a separate location) gives you a clean audit trail — you can see what was processed and when. Overwriting loses the original delivery.

**Better pattern:** Copy file from landing → Bronze, then Delete from landing, then move original to `archive/YYYY-MM-DD/` using a second Copy Activity.

---

**Q9 — Lookup output: First row only Yes vs No**

The Lookup Activity runs a query and returns rows.

**First row only: Yes**
```json
{
  "firstRow": { "table_name": "payments", "endpoint": "/api/db/payments/" }
}
```
Reference: `@activity('lkp').output.firstRow.endpoint`

Use when: you need one config value — e.g. read the current watermark from a `pipeline_audit` table where the latest row has the max `updated_at`.

**First row only: No**
```json
{
  "count": 5,
  "value": [
    { "table_name": "payments", ... },
    { "table_name": "sessions", ... },
    ...
  ]
}
```
Reference: `@activity('lkp').output.value` — pass to ForEach Items

Use when: you need all rows — endpoint config list, list of tables to process, list of files to copy.

---

**Q10 — Lookup output index access**

The config file has 5 rows. `@activity('lkp').output.value[2]` returns the **third item** (index is zero-based):
```json
{ "table_name": "customers", "endpoint": "/api/db/customers/" }
```

**If the config file only has 2 rows:**
`output.value[2]` throws an **array index out of bounds** error — ADF fails the activity with an expression evaluation error.

This is why you should avoid hardcoded index access when the array size can vary. Pass the entire array to ForEach and let `@item()` handle each element safely.

---

**Q11 — Fail Activity advantage**

Without Fail Activity: the pipeline might succeed with `rowsCopied: 0` and no error — the Monitor shows green, no alert fires, and the team doesn't know data was missed until someone checks Bronze manually.

With Fail Activity after an `@equals(output.rowsCopied, 0)` If Condition True branch:
- Pipeline status: **Failed** (visible in Monitor)
- Error message: `"API returned 0 records for payments endpoint"` — specific, human-readable
- Azure Monitor alert fires automatically
- On-call engineer sees exactly what failed and why — no guessing

The Fail Activity converts a silent logical failure into a visible operational failure.

---

**Q12 — Wait Activity and timeout risk**

Maximum Wait time in ADF: **604800 seconds (7 days)**.

The pipeline default timeout is 12 hours. A 300-second Wait is fine — it adds 5 minutes to a pipeline that otherwise completes in 20 seconds.

Risk: if you put Wait inside an Until loop that runs 1000 iterations, `1000 × 300 seconds = 300,000 seconds ≈ 3.5 days` — well past the 12-hour timeout. The pipeline gets killed before the loop completes.

**Safe use:** Wait at the pipeline level (once) or inside a loop with a small count. Never put a large Wait inside a loop with an unknown number of iterations.

---

**Q13 — Making 3 sequential Copy Activities run faster**

Replace the sequential chain with a **ForEach Activity** (Sequential: false, Batch count: 3):

Instead of:
```
Copy Table A ─► Copy Table B ─► Copy Table C     (serial — 3× slowest table)
```

Create a config array and loop:
```
ForEach [Table A, Table B, Table C]  (parallel, batch=3)
  Copy Activity  →  copies each table simultaneously
                    Total time = slowest single table (≈ 1×)
```

Or for a quick non-config approach: remove the dependency arrows between the 3 Copy Activities entirely. When no dependency exists between activities, ADF runs them in parallel automatically.

---

## Iteration & Conditionals

**Q14 — If Condition basics**

If Condition evaluates a boolean expression and routes to one of two branches.

- Expression **must evaluate to `true` or `false`** — not a string, not a number. Use functions like `equals()`, `greater()`, `empty()`, `and()`, `or()`.
- **False branch can be left empty** — if the condition is false and the False branch has no activities, the pipeline simply continues past the If Condition.

---

**Q15 — Case-sensitive equals expression**

```
@equals(pipeline().parameters.p_load_type, 'full')
```

**If someone passes `"Full"` with a capital F:** `equals()` is case-sensitive → returns `false` → the False (incremental) branch runs even though they intended a full load.

This is a common production bug. Fix options:
1. Document that `p_load_type` must be lowercase and validate in the trigger definition
2. Add `toLower()` in the expression: `@equals(toLower(pipeline().parameters.p_load_type), 'full')`

---

**Q16 — If Condition vs Switch**

**If Condition:** 2 branches — True and False.

**Switch:** N branches — one per case value + a Default.

**Switch is clearly better when:** you have more than 2 options on the same expression.

Example with If Condition (ugly with 3+ branches):
```
If p_env == 'dev'
  True:  Set URL = dev
  False:
    If p_env == 'staging'
      True:  Set URL = staging
      False:
        If p_env == 'prod' ... (nesting gets unreadable)
```

Same logic with Switch (clean):
```
Switch @pipeline().parameters.p_env
  Case 'dev':     Set v_api_url = dev URL
  Case 'staging': Set v_api_url = staging URL
  Case 'prod':    Set v_api_url = prod URL
  Default:        Fail "Unknown environment"
```

Rule of thumb: 2 branches → If Condition. 3+ branches on the same value → Switch.

---

**Q17 — ForEach basics**

ForEach iterates over an array and runs inner activities for each item.

**Sequential: true** — one item at a time, oldest first. Total time = sum of all items.
**Sequential: false (Batch mode)** — items run in parallel up to Batch count. Total time ≈ slowest single item.

**Maximum batch count: 50**

---

**Q18 — 8 items, batch count 3 — execution order**

```
Batch 1: items 1, 2, 3 run simultaneously
         (waits for all 3 to complete)
Batch 2: items 4, 5, 6 run simultaneously
         (waits for all 3 to complete)
Batch 3: items 7, 8 run simultaneously
         (only 2 remain)
```

Total wall-clock time = (slowest item in batch 1) + (slowest in batch 2) + (slowest in batch 3).

Note: "Batch count" means max concurrency — ADF doesn't rigidly group into batches. It keeps `n` slots open. As one item completes, the next item from the queue starts immediately. So in practice item 4 could start before item 3 finishes if item 3 is the slowest.

---

**Q19 — `@item()` during second ForEach iteration**

ForEach items:
```json
[{"endpoint": "/payments/"}, {"endpoint": "/sessions/"}]
```

During the **second iteration** (index 1):
```
@item()           →  { "endpoint": "/sessions/" }
@item().endpoint  →  "/sessions/"
```

`@item()` always refers to the current iteration's element — it changes with each iteration.

---

**Q20 — Until vs ForEach — when to use Until**

**ForEach:** You have a known array upfront. You know the count. ADF iterates over each element.

**Until:** You don't know how many iterations you need until the pipeline is running. The stop condition is determined at runtime.

**Exact scenario where Until is required:**
Paginating a REST API where the total page count is returned in the first response:
```
GET /api/db/payments/?page=1  → { "count": 1542, "total_pages": 16, "results": [...] }
```
You cannot use ForEach because you don't know there are 16 pages until you call the API. You set `v_total_pages = 16` from the first response, then use Until to loop until `v_current_page > v_total_pages`. Each iteration increments the counter and copies one page.

---

**Q21 — Self-reference limitation and workaround**

ADF evaluates the Set Variable expression by reading all referenced variables at the start of the evaluation, then writing the result. If the same variable is both read and written, ADF blocks it to prevent ambiguity in parallel execution contexts.

**Workaround — two variables:**
```
Activity 1: act_set_temp
  v_temp = @add(variables('v_counter'), 1)    ← reads v_counter, writes v_temp

Activity 2: act_increment
  v_counter = @variables('v_temp')            ← reads v_temp, writes v_counter
```

`act_set_temp` → `act_increment` (sequential dependency). Two separate evaluations, no self-reference.

---

**Q22 — Until loop never stops — two causes**

**Cause 1: Condition never becomes true**
The stop condition is `@greaterOrEquals(variables('v_current_page'), variables('v_total_pages'))` but `v_total_pages` was never set — it defaults to `0` (int default). `v_current_page` starts at `1`, which is already `>= 0`, so the loop should stop immediately… unless the variable type mismatch causes the comparison to fail or the initial value is wrong.

Diagnose: Add a Set Variable at the start of the loop body that outputs `v_current_page` to an Append Variable (Array) — watch the array grow in Monitor → Variables output.

**Cause 2: Counter not incrementing**
The `act_set_temp` → `act_increment` activities are missing or in the wrong order. `v_current_page` stays at `1` forever → condition is never true → infinite loop.

Diagnose: In Monitor → Until → open the loop → check each iteration's activity runs → confirm `act_increment` ran and `v_current_page` changed.

---

**Q23 — Filter Activity vs WHERE in Lookup**

**Filter Activity:** Runs after data is already in memory (from Lookup or another activity output). Filters the array in ADF's pipeline engine — no query to the source.

**WHERE in Lookup query:** Filters at the source — only matching rows are returned from the database/file. Less data transferred.

**Difference:**
- Lookup WHERE: source-side filtering (preferred when possible — less data transferred)
- Filter Activity: client-side filtering of an already-returned array

**When Filter is needed:** The source doesn't support WHERE (e.g. a JSON file from ADLS Gen2 — you can't push a WHERE clause into a JSON read). You load all rows with Lookup, then use Filter to keep only the ones you need.

---

**Q24 — Filter expression for files > 50MB + ForEach**

Lookup returns:
```json
[
  {"name": "file_a.json", "size": 120000000},
  {"name": "file_b.json", "size": 30000000},
  {"name": "file_c.json", "size": 80000000}
]
```

**Filter Activity:**
- Items: `@activity('act_lookup_files').output.value`
- Condition: `@greater(item().size, 52428800)`  ← 50MB in bytes (50 × 1024 × 1024)

**Filter output:**
```json
[
  {"name": "file_a.json", "size": 120000000},
  {"name": "file_c.json", "size": 80000000}
]
```

**Pass to ForEach:**
```
ForEach Items: @activity('act_filter_large').output.value
  Copy Activity: source file = @item().name
```

---

**Q25 — ForEach parallel Append Variable returns fewer items**

**Root cause: Race condition on Append Variable in parallel ForEach.**

When `batch count = 5` and all 5 iterations run simultaneously, all 5 try to append to `v_processed_names` at the same time. ADF's Append Variable is not thread-safe in parallel ForEach — some appends are lost when they collide.

**Fix options:**
1. Set **Is Sequential: true** — one iteration at a time, no collisions. Slower but correct.
2. Don't use Append Variable inside a parallel ForEach — collect results differently. After the ForEach, use a Lookup to query what was actually written to Bronze (more reliable than an in-memory array).
3. Use the Append Variable only in a sequential inner step that runs after the parallel Copy Activity in each iteration.

This is a known ADF limitation — parallel Append Variable is unsafe.

---

## Senior / Mixed

**Q26 — Pipeline design: Lookup → Filter → ForEach with auth**

```
Activity                    Type              Key configuration
──────────────────────────────────────────────────────────────────────
act_lookup_endpoints        Lookup            Dataset: ds_endpoint_config
                                              First row only: No
act_get_username            Web Activity      GET Key Vault voltgrid-username
act_get_password            Web Activity      GET Key Vault voltgrid-password
act_api_login               Web Activity      POST /api/auth/login/
act_set_token               Set Variable      v_token = output.token
act_filter_active           Filter            @equals(item().status, 'active')
act_set_ingestion_date      Set Variable      v_ingestion_date = formatDateTime(utcnow(), 'yyyy-MM-dd')
act_loop_active_endpoints   ForEach           Items: @activity('act_filter_active').output.value
                                              Sequential: false, Batch: 5
  └─ act_copy_endpoint      Copy Activity     Source: @item().endpoint + Auth header
                                              Sink: bronze/api/@{item().table_name}/...
```

Dependency chain:
`act_lookup_endpoints` and `act_get_username`/`act_get_password` run in parallel (no dependency on each other) → `act_api_login` waits for both username and password → `act_set_token` → `act_filter_active` (needs lookup output) → `act_loop_active_endpoints`

---

**Q27 — ForEach batch 50 causes 429 Too Many Requests**

**Root cause:** The VoltGrid API has a rate limit — it cannot handle 50 simultaneous connections. With batch count 50, ADF fires 50 Copy Activities at the same moment, all calling the same API → API returns 429.

**Fix 1 — Reduce batch count:**
Lower `batch count` from 50 to 3–5. ADF processes 5 endpoints simultaneously — manageable for the API.

**Fix 2 — Add Wait inside the ForEach:**
Add a **Wait Activity** (5–10 seconds) after each Copy Activity inside the ForEach. Between iterations, each slot pauses before starting the next item — spreads load over time. Works with higher batch counts but adds total duration.

**Underlying lesson:** ForEach batch count ≠ "how many the API can handle." Always check API rate limits before setting parallelism. Start with batch count 1 (sequential), confirm it works, then increase gradually.

---

**Q28 — Paginating unknown total pages: Until + first-call Web Activity**

**ForEach alone cannot do this** because ForEach needs the full array before it starts. You don't know the array (`[page_1, page_2, ..., page_N]`) until you call the API.

**Correct pattern: Web Activity + Until**

```
Step 1: Web Activity (act_get_total_pages)
  GET /api/db/payments/?page=1&page_size=100
  Output: { "total_pages": 16, "results": [...] }

Step 2: Set Variable (act_set_total_pages)
  v_total_pages = @int(activity('act_get_total_pages').output.total_pages)

Step 3: Copy Activity (copy page 1 from the Web Activity response)

Step 4: Set Variable (v_current_page = 2)

Step 5: Until (condition: @greaterOrEquals(variables('v_current_page'), variables('v_total_pages')))
  Inner:
    Copy Activity → /api/db/payments/?page=@{variables('v_current_page')}
    act_set_temp_page   → v_temp = @add(variables('v_current_page'), 1)
    act_increment_page  → v_current_page = @variables('v_temp')
```

---

**Q29 — Child pipeline Until loop vs parent timeout**

The parent pipeline timeout of 1 hour controls the **parent pipeline run** — including all time spent waiting for child pipelines. When `Wait on completion: Yes`, the parent waits for the child — and the parent's own timeout counts down during that wait.

After 1 hour, the **parent pipeline is killed** by ADF. The child pipeline continues running (it has its own timeout). But the parent's Execute Pipeline activity shows as Failed (timeout exceeded).

**Fix:** Increase the parent pipeline timeout (in the pipeline settings or the Execute Pipeline activity's policy timeout) to be longer than the child's worst-case duration. Or set `Wait on completion: No` and use a separate monitoring mechanism.

---

**Q30 — System design: `pl_bronze_all_endpoints`**

```
Activity                     Type              Key expression / setting
───────────────────────────────────────────────────────────────────────────────
act_lookup_config            Lookup            ds_endpoint_config → endpoint_config.json
                                               First row only: No
act_get_username             Web Activity      GET key-vault-session-ded → voltgrid-username
act_get_password             Web Activity      GET key-vault-session-ded → voltgrid-password
  (parallel with lookup)
act_api_login                Web Activity      POST /api/auth/login/
                                               Body: @concat('{"username":"',act_get_username.output.value,'","password":"',act_get_password.output.value,'"}')
act_set_token                Set Variable      v_token = @activity('act_api_login').output.token
act_set_ingestion_date       Set Variable      v_ingestion_date = @formatDateTime(utcnow(),'yyyy-MM-dd')
act_loop_endpoints           ForEach           Items: @activity('act_lookup_config').output.value
                                               Sequential: false, Batch: 5
  └─ act_copy_endpoint       Copy Activity     Source URL: @item().endpoint
                                               Auth: Token @{variables('v_token')}
                                               Sink: bronze/api/@{item().table_name}/ingestion_date=@{variables('v_ingestion_date')}/page_1.json
  └─ act_check_rows          If Condition      @equals(activity('act_copy_endpoint').output.rowsCopied, 0)
       True branch:
         act_alert_zero      Web Activity      POST Teams webhook
                                               Body: @concat('{"text":"Zero rows for ',item().table_name,'"}')
       False branch:
         act_log_count       Append Variable   v_row_counts += @activity('act_copy_endpoint').output.rowsCopied
```

**Pipeline variables declared:**
- `v_token` (String)
- `v_ingestion_date` (String)
- `v_row_counts` (Array)

**Note on Append Variable in parallel ForEach:** `v_row_counts` may lose entries in parallel mode (see Q25). For production, log counts to ADLS Gen2 (a JSON file per endpoint) instead of an in-memory array — more reliable and persisted across pipeline runs.
