# Day 5 — Remaining ADF Activities: Concept & Usage

> **Goal:** Understand the remaining activities from the General and Iteration & Conditionals categories that were listed but not explained in detail in Day 4.
> **No API required** — all examples use simple inline data, ADLS files, or Azure SQL so you can test without any external service.

---

## Activities Covered Today

From the ADF Activities panel:

```
General (remaining)
  ├── Lookup          ← read a config table or file
  ├── Delete          ← remove files from storage
  ├── Script          ← run arbitrary SQL on Azure SQL / Synapse
  ├── Wait            ← pause for N seconds
  └── WebHook         ← call a URL and wait for an async callback

Iteration & Conditionals (remaining)
  ├── Filter          ← reduce an array to matching items
  ├── ForEach         ← loop over an array
  ├── Switch          ← multi-branch routing
  └── Until           ← loop until condition is true
```

---

## Part 1: General Activities

---

### 1.1 Lookup Activity

**What it does:**
Reads rows from a dataset and returns them as a JSON array that the pipeline can use. Think of it as loading a small config table or file into memory so downstream activities can loop over it or reference values from it.

**Supported sources:** Azure SQL, ADLS Gen2 (JSON / CSV / Parquet), Cosmos DB, REST, and more.

**Key setting — First row only:**
- `Yes` → returns one object: `{ "firstRow": { ... } }` — use when you want a single config value
- `No` → returns all rows: `{ "count": N, "value": [ {...}, {...} ] }` — use with ForEach

**Simple example — load a list of table names from a JSON file:**

Config file (`bronze/config/tables.json`) uploaded to ADLS:
```json
[
  { "table": "payments",  "active": true  },
  { "table": "sessions",  "active": true  },
  { "table": "customers", "active": false }
]
```

```
Lookup Activity
  Dataset:        ds_tables_config   (points to tables.json)
  First row only: No

Output:
{
  "count": 3,
  "value": [
    { "table": "payments",  "active": true  },
    { "table": "sessions",  "active": true  },
    { "table": "customers", "active": false }
  ]
}
```

**How to reference the output:**
```
All rows:          @activity('lkp_tables').output.value
Row count:         @activity('lkp_tables').output.count
First row:         @activity('lkp_tables').output.firstRow
Row at index 1:    @activity('lkp_tables').output.value[1].table
```

**When to use:**
- Load endpoint configs, table lists, or environment settings from a file — then pass to ForEach
- Read the last successful watermark from a SQL table before running an incremental load
- Read a single config value (e.g., base URL, page size) using First row only: Yes

---

### 1.2 Delete Activity

**What it does:**
Deletes files or folders from ADLS Gen2, Azure Blob Storage, or a file system. Used to clean up processed files, remove old partitions, or archive data.

**Key settings:**
- **Dataset:** points to the file(s) or folder to delete
- **Recursive:** `true` deletes the folder and all its contents; `false` deletes only the specified file
- **Enable logging:** writes a log of what was deleted to an ADLS path — useful for audit

**Simple example — delete a processed landing file:**

```
Pipeline flow:
  act_copy_to_bronze    (Copy Activity — moves landing/payments.csv to Bronze)
       │ On Success
  act_delete_landing    (Delete Activity — removes landing/payments.csv)

Delete Activity settings:
  Dataset:   ds_landing_payments   (points to landing/payments.csv)
  Recursive: false
```

**Output in Monitor:**
```json
{ "filesDeleted": 1, "filesSkipped": 0, "dataRead": 0 }
```

If the file doesn't exist: `filesDeleted: 0, filesSkipped: 1` — Delete **does not fail** on missing files.

**Example — delete an entire date partition folder:**
```
Dataset path:  bronze/api/payments/ingestion_date=2026-09-01/
Recursive:     true

Output: { "filesDeleted": 5, "filesSkipped": 0 }  ← deleted 5 files inside the folder
```

**When to use:**
- After copying a landing file to Bronze, delete it from landing so it isn't processed twice
- Clean up temp files created during a pipeline run
- Remove old date partitions as part of a retention policy

**Important:** ADLS Gen2 deletion is irreversible unless soft delete is enabled on the storage account. Always test on non-critical files first.

---

### 1.3 Script Activity

**What it does:**
Runs arbitrary SQL statements directly on Azure SQL Database, Azure Synapse Analytics (dedicated SQL pool), or SQL Server. Unlike Stored Procedure Activity (which calls a pre-defined procedure), Script Activity lets you write inline SQL — DDL, DML, or queries — directly in the pipeline settings.

**Supported targets:** Azure SQL Database, Azure SQL Managed Instance, Synapse Dedicated SQL Pool, SQL Server (via Self-hosted IR)

**Key settings:**
- **Linked service:** your Azure SQL linked service
- **Script type:** `Query` (returns rows) or `NonQuery` (INSERT / UPDATE / DELETE / DDL — returns rows affected)
- **Scripts:** the SQL text — can use ADF dynamic content expressions

**When to choose Script over Stored Procedure:**
- You want to write quick inline SQL without creating a stored procedure in the database
- DDL operations: `CREATE TABLE`, `TRUNCATE TABLE`, `ALTER TABLE`
- One-off cleanup or maintenance SQL inside a pipeline

**Example 1 — truncate a staging table before loading:**
```
Script Activity
  Linked service: ls_azure_sql
  Script type:    NonQuery
  Script:         TRUNCATE TABLE stg_payments;
```

**Example 2 — insert an audit row with dynamic content:**
```
Script Activity
  Linked service: ls_azure_sql
  Script type:    NonQuery
  Script:
    INSERT INTO pipeline_audit (pipeline_name, run_date, status)
    VALUES (
      '@{pipeline().pipelineName}',
      '@{variables('v_run_date')}',
      'started'
    );
```

**Example 3 — query and return a watermark value (Script type: Query):**
```
Script Activity
  Script type: Query
  Script:      SELECT MAX(updated_at) AS last_run FROM pipeline_audit
               WHERE pipeline_name = 'pl_bronze_api_payments'

Output:
{
  "resultSets": [
    [ { "last_run": "2026-09-30T08:00:00" } ]
  ]
}

Reference downstream:
  @activity('act_get_watermark').output.resultSets[0][0].last_run
```

**Script Activity vs Stored Procedure Activity:**

| | Script Activity | Stored Procedure Activity |
|---|---|---|
| SQL location | Inline in ADF pipeline | Pre-defined in the database |
| Flexibility | Write any SQL ad hoc | Reuse pre-tested procedure |
| Returning values | Query type returns resultSets | Via OUTPUT parameters |
| Best for | Quick inline SQL, DDL, one-off ops | Reusable logic, complex upserts |

---

### 1.4 Wait Activity

**What it does:**
Pauses the pipeline execution for a fixed number of seconds before the next activity starts. No data is moved or processed — it simply sleeps.

**Key setting:**
- **Wait time in seconds:** integer, max 604800 (7 days)

**Simple examples:**

```
Example 1 — pause 5 seconds between two Copy Activities:
  act_copy_page_1  →  Wait (5s)  →  act_copy_page_2

Example 2 — pause 30 seconds inside a ForEach to respect API rate limits:
  ForEach (loop over endpoints)
    └─ act_copy_endpoint  →  Wait (30s)
```

**When to use:**
- Rate limiting — the target API allows only N requests per minute, so add a pause between calls
- Eventual consistency — an upstream write takes a few seconds to become visible to a downstream read
- Debugging — artificially slow a pipeline to observe intermediate Monitor states

**What Wait does NOT do:**
- It does not poll for a condition — use Validation Activity for that
- It does not adapt to load — the wait is always exactly N seconds regardless of what else is happening

**Wait in Monitor:**
```
act_wait_between_pages   Succeeded   Duration: 5s
```

---

### 1.5 WebHook Activity

**What it does:**
Calls an HTTP endpoint and then **pauses the pipeline**, waiting for the external system to call back an ADF callback URL with a success or failure signal. The pipeline resumes only after receiving the callback (or times out).

This is fundamentally different from Web Activity, which fires an HTTP call and moves on immediately with whatever the HTTP response was.

**The two-step flow:**

```
Pipeline
  ↓
WebHook Activity fires → POST to your external URL
  ↓ (pipeline PAUSED — waiting)
External system does its work (could take minutes or hours)
  ↓
External system POSTs back to ADF callback URL with { "Output": {...} }
  ↓
Pipeline RESUMES → next activity runs
```

**Key settings:**
- **URL:** the endpoint ADF calls to start the external job
- **Method:** POST (only POST is supported)
- **Body:** JSON payload sent to the external system — must include `@activity('WebHook1').output.callBackUri` so the external system knows where to call back
- **Authentication:** None, Basic, Client Certificate, or MSI
- **Timeout:** max time to wait for the callback. Format: `D.HH:MM:SS`. If no callback arrives within this time, the activity fails.

**Simple example — trigger a long-running data quality job:**

```
WebHook Activity settings:
  URL:    https://your-service.com/api/start-quality-check
  Method: POST
  Body:
    {
      "table": "payments",
      "callback_url": "@{activity('WebHook1').output.callBackUri}"
    }
  Timeout: 0.01:00:00  (1 hour)

Pipeline pauses here.

External service does quality check (could take 20 minutes).
External service POSTs back to callBackUri:
  { "statusCode": "200", "Output": { "rows_checked": 500, "errors": 0 } }

Pipeline resumes → next activity can reference:
  @activity('WebHook1').output.rows_checked  → 500
```

**WebHook vs Web Activity:**

| | Web Activity | WebHook Activity |
|---|---|---|
| Waits for response | Immediate HTTP response | Waits for async callback |
| Use when | Short synchronous calls (Key Vault, login) | Long-running async external jobs |
| Pipeline paused? | No — continues after HTTP response | Yes — paused until callback or timeout |
| Callback URL | Not applicable | External system must call back to ADF |

**When to use:**
- Trigger a long-running external job (quality check, ML training, file processing service) and wait for it to complete
- Any scenario where the external system needs minutes or hours and will notify ADF when done

---

## Part 2: Iteration & Conditionals Activities

---

### 2.1 Filter Activity

**What it does:**
Takes an array and returns a subset — only items where the condition is `true`. Exactly like a `WHERE` clause applied to an in-memory array.

**Key settings:**
- **Items:** the input array — usually from a Lookup output or a hardcoded array
- **Condition:** an expression evaluated for each item — `true` keeps it, `false` drops it
- Inside the condition, `@item()` refers to the current element being evaluated

**Simple example — keep only active tables:**

```
Input array (from Lookup):
[
  { "table": "payments",  "active": true  },
  { "table": "sessions",  "active": true  },
  { "table": "customers", "active": false }
]

Filter Activity
  Items:     @activity('lkp_tables').output.value
  Condition: @equals(item().active, true)

Output:
{
  "value": [
    { "table": "payments", "active": true },
    { "table": "sessions", "active": true }
  ],
  "filterCount": 2
}
```

**Another example — filter files larger than 1MB:**
```
Input: [{ "name": "big.json", "size": 2048000 }, { "name": "small.json", "size": 512 }]
Condition: @greater(item().size, 1000000)
Output: [{ "name": "big.json", "size": 2048000 }]
```

**Referencing Filter output downstream:**
```
@activity('act_filter_active').output.value          → the filtered array
@activity('act_filter_active').output.filterCount    → how many items passed
```

**When to use:**
- Remove inactive entries from a config list before passing to ForEach
- Skip already-processed files (filter out files where `status = 'done'`)
- Select only records matching a condition for special handling

---

### 2.2 ForEach Activity

**What it does:**
Loops over an array and runs a set of inner activities for each element — like a `for` loop in code. The inner activities run inside a separate canvas (you click into the ForEach to build the inner flow).

**Key settings:**

| Setting | What it controls |
|---|---|
| Items | The array to iterate — from Lookup, Filter, or a hardcoded `["a","b","c"]` |
| Is Sequential | `true` = one at a time in order. `false` = parallel |
| Batch count | Max parallel items when Sequential = false. Range: 1–50 |

**Inside ForEach, reference the current element:**
```
@item()              → the whole current object
@item().table        → a field of the current object
@item().endpoint     → another field
```

**Simple example — loop over a hardcoded list:**

```
ForEach Activity
  Items:         ["payments", "sessions", "customers"]
  Sequential:    true

  Inner canvas:
    Web Activity
      URL:   @concat('https://example.com/api/', item())
      Method: GET
```

Iteration 1: calls `https://example.com/api/payments`
Iteration 2: calls `https://example.com/api/sessions`
Iteration 3: calls `https://example.com/api/customers`

**Parallel vs Sequential — visual:**

```
Sequential (batch=1):           Parallel (batch=3):
  item1 → done                    item1 ─┐
  item2 → done                    item2 ─┼─ all run at same time
  item3 → done                    item3 ─┘
  Total = T1 + T2 + T3            Total = max(T1, T2, T3)
```

**Use Sequential when:**
- Items must be processed in order (page 1 before page 2)
- Target system allows only 1 concurrent connection

**Use Parallel (batch count > 1) when:**
- Items are independent of each other
- You want to reduce total run time

**Inner canvas note:** Variables declared at the pipeline level are shared — but Append Variable inside a parallel ForEach can have race conditions (two iterations appending at the same time). Use sequential if you need to collect results into a variable safely.

---

### 2.3 Switch Activity

**What it does:**
Evaluates a string expression and runs the matching case branch. Like an `if-else if-else` ladder or a `switch/case` statement in code. Each case is a separate inner canvas with its own activities.

**Key settings:**
- **On:** the expression to evaluate — must return a string
- **Cases:** named branches — the case value is matched against the expression result
- **Default:** the branch that runs if no case matches

**Simple example — route by environment name:**

```
Pipeline parameter: p_env  (String, default "dev")

Switch Activity
  On: @pipeline().parameters.p_env

  Case "dev":
    Set Variable → v_base_url = "https://dev.example.com"

  Case "staging":
    Set Variable → v_base_url = "https://staging.example.com"

  Case "prod":
    Set Variable → v_base_url = "https://prod.example.com"

  Default:
    Fail Activity → Message: "Unknown environment: @{pipeline().parameters.p_env}"
                    Error code: BAD_ENV
```

- Debug with `p_env = dev` → `v_base_url = https://dev.example.com`
- Debug with `p_env = uat` → Default fires → pipeline fails with `BAD_ENV`

**Another example — route by file type:**
```
Switch on: @pipeline().parameters.p_file_type

  Case "csv":   Copy Activity with CSV dataset
  Case "json":  Copy Activity with JSON dataset
  Case "parquet": Copy Activity with Parquet dataset
  Default:      Fail → "Unsupported file type"
```

**Switch vs If Condition:**

| | If Condition | Switch |
|---|---|---|
| Number of branches | 2 (True / False) | N (one per case + Default) |
| Expression type | Boolean (`true`/`false`) | String (matched against case values) |
| Use when | Binary decision | 3+ options on the same value |

---

### 2.4 Until Activity

**What it does:**
Repeats a set of inner activities until a boolean condition becomes `true`. Like a `do...while` loop — the inner activities always run at least once before the condition is checked.

**Key settings:**
- **Expression:** checked after each iteration — when `true`, the loop stops
- **Timeout:** max time the loop can run (default 7 days) — format `D.HH:MM:SS`

**Simple example — retry until success:**

```
Pipeline variables:
  v_success  (Boolean, default false)
  v_attempts (String,  default "0")
  v_temp     (String)

Until Activity
  Expression: @equals(variables('v_success'), true)
  Timeout:    0.00:10:00  (stop after 10 minutes if never succeeds)

  Inner canvas:
    Web Activity → act_call_api
      URL: https://example.com/api/status
      Method: GET

    If Condition → act_check_result
      Expression: @equals(activity('act_call_api').output.status, 'ready')

      True branch (API is ready → mark success):
        Set Variable → v_success = true

      False branch (not ready yet → increment counter):
        Set Variable → v_temp     = @string(add(int(variables('v_attempts')), 1))
        Set Variable → v_attempts = @variables('v_temp')
```

**Why two Set Variable activities for the counter?**
ADF does not allow a variable to read and write itself in the same expression. `v_attempts = add(variables('v_attempts'), 1)` throws a runtime error. The workaround:
1. Write the new value to a temp variable: `v_temp = add(variables('v_attempts'), 1)`
2. Copy temp to the real counter: `v_attempts = @variables('v_temp')`

**Iteration flow visual:**

```
Start → Inner activities run → check condition
  false → Inner activities run again → check condition
  false → Inner activities run again → check condition
  true  → Loop exits → next activity in pipeline
```

**Until vs ForEach:**

| | ForEach | Until |
|---|---|---|
| Know the list upfront? | Yes — pass the array | No — loop until condition |
| Fixed number of iterations? | Yes | No — depends on when condition is met |
| Pagination (unknown total pages) | Cannot | Use Until |
| Loop over a known endpoint list | Use ForEach | Awkward |

**When to use:**
- Paginate an API when you don't know the total pages upfront — loop until the page returns 0 rows
- Poll an external system until it reports `status = 'ready'`
- Retry logic — loop until success or max attempts reached

---

## Part 3: How These Activities Connect

A realistic pipeline using all eight Day 5 activities together — no external API needed, just ADLS Gen2 and Azure SQL:

```
pl_demo_day5  (standalone demo pipeline)

┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  [act_lookup_tables]                                        │
│  Lookup — reads tables.json from ADLS                       │
│  Output: [{table:"payments",active:true}, ...]              │
│                 │                                           │
│         [act_filter_active]                                 │
│         Filter — keeps only active:true rows                │
│                 │                                           │
│   [act_route_env]  ← Switch on p_env parameter             │
│   dev  → Set Variable: v_schema = "stg"                    │
│   prod → Set Variable: v_schema = "prod"                    │
│   Default → Fail                                            │
│                 │                                           │
│   [act_loop_tables]  ← ForEach over filtered array         │
│   ┌─────────────────────────────────────────────┐          │
│   │ Inner canvas (once per table):              │          │
│   │                                             │          │
│   │  [act_truncate_stg]                         │          │
│   │  Script — TRUNCATE TABLE stg.@{item().table}│          │
│   │                                             │          │
│   │  [act_wait_brief]                           │          │
│   │  Wait — 2 seconds                           │          │
│   │                                             │          │
│   │  [act_copy_to_stg]                          │          │
│   │  Copy — ADLS file → Azure SQL stg table     │          │
│   └─────────────────────────────────────────────┘          │
│                 │                                           │
│   [act_poll_until_ready]  ← Until loop                     │
│   Loop until SQL table row count > 0                        │
│   Polls every 10 seconds, max 5 minutes                     │
│                 │                                           │
│   [act_cleanup_landing]  ← Delete                          │
│   Delete — removes processed ADLS files                     │
│                 │                                           │
│   [act_notify_webhook]  ← WebHook                          │
│   Calls external quality-check service                      │
│   Waits for callback before pipeline ends                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Quick Reference — All Day 5 Activities

```
Activity      When to use
────────────────────────────────────────────────────────────────────────
Lookup        Load a config list or single value from a file/table
Delete        Remove processed files or old partitions from storage
Script        Run inline SQL (DDL, DML, SELECT) without a stored procedure
Wait          Pause N seconds — rate limiting, buffers, debugging
WebHook       Fire an async job and wait for it to call back before continuing
Filter        Reduce an array to items matching a condition
ForEach       Loop over a known array — sequential or parallel
Switch        Multi-branch routing based on a string value (3+ cases)
Until         Loop until a condition is true — unknown iteration count
```
