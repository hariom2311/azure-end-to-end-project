# Day 4 — ADF Activities Deep Dive

> **Goal:** Understand every activity in the **General** and **Iteration & Conditionals** categories — what each one does, when to use it, and how it fits into the `pl_bronze_api_payments` pipeline we already built.

---

## How Activities Are Organised in ADF Studio

When you open the Activities panel in Author view, activities are grouped into categories:

```
Activities Panel
├── General
│   ├── Copy data
│   ├── Web
│   ├── Set Variable
│   ├── Append Variable
│   ├── Execute Pipeline
│   ├── Get Metadata
│   ├── Delete
│   ├── Lookup
│   ├── Wait
│   └── Fail
│
├── Iteration & Conditionals
│   ├── ForEach
│   ├── If Condition
│   ├── Switch
│   ├── Until
│   └── Filter
│
├── Move & Transform
│   ├── Copy data        (also appears in General)
│   └── Data Flow
│
└── (others: Databricks, HDInsight, Azure Function, etc.)
```

We focus on **General** and **Iteration & Conditionals** — these cover 90% of real pipeline logic.

---

## Part 1: General Activities

### 1.1 Copy Data

The workhorse of ADF. Moves data from a source dataset to a sink dataset.

**What it does:**
- Reads from source (REST API, SQL, ADLS, Blob, S3, etc.)
- Optionally maps/converts columns
- Writes to sink (ADLS, SQL, Cosmos, etc.)
- Handles parallelism via DIUs (Data Integration Units)

**Already in our pipeline:** `act_copy_payments`

```
act_copy_payments
  Source:  ds_voltgrid_payments_src  → GET /api/db/payments/?page=1&page_size=100
           Authorization header: Token @{variables('v_token')}
  Sink:    ds_bronze_payments_sink   → bronze/api/payments/ingestion_date=2026-09-28/page_1.json
```

**Key settings:**

| Setting | What it controls |
|---|---|
| Source dataset | Where to read from |
| Sink dataset | Where to write to |
| Additional headers | Extra HTTP headers (e.g. Authorization) |
| Retry | How many times to retry on transient failure |
| Timeout | Max time before the activity is killed |
| DIUs | Compute parallelism (higher = faster, more expensive) |
| Fault tolerance | Skip incompatible rows vs fail immediately |

**When you use it:** Any time you move data from A to B — API to ADLS, SQL to ADLS, ADLS to SQL. It is used in every pipeline.

---

### 1.2 Web Activity

Makes an HTTP request to any URL and captures the JSON response. Not a data movement activity — it is a general-purpose HTTP call.

**Already in our pipeline:** `act_get_username`, `act_get_password`, `act_api_login`

```
act_get_username
  Method:  GET
  URL:     https://key-vault-session-ded.vault.azure.net/secrets/voltgrid-username/?api-version=7.0
  Auth:    Managed Identity (resource: https://vault.azure.net)
  Output:  { "value": "voltgrid_demo", ... }
  Used as: activity('act_get_username').output.value

act_api_login
  Method:  POST
  URL:     https://ev-project-navy-mu.vercel.app/api/auth/login/
  Body:    {"username": "voltgrid_demo", "password": "EVcharge@AU2025"}
  Output:  { "token": "abc123..." }
  Used as: activity('act_api_login').output.token
```

**Other uses of Web Activity:**
- Call Azure Key Vault to read a secret (as we do)
- Send a Teams / Slack alert on failure
- Trigger an Azure Function
- Notify an external system that Bronze data has landed
- Call any REST webhook

**Important:** Web Activity returns the full HTTP response body as a JSON object. You reference fields with `.output.fieldName`.

---

### 1.3 Set Variable

Stores a value into a pipeline variable so downstream activities can use it.

**Already in our pipeline:** `act_set_token`, `act_set_ingestion_date`

```
act_set_token
  Variable: v_token
  Value:    @activity('act_api_login').output.token

act_set_ingestion_date
  Variable: v_ingestion_date
  Value:    @formatDateTime(utcnow(), 'yyyy-MM-dd')
```

**Rules:**
- Can only set variables declared in the pipeline's Variables tab
- Cannot self-reference: `v_x = @add(variables('v_x'), 1)` is blocked — use a temp variable instead (see Until Activity section)
- Variables are pipeline-scoped — child pipelines do not share parent variables

**When you use it:** Any time the pipeline needs to compute and remember a value for later use — token storage, date capture, counters, watermarks.

---

### 1.4 Append Variable

Adds a value to a variable of type **Array**. Does not replace the array — appends one item to it.

**Example use case — collect page results:**
```
Pipeline variable: v_pages_fetched  (Array, starts as [])

Inside a ForEach loop, after each Copy Activity:
  Append Variable
    Variable: v_pages_fetched
    Value:    @activity('act_copy_payments').output.rowsCopied

After the loop: v_pages_fetched = [100, 100, 87]  ← rows copied per page
```

**How it differs from Set Variable:**

| | Set Variable | Append Variable |
|---|---|---|
| Variable type | Any (String, Int, Bool, Array) | Array only |
| Effect | Replaces the entire variable value | Adds one item to the end of the array |
| Use for | Single values | Building a list across loop iterations |

**When you use it:** Collecting results across ForEach iterations — page counts, error messages, filenames processed.

---

### 1.5 Execute Pipeline

Calls another ADF pipeline and optionally waits for it to finish.

```
Pipeline A (orchestrator)
  ├── Execute Pipeline → pl_bronze_api_payments   (wait: true)
  ├── Execute Pipeline → pl_bronze_api_sessions   (wait: true)
  └── Execute Pipeline → pl_bronze_api_customers  (wait: true)
```

**Settings:**

| Setting | Options | What it means |
|---|---|---|
| Pipeline | Select any pipeline in the factory | The child pipeline to run |
| Wait on completion | Yes / No | Yes = parent waits for child to finish before moving on. No = fires and forgets |
| Parameters | Key-value pairs | Values passed to the child pipeline's parameters |

**When to use:**
- Break a complex pipeline into smaller, reusable child pipelines
- An orchestrator pipeline runs multiple child pipelines in sequence or parallel
- Reuse the same login pipeline across many ingestion pipelines

**In the VoltGrid project:** You could have one `pl_orchestrator` that calls `pl_bronze_api_payments`, `pl_bronze_api_sessions`, `pl_bronze_api_customers` in sequence — each child pipeline handles its own endpoint.

```
pl_orchestrator
  Execute Pipeline: pl_bronze_api_payments   ─On Success─►
  Execute Pipeline: pl_bronze_api_sessions   ─On Success─►
  Execute Pipeline: pl_bronze_api_customers
```

**Parallel child pipelines:**
To run all three simultaneously, set **Wait on completion: No** on all three Execute Pipeline activities (they fire and ADF does not wait). Use an If Condition or a downstream activity with On Completion dependency to check results.

---

### 1.6 Get Metadata

Reads metadata about a file or folder in ADLS Gen2 or Blob Storage — without reading the file content. Returns properties like file size, last modified time, child items (files in a folder), exists or not.

**Common field values you can request:**

| Field name | What it returns |
|---|---|
| `exists` | `true` / `false` — does the file/folder exist? |
| `size` | File size in bytes |
| `lastModified` | Last modified timestamp |
| `itemType` | `"File"` or `"Folder"` |
| `childItems` | Array of files/folders inside a directory |

**Example use case — check if Bronze file landed before running Silver:**

```
Get Metadata
  Dataset: ds_bronze_payments_sink
  Field list: [exists, size]
  Output: { "exists": true, "size": 45320 }

→ If Condition
  Expression: @and(activity('act_check_file').output.exists,
                   greater(activity('act_check_file').output.size, 0))
    True  → Databricks Notebook (run Silver transform)
    False → Fail Activity ("Bronze file missing or empty")
```

**When you use it:** Before running a transformation — verify the source file exists and is not empty. Before archiving — check what files are in a folder.

---

### 1.7 Delete Activity

Deletes files or folders from ADLS Gen2, Blob Storage, or a file system.

**Settings:**
- Dataset pointing to the file(s) to delete
- **Recursive:** delete folder and all its contents
- **Max concurrent connections:** parallelism for bulk deletes

**Example use case — clean up landing zone after ingestion:**

```
act_copy_payments    ─On Success─►
act_delete_landing
  Dataset: ds_landing_payments   → bronze/landing/payments/settlements_2026-09-28.csv
  Recursive: false
```

**When you use it:**
- Remove a file from a landing zone after it has been processed and moved to Bronze
- Clean up temp files created during a pipeline run
- Archive pattern: copy file to archive location, then delete original

**Caution:** Deletion is irreversible in ADLS Gen2 unless soft delete is enabled on the storage account. Always test with a non-critical file first.

---

### 1.8 Lookup Activity

Runs a query against a dataset and returns the result rows as a JSON array. The pipeline can then use those rows to drive downstream logic.

**Supports:** Azure SQL, Cosmos DB, ADLS Gen2 (JSON/CSV/Parquet), REST, and more.

**Output structure:**
```json
{
  "count": 3,
  "value": [
    { "table_name": "payments",  "endpoint": "/api/db/payments/" },
    { "table_name": "sessions",  "endpoint": "/api/db/sessions/" },
    { "table_name": "customers", "endpoint": "/api/db/customers/" }
  ],
  "firstRow": { "table_name": "payments", "endpoint": "/api/db/payments/" }
}
```

**Reference in expressions:**
```
@activity('act_lookup_config').output.value          → full array
@activity('act_lookup_config').output.count          → number of rows
@activity('act_lookup_config').output.firstRow       → first row only
@activity('act_lookup_config').output.value[0].table_name  → "payments"
```

**Example use case — metadata-driven pipeline for all VoltGrid endpoints:**

Instead of hardcoding 5 Copy Activities, store the config in a JSON file:

```json
// bronze/config/endpoint_config.json
[
  { "table_name": "payments",  "endpoint": "/api/db/payments/"  },
  { "table_name": "sessions",  "endpoint": "/api/db/sessions/"  },
  { "table_name": "customers", "endpoint": "/api/db/customers/" },
  { "table_name": "stations",  "endpoint": "/api/db/stations/"  },
  { "table_name": "vehicles",  "endpoint": "/api/db/vehicles/"  }
]
```

```
Lookup Activity
  Dataset: ds_endpoint_config   → bronze/config/endpoint_config.json
  First row only: No            → returns all 5 rows
  Output: { value: [{...}, {...}, ...] }

ForEach Activity
  Items: @activity('act_lookup_config').output.value
  → Copy Activity per item: source endpoint = @item().endpoint
```

Adding a 6th endpoint = edit the JSON file. Zero pipeline changes.

**When you use it:** Read a config table/file to drive a ForEach loop. Read the watermark from a `pipeline_audit` Delta table. Read a list of tables to process.

---

### 1.9 Wait Activity

Pauses the pipeline for a fixed number of seconds before the next activity runs.

**Settings:**
- Wait time in seconds (max 604800 = 7 days)

**Example use case — respect API rate limits:**

```
act_copy_payments_page_1   ─On Success─►
Wait (30 seconds)          ─On Success─►
act_copy_payments_page_2
```

Or inside a ForEach — add a small pause between iterations to avoid hitting the API rate limit.

**When you use it:**
- API rate limiting — pause between calls so you don't get 429 Too Many Requests
- Waiting for an external system to finish processing before polling it
- Adding a buffer between a write and a downstream read (eventual consistency)

**Caution:** Wait time counts against the pipeline timeout. A pipeline has a 24-hour default timeout — very long Waits can cause issues.

---

### 1.10 Fail Activity

Deliberately fails the pipeline with a custom error message and error code. Used to enforce business rules.

**Settings:**
- **Message:** the error text that appears in Monitor
- **Error code:** a custom code you define (string)

**Example use case — stop the pipeline if the API returned 0 rows:**

```
act_copy_payments    (rowsCopied available in output)
    ↓ On Success
If Condition
  Expression: @equals(activity('act_copy_payments').output.rowsCopied, 0)
    True →
      Fail Activity
        Message:    "API returned 0 records for payments endpoint"
        Error code: "EMPTY_SOURCE"
    False →
      (continue pipeline)
```

**Monitor shows:** the Fail Activity's message as the pipeline failure reason — much clearer than a generic system error.

**When you use it:**
- Assert a business rule (e.g. "we must always get at least 1 row")
- Make an implicit failure explicit with a clear message
- Stop a pipeline when a prerequisite check fails

---

## Part 2: Iteration & Conditionals Activities

### 2.1 If Condition

Evaluates a boolean expression and runs one of two branches — True or False.

```
If Condition
  Expression: @equals(pipeline().parameters.p_load_type, 'full')
  ┌─ True branch ──────────────────────────────────────────┐
  │  Set Variable: v_watermark = '1900-01-01T00:00:00Z'   │
  └────────────────────────────────────────────────────────┘
  ┌─ False branch ─────────────────────────────────────────┐
  │  Set Variable: v_watermark = @pipeline().parameters.p_watermark  │
  └────────────────────────────────────────────────────────┘
```

**Important:** The True and False branches are separate mini-canvases — you click into each to add activities. Activities inside a branch are invisible from the outer pipeline canvas.

**Example in the VoltGrid pipeline — full vs incremental load switch:**

```
Pipeline parameter: p_load_type  (String, "full" or "incremental")
Pipeline variable:  v_watermark  (String)

If Condition: @equals(pipeline().parameters.p_load_type, 'full')
  True:  Set Variable → v_watermark = '1900-01-01T00:00:00Z'
  False: Set Variable → v_watermark = @pipeline().parameters.p_watermark

act_copy_payments source URL:
  /api/db/payments/?page=1&updated_after=@{variables('v_watermark')}

Full load:        updated_after=1900-01-01T00:00:00Z  → returns everything
Incremental:      updated_after=2026-09-27T01:00:00Z  → returns only recent records
```

**Rules:**
- Expression must evaluate to `true` or `false` (boolean)
- False branch is optional — leave it empty if nothing should happen on False
- Both branches can contain multiple activities chained together

---

### 2.2 Switch Activity

Like an If Condition but for more than two branches. Evaluates an expression and matches it against case values — like a `switch/case` in code.

```
Switch Activity
  Expression: @pipeline().parameters.p_endpoint

  Case "payments":
    Copy Activity → source: /api/db/payments/
  
  Case "sessions":
    Copy Activity → source: /api/db/sessions/
  
  Case "customers":
    Copy Activity → source: /api/db/customers/
  
  Default:
    Fail Activity → "Unknown endpoint: @{pipeline().parameters.p_endpoint}"
```

**When you use it:**
- More than 2 branches based on a single value
- Route pipeline logic based on a parameter (endpoint name, environment, region)

**vs. If Condition:** If Condition = 2 branches (True/False). Switch = N branches (one per case + default).

---

### 2.3 ForEach Activity

Loops over an array and runs inner activities for each item. The inner canvas is separate — you click into the ForEach to add the activities that repeat.

**Settings:**

| Setting | What it controls |
|---|---|
| Items | The array to iterate — usually from Lookup output or a hardcoded array |
| Is Sequential | `true` = one item at a time. `false` = items run in parallel |
| Batch count | Max parallel items (1–50). Only applies when Sequential = false |

**Reference the current item in inner activities:**
```
@item()                    → the entire current item object
@item().table_name         → a specific field of the current item
@item().endpoint           → another field
```

**Example — loop over all 5 VoltGrid endpoints:**

```
Lookup (reads endpoint_config.json)
  output.value = [
    { "table_name": "payments",  "endpoint": "/api/db/payments/"  },
    { "table_name": "sessions",  "endpoint": "/api/db/sessions/"  },
    ...
  ]

ForEach
  Items:         @activity('act_lookup_config').output.value
  Sequential:    false
  Batch count:   5

  Inner canvas:
    Web Activity (login) → Set Variable (token) → Copy Activity
      Source URL:  @concat(pipeline().parameters.p_base_url, item().endpoint)
      Sink folder: bronze/api/@{item().table_name}/ingestion_date=@{variables('v_ingestion_date')}/
```

**Visual — parallel ForEach execution:**

```
Lookup returns 5 items

ForEach (batch=5, sequential=false):
  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐
  │ payments   │  │ sessions   │  │ customers  │  │ stations   │  │ vehicles   │
  │ Copy →     │  │ Copy →     │  │ Copy →     │  │ Copy →     │  │ Copy →     │
  │ Bronze     │  │ Bronze     │  │ Bronze     │  │ Bronze     │  │ Bronze     │
  └────────────┘  └────────────┘  └────────────┘  └────────────┘  └────────────┘
  All 5 run simultaneously → total time = slowest single endpoint
```

**Sequential ForEach — when to use it:**
```
ForEach (sequential=true):
  item 1 → completes →
  item 2 → completes →
  item 3 → completes
  Total time = sum of all items
```

Use sequential when:
- API allows only 1 concurrent connection
- Items must be processed in order (e.g. page 1 before page 2)
- Downstream system cannot handle parallel writes

---

### 2.4 Until Activity

A loop that keeps repeating its inner activities until a boolean condition becomes `true`. Like a `do-while` loop in code.

**Settings:**
- **Expression:** checked after each iteration — when `true`, the loop stops
- **Timeout:** max duration before the loop is forcibly stopped (default 7 days)

**The ADF self-reference limitation and workaround:**

You cannot do `v_current_page = v_current_page + 1` in one Set Variable. You need two variables:

```
Variables:
  v_current_page  (int, starts at 1)
  v_temp_page     (int, starts at 0)
  v_total_pages   (int, set before the loop)

Until condition: @greaterOrEquals(variables('v_current_page'), variables('v_total_pages'))

Inner canvas:
  act_copy_page
    Source: ?page=@{variables('v_current_page')}
    Sink:   page_@{variables('v_current_page')}.json

  act_set_temp_page
    v_temp_page = @add(variables('v_current_page'), 1)

  act_increment_page
    v_current_page = @variables('v_temp_page')
```

**Visual — Until loop pagination for VoltGrid payments:**

```
API has 15 pages of payments (v_total_pages = 15)

Iteration 1:  v_current_page=1  → copies page_1.json  → v_current_page becomes 2
Iteration 2:  v_current_page=2  → copies page_2.json  → v_current_page becomes 3
...
Iteration 15: v_current_page=15 → copies page_15.json → v_current_page becomes 16
              condition check: 16 >= 15 → TRUE → loop exits

Bronze result:
  bronze/api/payments/ingestion_date=2026-09-28/
    page_1.json   page_2.json   page_3.json   ...   page_15.json
```

**When to use Until vs ForEach:**

| | ForEach | Until |
|---|---|---|
| You know the list upfront | ✅ perfect | ❌ awkward |
| You don't know how many iterations | ❌ can't | ✅ perfect |
| Paginating an API where total pages is returned in the response | ❌ | ✅ |
| Looping over a fixed list of endpoints | ✅ | ❌ |

**In the VoltGrid project:** Until is used for pagination (total pages unknown until first API call). ForEach is used for looping over endpoints (list is fixed and known).

---

### 2.5 Filter Activity

Filters an array to keep only items that match a condition. Returns a subset of the input array.

**Settings:**
- **Items:** input array (usually from Lookup or another activity output)
- **Condition:** expression evaluated per item — `true` keeps the item, `false` drops it

**Example — filter only failed endpoints from a config:**

```
Filter Activity
  Items:     @activity('act_lookup_config').output.value
  Condition: @equals(item().status, 'active')

Output: @activity('act_filter_active').output.value  → only active endpoints

ForEach
  Items: @activity('act_filter_active').output.value
  → only processes active endpoints
```

**Another example — filter large files for special handling:**

```
Get Metadata returns child items with sizes.
Filter: @greater(item().size, 100000000)  → files > 100MB
ForEach the filtered list → use different DIU settings for large files
```

**When you use it:** Pre-process a list before ForEach — remove inactive items, skip already-processed files, select only records matching a condition.

---

## Part 3: How They All Connect in the VoltGrid Pipeline

Here is a realistic Day 4 version of the pipeline using most of the activities above:

```
pl_bronze_api_all_endpoints  (Day 4 concept pipeline)

┌──────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  [act_get_username] ──┐                                                      │
│  [act_get_password] ──┴──► [act_api_login] ──► [act_set_token]              │
│                                                        │                     │
│                                                   On Success                 │
│                                                        ↓                     │
│  [act_set_ingestion_date]                                                    │
│   v_ingestion_date = @formatDateTime(utcnow(), 'yyyy-MM-dd')                 │
│                                                        │                     │
│                                                   On Success                 │
│                                                        ↓                     │
│  [act_lookup_config]                                                         │
│   Reads bronze/config/endpoint_config.json                                   │
│   Returns: [{table_name, endpoint}, ...]                                     │
│                                                        │                     │
│                                                   On Success                 │
│                                                        ↓                     │
│  [act_filter_active]   ← Filter Activity                                     │
│   Keeps only items where status = "active"                                   │
│                                                        │                     │
│                                                   On Success                 │
│                                                        ↓                     │
│  [ForEach — act_loop_endpoints]                                              │
│   Items: @activity('act_filter_active').output.value                         │
│   Sequential: false  │  Batch count: 5                                       │
│   ┌─────────────────────────────────────────────────────────┐                │
│   │  Inner canvas (runs once per endpoint):                 │                │
│   │                                                         │                │
│   │  [act_copy_endpoint]   ← Copy Activity                  │                │
│   │   Source: @item().endpoint with Authorization header    │                │
│   │   Sink:   bronze/api/@{item().table_name}/              │                │
│   │           ingestion_date=@{variables('v_ingestion_date')}│               │
│   │                                                         │                │
│   │  [act_check_rows]   ← If Condition                      │                │
│   │   Expression: @equals(activity('act_copy_endpoint')     │                │
│   │               .output.rowsCopied, 0)                    │                │
│   │     True  → Fail Activity "Zero rows for @{item().table_name}" │         │
│   │     False → Append Variable: v_pages_fetched            │                │
│   │             += activity('act_copy_endpoint').output.rowsCopied │         │
│   └─────────────────────────────────────────────────────────┘                │
│                                                        │                     │
│                                                 On Failure                   │
│                                                        ↓                     │
│  [act_alert_failure]   ← Web Activity                                        │
│   POST Teams webhook: "Pipeline FAILED: @{pipeline().pipelineName}"          │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Activities used in this concept pipeline:**
- Web Activity — login + alert
- Set Variable — token, ingestion date
- Lookup — config file
- Filter — only active endpoints
- ForEach — loop over endpoints
- Copy Data — fetch and write each endpoint
- If Condition — check for zero rows
- Fail — explicit failure message
- Append Variable — collect row counts

---

## Quick Reference — Activity Decision Guide

```
I need to...                                  Use this activity
────────────────────────────────────────────────────────────────
Move data from A to B                      →  Copy Data
Call an HTTP endpoint (auth, webhook)      →  Web Activity
Store a computed value for later           →  Set Variable
Add an item to a list across iterations   →  Append Variable
Run another pipeline                       →  Execute Pipeline
Check if a file exists / get file size    →  Get Metadata
Remove a file after processing            →  Delete
Read a config table / query results       →  Lookup
Pause between API calls                   →  Wait
Stop the pipeline with a clear message    →  Fail
Two branches (true/false)                 →  If Condition
Many branches (A/B/C/D)                   →  Switch
Loop over a known list                    →  ForEach
Loop until a condition is met             →  Until
Remove items from an array                →  Filter
```
