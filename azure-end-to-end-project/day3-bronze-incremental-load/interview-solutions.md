# Day 3 — Interview Solutions: Bronze Incremental Load

---

## Unity Catalog and Access Connector

**A1**
An Access Connector is an Azure resource (`Microsoft.Databricks/accessConnectors`) that has a system-assigned Managed Identity. Unity Catalog uses this MI to access storage — no password, no client secret, no rotation needed. Azure manages the identity.

| | Access Connector | Service Principal |
|---|---|---|
| Credentials | None — Azure handles token issuance | Client ID + Secret (expires, must rotate) |
| Risk of leak | None — no secret exists | Secret can be exposed in logs, code, ARM |
| Setup | Create resource + assign RBAC role | Create SP + create secret + store in KV + assign RBAC |
| Lifecycle | Tied to the Access Connector resource | Independent — must be managed separately |

It is preferred because it completely eliminates the credential management burden. If the Managed Identity is compromised (e.g., someone gains access to the Azure resource), the blast radius is limited to what the MI's RBAC roles allow — and RBAC roles can be revoked immediately.

---

**A2**
The three-layer chain:

```
Access Connector (Azure resource)
  └── has a system-assigned Managed Identity
  └── that MI has Storage Blob Data Contributor on evdatalakedev
        ↓ referenced by
  Storage Credential (Unity Catalog object)
    name: mi-ev-credential
    holds: the Access Connector resource ID
        ↓ referenced by
  External Location (Unity Catalog object)
    name: ext-loc-ev-bronze
    URL: abfss://bronze@evdatalakedev.dfs.core.windows.net/
    credential: mi-ev-credential
```

- **Access Connector:** provides the MI that has actual Azure RBAC access to storage
- **Storage Credential:** Unity Catalog's reference to that MI — decouples the Databricks-side config from the Azure resource ID
- **External Location:** maps a storage path to a credential — tells Unity Catalog "when any notebook accesses paths under `abfss://bronze@evdatalakedev...`, use `mi-ev-credential` to authenticate"

If the External Location is removed: notebooks lose the credential injection. Any `abfss://bronze@evdatalakedev...` path access fails with `No storage credential found for path`. The Storage Credential and Access Connector still exist — you just need to recreate the External Location.

---

**A3**
Most likely cause: the Access Connector's Managed Identity has `Storage Blob Data Reader` instead of `Storage Blob Data Contributor`, or the RBAC role hasn't propagated yet.

`Storage Blob Data Reader` grants Read + List but NOT Write. The test connection correctly shows ✅ Read, ✅ List and fails on Write.

Fix:
1. Portal → `evdatalakedev` → **Access Control (IAM)** → check existing role assignments for the AC's MI
2. If it has `Reader` → add role assignment: `Storage Blob Data Contributor` for the Access Connector MI
3. If the role was just assigned → wait 2 minutes for propagation → retest

---

**A4**
Unity Catalog's **credential injection** mechanism:

1. When the notebook executes `spark.read.format("delta").load("abfss://bronze@evdatalakedev...")`, Spark sends the path to the Unity Catalog driver
2. Unity Catalog looks up the External Locations registered in the metastore — finds `ext-loc-ev-bronze` covering `abfss://bronze@evdatalakedev.dfs.core.windows.net/`
3. Unity Catalog retrieves the Storage Credential `mi-ev-credential` linked to that External Location
4. Unity Catalog fetches a short-lived OAuth token using the Access Connector's Managed Identity
5. This token is injected into the Spark context for this specific storage path
6. The read proceeds — the notebook author never sees a credential

This is fundamentally different from `spark.conf.set()` (session-scoped, requires the notebook author to know the credential) and from account key (full access, never expiring, dangerous if leaked).

---

**A5**
The engineer should NOT block on File Events. The File Events failures are optional warnings — they affect event-driven features (auto-refresh of streaming tables, Storage Events triggers on the Databricks side) but have zero impact on regular Delta table reads and writes.

What File Events do: they enable Databricks to listen for blob-created/deleted events from ADLS Gen2 via EventGrid subscriptions. This allows Auto Loader and streaming tables to get notified of new files without polling. For this project's batch ADF ingestion, File Events are irrelevant.

The three extra RBAC roles needed to fix it (optional, can be done later):
- `Storage Account Contributor` — to create EventGrid subscriptions
- `EventGrid EventSubscription Contributor` — to manage subscriptions
- `Storage Queue Data Contributor` — to write to the notification queue

---

## Delta Control Table

**A6**
A **Managed Delta table** is owned by Unity Catalog — when you `DROP TABLE`, the underlying Parquet files and transaction log in the storage are also deleted. Unity Catalog chose the location when the table was created.

An **External Delta table** is created with `LOCATION 'abfss://...'` — you choose the storage path. Unity Catalog only manages the metadata. When you `DROP TABLE`, only the Unity Catalog metadata entry is removed. **The Delta files in ADLS Gen2 are NOT deleted** — they remain at `bronze/ctl/pipeline_control/`.

This project uses External because:
- The control table lives inside the Bronze container — it is part of the data lake, not an internal Databricks store
- If the Databricks workspace is destroyed and rebuilt, a `CREATE TABLE ... LOCATION '...'` re-registers the existing data without loss
- The watermark history is preserved even if Unity Catalog metadata is wiped

---

**A7**
`1900-01-01T00:00:00Z` is used as the "beginning of time" watermark — it means "fetch everything from the beginning" on the first run.

**Why not `null`:**
ADF passes `last_watermark` into a URL using `encodeUriComponent(variables('v_last_watermark'))`. If the value is `null`, the URL becomes:
```
/api/db/payments/?page=1&page_size=1000&updated_after=null
```
The API receives the string `"null"` as the filter value — depending on the server implementation, it may:
- Return 0 records (filter is invalid)
- Return an error
- Treat it as a literal string and find no matching dates

**Why not today's date:** On the first run you want ALL historical records, not just today's.

**Why 1900:** It is universally understood as "epoch/beginning of time" in ISO 8601 format. All real `updated_at` timestamps in the API are in 2025–2026 — so `updated_after=1900-01-01T00:00:00Z` returns everything.

---

**A8**
The error is in the ADF expression. `activity('act_read_ctl').output.runOutput` returns a **JSON string** — not a JSON object. You cannot use dot-notation directly on a string.

Wrong:
```
@activity('act_read_ctl').output.runOutput.endpoint
```

Correct — use `json()` to parse the string first, then dot-notation:
```
@json(activity('act_read_ctl').output.runOutput).endpoint
```

`json()` converts the string `'{"endpoint": "/api/db/payments/", ...}'` into an object, then `.endpoint` accesses the field. This is required any time a Databricks notebook returns structured data via `dbutils.notebook.exit(json.dumps(result))`.

---

**A9**
Two reasons soft-delete is better than hard delete:

1. **Audit trail:** The control table is the history of what was configured for each source. If someone asks "why was payments loading 500 records per page instead of 1000 last month?", a soft-deleted row can answer that. A hard delete erases the evidence permanently.

2. **Accidental reactivation safety:** If a source is deactivated and someone later wants to re-enable it, they check `is_active = false` rows, understand the configuration that was used before, and make an informed decision. With hard delete, they'd have to reconstruct the configuration from scratch — risking misconfiguration (wrong endpoint, wrong page size, lost watermark).

Additional reason: ADF pipelines may be re-triggered for a past date. A soft-deleted row with `is_active = false` causes the pipeline to intentionally skip it rather than silently failing because the row doesn't exist.

---

**A10**
Capturing `v_run_ts` at pipeline **start** (via `act_set_run_ts`) is correct.

If `utcnow()` was captured at the END — after all pages loaded — the watermark would be set to a time that is AFTER all the data was written. This sounds fine but creates a subtle gap:

```
Pipeline starts: 02:00 UTC
Pipeline ends:   02:25 UTC (25 minutes for 321 pages)

Records in API updated between 02:00 and 02:25 exist.

If watermark = 02:25:
  → next incremental run fetches records updated AFTER 02:25
  → records updated between 02:00 and 02:25 are MISSED ❌

If watermark = 02:00 (pipeline start):
  → next incremental run fetches records updated AFTER 02:00
  → records updated between 02:00 and 02:25 are included ✅
  → some records from this run are re-fetched but that is safe (idempotent MERGE in Silver handles it)
```

Capturing at start creates slight overlap (safe). Capturing at end creates a gap (data loss). Overlap is always preferable to a gap in incremental loads.

---

## Pagination and Until Loop

**A11**
```
total_pages = 321  ×  page_size = 1000  =  ~321,000 records
```
(The last page may have fewer than 1,000 records — the exact total from the API's `pagination.total` field is 320,217 based on the sample data.)

`total_pages` controls the Until loop exit condition:
```
@greaterOrEquals(variables('v_current_page'), add(variables('v_total_pages'), 1))
```
The loop runs while `v_current_page` is 2, 3, 4 ... 321. When `v_current_page` becomes 322 (= 321 + 1), the condition is true and the loop exits. Total iterations: 320 (pages 2 through 321; page 1 was already fetched separately).

---

**A12**
The most common cause: the Set Variable activity for the increment is placed **outside** the Until loop body instead of inside it.

In ADF, an Until activity has an inner body — you drag activities INTO the Until canvas. If `act_increment_page` is on the main canvas (outside Until), it runs once before the loop, incrementing from 2 to 3 — then the loop body runs with `v_current_page` stuck at 2 forever (the external increment doesn't affect loop state).

Fix: drag `act_increment_page` to be inside the Until body. Verify by clicking the Until activity → Edit (pencil icon) — the inner canvas shows only the activities inside the loop. Both `act_copy_page` and `act_increment_page` must appear there.

---

**A13**
When `act_copy_page` fails with a 429 on page 6:
- The Until loop iteration fails → the Until activity itself fails → the pipeline fails
- `act_update_watermark` does NOT run (it depends on Until success)
- The On Failure path → `act_alert_failure` runs → sends alert

**Retry policy:** Set retry directly on `act_copy_page` inside the Until loop:
- Retry count: `3`
- Retry interval: `60` seconds

This gives the API time to recover from rate limiting before ADF retries. With 3 retries + 60s interval, ADF waits up to 3 minutes before marking the activity failed. A 429 typically resolves in <30 seconds.

Additionally, set the retry at the Until activity level (outer retry) for catastrophic failures that exhaust inner retries.

---

**A14**
Page 1 is fetched separately because it serves two purposes simultaneously:
1. **Get the data** — write page 1 records to Bronze
2. **Get `total_pages`** — determine how many iterations the Until loop needs

Inside an ADF Until loop, you cannot read a variable that was set in the same iteration's activities from outside the loop before it starts. You need `total_pages` BEFORE the loop begins to define the exit condition. The only way to get it is to make at least one API call outside the loop.

Using a Web Activity for page 1 (rather than Copy Activity) lets you access the full response body including `pagination.total_pages` — which you then store in `v_total_pages` for the loop condition.

---

**A15**
The condition `@greaterOrEquals(v_current_page, add(v_total_pages, 1))` exits when `v_current_page > v_total_pages`:

```
v_total_pages = 321
Loop runs for: v_current_page = 2, 3, ... 321
After page 321: v_current_page becomes 322
322 >= 321 + 1 = 322  → true → EXIT ✅
```

If the condition was `@equals(v_current_page, v_total_pages)`:
```
After page 320: v_current_page = 321
321 == 321 → true → EXIT ← but page 321 hasn't run yet! ❌
```
`equals` exits before the last page is processed. `greaterOrEquals` with `v_total_pages + 1` is correct because the increment happens AFTER the Copy Activity — so when `v_current_page = v_total_pages`, the condition is still false (not yet done), the Copy runs, then the increment makes it `v_total_pages + 1`, which finally satisfies `greaterOrEquals`.

---

## Incremental Load and Idempotency

**A16**
Incremental load means: instead of fetching all records from the API every run, fetch only records that have changed since the last successful run.

`updated_after` is a query parameter the VoltGrid API accepts: `?updated_after=2026-10-14T02:00:00Z` — returns only records where `updated_at > '2026-10-14T02:00:00Z'`.

Its value comes from `ev_catalog.ctl.pipeline_control.last_watermark`. On the second run, this holds the timestamp written by `nb_update_watermark` after the first run succeeded. Instead of fetching 321 pages of full data, the incremental run might fetch only 3 pages of records changed in the last 24 hours.

---

**A17**
Idempotent means: running the pipeline multiple times with the same inputs produces the same output — no duplicates, no corruption, no extra files.

Concrete example:
- Run 1 on 2026-10-14: `updated_after = 1900-01-01`, `ingestion_date = 2026-10-14`
  - Writes: `bronze/api/payments/ingestion_date=2026-10-14/payments_page_001.json` ... `_321.json`
- Run 2 on 2026-10-14 (same day, same watermark — e.g. watermark update failed):
  - Same `updated_after = 1900-01-01`, same `ingestion_date = 2026-10-14`
  - Writes to the exact same file paths → **overwrites** with identical data
  - Result in ADLS: still 321 files, same content

The overwrite is safe because: same API request → same records → same JSON content → overwritten file is byte-for-byte identical (ordering may vary — but Silver's MERGE on `payment_id` handles dedup anyway).

---

**A18**
No data problem. The re-triggered pipeline:
- Same `updated_after` watermark (first run hasn't updated the control table yet if it's still running, or already did — either way)
- Same `ingestion_date = 2026-10-15`
- Same 5 pages → same file names → **overwrites with identical content**

ADF's sink writes files atomically — a partial write followed by overwrite from the same data produces a complete, correct file. The Bronze layer contains the same 5 files after both runs as after one run.

The watermark update notebook runs after the loop completes. If it runs twice (once per pipeline run), the second `UPDATE` sets `last_watermark` to the same value (same `v_run_ts` for both if they started within the same second) or a slightly later value — both are safe for the next incremental run.

---

**A19**
The expression in `act_set_watermark`:
```
@if(
  and(
    not(empty(pipeline().parameters.p_load_type_override)),
    equals(pipeline().parameters.p_load_type_override, 'full')
  ),
  '1900-01-01T00:00:00Z',
  json(activity('act_read_ctl').output.runOutput).last_watermark
)
```

Trace:
1. `pipeline().parameters.p_load_type_override = 'full'`
2. `not(empty('full'))` = `not(false)` = `true`
3. `equals('full', 'full')` = `true`
4. `and(true, true)` = `true`
5. → returns `'1900-01-01T00:00:00Z'` (the epoch / full load marker)

The control table's `last_watermark` column is **not changed before the run** — it still holds the previous watermark. The forced epoch is only used as `v_last_watermark` inside this pipeline run. After all pages succeed, `nb_update_watermark` writes `v_run_ts` (current UTC) back to the control table — exactly the same as a normal incremental run. The control table's history is preserved; the override only affects this single run's `updated_after` value.

---

**A20**
Bronze rerunning from page 1 is NOT a data problem — it is correct, expected, and safe.

When the pipeline reruns from page 1 on the same `ingestion_date`:
- Pages 1–199 are written again with identical data → overwrite existing files
- Pages 200–321 are written for the first time
- End state: all 321 files exist, all correct

Bronze is append-only at the partition level. Within a partition (same `ingestion_date`), overwrites are safe because the data is idempotent (same page → same records → same file content).

**Downstream in Silver:** Silver uses MERGE on `payment_id` (primary key). Re-processing pages 1–199 from Bronze in Silver upserts the same records — the MERGE matches on existing `payment_id` values and updates fields that haven't changed to the same values. No phantom duplicates. This is why the Medallion Architecture uses MERGE at Silver — Bronze can be replayed as many times as needed.

---

## File Naming and Path Design

**A21**
Without zero-padding, file names sort lexicographically instead of numerically:

```
Without padding:         With padding (3-digit):
payments_page_1.json     payments_page_001.json
payments_page_10.json    payments_page_002.json
payments_page_100.json   payments_page_003.json
payments_page_2.json         ...
payments_page_20.json    payments_page_099.json
                         payments_page_100.json
```

Without padding, `page_10` sorts before `page_2` (because `1` < `2`). Tools that process files in order (Spark glob reads, shell scripts, monitoring dashboards) process them out of sequence. This is especially a problem if files are processed by a downstream job that expects temporal ordering.

Zero-padded 3-digit numbers sort correctly up to 999 pages. The payments dataset has ~321 pages at 1,000 per page — well within this range.

---

**A22**
Two operational benefits of driving configuration from the control table:

1. **Zero-downtime source addition:** Adding a new API endpoint (e.g. `/api/db/customers/`) requires only an `INSERT INTO ev_catalog.ctl.pipeline_control ...` statement. No ADF pipeline changes, no deployment, no review cycle. The pipeline reads the new row and handles the new source with the exact same logic.

2. **Independent watermark management per source:** Each source has its own `last_watermark`. Payments can be at 2026-10-14 while sessions is still at 2026-09-30 (if it was added later or failed). A hardcoded parameter in ADF would require a different pipeline run for each source with manual watermark management. The control table tracks each source independently and automatically.

---

**A23**
Complete change required:

**Control table (1 SQL statement):**
```sql
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('sessions', '/api/db/sessions/', 'api/sessions', 'sessions', 1000, 'full', '1900-01-01T00:00:00Z', true);
```

**Databricks changes:** None. Both notebooks (`nb_read_control_table`, `nb_update_watermark`) use `source_name` as a parameter — they work for any source_name value.

**ADF changes:** None. The pipeline `pl_bronze_incremental_load` already reads everything from the control table. To trigger sessions ingestion, run:
- Trigger Now → `p_source_name = sessions`, `p_load_type_override = full`

The sink path resolves automatically to `bronze/api/sessions/ingestion_date=.../sessions_page_NNN.json` — no pipeline edit required.

---

**A24**
The folder `ingestion_date=2026-10-14` uses **Hive-style partition format** — key=value pairs in directory names. This is a standard convention understood by:

- **Apache Spark / Databricks:** When reading with `spark.read.format("json").load("abfss://bronze@.../api/payments/")`, Spark automatically discovers all `ingestion_date=*/` subdirectories and makes `ingestion_date` available as a partition column. No manual schema definition needed.
- **Delta Lake:** When converting Bronze JSON to Delta at Silver, partition pruning uses `WHERE ingestion_date = '2026-10-14'` to skip reading all other date folders — huge performance gain on large datasets.
- **Databricks SQL:** `SELECT * FROM bronze_payments WHERE ingestion_date = '2026-10-14'` prunes to just one folder.

Without the `ingestion_date=` prefix (just `2026-10-14/`), Spark reads the folder as a sub-path but does NOT expose it as a queryable partition column without explicit configuration.

---

**A25**
Design for parallelising page fetches in ADF while remaining idempotent:

**Approach: Batch processing with ForEach**

Instead of a sequential Until loop, generate a list of page numbers, then use **ForEach** with batch count = 5:

```
act_read_ctl → act_fetch_page1_meta (get total_pages) → act_generate_page_list
    ↓
act_generate_page_list (Set Variable):
  v_page_list = @range(1, add(variables('v_total_pages'), 1))
  → produces [1, 2, 3, ..., 321]
    ↓
ForEach (items: @variables('v_page_list'), batchCount: 5, isSequential: false)
  └── act_copy_page (Copy Activity, page = @item())
    ↓
act_update_watermark
```

**Why this is idempotent:** Same page number → same file name → same data. Running 5 pages in parallel produces the same files as running them sequentially. Overwrites are safe.

**Constraints and risks:**

| Risk | Mitigation |
|---|---|
| API rate limiting (429) | Set retry on ForEach inner activity: 3 retries, 60s interval |
| ADF concurrent activity cap (16 per pipeline) | Keep batchCount ≤ 10 to leave headroom |
| Partial failure — 3/5 pages succeed, 2 fail | The ForEach iteration fails → watermark is NOT updated → safe to rerun full batch |
| Memory pressure on ADF integration runtime | Reduce batchCount if Copy Activities consume significant memory per execution |
| Token expiry across 321 pages at 5 parallel | VoltGrid token may expire mid-run (~25 min run ÷ 5 = 5 min, token valid longer) — still monitor |

The watermark update only runs after ForEach succeeds — any failure leaves the watermark unchanged, making a full rerun safe and correct.

---
