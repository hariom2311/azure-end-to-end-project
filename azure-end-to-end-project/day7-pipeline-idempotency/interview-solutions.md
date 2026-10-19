# Day 7 — Interview Solutions: Pipeline Idempotency + Blob Storage Ingestion

---

## Pipeline Idempotency and RunId

**A1**
The overwrite bug occurs when two pipeline runs on the same day write to the same folder path.

**Scenario:**
```
Run 1 — 09:00, full load, watermark = 1900-01-01
  → API returns 500 pages
  → Sink: api/payments/ingestion_date=2026-10-18/payments_page_1.json ... payments_page_500.json
  → Watermark updated to: 2026-10-18T09:00:00Z

Run 2 — 14:00, incremental, watermark = 2026-10-18T09:00:00Z
  → API returns 50 new pages
  → Sink: api/payments/ingestion_date=2026-10-18/payments_page_1.json ... payments_page_50.json
  → payments_page_1.json through payments_page_50.json are OVERWRITTEN
  → payments_page_51.json through payments_page_500.json from Run 1 are now orphaned
```

**Root cause:** The sink folder path only contains `ingestion_date`, which is the same for every run on the same calendar day. ADF Copy Activity overwrites files with identical names by default. When Run 2 writes fewer pages than Run 1, the old content from pages 1–50 is replaced with Run 2's data, while pages 51–500 still contain Run 1's data — Bronze is now mixed and invalid.

---

**A2**
`pipeline().RunId` is a built-in ADF system variable — a UUID (128-bit GUID in `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` format) that ADF assigns to each pipeline run at the moment the run is created. It is unique across every run, including manual re-runs of the same trigger window.

**Why preferred over `utcnow()`:**

| Property | `pipeline().RunId` | `utcnow()` |
|---|---|---|
| Format | `abc12345-1234-1234-1234-abcdef123456` | `2026-10-18T09:00:00.0000000Z` |
| Path-safe | Yes — only letters, digits, hyphens | No — colons in ADLS paths are rejected |
| Unique per run | Yes — assigned once at run creation | No — two calls in same millisecond return same value |
| Requires transformation | None | Must `replace(utcnow(), ':', '')` — fragile |
| Available without extra activity | Yes — built-in system variable | Yes — but requires `replace()` chain |

`pipeline().RunId` is already path-safe, unique per run, and requires zero transformation. It is the correct choice.

---

**A3**
After the second run:

```
blob/invoices/ingestion_date=2026-10-18/INV-AU-2026-0002266.pdf  ← Run 2 overwrote Run 1
blob/invoices/ingestion_date=2026-10-18/INV-AU-2026-0002267.pdf  ← Run 2 overwrote Run 1
blob/invoices/ingestion_date=2026-10-18/INV-AU-2026-0002268.pdf  ← Run 2 overwrote Run 1
blob/invoices/ingestion_date=2026-10-18/INV-AU-2026-0002270.pdf  ← Run 1 only (Run 2 didn't touch)
... (12 more from Run 1, untouched)
```

**Is it a data problem?** For invoices: maybe not — if the same invoice file name appears in both runs and the content is identical, overwriting is safe. But if Run 2 wrote _fewer_ files (e.g. only 3 matching the wildcard at that time), then 12 files from Run 1 survive untouched. This creates an inconsistent snapshot: 3 "current" files + 12 "stale" files from the first run in the same folder. Downstream Silver processing cannot tell which files belong to which run — **this is a correctness problem.**

---

**A4**
After the second run (with RunId isolation):

```
blob/invoices/ingestion_date=2026-10-18/run_id=abc12345-.../  ← Run 1: 15 PDF files
blob/invoices/ingestion_date=2026-10-18/run_id=def67890-.../  ← Run 2: 3 PDF files
```

**Total:** 18 files across 2 subfolders. Run 1's 15 files are completely untouched.

Silver reads both subfolders using a wildcard: `blob/invoices/ingestion_date=2026-10-18/run_id=*/` and deduplicates by invoice ID. If Run 2's 3 files are updated versions of 3 from Run 1, MERGE keeps the latest. The 12 files only in Run 1 are preserved. Bronze is correct and complete.

---

**A5**
Each child pipeline run gets its **own unique RunId** — it does not inherit the master pipeline's RunId.

When `pl_bronze_master` calls `ExecutePipeline` for `pl_bronze_incremental_load_v3`, ADF creates a brand new pipeline run for the child. That child run is assigned a new GUID by ADF. The master's `pipeline().RunId` is only available within the master pipeline's own expression context.

This means even in parallel execution (Day 6's `pl_bronze_master_parallel` with `batchCount: 5`), all five concurrent child runs each get unique RunIds, so their sink paths never collide:
```
api/payments/ingestion_date=2026-10-18/run_id=child-run-1-guid/
api/sessions/ingestion_date=2026-10-18/run_id=child-run-2-guid/
```

---

**A6**
Storing `pipeline().RunId` in a String variable is **technically possible** but **unnecessary**. You would use a Set Variable activity:
```
Variable: v_run_id
Value: @pipeline().RunId
```
This works because `pipeline().RunId` is already a String.

However, it is not necessary. `pipeline().RunId` is available in every expression context throughout the pipeline with zero setup — in Copy Activity sink parameters, dataset parameters, Web Activity URLs, etc. Creating a variable just adds an activity with no benefit.

**What happens:** The Set Variable activity succeeds and `v_run_id` holds the GUID. Using `@variables('v_run_id')` vs `@pipeline().RunId` in downstream activities produces identical values. Skip the variable — use `pipeline().RunId` directly.

---

## Blob Storage and SAS Authentication

**A7**
A **Shared Access Signature (SAS)** is a token appended to an Azure Storage URL that grants time-limited, permission-scoped access to a storage resource without exposing the account key.

`sp=rl` means **signed permissions = read + list**:
- `r` — read: download blob contents
- `l` — list: enumerate blobs in a container or directory

This is a read-only SAS. The token cannot write, delete, or create blobs. Even if the SAS URL is leaked, an attacker can only read/list the `source` container — they cannot modify or delete any data.

---

**A8**
Two main reasons to use SAS over Account Key or Managed Identity for an external source:

**1. You do not own the account — Account Key is not available.**
The `dataenggdailystorage` account belongs to the EV hardware vendor. They will not share their account key (which grants full administrative control). SAS is the standard mechanism for granting scoped access to external consumers without sharing root credentials.

**2. Managed Identity only works within your own tenant.**
ADF's Managed Identity is granted access via Azure RBAC on resources in your own Azure subscription. The vendor's storage account is in their subscription/tenant. RBAC assignments across tenants require cross-tenant configurations the vendor would need to set up. SAS is stateless and works across tenants with no configuration on either side beyond sharing the URL.

---

**A9**
On 2026-10-20, the SAS token is expired. Every pipeline run triggers `ls_blob_source` to authenticate — ADF sends the SAS URL to Azure Storage, which checks the `se` (signed expiry) parameter. The request is rejected with `403 AuthenticationFailed: Signed expiry time is already past`. All three copy activities fail immediately — no data is read or written.

**Correct renewal process:**
1. Contact the vendor to generate a new SAS token (or if you have access to their storage account, use Azure Portal → Storage account → Shared access signature → new expiry → Generate SAS)
2. In ADF Studio → **Manage** → **Linked services** → `ls_blob_source` → **Edit**
3. Replace the SAS URI with the new token's full URL
4. **Test connection** → green
5. **Publish all**
6. Re-run any failed pipeline runs

**Proactive practice:** Set a reminder 30 days before expiry. Consider storing the SAS URI in Azure Key Vault and referencing it from the linked service — Key Vault reference means you update only the secret, not the linked service itself.

---

**A10**
`ls_adls_bronze` connects to your own ADLS Gen2 account (`evdatalakedev2`) using Managed Identity authentication. Using it to read from `dataenggdailystorage` is wrong for two reasons:

1. **Authentication mismatch:** Managed Identity is bound to `evdatalakedev2`. When ADF uses `ls_adls_bronze` to construct a request to `dataenggdailystorage`, the MI token is issued for `evdatalakedev2.dfs.core.windows.net` — Azure Storage rejects it with `403 AuthorizationPermissionMismatch` because the MI has no role assignment on the external account.

2. **Account name is hardcoded in the linked service:** `ls_adls_bronze` has `accountName: evdatalakedev2` in its `typeProperties`. A dataset built on this linked service constructs its base URL as `https://evdatalakedev2.dfs.core.windows.net/`. Pointing it at `dataenggdailystorage` is not possible without editing the linked service — at which point you've broken `ls_adls_bronze` for everything else.

A separate linked service (`ls_blob_source`) is mandatory for a separate storage account.

---

**A11**
`sr=c` (container-level SAS) grants access to all blobs in the container. `sr=b` (blob-level SAS) grants access to exactly one blob by its full path.

**Practical difference for folder reads:**
ADF's copy activity reads a folder by first listing its blobs (using the `list` permission on the container), then reading each blob individually. A container-level SAS covers both operations in a single token.

A blob-level SAS (`sr=b`) specifies exactly one blob path (e.g. `config/states.xml`). You would need a separate SAS token for each of the 19 config files, 15+ invoice PDFs, and 3 report JSON files — impossible to manage at pipeline level.

For any "read a folder of files" use case, `sr=c` is the only practical choice.

---

## Dataset Design and Format Handling

**A12**
PDF files must use **`BinarySource`** (Binary dataset type).

PDFs are binary-encoded documents — they are not text, XML, JSON, or any format ADF can parse into rows. ADF's format-specific sources (XmlSource, JsonSource, DelimitedTextSource, etc.) attempt to parse the file content into a tabular schema. Applying any of those parsers to a PDF binary stream will fail with a parse error.

`BinarySource` treats every file as an opaque byte stream and copies it without any parsing or schema inference. This is the correct approach for files where the content is consumed by a downstream system (e.g. an invoice processing service), not by ADF or Databricks column-level operations.

---

**A13**
The sink dataset `ds_bronze_blob_sink` is `Binary` type because Bronze is a raw landing zone — files land exactly as they were in the source, byte-for-byte. No transformation, no schema enforcement, no format conversion.

Using a format-specific sink (e.g. `JsonSink`) would:
1. Require the sink to know the schema of the incoming data
2. Potentially re-serialize the data in a different byte encoding (e.g. UTF-8 vs UTF-16, pretty-print vs compact JSON)
3. Fail entirely for PDF files because binary content cannot be written to a text-format sink

`BinarySink` writes any input stream (XML bytes, PDF bytes, JSON bytes) directly to ADLS without any modification. The files in Bronze are identical to the source files. Silver/Gold layers apply format-specific readers when needed.

---

**A14**
Trace for pipeline run on 2026-10-19:

1. `addDays(utcnow(), -1)` → `2026-10-18T<time>Z`
2. `formatDateTime(2026-10-18T..., 'yyyy/MM/dd')` → `2026/10/18`
3. `p_date` = `2026/10/18`
4. Dataset folder path: `@concat('invoices/', '2026/10/18')` → `invoices/2026/10/18`
5. ADF reads from container `source`, folder `invoices/2026/10/18`
6. Full path in source storage: `dataenggdailystorage/source/invoices/2026/10/18/`

All `*.pdf` files in that folder are copied to:
```
bronze/blob/invoices/ingestion_date=2026-10-18/run_id=<pipeline GUID>/
```

---

**A15**
On 2026-10-19, the October reports (`kpi_report_202610.json`, etc.) may or may not exist yet — October is not finished until 2026-10-31. Monthly reports are typically generated at month-end and may cover only weeks 1–2 of October at this point, or may not exist at all.

**Should the pipeline read current month or previous month?**

It depends on when reports are generated:
- If reports are **month-end finalised** (posted on the 1st of the following month), the pipeline should read **previous month** (`addDays(startOfMonth(utcnow()), -1)` to get the last day of the previous month, then `formatDateTime(..., 'yyyy/MM')`).
- If reports are **rolling/updated daily** within the current month, reading current month (`formatDateTime(utcnow(), 'yyyy/MM')`) is correct.

From the screenshots: `reports/2026/06/` exists with June files — suggesting month-end publishing. For production: confirm with the vendor. The pipeline JSON uses current month (`formatDateTime(utcnow(), 'yyyy/MM')`) as a default; adjust the expression if the vendor uses previous-month pattern.

---

## Pipeline Activities and Date Expressions

**A16**
```
@formatDateTime(addDays(utcnow(), -1), 'yyyy-MM-dd')
```

- `utcnow()` → current UTC timestamp (e.g. `2026-10-19T02:00:00.0000000Z`)
- `addDays(..., -1)` → subtract one day → `2026-10-18T02:00:00.0000000Z`
- `formatDateTime(..., 'yyyy-MM-dd')` → extract date portion → `2026-10-18`

Alternative using `convertTimeZone` if you want AEDT "yesterday":
```
@formatDateTime(addDays(convertTimeZone(utcnow(), 'UTC', 'AUS Eastern Standard Time'), -1), 'yyyy-MM-dd')
```
This matters if "yesterday" should be defined in Sydney local time rather than UTC.

---

**A17**
**Benefit:** All three copy activities run concurrently. Wall-clock time = slowest single activity, not sum of all three. If config takes 30s, invoices takes 2 min, reports takes 20s — total pipeline time is ~2 min, not ~3 min. Network I/O to the source blob and to ADLS is parallelised.

**Risk:** If two activities both fail, the pipeline has two failure nodes to investigate. More importantly, ADF's Integration Runtime (IR) connection limit is shared — if you have hundreds of concurrent pipelines, parallel activities inside a single pipeline consume more IR connections simultaneously. For three lightweight copy activities this is not a concern; at scale (20+ parallel activities), tune `batchCount` or switch to sequential.

**No dependency risk here:** The three copy activities read from different source folders and write to different sink folders. There is no shared state between them. Parallel execution is correct and safe.

---

**A18**
When the source folder exists but is empty (no files matching `*.pdf`), ADF's Copy Activity behavior depends on `enableSkipIncompatibleRow` and source settings:

**Default behavior:** The Copy Activity succeeds with `filesRead: 0`, `rowsCopied: 0`, `dataWritten: 0 bytes`. No error is thrown. The activity shows as Succeeded (green) in Monitor.

**Sink behavior:** Because no files were read, no files are written to Bronze. The sink folder path is NOT created (ADLS only creates the folder when at least one blob is written).

**Pipeline result:** The overall pipeline continues and succeeds. The other two copy activities (config and reports) are unaffected since they have no `dependsOn` on `act_copy_invoices`.

**What to do:** If 0-file runs should be explicitly flagged, add an If Condition activity after `act_copy_invoices` that checks `@equals(activity('act_copy_invoices').output.filesRead, 0)` and routes to a warning Web Activity (Teams notification or Azure Monitor alert).

---

**A19**
Config files are acceptable for full load because:

1. **Volume is tiny:** 19 XML files, each a few KB. A full copy completes in seconds and costs near-zero in data transfer.
2. **Files are reference data — content is what matters, not the delta.** If `states.xml` is updated to add a new state/territory, a full load picks up the change. Incremental load would require tracking file modification timestamps — complex for blob storage compared to a database `updated_at` column.
3. **No watermark concept for files:** Files do not expose a reliable "changed since last run" API. You would need to compare file modification dates — which requires a previous state snapshot — far more complex than just copying all 19 files.

**Why NOT for invoice/session API sources:**
- Invoice PDFs can be thousands of files per day × 365 days = hundreds of thousands of files. Full load daily would re-copy all historical files every run — expensive and slow.
- Session API has millions of rows behind it. Full load means reading the entire database every day.
- These sources have explicit change-tracking via `updated_after` watermark or date-folder partitioning, making incremental load feasible.

---

**A20**
Silver reads all runs for a given date using a wildcard across the `run_id=*` level:

```python
# Read all invoice files for 2026-10-18 across all runs
df = spark.read.format("binaryFile").load(
    "abfss://bronze@evdatalakedev2.dfs.core.windows.net/blob/invoices/ingestion_date=2026-10-18/run_id=*/"
)
```

Or using `recursiveFileLookup` with a base path and filter:
```python
df = spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(
    "abfss://bronze@evdatalakedev2.dfs.core.windows.net/blob/invoices/"
)
df = df.filter(col("path").contains("ingestion_date=2026-10-18"))
```

**Wildcard path:** `blob/invoices/ingestion_date=2026-10-18/run_id=*/`

For invoice deduplication in Silver: deduplicate on the invoice file name (extracted from the `path` column). If the same `INV-AU-2026-0002266.pdf` appears in two `run_id=` subfolders, keep only one copy using `dropDuplicates(["file_name"])`.

---

## Triggers and Scheduling

**A21**
02:00 UTC = **13:00 AEDT (UTC+11)** in October (AEDT is active during Australian summer, which starts in late October, but Sydney observes AEDT — 11 hours ahead of UTC during daylight saving).

Wait — October 19 is before daylight saving starts in NSW (last Sunday of October). In mid-October, Sydney is on AEST (UTC+10). So 02:00 UTC = **12:00 noon AEST**.

**Is it safe?** Yes. By noon Sydney time, the previous day (2026-10-18) has been over for 12 hours. Invoice generation systems run overnight or at business close. Any invoice generated on 2026-10-18 would have been written to the `invoices/2026/10/18/` folder by end of day 2026-10-18 AU time — well before the 02:00 UTC pipeline runs on 2026-10-19.

02:00 UTC daily is a standard pattern for "load yesterday's data" pipelines targeting Australian business data.

---

**A22**
**Key difference in failure behaviour:**

| Behaviour | Schedule Trigger | Tumbling Window Trigger |
|---|---|---|
| Run fails | Next window fires regardless — failed window is gone | Failed window stays in "failed" state, retries per policy |
| System down for 3 days | 3 missed fires are skipped — no backfill | 3 missed windows queue up, run in sequence on recovery |
| Pipeline re-run of a missed window | Manual only | Automatic on recovery |

**When to switch to Tumbling Window for this pipeline:**
- If missing a day's invoice load causes a compliance gap (e.g. invoices for reporting must be ingested for every calendar day with no gaps)
- If the vendor's SAS token expires and you need assurance that all affected days retry automatically after renewal
- If downstream Silver/Gold processing depends on a complete sequence of daily Bronze runs

For a "best effort" daily blob copy where gaps are tolerable, Schedule trigger is fine. For regulatory/compliance data pipelines, Tumbling Window is the correct choice.

---

**A23**
The Schedule trigger fires independently each day — it has no memory of previous days' results.

**What happens to 2026-10-18 invoice data:**
On 2026-10-20 the trigger fires for that day's run. It reads `invoices/2026/10/19/` (yesterday = Oct 19) — it does NOT retry Oct 18. The Oct 18 invoice data (`invoices/2026/10/18/`) is **never automatically recovered** with a Schedule trigger.

**Recovery options:**
1. **Manual re-run:** In ADF Monitor → find the failed 2026-10-19 run → **Rerun**. This re-triggers the pipeline. Since `act_copy_invoices` reads `addDays(utcnow(), -1)` — which is NOW Oct 18 relative to the rerun time (still Oct 19) — it will pick up Oct 18 data. But if re-run on Oct 20, `addDays(utcnow(), -1)` = Oct 19, not Oct 18. **The Oct 18 data is lost** without manual intervention.
2. **Parameterise the date:** Add a pipeline parameter `p_override_date` — if supplied, use it; otherwise use `addDays(utcnow(), -1)`. Then a manual run with `p_override_date = 2026/10/18` recovers the correct folder.
3. **Switch to Tumbling Window:** The trigger retries the failed window automatically with the correct window date.

---

**A24**
Bronze = raw landing zone, files copied byte-for-byte. The justification:

1. **Replayability:** If Silver or Gold transformations have a bug, you can re-process Bronze without re-ingesting from the source. The source may not retain historical files (vendor may clean up old SAS files). Bronze is your permanent copy.

2. **No loss of information:** Parsing at ingest time (e.g. converting XML to Parquet in Bronze) throws away attributes, namespaces, or fields that the current Silver schema doesn't capture. A future requirement might need a field you discarded. Raw files preserve everything.

3. **Auditability:** Regulators may require the original invoice PDF — not a Silver transformation of it. Bronze holds the court-admissible original.

4. **Decoupling:** The Bronze pipeline is generic — it doesn't know or care about the schema. Schema evolution (vendor adds a new XML element) requires no changes to the Bronze pipeline, only to Silver transformations.

**Silver handles:** Schema enforcement, format parsing (XML → structured columns, JSON → Delta table), deduplication, data quality checks. **Gold handles:** Business aggregations, KPIs.

---

**A25**
**Strategy for resilience to vendor-side changes:**

**1. Renamed folders (e.g. `config/` → `configuration/`):**
- Extract folder paths into a configuration table (in Unity Catalog or a Key Vault secret), not hardcoded in datasets.
- Create a control table row per blob source type with `source_folder`, `file_pattern`, `sink_prefix` columns.
- A Databricks notebook reads the control table and constructs the ADF dataset parameter at pipeline runtime — same pattern as Day 6's `ev_catalog.ctl.pipeline_control`.
- When the vendor renames a folder: one SQL UPDATE, no ADF changes, no pipeline redeployment.

**2. New file formats (e.g. vendor adds `.csv` alongside `.xml`):**
- Use `BinarySource` for all source datasets instead of format-specific sources (`XmlSource`, `JsonSource`). Binary copies files without caring about format — new formats land in Bronze unchanged.
- Silver notebooks apply format-specific readers and fail fast with a clear error if a new format appears — catch it at transformation time, not ingest time.
- Alternative: configure wildcard as `*` (all files) instead of `*.xml` — pick up every file regardless of extension.

**3. SAS token changes without warning:**
- Store the SAS URI in **Azure Key Vault** as a secret (`ls-blob-source-sas-uri`).
- Configure `ls_blob_source` to use **Key Vault reference** for the SAS URI rather than embedding it directly in the linked service JSON.
- When the vendor issues a new token: update the Key Vault secret only — no ADF linked service edit, no Publish All required.
- Add an Azure Monitor alert on secret expiry (30-day warning) so you can proactively request renewal before the pipeline fails.

---
