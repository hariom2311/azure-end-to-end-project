# Day 7 — Pipeline Idempotency: Run-ID Partitioned Sink

**Duration:** 1 hour | **Goal:** Fix the double-run overwrite bug in `pl_bronze_incremental_load_v2` by isolating each pipeline run into its own subfolder using `pipeline().RunId`. Produces `pl_bronze_incremental_load_v3`.

> **Prerequisites:** Day 5 complete — `pl_bronze_incremental_load_v2` exists and is working.

---

## The Problem

When the same source runs **twice on the same day**, Run 2 overwrites Run 1's Bronze files.

**Scenario:**

```
Run 1 — 09:00, watermark = 1900-01-01 (first full load)
  → API returns 500 pages
  → Writes: api/payments/ingestion_date=2026-10-18/payments_page_1.json
             api/payments/ingestion_date=2026-10-18/payments_page_2.json
             ...
             api/payments/ingestion_date=2026-10-18/payments_page_500.json
  → Watermark updated to: 2026-10-18T09:00:00Z

Run 2 — 14:00, watermark = 2026-10-18T09:00:00Z (incremental)
  → API returns only 50 new pages (records created between 09:00 and 14:00)
  → Writes: api/payments/ingestion_date=2026-10-18/payments_page_1.json  ← OVERWRITES Run 1
             api/payments/ingestion_date=2026-10-18/payments_page_2.json  ← OVERWRITES Run 1
             ...
             api/payments/ingestion_date=2026-10-18/payments_page_50.json ← OVERWRITES Run 1
  → pages 51–500 from Run 1 are now ORPHANED (wrong data for those page numbers)
```

**Result:** Bronze layer has corrupted data — page 1 contains only 14:00 records, but pages 51–500 contain 09:00 records. Silver transformation will merge these incorrectly.

**Zero-page case (same bug, worse outcome):**

```
Run 2 — watermark is current, API returns 0 new pages
  → total_pages = 0
  → Copy data1 runs with 0 rows → writes an EMPTY payments_page_1.json
  → Run 1's data for page 1 is gone
```

---

## The Fix — `pipeline().RunId` Subfolder

Every ADF pipeline run has a unique GUID — `pipeline().RunId`. Including it in the sink folder path means each run writes to its own isolated subfolder. **No run can ever overwrite another run's files.**

**New folder structure:**

```
api/payments/ingestion_date=2026-10-18/run_id=abc12345-.../payments_page_1.json
api/payments/ingestion_date=2026-10-18/run_id=abc12345-.../payments_page_500.json

api/payments/ingestion_date=2026-10-18/run_id=def67890-.../payments_page_1.json
api/payments/ingestion_date=2026-10-18/run_id=def67890-.../payments_page_50.json
```

Run 1 and Run 2 are completely isolated. Silver reads all `run_id=*` subfolders and deduplicates by primary key — correct data is always preserved.

**Only two expression changes needed — no new variables, no new activities.**

---

## What Changes vs Day 5 (`pl_bronze_incremental_load_v2`)

```
Activity               Change
─────────────────────  ──────────────────────────────────────────────────────
Copy data1             p_folder expression: add /run_id=@{pipeline().RunId}
act_copy_page          p_folder expression: add /run_id=@{pipeline().RunId}
Everything else        UNCHANGED
```

---

## Part 1 — Create `pl_bronze_incremental_load_v3`

### Option A — Import JSON (Recommended)

ADF Studio → Author → Pipelines → **⋮ (three dots)** → **Import from pipeline JSON**

Import file: `pipelines/pl_bronze_incremental_load_v3.json`

Publish all.

---

### Option B — Clone from v2 and Edit Manually

**1. Clone the pipeline**

ADF Studio → Author → Pipelines → `pl_bronze_incremental_load_v2` → **⋮** → **Clone**

Rename the clone to `pl_bronze_incremental_load_v3`.

---

**2. Edit `Copy data1` sink — p_folder**

Click `Copy data1` → **Sink tab** → scroll to dataset parameters → `p_folder` → **Open dynamic content editor**

**Old value:**
```
@concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'))
```

**New value:**
```
@concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'), '/run_id=', pipeline().RunId)
```

---

**3. Edit `act_copy_page` sink — p_folder (inside the Until loop)**

Click the Until loop → **pencil icon** to open inner canvas → click `act_copy_page` → **Sink tab** → `p_folder` → **Open dynamic content editor**

**Old value:**
```
@concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'))
```

**New value:**
```
@concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'), '/run_id=', pipeline().RunId)
```

Back arrow → **Publish all**.

---

## Part 2 — Why `pipeline().RunId`

| Property | Value |
|---|---|
| What it is | A UUID assigned by ADF when a pipeline run starts |
| Format | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` (standard GUID) |
| Unique per | Each individual pipeline run — re-run of the same trigger = new RunId |
| Safe in ADLS paths | Yes — GUIDs only contain letters, digits, and hyphens |
| No new variable needed | `pipeline().RunId` is a built-in ADF system variable, available anywhere in any expression |
| Cost | Zero — no extra activities, no extra cluster time |

**Why not use `utcnow()` or `v_run_ts`?**

`utcnow()` returns a timestamp with colons (`2026-10-18T09:00:00.0000000Z`) — ADLS Gen2 rejects colons in folder names. You would need a `replace()` call to strip them. `pipeline().RunId` is already path-safe with no transformation needed.

**Why not use `v_ingestion_date` alone?**

`v_ingestion_date` is `YYYY-MM-DD` — same value for every run on the same day. That is exactly the bug. RunId adds a dimension that is unique per run, not per day.

---

## Part 3 — Silver Layer Impact

Silver notebooks read Bronze with a wildcard path. Update the Bronze read path in Silver to include the `run_id=*` wildcard:

**Old Silver read path:**
```python
df = spark.read.json("abfss://bronze@evdatalakedev2.dfs.core.windows.net/api/payments/ingestion_date=2026-10-18/")
```

**New Silver read path (reads all runs for a given date):**
```python
df = spark.read.json("abfss://bronze@evdatalakedev2.dfs.core.windows.net/api/payments/ingestion_date=*/run_id=*/")
```

Or partition-filtered for a specific date:
```python
df = spark.read.json(
    "abfss://bronze@evdatalakedev2.dfs.core.windows.net/api/payments/",
    recursiveFileLookup=True
)
# Filter by partition column after read
df = df.filter(col("ingestion_date") == "2026-10-18")
```

**Deduplication in Silver:** Since multiple runs for the same day may have overlapping records (Run 1 full load + Run 2 incremental both contain records from before 09:00 for full sources), Silver must deduplicate by primary key before writing. Use `dropDuplicates(["id"])` or a `ROW_NUMBER()` window with `ORDER BY updated_at DESC` to keep the latest version of each record.

> **This is the correct medallion architecture pattern:** Bronze = raw, append-only, never delete. Silver = cleansed, deduplicated. The fix lives in Bronze (isolation) + Silver (dedup), not in trying to make Bronze perfectly non-overlapping.

---

## Part 4 — Updated Day 6 Master Pipeline

The master pipeline `pl_bronze_master` (Day 6) calls `pl_bronze_incremental_load_v2` for each source. To use the idempotent version, update each `ExecutePipeline` activity's invoked pipeline reference from `pl_bronze_incremental_load_v2` to `pl_bronze_incremental_load_v3`.

> Each child pipeline run gets its own `pipeline().RunId` — the master's RunId is different from each child's RunId. So even in parallel execution, every child run has a unique path.

---

## New Sink Path Reference

| Expression part | Example value |
|---|---|
| `variables('v_sink_base_path')` | `api/payments` |
| `variables('v_ingestion_date')` | `2026-10-18` |
| `pipeline().RunId` | `abc12345-1234-1234-1234-abcdef123456` |
| Full folder (page 1) | `api/payments/ingestion_date=2026-10-18/run_id=abc12345-.../` |
| Full file (page 1) | `payments_page_1.json` |
| Full file (loop page 42) | `payments_page_42.json` |

---

## Day 7 Checklist

- [ ] `pl_bronze_incremental_load_v3` created (clone of v2 or JSON import)
- [ ] `Copy data1` sink `p_folder` includes `/run_id=@{pipeline().RunId}`
- [ ] `act_copy_page` sink `p_folder` includes `/run_id=@{pipeline().RunId}` (inside Until loop)
- [ ] Test run: trigger with `p_source_name=payments`, `p_load_type_override=full`
- [ ] Verify ADLS: `api/payments/ingestion_date=<today>/run_id=<guid>/payments_page_*.json` all present
- [ ] Trigger a second run immediately after — verify a NEW `run_id=<different-guid>` subfolder is created
- [ ] Verify first run's files are untouched
- [ ] Update `pl_bronze_master` (Day 6) — change all 19 ExecutePipeline references from `v2` to `v3`
- [ ] Silver notebooks updated to read with `run_id=*` wildcard and deduplicate by primary key

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| Files still being overwritten | Edited only one of the two Copy activities — forgot `act_copy_page` inside the Until loop | Open Until → pencil → check `act_copy_page` sink p_folder also has RunId |
| `pipeline().RunId` shows as literal string | Expression not toggled — entered as static text | Click the field → toggle to **Dynamic content** → re-enter the expression |
| Silver reads 0 rows after fix | Silver path still hardcoded without `run_id=*` wildcard | Update Silver Bronze read path to use `recursiveFileLookup=True` or `run_id=*/` wildcard |
| Duplicate records in Silver | Two runs both contain the same records (full load overlap) | Add `dropDuplicates(["id"])` or window dedup in Silver notebook |

---

## Part 5 — Blob Storage Source Ingestion

### 5.1 — Source Overview

The EV hardware vendor exposes raw operational files in an external Azure Blob Storage account (`dataenggdailystorage`, container `source`). Three source types land there:

| Source | Path pattern | Format | Load strategy |
|---|---|---|---|
| Config / reference XML | `source/config/*.xml` | XML | Full load every run |
| Invoice PDFs | `source/invoices/YYYY/MM/DD/*.pdf` | Binary (PDF) | Full load for **yesterday's** date folder |
| Monthly reports | `source/reports/YYYY/MM/*.json` | JSON | Full load every run |

This storage account is external — you do not own it and cannot use Managed Identity. Access is via a vendor-provided read-only SAS token (`sp=rl`).

---

### 5.2 — Linked Service: `ls_blob_source`

**Why a separate linked service:**
`ls_adls_bronze` connects to your own ADLS Gen2 account using Managed Identity. That identity has no access to the vendor's storage account. SAS authentication is the standard pattern for cross-tenant, read-only access to an external Blob Storage container.

**Connection details:**

| Property | Value |
|---|---|
| Storage account | `dataenggdailystorage` |
| Container | `source` |
| Blob endpoint | `https://dataenggdailystorage.blob.core.windows.net` |
| SAS token | `sp=rl&st=2026-10-19T14:19:08Z&se=2027-10-19T22:34:08Z&spr=https&sv=2026-02-06&sr=c&sig=htxUXkd2knSowSujglmMYRoGWHQkCbXG2fh7bmi91No%3D` |
| Full SAS URL | `https://dataenggdailystorage.blob.core.windows.net/source?sp=rl&st=2026-10-19T14:19:08Z&se=2027-10-19T22:34:08Z&spr=https&sv=2026-02-06&sr=c&sig=htxUXkd2knSowSujglmMYRoGWHQkCbXG2fh7bmi91No%3D` |
| Expires | `2027-10-19T22:34:08Z` |
| Permissions | Read + List only (`sp=rl`) |

**ADF Studio → Manage → Linked services → + New → Azure Blob Storage**

| Field | Value |
|---|---|
| Name | `ls_blob_source` |
| Authentication method | SAS URI |
| SAS URL | *(paste Full SAS URL from table above)* |

Test connection → green → Create → Publish all.

> **Note on the SAS URL:** The URL above uses `sr=c` (container-level), which allows listing and reading all blobs in `source/`. A blob-level SAS (`sr=b`) would only grant access to one specific file — not usable for folder reads.

---

### 5.3 — Source Datasets

**`ds_blob_config_src`** — XML, no parameters (always reads full `config/` folder)

- Linked service: `ls_blob_source`
- Type: XML
- Container: `source`
- Folder: `config`
- No parameters needed — folder never changes

**`ds_blob_invoices_src`** — Binary, parameterised date

- Linked service: `ls_blob_source`
- Type: Binary
- Container: `source`
- Parameter: `p_date` (String) — format `YYYY/MM/DD`
- Folder path (dynamic): `@concat('invoices/', dataset().p_date)`

**`ds_blob_reports_src`** — JSON, parameterised year/month

- Linked service: `ls_blob_source`
- Type: JSON
- Container: `source`
- Parameter: `p_year_month` (String) — format `YYYY/MM`
- Folder path (dynamic): `@concat('reports/', dataset().p_year_month)`

---

### 5.4 — Sink Dataset

**`ds_bronze_blob_sink`** — Binary sink on ADLS Gen2

- Linked service: `ls_adls_bronze` (your existing ADLS linked service)
- Type: Binary
- File system: `bronze`
- Parameter: `p_folder` (String) — full path set by each copy activity
- Folder path (dynamic): `@dataset().p_folder`

**Why Binary sink?** Bronze is a raw landing zone — files must land byte-for-byte identical to the source. A format-specific sink would attempt to parse and re-serialise the content. Binary sink passes through any format (XML, PDF, JSON) without modification.

---

### 5.5 — Pipeline: `pl_bronze_blob_ingestion`

Three Copy activities run **in parallel** (no `dependsOn` between them):

```
[act_copy_config] ──┐
[act_copy_invoices]──┤  (all start simultaneously)
[act_copy_reports] ──┘
```

**`act_copy_config`**

| Field | Value |
|---|---|
| Source | `ds_blob_config_src` |
| Source file filter | `*.xml` |
| Sink | `ds_bronze_blob_sink` |
| Sink `p_folder` | `@concat('blob/config/ingestion_date=', formatDateTime(utcnow(), 'yyyy-MM-dd'), '/run_id=', pipeline().RunId)` |
| Retry | 2 retries, 60s interval |

**`act_copy_invoices`**

| Field | Value |
|---|---|
| Source | `ds_blob_invoices_src` |
| Source `p_date` | `@formatDateTime(addDays(utcnow(), -1), 'yyyy/MM/dd')` |
| Source file filter | `*.pdf` |
| Sink | `ds_bronze_blob_sink` |
| Sink `p_folder` | `@concat('blob/invoices/ingestion_date=', formatDateTime(addDays(utcnow(), -1), 'yyyy-MM-dd'), '/run_id=', pipeline().RunId)` |
| Retry | 2 retries, 60s interval |

**`act_copy_reports`**

| Field | Value |
|---|---|
| Source | `ds_blob_reports_src` |
| Source `p_year_month` | `@formatDateTime(utcnow(), 'yyyy/MM')` |
| Source file filter | `*.json` |
| Sink | `ds_bronze_blob_sink` |
| Sink `p_folder` | `@concat('blob/reports/ingestion_date=', formatDateTime(utcnow(), 'yyyy-MM-dd'), '/run_id=', pipeline().RunId)` |
| Retry | 2 retries, 60s interval |

**Why yesterday for invoices?** Invoices are generated throughout the business day and finalised at day close. Running the pipeline at 02:00 UTC (noon AEST) means the previous calendar day is fully closed in Australian time — yesterday's invoice folder is complete. Running for today's folder at 02:00 AEST risks a partial set.

**Why current month for reports?** Monthly report files (`kpi_report_YYYYMM.json`) are updated intra-month. A full overwrite each day picks up the latest version. If reports are finalised only at month-end, change to previous month: `@formatDateTime(addDays(startOfMonth(utcnow()), -1), 'yyyy/MM')`.

---

### 5.6 — Bronze Folder Structure After Pipeline Run

```
bronze/
├── blob/
│   ├── config/
│   │   └── ingestion_date=2026-10-19/
│   │       └── run_id=abc12345-.../
│   │           ├── connector_standards.xml
│   │           ├── connector_types.xml
│   │           ├── firmware_manifest_v7_4_2.xml
│   │           ├── network_topology.xml
│   │           ├── operator_registry.xml
│   │           ├── states.xml
│   │           └── ... (19 XML files total)
│   ├── invoices/
│   │   └── ingestion_date=2026-10-18/
│   │       └── run_id=abc12345-.../
│   │           ├── INV-AU-2026-0002266.pdf
│   │           ├── INV-AU-2026-0002267.pdf
│   │           └── ... (all PDFs from that day)
│   └── reports/
│       └── ingestion_date=2026-10-19/
│           └── run_id=abc12345-.../
│               ├── kpi_report_202610.json
│               ├── sla_report_202610.json
│               └── state_breakdown_202610.json
```

---

### 5.7 — Trigger: `tr_blob_ingestion_daily`

| Field | Value |
|---|---|
| Type | Schedule trigger |
| Frequency | Daily |
| Time | 02:00 UTC |
| Start | `2026-10-20T02:00:00Z` |
| Pipeline | `pl_bronze_blob_ingestion` |

**ADF Studio → Manage → Triggers → + New:**
- Type: Schedule
- Start time: `2026-10-20T02:00:00Z`
- Recurrence: Every 1 Day at 02:00
- Timezone: UTC
- Associate pipeline: `pl_bronze_blob_ingestion`

Activate trigger → Publish all.

---

### 5.8 — SAS Token Management

The SAS token in `ls_blob_source` expires on `2027-10-19T22:34:08Z`. When it expires, every pipeline run fails with `403 AuthenticationFailed`.

**Renewal process:**
1. Request new SAS token from vendor (or generate from Azure Portal if you have access to `dataenggdailystorage`)
2. ADF Studio → Manage → Linked services → `ls_blob_source` → Edit
3. Replace SAS URL → Test connection → Save → Publish all

**Best practice — Key Vault reference:**
Store the SAS URI as an Azure Key Vault secret (`ls-blob-source-sas-uri`) and reference it in the linked service. Renewal requires only updating the secret — no ADF edits, no Publish All required. See `practice-exercises.md` Bonus exercise for setup steps.

---

## Updated Day 7 Checklist

**Part 1 — Idempotency fix (`pl_bronze_incremental_load_v3`):**
- [ ] `pl_bronze_incremental_load_v3` created (JSON import or manual clone)
- [ ] `Copy data1` sink `p_folder` includes `/run_id=@{pipeline().RunId}`
- [ ] `act_copy_page` sink `p_folder` includes `/run_id=@{pipeline().RunId}` (inside Until loop)
- [ ] Test run: payments full load → ADLS path contains `run_id=<guid>`
- [ ] Second run produces a different `run_id=` subfolder — first run untouched
- [ ] `pl_bronze_master` (Day 6) updated to reference v3

**Part 5 — Blob ingestion pipeline:**
- [ ] `ls_blob_source` linked service created (SAS auth), Test connection green
- [ ] `ds_blob_config_src` dataset created — XML, `config/` folder
- [ ] `ds_blob_invoices_src` dataset created — Binary, `invoices/` + `p_date` parameter
- [ ] `ds_blob_reports_src` dataset created — JSON, `reports/` + `p_year_month` parameter
- [ ] `ds_bronze_blob_sink` dataset created — Binary, ADLS Gen2, `p_folder` parameter
- [ ] `pl_bronze_blob_ingestion` pipeline created — three parallel Copy activities
- [ ] Test run: all three activities green, ADLS shows files under `blob/config/`, `blob/invoices/`, `blob/reports/`
- [ ] Second run: new `run_id=` subfolders, first run untouched
- [ ] `tr_blob_ingestion_daily` trigger created, activated, set to 02:00 UTC
- [ ] (Bonus) SAS URI moved to Key Vault

---

## Blob Ingestion Common Errors

| Error | Cause | Fix |
|---|---|---|
| `403 AuthenticationFailed: Signed expiry time is already past` | SAS token expired | Generate new SAS, update `ls_blob_source` SAS URL, Test connection, Publish |
| `403 AuthorizationPermissionMismatch` on `ls_blob_source` | Wrong auth method — using MI instead of SAS | Delete and recreate linked service with Authentication method = SAS URI |
| `The specified container does not exist` | SAS URL uses wrong container name | Verify SAS URL contains the correct container path |
| Source `0 files read` but no error | Source folder exists but is empty (e.g. no invoices for that date) | Expected — ADF Copy succeeds with 0 files. Add If Condition check if you need explicit alerts |
| PDF files landing as corrupt/empty in Bronze | Sink dataset typed as non-Binary (e.g. Json) | Change `ds_bronze_blob_sink` type to Binary |
| `dataset().p_date` resolves to empty string | Dataset used directly without passing the parameter from copy activity | In copy activity → Source tab → dataset parameters → pass the expression for `p_date` |
| XML files fail to copy with parse error | Source dataset typed as Xml but sink typed as Json or Parquet | Sink must be Binary for raw file copy — do not use format-specific sink |
