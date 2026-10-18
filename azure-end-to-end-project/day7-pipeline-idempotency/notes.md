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
