# Day 8 — Silver Layer: API Data Transformation
**Source:** Bronze Volume JSON (`/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/api/`)
**Sink:** Silver Volume Delta (`/Volumes/dbw_ev_intelligence_dev/default/silver-volume/api/`)

## Three Notebooks — Learning Progression

| Notebook | Scope | Purpose |
|---|---|---|
| `01_silver_payments_simple_v1.ipynb` | Payments only | Every step written out explicitly — no functions, no loops. Learn what each transformation does. |
| `02_silver_all_entities_forloop_v2.ipynb` | All 17 entities | Same logic as v1, wrapped in a `for` loop over an entity config list. Full overwrite only. |
| `03_silver_all_entities_optimised_v3.ipynb` | All 17 entities | Production version — widget parameters, helper functions, full + incremental load, Delta MERGE upsert, run summary. |

**Teach in order: v1 -> v2 -> v3.**
- v1: understand the transformations one step at a time
- v2: see how to scale to many entities with a loop
- v3: see how to make it production-ready with load modes, error handling, and Delta merge

---

## Where Does This Notebook Fit in ADF?

Silver transformation runs **after** Bronze ingestion completes. Two options:

### Option A — Separate Silver Pipeline (Recommended)

```
ADF Pipeline 1: pl_bronze_api_ingest_v4
  └── runs every 2 hours
  └── calls Databricks notebook: 01_bronze_api_ingest_databricks
  └── writes JSON to Bronze Volume

ADF Pipeline 2: pl_silver_api_transformation  <-- NEW
  └── triggered by Pipeline 1 completing (Execute Pipeline activity)
  └── calls Databricks notebook: 03_silver_all_entities_optimised_v3
  └── reads Bronze JSON, writes Silver Delta
```

**How to chain them in ADF:**
1. Open `pl_bronze_api_ingest_v4` in ADF
2. Add an **Execute Pipeline** activity after the last Bronze activity
3. Set **Invoked pipeline:** `pl_silver_api_transformation`
4. Set **Wait on completion:** `true`
5. The Silver pipeline only runs if Bronze succeeds

**Why separate pipelines:**
- Bronze and Silver can fail independently — easier to rerun just Silver if it fails
- Silver can be triggered on its own for a backfill without re-running Bronze
- Cleaner monitoring — each pipeline has its own run history

### Option B — Same Pipeline, Second Notebook Activity

```
ADF Pipeline: pl_bronze_api_ingest_v4
  └── act_bronze_ingest  (Databricks Notebook — Bronze)
        └── on success
              └── act_silver_transform  (Databricks Notebook — Silver)
```

**How to add it:**
1. Open your existing pipeline in ADF
2. Drag a second **Notebook** activity onto the canvas
3. Connect with a **Success** dependency arrow from the Bronze activity
4. Configure: same Linked Service, path = `/Shared/silver_transformation/03_silver_all_entities_optimised_v3`
5. Base parameter: `load_type = incremental`

**Why you might choose this:**
- Simpler — one pipeline to trigger and monitor
- Fewer ADF objects to manage

**Recommended: Option A** — separate pipelines give better control and rerunability.

---

---

## What v3 Does (Production Notebook)

Reads all 17 entities from Bronze, applies PySpark transformations, and writes clean Delta tables to Silver.

| Step | What happens |
|---|---|
| 1 | Discover Bronze JSON files for the entity (all dates or specific date) |
| 2 | Read raw JSON with `spark.read.json()` (handles multi-line, multi-page files) |
| 3 | Explode the `data` array (strip API pagination wrapper) |
| 4 | Cast every column to correct type (timestamps, decimals, integers, dates) |
| 5 | Add Silver audit columns: `silver_ingested_at`, `silver_load_type`, `silver_pipeline` |
| 6 | Deduplicate on natural key — keep latest `updated_at` per key |
| 7 | Write to Silver as Delta — overwrite (full) or merge/upsert (incremental) |
| 8 | Print run summary with bronze rows vs silver rows per entity |
| 9 | Spot-check 3 entities: row count, schema, sample rows |

---

## Bronze -> Silver Data Flow

```
Bronze Volume
  /api/payments/ingestion_date=2026-07-13/page_1.json
  /api/payments/ingestion_date=2026-07-13/page_2.json
  ...
  /api/sessions/ingestion_date=2026-07-13/page_1.json
  ...

      PySpark reads JSON -> explode data[] -> cast types -> deduplicate -> Delta merge

Silver Volume
  /api/payments/    (Delta table — partitioned by natural key)
  /api/sessions/    (Delta table)
  ...
  /api/weather/     (Delta table)
```

---

## Silver Volume Path

```
/Volumes/dbw_ev_intelligence_dev/default/silver-volume/api/<entity>/
```

Uses the same `default` schema as Bronze — `silver-volume` is a separate Volume under the same catalog/schema as `bronze-volume`. No new schema creation needed.

---

## Prerequisites

| Requirement | Where set up |
|---|---|
| Bronze Volume populated with API JSON | Day 7 — `01_bronze_api_ingest_databricks.ipynb` must have run at least once |
| Unity Catalog Silver Volume | Create if not exists (see Part A below) |
| `dev-cluster` running Databricks Runtime 13.x+ | Required for Delta Lake + PySpark |
| `delta` library | Pre-installed on Databricks Runtime — no pip install needed |

---

## Part A — Create the Silver Volume (One-Time Setup)

Silver uses the same `default` schema as Bronze. You only need to create one new Volume.

1. Databricks → left sidebar → **Catalog** (catalog icon)
2. Expand `dbw_ev_intelligence_dev` catalog → click **default** schema
3. Click **+** → **Create volume**
   - **Volume name:** `silver-volume`
   - **Volume type:** `External`
   - **External location:** `evdatalakedev-silver` (already set up in Day 2)
   - **Path:** leave blank (root of the silver container)
4. Click **Create**

Or via SQL in any notebook:
```sql
CREATE EXTERNAL VOLUME IF NOT EXISTS dbw_ev_intelligence_dev.default.`silver-volume`
  LOCATION 'abfss://silver@evdatalakedev.dfs.core.windows.net/';
```

Verify the path resolves:
```python
dbutils.fs.ls("/Volumes/dbw_ev_intelligence_dev/default/silver-volume/")
```

---

## Part B — Upload the Notebook

1. Databricks → left sidebar → **Workspace** → **Shared** → `bronze_ingestion` (or create `silver_transformation` folder)
2. **⋮** → **Import** → select `01_silver_api_transformation.ipynb`
3. Confirm path:
   ```
   /Shared/silver_transformation/01_silver_api_transformation
   ```

---

## Part C — Run a Full Load (First Time)

Full load reads ALL Bronze JSON files across ALL ingestion dates and overwrites Silver.

### Step 1 — Open the notebook

Databricks → Workspace → `/Shared/silver_transformation/01_silver_api_transformation`

### Step 2 — Set widgets

| Widget | Value for full load |
|---|---|
| `Load Type (full / incremental)` | `full` |
| `Ingestion Date (YYYY-MM-DD, blank = latest)` | *(leave blank)* |

### Step 3 — Attach to cluster

Click **Connect** → select `dev-cluster`

### Step 4 — Run All

Click **Run all**. Expected runtime: 10–30 minutes (reading all pages across all dates for 17 entities).

### Step 5 — Verify Cell 8 output

```
SILVER API TRANSFORMATION — RUN SUMMARY
  load_type      : full
  ingestion_date : (all partitions)
  run_timestamp  : 2026-07-13T06:00:00Z
  entities total : 17
  succeeded      : 17
  skipped        : 0
  failed         : 0

  [OK]   payments                  succeeded   bronze=  4500  silver=  4500
  [OK]   sessions                  succeeded   bronze= 12000  silver= 12000
  [OK]   customers                 succeeded   bronze=  3200  silver=  3200
  ...
  [OK]   weather                   succeeded   bronze=   200  silver=   200
```

---

## Part D — Incremental Load

Incremental load reads only Bronze files for a specific `ingestion_date` and merges (upserts) into Silver Delta — no data is deleted, existing records are updated if `updated_at` is newer.

### Step 1 — Set widgets

| Widget | Value |
|---|---|
| `Load Type` | `incremental` |
| `Ingestion Date` | `2026-07-13` (today's date — match the Bronze ingestion date) |

### Step 2 — Run All

Cells run the same pipeline. Cell 7 will print:
```
  Processing: payments ...  OK  (120 bronze rows -> 120 silver rows)
  Processing: sessions ...  OK  (340 bronze rows -> 340 silver rows)
  ...
```

Silver rows = new or updated records merged into the existing Delta table. Records not present in today's Bronze slice remain unchanged in Silver.

---

## Part E — Create the Databricks Job

After verifying the notebook works manually, schedule it as a Databricks Job.

### Step 1 — Open Workflows

Databricks → left sidebar → **Workflows** → **+ Create job**

### Step 2 — Name the Job

```
job_silver_api_transformation
```

### Step 3 — Configure the Task

| Field | Value |
|---|---|
| Task name | `task_silver_api_transform` |
| Type | `Notebook` |
| Source | `Workspace` |
| Path | `/Shared/silver_transformation/01_silver_api_transformation` |
| Cluster | `dev-cluster` |

### Step 4 — Add Parameters

| Key | Value |
|---|---|
| `load_type` | `incremental` |
| `ingestion_date` | *(leave blank — reads latest available Bronze partition)* |

### Step 5 — Set Schedule

Run Silver after Bronze completes. Two options:

**Option A — Fixed delay (simple):**
| Field | Value |
|---|---|
| Trigger type | `Scheduled` |
| Cron expression | `30 */2 * * *` |
| Timezone | `UTC` |

> `30 */2 * * *` = 30 minutes after every even hour — runs 30 min after the Bronze API job (`0 */2 * * *`).

**Option B — Task dependency (recommended):**
Chain this notebook as a second task inside the Bronze pipeline job so Silver only runs after Bronze succeeds.
1. Open `job_bronze_api_ingest_databricks`
2. Click **+ Add task**
3. Set **Depends on:** `task_api_ingest_all_entities`
4. Path: `/Shared/silver_transformation/01_silver_api_transformation`
5. Parameters: `load_type=incremental`

This guarantees Silver never runs on stale data.

---

## Transformation Logic Reference

### Type Casting

Every column is explicitly cast using the schema registry in Cell 4:

| Column Type | Example | PySpark Cast |
|---|---|---|
| Timestamp | `created_at`, `updated_at`, `start_time` | `.cast("timestamp")` |
| Date | `hire_date`, `scheduled_date` | `.cast("date")` |
| Decimal | `amount`, `energy_kwh`, `price_per_kwh` | `.cast("decimal(10,2)")` |
| Integer | `year`, `duration_minutes`, `total_chargers` | `.cast("integer")` |
| Long | `population` | `.cast("long")` |
| String | IDs, names, statuses | `.cast("string")` |

Bronze JSON stores everything as strings (JSON has no native types beyond string/number/bool). Casting to typed columns enables partition pruning, aggregations, and joins in the Gold layer.

### Deduplication

Bronze can contain duplicate records across pages or across incremental runs. Silver deduplicates using a window function:

```python
window = Window.partitionBy(natural_key).orderBy(F.col("updated_at").desc())
deduped_df = (
    typed_df
    .withColumn("_row_num", F.row_number().over(window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)
```

For each `natural_key` value, only the row with the most recent `updated_at` is kept.

### Delta Merge (Incremental)

On incremental runs, Silver uses Delta Lake MERGE (upsert) — not overwrite:

```python
delta_table.alias("target")
    .merge(
        deduped_df.alias("source"),
        f"target.{natural_key} = source.{natural_key}"
    )
    .whenMatchedUpdateAll()    # update existing record if key matches
    .whenNotMatchedInsertAll() # insert new record if key is new
    .execute()
```

Records in Silver that are NOT in today's Bronze slice are left unchanged — no deletes.

### Silver Audit Columns

Every Silver row gets 3 additional columns:

| Column | Value | Purpose |
|---|---|---|
| `silver_ingested_at` | Timestamp of this notebook run | Lineage — when did this record arrive in Silver |
| `silver_load_type` | `full` or `incremental` | Lineage — how was it loaded |
| `silver_pipeline` | `pl_silver_api_transformation_v1` | Lineage — which pipeline wrote it |

---

## Silver Delta Table Reference

| Entity | Natural Key | CDC Field | Silver Path |
|---|---|---|---|
| payments | `payment_id` | `updated_at` | `…/api/payments/` |
| sessions | `session_id` | `updated_at` | `…/api/sessions/` |
| customers | `customer_id` | `updated_at` | `…/api/customers/` |
| fleet | `vehicle_id` | `updated_at` | `…/api/fleet/` |
| chargers | `charger_id` | `updated_at` | `…/api/chargers/` |
| vehicles | `vehicle_id` | `updated_at` | `…/api/vehicles/` |
| stations | `station_id` | `updated_at` | `…/api/stations/` |
| complaints | `complaint_id` | `updated_at` | `…/api/complaints/` |
| maintenance_events | `event_id` | `updated_at` | `…/api/maintenance_events/` |
| energy_prices | `price_id` | `updated_at` | `…/api/energy_prices/` |
| tariffs | `tariff_id` | `updated_at` | `…/api/tariffs/` |
| charge_cards | `card_id` | `updated_at` | `…/api/charge_cards/` |
| employees | `employee_id` | `updated_at` | `…/api/employees/` |
| partners | `partner_id` | `updated_at` | `…/api/partners/` |
| cities | `city_id` | `updated_at` | `…/api/cities/` |
| states | `state_code` | `updated_at` | `…/api/states/` |
| weather | `city_id` | `updated_at` | `…/api/weather/` |

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `Path does not exist: /Volumes/.../silver-volume` | Silver Volume not created | Follow Part A to create the UC Volume |
| `Column 'data' not found in Bronze JSON` | Bronze file has wrong structure | Check Bronze file manually: `spark.read.json("path/page_1.json").show()` |
| `No Bronze JSON files found` — entity skipped | Bronze not yet populated for this entity | Run Day 7 Bronze notebook first |
| `AnalysisException: Schema mismatch` | New columns in API response not in registry | Add missing column to `cast_map` in Cell 4 for that entity |
| `DeltaAnalysisException: ... is not a Delta table` | Silver path exists but not as Delta (e.g. stale Parquet files) | Delete the path: `dbutils.fs.rm("path", True)` and re-run full load |
| `DecimalType overflow` | API returned a value wider than the declared precision | Increase precision in `cast_map`, e.g. `decimal(12,2)` |
| Incremental load shows 0 rows | `ingestion_date` does not match any Bronze partition | Check Bronze: `dbutils.fs.ls(f"{BRONZE_API_BASE}/payments/")` |
| `mergeSchema` conflict | Column type changed between full and incremental runs | Run full load to reset Silver schema |

---

## Verify Silver Contents (Any Notebook)

```python
SILVER_VOLUME = "/Volumes/dbw_ev_intelligence_dev/default/silver-volume"

# List all Silver Delta tables
for entity in ["payments", "sessions", "customers", "weather"]:
    path = f"{SILVER_VOLUME}/api/{entity}"
    df   = spark.read.format("delta").load(path)
    print(f"{entity:<25} rows={df.count():>6}  cols={len(df.columns)}")

# Read a single entity and inspect
payments_df = spark.read.format("delta").load(f"{SILVER_VOLUME}/api/payments")
payments_df.printSchema()
payments_df.show(5, truncate=False)

# Check Delta history (audit trail)
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, f"{SILVER_VOLUME}/api/payments")
dt.history().show(5, truncate=False)
```

---

## Bronze vs Silver vs Gold — Layer Summary

| Layer | Format | Purpose | This day |
|---|---|---|---|
| Bronze | Raw JSON (files) | Exact copy of API response, no transformation | Day 7 |
| Silver | Delta tables | Typed, deduplicated, cleaned, schema-enforced | **Day 8** |
| Gold | Delta tables | Business aggregates, KPIs, analytics-ready | Day 9+ |
