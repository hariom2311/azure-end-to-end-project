# Day 10 — Dynamic Silver Pipeline with Control Table

## Overview

Day 9 built 16 individual Silver pipelines chained sequentially in the master pipeline (one
`ExecutePipeline` activity per entity, 16 activities on the canvas).

Day 10 replaces that with a **control-table-driven ForEach** approach:

| | Day 9 | Day 10 |
|---|---|---|
| Silver pipelines | 16 (one per entity) | **2** (runner + dynamic master) |
| Activities on master canvas | 18 | **3** |
| Add new entity | Create notebook + pipeline + update master + Publish | **INSERT one row** into control table |
| Disable an entity | Edit pipeline + Publish | `UPDATE is_active = false` |
| Execution order | Hardcoded by activity chain | `run_order` column in control table |

> **Prerequisites:** Day 9 complete — all 16 Silver notebooks (`nb_silver_payments` through
> `nb_silver_vehicles`) exist in Databricks workspace. This day does NOT change the notebooks.
> It only changes how ADF discovers and calls them.

---

## Part 1: Architecture

### Day 9 Silver execution (hardcoded)

```
pl_bronze_master_foreach
    └── act_foreach_source        (Bronze ForEach)
    └── act_silver_payments       → pl_silver_payments       → nb_silver_payments
    └── act_silver_charge_cards   → pl_silver_charge_cards   → nb_silver_charge_cards
    └── act_silver_chargers       → pl_silver_chargers       → nb_silver_chargers
    ... (13 more activities)
    └── act_silver_vehicles       → pl_silver_vehicles       → nb_silver_vehicles
```

**Problem:** 18 activities on the master canvas. Adding entity #17 means: write notebook → write pipeline JSON → add activity to master → Publish. An ADF developer must touch the pipeline.

### Day 10 Silver execution (dynamic)

```
pl_bronze_master_foreach
    └── act_list_sources          → nb_list_active_sources   (Bronze control table)
    └── act_foreach_source        → pl_bronze_incremental_load_v3 per source
    └── act_silver_dynamic        → pl_silver_dynamic_foreach
                                        └── act_list_silver_notebooks → nb_list_silver_notebooks
                                                                         (reads silver_control table)
                                        └── act_foreach_silver (ForEach, sequential)
                                                └── act_run_silver_runner → pl_silver_runner
                                                                             → nb_silver_<entity>
                                                                               (name from control table)
```

**Adding entity #17:** Write notebook → `INSERT` one row into `ev_catalog.ctl.silver_control`. Zero ADF changes.

### Control table driven design

```
ev_catalog.ctl.silver_control
┌────────────────────────────────────────────────────────────────────────────┐
│ entity_name │ notebook_name              │ run_order │ is_active │ ...     │
│ payments    │ nb_silver_payments         │ 1         │ true      │         │
│ sessions    │ nb_silver_sessions         │ 2         │ true      │         │
│ chargers    │ nb_silver_chargers         │ 3         │ true      │         │
│ ...         │ ...                        │ ...       │ ...       │         │
│ vehicles    │ nb_silver_vehicles         │ 16        │ true      │         │
└────────────────────────────────────────────────────────────────────────────┘
```

`nb_list_silver_notebooks` reads this table and returns an ordered JSON array of notebooks.
The ForEach iterates that array, passing each `notebook_name` to `pl_silver_runner` at runtime.

---

## Part 2: Control Table Setup

### Step 1 — Create the Silver control table

Run in a Databricks SQL cell or notebook:

```sql
CREATE TABLE IF NOT EXISTS ev_catalog.ctl.silver_control (
    entity_name     STRING  NOT NULL,
    notebook_name   STRING  NOT NULL,
    run_order       INT     NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    description     STRING,
    created_at      TIMESTAMP DEFAULT current_timestamp(),
    updated_at      TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Silver layer control table — drives pl_silver_dynamic_foreach';
```

> If `ev_catalog.ctl` schema does not exist yet (it was created in Day 6 for Bronze):
> ```sql
> CREATE SCHEMA IF NOT EXISTS ev_catalog.ctl;
> ```

### Step 2 — Insert all 16 Silver entities

```sql
INSERT INTO ev_catalog.ctl.silver_control
    (entity_name, notebook_name, run_order, is_active, description)
VALUES
    ('payments',           'nb_silver_payments',           1,  true, 'Payments — Rule A D G J'),
    ('charge_cards',       'nb_silver_charge_cards',       2,  true, 'Charge cards — Rule A F G (PCI card mask)'),
    ('chargers',           'nb_silver_chargers',           3,  true, 'Chargers — Rule A C G (fault flag)'),
    ('cities',             'nb_silver_cities',             4,  true, 'Cities reference table — Rule A G'),
    ('complaints',         'nb_silver_complaints',         5,  true, 'Complaints — Rule A C G I (SLA breach)'),
    ('customers',          'nb_silver_customers',          6,  true, 'Customers — Rule A F G (PCI email mask)'),
    ('employees',          'nb_silver_employees',          7,  true, 'Employees reference table — Rule A G'),
    ('energy_prices',      'nb_silver_energy_prices',      8,  true, 'Energy prices — Rule A G, is_current flag'),
    ('maintenance_events', 'nb_silver_maintenance_events', 9,  true, 'Maintenance events — Rule A C G, high_mttr_flag'),
    ('partners',           'nb_silver_partners',           10, true, 'Partners — Rule A G, contract_expired_flag'),
    ('pipeline_audit',     'nb_silver_pipeline_audit',     11, true, 'Pipeline audit — Rule A G, success_rate_pct'),
    ('sessions',           'nb_silver_sessions',           12, true, 'Sessions — Rule A B G I J'),
    ('states',             'nb_silver_states',             13, true, 'States reference table — Rule A G'),
    ('stations',           'nb_silver_stations',           14, true, 'Stations — Rule A G, state_region'),
    ('tariffs',            'nb_silver_tariffs',            15, true, 'Tariffs — Rule A G H (is_current recompute)'),
    ('vehicles',           'nb_silver_vehicles',           16, true, 'Vehicles — Rule A G H (SCD2 is_current)');
```

### Step 3 — Verify

```sql
SELECT entity_name, notebook_name, run_order, is_active
FROM   ev_catalog.ctl.silver_control
ORDER  BY run_order;
-- Should return 16 rows, all is_active = true
```

---

## Part 3: Notebook — nb_list_silver_notebooks

This notebook replaces the hardcoded 16-activity chain. It reads the control table and returns
a JSON array string that ADF's ForEach can parse directly.

### Create in Databricks

Path: `/Users/hariomsuryawanshi68258@gmail.com/nb_list_silver_notebooks`

```python
# ── Cell 1: read active Silver notebooks from control table ───────────────────
import json

rows = spark.sql("""
    SELECT notebook_name, entity_name
    FROM   ev_catalog.ctl.silver_control
    WHERE  is_active = true
    ORDER  BY run_order ASC
""").collect()

if not rows:
    dbutils.notebook.exit("[]")

notebooks = [
    {"notebook_name": r["notebook_name"], "entity_name": r["entity_name"]}
    for r in rows
]

print(f"Active Silver notebooks ({len(notebooks)}):")
for nb in notebooks:
    print(f"  {nb['notebook_name']}")

# ── Cell 2: exit with JSON string ─────────────────────────────────────────────
dbutils.notebook.exit(json.dumps(notebooks))
```

### Test it manually before wiring to ADF

Run the notebook manually. The exit value in the "Result" panel should look like:
```json
[
  {"notebook_name": "nb_silver_payments",     "entity_name": "payments"},
  {"notebook_name": "nb_silver_charge_cards", "entity_name": "charge_cards"},
  ...
]
```

If you see an empty string or Python exception, check that `ev_catalog.ctl.silver_control` was created and populated (Part 2).

---

## Part 4: ADF Pipelines — Step by Step (UI)

### 4.1 pl_silver_runner — the generic Silver executor

This is a **single pipeline** that runs any Silver notebook. It receives the notebook name
as a parameter from the ForEach and builds the Databricks path at runtime.

**Create the pipeline:**

1. ADF Studio → Author → **+ (New pipeline)** → name: `pl_silver_runner`
2. In Settings tab → Folder: `silver`
3. Add three **Parameters** (Parameters tab at bottom):

| Name | Type | Default |
|------|------|---------|
| `p_notebook` | String | `nb_silver_payments` |
| `p_run_date` | String | *(blank)* |
| `p_run_type` | String | `incremental` |

4. Drag **Databricks Notebook** activity onto canvas → name: `act_run_silver_notebook`
5. Open activity → **Azure Databricks** tab:
   - Linked service: `ls_databricks`
6. Open activity → **Settings** tab:
   - Notebook path: click the expression toggle (blue pill) → paste:
     ```
     @concat('/Users/hariomsuryawanshi68258@gmail.com/', pipeline().parameters.p_notebook)
     ```
7. In Settings tab → **Base parameters** → + New:

| Name | Value (Expression) |
|------|-------------------|
| `p_run_date` | `@if(empty(pipeline().parameters.p_run_date), formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd'), pipeline().parameters.p_run_date)` |
| `p_run_type` | `@pipeline().parameters.p_run_type` |

8. Open activity → **General** tab → set Timeout: `02:00:00`, Retry: `1`, Retry interval: `60`
9. **Publish All**

> **How the notebook path expression works:**
> `@concat('/Users/hariomsuryawanshi68258@gmail.com/', pipeline().parameters.p_notebook)`
> When `p_notebook = 'nb_silver_sessions'` → path becomes
> `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_sessions`
> This is the exact Databricks workspace path where you imported the notebook.

### 4.2 pl_silver_dynamic_foreach — the Silver master

This pipeline calls `nb_list_silver_notebooks` to get the ordered list, then ForEach iterates it
and calls `pl_silver_runner` for each entity.

**Create the pipeline:**

1. ADF Studio → Author → **+ (New pipeline)** → name: `pl_silver_dynamic_foreach`
2. Folder: `silver`
3. Add two **Parameters**:

| Name | Type | Default |
|------|------|---------|
| `p_run_date` | String | *(blank)* |
| `p_run_type` | String | `incremental` |

**Activity 1 — read control table:**

4. Drag **Databricks Notebook** activity → name: `act_list_silver_notebooks`
5. Azure Databricks tab: Linked service → `ls_databricks`
6. Settings tab: Notebook path (static string, NOT expression):
   `/Users/hariomsuryawanshi68258@gmail.com/nb_list_silver_notebooks`
7. General tab: Timeout `00:10:00`, Retry `0`

**Activity 2 — ForEach over notebooks:**

8. Drag **ForEach** activity → name: `act_foreach_silver`
9. Draw success arrow from `act_list_silver_notebooks` → `act_foreach_silver`
10. Open ForEach → **Settings** tab:
    - Items (expression toggle ON):
      ```
      @json(string(activity('act_list_silver_notebooks').output.runOutput))
      ```
    - Sequential: **checked ON** (Silver notebooks share a cluster — run one at a time)
    - Batch count: leave blank (not relevant when sequential is ON)

11. Inside the ForEach, click **+ Add activity** (pencil icon):
    - Drag **Execute Pipeline** activity → name: `act_run_silver_runner`
    - Pipeline reference: `pl_silver_runner`
    - Wait on completion: **ON**
    - Parameters → + New:

| Name | Value (Expression) |
|------|-------------------|
| `p_notebook` | `@item().notebook_name` |
| `p_run_date` | `@pipeline().parameters.p_run_date` |
| `p_run_type` | `@pipeline().parameters.p_run_type` |

12. Close ForEach editor → **Publish All**

> **Why `@json(string(...))` and not just `@json(...)`?**
> `activity().output.runOutput` returns a raw string from the notebook's `dbutils.notebook.exit()`.
> ADF sometimes wraps it in an extra layer when the notebook exits with a string — `string()` forces
> it to a plain string before `json()` parses it. Without `string()`, you can get the error:
> `json expects its parameter to be a string, provided value is of type 'Object'`.

### 4.3 Update pl_bronze_master_foreach

Replace the old 16-activity Silver chain with a single `ExecutePipeline` call to `pl_silver_dynamic_foreach`.

**Option A — Import from JSON (fastest):**

1. Delete the existing `pl_bronze_master_foreach` from ADF Studio
2. Author → Pipelines → **⋮ → Import from pipeline JSON**
3. Select `day10-silver-control-table-and-dynamic-pipeline/pipelines/pl_bronze_master_foreach.json`
4. Publish All

**Option B — Edit manually in UI:**

1. Open `pl_bronze_master_foreach` in ADF Studio
2. Delete all activities after `act_foreach_source` (the 16 `act_silver_*` activities)
3. Drag **Execute Pipeline** activity → name: `act_silver_dynamic`
4. Draw success arrow from `act_foreach_source` → `act_silver_dynamic`
5. Pipeline reference: `pl_silver_dynamic_foreach`
6. Wait on completion: **ON**
7. Parameters:

| Name | Value (Expression) |
|------|-------------------|
| `p_run_date` | `@pipeline().parameters.p_run_date` |
| `p_run_type` | `@pipeline().parameters.p_run_type` |

8. Add two new **Parameters** to the master pipeline (Parameters tab): `p_run_date` (String), `p_run_type` (String, default: `incremental`)
9. Publish All

---

## Part 5: Final Pipeline Topology

```
pl_bronze_master_foreach  (3 activities on canvas)
│
├── act_list_sources
│   └── nb_list_active_sources  (reads ev_catalog.ctl.pipeline_control)
│       exits: '["payments","sessions","chargers",...]'
│
├── act_foreach_source  (ForEach, sequential)
│   └── act_run_source → pl_bronze_incremental_load_v3
│       p_source_name = @item()
│
└── act_silver_dynamic → pl_silver_dynamic_foreach
    │
    ├── act_list_silver_notebooks
    │   └── nb_list_silver_notebooks  (reads ev_catalog.ctl.silver_control)
    │       exits: '[{"notebook_name":"nb_silver_payments","entity_name":"payments"}, ...]'
    │
    └── act_foreach_silver  (ForEach, sequential)
        └── act_run_silver_runner → pl_silver_runner
            p_notebook = @item().notebook_name   ← dynamic, from control table
            p_run_date = @pipeline().parameters.p_run_date
            p_run_type = @pipeline().parameters.p_run_type
            │
            └── act_run_silver_notebook (DatabricksNotebook)
                notebookPath = @concat('/Users/hariomsuryawanshi68258@gmail.com/',
                                        pipeline().parameters.p_notebook)
                baseParameters:
                  p_run_date = TriggerTime date (or override)
                  p_run_type = incremental / full
```

---

## Part 6: How to Add a New Silver Entity (Operational Guide)

Adding entity #17 (`bookings`) with Day 10 requires **zero ADF changes**:

**Step 1 — Write the notebook** (same pattern as all Day 9 notebooks):
- File: `nb_silver_bookings.py`
- Import to Databricks: `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_bookings`

**Step 2 — Insert one control table row:**
```sql
INSERT INTO ev_catalog.ctl.silver_control
    (entity_name, notebook_name, run_order, is_active, description)
VALUES
    ('bookings', 'nb_silver_bookings', 17, true, 'Bookings — Rule A G');
```

**Step 3 — Done.** Next pipeline trigger automatically picks up `nb_silver_bookings` at position 17.

Compare to Day 9 approach: notebook → pipeline JSON → import pipeline → update master → Publish (5 steps, requires ADF developer access).

---

## Part 7: Enabling / Disabling Entities

### Disable one entity (e.g. pipeline_audit during incident)
```sql
UPDATE ev_catalog.ctl.silver_control
SET    is_active = false,
       updated_at = current_timestamp()
WHERE  entity_name = 'pipeline_audit';
```
Next run: `nb_list_silver_notebooks` skips this row — notebook never triggered. No ADF change.

### Re-enable
```sql
UPDATE ev_catalog.ctl.silver_control
SET    is_active = true,
       updated_at = current_timestamp()
WHERE  entity_name = 'pipeline_audit';
```

### Change execution order (run sessions before payments)
```sql
UPDATE ev_catalog.ctl.silver_control
SET    run_order = 0, updated_at = current_timestamp()
WHERE  entity_name = 'sessions';
```
Order is controlled by `ORDER BY run_order ASC` in the notebook — no ADF change.

---

## Part 8: Day 9 vs Day 10 Side-by-Side Comparison

| Aspect | Day 9 (hardcoded) | Day 10 (control table) |
|--------|-------------------|------------------------|
| Silver pipelines | 16 individual + 1 master | **1 runner + 1 dynamic master** |
| Canvas activities (master) | 18 | **3** |
| Add entity #17 | Write notebook + pipeline JSON + update master + Publish | Write notebook + **INSERT 1 row** |
| Disable entity | Edit pipeline activity + Publish | `UPDATE is_active = false` |
| Change order | Edit activity dependsOn chain + Publish | `UPDATE run_order` |
| Backfill single entity | Trigger `pl_silver_<entity>` with `p_run_date` | Disable all others, trigger master (or call `pl_silver_runner` directly with `p_notebook`) |
| ADF developer access needed to add entity | **Yes** | **No** |
| ADF Monitor — what you see | 16+ named activity boxes | ForEach → iterations → each shows notebook_name |
| Source of truth for execution order | ADF activity chain | `silver_control.run_order` column |

---

## Part 9: Practice Exercises

### Exercise 1 — Bootstrap and verify
1. Create `ev_catalog.ctl.silver_control` using the DDL in Part 2
2. Insert all 16 rows
3. Create `nb_list_silver_notebooks` in Databricks
4. Run the notebook manually — verify the exit value is a valid JSON array with 16 objects
5. Query `SELECT count(*) FROM ev_catalog.ctl.silver_control WHERE is_active = true` → should return 16

### Exercise 2 — Build pl_silver_runner via UI
Following Part 4.1 step by step, build `pl_silver_runner` in ADF Studio.
After building it, trigger it manually with:
- `p_notebook = nb_silver_states` (small table, fast)
- `p_run_date = ` (blank — uses today)
- `p_run_type = incremental`

Verify `ev_catalog.silver.sl_states` has rows.

### Exercise 3 — Build pl_silver_dynamic_foreach via UI
Following Part 4.2 step by step, build the ForEach pipeline.
Before testing end-to-end, test it standalone with `p_run_type = incremental`.
Watch Monitor → drill into ForEach → verify each iteration shows the correct notebook name.

### Exercise 4 — Disable and re-enable
1. `UPDATE is_active = false WHERE entity_name = 'pipeline_audit'`
2. Run `nb_list_silver_notebooks` manually — verify pipeline_audit is absent from the output
3. Trigger `pl_silver_dynamic_foreach` — confirm it runs 15 iterations, not 16
4. Re-enable: `UPDATE is_active = true WHERE entity_name = 'pipeline_audit'`

### Exercise 5 — Add a fictional entity
1. Add a row: `INSERT INTO ev_catalog.ctl.silver_control VALUES ('test_entity', 'nb_silver_payments', 99, true, 'Test')`
   (reuse payments notebook just to test routing)
2. Run `nb_list_silver_notebooks` — verify `nb_silver_payments` appears twice (at position 1 and 99)
3. Check that `pl_silver_runner` correctly resolves `nb_silver_payments` for both rows
4. Clean up: `DELETE FROM ev_catalog.ctl.silver_control WHERE entity_name = 'test_entity'`

### Exercise 6 — Backfill a single entity
Without the individual `pl_silver_<entity>` pipelines from Day 9, how do you backfill one entity?
1. Disable all other entities: `UPDATE ev_catalog.ctl.silver_control SET is_active = false WHERE entity_name != 'sessions'`
2. Trigger `pl_silver_dynamic_foreach` with `p_run_date = 2026-10-01`
3. Re-enable: `UPDATE ev_catalog.ctl.silver_control SET is_active = true`

*Or* call `pl_silver_runner` directly (it is a standalone pipeline):
- `p_notebook = nb_silver_sessions`, `p_run_date = 2026-10-01`, `p_run_type = incremental`

### Exercise 7 — Dynamic path expression
The notebook path expression in `pl_silver_runner` is:
```
@concat('/Users/hariomsuryawanshi68258@gmail.com/', pipeline().parameters.p_notebook)
```
What does ADF resolve this to when `p_notebook = 'nb_silver_complaints'`?
Write the answer without running it. Then trigger the pipeline to verify.

---

## Part 10: Interview Questions and Answers

### Q1. What is the problem with Day 9's approach of one pipeline per Silver entity?
**A.** Every new entity requires: write notebook → write pipeline JSON → import to ADF → add `ExecutePipeline` activity to master → Publish. This requires ADF developer access and a deployment cycle. With 16 entities the master pipeline canvas has 18 activities, which is hard to maintain. If the execution order needs to change, the activity `dependsOn` chain must be manually re-wired and re-published.

---

### Q2. How does the control table pattern solve the maintenance problem?
**A.** The control table (`ev_catalog.ctl.silver_control`) is the single source of truth for which Silver notebooks exist, in what order to run them, and whether they are currently active. `nb_list_silver_notebooks` reads this table at runtime and returns an ordered list. ADF's ForEach iterates that list. Adding a new entity means inserting one SQL row — no ADF pipeline changes, no publish, no deployment needed.

---

### Q3. Why does pl_silver_runner use `@concat(...)` for the notebook path instead of a static string?
**A.** Because the pipeline is generic — it must be able to run any of the 16 Silver notebooks. If the path were static (`/Users/.../nb_silver_payments`), the pipeline could only ever call the payments notebook. The `@concat` expression builds the path dynamically from the `p_notebook` parameter, which is passed by the ForEach from the control table row. This is what makes a single `pl_silver_runner` replace 16 individual pipelines.

---

### Q4. What does `@json(string(activity('act_list_silver_notebooks').output.runOutput))` do — why both `string()` and `json()`?
**A.**
- `activity('act_list_silver_notebooks').output.runOutput` — the notebook's exit value, but ADF may return it as an Object (parsed JSON) or a String depending on context.
- `string(...)` — forces it to a plain string (handles the Object case, no-op if already string).
- `json(...)` — parses the string back into an ADF Array so ForEach can iterate over it.

Without `string()`: if runOutput is already an Object, `json()` throws `json expects its parameter to be a string`. Without `json()`: ForEach tries to iterate the characters of the string instead of the array elements.

---

### Q5. Why is the ForEach `isSequential = true` for Silver notebooks?
**A.** All Silver notebooks run on the same Databricks job cluster. Running them in parallel would submit multiple notebook jobs simultaneously — the cluster would queue them, causing unpredictable delays and potential timeouts. Sequential execution ensures each notebook runs to completion before the next starts, giving it full cluster resources. If performance becomes a concern, the solution is to provision separate clusters per notebook (expensive) or run parallel with batchCount ≤ 3.

---

### Q6. How would you backfill a single Silver entity with the Day 10 design?
**A.** Two options:
1. **Direct runner call:** Trigger `pl_silver_runner` directly with `p_notebook = nb_silver_sessions`, `p_run_date = 2026-10-01`. This bypasses the control table entirely — useful for ad-hoc backfills.
2. **Isolation via control table:** `UPDATE is_active = false` for all entities except the target, trigger `pl_silver_dynamic_foreach`, then re-enable all. Less elegant but useful when the dynamic pipeline has pre/post hooks you want to preserve.

---

### Q7. What happens if `nb_list_silver_notebooks` exits with `"[]"` (empty array)?
**A.** ADF's ForEach receives an empty array and performs zero iterations — no Silver notebooks run. The ForEach activity itself succeeds (zero iterations is success, not failure). The pipeline completes without error. This is the expected behaviour when all entities are disabled (`is_active = false`), but it can be a silent failure if the control table is accidentally empty. Add a monitoring check: alert if `ForEach.output.numberOfSucceededIterations == 0` in the pipeline run.

---

### Q8. What is the difference between `p_run_date` in pl_silver_runner vs the Day 9 pipelines?
**A.** Identical logic, but Day 9 each pipeline had it hardcoded in its own JSON. Day 10 centralises it in `pl_silver_runner`:
```
@if(empty(pipeline().parameters.p_run_date),
    formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd'),
    pipeline().parameters.p_run_date)
```
When `p_run_date` is blank → uses today's TriggerTime date (daily scheduled run).
When `p_run_date` is set → uses that date (manual backfill). The notebooks receive this as their `p_run_date` widget value and read the corresponding Bronze partition.

---

### Q9. How is the `run_order` column used and why is it important?
**A.** `nb_list_silver_notebooks` uses `ORDER BY run_order ASC` when querying the control table. The returned array is ordered by this column, so ADF's ForEach iterates notebooks in that exact sequence. This matters because some Silver entities could depend on others (e.g. sessions references payments for foreign-key integrity checks). By controlling `run_order` via SQL `UPDATE`, the execution sequence can be changed without any ADF modification.

---

### Q10. Could you run the Silver ForEach in parallel? What would need to change?
**A.** Yes, by setting `isSequential = false` and `batchCount = N` on `act_foreach_silver`. Trade-offs:
- **Pro:** Wall-clock time reduced from (sum of all notebook runtimes) to (longest notebook runtime × ceil(16 / N))
- **Con:** Multiple notebooks compete for the same Databricks cluster resources — risk of OOM, job queueing, and timeouts
- **Requirements to make it safe:** Each Silver table must write to a different Delta path (already true — each has its own `SILVER_PATH`). Delta's ACID guarantees prevent two notebooks from corrupting the same table. However, if two notebooks both write to `ev_catalog.quarantine.*`, quarantine `mode("append")` is safe under concurrent writes.
- **Recommended batchCount:** `3–5` on a shared job cluster. Monitor Databricks job queue in the first run.

---

### Q11. What is the difference between the Bronze control table and the Silver control table?
**A.**

| | Bronze (`pipeline_control`) | Silver (`silver_control`) |
|---|---|---|
| Purpose | Which API sources to ingest | Which Silver notebooks to run |
| Key columns | endpoint, watermark, page_size, load_type | notebook_name, run_order, is_active |
| Read by | `nb_list_active_sources`, `nb_read_control_table` | `nb_list_silver_notebooks` |
| Updated by | `nb_update_watermark` (after each run) | Manual SQL (or future orchestration notebook) |
| Row = | One REST API source | One Silver transformation notebook |

---

### Q12. Why does pl_silver_runner have a default value for p_notebook?
**A.** ADF requires a default for optional parameters if you want to trigger the pipeline manually without specifying every parameter. The default `nb_silver_payments` makes it easy to test the pipeline in isolation: just trigger it with no overrides and it runs the payments notebook. In production it is always called from the ForEach with an explicit `p_notebook` value — the default never applies in that path.

---

## Part 11: Common Errors and Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `json expects its parameter to be a string, provided value is of type 'Object'` | `@json()` received a pre-parsed Object from runOutput — missing `string()` wrapper | Change ForEach items to `@json(string(activity('act_list_silver_notebooks').output.runOutput))` |
| ForEach runs 0 iterations | All rows in `silver_control` have `is_active = false`, or table is empty | Run `SELECT * FROM ev_catalog.ctl.silver_control` — check is_active column |
| `notebook not found` error in Databricks activity | `@concat` expression produces wrong path (e.g. email in notebook_name column) | Verify `silver_control.notebook_name` values are just the notebook name without path (e.g. `nb_silver_payments` NOT `/Users/.../nb_silver_payments`) |
| `item().notebook_name` resolves to null | ForEach items expression returns flat strings instead of objects | Check `nb_list_silver_notebooks` exit value — must be `[{"notebook_name": "...", "entity_name": "..."}, ...]` not `["nb_silver_payments", ...]` |
| Silver notebook exits `NO_DATA` | Bronze partition for that date doesn't exist | Run Bronze pipeline first, or pass `p_run_date = ''` for full load |
| `act_list_silver_notebooks` times out | Cluster cold start exceeds 10-min timeout | Increase General tab timeout to `00:20:00`, or use a warm interactive cluster |
| Pipeline fails on entity N, entities N+1..16 skipped | ForEach `isSequential = true` — failure of one iteration stops the rest | Fix the failing notebook, then re-run with only that entity `is_active = true` to resume |
| `DELTA_CONCURRENT_WRITE` error | Two concurrent Silver notebooks writing to same table | Should not happen with sequential ForEach — if it occurs, check for duplicate `notebook_name` rows in control table |

---

## Part 12: Day 10 Checklist

**Control table:**
- [ ] `ev_catalog.ctl.silver_control` created
- [ ] All 16 rows inserted
- [ ] Verify: `SELECT count(*) FROM ev_catalog.ctl.silver_control WHERE is_active = true` → 16

**Notebook:**
- [ ] `nb_list_silver_notebooks.py` created in Databricks at `/Users/hariomsuryawanshi68258@gmail.com/nb_list_silver_notebooks`
- [ ] Notebook tested manually — exit value is valid JSON array with 16 objects
- [ ] Each object has keys `notebook_name` and `entity_name`

**ADF — pl_silver_runner:**
- [ ] Pipeline created with 3 parameters: `p_notebook`, `p_run_date`, `p_run_type`
- [ ] Databricks Notebook activity: path expression `@concat('/Users/hariomsuryawanshi68258@gmail.com/', pipeline().parameters.p_notebook)`
- [ ] Base parameters: `p_run_date` with TriggerTime fallback expression, `p_run_type`
- [ ] Timeout: `02:00:00`, Retry: `1`
- [ ] Tested manually with `p_notebook = nb_silver_states` — `sl_states` has rows
- [ ] Published

**ADF — pl_silver_dynamic_foreach:**
- [ ] Pipeline created with 2 parameters: `p_run_date`, `p_run_type`
- [ ] `act_list_silver_notebooks`: notebook path `/Users/hariomsuryawanshi68258@gmail.com/nb_list_silver_notebooks`
- [ ] `act_foreach_silver`: items expression with `@json(string(...))`, Sequential ON
- [ ] `act_run_silver_runner`: `p_notebook = @item().notebook_name`, wait ON
- [ ] Tested standalone — Monitor shows 16 ForEach iterations, all Succeeded
- [ ] Published

**ADF — pl_bronze_master_foreach (updated):**
- [ ] Old 16-activity Silver chain removed
- [ ] `act_silver_dynamic` → `pl_silver_dynamic_foreach` added after `act_foreach_source`
- [ ] `p_run_date` and `p_run_type` parameters passed through
- [ ] Full end-to-end test: Bronze loads + all 16 Silver notebooks run via ForEach
- [ ] Published
