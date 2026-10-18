# Day 6 — Multi-Source Orchestration: All 20 VoltGrid API Sources

**Duration:** 2 hours | **Goal:** Register all 20 VoltGrid API sources in the control table and run a single master pipeline that ingests every source — file-based scenarios, DB facts, DB dimensions, and reference tables — into Bronze in one scheduled run.

> **Prerequisites:** Day 5 complete — `pl_bronze_incremental_load_v2` deployed and tested for payments. Day 6 adds 17 new control table rows and expands `pl_bronze_master` to chain all 20 sources.

---

## BEFORE YOU START — Student Customization Guide

No new linked services or datasets needed — Day 6 reuses everything from Day 4/5.

| What to Check | Instructor's Value |
|---|---|
| Unity Catalog control table path | `ev_catalog.ctl.pipeline_control` |
| Child pipeline name in every ExecutePipeline | `pl_bronze_incremental_load_v2` |
| ADF pipeline folder (optional) | `bronze` |
| ADF MI Application ID (if table permissions need re-granting) | `9245da2f-7129-4e0c-92c4-134fe896e294` |

---

## API Source Overview

The VoltGrid API exposes 20 data sources across four groups:

### Group 1 — File-Based Scenario APIs (3)
These serve static load files (scenarios 1–5). Each run fetches the same records unless the load scenario changes.

| source_name | Endpoint | load_type | Primary Key |
|---|---|---|---|
| `payments` | `/api/db/payments/` | incremental | `payment_id` |
| `fleet` | `/api/fleet/` | full | `vehicle_id` |
| `weather` | `/api/weather/` | full | `city_id` |

### Group 2 — DB Fact / High-Frequency Tables (6)
Live database rows — change frequently. Use `updated_after` watermark to fetch only new/changed rows.

| source_name | Endpoint | load_type | Primary Key | Fields |
|---|---|---|---|---|
| `db_payments` | `/api/db/payments/` | incremental | `payment_id` | payment_id, session_id, customer_id, gateway, amount_aud, gst, payment_mode, status, processed_at |
| `db_sessions` | `/api/db/sessions/` | incremental | `session_id` | session_id, vehicle_id, station_id, customer_id, started_at, ended_at, duration_min, energy_kwh, cost_aud, peak_power_kw, connector_type, session_status, payment_id |
| `db_energy_prices` | `/api/db/energy-prices/` | incremental | `price_id` | price_id, station_id, state_code, price_per_kwh, off_peak_price, peak_price, currency, effective_from, effective_to, tariff_type |
| `db_complaints` | `/api/db/complaints/` | incremental | `complaint_id` | complaint_id, customer_id, station_id, category, priority, sla_breach, status, created_ts, resolved_ts |
| `db_maintenance_events` | `/api/db/maintenance-events/` | incremental | `event_id` | event_id, charger_id, station_id, employee_id, root_cause, mttr_hours, event_ts |
| `db_pipeline_audit` | `/api/db/pipeline-audit/` | incremental | `run_id` | run_id, pipeline_name, source_name, load_type, status, rows_read, rows_written, rows_rejected, started_at, ended_at, duration_sec, error_message, sla_met, watermark_value |

### Group 3 — DB Dimension Tables (6, slow-changing)
Change less often but may have profile updates — incremental to catch them.

| source_name | Endpoint | load_type | Primary Key | Key Fields |
|---|---|---|---|---|
| `db_customers` | `/api/db/customers/` | incremental | `customer_id` | customer_id, full_name, email, phone, loyalty_tier, signup_date |
| `db_vehicles` | `/api/db/vehicles/` | incremental | `vehicle_id` | vehicle_id, make, model, year, battery_capacity_kwh, range_km, vehicle_type, registration_state, partner_id, effective_from, effective_to, is_current *(SCD2)* |
| `db_chargers` | `/api/db/chargers/` | incremental | `charger_id` | charger_id, station_id, connector_type, max_kw, firmware_version, install_date, status |
| `db_charge_cards` | `/api/db/charge-cards/` | incremental | `card_id` | card_id, customer_id, rfid_uid, card_number, card_type, status |
| `db_partners` | `/api/db/partners/` | incremental | `partner_id` | partner_id, partner_name, state, status, revenue_share_pct, contract_start, contract_end |
| `db_employees` | `/api/db/employees/` | incremental | `employee_id` | employee_id, name, department, station_id, state, hire_date |

### Group 4 — DB Reference Tables (4, full load)
Rarely change — full load keeps them simple. Small row counts.

| source_name | Endpoint | load_type | Primary Key | What it is |
|---|---|---|---|---|
| `db_states` | `/api/db/states/` | full | `state_code` | 8 Australian states/territories |
| `db_cities` | `/api/db/cities/` | full | `city_id` | City → postcode → state mapping |
| `db_tariffs` | `/api/db/tariffs/` | full | `tariff_id` | Energy tariff rate schedules |
| `db_stations` | `/api/db/stations/` | full | `station_id` | Charging station infrastructure |

---

## Part 1 — Register All Sources in the Control Table

### 1.1 Run the INSERT Statements

Open Databricks → SQL editor → run `control-table-setup.sql` (in this folder).

The script inserts **17 new rows** (the 3 file-based sources — payments, fleet, weather — were inserted in the earlier Day 6 step).

After running, verify:

```sql
SELECT source_name, endpoint, load_type, is_active
FROM ev_catalog.ctl.pipeline_control
ORDER BY source_name;
```

You should see **20 rows** (or 19 if `payments` and `db_payments` are counted as one — they share the same endpoint but have separate `source_name` keys and independent watermarks).

### 1.2 load_type Decision Guide

| Scenario | Use `incremental` | Use `full` |
|---|---|---|
| Table has `updated_at` column | Yes | — |
| API supports `?updated_after=` filter | Yes | — |
| Table has < 10k rows and changes infrequently | — | Yes |
| Table is append-only (audit logs, events) | Yes | — |
| Table is a lookup / reference (states, postcodes) | — | Yes |

All DB endpoints support `updated_after` (via `UpdatedAtFilterSet` in the API). Reference tables (states, cities, tariffs, stations) are set to `full` because they are tiny and rarely change — simpler to just reload them.

---

## Part 2 — Create the Master Pipeline `pl_bronze_master`

> **Option A — Import from JSON (fastest):** ADF Studio → Author → Pipelines → `...` menu on the Pipelines heading → **Import from JSON** → upload `pipelines/pl_bronze_master.json`. Skip to Section 2.4 to verify.
>
> **Option B — Build manually (recommended for learning):** Follow Sections 2.1 through 2.3 below step by step.

---

### 2.1 Create the Pipeline and Add the Parameter

1. ADF Studio → **Author** → Pipelines → **+** → **New pipeline**
2. Rename it: in the Properties panel (right side) set the name to `pl_bronze_master`
3. Optionally set the folder to `bronze` (Properties → Folder)
4. Click the **Parameters** tab (bottom of the canvas):
   - **+ New** → Name: `p_load_type_override` · Type: `String` · Default value: *(leave empty — do not write null)*
5. Click anywhere on the canvas to deselect — do NOT save yet, add all activities first

> **Why `p_load_type_override`?** Passing `"full"` at trigger time forces every child pipeline to do a full reload, ignoring the watermark stored in the control table. Useful for the initial load and for data repairs. Empty string means each source uses its own `load_type` from the control table.

---

### 2.2 Add the First Execute Pipeline Activity

Every activity in this pipeline follows the same pattern. Add the first one in full detail, then repeat for the remaining 18.

1. In the Activities search box (left panel) type `Execute` → drag **Execute Pipeline** onto the canvas
2. Click the activity to select it → Properties panel opens at the bottom

**General tab:**
- Name: `act_run_payments`
- Description: `File-based payments (load scenarios 1-5)`

**Settings tab:**
- Invoked pipeline → click the dropdown → select `pl_bronze_incremental_load_v2`
- **Wait on completion: ON** — this is critical. If this is off, the master pipeline will fire all sources simultaneously without waiting for each one to finish.

**Parameters tab** (appears after selecting the invoked pipeline):

| Name | Value | Type |
|---|---|---|
| `p_source_name` | `payments` | Expression *(or just type the string — no `@` needed for a literal)* |
| `p_load_type_override` | `@pipeline().parameters.p_load_type_override` | Expression |

> `@pipeline().parameters.p_load_type_override` passes the master pipeline's override parameter down to every child. This is what lets you control all 20 sources with a single trigger parameter.

---

### 2.3 Add the Remaining 18 Activities and Chain Them

For each subsequent activity, the steps are:
1. Drag a new **Execute Pipeline** onto the canvas
2. Set the **Name**, **Invoked pipeline**, **Wait on completion ON**, and **Parameters** as shown in the table below
3. **Connect** it to the previous activity: hover over the previous activity box → a small arrow appears on the right edge → drag it to the new activity. The arrow must be **green** (success condition). If it appears red or blue, right-click the arrow → **On success**

Repeat for all 19 activities in order:

| # | Activity Name | `p_source_name` value | Depends On (previous activity) |
|---|---|---|---|
| 1 | `act_run_payments` | `payments` | — (pipeline start) |
| 2 | `act_run_fleet` | `fleet` | `act_run_payments` |
| 3 | `act_run_weather` | `weather` | `act_run_fleet` |
| 4 | `act_run_db_payments` | `db_payments` | `act_run_weather` |
| 5 | `act_run_db_sessions` | `db_sessions` | `act_run_db_payments` |
| 6 | `act_run_db_energy_prices` | `db_energy_prices` | `act_run_db_sessions` |
| 7 | `act_run_db_complaints` | `db_complaints` | `act_run_db_energy_prices` |
| 8 | `act_run_db_maintenance_events` | `db_maintenance_events` | `act_run_db_complaints` |
| 9 | `act_run_db_pipeline_audit` | `db_pipeline_audit` | `act_run_db_maintenance_events` |
| 10 | `act_run_db_customers` | `db_customers` | `act_run_db_pipeline_audit` |
| 11 | `act_run_db_vehicles` | `db_vehicles` | `act_run_db_customers` |
| 12 | `act_run_db_chargers` | `db_chargers` | `act_run_db_vehicles` |
| 13 | `act_run_db_charge_cards` | `db_charge_cards` | `act_run_db_chargers` |
| 14 | `act_run_db_partners` | `db_partners` | `act_run_db_charge_cards` |
| 15 | `act_run_db_employees` | `db_employees` | `act_run_db_partners` |
| 16 | `act_run_db_states` | `db_states` | `act_run_db_employees` |
| 17 | `act_run_db_cities` | `db_cities` | `act_run_db_states` |
| 18 | `act_run_db_tariffs` | `db_tariffs` | `act_run_db_cities` |
| 19 | `act_run_db_stations` | `db_stations` | `act_run_db_tariffs` |

> **All 19 activities use the same Settings tab configuration:**
> - Invoked pipeline: `pl_bronze_incremental_load_v2`
> - Wait on completion: **ON**
> - Parameter `p_load_type_override`: `@pipeline().parameters.p_load_type_override`
>
> **Only `p_source_name` differs** per activity — it is the literal string value from the table above (e.g. `payments`, `fleet`, `db_payments`, etc.).

**Dependency condition — how to set it correctly:**

When you drag an arrow from activity A to activity B, ADF defaults to `Succeeded`. To verify:
- Click the arrow between two activities → Properties panel shows the condition
- It must say **On success** (green arrow, condition = `Succeeded`)
- Do NOT use `Completed` (runs even if previous failed) or `Failed`

---

### 2.4 Verify the Canvas

After adding all 19 activities, the canvas should look like a single horizontal (or vertical) chain of 19 boxes, each connected to the next by a **green arrow**. Use **Fit to screen** (the four-corner icon in the canvas toolbar) to see the full chain at once.

Quick verification checklist:
- [ ] 19 activities on the canvas
- [ ] Every activity is connected with a **green** (success) arrow to the next
- [ ] No activity is disconnected or floating on its own
- [ ] Click each activity → Settings tab → **Wait on completion** is checked **ON**
- [ ] Click each activity → Parameters tab → `p_source_name` has the correct literal value for that source
- [ ] Click each activity → Parameters tab → `p_load_type_override` value is `@pipeline().parameters.p_load_type_override` (expression mode)

---

### 2.5 Publish

Click **Publish all**. ADF will validate the pipeline before publishing — if any activity has a missing required field, validation will fail and highlight the activity.

Both `pl_bronze_master` and `pl_bronze_incremental_load_v2` must be in a published state. If `pl_bronze_incremental_load_v2` is not yet published, publish it first.

---

## Part 3 — Running the Master Pipeline

### 3.1 Initial Full Load (First Run)

**Trigger now** → Parameters:
- `p_load_type_override`: `full`

This forces all 20 sources to do a full load regardless of their control table `load_type`. Use this when Bronze is empty or after a full reset.

### 3.2 Daily Scheduled Run (After Initial Load)

**Trigger now / Add trigger** → Parameters:
- `p_load_type_override`: *(empty)*

Each source uses its own `load_type`:
- Incremental sources fetch only records with `updated_at > last_watermark`
- Full sources re-fetch everything (small tables — this is fine)

### 3.3 Estimated Run Order and Times

| Order | Source | load_type | Expected pages | Notes |
|---|---|---|---|---|
| 1 | payments | incremental | 1–10 | Only new transactions since last run |
| 2 | fleet | full | 1–5 | All vehicle records every run |
| 3 | weather | full | 1–3 | All city observations every run |
| 4 | db_payments | incremental | 1–10 | Live DB transactions |
| 5 | db_sessions | incremental | 1–20 | High volume — one row per charge session |
| 6 | db_energy_prices | incremental | 1–50 | Hourly rows — high volume |
| 7 | db_complaints | incremental | 1–5 | Lower volume |
| 8 | db_maintenance_events | incremental | 1–5 | Lower volume |
| 9 | db_pipeline_audit | incremental | 1–5 | One row per pipeline run |
| 10 | db_customers | incremental | 1–10 | Slow-changing |
| 11 | db_vehicles | incremental | 1–10 | SCD2 rows, slow-changing |
| 12 | db_chargers | incremental | 1–5 | Slow-changing |
| 13 | db_charge_cards | incremental | 1–10 | Slow-changing |
| 14 | db_partners | incremental | 1–3 | Very slow-changing |
| 15 | db_employees | incremental | 1–5 | Very slow-changing |
| 16 | db_states | full | 1 | 8 rows — trivial |
| 17 | db_cities | full | 1–5 | Small reference |
| 18 | db_tariffs | full | 1–3 | Small reference |
| 19 | db_stations | full | 1–10 | Medium reference |

### 3.4 Monitoring

ADF Monitor → Pipeline runs → find `pl_bronze_master` → click it → each of the 19 activities links to its child `pl_bronze_incremental_load_v2` run. Drill through to see individual activities inside the child.

---

## Part 4 — Bronze Output Structure

```
ADLS bronze container
└── api/
    ├── payments/              ingestion_date=<today>/  payments_page_*.json
    ├── fleet/                 ingestion_date=<today>/  fleet_page_*.json
    ├── weather/               ingestion_date=<today>/  weather_page_*.json
    └── db/
        ├── payments/          ingestion_date=<today>/  db_payments_page_*.json
        ├── sessions/          ingestion_date=<today>/  db_sessions_page_*.json
        ├── energy_prices/     ingestion_date=<today>/  db_energy_prices_page_*.json
        ├── complaints/        ingestion_date=<today>/  db_complaints_page_*.json
        ├── maintenance_events/ingestion_date=<today>/  db_maintenance_events_page_*.json
        ├── pipeline_audit/    ingestion_date=<today>/  db_pipeline_audit_page_*.json
        ├── customers/         ingestion_date=<today>/  db_customers_page_*.json
        ├── vehicles/          ingestion_date=<today>/  db_vehicles_page_*.json
        ├── chargers/          ingestion_date=<today>/  db_chargers_page_*.json
        ├── charge_cards/      ingestion_date=<today>/  db_charge_cards_page_*.json
        ├── partners/          ingestion_date=<today>/  db_partners_page_*.json
        ├── employees/         ingestion_date=<today>/  db_employees_page_*.json
        ├── states/            ingestion_date=<today>/  db_states_page_*.json
        ├── cities/            ingestion_date=<today>/  db_cities_page_*.json
        ├── tariffs/           ingestion_date=<today>/  db_tariffs_page_*.json
        └── stations/          ingestion_date=<today>/  db_stations_page_*.json
```

---

## Part 5 — How to Add a New Source

**For `pl_bronze_master` (static chain):**
1. INSERT one row into the control table
2. Add one `ExecutePipeline` activity at the end of the chain → publish

**For `pl_bronze_master_foreach` or `pl_bronze_master_parallel` (dynamic):**
1. INSERT one row into the control table with `is_active = true`
2. That's it — no pipeline change needed. The Lookup reads all active rows at runtime.

```sql
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('my_source', '/api/db/my-source/', 'api/db/my_source', 'my_source', 1000, 'incremental', '1900-01-01T00:00:00Z', true);
```

This is the key advantage of the ForEach design — the pipeline never needs to be touched again.

---

## Part 6 — Alternative Design: Dynamic ForEach (Sequential)

> **Pipeline:** `pl_bronze_master_foreach` · **File:** `pipelines/pl_bronze_master_foreach.json`
>
> **No new linked service or dataset needed** — uses `ls_databricks` exactly like Day 5.

Instead of 19 hardcoded `ExecutePipeline` activities, this pipeline uses **3 activities total**: a notebook to read all active sources, a SetVariable to store the list, and a ForEach to iterate over it.

```
act_list_sources    (DatabricksNotebook — nb_list_active_sources)
      │  exits with: '["payments","fleet","weather","db_payments",...]'
      ▼
act_set_sources     (SetVariable — v_sources = notebook runOutput)
      │
      ▼
act_foreach_source  (ForEach — iterates over json(v_sources), isSequential=true)
      └─ act_run_source  (ExecutePipeline — p_source_name = @item())
```

### Step 1 — Create the Notebook `nb_list_active_sources`

This is the only new artifact needed. Create it in Databricks exactly the same way as `nb_read_control_table` from Day 3.

**Databricks → your workspace folder → Create → Notebook → name: `nb_list_active_sources`**

```python
# nb_list_active_sources
# No parameters — reads all active sources from control table
# Exits with a JSON array string of source_names

import json

rows = spark.sql("""
    SELECT source_name
    FROM ev_catalog.ctl.pipeline_control
    WHERE is_active = true
    ORDER BY source_name
""").collect()

source_names = [r["source_name"] for r in rows]

if not source_names:
    raise Exception("No active sources found in ev_catalog.ctl.pipeline_control")

dbutils.notebook.exit(json.dumps(source_names))
```

**What it returns:**

`dbutils.notebook.exit()` passes a string back to ADF as `activity('act_list_sources').output.runOutput`. The string is a JSON array:
```
["db_charge_cards","db_chargers","db_cities","db_complaints","db_customers",
 "db_employees","db_energy_prices","db_maintenance_events","db_partners",
 "db_payments","db_pipeline_audit","db_sessions","db_states","db_stations",
 "db_tariffs","db_vehicles","fleet","payments","weather"]
```

> This is the same `dbutils.notebook.exit()` pattern used in Day 3/4/5. ADF treats the exit value as a string — `runOutput` contains the raw string, not a parsed array. That is why the next step uses `SetVariable` + `json()` to convert it.

---

### Step 2 — Add Pipeline Variable `v_sources`

Before adding activities, add a pipeline variable to hold the source list.

**Variables tab (bottom of canvas):**
- Name: `v_sources`
- Type: `String`
- Default value: `[]`

> **Why a variable and not a direct expression?** ADF expressions in ForEach `items` cannot reference activity outputs directly when the ForEach depends on a SetVariable in between. The SetVariable acts as a bridge — store the output in a variable, then ForEach reads the variable.

---

### Step 3 — Build the Pipeline `pl_bronze_master_foreach`

**1. Create the pipeline**

ADF Studio → Author → Pipelines → **+ New pipeline** → rename to `pl_bronze_master_foreach`

Add parameter:
- Name: `p_load_type_override` · Type: `String` · Default: *(empty)*

Add variable:
- Name: `v_sources` · Type: `String` · Default: `[]`

---

**2. Add `act_list_sources` (Databricks Notebook)**

Drag **Databricks Notebook** from the Activities panel (under Databricks group) onto the canvas:

- **General tab** → Name: `act_list_sources`
- **Azure Databricks tab:**
  - Linked service: `ls_databricks`
- **Settings tab:**
  - Notebook path: `/Users/your-email@gmail.com/nb_list_active_sources`
  - *(no base parameters needed — notebook reads all active sources)*
- Depends on: nothing (pipeline start)

---

**3. Add `act_set_sources` (Set Variable)**

Drag **Set Variable** onto the canvas → connect `act_list_sources` → `act_set_sources` (green arrow):

- **General tab** → Name: `act_set_sources`
- **Variables tab:**
  - Variable: `v_sources`
  - Value: `@activity('act_list_sources').output.runOutput` (Expression)
- Depends on: `act_list_sources` (Succeeded)

> `output.runOutput` is the string that `dbutils.notebook.exit()` returned — the raw JSON array string like `["payments","fleet",...]`. It is stored as a String variable here.

---

**4. Add `act_foreach_source` (ForEach)**

Drag **ForEach** onto the canvas → connect `act_set_sources` → `act_foreach_source` (green arrow):

- **General tab** → Name: `act_foreach_source`
- **Settings tab:**
  - Items: `@json(variables('v_sources'))` (Expression)
  - Sequential: **checked ON**
  - Batch count: *(leave empty — not used when sequential)*
- Depends on: `act_set_sources` (Succeeded)

> **Why `@json(variables('v_sources'))` and not just `variables('v_sources')`?**
> `v_sources` holds a **string** — ADF ForEach `items` requires an **array**. `json()` parses the string into a real array so ForEach can iterate over it. Without this, ADF throws `The expression 'variables(v_sources)' cannot be evaluated because type 'String' cannot be converted to type 'Array'`.

---

**5. Add `act_run_source` inside the ForEach**

Click the **pencil icon** on `act_foreach_source` to open the inner canvas. Drag **Execute Pipeline** into it:

- **General tab** → Name: `act_run_source`
- **Settings tab:**
  - Invoked pipeline: `pl_bronze_incremental_load_v2`
  - Wait on completion: **ON**
- **Parameters tab:**

  | Name | Value | Note |
  |---|---|---|
  | `p_source_name` | `@item()` | `item()` is a plain string — the source_name value itself (e.g. `"payments"`) |
  | `p_load_type_override` | `@pipeline().parameters.p_load_type_override` | Passed through from master |

- Depends on: nothing inside loop (first and only activity in the body)

Click the **back arrow** (top-left of inner canvas) to return to the main canvas.

> **Why `@item()` and not `@item().source_name`?**
> The notebook exits with a plain string array `["payments","fleet",...]` — not an array of objects. Each element is a plain string, so `@item()` gives the string directly. If the notebook had returned `[{"source_name":"payments"},...]`, you would use `@item().source_name`. Since we return a flat string array, `@item()` is the whole value.

---

**6. Publish all**

Click **Publish all**. ADF validates the pipeline first — check that `v_sources` variable is listed, all three activities are present, and the ForEach inner canvas shows `act_run_source`.

---

### Key Advantage Over Static Chain

Adding a 21st source = **INSERT one row into the control table, done.** The notebook reads all `is_active = true` rows fresh every run — no ADF Studio changes, no publish.

Disabling a source temporarily = `UPDATE ev_catalog.ctl.pipeline_control SET is_active = false WHERE source_name = 'db_states'` — the notebook excludes it automatically next run.

### Ordering Note

`ORDER BY source_name` in the notebook gives alphabetical order. If you need a specific execution order (e.g. reference tables before facts), add a `run_order` integer column to the control table and change the notebook query to `ORDER BY run_order`.

---

## Part 7 — Alternative Design: Parallel ForEach

> **Pipeline:** `pl_bronze_master_parallel` · **File:** `pipelines/pl_bronze_master_parallel.json`

Identical structure to Part 6 — same notebook, same SetVariable, same `@json(variables('v_sources'))` expression — but the ForEach runs up to 5 sources simultaneously instead of one at a time.

```
act_list_sources           (DatabricksNotebook — nb_list_active_sources)
      │
      ▼
act_set_sources            (SetVariable — v_sources = runOutput)
      │
      ▼
act_foreach_source_parallel (ForEach — isSequential=false, batchCount=5)
      └─ act_run_source     (ExecutePipeline — p_source_name = @item())
```

### Build Manually

Follow Part 6 Steps 1–5 exactly, with these two differences when configuring the ForEach:

- Pipeline name: `pl_bronze_master_parallel`
- ForEach activity name: `act_foreach_source_parallel`
- **Settings tab** on ForEach:
  - Sequential: **unchecked OFF**
  - Batch count: `5`

Everything else — the notebook, the variable, the `@json()` expression, the `@item()` in the child parameters — is identical.

### How Parallelism Works

`batchCount` controls how many iterations ADF starts at the same time. As each finishes, the next queued item starts immediately.

```
20 sources, batchCount=5

t=0   → sources 1–5  start simultaneously
t=5m  → sources 6–10  start (as 1–5 finish)
t=10m → sources 11–15 start
t=15m → sources 16–20 start
t=20m → all done

vs sequential: 20 × 5min = 100 min
vs parallel-5:  4 × 5min =  20 min  (4× faster)
```

Wall-clock ≈ `ceil(total_sources / batchCount) × slowest_single_source_time`

### batchCount — How to Size It

| Cluster type | Recommended batchCount | Reason |
|---|---|---|
| Single shared job cluster | `3–5` | Job cluster queues notebook submissions — too many at once causes start-up delays and timeouts |
| Interactive always-on cluster | `5–10` | More headroom, but watch for memory pressure if sources have large page counts |
| Separate job cluster per source | `10–20` | Each child spins its own cluster — expensive but fully isolated |
| ADF hard max | `50` | ADF will reject a ForEach batchCount above 50 |

> Start at `5`. After the first parallel run, open ADF Monitor → each child pipeline run → check `started_at` vs master start time. If child pipelines are starting 2–3 minutes after the master began, the cluster is queueing — reduce batchCount.

### Parallel Watermark Safety

Each source has its own row in `ev_catalog.ctl.pipeline_control`. Two sources running simultaneously cannot interfere because:
- `nb_read_control_table` filters `WHERE source_name = '<this_source>'` — reads only its own row
- `nb_update_watermark` updates `WHERE source_name = '<this_source>'` — writes only its own row
- Delta Lake provides row-level ACID transactions — concurrent `UPDATE` to different rows never conflicts

**Parallel execution is safe.** ✓

### Failure in Parallel Mode

If source D fails while A, B, C, E are still running:
- A, B, C, E continue to completion and their watermarks update normally
- D's watermark is NOT updated — next run retries D from its last successful watermark
- Remaining queued sources (F, G, H...) continue starting as slots free up
- After all iterations complete, the ForEach activity itself is marked **Failed** (because at least one iteration failed)
- Any activity placed after `act_foreach_source_parallel` on the main canvas will be skipped

---

## Part 8 — Comparing All Three Master Pipeline Designs

| | `pl_bronze_master` (static) | `pl_bronze_master_foreach` (dynamic sequential) | `pl_bronze_master_parallel` (dynamic parallel) |
|---|---|---|---|
| **Activities on canvas** | 19 | 3 | 3 |
| **New linked service / dataset needed** | No | No | No |
| **New notebook needed** | No | `nb_list_active_sources` | `nb_list_active_sources` (same) |
| **Execution order** | Hardcoded by activity chain | `ORDER BY` in notebook SQL | Uncontrolled within each batch |
| **Add new source** | INSERT + add activity + publish | INSERT one row only | INSERT one row only |
| **Disable a source** | Edit activity + publish | `SET is_active = false` | `SET is_active = false` |
| **Run time (20 sources × 5 min)** | ~100 min | ~100 min | ~20 min (batchCount=5) |
| **Failure isolation** | Source N fails → N+1..19 skipped | ForEach continues other items | ForEach continues other items |
| **Monitoring in ADF** | 19 named boxes — easy to read | 3 boxes — drill into ForEach to see iterations | Same |
| **Databricks cluster pressure** | 1 notebook job at a time | 1 notebook job at a time | Up to batchCount jobs at once |
| **Best for** | Classroom / explicit ordering | Production — easy to maintain | Production — large cluster, time-sensitive |

### Which One to Use

- **Learning / classroom:** `pl_bronze_master` — every source is a named box on the canvas, easy to see at a glance
- **Production (simple, low maintenance):** `pl_bronze_master_foreach` — add sources without ever opening ADF Studio again
- **Production (performance matters):** `pl_bronze_master_parallel` — when 20 sources × sequential is too slow and the Databricks cluster can handle concurrent jobs

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `nb_list_active_sources` returns empty list | No rows with `is_active = true` in control table | Run `control-table-setup.sql`, verify with `SELECT * FROM ev_catalog.ctl.pipeline_control` |
| ForEach runs but child pipeline not found | `pl_bronze_incremental_load_v2` not published | Publish the child pipeline before triggering the master |
| `String cannot be converted to Array` on ForEach | `v_sources` passed directly without `json()` | ForEach items must be `@json(variables('v_sources'))` not `variables('v_sources')` |
| `@item()` is an object, not a string | Notebook returned array of objects e.g. `[{"source_name":"payments"}]` | Change notebook to return flat array `["payments","fleet",...]` — or change parameter to `@item().source_name` |
| `act_list_sources` fails — notebook not found | Notebook path wrong or notebook not saved | Update the notebook path in the activity to match your Databricks email path |
| Notebook exits with no output | Exception before `dbutils.notebook.exit()` | Wrap query in try/except and always call exit, even on error — check Databricks notebook run logs |
| `act_read_ctl` fails in child pipeline — source not found | `source_name` in control table doesn't match `@item()` value from notebook | The notebook returns `source_name` column values verbatim — ensure control table `source_name` values match exactly |
| Parallel: Databricks jobs queuing | batchCount too high for cluster | Reduce batchCount — check child pipeline start times in Monitor |
| `404 Not Found` on API call | Wrong endpoint in control table | `UPDATE ev_catalog.ctl.pipeline_control SET endpoint='...' WHERE source_name='...'` |
| Static master pipeline times out | 19 sequential sources exceed 12h ADF timeout | Increase pipeline timeout in Settings, or switch to ForEach design |

---

## Day 6 Checklist

**Control table:**
- [ ] All rows inserted (run `control-table-setup.sql`)
- [ ] Verify with `SELECT source_name, endpoint, load_type, is_active FROM ev_catalog.ctl.pipeline_control ORDER BY source_name`
- [ ] Each row: correct `endpoint`, `sink_base_path`, `file_prefix`, `load_type`, `is_active=true`

**Notebook (for ForEach pipelines):**
- [ ] `nb_list_active_sources` created in Databricks
- [ ] Notebook tested manually — run it and check the exit value is a valid JSON array string
- [ ] Notebook path updated in `act_list_sources` to match your Databricks email

**`pl_bronze_master` (static chain):**
- [ ] 19 ExecutePipeline activities, linear chain, all green success arrows
- [ ] All activities: `waitOnCompletion=true`, correct `p_source_name` literal, `p_load_type_override` passed through
- [ ] Published

**`pl_bronze_master_foreach` (dynamic sequential):**
- [ ] Pipeline variable `v_sources` (String, default `[]`) added
- [ ] `act_list_sources`: linked service `ls_databricks`, notebook path correct, no base parameters
- [ ] `act_set_sources`: variable `v_sources` = `@activity('act_list_sources').output.runOutput`
- [ ] `act_foreach_source`: items = `@json(variables('v_sources'))`, Sequential = **ON**
- [ ] `act_run_source` inside ForEach: `p_source_name = @item()`, wait on completion = **ON**
- [ ] Published

**`pl_bronze_master_parallel` (dynamic parallel):**
- [ ] Same as foreach above except: ForEach Sequential = **OFF**, batchCount = `5`
- [ ] ForEach activity named `act_foreach_source_parallel`
- [ ] Published

**Testing:**
- [ ] Run `nb_list_active_sources` manually in Databricks — verify output is a JSON array of all active `source_name` values
- [ ] `pl_bronze_master` full load (`p_load_type_override="full"`) completes — all Bronze folders populated
- [ ] `pl_bronze_master_foreach` full load completes — same Bronze output, Monitor shows ForEach with N iterations
- [ ] `pl_bronze_master_parallel` full load completes — Monitor shows multiple child runs overlapping in time
- [ ] Incremental run (empty override) — incremental sources pick up 0 pages (watermark current), full sources reload
- [ ] Disable test: `UPDATE ev_catalog.ctl.pipeline_control SET is_active=false WHERE source_name='db_states'` → ForEach pipelines skip it; static master still runs it
