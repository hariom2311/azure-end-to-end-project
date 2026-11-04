# Day 14 — Delta Live Tables (DLT)

**Duration:** 2 hours | **Goal:** Understand Delta Live Tables, build a realistic DLT pipeline for the EV charging project, and understand how DLT differs from manual foreachBatch streaming.

> **Project context:** Days 13 built Bronze → Silver → Gold streaming using manual Structured Streaming with `foreachBatch`. Day 14 replaces that with Delta Live Tables — Databricks' managed pipeline framework that handles checkpointing, DQ tracking, lineage, and auto-scaling automatically.

---

## What We Built

```
day14-delta-live-tables/
├── notebooks/
│   └── dlt_ev_charging_pipeline.py   ← full DLT pipeline (Bronze → Silver → Gold)
├── notes.md
├── practice-exercises.md
├── interview-questions.md
└── interview-solutions.md
```

### Pipeline Tables

| Table | Layer | Type | Grain |
|---|---|---|---|
| `bronze_vehicle_battery_events` | Bronze | Streaming table | One row per raw Event Hub event |
| `silver_vehicle_battery_events` | Silver | Streaming table | One row per validated event (bad rows dropped) |
| `gold_charging_progress_live` | Gold | Streaming table | One row per vehicle + station + 5-min window |
| `gold_daily_station_energy` | Gold | Batch table | One row per station + day |
| `gold_overtemp_alerts_24h` | Gold | View | Vehicles with overtemp in last 24h |

---

## What is Delta Live Tables?

DLT is a declarative pipeline framework built on top of Spark Structured Streaming and Delta Lake. Instead of writing explicit `readStream` / `writeStream` / `foreachBatch` code, you declare **what** each table should contain using `@dlt.table` decorators — Databricks manages **how** it runs.

```python
# Without DLT — manual streaming (Day 13 approach)
bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)
silver_query = (
    bronze_stream
    .writeStream
    .foreachBatch(transform_to_silver)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="60 seconds")
    .start()
)

# With DLT — declarative
@dlt.table(name="silver_vehicle_battery_events")
def silver_vehicle_battery_events():
    return dlt.read_stream("bronze_vehicle_battery_events").transform(...)
```

DLT handles: checkpointing, retries, cluster lifecycle, schema evolution, event logging, lineage graph — automatically.

---

## Core DLT Concepts

### 1. Tables vs Views

| Type | Created with | Materialised? | Use case |
|---|---|---|---|
| Streaming table | `@dlt.table` + `dlt.read_stream()` | Yes — Delta table | Continuous ingest, append-mostly |
| Materialized view | `@dlt.table` + `dlt.read()` | Yes — Delta table | Batch aggregations refreshed per trigger |
| View | `@dlt.view` | No — computed on read | Lightweight transforms, alert filters |

### 2. `dlt.read_stream()` vs `dlt.read()`

| | `dlt.read_stream()` | `dlt.read()` |
|---|---|---|
| Processing | Incremental — only new records since last checkpoint | Full snapshot — entire table every run |
| Output table type | Streaming table | Materialized view / batch table |
| Use when | Source is a stream (Event Hub, Kafka, Auto Loader) | Source is a stable Delta table, batch aggregate |

In this project:
- `bronze_vehicle_battery_events` uses `readStream` from ADLS path (new events each batch)
- `silver_vehicle_battery_events` uses `dlt.read_stream("bronze_")` (process new Bronze rows)
- `gold_charging_progress_live` uses `dlt.read_stream("silver_")` (window aggregation on stream)
- `gold_daily_station_energy` uses `dlt.read("silver_")` (full snapshot for daily totals)

### 3. Expectations (Data Quality)

DLT expectations replace manual if/else DQ filtering with declarative rules:

```python
@dlt.expect_or_drop("valid_battery_pct", "battery_pct BETWEEN 0 AND 100")
@dlt.table(name="silver_vehicle_battery_events")
def silver():
    return dlt.read_stream("bronze_vehicle_battery_events")
```

Three violation modes:

| Decorator | On violation | Bad rows |
|---|---|---|
| `@dlt.expect` | Warn — metric tracked, row kept | Rows pass through; violation logged |
| `@dlt.expect_or_drop` | Drop — row removed from output | Dropped; count tracked in event log |
| `@dlt.expect_or_fail` | Fail — entire pipeline batch stops | Pipeline stops; must fix data or rule |

DLT tracks pass/fail counts per rule per batch in the **pipeline event log** — visible in the DLT UI without writing any logging code.

**In this project:** Rules R01–R08 use `expect_or_drop` (invalid events are dropped). The `not_corrupt` rule uses `expect` (keep corrupt rows in Silver for analysis but flag them).

### 4. Pipeline Modes

| Mode | Behaviour | Use case |
|---|---|---|
| **Triggered** | Runs once, processes all pending data, stops cluster | Daily batch, cost-sensitive workloads |
| **Continuous** | Cluster stays up, processes events as they arrive | Near-real-time streaming (latency < 1 min) |

This project uses **Triggered** mode with an hourly schedule — the cluster starts, processes all events since the last run, writes Gold tables, then stops. This keeps cluster costs predictable.

### 5. Auto Loader (Cloud Files) vs Delta Source

| Source | When to use |
|---|---|
| `format("cloudFiles")` — Auto Loader | New files landing in ADLS (CSV, JSON, Parquet) |
| `format("delta")` — Delta source | Reading from an existing Delta table (like Bronze) |
| Kafka / Event Hub direct | Real-time streaming with sub-second latency |

In this project, Bronze is already a Delta table written by the Day 13 Event Hub consumer. The DLT Silver table reads from Bronze Delta directly — no need for Auto Loader.

### 6. DLT vs Manual Structured Streaming (Day 13 comparison)

| Aspect | Manual (Day 13) | Delta Live Tables (Day 14) |
|---|---|---|
| Checkpointing | Manual — must specify path per query | Automatic — DLT manages per table |
| DQ enforcement | Manual if/else + quarantine writes | Declarative `@dlt.expect` decorators |
| DQ metrics | Manual logging / print statements | Built-in event log + pipeline UI |
| Lineage | None — invisible to Databricks | Auto-generated DAG in DLT UI |
| Retries | Manual try/except | Automatic with configurable retry policy |
| Schema evolution | Manual — must handle new columns | `schema_evolution = "addNewColumns"` |
| Cluster management | Manual — start/stop Spark application | Fully managed by DLT runtime |
| Multi-table orchestration | Manual dependencies / separate notebooks | Declared via `dlt.read()` — DLT infers DAG |

---

## Pipeline Architecture for This Project

```
Event Hub (vehicle-battery-live)
        │
        │  Day 13 consumer writes raw events
        ▼
Bronze Delta path (ADLS)
  abfss://bronze@evdatalakedev2.../event-stream/vehicle_battery_live/
        │
        │  DLT reads stream
        ▼
┌─────────────────────────────────────────────────────┐
│          DLT Pipeline: dlt_ev_charging_pipeline      │
│                                                     │
│  bronze_vehicle_battery_events (streaming table)    │
│    Raw events — no transforms, no DQ                │
│          │                                          │
│  silver_vehicle_battery_events (streaming table)    │
│    9 DQ expectations (expect_or_drop)               │
│    + event_ts parsed, battery_pct rounded           │
│    + session_date derived for partitioning          │
│          │                                          │
│          ├── gold_charging_progress_live (streaming)│
│          │     5-min tumbling window per vehicle    │
│          │     overtemp_flag, eta, event_count      │
│          │          │                               │
│          │   gold_overtemp_alerts_24h (view)        │
│          │     Vehicles with temp > 45°C today      │
│          │                                          │
│          └── gold_daily_station_energy (batch)      │
│                kWh delivered per station per day    │
└─────────────────────────────────────────────────────┘
```

---

## Step-by-Step: Create the DLT Pipeline in Databricks UI

### Step 1 — Upload the Notebook
1. Databricks UI → Workspace → navigate to `/Shared/ev-intelligence/`
2. Import → `dlt_ev_charging_pipeline.py`

### Step 2 — Create the Pipeline
1. Databricks UI → Delta Live Tables → Create Pipeline
2. Fill in:
   - **Pipeline name:** `dlt-ev-charging-pipeline`
   - **Product edition:** Advanced (required for expectations + change data feed)
   - **Pipeline mode:** Triggered
   - **Source code:** `/Shared/ev-intelligence/dlt_ev_charging_pipeline`
   - **Target schema:** `ev_catalog.dlt_ev_charging`
   - **Storage location:** `abfss://pipeline@evdatalakedev2.dfs.core.windows.net/dlt/ev-charging/`

3. Under **Configuration**, add:
   ```
   ev.adls_account   = evdatalakedev2
   ev.eh_namespace   = evh-ev-intelligence-dev
   ev.eh_topic       = vehicle-battery-live
   ```

4. Under **Cluster**, select:
   - Cluster policy: `ev-job-cluster-policy`
   - Min workers: 1, Max workers: 4 (auto-scaling)
   - Node type: `Standard_DS3_v2`

### Step 3 — Run the Pipeline
1. Click **Start** → Triggered run begins
2. Watch the DAG form in the pipeline UI — tables appear as nodes
3. Click any table node to see: row counts, expectation pass/fail counts, duration

### Step 4 — Schedule the Pipeline
1. Pipeline settings → Schedule → Add schedule
2. Cron: `0 * * * *` (every hour)
3. This triggers the pipeline at the top of every hour — cluster starts, runs, stops

### Step 5 — Query the Output Tables
```sql
-- Silver: validated events
SELECT * FROM ev_catalog.dlt_ev_charging.silver_vehicle_battery_events
ORDER BY sl_ingested_at DESC LIMIT 20;

-- Gold: charging windows
SELECT vehicle_id, station_id, window_start, battery_pct_start, battery_pct_end,
       overtemp_flag, eta_minutes_to_full
FROM ev_catalog.dlt_ev_charging.gold_charging_progress_live
ORDER BY window_start DESC LIMIT 20;

-- Gold: daily energy
SELECT station_id, session_date, total_kwh_delivered, unique_vehicles
FROM ev_catalog.dlt_ev_charging.gold_daily_station_energy
ORDER BY session_date DESC, total_kwh_delivered DESC;

-- Alert view: overtemp events today
SELECT * FROM ev_catalog.dlt_ev_charging.gold_overtemp_alerts_24h;
```

### Step 6 — View DQ Metrics
1. DLT UI → pipeline run → click `silver_vehicle_battery_events` node
2. Right panel shows: rows passed, rows dropped, per-expectation counts
3. Event log table: `SELECT * FROM ev_catalog.dlt_ev_charging.event_log`

---

## DLT Pipeline Configuration Reference

```json
{
  "name": "dlt-ev-charging-pipeline",
  "edition": "ADVANCED",
  "continuous": false,
  "target": "ev_catalog.dlt_ev_charging",
  "libraries": [
    { "notebook": { "path": "/Shared/ev-intelligence/dlt_ev_charging_pipeline" } }
  ],
  "clusters": [
    {
      "label": "default",
      "autoscale": { "min_workers": 1, "max_workers": 4 },
      "node_type_id": "Standard_DS3_v2"
    }
  ],
  "configuration": {
    "ev.adls_account": "evdatalakedev2",
    "ev.eh_namespace": "evh-ev-intelligence-dev",
    "ev.eh_topic":     "vehicle-battery-live"
  }
}
```

---

## Key Commands

```bash
# Deploy DLT pipeline via Databricks CLI (DAB)
# Add to databricks/resources/pipelines.yml:
#   resources:
#     pipelines:
#       dlt_ev_charging:
#         name: dlt-ev-charging-pipeline
#         target: ev_catalog.dlt_ev_charging
#         libraries:
#           - notebook:
#               path: ../../day14-delta-live-tables/notebooks/dlt_ev_charging_pipeline
# Then:
databricks bundle deploy --target staging

# Trigger a pipeline run via CLI
databricks pipelines start --pipeline-id <pipeline-id>

# Get pipeline status
databricks pipelines get --pipeline-id <pipeline-id>

# View DQ event log in SQL
# SELECT * FROM ev_catalog.dlt_ev_charging.event_log ORDER BY timestamp DESC LIMIT 50;
```
