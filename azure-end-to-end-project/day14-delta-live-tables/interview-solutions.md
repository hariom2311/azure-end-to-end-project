# Day 14 — Interview Solutions: Delta Live Tables

---

## DLT Fundamentals

**A1**
Delta Live Tables is Databricks' declarative pipeline framework for building reliable, maintainable, and observable data pipelines on top of Spark Structured Streaming and Delta Lake.

**Problem it solves vs manual Structured Streaming:**

| Manual Structured Streaming | Delta Live Tables |
|---|---|
| You write `readStream` / `writeStream` / `foreachBatch` for every table | You declare `@dlt.table` functions — DLT generates the streaming code |
| You manage checkpoint paths manually — one per query, easy to misconfigure | Checkpoints are automatic per table |
| DQ is manual if/else + quarantine writes — not tracked centrally | `@dlt.expect` decorators — pass/fail counts tracked in event log automatically |
| No lineage — Databricks cannot see dependencies between tables | DLT infers a DAG from `dlt.read_stream()` calls and displays it in the UI |
| Retries require custom try/except logic | Automatic retries with configurable policy |
| Schema evolution requires manual column handling | `schema_evolution = "addNewColumns"` configuration |
| Multi-table orchestration requires separate notebooks or jobs | All tables in one pipeline — DLT handles execution order |

In short: DLT moves you from writing pipeline mechanics to writing pipeline logic.

---

**A2**

| Type | Materialized? | Created with | Use case |
|---|---|---|---|
| **Streaming table** | Yes — written to Delta on disk | `@dlt.table` + `dlt.read_stream()` | Continuous ingest of new events; each run appends only new data |
| **Materialized view** | Yes — written to Delta on disk | `@dlt.table` + `dlt.read()` | Batch aggregations that need to be queried repeatedly without recomputing each time |
| **View** | No — recomputed on every query | `@dlt.view` | Lightweight filters or transforms consumed only by other DLT tables — not exposed directly to end users |

**Examples from this project:**
- **Streaming table:** `silver_vehicle_battery_events` — new Bronze events are processed incrementally each run; the Silver table grows append-only
- **Materialized view:** `gold_daily_station_energy` — full snapshot aggregation of Silver events into daily kWh totals; refreshed on each pipeline trigger
- **View:** `gold_overtemp_alerts_24h` — filters `gold_charging_progress_live` for the last 24 hours; computed on query, not stored; used by the alert dashboard

---

**A3**

`dlt.read_stream(table_name)`:
- Reads only **new records** that have arrived since the last checkpoint
- Used when the upstream is a DLT streaming table or external Delta table that grows over time
- Output: a streaming DataFrame — the downstream table must be a streaming table
- Example: `silver_vehicle_battery_events` reads from `bronze_vehicle_battery_events` using `read_stream` — only processes Bronze rows that arrived after the last Silver checkpoint

`dlt.read(table_name)`:
- Reads a **complete snapshot** of the upstream table on every pipeline run
- Used when you need the full table (e.g. a dimension lookup, or a batch aggregation over all history)
- Output: a static DataFrame — the downstream table is a materialized view (batch)
- Example: `gold_daily_station_energy` uses `dlt.read("silver_vehicle_battery_events")` — it aggregates all Silver events by day, so it needs the full table every time

**Rule of thumb:** use `read_stream` when you only want to process what's new; use `read` when you need to see everything.

---

## Expectations and Data Quality

**A4**
Three violation modes:

| Decorator | On violation | Bad row fate | Pipeline fate |
|---|---|---|---|
| `@dlt.expect("rule", "condition")` | Warn | Kept in output table | Continues normally |
| `@dlt.expect_or_drop("rule", "condition")` | Drop | Removed from output; count tracked in event log | Continues normally |
| `@dlt.expect_or_fail("rule", "condition")` | Fail | Not written | Entire pipeline batch stops and enters FAILED state |

**When to use each:**
- `expect`: monitoring rules — you want visibility but the data is still useful (e.g. unusual but not invalid values)
- `expect_or_drop`: strict DQ gates — invalid data must not reach Silver (e.g. null keys, out-of-range battery pct)
- `expect_or_fail`: critical invariants that indicate data corruption — if violated, human review is required before the pipeline runs again (e.g. duplicate primary keys in a reference dimension)

---

**A5**
`not_corrupt` checks `_rescue_data IS NULL`. The `_rescue_data` column is a Databricks schema-on-read field that holds the raw JSON for any fields that could not be parsed into the declared schema.

**Why `expect_or_drop` is wrong here:**
- A row with `_rescue_data IS NOT NULL` means some fields were unparseable, but the core fields (`event_id`, `vehicle_id`, `battery_pct`, etc.) may still be valid
- Dropping the row entirely loses potentially recoverable data
- `_rescue_data` is not NULL when new fields are added by the producer that the Silver schema does not know about yet — these rows are still valid, just unrecognized fields

**Why `expect` is correct:**
- Keep the row in Silver (the core fields are valid and pass the other 8 rules)
- Track the count of corrupt rows in the event log — the pipeline team investigates separately
- Engineers can query Silver to find rows where `_rescue_data IS NOT NULL` and decide whether to update the schema or quarantine those rows

---

**A6**
Three explanations for fewer Silver rows than Bronze without any expectation violations:

1. **Deduplication by DLT** — DLT's streaming tables use the `event_id` as a natural dedup key when merge mode is configured. If the same `event_id` arrives in multiple Bronze batches (Event Hub replay), Silver counts one row but Bronze counts two.

2. **Windowed aggregation** — if the Silver table is an aggregation (e.g. a windowed group-by), many Bronze input rows produce fewer output rows. In this project `gold_charging_progress_live` has far fewer rows than Silver because it aggregates 10 vehicle events into one 5-minute window row.

3. **NULL rows filtered by a `.filter()` or `.where()` in the table function** — if the DLT table function has an explicit filter condition (e.g. `.filter(F.col("session_date") >= "2026-01-01")`) that is not declared as an expectation, rows outside the filter are simply excluded without being counted as violations.

---

## Pipeline Modes and Performance

**A7**
**Triggered mode:** the pipeline cluster starts, processes all pending data since the last run, writes the Delta tables, then the cluster shuts down. Cluster cost is proportional to data volume and run duration.

**Continuous mode:** the cluster stays running permanently, processing events as they arrive with sub-second to seconds latency. Cluster cost is constant regardless of data volume.

**For this EV charging pipeline: Triggered mode (hourly schedule).**

Reasons:
- Vehicle battery telemetry does not require sub-second freshness — 5-minute window Gold tables updated hourly are acceptable for the alert dashboard
- Triggered mode is significantly cheaper — cluster runs ~15 minutes per hour vs 60 minutes in Continuous mode
- The overtemperature alert view queries the existing Gold table directly — it does not need Continuous mode to surface alerts
- If near-real-time (< 1 minute latency) were required, Continuous mode would be chosen and costs would be justified

---

**A8**
DLT handles overlapping scheduled runs with **pipeline-level concurrency control**. When the 10:00 trigger fires and the 09:00 run is still active:

- In **Triggered mode**: DLT queues the 10:00 run. It will start automatically as soon as the 09:00 run completes. The 10:00 run will then process all events that arrived between the 09:00 run's checkpoint and 10:00 — it catches up without missing data.
- Runs do not run in parallel for the same pipeline — DLT prevents concurrent runs on the same pipeline to avoid checkpoint conflicts.

This is handled automatically by DLT — with manual Structured Streaming, you would need to implement your own run-locking or idempotency logic to handle this case safely.

---

## DLT vs Manual Streaming

**A9**
Comparison of Day 13 `foreachBatch` Silver vs Day 14 DLT Silver:

| Dimension | Day 13 — foreachBatch | Day 14 — DLT |
|---|---|---|
| **Checkpointing** | Manual: `.option("checkpointLocation", CHECKPOINT_PATH)` — engineer manages the path, risk of path collision or deletion | Automatic: DLT manages a checkpoint per table inside the pipeline storage location — no path management needed |
| **DQ enforcement** | Manual: 9 `withColumn` boolean flags, `F.concat_ws` reason string, explicit `.filter()` to split clean vs bad, separate `.write()` to quarantine | Declarative: 9 `@dlt.expect_or_drop` decorators — 9 lines instead of 50+ lines; counts tracked automatically in event log |
| **Lineage** | None: Databricks UI shows no relationship between Bronze and Silver notebooks | Automatic: DLT builds a DAG from `dlt.read_stream("bronze_")` — visible in pipeline UI with clickable table nodes |
| **Retries** | None: if the cluster crashes mid-batch, the job fails and requires manual restart | Automatic: DLT retries failed micro-batches with configurable backoff before marking the pipeline FAILED |
| **Schema evolution** | Manual: new fields in the Event Hub payload cause schema mismatch errors — engineer must update the declared schema | Configurable: `schema_evolution = "addNewColumns"` automatically adds new columns to the Delta table without breaking existing reads |

---

**A10**
Three things DLT provides that manual Structured Streaming cannot easily replicate:

1. **Built-in DQ event log and UI** — DLT tracks every expectation pass/fail count per batch per table in a queryable event log (`SELECT * FROM <pipeline_schema>.event_log`). With manual Structured Streaming you would have to implement this yourself: add a custom metrics counter, write it to a log table, build a dashboard on top. DLT provides this out of the box with zero extra code.

2. **Automatic DAG and lineage** — DLT infers the dependency graph from `dlt.read()` and `dlt.read_stream()` calls and renders it as an interactive DAG in the UI. You can click any table node to see row counts, expectation metrics, run duration, and schema. With manual Structured Streaming, the dependencies between notebooks are invisible to Databricks — they exist only in your head or documentation.

3. **Managed pipeline lifecycle** — DLT manages cluster startup/shutdown, checkpoint consistency across all tables, ordered execution of dependent tables, and recovery from partial failures. With manual Structured Streaming, if you have three notebooks (Bronze, Silver, Gold) each with their own streaming queries, you need a separate orchestrator (ADF, Databricks Jobs with task dependencies) to start them in order, monitor each, and handle failures. DLT collapses all of this into a single pipeline object.

---

## Project-Specific Questions

**A11**
Current 5-minute tumbling window:
```python
F.window(F.col("event_ts_ts"), "5 minutes")
# Produces: [10:00–10:05], [10:05–10:10], [10:10–10:15], ...
```

5-minute sliding window (new window every 1 minute):
```python
F.window(F.col("event_ts_ts"), "5 minutes", "1 minute")
# Produces: [10:00–10:05], [10:01–10:06], [10:02–10:07], ...
```

**Cost implications:**

- **Storage:** Each event now appears in 5 windows instead of 1 — storage grows 5×. For 10 vehicles × 10 events/second × 60 seconds = 6,000 events/minute; with tumbling windows that is 12 output rows/minute; with sliding windows it is 60 output rows/minute (5× more).

- **Compute:** Spark must maintain 5 open windows per vehicle at all times instead of 1. Shuffle and state store size grows proportionally. Expect ~3–5× higher CPU and memory usage on the streaming cluster.

- **Recommended approach:** Only use sliding windows if the product requirement genuinely needs 1-minute freshness with 5-minute context. For the EV alert dashboard, a tumbling window refreshed every 5 minutes is almost always sufficient and far cheaper.

---

**A12**
**Problem with `dlt.read()` for a 30-minute window:**

`dlt.read()` returns a **full snapshot** of the Silver table every time the pipeline runs. If you apply a `.filter(F.col("event_ts_ts") >= F.date_sub(F.current_timestamp(), ...))` inside the `dlt.read()` call, it works in a single run. But the resulting table is a **materialized view** of the filtered snapshot — it is rewritten in full on every pipeline trigger and does not behave as a streaming window. It also does not handle late-arriving data correctly because `current_timestamp()` at the time of the pipeline trigger is the reference point, not the event time.

**Correct fix — switch to `dlt.read_stream()` with a watermark:**

```python
@dlt.table(name="gold_last_30min_station_summary")
def gold_last_30min_station_summary():
    return (
        dlt.read_stream("silver_vehicle_battery_events")
        .withWatermark("event_ts_ts", "30 minutes")   # drop state older than 30 min
        .groupBy(
            F.col("station_id"),
            F.window(F.col("event_ts_ts"), "30 minutes", "5 minutes"),  # sliding
        )
        .agg(F.count("*").alias("event_count"), ...)
    )
```

The **watermark** tells Spark to discard state for events older than 30 minutes from the latest event time seen — this bounds memory usage and correctly defines the "last 30 minutes" relative to the data clock, not the wall clock. This cannot be expressed with `dlt.read()` (batch snapshot) — it requires `dlt.read_stream()` with watermarking.
