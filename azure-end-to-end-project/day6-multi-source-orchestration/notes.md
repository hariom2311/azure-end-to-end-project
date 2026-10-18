# Day 6 — Multi-Source Orchestration: Fleet, Weather + Master Pipeline

**Duration:** 1.5 hours | **Goal:** Extend the Bronze layer to ingest all three VoltGrid data sources — payments (existing), fleet (new), and weather (new) — using a single master pipeline that calls the Day 5 child pipeline sequentially for each source.

> **Prerequisites:** Day 5 complete — `pl_bronze_incremental_load_v2` deployed and tested. Day 6 adds new control table rows and a new `pl_bronze_master` pipeline that calls it.

---

## BEFORE YOU START — Student Customization Guide

No new linked services or datasets are needed. Day 6 reuses everything from Day 4/5.

| Where | What to Check | Instructor's Value |
|---|---|---|
| Control table catalog path | Your Unity Catalog name | `ev_catalog.ctl.pipeline_control` |
| `pl_bronze_master` → each `ExecutePipeline` | Child pipeline name must match exactly | `pl_bronze_incremental_load_v2` |
| ADF Pipeline folder | Optional — instructor uses `bronze` folder | `bronze` |
| ADF MI Application ID (for new GRANTs if needed) | Your ADF MI App ID | `9245da2f-7129-4e0c-92c4-134fe896e294` |

---

## Overview: What Day 6 Adds

```
Day 4 / 5 (payments only):
  pl_bronze_incremental_load_v2 → runs for payments → writes Bronze

Day 6 (all sources):
  pl_bronze_master
    ├─ act_run_payments  → pl_bronze_incremental_load_v2 (p_source_name='payments')
    ├─ act_run_fleet     → pl_bronze_incremental_load_v2 (p_source_name='fleet')    [NEW]
    └─ act_run_weather   → pl_bronze_incremental_load_v2 (p_source_name='weather')  [NEW]

Control table additions:
  INSERT fleet row   → endpoint=/api/fleet/,   load_type=full
  INSERT weather row → endpoint=/api/weather/, load_type=full
```

The child pipeline `pl_bronze_incremental_load_v2` is **unchanged**. It reads all config from the control table using `p_source_name` as the lookup key. Adding fleet and weather sources is purely a data operation (INSERT) plus a new orchestrator pipeline.

---

## Part 1 — Register New Sources in the Control Table

### 1.1 API Endpoints Overview

Three data sources are available from the VoltGrid API:

| Source | Endpoint | Primary Key | Notes |
|---|---|---|---|
| Payments | `/api/db/payments/` | `payment_id` | Incremental — supports `updated_after` filter |
| Fleet | `/api/fleet/` | `vehicle_id` | Full load — no `updated_after` support |
| Weather | `/api/weather/` | `city_id` | Full load — no `updated_after` support |

All three support `load` (1–5, controls dataset scenario), `page`, and `page_size` query parameters.

> **Fleet and weather are full loads** because the API does not expose an `updated_after` filter for them. Every run fetches all data. The watermark is still written to the control table (keeps the schema consistent) but is not used in the API call.

### 1.2 Run the INSERT Statements

Open Databricks → SQL editor (or a SQL cell in a notebook):

```sql
-- Fleet source
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('fleet', '/api/fleet/', 'api/fleet', 'fleet', 1000, 'full', '1900-01-01T00:00:00Z', true);

-- Weather source
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('weather', '/api/weather/', 'api/weather', 'weather', 1000, 'full', '1900-01-01T00:00:00Z', true);
```

Verify all three rows exist:
```sql
SELECT source_name, endpoint, sink_base_path, load_type, last_watermark, is_active
FROM ev_catalog.ctl.pipeline_control
ORDER BY source_name;
```

Expected result:

| source_name | endpoint | sink_base_path | load_type | is_active |
|---|---|---|---|---|
| fleet | `/api/fleet/` | `api/fleet` | full | true |
| payments | `/api/db/payments/` | `api/payments` | incremental | true |
| weather | `/api/weather/` | `api/weather` | full | true |

### 1.3 Verify ADF MI Access (if needed)

The GRANT statements from Day 4 already cover the control table for all rows — no additional grants are needed. If you recreated the table:

```sql
GRANT USAGE ON CATALOG ev_catalog TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT USAGE ON SCHEMA ev_catalog.ctl TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.ctl.pipeline_control TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
```

---

## Part 2 — Create the Master Pipeline

### 2.1 What Is a Master Pipeline?

`pl_bronze_master` is an **orchestrator** — it has no data movement activities. It only contains `ExecutePipeline` activities that call `pl_bronze_incremental_load_v2` with different parameters.

This pattern is called **pipeline chaining** and is the standard ADF approach for running multiple sources one after another in a controlled sequence.

### 2.2 Import from JSON

Go to ADF Studio → **Author** → Pipelines → New pipeline from template (or use the `...` menu → Import from JSON) → upload `pipelines/pl_bronze_master.json`.

Alternatively, create manually:
1. Create new pipeline named `pl_bronze_master`
2. Add 3 `Execute Pipeline` activities (from Activity toolbar → General)
3. Name them `act_run_payments`, `act_run_fleet`, `act_run_weather`
4. Configure each (see Section 2.3)
5. Connect them: `act_run_payments` → `act_run_fleet` → `act_run_weather` (drag success arrow)

### 2.3 Configure Each Execute Pipeline Activity

Each activity has the same configuration pattern — only `p_source_name` differs.

**Settings tab:**
- Invoked pipeline: `pl_bronze_incremental_load_v2`
- Wait on completion: **Yes** (critical — ensures sequential execution)

**Parameters tab:**

| Parameter | Value (Expression) |
|---|---|
| `p_source_name` | `payments` / `fleet` / `weather` (literal string — no expression needed) |
| `p_load_type_override` | `@pipeline().parameters.p_load_type_override` |

> **Why pass `p_load_type_override` from parent?** This lets you trigger `pl_bronze_master` with `p_load_type_override="full"` to force a full reload of all sources (useful for initial load or data repairs) without touching individual pipelines.

### 2.4 Add the Master Pipeline Parameter

In `pl_bronze_master`, go to **Parameters** tab → add:

| Name | Type | Default value |
|---|---|---|
| `p_load_type_override` | String | *(empty)* |

### 2.5 Publish

Click **Publish all**. Both `pl_bronze_master` and (if not already published) `pl_bronze_incremental_load_v2` must be published before the first run.

---

## Part 3 — Running the Master Pipeline

### 3.1 First Run — Full Load of All Sources

**Trigger now** → Parameters:
- `p_load_type_override`: `full`

This forces all three sources to use `load_type=full`, ignoring whatever is in the control table. Useful for the first run when the Bronze container is empty.

Expected run time: depends on number of pages for each source. Monitor in ADF Studio → **Monitor** → Pipeline runs.

### 3.2 Subsequent Runs — Incremental (Payments) + Full (Fleet/Weather)

**Trigger now** → Parameters:
- `p_load_type_override`: *(leave empty)*

Each source uses its own `load_type` from the control table:
- **Payments** → incremental (fetches records updated since last watermark)
- **Fleet** → full (fetches all fleet records every run)
- **Weather** → full (fetches all weather records every run)

### 3.3 Monitoring

In ADF Monitor → Pipeline runs, find `pl_bronze_master`. Click the pipeline name to see child runs — each `ExecutePipeline` activity shows as a link to the child pipeline run. Click through to see individual activity status inside the child.

---

## Part 4 — Bronze Output Structure After Day 6

```
ADLS bronze container
└── api/
    ├── payments/
    │   └── ingestion_date=2026-10-18/
    │       ├── payments_page_1.json
    │       └── payments_page_N.json
    ├── fleet/
    │   └── ingestion_date=2026-10-18/
    │       ├── fleet_page_1.json
    │       └── fleet_page_N.json
    └── weather/
        └── ingestion_date=2026-10-18/
            ├── weather_page_1.json
            └── weather_page_N.json
```

---

## Part 5 — How the Child Pipeline Handles Fleet and Weather

The child pipeline `pl_bronze_incremental_load_v2` was designed to be generic — it reads all config from the control table and uses `p_source_name` as the key. This is how each step behaves for fleet and weather:

| Step | Payments | Fleet / Weather |
|---|---|---|
| `act_read_ctl` | Reads payments row | Reads fleet/weather row — different endpoint + path |
| `v_endpoint` | `/api/db/payments/` | `/api/fleet/` or `/api/weather/` |
| `v_sink_base_path` | `api/payments` | `api/fleet` or `api/weather` |
| `act_set_watermark` | Uses `last_watermark` from control table | Same — but `load_type='full'` sets watermark to `1900-01-01` |
| API call `?updated_after=...` | Used for incremental filter | `1900-01-01` = fetches all records (same as full load) |
| Watermark update | Writes current run timestamp | Writes current run timestamp (but next run ignores it for full load) |

> For fleet and weather, `load_type = 'full'` in the control table means the Databricks notebook sets `v_last_watermark = '1900-01-01T00:00:00Z'` regardless of the stored watermark — the API call always fetches all records.

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| Child pipeline not found | `pl_bronze_incremental_load_v2` not published yet | Publish child pipeline before running master |
| `act_read_ctl` fails for fleet/weather | Control table row not inserted | Run the INSERT statements in Section 1.2 and verify with SELECT |
| `404 Not Found` on fleet or weather | Wrong endpoint in control table (e.g. `/api/fleet` missing trailing slash) | UPDATE the control table row: `UPDATE ev_catalog.ctl.pipeline_control SET endpoint='/api/fleet/' WHERE source_name='fleet'` |
| Master pipeline fails on `act_run_fleet` with `act_run_payments` still succeeded | Check child pipeline logs for fleet — likely a config or auth issue | Open Monitor → act_run_fleet → view child run |
| All sources ran but no files in Bronze | Check `is_active = true` for each source in control table | `SELECT source_name, is_active FROM ev_catalog.ctl.pipeline_control` |

---

## Day 6 Checklist

**Control table:**
- [ ] Fleet row inserted with `endpoint='/api/fleet/'`, `load_type='full'`
- [ ] Weather row inserted with `endpoint='/api/weather/'`, `load_type='full'`
- [ ] All three rows visible in `SELECT * FROM ev_catalog.ctl.pipeline_control`

**Master pipeline:**
- [ ] `pl_bronze_master` created with 3 Execute Pipeline activities
- [ ] `act_run_fleet` depends on `act_run_payments` (success)
- [ ] `act_run_weather` depends on `act_run_fleet` (success)
- [ ] `waitOnCompletion: true` on all three activities
- [ ] `p_load_type_override` parameter added to master pipeline
- [ ] Published successfully

**Testing:**
- [ ] Full load run (`p_load_type_override="full"`) completes — all three sources show files in Bronze
- [ ] Check fleet Bronze: `bronze/api/fleet/ingestion_date=<today>/fleet_page_1.json` exists
- [ ] Check weather Bronze: `bronze/api/weather/ingestion_date=<today>/weather_page_1.json` exists
- [ ] Incremental run (empty override) completes — payments picks up only new records; fleet/weather do full load again

---

## Architecture Summary

```
Trigger / Schedule
       │
       ▼
pl_bronze_master   ← orchestrator (no data movement)
  p_load_type_override=""
       │
       ├─ act_run_payments ─────────────────────┐
       │    pl_bronze_incremental_load_v2        │
       │    p_source_name="payments"             │
       │                                         ▼
       │                              ev_catalog.ctl.pipeline_control
       │                              row: payments, incremental, <watermark>
       │
       ├─ act_run_fleet (after payments) ───────┐
       │    pl_bronze_incremental_load_v2        │
       │    p_source_name="fleet"                │
       │                                         ▼
       │                              row: fleet, full, 1900-01-01
       │
       └─ act_run_weather (after fleet) ────────┐
            pl_bronze_incremental_load_v2        │
            p_source_name="weather"              │
                                                 ▼
                                      row: weather, full, 1900-01-01
```
