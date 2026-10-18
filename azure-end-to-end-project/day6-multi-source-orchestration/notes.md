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

## Part 2 — Import and Configure the Master Pipeline

### 2.1 Import from JSON

ADF Studio → Author → Pipelines → `...` menu → Import from JSON → upload `pipelines/pl_bronze_master.json`.

Or create manually: add 19 `Execute Pipeline` activities in sequence, each calling `pl_bronze_incremental_load_v2`.

### 2.2 Pipeline Structure

The master pipeline has:
- **1 parameter:** `p_load_type_override` (string, default `""`)
- **19 activities** — all type `ExecutePipeline`
- **waitOnCompletion = true** on every activity (this is what makes them sequential)
- Each activity depends on the previous one with `dependencyConditions: ["Succeeded"]`

### 2.3 Verify the Chain

In ADF Studio after import, the pipeline canvas should show a linear chain of 19 boxes connected by green arrows (success arrows). If any box shows without an arrow connecting it to the next, check the `dependsOn` settings.

### 2.4 Publish

Publish all. Both the master pipeline and `pl_bronze_incremental_load_v2` must be in a published state before triggering.

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

1. **INSERT one row** into the control table:
   ```sql
   INSERT INTO ev_catalog.ctl.pipeline_control
       (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
   VALUES
       ('my_source', '/api/db/my-source/', 'api/db/my_source', 'my_source', 1000, 'incremental', '1900-01-01T00:00:00Z', true);
   ```

2. **Add one ExecutePipeline activity** to `pl_bronze_master` — depends on the last existing activity:
   ```json
   {
       "name": "act_run_my_source",
       "type": "ExecutePipeline",
       "dependsOn": [{ "activity": "act_run_db_stations", "dependencyConditions": ["Succeeded"] }],
       "typeProperties": {
           "pipeline": { "referenceName": "pl_bronze_incremental_load_v2", "type": "PipelineReference" },
           "parameters": {
               "p_source_name": { "value": "my_source", "type": "Expression" },
               "p_load_type_override": { "value": "@pipeline().parameters.p_load_type_override", "type": "Expression" }
           },
           "waitOnCompletion": true
       }
   }
   ```

3. **Publish** — no changes needed to the child pipeline or any dataset.

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `act_read_ctl` fails — source not found | Control table row missing for that source_name | Run INSERT for that source in `control-table-setup.sql` |
| `404 Not Found` on API call | Wrong endpoint in control table (hyphen vs underscore, missing trailing slash) | `UPDATE ev_catalog.ctl.pipeline_control SET endpoint='...' WHERE source_name='...'` |
| All child runs succeed but wrong watermark written | The Databricks watermark notebook uses `source_name` to look up the row — must match exactly | Verify source_name in control table matches exactly what is passed as `p_source_name` |
| Sources N+1..19 skipped | Source N failed — downstream activities skipped due to `dependencyConditions: Succeeded` | Fix source N, re-run master — completed sources have 0 new pages (no-op), failed source retries |
| Master pipeline times out | 19 sequential sources takes longer than 12h default ADF timeout | Increase pipeline timeout in Settings tab, or split into two master pipelines |

---

## Day 6 Checklist

**Control table:**
- [ ] All 17 new rows inserted (run `control-table-setup.sql`)
- [ ] Verify 20 rows total in `SELECT * FROM ev_catalog.ctl.pipeline_control`
- [ ] Each row has correct `endpoint`, `sink_base_path`, `file_prefix`, `load_type`, `is_active=true`

**Master pipeline:**
- [ ] `pl_bronze_master` has 19 ExecutePipeline activities
- [ ] All activities have `waitOnCompletion: true`
- [ ] Chain is correct: each activity depends on the previous one
- [ ] `p_load_type_override` parameter added
- [ ] Published successfully

**Testing:**
- [ ] Full load run (`p_load_type_override="full"`) completes for all 20 sources
- [ ] All 19 Bronze folders created and populated
- [ ] Incremental run (empty override) completes — incremental sources pick up 0 pages (no new data since full run), full sources reload
- [ ] Monitor shows correct child pipeline runs for each source
