-- Day 6 — Control Table Setup: All 17 VoltGrid API Sources
-- Run in Databricks SQL editor or a SQL cell in a notebook
-- Prerequisite: ev_catalog.ctl.pipeline_control table must exist (created in Day 3)
--
-- Source types:
--   incremental  — supports ?updated_after=<watermark>, fetches only new/changed rows
--   full         — no watermark filter; fetches all rows every run (reference/dim tables)
--
-- The payments row was inserted in Day 3/4. The fleet and weather rows were
-- added earlier in Day 6. This script inserts all remaining 14 sources.
-- Run the full script — INSERT OR IGNORE (or check before inserting) if rows already exist.

-- ─────────────────────────────────────────────────────────────────────────────
-- Verify existing rows before inserting
-- ─────────────────────────────────────────────────────────────────────────────
SELECT source_name, endpoint, load_type, is_active
FROM ev_catalog.ctl.pipeline_control
ORDER BY source_name;

-- ─────────────────────────────────────────────────────────────────────────────
-- FILE-BASED SCENARIO SOURCES (already inserted in earlier Day 6 steps)
-- Shown here for completeness — skip if already present
-- ─────────────────────────────────────────────────────────────────────────────

-- payments (file-based, incremental)
-- INSERT INTO ev_catalog.ctl.pipeline_control
--     (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
-- VALUES ('payments', '/api/db/payments/', 'api/payments', 'payments', 1000, 'incremental', '1900-01-01T00:00:00Z', true);

-- fleet (file-based, full)
-- INSERT INTO ev_catalog.ctl.pipeline_control
--     (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
-- VALUES ('fleet', '/api/fleet/', 'api/fleet', 'fleet', 1000, 'full', '1900-01-01T00:00:00Z', true);

-- weather (file-based, full)
-- INSERT INTO ev_catalog.ctl.pipeline_control
--     (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
-- VALUES ('weather', '/api/weather/', 'api/weather', 'weather', 1000, 'full', '1900-01-01T00:00:00Z', true);

-- ─────────────────────────────────────────────────────────────────────────────
-- DB CRUD — FACT / HIGH-FREQUENCY TABLES (incremental)
-- These change frequently → use updated_after watermark to load only new rows
-- ─────────────────────────────────────────────────────────────────────────────

-- DB Payments (fact — payment transactions)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_payments', '/api/db/payments/', 'api/payments', 'db_payments', 1000, 'incremental', '1900-01-01T00:00:00Z', true);

-- DB Charging Sessions (fact — one row per charge session)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_sessions', '/api/db/sessions/', 'api/sessions', 'db_sessions', 1000, 'incremental', '1900-01-01T00:00:00Z', true);

-- DB Energy Prices (hourly pricing feed — changes every hour)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_energy_prices', '/api/db/energy-prices/', 'api/energy_prices', 'db_energy_prices', 1000, 'incremental', '1900-01-01T00:00:00Z', true);

-- DB Complaints (operational — status changes over lifecycle)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_complaints', '/api/db/complaints/', 'api/complaints', 'db_complaints', 1000, 'incremental', '1900-01-01T00:00:00Z', true);

-- DB Maintenance Events (operational — resolved_ts updates over time)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_maintenance_events', '/api/db/maintenance-events/', 'api/maintenance_events', 'db_maintenance_events', 1000, 'incremental', '1900-01-01T00:00:00Z', true);

-- DB Pipeline Audit (observability — every pipeline run is a row)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_pipeline_audit', '/api/db/pipeline-audit/', 'api/pipeline_audit', 'db_pipeline_audit', 1000, 'incremental', '1900-01-01T00:00:00Z', true);

-- ─────────────────────────────────────────────────────────────────────────────
-- DB CRUD — DIMENSION TABLES (incremental — slow-changing)
-- Change less often but still updated → incremental to catch profile updates
-- ─────────────────────────────────────────────────────────────────────────────

-- DB Customers (dim — loyalty tier, contact info changes)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_customers', '/api/db/customers/', 'api/customers', 'db_customers', 1000, 'incremental', '1900-01-01T00:00:00Z', true);

-- DB Vehicles (dim — SCD2, battery specs, registration changes)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_vehicles', '/api/db/vehicles/', 'api/vehicles', 'db_vehicles', 1000, 'incremental', '1900-01-01T00:00:00Z', true);

-- DB Chargers (dim — firmware updates, status changes)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_chargers', '/api/db/chargers/', 'api/chargers', 'db_chargers', 1000, 'incremental', '1900-01-01T00:00:00Z', true);

-- DB Charge Cards (dim — status changes, new cards issued)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_charge_cards', '/api/db/charge-cards/', 'api/charge_cards', 'db_charge_cards', 1000, 'incremental', '1900-01-01T00:00:00Z', true);

-- DB Partners (dim — revenue share, contract end updates)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_partners', '/api/db/partners/', 'api/partners', 'db_partners', 1000, 'incremental', '1900-01-01T00:00:00Z', true);

-- DB Employees (dim — department, station assignment changes)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_employees', '/api/db/employees/', 'api/employees', 'db_employees', 1000, 'incremental', '1900-01-01T00:00:00Z', true);

-- ─────────────────────────────────────────────────────────────────────────────
-- DB CRUD — REFERENCE TABLES (full load)
-- Rarely change — full load is simpler and data volumes are small
-- ─────────────────────────────────────────────────────────────────────────────

-- DB States (reference — 8 Australian states/territories, rarely changes)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_states', '/api/db/states/', 'api/states', 'db_states', 1000, 'full', '1900-01-01T00:00:00Z', true);

-- DB Cities (reference — postcode mapping, rarely changes)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_cities', '/api/db/cities/', 'api/cities', 'db_cities', 1000, 'full', '1900-01-01T00:00:00Z', true);

-- DB Tariffs (reference — rate schedules, versioned, new versions added not updated)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_tariffs', '/api/db/tariffs/', 'api/tariffs', 'db_tariffs', 1000, 'full', '1900-01-01T00:00:00Z', true);

-- DB Charging Stations (reference — infrastructure, new stations added rarely)
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('db_stations', '/api/db/stations/', 'api/stations', 'db_stations', 1000, 'full', '1900-01-01T00:00:00Z', true);

-- ─────────────────────────────────────────────────────────────────────────────
-- Verify all sources registered
-- ─────────────────────────────────────────────────────────────────────────────
SELECT source_name, endpoint, sink_base_path, page_size, load_type, is_active
FROM ev_catalog.ctl.pipeline_control
ORDER BY source_name;

-- ─────────────────────────────────────────────────────────────────────────────
-- Expected: 20 rows total (3 file-based + 17 DB CRUD)
--
-- source_name           | endpoint                       | load_type
-- ----------------------|--------------------------------|-----------
-- db_charge_cards       | /api/db/charge-cards/          | incremental
-- db_chargers           | /api/db/chargers/              | incremental
-- db_cities             | /api/db/cities/                | full
-- db_complaints         | /api/db/complaints/            | incremental
-- db_customers          | /api/db/customers/             | incremental
-- db_employees          | /api/db/employees/             | incremental
-- db_energy_prices      | /api/db/energy-prices/         | incremental
-- db_maintenance_events | /api/db/maintenance-events/    | incremental
-- db_partners           | /api/db/partners/              | incremental
-- db_payments           | /api/db/payments/              | incremental
-- db_pipeline_audit     | /api/db/pipeline-audit/        | incremental
-- db_sessions           | /api/db/sessions/              | incremental
-- db_states             | /api/db/states/                | full
-- db_stations           | /api/db/stations/              | full
-- db_tariffs            | /api/db/tariffs/               | full
-- db_vehicles           | /api/db/vehicles/              | incremental
-- fleet                 | /api/fleet/                    | full
-- payments              | /api/db/payments/              | incremental
-- weather               | /api/weather/                  | full
-- ─────────────────────────────────────────────────────────────────────────────
