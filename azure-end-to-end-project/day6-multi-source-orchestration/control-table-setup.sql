-- Day 6 — Control Table Setup: Fleet and Weather Sources
-- Run in Databricks SQL editor or notebook (SQL cell)
-- Prerequisite: ev_catalog.ctl.pipeline_control table must exist (created in Day 3)
-- The payments row should already be present from Day 3/4 setup.
-- These statements add fleet and weather as new sources.

-- ─────────────────────────────────────────────────────────────────────────────
-- Verify existing rows before inserting
-- ─────────────────────────────────────────────────────────────────────────────
SELECT source_name, endpoint, load_type, last_watermark, is_active
FROM ev_catalog.ctl.pipeline_control
ORDER BY source_name;

-- ─────────────────────────────────────────────────────────────────────────────
-- Insert: Fleet source
-- Endpoint: /api/fleet/
-- Primary key: vehicle_id
-- Fields: vehicle_id, distance_km, battery_pct, trip_count, avg_speed_kmh,
--         partner_id, recorded_date
-- load_type = 'full' — no updated_after filter on this endpoint
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('fleet', '/api/fleet/', 'api/fleet', 'fleet', 1000, 'full', '1900-01-01T00:00:00Z', true);

-- ─────────────────────────────────────────────────────────────────────────────
-- Insert: Weather source
-- Endpoint: /api/weather/
-- Primary key: city_id
-- Fields: city_id, state_code, temperature_c, humidity_pct, condition,
--         wind_kmh, observed_at
-- load_type = 'full' — no updated_after filter on this endpoint
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('weather', '/api/weather/', 'api/weather', 'weather', 1000, 'full', '1900-01-01T00:00:00Z', true);

-- ─────────────────────────────────────────────────────────────────────────────
-- Verify all three sources are now registered
-- ─────────────────────────────────────────────────────────────────────────────
SELECT source_name, endpoint, sink_base_path, file_prefix, page_size,
       load_type, last_watermark, is_active
FROM ev_catalog.ctl.pipeline_control
ORDER BY source_name;

-- ─────────────────────────────────────────────────────────────────────────────
-- (Optional) Grant ADF Managed Identity access to read/write new rows
-- Run only if you created the control table with strict permissions.
-- ADF MI Application ID: 9245da2f-7129-4e0c-92c4-134fe896e294
-- ─────────────────────────────────────────────────────────────────────────────
-- GRANT USAGE ON CATALOG ev_catalog TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
-- GRANT USAGE ON SCHEMA ev_catalog.ctl TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
-- GRANT SELECT, MODIFY ON TABLE ev_catalog.ctl.pipeline_control TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
-- (These were already run in Day 4 — no need to repeat unless table was recreated)

-- ─────────────────────────────────────────────────────────────────────────────
-- Expected result after inserts:
--
-- source_name | endpoint          | sink_base_path | file_prefix | page_size | load_type | last_watermark          | is_active
-- ------------|-------------------|----------------|-------------|-----------|-----------|-------------------------|----------
-- fleet       | /api/fleet/       | api/fleet      | fleet       | 1000      | full      | 1900-01-01T00:00:00Z    | true
-- payments    | /api/db/payments/ | api/payments   | payments    | 1000      | incremental| <last run ts>          | true
-- weather     | /api/weather/     | api/weather    | weather     | 1000      | full      | 1900-01-01T00:00:00Z    | true
-- ─────────────────────────────────────────────────────────────────────────────
