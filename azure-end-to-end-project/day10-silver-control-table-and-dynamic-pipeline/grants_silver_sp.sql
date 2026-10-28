-- =============================================================================
-- grants_silver_sp.sql
-- Unity Catalog grants for ADF Service Principal (Silver layer)
-- SP Object ID : 9245da2f-7129-4e0c-92c4-134fe896e294
--
-- IMPORTANT — run in two phases:
--
--   PHASE 1 : Run BEFORE the first pipeline trigger (no tables exist yet)
--             Sections 0, 1, 2a, 3a below
--
--   PHASE 2 : Run AFTER the first full pipeline run completes
--             (tables are created by the SP during the run, so grants succeed)
--             Sections 2b, 3b below
--
-- Why two phases?
--   GRANT ... ON TABLE fails with "table not found" if the table does not
--   exist yet. The SP creates the tables during the first pipeline run using
--   CREATE TABLE IF NOT EXISTS. Once those tables exist, run Phase 2.
-- =============================================================================


-- =============================================================================
-- PHASE 1 — Run BEFORE the first pipeline trigger
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 0. External Location — three privileges needed on each location:
--
--    CREATE EXTERNAL TABLE : create Delta table at that ADLS path
--    READ FILES            : read Delta log + parquet files (needed every run)
--    WRITE FILES           : write Delta log + parquet files (needed every run)
--
--    Why intermittent errors?
--    Unity Catalog issues short-lived ADLS credential tokens (~1 hour).
--    When ForEach runs 16 notebooks sequentially the token can expire
--    mid-pipeline. READ FILES / WRITE FILES grants ensure the token is
--    refreshed per-operation rather than cached from the first notebook.
--
--    Run: SHOW EXTERNAL LOCATIONS; to confirm the exact names.
-- ---------------------------------------------------------------------------
GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES ON EXTERNAL LOCATION `ext-loc-ev-silver`
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES ON EXTERNAL LOCATION `ext-loc-ev-quarantine`
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- 1. Catalog-level access (required for any schema/table access below)
-- ---------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG ev_catalog
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- 1. Bronze Volume — SP reads raw JSON pages from here
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.bronze
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT READ VOLUME ON VOLUME ev_catalog.bronze.api
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- 2a. Silver Schema — allow SP to create tables and read the control table
--     (silver_control already exists — created by admin setup before Day 10)
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.silver
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT CREATE TABLE ON SCHEMA ev_catalog.silver
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- silver_control was created by admin — grant SP read+write on it now
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.silver_control
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- 3a. Quarantine Schema — allow SP to create quarantine tables
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.quarantine
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT CREATE TABLE ON SCHEMA ev_catalog.quarantine
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;


-- =============================================================================
-- PHASE 2 — Run AFTER the first full pipeline run completes
--           (all sl_* and quarantine tables now exist)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 2b. Silver entity tables — SELECT + MODIFY for Delta MERGE upserts
-- ---------------------------------------------------------------------------
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_charge_cards
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_chargers
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_cities
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_complaints
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_customers
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_employees
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_energy_prices
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_maintenance_events
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_partners
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_payments
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_pipeline_audit
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_sessions
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_states
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_stations
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_tariffs
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_vehicles
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- 3b. Quarantine entity tables — SELECT + MODIFY for append writes
-- ---------------------------------------------------------------------------
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.charge_cards
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.chargers
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.cities
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.complaints
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.customers
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.employees
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.energy_prices
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.maintenance_events
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.partners
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.payments
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.pipeline_audit
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.sessions
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.states
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.stations
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.tariffs
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.vehicles
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- =============================================================================
-- VERIFY (run after Phase 2 to confirm all grants are in place)
-- =============================================================================
-- SHOW GRANTS ON CATALOG ev_catalog;
-- SHOW GRANTS ON SCHEMA ev_catalog.silver;
-- SHOW GRANTS ON SCHEMA ev_catalog.quarantine;
-- SHOW GRANTS ON TABLE ev_catalog.silver.silver_control;
