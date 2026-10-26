-- =============================================================================
-- grants_silver_sp.sql
-- Unity Catalog grants for ADF Service Principal (Silver layer)
-- SP Object ID : 9245da2f-7129-4e0c-92c4-134fe896e294
--
-- Run once in a Databricks SQL workspace connected to the Unity Catalog
-- metastore (as a metastore admin or catalog owner).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 0. Catalog-level access (required for any schema/table access below)
-- ---------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG ev_catalog
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- 1. Bronze Volume — read raw JSON pages
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.bronze
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT READ VOLUME ON VOLUME ev_catalog.bronze.api
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- 2. Silver Schema — create + write Delta tables
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.silver
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT CREATE TABLE ON SCHEMA ev_catalog.silver
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Control table (read by nb_list_silver_notebooks, written by admin setup)
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.silver_control
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Silver entity tables (MODIFY covers INSERT + UPDATE needed by Delta MERGE)
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
-- 3. Quarantine Schema — create + write bad-row tables
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.quarantine
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT CREATE TABLE ON SCHEMA ev_catalog.quarantine
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Quarantine entity tables (append-only writes from quality checks)
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
-- VERIFY (run after the GRANTs to confirm)
-- =============================================================================
-- SHOW GRANTS ON CATALOG ev_catalog;
-- SHOW GRANTS ON SCHEMA ev_catalog.silver;
-- SHOW GRANTS ON SCHEMA ev_catalog.quarantine;
-- SHOW GRANTS ON TABLE ev_catalog.silver.silver_control;
