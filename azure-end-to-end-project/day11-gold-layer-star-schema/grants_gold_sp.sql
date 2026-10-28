-- =============================================================================
-- grants_gold_sp.sql
-- Unity Catalog grants for ADF Service Principal (Gold layer)
-- SP Object ID : 9245da2f-7129-4e0c-92c4-134fe896e294
--
-- PHASE 1 — Run BEFORE the first Gold pipeline trigger
-- PHASE 2 — Run AFTER the first full Gold pipeline run (tables now exist)
-- =============================================================================

-- =============================================================================
-- PHASE 1 — Run BEFORE the first pipeline trigger
-- =============================================================================

-- ---------------------------------------------------------------------------
-- PRE-REQUISITE: Create the Gold External Location (run once, admin only)
--
-- What: Maps a name to the ADLS Gold container so Unity Catalog allows
--       CREATE TABLE ... LOCATION 'abfss://gold@...' calls.
--
-- UI Steps (Databricks → Catalog → External Data → External Locations):
--   1. Click "Create location"
--   2. External location name : ext-loc-ev-gold
--   3. URL                    : abfss://gold@evdatalakedev2.dfs.core.windows.net/
--   4. Storage credential     : select existing credential (same as Silver/Bronze)
--   5. Click Create → Test connection → should show "Connection successful"
--
-- Verify:
--   SHOW EXTERNAL LOCATIONS;
--   DESCRIBE EXTERNAL LOCATION `ext-loc-ev-gold`;
-- ---------------------------------------------------------------------------

-- Grant SP access to the Gold external location (metastore admin required)
GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES ON EXTERNAL LOCATION `ext-loc-ev-gold`
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Gold catalog + schema access
GRANT USE CATALOG ON CATALOG ev_catalog
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT USE SCHEMA ON SCHEMA ev_catalog.gold
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT CREATE TABLE ON SCHEMA ev_catalog.gold
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Silver schemas — read access (Gold reads from Silver)
GRANT USE SCHEMA ON SCHEMA ev_catalog.silver
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- gold_control table exists after nb_gold_setup_control_table runs (admin step)
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.gold_control
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- =============================================================================
-- PHASE 2 — Run AFTER the first full Gold pipeline run
--           (all Gold tables created by the SP during the run)
-- =============================================================================

-- Silver entity tables — SELECT only (Gold reads, never writes Silver)
GRANT SELECT ON TABLE ev_catalog.silver.sl_stations
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_cities
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_states
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_customers
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_chargers
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_tariffs
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_sessions
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_payments
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_partners
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_employees
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_vehicles
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_maintenance_events
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_complaints
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_charge_cards
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_energy_prices
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.silver.sl_pipeline_audit
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Gold dimension tables
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.dim_station
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.dim_customer
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.dim_charger
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.dim_tariff
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Gold fact tables
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.fact_charging_session
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.fact_payments
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Gold mart tables
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.mart_revenue_by_geo_month
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- =============================================================================
-- VERIFY (run after Phase 2)
-- =============================================================================
-- SHOW GRANTS ON SCHEMA ev_catalog.gold;
-- SHOW GRANTS ON TABLE ev_catalog.gold.gold_control;
-- SHOW GRANTS ON TABLE ev_catalog.gold.fact_charging_session;
