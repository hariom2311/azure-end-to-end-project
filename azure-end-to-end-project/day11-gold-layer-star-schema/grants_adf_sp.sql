-- =============================================================================
-- grants_adf_sp.sql
-- Unity Catalog grants for ADF Service Principal
-- SP Object ID : 9245da2f-7129-4e0c-92c4-134fe896e294
-- Run as       : Metastore Admin in Databricks SQL Editor
--
-- PHASE 1 — Run BEFORE first pipeline trigger (objects already exist)
-- PHASE 2 — Run AFTER first full pipeline run (tables created by SP)
--
-- Covers all three layers: Bronze → Silver → Gold
-- =============================================================================


-- =============================================================================
-- PHASE 1 — Run BEFORE first pipeline trigger
-- =============================================================================

-- ---------------------------------------------------------------------------
-- External Locations
-- All three privileges needed:
--   CREATE EXTERNAL TABLE : create Delta table at the ADLS path
--   READ FILES            : read Delta log + parquet on every run
--   WRITE FILES           : write Delta log + parquet on every run
-- Without READ FILES + WRITE FILES the error is intermittent because Unity
-- Catalog's short-lived ADLS tokens expire mid-pipeline after ~1 hour.
-- ---------------------------------------------------------------------------
GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES ON EXTERNAL LOCATION `ext-loc-ev-bronze`
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES ON EXTERNAL LOCATION `ext-loc-ev-silver`
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES ON EXTERNAL LOCATION `ext-loc-ev-quarantine`
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES ON EXTERNAL LOCATION `ext-loc-ev-gold`
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- Catalog
-- ---------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG ev_catalog
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- Bronze schema + volume
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.bronze
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT READ VOLUME ON VOLUME ev_catalog.bronze.api
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- CTL schema (Bronze pipeline control table)
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.ctl
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT ON TABLE ev_catalog.ctl.pipeline_control
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- Silver schema
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.silver
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT CREATE TABLE ON SCHEMA ev_catalog.silver
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.silver_control
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- Quarantine schema
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.quarantine
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT CREATE TABLE ON SCHEMA ev_catalog.quarantine
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- Gold schema
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.gold
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT CREATE TABLE ON SCHEMA ev_catalog.gold
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.gold_control
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;


-- =============================================================================
-- PHASE 2 — Run AFTER first full pipeline run
--           (all sl_*, quarantine, and gold tables now exist)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Silver entity tables (MERGE needs SELECT + MODIFY)
-- ---------------------------------------------------------------------------
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_charge_cards        TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_chargers             TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_cities               TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_complaints           TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_customers            TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_employees            TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_energy_prices        TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_maintenance_events   TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_partners             TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_payments             TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_pipeline_audit       TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_sessions             TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_states               TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_stations             TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_tariffs              TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_vehicles             TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- Quarantine tables (append writes from quality checks)
-- ---------------------------------------------------------------------------
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.charge_cards        TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.chargers            TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.cities              TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.complaints          TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.customers           TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.employees           TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.energy_prices       TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.maintenance_events  TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.partners            TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.payments            TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.pipeline_audit      TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.sessions            TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.states              TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.stations            TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.tariffs             TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.vehicles            TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- ---------------------------------------------------------------------------
-- Gold tables (Gold notebooks read Silver + write Gold)
-- ---------------------------------------------------------------------------
GRANT SELECT ON TABLE ev_catalog.silver.sl_charge_cards        TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_chargers            TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_cities              TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_complaints          TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_customers           TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_employees           TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_energy_prices       TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_maintenance_events  TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_partners            TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_payments            TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_pipeline_audit      TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_sessions            TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_states              TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_stations            TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_tariffs             TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_vehicles            TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.dim_station               TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.dim_customer              TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.dim_charger               TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.dim_tariff                TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.fact_charging_session     TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.fact_payments             TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.mart_revenue_by_geo_month TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- =============================================================================
-- VERIFY
-- =============================================================================
-- SHOW GRANTS ON EXTERNAL LOCATION `ext-loc-ev-silver`;
-- SHOW GRANTS ON SCHEMA ev_catalog.silver;
-- SHOW GRANTS ON TABLE ev_catalog.silver.sl_payments;
-- SHOW GRANTS ON TABLE ev_catalog.gold.fact_charging_session;
