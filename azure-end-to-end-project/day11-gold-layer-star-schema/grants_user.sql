-- =============================================================================
-- grants_user.sql
-- Unity Catalog grants for human user running notebooks directly
-- User : hariomsuryawanshi68258@gmail.com
-- Run as : Metastore Admin in Databricks SQL Editor
--
-- PHASE 1 — Run BEFORE first notebook run (objects already exist)
-- PHASE 2 — Run AFTER first full pipeline run (tables created by notebooks)
--
-- Covers all three layers: Bronze → Silver → Gold
-- =============================================================================


-- =============================================================================
-- PHASE 1 — Run BEFORE first notebook run
-- =============================================================================

-- ---------------------------------------------------------------------------
-- External Locations
-- All three privileges needed to create Delta tables and read/write ADLS:
--   CREATE EXTERNAL TABLE : create Delta table at the ADLS path
--   READ FILES            : read Delta log + parquet on every run
--   WRITE FILES           : write Delta log + parquet on every run
-- ---------------------------------------------------------------------------
GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES ON EXTERNAL LOCATION `ext-loc-ev-bronze`
TO `hariomsuryawanshi68258@gmail.com`;

GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES ON EXTERNAL LOCATION `ext-loc-ev-silver`
TO `hariomsuryawanshi68258@gmail.com`;

GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES ON EXTERNAL LOCATION `ext-loc-ev-quarantine`
TO `hariomsuryawanshi68258@gmail.com`;

GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES ON EXTERNAL LOCATION `ext-loc-ev-gold`
TO `hariomsuryawanshi68258@gmail.com`;

-- ---------------------------------------------------------------------------
-- Catalog
-- ---------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG ev_catalog
TO `hariomsuryawanshi68258@gmail.com`;

-- ---------------------------------------------------------------------------
-- Bronze schema + volume
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.bronze
TO `hariomsuryawanshi68258@gmail.com`;

GRANT READ VOLUME ON VOLUME ev_catalog.bronze.api
TO `hariomsuryawanshi68258@gmail.com`;

-- ---------------------------------------------------------------------------
-- CTL schema (Bronze pipeline control table)
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.ctl
TO `hariomsuryawanshi68258@gmail.com`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.ctl.pipeline_control
TO `hariomsuryawanshi68258@gmail.com`;

-- ---------------------------------------------------------------------------
-- Silver schema
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.silver
TO `hariomsuryawanshi68258@gmail.com`;

GRANT CREATE TABLE ON SCHEMA ev_catalog.silver
TO `hariomsuryawanshi68258@gmail.com`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.silver_control
TO `hariomsuryawanshi68258@gmail.com`;

-- ---------------------------------------------------------------------------
-- Quarantine schema
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.quarantine
TO `hariomsuryawanshi68258@gmail.com`;

GRANT CREATE TABLE ON SCHEMA ev_catalog.quarantine
TO `hariomsuryawanshi68258@gmail.com`;

-- ---------------------------------------------------------------------------
-- Gold schema
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA ev_catalog.gold
TO `hariomsuryawanshi68258@gmail.com`;

GRANT CREATE TABLE ON SCHEMA ev_catalog.gold
TO `hariomsuryawanshi68258@gmail.com`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.gold_control
TO `hariomsuryawanshi68258@gmail.com`;


-- =============================================================================
-- PHASE 2 — Run AFTER first full pipeline run
--           (all sl_*, quarantine, and gold tables now exist)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Silver entity tables (MERGE needs SELECT + MODIFY)
-- ---------------------------------------------------------------------------
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_charge_cards        TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_chargers             TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_cities               TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_complaints           TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_customers            TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_employees            TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_energy_prices        TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_maintenance_events   TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_partners             TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_payments             TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_pipeline_audit       TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_sessions             TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_states               TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_stations             TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_tariffs              TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.silver.sl_vehicles             TO `hariomsuryawanshi68258@gmail.com`;

-- ---------------------------------------------------------------------------
-- Quarantine tables (append writes from quality checks)
-- ---------------------------------------------------------------------------
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.charge_cards        TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.chargers            TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.cities              TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.complaints          TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.customers           TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.employees           TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.energy_prices       TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.maintenance_events  TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.partners            TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.payments            TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.pipeline_audit      TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.sessions            TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.states              TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.stations            TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.tariffs             TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.quarantine.vehicles            TO `hariomsuryawanshi68258@gmail.com`;

-- ---------------------------------------------------------------------------
-- Gold tables (Gold notebooks read Silver + write Gold)
-- ---------------------------------------------------------------------------
GRANT SELECT ON TABLE ev_catalog.silver.sl_charge_cards        TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_chargers            TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_cities              TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_complaints          TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_customers           TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_employees           TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_energy_prices       TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_maintenance_events  TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_partners            TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_payments            TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_pipeline_audit      TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_sessions            TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_states              TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_stations            TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_tariffs             TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT ON TABLE ev_catalog.silver.sl_vehicles            TO `hariomsuryawanshi68258@gmail.com`;

GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.dim_station               TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.dim_customer              TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.dim_charger               TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.dim_tariff                TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.fact_charging_session     TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.fact_payments             TO `hariomsuryawanshi68258@gmail.com`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.mart_revenue_by_geo_month TO `hariomsuryawanshi68258@gmail.com`;

-- =============================================================================
-- VERIFY
-- =============================================================================
-- SHOW GRANTS ON EXTERNAL LOCATION `ext-loc-ev-silver`;
-- SHOW GRANTS ON SCHEMA ev_catalog.silver;
-- SHOW GRANTS ON TABLE ev_catalog.silver.sl_payments;
-- SHOW GRANTS ON TABLE ev_catalog.gold.fact_charging_session;
