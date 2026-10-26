# Databricks notebook: nb_gold_setup_control_table
# Creates ev_catalog.gold schema (if not exists) and the gold_control table.
# Run once by admin before the first Gold pipeline trigger.

# ── Cell 1: create gold schema ────────────────────────────────────────────────
spark.sql("CREATE SCHEMA IF NOT EXISTS ev_catalog.gold COMMENT 'Gold layer — star schema, dims, facts, marts'")
print("Schema ev_catalog.gold: OK")

# ── Cell 2: create gold_control table ─────────────────────────────────────────
spark.sql("""
CREATE TABLE IF NOT EXISTS ev_catalog.gold.gold_control (
    notebook_name   STRING  NOT NULL,
    layer           STRING  NOT NULL,
    run_order       INT     NOT NULL,
    is_active       BOOLEAN NOT NULL,
    description     STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer control table — drives pl_gold_dynamic_foreach'
""")
print("Table ev_catalog.gold.gold_control: OK")

# ── Cell 3: insert control rows ───────────────────────────────────────────────
# Dims run first (run_order 1-4), then Facts (10-11), then Marts (20+)
spark.sql("""
INSERT INTO ev_catalog.gold.gold_control
    (notebook_name, layer, run_order, is_active, description, created_at, updated_at)
VALUES
    ('nb_gold_dim_station',              'dim',  1,  true, 'SCD1 — station with city/state hierarchy',  current_timestamp(), current_timestamp()),
    ('nb_gold_dim_customer',             'dim',  2,  true, 'SCD2 — customer loyalty tier tracking',     current_timestamp(), current_timestamp()),
    ('nb_gold_dim_charger',              'dim',  3,  true, 'SCD2 — charger firmware version tracking',  current_timestamp(), current_timestamp()),
    ('nb_gold_dim_tariff',               'dim',  4,  true, 'SCD2 — tariff version windows',             current_timestamp(), current_timestamp()),
    ('nb_gold_fact_charging_session',    'fact', 10, true, 'Fact — session grain, reconciliation flag', current_timestamp(), current_timestamp()),
    ('nb_gold_fact_payments',            'fact', 11, true, 'Fact — payment grain, refund flag',         current_timestamp(), current_timestamp()),
    ('nb_gold_mart_revenue_by_geo_month','mart', 20, true, 'Mart — revenue by state/city/station/month',current_timestamp(), current_timestamp())
""")
print("Control rows inserted.")

# ── Cell 4: verify ────────────────────────────────────────────────────────────
spark.sql("""
SELECT notebook_name, layer, run_order, is_active, description
FROM   ev_catalog.gold.gold_control
ORDER  BY run_order
""").display()
