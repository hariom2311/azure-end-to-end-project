# Databricks notebook: nb_gold_dim_charger
# Source : ev_catalog.silver.sl_chargers  +  sl_stations
# Sink   : ev_catalog.gold.dim_charger  (Delta, external)
# Type   : SCD Type 2 — track firmware_version changes over time
# Grain  : One row per charger version

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F
from delta.tables import DeltaTable

GOLD_TABLE = "ev_catalog.gold.dim_charger"
GOLD_PATH  = "abfss://gold@evdatalakedev2.dfs.core.windows.net/dimensions/dim_charger/"

dbutils.widgets.text("p_run_date", "", "Run date YYYY-MM-DD (blank = full load)")
run_date = dbutils.widgets.get("p_run_date").strip()

# ── Cell 2: read Silver sources ────────────────────────────────────────────────
df_chargers  = spark.table("ev_catalog.silver.sl_chargers")
df_stations  = spark.table("ev_catalog.silver.sl_stations")

# ── Cell 3: join and build incoming dimension rows ────────────────────────────
df_incoming = (
    df_chargers.alias("c")
    .join(df_stations.alias("s"), F.col("c.station_id") == F.col("s.station_id"), "left")
    .select(
        F.col("c.charger_id")         .alias("charger_key"),
        F.col("c.station_id")         .alias("station_key"),
        F.col("s.station_name"),
        F.col("c.connector_type"),
        F.col("c.max_kw").cast("double"),
        F.col("c.firmware_version"),
        F.col("c.status")             .alias("charger_status"),
        F.col("c.fault_flag"),
        F.col("c.created_at_utc"),
        F.col("c.updated_at_utc"),
    )
    .withColumn("effective_from",  F.current_date())
    .withColumn("effective_to",    F.lit(None).cast("date"))
    .withColumn("is_current",      F.lit(True))
    .withColumn("gold_updated_at", F.current_timestamp())
)

print(f"Incoming charger rows: {df_incoming.count()}")

# ── Cell 4: CREATE TABLE IF NOT EXISTS ────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
    charger_key      STRING   NOT NULL,
    station_key      STRING,
    station_name     STRING,
    connector_type   STRING,
    max_kw           DOUBLE,
    firmware_version STRING,
    charger_status   STRING,
    fault_flag       BOOLEAN,
    created_at_utc   TIMESTAMP,
    updated_at_utc   TIMESTAMP,
    effective_from   DATE     NOT NULL,
    effective_to     DATE,
    is_current       BOOLEAN,
    gold_updated_at  TIMESTAMP
)
USING DELTA
LOCATION '{GOLD_PATH}'
""")

# ── Cell 5: SCD2 MERGE ────────────────────────────────────────────────────────
# Step A — expire rows where firmware_version changed
spark.sql(f"""
MERGE INTO {GOLD_TABLE} t
USING (
    SELECT s.charger_key, s.firmware_version
    FROM   {GOLD_TABLE} t2
    JOIN   (SELECT charger_key, firmware_version FROM delta.`{GOLD_PATH}`) s
              ON t2.charger_key      = s.charger_key
             AND t2.is_current       = true
             AND t2.firmware_version != s.firmware_version
) changed
ON t.charger_key = changed.charger_key AND t.is_current = true
WHEN MATCHED THEN UPDATE SET
    t.effective_to    = date_sub(current_date(), 1),
    t.is_current      = false,
    t.gold_updated_at = current_timestamp()
""")

# Step B — insert new / updated current rows
tgt = DeltaTable.forName(spark, GOLD_TABLE)
(
    tgt.alias("t")
       .merge(
           df_incoming.alias("s"),
           "t.charger_key = s.charger_key AND t.is_current = true AND t.firmware_version = s.firmware_version"
       )
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute()
)
print(f"MERGE done: {GOLD_TABLE}")

# ── Cell 6: verify ────────────────────────────────────────────────────────────
spark.sql(f"""
SELECT connector_type, charger_status, COUNT(*) AS cnt, SUM(CAST(fault_flag AS INT)) AS faulted
FROM   {GOLD_TABLE}
WHERE  is_current = true
GROUP  BY connector_type, charger_status
ORDER  BY connector_type
""").display()
