# Databricks notebook: nb_gold_dim_station
# Source : ev_catalog.silver.sl_stations  +  sl_cities  +  sl_states
# Sink   : ev_catalog.gold.dim_station  (Delta, external)
# Type   : SCD Type 1 — overwrite changed attributes in-place
# Grain  : One row per physical charging station

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F
from delta.tables import DeltaTable

GOLD_TABLE = "ev_catalog.gold.dim_station"
GOLD_PATH  = "abfss://gold@evdatalakedev2.dfs.core.windows.net/dimensions/dim_station/"

dbutils.widgets.text("p_run_date", "", "Run date YYYY-MM-DD (blank = full load)")
run_date = dbutils.widgets.get("p_run_date").strip()

# ── Cell 2: read Silver sources ───────────────────────────────────────────────
df_stations = spark.table("ev_catalog.silver.sl_stations")
df_cities   = spark.table("ev_catalog.silver.sl_cities")
df_states   = spark.table("ev_catalog.silver.sl_states")

# ── Cell 3: join and build dimension ──────────────────────────────────────────
df_dim = (
    df_stations.alias("st")
    .join(df_cities.alias("ci"),  F.col("st.city_id")  == F.col("ci.city_id"),  "left")
    .join(df_states.alias("sr"),  F.col("ci.state_code") == F.col("sr.state_code"), "left")
    .select(
        F.col("st.station_id")                     .alias("station_key"),
        F.col("st.station_name")                   .alias("station_name"),
        F.col("st.site_type")                      .alias("site_type"),
        F.col("st.status")                         .alias("station_status"),
        F.col("st.latitude")                       .alias("latitude"),
        F.col("st.longitude")                      .alias("longitude"),
        F.col("st.address")                        .alias("address"),
        F.col("st.state_region")                   .alias("state_region"),
        F.col("ci.city_id")                        .alias("city_key"),
        F.col("ci.city_name")                      .alias("city_name"),
        F.col("ci.postcode")                       .alias("postcode"),
        F.col("sr.state_code")                     .alias("state_code"),
        F.col("sr.state_name")                     .alias("state_name"),
        F.current_timestamp()                      .alias("gold_updated_at"),
    )
)

print(f"dim_station rows: {df_dim.count()}")
df_dim.show(3, truncate=80)

# ── Cell 4: CREATE TABLE IF NOT EXISTS ────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
    station_key      STRING  NOT NULL,
    station_name     STRING,
    site_type        STRING,
    station_status   STRING,
    latitude         DOUBLE,
    longitude        DOUBLE,
    address          STRING,
    state_region     STRING,
    city_key         STRING,
    city_name        STRING,
    postcode         STRING,
    state_code       STRING,
    state_name       STRING,
    gold_updated_at  TIMESTAMP
)
USING DELTA
LOCATION '{GOLD_PATH}'
""")

# ── Cell 5: MERGE (SCD Type 1 — update in-place) ─────────────────────────────
tgt = DeltaTable.forName(spark, GOLD_TABLE)
(
    tgt.alias("t")
       .merge(df_dim.alias("s"), "t.station_key = s.station_key")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute()
)
print(f"MERGE done: {GOLD_TABLE}")

# ── Cell 6: verify ────────────────────────────────────────────────────────────
spark.sql(f"SELECT state_name, COUNT(*) AS station_count FROM {GOLD_TABLE} GROUP BY state_name ORDER BY station_count DESC").display()
