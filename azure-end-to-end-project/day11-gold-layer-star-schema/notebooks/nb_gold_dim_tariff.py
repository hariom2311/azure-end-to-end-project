# Databricks notebook: nb_gold_dim_tariff
# Source : ev_catalog.silver.sl_tariffs
# Sink   : ev_catalog.gold.dim_tariff  (Delta, external)
# Type   : SCD Type 2 — one row per tariff version (effective_from / effective_to)
# Grain  : One row per tariff version window

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F
from delta.tables import DeltaTable

GOLD_TABLE = "ev_catalog.gold.dim_tariff"
GOLD_PATH  = "abfss://gold@evdatalakedev2.dfs.core.windows.net/dimensions/dim_tariff/"

dbutils.widgets.text("p_run_date", "", "Run date YYYY-MM-DD (blank = full load)")
run_date = dbutils.widgets.get("p_run_date").strip()

# ── Cell 2: read Silver source ─────────────────────────────────────────────────
df_silver = spark.table("ev_catalog.silver.sl_tariffs")

# ── Cell 3: build dimension ───────────────────────────────────────────────────
# sl_tariffs already tracks versions with effective_from / effective_to / is_current
# We promote directly to Gold, adding a surrogate key and gold metadata.
df_dim = (
    df_silver.select(
        F.col("tariff_id")                          .alias("tariff_key"),
        F.col("tariff_name"),
        F.col("rate_per_kwh").cast("double"),
        F.col("peak_rate_per_kwh").cast("double"),
        F.col("offpeak_rate_per_kwh").cast("double"),
        F.col("peak_start_hour").cast("int"),
        F.col("peak_end_hour").cast("int"),
        F.col("gst_inclusive").cast("boolean"),
        F.to_date(F.col("effective_from"))          .alias("effective_from"),
        F.to_date(F.col("effective_to"))            .alias("effective_to"),
        F.col("is_current").cast("boolean"),
        F.current_timestamp()                       .alias("gold_updated_at"),
    )
)

print(f"dim_tariff rows: {df_dim.count()}")
df_dim.show(3, truncate=80)

# ── Cell 4: CREATE TABLE IF NOT EXISTS ────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
    tariff_key           STRING   NOT NULL,
    tariff_name          STRING,
    rate_per_kwh         DOUBLE,
    peak_rate_per_kwh    DOUBLE,
    offpeak_rate_per_kwh DOUBLE,
    peak_start_hour      INT,
    peak_end_hour        INT,
    gst_inclusive        BOOLEAN,
    effective_from       DATE,
    effective_to         DATE,
    is_current           BOOLEAN,
    gold_updated_at      TIMESTAMP
)
USING DELTA
LOCATION '{GOLD_PATH}'
""")

# ── Cell 5: MERGE ─────────────────────────────────────────────────────────────
tgt = DeltaTable.forName(spark, GOLD_TABLE)
(
    tgt.alias("t")
       .merge(
           df_dim.alias("s"),
           "t.tariff_key = s.tariff_key AND t.effective_from = s.effective_from"
       )
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute()
)
print(f"MERGE done: {GOLD_TABLE}")

# ── Cell 6: verify ────────────────────────────────────────────────────────────
spark.sql(f"""
SELECT tariff_name, rate_per_kwh, effective_from, effective_to, is_current
FROM   {GOLD_TABLE}
ORDER  BY tariff_name, effective_from DESC
""").display()
