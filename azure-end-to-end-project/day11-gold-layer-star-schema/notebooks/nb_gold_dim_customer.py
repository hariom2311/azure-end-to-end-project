# Databricks notebook: nb_gold_dim_customer
# Source : ev_catalog.silver.sl_customers
# Sink   : ev_catalog.gold.dim_customer  (Delta, external)
# Type   : SCD Type 2 — track loyalty_tier changes over time
# Grain  : One row per customer version (customer_id + effective_from)

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

GOLD_TABLE = "ev_catalog.gold.dim_customer"
GOLD_PATH  = "abfss://gold@evdatalakedev2.dfs.core.windows.net/dimensions/dim_customer/"

dbutils.widgets.text("p_run_date", "", "Run date YYYY-MM-DD (blank = full load)")
run_date = dbutils.widgets.get("p_run_date").strip()

# ── Cell 2: read Silver source ─────────────────────────────────────────────────
df_silver = spark.table("ev_catalog.silver.sl_customers")

# ── Cell 3: build SCD2 dimension ──────────────────────────────────────────────
# Silver has one row per customer_id (deduped, latest wins).
# For Gold SCD2, compare incoming row to current Gold row.
# If loyalty_tier changed → expire old row, insert new row.
# New customers → insert directly.

df_incoming = (
    df_silver.select(
        F.col("customer_id")                          .alias("customer_key"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("email_masked"),
        F.col("phone"),
        F.col("loyalty_tier"),
        F.col("signup_date"),
        F.col("created_at_utc"),
        F.col("updated_at_utc"),
    )
    .withColumn("effective_from", F.current_date())
    .withColumn("effective_to",   F.lit(None).cast("date"))
    .withColumn("is_current",     F.lit(True))
    .withColumn("gold_updated_at", F.current_timestamp())
)

print(f"Incoming customer rows: {df_incoming.count()}")

# ── Cell 4: CREATE TABLE IF NOT EXISTS ────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
    customer_key     STRING   NOT NULL,
    first_name       STRING,
    last_name        STRING,
    email_masked     STRING,
    phone            STRING,
    loyalty_tier     STRING,
    signup_date      STRING,
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
# Step A — expire existing current rows where loyalty_tier changed
spark.sql(f"""
MERGE INTO {GOLD_TABLE} t
USING (
    SELECT s.customer_key, s.loyalty_tier
    FROM   {GOLD_TABLE} t2
    JOIN   (SELECT customer_key, loyalty_tier FROM delta.`{GOLD_PATH}`) s
              ON t2.customer_key = s.customer_key
             AND t2.is_current   = true
             AND t2.loyalty_tier != s.loyalty_tier
) changed
ON t.customer_key = changed.customer_key AND t.is_current = true
WHEN MATCHED THEN UPDATE SET
    t.effective_to   = date_sub(current_date(), 1),
    t.is_current     = false,
    t.gold_updated_at = current_timestamp()
""")

# Step B — insert new + changed rows
tgt = DeltaTable.forName(spark, GOLD_TABLE)
(
    tgt.alias("t")
       .merge(
           df_incoming.alias("s"),
           "t.customer_key = s.customer_key AND t.is_current = true AND t.loyalty_tier = s.loyalty_tier"
       )
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute()
)
print(f"MERGE done: {GOLD_TABLE}")

# ── Cell 6: verify ────────────────────────────────────────────────────────────
spark.sql(f"""
SELECT loyalty_tier, is_current, COUNT(*) AS cnt
FROM   {GOLD_TABLE}
GROUP  BY loyalty_tier, is_current
ORDER  BY loyalty_tier, is_current DESC
""").display()
