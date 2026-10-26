# Databricks notebook: nb_gold_fact_payments
# Source : ev_catalog.silver.sl_payments
# Sink   : ev_catalog.gold.fact_payments  (Delta, external)
# Grain  : One row per payment transaction
# Key metrics : amount_aud, gst, gateway, status, refund_flag

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F
from delta.tables import DeltaTable

GOLD_TABLE = "ev_catalog.gold.fact_payments"
GOLD_PATH  = "abfss://gold@evdatalakedev2.dfs.core.windows.net/facts/fact_payments/"

dbutils.widgets.text("p_run_date", "", "Run date YYYY-MM-DD (blank = full load)")
dbutils.widgets.text("p_run_type", "", "Run type full/incremental (blank = full load)")
run_date = dbutils.widgets.get("p_run_date").strip()
run_type = dbutils.widgets.get("p_run_type").strip()

# ── Cell 2: read Silver source ─────────────────────────────────────────────────
df_silver = spark.table("ev_catalog.silver.sl_payments")

if run_date and run_type == 'incremental':
    df_silver = df_silver.filter(F.col("sl_batch_id") == run_date)
    print(f"Incremental: filtering payments for batch_id = {run_date}")

# ── Cell 3: build fact ────────────────────────────────────────────────────────
df_fact = (
    df_silver.select(
        F.col("payment_id")                               .alias("payment_key"),
        F.col("session_id")                               .alias("session_key"),
        F.col("customer_id")                              .alias("customer_key"),
        # Time key
        F.date_format(F.col("processed_at_aest"), "yyyyMMddHH").cast("long").alias("time_key"),
        F.to_date(F.col("processed_at_aest"))             .alias("payment_date"),
        # Measures
        F.col("amount_aud").cast("double"),
        F.col("excl_gst_aud").cast("double"),
        F.col("gst_amount_aud").cast("double"),
        # Attributes
        F.col("gateway"),
        F.col("payment_mode"),
        F.col("status")                                   .alias("payment_status"),
        F.col("billing_anomaly_flag"),
        # Refund flag: status = 'refunded'
        F.when(F.lower(F.col("status")) == "refunded", True).otherwise(False).alias("refund_flag"),
        # Metadata
        F.current_timestamp()                             .alias("gold_updated_at"),
        F.lit(run_date if run_date else "full")           .alias("gold_batch_id"),
    )
)

total = df_fact.count()
print(f"Fact rows: {total}")
if total == 0:
    print("No payments to process. Exiting.")
    dbutils.notebook.exit("NO_DATA")

df_fact.show(3, truncate=80)

# ── Cell 4: CREATE TABLE IF NOT EXISTS ────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
    payment_key          STRING    NOT NULL,
    session_key          STRING,
    customer_key         STRING,
    time_key             LONG,
    payment_date         DATE,
    amount_aud           DOUBLE,
    excl_gst_aud         DOUBLE,
    gst_amount_aud       DOUBLE,
    gateway              STRING,
    payment_mode         STRING,
    payment_status       STRING,
    billing_anomaly_flag BOOLEAN,
    refund_flag          BOOLEAN,
    gold_updated_at      TIMESTAMP,
    gold_batch_id        STRING
)
USING DELTA
LOCATION '{GOLD_PATH}'
""")

# ── Cell 5: MERGE ─────────────────────────────────────────────────────────────
tgt = DeltaTable.forName(spark, GOLD_TABLE)
(
    tgt.alias("t")
       .merge(df_fact.alias("s"), "t.payment_key = s.payment_key")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute()
)
print(f"MERGE done: {GOLD_TABLE}")

# ── Cell 6: post-load breakdown ───────────────────────────────────────────────
spark.sql(f"""
SELECT
    payment_status,
    gateway,
    COUNT(*)                      AS txn_count,
    ROUND(SUM(amount_aud), 2)     AS total_aud,
    SUM(CAST(refund_flag AS INT)) AS refunds,
    SUM(CAST(billing_anomaly_flag AS INT)) AS anomalies
FROM {GOLD_TABLE}
GROUP BY payment_status, gateway
ORDER BY total_aud DESC
""").display()
