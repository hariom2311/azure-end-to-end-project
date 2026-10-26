# Databricks notebook: nb_gold_fact_charging_session
# Source : ev_catalog.silver.sl_sessions  +  sl_payments  +  sl_tariffs
# Sink   : ev_catalog.gold.fact_charging_session  (Delta, external)
# Grain  : One row per charging session
# Key metrics : energy_kwh, duration_min, expected_amount, billed_amount,
#               reconciliation_status (MATCH / MISMATCH / UNPAID)

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F
from delta.tables import DeltaTable

GOLD_TABLE = "ev_catalog.gold.fact_charging_session"
GOLD_PATH  = "abfss://gold@evdatalakedev2.dfs.core.windows.net/facts/fact_charging_session/"

dbutils.widgets.text("p_run_date", "", "Run date YYYY-MM-DD (blank = full load)")
dbutils.widgets.text("p_run_type", "", "Run type full/incremental (blank = full load)")
run_date = dbutils.widgets.get("p_run_date").strip()
run_type = dbutils.widgets.get("p_run_type").strip()

# ── Cell 2: read Silver sources ────────────────────────────────────────────────
df_sessions  = spark.table("ev_catalog.silver.sl_sessions")
df_payments  = spark.table("ev_catalog.silver.sl_payments")
df_tariffs   = spark.table("ev_catalog.silver.sl_tariffs")

# Incremental filter on sessions
if run_date and run_type == 'incremental':
    df_sessions = df_sessions.filter(F.col("sl_batch_id") == run_date)
    print(f"Incremental: filtering sessions for batch_id = {run_date}")

# ── Cell 3: resolve current tariff for each session ───────────────────────────
# Join each session to the tariff version that was active on the session date
df_current_tariff = df_tariffs.filter(F.col("is_current") == True).select(
    F.col("tariff_id"),
    F.col("rate_per_kwh").cast("double"),
)

# ── Cell 4: build fact ────────────────────────────────────────────────────────
df_fact = (
    df_sessions.alias("s")
    # Bring in payment (left join — session may not have payment yet)
    .join(
        df_payments.alias("p").select("session_id", "amount_aud", "status", "gateway", "payment_mode"),
        F.col("s.session_id") == F.col("p.session_id"),
        "left"
    )
    # Bring in current tariff rate
    .join(
        df_current_tariff.alias("t"),
        F.col("s.tariff_id") == F.col("t.tariff_id"),
        "left"
    )
    .select(
        # Keys
        F.col("s.session_id")                           .alias("session_key"),
        F.col("s.customer_id")                          .alias("customer_key"),
        F.col("s.charger_id")                           .alias("charger_key"),
        F.col("s.station_id")                           .alias("station_key"),
        F.col("s.tariff_id")                            .alias("tariff_key"),
        # Time key (YYYYMMDDHH integer for DimTime join)
        F.date_format(F.col("s.started_at_aest"), "yyyyMMddHH").cast("long").alias("time_key"),
        F.to_date(F.col("s.started_at_aest"))           .alias("session_date"),
        # Measures
        F.col("s.energy_kwh").cast("double"),
        F.col("s.duration_min").cast("int"),
        # Expected amount = energy * current tariff rate
        F.round(F.col("s.energy_kwh") * F.col("t.rate_per_kwh"), 2).alias("expected_amount_aud"),
        # Billed amount from payment
        F.col("p.amount_aud")                           .alias("billed_amount_aud"),
        F.col("p.status")                               .alias("payment_status"),
        F.col("p.gateway")                              .alias("payment_gateway"),
        F.col("p.payment_mode"),
        # Reconciliation
        F.when(
            F.col("p.amount_aud").isNull(),
            "UNPAID"
        ).when(
            F.abs(F.col("p.amount_aud") - F.round(F.col("s.energy_kwh") * F.col("t.rate_per_kwh"), 2)) < 0.01,
            "MATCH"
        ).otherwise("MISMATCH")                         .alias("reconciliation_status"),
        # Flags
        F.col("s.sla_breach_flag"),
        F.col("s.late_event_flag"),
        F.col("s.excl_gst_aud")                        .alias("session_excl_gst_aud"),
        F.col("s.gst_amount_aud")                      .alias("session_gst_aud"),
        # Metadata
        F.current_timestamp()                           .alias("gold_updated_at"),
        F.lit(run_date if run_date else "full")         .alias("gold_batch_id"),
    )
)

total = df_fact.count()
print(f"Fact rows: {total}")
if total == 0:
    print("No sessions to process. Exiting.")
    dbutils.notebook.exit("NO_DATA")

df_fact.show(3, truncate=80)

# ── Cell 5: reconciliation summary (before write) ─────────────────────────────
df_fact.groupBy("reconciliation_status").count().display()

# ── Cell 6: CREATE TABLE IF NOT EXISTS ────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
    session_key            STRING    NOT NULL,
    customer_key           STRING,
    charger_key            STRING,
    station_key            STRING,
    tariff_key             STRING,
    time_key               LONG,
    session_date           DATE,
    energy_kwh             DOUBLE,
    duration_min           INT,
    expected_amount_aud    DOUBLE,
    billed_amount_aud      DOUBLE,
    payment_status         STRING,
    payment_gateway        STRING,
    payment_mode           STRING,
    reconciliation_status  STRING,
    sla_breach_flag        BOOLEAN,
    late_event_flag        BOOLEAN,
    session_excl_gst_aud   DOUBLE,
    session_gst_aud        DOUBLE,
    gold_updated_at        TIMESTAMP,
    gold_batch_id          STRING
)
USING DELTA
LOCATION '{GOLD_PATH}'
""")

# ── Cell 7: MERGE ─────────────────────────────────────────────────────────────
tgt = DeltaTable.forName(spark, GOLD_TABLE)
(
    tgt.alias("t")
       .merge(df_fact.alias("s"), "t.session_key = s.session_key")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute()
)
print(f"MERGE done: {GOLD_TABLE}")

# ── Cell 8: post-load reconciliation breakdown ────────────────────────────────
spark.sql(f"""
SELECT
    reconciliation_status,
    COUNT(*)                         AS session_count,
    ROUND(SUM(expected_amount_aud), 2) AS total_expected,
    ROUND(SUM(billed_amount_aud),   2) AS total_billed,
    ROUND(SUM(billed_amount_aud) - SUM(expected_amount_aud), 2) AS variance_aud
FROM {GOLD_TABLE}
GROUP BY reconciliation_status
ORDER BY reconciliation_status
""").display()
