# Databricks notebook: nb_gold_mart_revenue_by_geo_month
# Source : ev_catalog.gold.fact_payments  +  dim_station  +  dim_customer
# Sink   : ev_catalog.gold.mart_revenue_by_geo_month  (Delta, external)
# Grain  : State / City / Station x Calendar Month
# Used by: Revenue Dashboard in Power BI

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F
from delta.tables import DeltaTable

GOLD_TABLE = "ev_catalog.gold.mart_revenue_by_geo_month"
GOLD_PATH  = "abfss://gold@evdatalakedev2.dfs.core.windows.net/marts/mart_revenue_by_geo_month/"

dbutils.widgets.text("p_run_date", "", "Run date YYYY-MM-DD (blank = full rebuild)")
run_date = dbutils.widgets.get("p_run_date").strip()

# ── Cell 2: read Gold facts + dims ────────────────────────────────────────────
df_payments = spark.table("ev_catalog.gold.fact_payments")
df_sessions = spark.table("ev_catalog.gold.fact_charging_session")
df_station  = spark.table("ev_catalog.gold.dim_station")

# ── Cell 3: join sessions → station → geo ─────────────────────────────────────
df_session_geo = (
    df_sessions.alias("s")
    .join(df_station.alias("d"), F.col("s.station_key") == F.col("d.station_key"), "left")
    .join(
        df_payments.alias("p").select("session_key", "amount_aud", "payment_status", "refund_flag"),
        F.col("s.session_key") == F.col("p.session_key"),
        "left"
    )
    .select(
        F.col("d.state_name"),
        F.col("d.city_name"),
        F.col("d.station_key"),
        F.col("d.station_name"),
        F.col("d.state_region"),
        F.year(F.col("s.session_date"))                  .alias("year"),
        F.month(F.col("s.session_date"))                 .alias("month"),
        F.date_format(F.col("s.session_date"), "yyyy-MM").alias("year_month"),
        F.col("s.energy_kwh"),
        F.col("s.duration_min"),
        F.col("s.expected_amount_aud"),
        F.col("p.amount_aud")                            .alias("billed_amount_aud"),
        F.col("p.payment_status"),
        F.col("p.refund_flag"),
        F.col("s.reconciliation_status"),
    )
)

# ── Cell 4: aggregate ─────────────────────────────────────────────────────────
df_mart = (
    df_session_geo
    .groupBy("state_name", "city_name", "station_key", "station_name", "state_region", "year", "month", "year_month")
    .agg(
        F.count("*")                                        .alias("session_count"),
        F.round(F.sum("energy_kwh"),          2)           .alias("total_energy_kwh"),
        F.round(F.avg("duration_min"),        1)           .alias("avg_duration_min"),
        F.round(F.sum("expected_amount_aud"), 2)           .alias("total_expected_aud"),
        F.round(F.sum("billed_amount_aud"),   2)           .alias("total_billed_aud"),
        F.round(
            F.sum("billed_amount_aud") - F.sum("expected_amount_aud"), 2
        )                                                   .alias("revenue_variance_aud"),
        F.sum(F.when(F.col("reconciliation_status") == "MATCH",    1).otherwise(0)).alias("match_count"),
        F.sum(F.when(F.col("reconciliation_status") == "MISMATCH", 1).otherwise(0)).alias("mismatch_count"),
        F.sum(F.when(F.col("reconciliation_status") == "UNPAID",   1).otherwise(0)).alias("unpaid_count"),
        F.sum(F.when(F.col("refund_flag") == True, 1).otherwise(0)).alias("refund_count"),
        F.current_timestamp()                               .alias("gold_updated_at"),
    )
)

total = df_mart.count()
print(f"Mart rows: {total}")
df_mart.show(3, truncate=80)

# ── Cell 5: CREATE TABLE IF NOT EXISTS ────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
    state_name           STRING,
    city_name            STRING,
    station_key          STRING,
    station_name         STRING,
    state_region         STRING,
    year                 INT,
    month                INT,
    year_month           STRING,
    session_count        LONG,
    total_energy_kwh     DOUBLE,
    avg_duration_min     DOUBLE,
    total_expected_aud   DOUBLE,
    total_billed_aud     DOUBLE,
    revenue_variance_aud DOUBLE,
    match_count          LONG,
    mismatch_count       LONG,
    unpaid_count         LONG,
    refund_count         LONG,
    gold_updated_at      TIMESTAMP
)
USING DELTA
LOCATION '{GOLD_PATH}'
""")

# ── Cell 6: MERGE ─────────────────────────────────────────────────────────────
tgt = DeltaTable.forName(spark, GOLD_TABLE)
(
    tgt.alias("t")
       .merge(
           df_mart.alias("s"),
           "t.station_key = s.station_key AND t.year_month = s.year_month"
       )
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute()
)
print(f"MERGE done: {GOLD_TABLE}")

# ── Cell 7: top revenue stations this month ───────────────────────────────────
spark.sql(f"""
SELECT year_month, state_name, station_name,
       session_count, total_billed_aud, mismatch_count
FROM   {GOLD_TABLE}
ORDER  BY year_month DESC, total_billed_aud DESC
LIMIT  20
""").display()
