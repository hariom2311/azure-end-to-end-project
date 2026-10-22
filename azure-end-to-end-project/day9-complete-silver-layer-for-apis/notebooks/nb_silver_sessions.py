# Databricks notebook: Bronze to Silver  |  sl_sessions
# Source : /Volumes/ev_catalog/bronze/api/sessions/
# Sink   : ev_catalog.silver.sl_sessions  (Delta, external)
# Rules  : A-dedup  G-timezone(started_at/ended_at)  J-GST-split(cost_aud)  I-SLA-breach(>4h)  B-late-event-flag

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

BRONZE_VOL   = "/Volumes/ev_catalog/bronze/api/sessions/"
SILVER_TABLE = "ev_catalog.silver.sl_sessions"
SILVER_PATH  = "abfss://silver@evdatalakedev2.dfs.core.windows.net/sessions/"
QUARANTINE   = "abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/sessions/"

dbutils.widgets.text("p_run_date", "", "Run date YYYY-MM-DD (blank = full load)")
run_date = dbutils.widgets.get("p_run_date").strip()

# ── Cell 2: read bronze pages ─────────────────────────────────────────────────
if run_date:
    read_path = f"{BRONZE_VOL}ingestion_date={run_date}/"
else:
    read_path = BRONZE_VOL

print(f"Reading from: {read_path}")

df_pages = (
    spark.read.format("json")
    .option("multiline", "true")
    .option("recursiveFileLookup", "true")
    .load(read_path)
    .withColumn("_src", F.col("_metadata.file_path"))
)

# ── Cell 3: explode data array → one row per session record ───────────────────
df_exploded = (
    df_pages
    .withColumn("record", F.explode(F.col("data")))
    .select(
        F.col("record.id").cast("long")               .alias("id"),
        F.col("record.session_id")                    .alias("session_id"),
        F.col("record.vehicle_id")                    .alias("vehicle_id"),
        F.col("record.station_id")                    .alias("station_id"),
        F.col("record.customer_id")                   .alias("customer_id"),
        F.col("record.started_at")                    .alias("started_at"),
        F.col("record.ended_at")                      .alias("ended_at"),
        F.col("record.duration_min").cast("long")     .alias("duration_min"),
        F.col("record.energy_kwh").cast("double")     .alias("energy_kwh"),
        F.col("record.cost_aud").cast("double")       .alias("cost_aud"),
        F.col("record.peak_power_kw").cast("double")  .alias("peak_power_kw"),
        F.col("record.connector_type")                .alias("connector_type"),
        F.col("record.session_status")                .alias("session_status"),
        F.col("record.payment_id")                    .alias("payment_id"),
        F.col("record.created_at")                    .alias("created_at"),
        F.col("record.updated_at")                    .alias("updated_at"),
        F.col("_src"),
    )
)

total_records = df_exploded.count()
print(f"Total records after explode: {total_records}")

if total_records == 0:
    print("All pages returned empty data arrays — nothing to process. Exiting.")
    dbutils.notebook.exit("NO_DATA")

df_exploded.show(3, truncate=80)

# ── Cell 4: quality checks — split good vs bad, write quarantine ──────────────
df_checked = df_exploded.withColumn(
    "rejection_reason",
    F.when(F.col("session_id").isNull(),                              "null_session_id")
     .when(F.col("customer_id").isNull(),                             "null_customer_id")
     .when(F.col("station_id").isNull(),                              "null_station_id")
     .when(F.col("started_at").isNull(),                              "null_started_at")
     .when(F.col("cost_aud").isNull() | (F.col("cost_aud") < 0),     "invalid_cost_aud")
     .when(F.col("energy_kwh").isNull() | (F.col("energy_kwh") < 0), "invalid_energy_kwh")
     .otherwise(None)
)

df_bad  = df_checked.filter(F.col("rejection_reason").isNotNull())
df_good = df_checked.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")

bad_count = df_bad.count()
if bad_count > 0:
    (df_bad.withColumn("quarantine_ts", F.current_timestamp())
     .write.format("delta").mode("append").save(QUARANTINE))
    print(f"Quarantined: {bad_count} rows")
else:
    print("No bad rows — quarantine not written")

# ── Cell 5: Rule A — dedup (latest record per session_id by updated_at) ────────
w = Window.partitionBy("session_id").orderBy(F.col("updated_at").desc())
df_dedup = (
    df_good
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_src")
)

# ── Cell 6: Rule G — timezone (UTC to AEST/AEDT) ─────────────────────────────
df_tz = (
    df_dedup
    .withColumn("started_at_utc",  F.to_timestamp("started_at"))
    .withColumn("ended_at_utc",    F.to_timestamp("ended_at"))
    .withColumn("created_at_utc",  F.to_timestamp("created_at"))
    .withColumn("updated_at_utc",  F.to_timestamp("updated_at"))
    .withColumn("started_at_aest", F.from_utc_timestamp("started_at_utc", "Australia/Sydney"))
    .withColumn("ended_at_aest",   F.from_utc_timestamp("ended_at_utc",   "Australia/Sydney"))
    .withColumn("created_at_aest", F.from_utc_timestamp("created_at_utc", "Australia/Sydney"))
    .withColumn("updated_at_aest", F.from_utc_timestamp("updated_at_utc", "Australia/Sydney"))
    .drop("started_at", "ended_at", "created_at", "updated_at")
)

# ── Cell 7: Rule J — GST split on cost_aud ───────────────────────────────────
df_gst = (
    df_tz
    .withColumn("gst_amount_aud", F.round(F.col("cost_aud") / 11, 4))
    .withColumn("excl_gst_aud",   F.round(F.col("cost_aud") - F.col("cost_aud") / 11, 4))
)

# ── Cell 8: Rule I — SLA breach flag (session > 4 hours = 240 min) ────────────
df_sla = df_gst.withColumn(
    "sla_breach_flag",
    F.col("duration_min") > 240
)

# ── Cell 9: Rule B — late event flag (started more than 24h before ingestion) ──
# ingestion = created_at_utc used as proxy for ingestion time
df_late = df_sla.withColumn(
    "late_event_flag",
    (F.unix_timestamp("created_at_utc") - F.unix_timestamp("started_at_utc")) > 86400
)

# ── Cell 10: silver metadata columns ─────────────────────────────────────────
df_silver = (
    df_late
    .withColumn("sl_ingested_at", F.current_timestamp())
    .withColumn("sl_batch_id",    F.lit(run_date if run_date else "full"))
)

# ── Cell 11: CREATE TABLE IF NOT EXISTS ───────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    id                LONG,
    session_id        STRING,
    vehicle_id        STRING,
    station_id        STRING,
    customer_id       STRING,
    duration_min      LONG,
    energy_kwh        DOUBLE,
    cost_aud          DOUBLE,
    excl_gst_aud      DOUBLE,
    gst_amount_aud    DOUBLE,
    peak_power_kw     DOUBLE,
    connector_type    STRING,
    session_status    STRING,
    payment_id        STRING,
    sla_breach_flag   BOOLEAN,
    late_event_flag   BOOLEAN,
    started_at_utc    TIMESTAMP,
    started_at_aest   TIMESTAMP,
    ended_at_utc      TIMESTAMP,
    ended_at_aest     TIMESTAMP,
    created_at_utc    TIMESTAMP,
    created_at_aest   TIMESTAMP,
    updated_at_utc    TIMESTAMP,
    updated_at_aest   TIMESTAMP,
    sl_ingested_at    TIMESTAMP,
    sl_batch_id       STRING
)
USING DELTA
LOCATION '{SILVER_PATH}'
""")

# ── Cell 12: MERGE (upsert on session_id) ────────────────────────────────────
tgt = DeltaTable.forName(spark, SILVER_TABLE)

merge_result = (
    tgt.alias("t")
       .merge(df_silver.alias("s"), "t.session_id = s.session_id")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute()
)

print(f"MERGE done: {SILVER_TABLE}")

# ── Cell 13: run stats ────────────────────────────────────────────────────────
merge_metrics  = merge_result.collect()[0]
rows_inserted  = int(merge_metrics["num_inserted_rows"])
rows_updated   = int(merge_metrics["num_updated_rows"])
rows_in_source = int(df_silver.count())
rows_duplicate = rows_in_source - rows_inserted - rows_updated

print("\n" + "=" * 55)
print(f"  PIPELINE RUN STATS   batch_id = {run_date if run_date else 'full'}")
print("=" * 55)
print(f"  Source records read (after explode + dedup) : {rows_in_source:>10,}")
print(f"  Quarantined (bad quality)                   : {bad_count:>10,}")
print(f"  NEW rows inserted into Silver               : {rows_inserted:>10,}")
print(f"  EXISTING rows updated in Silver             : {rows_updated:>10,}")
print(f"  Duplicates removed before MERGE             : {rows_duplicate:>10,}")
print("=" * 55)

if rows_inserted == 0 and rows_updated == 0:
    print("  WARNING: Zero rows written — check Bronze partition exists for this date.")
elif rows_inserted == 0 and rows_updated > 0:
    print("  INFO: All records already existed — pipeline re-run, Silver is up to date.")
else:
    print("  OK: Silver updated successfully.")
print("=" * 55)

# ── Cell 14: post-load session breakdown ─────────────────────────────────────
spark.sql(f"""
SELECT
    session_status,
    connector_type,
    COUNT(*)                                              AS session_count,
    ROUND(AVG(duration_min), 1)                           AS avg_duration_min,
    ROUND(AVG(energy_kwh), 2)                             AS avg_energy_kwh,
    ROUND(SUM(cost_aud), 2)                               AS total_cost_aud,
    COUNT(CASE WHEN sla_breach_flag THEN 1 END)           AS sla_breaches,
    COUNT(CASE WHEN late_event_flag THEN 1 END)           AS late_events
FROM {SILVER_TABLE}
GROUP BY session_status, connector_type
ORDER BY session_count DESC
""").display()
