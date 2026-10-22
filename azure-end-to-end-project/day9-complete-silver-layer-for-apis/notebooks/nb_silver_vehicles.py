# Databricks notebook: Bronze to Silver  |  sl_vehicles
# Source : /Volumes/ev_catalog/bronze/api/vehicles/
# Sink   : ev_catalog.silver.sl_vehicles  (Delta, external)
# Rules  : A-dedup(is_current=True first)  G-timezone  H-SCD2(is_current already from source)

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

BRONZE_VOL   = "/Volumes/ev_catalog/bronze/api/vehicles/"
SILVER_TABLE = "ev_catalog.silver.sl_vehicles"
SILVER_PATH  = "abfss://silver@evdatalakedev2.dfs.core.windows.net/vehicles/"
QUARANTINE   = "abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/vehicles/"

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

# ── Cell 3: explode data array → one row per vehicle record ───────────────────
df_exploded = (
    df_pages
    .withColumn("record", F.explode(F.col("data")))
    .select(
        F.col("record.id").cast("long")                      .alias("id"),
        F.col("record.vehicle_id")                           .alias("vehicle_id"),
        F.col("record.make")                                 .alias("make"),
        F.col("record.model")                                .alias("model"),
        F.col("record.year").cast("int")                     .alias("year"),
        F.col("record.battery_capacity_kwh").cast("double")  .alias("battery_capacity_kwh"),
        F.col("record.range_km").cast("int")                 .alias("range_km"),
        F.col("record.vehicle_type")                         .alias("vehicle_type"),
        F.col("record.registration_state")                   .alias("registration_state"),
        F.col("record.partner_id")                           .alias("partner_id"),
        F.col("record.effective_from")                       .alias("effective_from"),
        F.col("record.effective_to")                         .alias("effective_to"),
        F.col("record.is_current")                           .alias("is_current"),
        F.col("record.created_at")                           .alias("created_at"),
        F.col("record.updated_at")                           .alias("updated_at"),
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
    F.when(F.col("vehicle_id").isNull(),               "null_vehicle_id")
     .when(F.col("make").isNull(),                     "null_make")
     .when(F.col("model").isNull(),                    "null_model")
     .when(F.col("year").isNull() | (F.col("year") < 1990), "invalid_year")
     .when(F.col("effective_from").isNull(),           "null_effective_from")
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

# ── Cell 5: Rule A — dedup (is_current=True rows preferred, then latest updated_at)
# SCD2 table: keep the current version if duplicates exist for same vehicle_id
w = Window.partitionBy("vehicle_id").orderBy(
    F.col("is_current").desc(),
    F.col("updated_at").desc()
)
df_dedup = (
    df_good
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_src")
)

# ── Cell 6: Rule G — timezone (UTC to AEST/AEDT) ─────────────────────────────
df_silver = (
    df_dedup
    .withColumn("created_at_utc",  F.to_timestamp("created_at"))
    .withColumn("updated_at_utc",  F.to_timestamp("updated_at"))
    .withColumn("created_at_aest", F.from_utc_timestamp("created_at_utc", "Australia/Sydney"))
    .withColumn("updated_at_aest", F.from_utc_timestamp("updated_at_utc", "Australia/Sydney"))
    .drop("created_at", "updated_at")
    .withColumn("sl_ingested_at", F.current_timestamp())
    .withColumn("sl_batch_id",    F.lit(run_date if run_date else "full"))
)

# ── Cell 7: preview ───────────────────────────────────────────────────────────
df_silver.show(3, truncate=80)

# ── Cell 10: CREATE TABLE IF NOT EXISTS ───────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    id                    LONG,
    vehicle_id            STRING,
    make                  STRING,
    model                 STRING,
    year                  INT,
    battery_capacity_kwh  DOUBLE,
    range_km              INT,
    vehicle_type          STRING,
    registration_state    STRING,
    partner_id            STRING,
    effective_from        STRING,
    effective_to          STRING,
    is_current            BOOLEAN,
    created_at_utc        TIMESTAMP,
    created_at_aest       TIMESTAMP,
    updated_at_utc        TIMESTAMP,
    updated_at_aest       TIMESTAMP,
    sl_ingested_at        TIMESTAMP,
    sl_batch_id           STRING
)
USING DELTA
LOCATION '{SILVER_PATH}'
""")

# ── Cell 11: MERGE (upsert on vehicle_id) ────────────────────────────────────
tgt = DeltaTable.forName(spark, SILVER_TABLE)

merge_result = (
    tgt.alias("t")
       .merge(df_silver.alias("s"), "t.vehicle_id = s.vehicle_id")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute()
)

print(f"MERGE done: {SILVER_TABLE}")

# ── Cell 12: run stats ────────────────────────────────────────────────────────
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

# ── Cell 13: fleet breakdown by make and vehicle type ────────────────────────
spark.sql(f"""
SELECT
    vehicle_type,
    make,
    COUNT(*)                                        AS vehicle_count,
    COUNT(CASE WHEN is_current THEN 1 END)          AS current_records,
    ROUND(AVG(battery_capacity_kwh), 1)             AS avg_battery_kwh,
    ROUND(AVG(range_km), 0)                         AS avg_range_km
FROM {SILVER_TABLE}
GROUP BY vehicle_type, make
ORDER BY vehicle_count DESC
""").display()
