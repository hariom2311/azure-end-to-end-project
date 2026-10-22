# Databricks notebook: Bronze to Silver  |  sl_stations
# Source : /Volumes/ev_catalog/bronze/api/stations/
# Sink   : ev_catalog.silver.sl_stations  (Delta, external)
# Rules  : A-dedup  G-timezone  state_region_flag

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

BRONZE_VOL   = "/Volumes/ev_catalog/bronze/api/stations/"
SILVER_TABLE = "ev_catalog.silver.sl_stations"
SILVER_PATH  = "abfss://silver@evdatalakedev2.dfs.core.windows.net/stations/"
QUARANTINE   = "abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/stations/"

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

# ── Cell 3: explode data array → one row per station record ───────────────────
df_exploded = (
    df_pages
    .withColumn("record", F.explode(F.col("data")))
    .select(
        F.col("record.id").cast("long")               .alias("id"),
        F.col("record.station_id")                    .alias("station_id"),
        F.col("record.name")                          .alias("name"),
        F.col("record.state_code")                    .alias("state_code"),
        F.col("record.city")                          .alias("city"),
        F.col("record.latitude").cast("double")       .alias("latitude"),
        F.col("record.longitude").cast("double")      .alias("longitude"),
        F.col("record.charger_type")                  .alias("charger_type"),
        F.col("record.max_power_kw").cast("double")   .alias("max_power_kw"),
        F.col("record.num_connectors").cast("int")    .alias("num_connectors"),
        F.col("record.operator")                      .alias("operator"),
        F.col("record.is_active")                     .alias("is_active"),
        F.col("record.commissioned_date")             .alias("commissioned_date"),
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
    F.when(F.col("station_id").isNull(),    "null_station_id")
     .when(F.col("name").isNull(),          "null_name")
     .when(F.col("state_code").isNull(),    "null_state_code")
     .when(F.col("latitude").isNull(),      "null_latitude")
     .when(F.col("longitude").isNull(),     "null_longitude")
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

# ── Cell 5: Rule A — dedup (latest record per station_id by updated_at) ────────
w = Window.partitionBy("station_id").orderBy(F.col("updated_at").desc())
df_dedup = (
    df_good
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_src")
)

# ── Cell 6: Rule G — timezone (UTC to AEST/AEDT) ─────────────────────────────
df_tz = (
    df_dedup
    .withColumn("created_at_utc",  F.to_timestamp("created_at"))
    .withColumn("updated_at_utc",  F.to_timestamp("updated_at"))
    .withColumn("created_at_aest", F.from_utc_timestamp("created_at_utc", "Australia/Sydney"))
    .withColumn("updated_at_aest", F.from_utc_timestamp("updated_at_utc", "Australia/Sydney"))
    .drop("created_at", "updated_at")
)

# ── Cell 7: state_region_flag ────────────────────────────────────────────────
df_region = df_tz.withColumn(
    "state_region",
    F.when(F.col("state_code").isin("NSW", "VIC", "QLD"), F.lit("Eastern"))
     .when(F.col("state_code") == "WA",                    F.lit("Western"))
     .otherwise(F.lit("Other"))
)

# ── Cell 8: silver metadata columns ──────────────────────────────────────────
df_silver = (
    df_region
    .withColumn("sl_ingested_at", F.current_timestamp())
    .withColumn("sl_batch_id",    F.lit(run_date if run_date else "full"))
)

# ── Cell 9: preview ───────────────────────────────────────────────────────────
df_silver.show(3, truncate=80)

# ── Cell 10: CREATE TABLE IF NOT EXISTS ───────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    id                  LONG,
    station_id          STRING,
    name                STRING,
    state_code          STRING,
    state_region        STRING,
    city                STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    charger_type        STRING,
    max_power_kw        DOUBLE,
    num_connectors      INT,
    operator            STRING,
    is_active           BOOLEAN,
    commissioned_date   STRING,
    created_at_utc      TIMESTAMP,
    created_at_aest     TIMESTAMP,
    updated_at_utc      TIMESTAMP,
    updated_at_aest     TIMESTAMP,
    sl_ingested_at      TIMESTAMP,
    sl_batch_id         STRING
)
USING DELTA
LOCATION '{SILVER_PATH}'
""")

# ── Cell 11: MERGE (upsert on station_id) ────────────────────────────────────
tgt = DeltaTable.forName(spark, SILVER_TABLE)

merge_result = (
    tgt.alias("t")
       .merge(df_silver.alias("s"), "t.station_id = s.station_id")
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

# ── Cell 13: stations by state and charger type ───────────────────────────────
spark.sql(f"""
SELECT
    state_region,
    state_code,
    charger_type,
    COUNT(*)                                       AS station_count,
    COUNT(CASE WHEN is_active THEN 1 END)          AS active_count,
    ROUND(AVG(max_power_kw), 2)                    AS avg_max_power_kw,
    SUM(num_connectors)                            AS total_connectors
FROM {SILVER_TABLE}
GROUP BY state_region, state_code, charger_type
ORDER BY state_region, state_code, station_count DESC
""").display()
