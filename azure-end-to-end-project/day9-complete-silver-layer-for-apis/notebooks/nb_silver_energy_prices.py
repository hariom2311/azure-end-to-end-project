# Databricks notebook: Bronze to Silver  |  sl_energy_prices
# Source : /Volumes/ev_catalog/bronze/api/energy_prices/
# Sink   : ev_catalog.silver.sl_energy_prices  (Delta, external)
# Rules  : A-dedup  G-timezone(effective_from/to)  is_current flag recompute

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

BRONZE_VOL   = "/Volumes/ev_catalog/bronze/api/energy_prices/"
SILVER_TABLE = "ev_catalog.silver.sl_energy_prices"
SILVER_PATH  = "abfss://silver@evdatalakedev2.dfs.core.windows.net/energy_prices/"
QUARANTINE   = "abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/energy_prices/"

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

# ── Cell 3: explode data array → one row per energy price record ──────────────
df_exploded = (
    df_pages
    .withColumn("record", F.explode(F.col("data")))
    .select(
        F.col("record.id").cast("long")                  .alias("id"),
        F.col("record.price_id")                         .alias("price_id"),
        F.col("record.station_id")                       .alias("station_id"),
        F.col("record.state_code")                       .alias("state_code"),
        F.col("record.price_per_kwh").cast("double")     .alias("price_per_kwh"),
        F.col("record.off_peak_price").cast("double")    .alias("off_peak_price"),
        F.col("record.peak_price").cast("double")        .alias("peak_price"),
        F.col("record.currency")                         .alias("currency"),
        F.col("record.effective_from")                   .alias("effective_from"),
        F.col("record.effective_to")                     .alias("effective_to"),
        F.col("record.tariff_type")                      .alias("tariff_type"),
        F.col("record.created_at")                       .alias("created_at"),
        F.col("record.updated_at")                       .alias("updated_at"),
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
    F.when(F.col("price_id").isNull(),         "null_price_id")
     .when(F.col("station_id").isNull(),       "null_station_id")
     .when(F.col("price_per_kwh").isNull() |
           (F.col("price_per_kwh") <= 0),      "invalid_price_per_kwh")
     .when(F.col("effective_from").isNull(),   "null_effective_from")
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

# ── Cell 5: Rule A — dedup (latest record per price_id by updated_at) ─────────
w = Window.partitionBy("price_id").orderBy(F.col("updated_at").desc())
df_dedup = (
    df_good
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_src")
)

# ── Cell 6: Rule G — timezone (UTC to AEST/AEDT) ─────────────────────────────
df_tz = (
    df_dedup
    .withColumn("effective_from_utc",  F.to_timestamp("effective_from"))
    .withColumn("effective_to_utc",    F.to_timestamp("effective_to"))
    .withColumn("created_at_utc",      F.to_timestamp("created_at"))
    .withColumn("updated_at_utc",      F.to_timestamp("updated_at"))
    .withColumn("effective_from_aest", F.from_utc_timestamp("effective_from_utc", "Australia/Sydney"))
    .withColumn("effective_to_aest",   F.from_utc_timestamp("effective_to_utc",   "Australia/Sydney"))
    .withColumn("created_at_aest",     F.from_utc_timestamp("created_at_utc",     "Australia/Sydney"))
    .withColumn("updated_at_aest",     F.from_utc_timestamp("updated_at_utc",     "Australia/Sydney"))
    .drop("effective_from", "effective_to", "created_at", "updated_at")
)

# ── Cell 7: is_current flag (effective_to is null OR effective_to > now) ──────
df_current = df_tz.withColumn(
    "is_current",
    F.col("effective_to_utc").isNull() | (F.col("effective_to_utc") > F.current_timestamp())
)

# ── Cell 8: silver metadata columns ──────────────────────────────────────────
df_silver = (
    df_current
    .withColumn("sl_ingested_at", F.current_timestamp())
    .withColumn("sl_batch_id",    F.lit(run_date if run_date else "full"))
)

# ── Cell 9: preview ───────────────────────────────────────────────────────────
df_silver.show(3, truncate=80)

# ── Cell 10: CREATE TABLE IF NOT EXISTS ───────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    id                  LONG,
    price_id            STRING,
    station_id          STRING,
    state_code          STRING,
    price_per_kwh       DOUBLE,
    off_peak_price      DOUBLE,
    peak_price          DOUBLE,
    currency            STRING,
    tariff_type         STRING,
    is_current          BOOLEAN,
    effective_from_utc  TIMESTAMP,
    effective_from_aest TIMESTAMP,
    effective_to_utc    TIMESTAMP,
    effective_to_aest   TIMESTAMP,
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

# ── Cell 11: MERGE (upsert on price_id) ──────────────────────────────────────
tgt = DeltaTable.forName(spark, SILVER_TABLE)

merge_result = (
    tgt.alias("t")
       .merge(df_silver.alias("s"), "t.price_id = s.price_id")
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

# ── Cell 13: current prices by state and tariff type ─────────────────────────
spark.sql(f"""
SELECT
    state_code,
    tariff_type,
    COUNT(*)                         AS price_records,
    ROUND(AVG(price_per_kwh), 4)     AS avg_price_per_kwh,
    ROUND(AVG(peak_price), 4)        AS avg_peak_price,
    ROUND(AVG(off_peak_price), 4)    AS avg_off_peak_price
FROM {SILVER_TABLE}
WHERE is_current = true
GROUP BY state_code, tariff_type
ORDER BY state_code, tariff_type
""").display()
