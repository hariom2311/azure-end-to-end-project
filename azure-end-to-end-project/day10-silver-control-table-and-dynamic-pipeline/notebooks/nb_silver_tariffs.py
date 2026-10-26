# Databricks notebook: Bronze to Silver  |  sl_tariffs
# Source : /Volumes/ev_catalog/bronze/api/tariffs/
# Sink   : ev_catalog.silver.sl_tariffs  (Delta, external)
# Rules  : A-dedup  G-timezone  H-SCD2(recompute is_current from effective_from/to)

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

BRONZE_VOL   = "/Volumes/ev_catalog/bronze/api/tariffs/"
SILVER_TABLE = "ev_catalog.silver.sl_tariffs"
SILVER_PATH  = "abfss://silver@evdatalakedev2.dfs.core.windows.net/tariffs/"
QUARANTINE   = "abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/tariffs/"

dbutils.widgets.text("p_run_date", "", "Run date YYYY-MM-DD (blank = full load)")
dbutils.widgets.text("p_run_type", "", "Run type full/incremental (blank = full load)")
run_date = dbutils.widgets.get("p_run_date").strip()
run_type = dbutils.widgets.get("p_run_type").strip()

# ── Cell 2: read bronze pages ─────────────────────────────────────────────────
if run_date and run_type == 'incremental':
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

# ── Cell 3: explode data array → one row per tariff record ────────────────────
df_exploded = (
    df_pages
    .withColumn("record", F.explode(F.col("data")))
    .select(
        F.col("record.id").cast("long")               .alias("id"),
        F.col("record.tariff_id")                     .alias("tariff_id"),
        F.col("record.version").cast("int")           .alias("version"),
        F.col("record.peak_offpeak")                  .alias("peak_offpeak"),
        F.col("record.rate_per_kwh").cast("double")   .alias("rate_per_kwh"),
        F.col("record.effective_from")                .alias("effective_from"),
        F.col("record.effective_to")                  .alias("effective_to"),
        F.col("record.is_current")                    .alias("is_current_source"),
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
    F.when(F.col("tariff_id").isNull(),        "null_tariff_id")
     .when(F.col("rate_per_kwh").isNull() |
           (F.col("rate_per_kwh") <= 0),       "invalid_rate_per_kwh")
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

# ── Cell 5: Rule A — dedup (latest record per tariff_id by updated_at) ─────────
w = Window.partitionBy("tariff_id").orderBy(F.col("updated_at").desc())
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

# ── Cell 7: Rule H — recompute is_current (effective_to is null OR >= today) ──
df_scd = df_tz.withColumn(
    "is_current",
    F.col("effective_to").isNull() | (F.to_date("effective_to") >= F.current_date())
).drop("is_current_source")

# ── Cell 8: silver metadata columns ──────────────────────────────────────────
df_silver = (
    df_scd
    .withColumn("sl_ingested_at", F.current_timestamp())
    .withColumn("sl_batch_id",    F.lit(run_date if run_date else "full"))
)

# ── Cell 9: preview ───────────────────────────────────────────────────────────
df_silver.show(3, truncate=80)

# ── Cell 10: CREATE TABLE IF NOT EXISTS ───────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    id               LONG,
    tariff_id        STRING,
    version          INT,
    peak_offpeak     STRING,
    rate_per_kwh     DOUBLE,
    effective_from   STRING,
    effective_to     STRING,
    is_current       BOOLEAN,
    created_at_utc   TIMESTAMP,
    created_at_aest  TIMESTAMP,
    updated_at_utc   TIMESTAMP,
    updated_at_aest  TIMESTAMP,
    sl_ingested_at   TIMESTAMP,
    sl_batch_id      STRING
)
USING DELTA
LOCATION '{SILVER_PATH}'
""")

# ── Cell 11: MERGE (upsert on tariff_id) ─────────────────────────────────────
tgt = DeltaTable.forName(spark, SILVER_TABLE)

merge_result = (
    tgt.alias("t")
       .merge(df_silver.alias("s"), "t.tariff_id = s.tariff_id")
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

# ── Cell 13: current tariffs by peak/off-peak ────────────────────────────────
spark.sql(f"""
SELECT
    peak_offpeak,
    is_current,
    COUNT(*)                       AS tariff_count,
    ROUND(MIN(rate_per_kwh), 4)    AS min_rate,
    ROUND(MAX(rate_per_kwh), 4)    AS max_rate,
    ROUND(AVG(rate_per_kwh), 4)    AS avg_rate
FROM {SILVER_TABLE}
GROUP BY peak_offpeak, is_current
ORDER BY is_current DESC, peak_offpeak
""").display()
