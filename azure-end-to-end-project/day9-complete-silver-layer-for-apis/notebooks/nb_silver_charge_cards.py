# Databricks notebook: Bronze to Silver  |  sl_charge_cards
# Source : /Volumes/ev_catalog/bronze/api/charge_cards/
# Sink   : ev_catalog.silver.sl_charge_cards  (Delta, external)
# Rules  : A-dedup  F-PCI-mask(card_number->card_last4)  G-timezone

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

BRONZE_VOL   = "/Volumes/ev_catalog/bronze/api/charge_cards/"
SILVER_TABLE = "ev_catalog.silver.sl_charge_cards"
SILVER_PATH  = "abfss://silver@evdatalakedev2.dfs.core.windows.net/charge_cards/"
QUARANTINE   = "abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/charge_cards/"

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

# ── Safe-cast "data" when Spark inferred it as STRING (empty-array partitions) ─
# Root cause: if ALL Bronze files in a partition contain "data": [], Spark infers
# the column as STRING. from_json re-parses it using the schema from non-empty rows.
from pyspark.sql.types import StringType, ArrayType
data_type = dict(df_pages.dtypes).get("data", "unknown")
if data_type == "string":
    # Use schema from a non-empty row; if none exists the whole load is empty data
    sample = df_pages.filter(F.col("data").isNotNull() & (F.col("data") != "[]")).limit(1).collect()
    if sample:
        inferred_schema = spark.read.json(
            spark.sparkContext.parallelize([sample[0]["data"]])
        ).schema
        array_schema = ArrayType(inferred_schema)
        df_pages = df_pages.withColumn("data", F.from_json(F.col("data"), array_schema))
    else:
        print("WARNING: No non-empty data rows found — partition may be fully empty.")
        dbutils.notebook.exit("NO_DATA")
df_pages = df_pages.filter(F.col("data").isNotNull() & (F.size(F.col("data")) > 0))

# ── Cell 3: explode data array → one row per charge card record ───────────────
df_exploded = (
    df_pages
    .withColumn("record", F.explode(F.col("data")))
    .select(
        F.col("record.id").cast("long")   .alias("id"),
        F.col("record.card_id")           .alias("card_id"),
        F.col("record.customer_id")       .alias("customer_id"),
        F.col("record.rfid_uid")          .alias("rfid_uid"),
        F.col("record.card_number")       .alias("card_number"),
        F.col("record.card_type")         .alias("card_type"),
        F.col("record.status")            .alias("status"),
        F.col("record.created_at")        .alias("created_at"),
        F.col("record.updated_at")        .alias("updated_at"),
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
    F.when(F.col("card_id").isNull(),      "null_card_id")
     .when(F.col("customer_id").isNull(),  "null_customer_id")
     .when(F.col("card_number").isNull(),  "null_card_number")
     .when(F.col("status").isNull(),       "null_status")
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

# ── Cell 5: Rule A — dedup (latest record per card_id by updated_at) ──────────
w = Window.partitionBy("card_id").orderBy(F.col("updated_at").desc())
df_dedup = (
    df_good
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_src")
)

# ── Cell 6: Rule G — timezone (UTC to AEST/AEDT) ─────────────────────────────
df_tz = (
    df_dedup
    .withColumn("created_at_utc",   F.to_timestamp("created_at"))
    .withColumn("updated_at_utc",   F.to_timestamp("updated_at"))
    .withColumn("created_at_aest",  F.from_utc_timestamp("created_at_utc", "Australia/Sydney"))
    .withColumn("updated_at_aest",  F.from_utc_timestamp("updated_at_utc", "Australia/Sydney"))
    .drop("created_at", "updated_at")
)

# ── Cell 7: Rule F — PCI masking (card_number → last 4 digits only) ───────────
df_pci = (
    df_tz
    .withColumn("card_last4", F.regexp_extract(F.col("card_number"), r"(\d{4})$", 1))
    .drop("card_number")
)

# ── Cell 8: silver metadata columns ──────────────────────────────────────────
df_silver = (
    df_pci
    .withColumn("sl_ingested_at", F.current_timestamp())
    .withColumn("sl_batch_id",    F.lit(run_date if run_date else "full"))
)

# ── Cell 9: preview ───────────────────────────────────────────────────────────
df_silver.show(3, truncate=80)

# ── Cell 10: CREATE TABLE IF NOT EXISTS ───────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    id               LONG,
    card_id          STRING,
    customer_id      STRING,
    rfid_uid         STRING,
    card_last4       STRING,
    card_type        STRING,
    status           STRING,
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

# ── Cell 11: MERGE (upsert on card_id) ───────────────────────────────────────
tgt = DeltaTable.forName(spark, SILVER_TABLE)

merge_result = (
    tgt.alias("t")
       .merge(df_silver.alias("s"), "t.card_id = s.card_id")
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

# ── Cell 13: post-load breakdown ──────────────────────────────────────────────
spark.sql(f"""
SELECT
    card_type,
    status,
    COUNT(*)   AS record_count,
    COUNT(CASE WHEN card_last4 IS NOT NULL THEN 1 END) AS masked_cards
FROM {SILVER_TABLE}
GROUP BY card_type, status
ORDER BY record_count DESC
""").display()
