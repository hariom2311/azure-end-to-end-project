# Databricks notebook: Bronze to Silver  |  sl_customers
# Source : /Volumes/ev_catalog/bronze/api/customers/
# Sink   : ev_catalog.silver.sl_customers  (Delta, external)
# Rules  : A-dedup  F-PCI-mask(email domain only)  G-timezone

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

BRONZE_VOL   = "/Volumes/ev_catalog/bronze/api/customers/"
SILVER_TABLE = "ev_catalog.silver.sl_customers"
SILVER_PATH  = "abfss://silver@evdatalakedev2.dfs.core.windows.net/customers/"
QUARANTINE   = "abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/customers/"

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

# ── Cell 3: explode data array → one row per customer record ──────────────────
df_exploded = (
    df_pages
    .withColumn("record", F.explode(F.col("data")))
    .select(
        F.col("record.id").cast("long") .alias("id"),
        F.col("record.customer_id")     .alias("customer_id"),
        F.col("record.full_name")       .alias("full_name"),
        F.col("record.email")           .alias("email"),
        F.col("record.phone")           .alias("phone"),
        F.col("record.loyalty_tier")    .alias("loyalty_tier"),
        F.col("record.signup_date")     .alias("signup_date"),
        F.col("record.created_at")      .alias("created_at"),
        F.col("record.updated_at")      .alias("updated_at"),
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
    F.when(F.col("customer_id").isNull(),  "null_customer_id")
     .when(F.col("full_name").isNull(),    "null_full_name")
     .when(F.col("email").isNull(),        "null_email")
     .when(F.col("phone").isNull(),        "null_phone")
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

# ── Cell 5: Rule A — dedup (latest record per customer_id by updated_at) ───────
w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())
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

# ── Cell 7: Rule F — PCI masking (email: keep domain, mask local part) ────────
# e.g. william.anderson4@mail.au → ****@mail.au
df_pci = df_tz.withColumn(
    "email_domain",
    F.regexp_extract(F.col("email"), r"@(.+)$", 1)
).withColumn(
    "email_masked",
    F.concat(F.lit("****@"), F.col("email_domain"))
).drop("email", "email_domain")

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
    customer_id      STRING,
    full_name        STRING,
    email_masked     STRING,
    phone            STRING,
    loyalty_tier     STRING,
    signup_date      STRING,
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

# ── Cell 11: MERGE (upsert on customer_id) ───────────────────────────────────
tgt = DeltaTable.forName(spark, SILVER_TABLE)

merge_result = (
    tgt.alias("t")
       .merge(df_silver.alias("s"), "t.customer_id = s.customer_id")
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

# ── Cell 13: post-load breakdown by loyalty tier ──────────────────────────────
spark.sql(f"""
SELECT
    loyalty_tier,
    COUNT(*) AS customer_count
FROM {SILVER_TABLE}
GROUP BY loyalty_tier
ORDER BY CASE loyalty_tier WHEN 'Platinum' THEN 1 WHEN 'Gold' THEN 2 WHEN 'Silver' THEN 3 ELSE 4 END
""").display()
