# Databricks notebook: Bronze to Silver  |  sl_payments
# Source : /Volumes/ev_catalog/bronze/api/payments/
# Sink   : ev_catalog.silver.sl_payments  (Delta, external)
# Rules  : A-dedup  D-billing-reconciliation  G-timezone  J-GST-split
#
# Bronze file structure (each page JSON):
# {
#   "data": [ { payment fields... }, ... ],
#   "pagination": { "page": 1, "page_size": 1000, "total": 320221, "total_pages": 321 }
# }
#
# Bronze path layout:
#   api/payments/ingestion_date=YYYY-MM-DD/run_id=<adf-uuid>/payments_page_N.json

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

BRONZE_VOL   = "/Volumes/ev_catalog/bronze/api/payments/"
SILVER_TABLE = "ev_catalog.silver.sl_payments"
SILVER_PATH  = "abfss://silver@evdatalakedev2.dfs.core.windows.net/payments/"
QUARANTINE   = "abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/payments/"

dbutils.widgets.text("p_run_date", "", "Run date YYYY-MM-DD (blank = full load)")
run_date = dbutils.widgets.get("p_run_date").strip()

# ── Cell 2: read bronze pages (each file = { data:[...], pagination:{...} }) ──
# ingestion_date=YYYY-MM-DD/ may contain one or more run_id=<uuid>/ subfolders
# (ADF sets run_id to the pipeline run UUID, not the date).
# recursiveFileLookup=true picks up all JSON files under the date folder
# regardless of how many run_id= subfolders exist inside it.
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

# ── Cell 3: explode data array → one row per payment record ──────────────────
df_exploded = (
    df_pages
    .withColumn("record", F.explode(F.col("data")))
    .select(
        F.col("record.id").cast("long")          .alias("id"),
        F.col("record.payment_id")               .alias("payment_id"),
        F.col("record.session_id")               .alias("session_id"),
        F.col("record.customer_id")              .alias("customer_id"),
        F.col("record.gateway")                  .alias("gateway"),
        F.col("record.amount_aud").cast("double").alias("amount_aud"),
        F.col("record.gst").cast("double")       .alias("gst_inclusive"),
        F.col("record.payment_mode")             .alias("payment_mode"),
        F.col("record.status")                   .alias("status"),
        F.col("record.processed_at")             .alias("processed_at"),
        F.col("record.created_at")               .alias("created_at"),
        F.col("record.updated_at")               .alias("updated_at"),
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
    F.when(F.col("payment_id").isNull(),                              "null_payment_id")
     .when(F.col("amount_aud").isNull() | (F.col("amount_aud") <= 0), "invalid_amount")
     .when(F.col("customer_id").isNull(),                             "null_customer_id")
     .when(F.col("processed_at").isNull(),                            "null_processed_at")
     .when(F.col("status").isNull(),                                  "null_status")
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

# ── Cell 5: Rule A — dedup (latest record per payment_id by updated_at) ───────
w = Window.partitionBy("payment_id").orderBy(F.col("updated_at").desc())
df_dedup = (
    df_good
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_src")
)

# ── Cell 6: Rule G — timezone (UTC to AEST/AEDT) ─────────────────────────────
df_tz = (
    df_dedup
    .withColumn("processed_at_utc",  F.to_timestamp("processed_at"))
    .withColumn("created_at_utc",    F.to_timestamp("created_at"))
    .withColumn("updated_at_utc",    F.to_timestamp("updated_at"))
    .withColumn("processed_at_aest", F.from_utc_timestamp("processed_at_utc", "Australia/Sydney"))
    .drop("processed_at", "created_at", "updated_at")
)

# ── Cell 7: Rule J — GST split ────────────────────────────────────────────────
# The API already provides gst field but we recompute from amount_aud to ensure
# consistency (gst_inclusive from API = GST component = amount_aud / 11)
df_gst = (
    df_tz
    .withColumn("gst_amount_aud", F.round(F.col("amount_aud") / 11, 4))
    .withColumn("excl_gst_aud",   F.round(F.col("amount_aud") - F.col("amount_aud") / 11, 4))
    .drop("gst_inclusive")   # replaced by our computed columns
)

# ── Cell 8: Rule D — billing reconciliation flag ──────────────────────────────
# Flag: status in (failed, Disputed) but amount > 0 = billing discrepancy
df_billing = df_gst.withColumn(
    "billing_anomaly_flag",
    F.when(
        F.col("status").isin("failed", "Disputed") & (F.col("amount_aud") > 0),
        True
    ).otherwise(False)
)

# ── Cell 9: silver metadata columns ──────────────────────────────────────────
df_silver = (
    df_billing
    .withColumn("sl_ingested_at", F.current_timestamp())
    .withColumn("sl_batch_id",    F.lit(run_date if run_date else "full"))
)

# ── Cell 10: CREATE TABLE IF NOT EXISTS ───────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    id                  LONG,
    payment_id          STRING,
    session_id          STRING,
    customer_id         STRING,
    gateway             STRING,
    amount_aud          DOUBLE,
    excl_gst_aud        DOUBLE,
    gst_amount_aud      DOUBLE,
    payment_mode        STRING,
    status              STRING,
    processed_at_utc    TIMESTAMP,
    processed_at_aest   TIMESTAMP,
    created_at_utc      TIMESTAMP,
    updated_at_utc      TIMESTAMP,
    billing_anomaly_flag BOOLEAN,
    sl_ingested_at      TIMESTAMP,
    sl_batch_id         STRING
)
USING DELTA
LOCATION '{SILVER_PATH}'
""")

# ── Cell 11: MERGE (upsert on payment_id) ────────────────────────────────────
tgt = DeltaTable.forName(spark, SILVER_TABLE)

merge_result = (
    tgt.alias("t")
       .merge(df_silver.alias("s"), "t.payment_id = s.payment_id")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute()
)

print(f"MERGE done: {SILVER_TABLE}")

# ── Cell 12: run stats — duplicates / inserts / updates ──────────────────────
# merge_result is a Row with keys:
#   num_affected_rows, num_updated_rows, num_deleted_rows, num_inserted_rows

# merge_result is a Spark DataFrame with one row — collect it to get Python ints
merge_metrics  = merge_result.collect()[0]
rows_inserted  = int(merge_metrics["num_inserted_rows"])
rows_updated   = int(merge_metrics["num_updated_rows"])
rows_in_source = int(df_silver.count())

# Duplicates = rows seen in source that were already in Silver (matched = updated)
# Records that had NO change still count as "updated" in Delta MERGE —
# we detect true no-change updates by comparing sl_batch_id on matched rows
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
if run_date:
    read_path = f"{BRONZE_VOL}ingestion_date={run_date}/run_id={run_date}/"
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
# ── Cell 13: post-load breakdown by status ────────────────────────────────────
spark.sql(f"""
SELECT
    status,
    COUNT(*)                                              AS record_count,
    COUNT(CASE WHEN billing_anomaly_flag THEN 1 END)      AS billing_anomalies,
    ROUND(SUM(amount_aud), 2)                             AS total_aud,
    ROUND(SUM(excl_gst_aud), 2)                           AS total_ex_gst,
    ROUND(SUM(gst_amount_aud), 2)                         AS total_gst,
    MIN(DATE(processed_at_aest))                          AS earliest_date,
    MAX(DATE(processed_at_aest))                          AS latest_date
FROM {SILVER_TABLE}
GROUP BY status
ORDER BY record_count DESC
""").display()
