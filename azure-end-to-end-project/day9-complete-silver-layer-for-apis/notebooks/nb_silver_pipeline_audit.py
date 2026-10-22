# Databricks notebook: Bronze to Silver  |  sl_pipeline_audit
# Source : /Volumes/ev_catalog/bronze/api/pipeline_audit/
# Sink   : ev_catalog.silver.sl_pipeline_audit  (Delta, external)
# Rules  : A-dedup  G-timezone(started_at/ended_at)  success_rate_pct  has_error_flag

# ── Cell 1: imports and config ────────────────────────────────────────────────
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

BRONZE_VOL   = "/Volumes/ev_catalog/bronze/api/pipeline_audit/"
SILVER_TABLE = "ev_catalog.silver.sl_pipeline_audit"
SILVER_PATH  = "abfss://silver@evdatalakedev2.dfs.core.windows.net/pipeline_audit/"
QUARANTINE   = "abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/pipeline_audit/"

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

# ── Cell 3: explode data array → one row per audit record ─────────────────────
df_exploded = (
    df_pages
    .withColumn("record", F.explode(F.col("data")))
    .select(
        F.col("record.id").cast("long")           .alias("id"),
        F.col("record.run_id")                    .alias("run_id"),
        F.col("record.pipeline_name")             .alias("pipeline_name"),
        F.col("record.source_name")               .alias("source_name"),
        F.col("record.load_type")                 .alias("load_type"),
        F.col("record.status")                    .alias("status"),
        F.col("record.rows_read").cast("long")    .alias("rows_read"),
        F.col("record.rows_written").cast("long") .alias("rows_written"),
        F.col("record.rows_rejected").cast("long").alias("rows_rejected"),
        F.col("record.started_at")                .alias("started_at"),
        F.col("record.ended_at")                  .alias("ended_at"),
        F.col("record.duration_sec").cast("long") .alias("duration_sec"),
        F.col("record.error_message")             .alias("error_message"),
        F.col("record.sla_met")                   .alias("sla_met"),
        F.col("record.watermark_value")           .alias("watermark_value"),
        F.col("record.created_at")                .alias("created_at"),
        F.col("record.updated_at")                .alias("updated_at"),
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
    F.when(F.col("run_id").isNull(),         "null_run_id")
     .when(F.col("pipeline_name").isNull(),  "null_pipeline_name")
     .when(F.col("status").isNull(),         "null_status")
     .when(F.col("started_at").isNull(),     "null_started_at")
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

# ── Cell 5: Rule A — dedup (latest record per run_id by updated_at) ───────────
w = Window.partitionBy("run_id").orderBy(F.col("updated_at").desc())
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

# ── Cell 7: success_rate_pct and has_error_flag ───────────────────────────────
df_enriched = df_tz.withColumn(
    "success_rate_pct",
    F.when(
        F.col("rows_read").isNotNull() & (F.col("rows_read") > 0),
        F.round(F.col("rows_written").cast("double") / F.col("rows_read") * 100, 2)
    ).otherwise(F.lit(None).cast("double"))
).withColumn(
    "has_error_flag",
    F.col("error_message").isNotNull() & (F.trim(F.col("error_message")) != "")
)

# ── Cell 8: silver metadata columns ──────────────────────────────────────────
df_silver = (
    df_enriched
    .withColumn("sl_ingested_at", F.current_timestamp())
    .withColumn("sl_batch_id",    F.lit(run_date if run_date else "full"))
)

# ── Cell 9: preview ───────────────────────────────────────────────────────────
df_silver.show(3, truncate=80)

# ── Cell 10: CREATE TABLE IF NOT EXISTS ───────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    id                  LONG,
    run_id              STRING,
    pipeline_name       STRING,
    source_name         STRING,
    load_type           STRING,
    status              STRING,
    rows_read           LONG,
    rows_written        LONG,
    rows_rejected       LONG,
    duration_sec        LONG,
    error_message       STRING,
    sla_met             BOOLEAN,
    watermark_value     STRING,
    success_rate_pct    DOUBLE,
    has_error_flag      BOOLEAN,
    started_at_utc      TIMESTAMP,
    started_at_aest     TIMESTAMP,
    ended_at_utc        TIMESTAMP,
    ended_at_aest       TIMESTAMP,
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

# ── Cell 11: MERGE (upsert on run_id) ────────────────────────────────────────
tgt = DeltaTable.forName(spark, SILVER_TABLE)

merge_result = (
    tgt.alias("t")
       .merge(df_silver.alias("s"), "t.run_id = s.run_id")
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

# ── Cell 13: pipeline run summary ────────────────────────────────────────────
spark.sql(f"""
SELECT
    pipeline_name,
    status,
    COUNT(*)                                              AS run_count,
    ROUND(AVG(success_rate_pct), 2)                       AS avg_success_rate_pct,
    ROUND(AVG(duration_sec), 0)                           AS avg_duration_sec,
    COUNT(CASE WHEN has_error_flag THEN 1 END)            AS runs_with_errors,
    COUNT(CASE WHEN sla_met = false THEN 1 END)           AS sla_breached_runs
FROM {SILVER_TABLE}
GROUP BY pipeline_name, status
ORDER BY pipeline_name, run_count DESC
""").display()
