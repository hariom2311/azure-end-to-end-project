"""
Silver Transformation: Charging Sessions (Blob CSV -> Silver Delta)

Source : /Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/
Sink   : /Volumes/dbw_ev_intelligence_dev/default/silver-volume/realtime/charging_sessions/

Steps:
  1. Clear cache / stale state
  2. Read Bronze CSV files (19-col schema, recursiveFileLookup)
  3. Derive updated_at from file path (YYYY/MM/DD/HH partition)
  4. Cast all columns to target types
  5. Trim whitespace from string columns
  6. Add Silver audit columns
  7. Drop NULL session_id / NULL updated_at rows
  8. Deduplicate on session_id (latest updated_at wins)
  9. Write to Silver as Delta (full overwrite)
 10. Verify output
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime, timezone

# ── 1. Clear cached state ─────────────────────────────────────────────────────

spark.catalog.clearCache()

for v in spark.catalog.listTables("default"):
    if v.isTemporary:
        spark.catalog.dropTempView(v.name)

print("Cache cleared")

# ── 2. Constants ──────────────────────────────────────────────────────────────

BRONZE_PATH = "/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions"
SILVER_PATH = "/Volumes/dbw_ev_intelligence_dev/default/silver-volume/realtime/charging_sessions"

RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"Bronze path : {BRONZE_PATH}")
print(f"Silver path : {SILVER_PATH}")
print(f"Run time    : {RUN_TS}")

# ── 3. Read Bronze CSV + derive updated_at + cast all columns ─────────────────

CS_SCHEMA = StructType([
    StructField("session_id",          StringType(), True),
    StructField("charger_id",          StringType(), True),
    StructField("station_id",          StringType(), True),
    StructField("vehicle_id",          StringType(), True),
    StructField("customer_id",         StringType(), True),
    StructField("plug_in_ts",          StringType(), True),
    StructField("charge_end_ts",       StringType(), True),
    StructField("duration_min",        StringType(), True),
    StructField("energy_kwh",          StringType(), True),
    StructField("peak_kw",             StringType(), True),
    StructField("connector_type",      StringType(), True),
    StructField("session_status",      StringType(), True),
    StructField("tariff_id",           StringType(), True),
    StructField("raw_device_temp_c",   StringType(), True),
    StructField("signal_strength_dbm", StringType(), True),
    StructField("firmware_ver",        StringType(), True),
    StructField("state_code",          StringType(), True),
    StructField("protocol_version",    StringType(), True),
    StructField("ingested_at",         StringType(), True),
])
CS_CSV_COLS = [f.name for f in CS_SCHEMA.fields]

# Step A: read raw strings
cs_raw = (
    spark.read
    .option("header", "true")
    .option("recursiveFileLookup", "true")
    .schema(CS_SCHEMA)
    .csv(BRONZE_PATH)
    .select(
        *[F.col(c) for c in CS_CSV_COLS],
        F.col("_metadata.file_path").alias("_file_path"),
    )
)

# Step B: derive updated_at string from file path partition YYYY/MM/DD/HH
cs_raw = (
    cs_raw
    .withColumn("_year",    F.regexp_extract(F.col("_file_path"), r"/(\d{4})/", 1))
    .withColumn("_month",   F.regexp_extract(F.col("_file_path"), r"/\d{4}/(\d{2})/", 1))
    .withColumn("_day",     F.regexp_extract(F.col("_file_path"), r"/\d{4}/\d{2}/(\d{2})/", 1))
    .withColumn("_hour",    F.regexp_extract(F.col("_file_path"), r"/\d{4}/\d{2}/\d{2}/(\d{2})/", 1))
    .withColumn("_upd_str", F.concat_ws(" ",
                                F.concat_ws("-", F.col("_year"), F.col("_month"), F.col("_day")),
                                F.col("_hour")))
    .drop("_file_path", "_year", "_month", "_day", "_hour")
)

# Step C: cast all columns in one select.
# Serverless clusters enforce ANSI mode — strict casts throw on bad values instead
# of returning NULL. Use try_cast() for all numeric/timestamp columns so malformed
# values become NULL and are either kept (non-key cols) or dropped later (key cols).
#
# Timestamps: CSV values are either ISO strings or Unix epoch floats ("13.85").
# Try ISO first; fall back to epoch-float; anything else → NULL.
def _safe_ts(col_name):
    iso   = F.try_to_timestamp(F.col(col_name))
    epoch = F.try_to_timestamp(F.col(col_name).try_cast("double").cast("string"))
    return F.when(iso.isNotNull(), iso).otherwise(epoch).alias(col_name)

typed_df = cs_raw.select(
    F.col("session_id").cast("string"),
    F.col("charger_id").cast("string"),
    F.col("station_id").cast("string"),
    F.col("vehicle_id").cast("string"),
    F.col("customer_id").cast("string"),
    _safe_ts("plug_in_ts"),
    _safe_ts("charge_end_ts"),
    F.col("duration_min").try_cast("integer"),
    F.col("energy_kwh").try_cast("decimal(10,4)"),
    F.col("peak_kw").try_cast("decimal(10,4)"),
    F.col("connector_type").cast("string"),
    F.col("session_status").cast("string"),
    F.col("tariff_id").cast("string"),
    F.col("raw_device_temp_c").try_cast("decimal(6,2)"),
    F.col("signal_strength_dbm").try_cast("integer"),
    F.col("firmware_ver").cast("string"),
    F.col("state_code").cast("string"),
    F.col("protocol_version").cast("string"),
    _safe_ts("ingested_at"),
    F.to_timestamp(F.col("_upd_str"), "yyyy-MM-dd HH").alias("updated_at"),
)

print(f"Row count : {typed_df.count()}")
typed_df.printSchema()
typed_df.show(3, truncate=True)

# ── 4. Trim whitespace from all string columns ────────────────────────────────

string_cols = [c for c, t in typed_df.dtypes if t == "string"]
trimmed_df  = typed_df
for col in string_cols:
    trimmed_df = trimmed_df.withColumn(col, F.trim(F.col(col)))

print(f"Trimmed string columns: {string_cols}")

# ── 5. Add Silver audit columns ───────────────────────────────────────────────

audited_df = (
    trimmed_df
    .withColumn("silver_ingested_at", F.lit(RUN_TS).cast("timestamp"))
    .withColumn("silver_load_type",   F.lit("full"))
    .withColumn("silver_pipeline",    F.lit("pl_silver_blob_charging_sessions_v1"))
)

print("After adding audit columns:")
audited_df.printSchema()

# ── 6. Drop rows with NULL session_id or NULL updated_at ─────────────────────
# NULL session_id: all null-keyed rows land in one dedup partition, one arbitrary
#   corrupt row survives into Silver.
# NULL updated_at: happens when the file path doesn't match YYYY/MM/DD/HH pattern;
#   regexp_extract returns "" -> to_timestamp returns NULL silently.

audited_df = audited_df.filter(
    F.col("session_id").isNotNull() &
    (F.trim(F.col("session_id")) != "") &
    F.col("updated_at").isNotNull()
)

print(f"Rows after NULL guard: {audited_df.count()}")

# ── 7. Deduplicate on session_id (latest updated_at wins) ────────────────────

window = Window.partitionBy("session_id").orderBy(F.col("updated_at").desc())

deduped_df = (
    audited_df
    .withColumn("_row_num", F.row_number().over(window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

before = audited_df.count()
after  = deduped_df.count()
print(f"Before dedup       : {before}")
print(f"After dedup        : {after}")
print(f"Duplicates removed : {before - after}")

# ── 8. Write to Silver as Delta table (full overwrite) ────────────────────────

(
    deduped_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(SILVER_PATH)
)

print(f"Written to  : {SILVER_PATH}")
print(f"Rows written: {deduped_df.count()}")

# ── 9. Verify Silver output ───────────────────────────────────────────────────

silver_df = spark.read.format("delta").load(SILVER_PATH)

print("Silver charging_sessions schema:")
silver_df.printSchema()
print(f"\nTotal rows: {silver_df.count()}")
silver_df.show(5, truncate=True)

print("\nNull check on session_id (should be 0):")
print(silver_df.filter(F.col("session_id").isNull()).count())

print("\nStatus distribution:")
silver_df.groupBy("session_status").count().orderBy("count", ascending=False).show()
