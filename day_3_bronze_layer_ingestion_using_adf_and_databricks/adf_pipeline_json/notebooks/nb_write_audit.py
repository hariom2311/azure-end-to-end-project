# Databricks notebook source
# MAGIC %md
# MAGIC ## nb_write_audit
# MAGIC **Called by:** `pl_bronze_api_payments_v3` → `act_write_audit` (Notebook Activity)
# MAGIC
# MAGIC Writes one row to `dbw_ev_intelligence_dev.default.pipeline_audit` Delta table
# MAGIC after every pipeline run (success or failure).
# MAGIC
# MAGIC **Parameters passed by ADF:**
# MAGIC - `pipeline_name` — name of the ADF pipeline
# MAGIC - `load_type` — `full` or `incremental`
# MAGIC - `watermark_value` — `updated_after` value used this run
# MAGIC - `ingestion_date` — Bronze partition date (yyyy-MM-dd)
# MAGIC - `total_pages` — total pages fetched this run
# MAGIC - `status` — `succeeded` or `failed`
# MAGIC - `pipeline_run_id` — ADF RunId GUID

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 1 — Read ADF parameters via dbutils.widgets

dbutils.widgets.text("pipeline_name",   "pl_bronze_api_payments_v3")
dbutils.widgets.text("load_type",       "full")
dbutils.widgets.text("watermark_value", "1900-01-01T00:00:00Z")
dbutils.widgets.text("ingestion_date",  "")
dbutils.widgets.text("total_pages",     "0")
dbutils.widgets.text("status",          "succeeded")
dbutils.widgets.text("pipeline_run_id", "")

pipeline_name   = dbutils.widgets.get("pipeline_name")
load_type       = dbutils.widgets.get("load_type")
watermark_value = dbutils.widgets.get("watermark_value")
ingestion_date  = dbutils.widgets.get("ingestion_date")
total_pages     = int(dbutils.widgets.get("total_pages"))
status          = dbutils.widgets.get("status")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

print(f"pipeline_name   : {pipeline_name}")
print(f"load_type       : {load_type}")
print(f"watermark_value : {watermark_value}")
print(f"ingestion_date  : {ingestion_date}")
print(f"total_pages     : {total_pages}")
print(f"status          : {status}")
print(f"pipeline_run_id : {pipeline_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 2 — Build the audit row and write to Delta table

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

audit_row = Row(
    pipeline_name   = pipeline_name,
    load_type       = load_type,
    watermark_value = watermark_value,
    ingestion_date  = ingestion_date,
    total_pages     = total_pages,
    status          = status,
    pipeline_run_id = pipeline_run_id,
)

df = spark.createDataFrame([audit_row])
df = df.withColumn("run_timestamp", current_timestamp())

(
    df.write
    .format("delta")
    .mode("append")
    .saveAsTable("dbw_ev_intelligence_dev.default.pipeline_audit")
)

print(f"Audit row written — pipeline: {pipeline_name}, status: {status}, load_type: {load_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 3 — Verify the row was written

display(
    spark.sql("""
        SELECT pipeline_name, load_type, watermark_value, ingestion_date,
               total_pages, status, run_timestamp
        FROM   dbw_ev_intelligence_dev.default.pipeline_audit
        ORDER  BY run_timestamp DESC
        LIMIT  5
    """)
)
