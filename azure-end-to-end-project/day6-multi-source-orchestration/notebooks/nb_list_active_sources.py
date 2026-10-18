# Databricks notebook — nb_list_active_sources
#
# Called by: ADF act_list_sources (DatabricksNotebook activity)
# Used in:   pl_bronze_master_foreach, pl_bronze_master_parallel
#
# ADF captures the exit value as:
#   activity('act_list_sources').output.runOutput
#
# ForEach items expression:
#   @json(string(activity('act_list_sources').output.runOutput))
#
# string() forces the value to a string regardless of how ADF
# deserialises runOutput. json() then parses it into an array
# for ForEach to iterate.

# noqa: F821  (spark, dbutils are Databricks globals — not visible to linter)

import json

rows = spark.sql("""
    SELECT source_name
    FROM ev_catalog.ctl.pipeline_control
    WHERE is_active = true
    ORDER BY source_name
""").collect()

source_names = [r["source_name"] for r in rows]

if not source_names:
    raise Exception(
        "No active sources found in ev_catalog.ctl.pipeline_control"
        " — check that is_active=true rows exist"
    )

# json.dumps() always produces a plain string — never a list object
exit_value = json.dumps(source_names)
dbutils.notebook.exit(exit_value)
