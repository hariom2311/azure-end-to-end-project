# Databricks notebook — nb_list_active_sources
#
# Called by: ADF act_list_sources (DatabricksNotebook activity)
# Used in:   pl_bronze_master_foreach, pl_bronze_master_parallel
# Returns:   JSON array string of active source_names via dbutils.notebook.exit()
#
# ADF captures the exit value as:
#   activity('act_list_sources').output.runOutput
# which is then stored in variable v_sources and parsed with @json(variables('v_sources'))
# before being passed to ForEach items.

import json

rows = spark.sql("""
    SELECT source_name
    FROM ev_catalog.ctl.pipeline_control
    WHERE is_active = true
    ORDER BY source_name
""").collect()

source_names = [r["source_name"] for r in rows]

if not source_names:
    raise Exception("No active sources found in ev_catalog.ctl.pipeline_control — check that is_active=true rows exist")

# Exit string must be valid JSON — ForEach uses @json(variables('v_sources')) to parse it
dbutils.notebook.exit(json.dumps(source_names))
