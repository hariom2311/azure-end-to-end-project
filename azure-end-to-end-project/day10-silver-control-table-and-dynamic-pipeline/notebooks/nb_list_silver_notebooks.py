# Databricks notebook: nb_list_silver_notebooks
# Reads ev_catalog.ctl.silver_control WHERE is_active = true
# Returns a JSON array string of {notebook_name, entity_name} objects ordered by run_order.
# Called by pl_silver_dynamic_foreach via act_list_silver_notebooks.
#
# Exit value (parsed by ADF ForEach items expression):
#   '[{"notebook_name":"nb_silver_payments","entity_name":"payments"}, ...]'

# ── Cell 1: read control table ────────────────────────────────────────────────
import json

rows = spark.sql("""
    SELECT notebook_name, entity_name
    FROM   ev_catalog.ctl.silver_control
    WHERE  is_active = true
    ORDER  BY run_order ASC
""").collect()

if not rows:
    dbutils.notebook.exit("[]")

notebooks = [{"notebook_name": r["notebook_name"], "entity_name": r["entity_name"]} for r in rows]

print(f"Active Silver notebooks ({len(notebooks)}):")
for nb in notebooks:
    print(f"  {nb['notebook_name']}")

# ── Cell 2: exit with JSON string (ADF reads this as runOutput) ───────────────
dbutils.notebook.exit(json.dumps(notebooks))
