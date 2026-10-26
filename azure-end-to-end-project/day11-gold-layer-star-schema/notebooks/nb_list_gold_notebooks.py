# Databricks notebook: nb_list_gold_notebooks
# Reads ev_catalog.gold.gold_control WHERE is_active = true
# Returns a JSON array string of {notebook_name, layer} objects ordered by run_order.
# Called by pl_gold_dynamic_foreach via act_list_gold_notebooks.
#
# Exit value (parsed by ADF ForEach items expression):
#   '[{"notebook_name":"nb_gold_dim_station","layer":"dim"}, ...]'

# ── Cell 1: read control table ────────────────────────────────────────────────
import json

rows = spark.sql("""
    SELECT notebook_name, layer
    FROM   ev_catalog.gold.gold_control
    WHERE  is_active = true
    ORDER  BY run_order ASC
""").collect()

if not rows:
    dbutils.notebook.exit("[]")

notebooks = [{"notebook_name": r["notebook_name"], "layer": r["layer"]} for r in rows]

print(f"Active Gold notebooks ({len(notebooks)}):")
for nb in notebooks:
    print(f"  [{nb['layer']:4s}]  {nb['notebook_name']}")

# ── Cell 2: exit with JSON string (ADF reads this as runOutput) ───────────────
dbutils.notebook.exit(json.dumps(notebooks))
