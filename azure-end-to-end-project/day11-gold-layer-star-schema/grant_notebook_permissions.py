"""
grant_notebook_permissions.py
─────────────────────────────────────────────────────────────────────────────
Run this in a Databricks notebook (as admin) OR locally with the Databricks
CLI / SDK configured.

PURPOSE
  The ADF Service Principal (9245da2f-7129-4e0c-92c4-134fe896e294) gets:
    "Unable to access the notebook ... lacks the required permissions"
  even though Unity Catalog SQL grants are in place.

  Unity Catalog grants (grants_adf_sp.sql) control TABLE/SCHEMA/CATALOG access.
  Notebook access is a WORKSPACE-LEVEL ACL — a completely separate system
  managed via the Databricks Permissions API, not SQL GRANT.

  The ADF "Notebook" activity resolves the notebook path under the caller's
  workspace identity. The SP must have CAN_RUN on the notebook (or the folder
  containing it) in the Databricks workspace ACL.

WHAT THIS SCRIPT DOES
  Grants CAN_RUN to the ADF SP on every Gold notebook under:
    /Users/hariomsuryawanshi68258@gmail.com/gold/

  Also grants CAN_RUN on the parent /gold/ folder so future notebooks
  added to that folder are automatically covered.

HOW TO RUN
  Option A — Run inside a Databricks notebook as an admin user:
    1. Paste this script into a cell (or upload the file).
    2. Run it. The script uses dbutils.secrets and the Databricks REST API.

  Option B — Run from Databricks CLI on your laptop:
    pip install databricks-sdk
    python grant_notebook_permissions.py

VARIABLES TO UPDATE IF PATHS DIFFER
  WORKSPACE_URL  : your Databricks workspace URL
  SP_APPLICATION_ID : the SP's application/client ID (used as principal_name)
  GOLD_FOLDER_PATH  : workspace path to your gold notebooks folder
─────────────────────────────────────────────────────────────────────────────
"""

import requests
import json

# ── Config ────────────────────────────────────────────────────────────────────
WORKSPACE_URL    = "https://<your-databricks-workspace>.azuredatabricks.net"
SP_APPLICATION_ID = "9245da2f-7129-4e0c-92c4-134fe896e294"   # ADF SP client ID

# Path where gold notebooks live in the workspace
GOLD_FOLDER_PATH = "/Users/hariomsuryawanshi68258@gmail.com/gold"

# Individual notebook paths (in case folder-level grant is not enough)
GOLD_NOTEBOOKS = [
    "/Users/hariomsuryawanshi68258@gmail.com/gold/nb_gold_dim_station",
    "/Users/hariomsuryawanshi68258@gmail.com/gold/nb_gold_dim_customer",
    "/Users/hariomsuryawanshi68258@gmail.com/gold/nb_gold_dim_charger",
    "/Users/hariomsuryawanshi68258@gmail.com/gold/nb_gold_dim_tariff",
    "/Users/hariomsuryawanshi68258@gmail.com/gold/nb_gold_fact_charging_session",
    "/Users/hariomsuryawanshi68258@gmail.com/gold/nb_gold_fact_payments",
    "/Users/hariomsuryawanshi68258@gmail.com/gold/nb_gold_mart_revenue_by_geo_month",
]

# ── Token — two options ───────────────────────────────────────────────────────
# If running inside a Databricks notebook, get the token automatically:
try:
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
except Exception:
    # Running locally — set your PAT here or via env var
    import os
    token = os.environ.get("DATABRICKS_TOKEN", "<your-admin-pat-here>")

HEADERS = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

# ── Helper: grant CAN_RUN on a workspace object ───────────────────────────────
def grant_can_run(object_type: str, object_path: str):
    """
    object_type : "notebooks" | "directories"
    object_path : workspace path, e.g. /Users/.../gold/nb_gold_dim_station
    """
    # Step 1: get the object ID from the path
    list_url = f"{WORKSPACE_URL}/api/2.0/workspace/get-status"
    resp = requests.get(list_url, headers=HEADERS, params={"path": object_path})
    resp.raise_for_status()
    obj_info = resp.json()
    obj_id = obj_info.get("object_id")
    if not obj_id:
        print(f"  SKIP — could not find object_id for {object_path}")
        return

    # Step 2: patch permissions — add CAN_RUN for the SP
    perm_url = f"{WORKSPACE_URL}/api/2.0/permissions/{object_type}/{obj_id}"
    payload = {
        "access_control_list": [
            {
                "service_principal_name": SP_APPLICATION_ID,
                "permission_level": "CAN_RUN"
            }
        ]
    }
    resp = requests.patch(perm_url, headers=HEADERS, data=json.dumps(payload))
    resp.raise_for_status()
    print(f"  OK  — CAN_RUN granted on {object_path}")


# ── Main ──────────────────────────────────────────────────────────────────────
print("=" * 70)
print("Granting CAN_RUN to ADF SP on Gold notebooks")
print(f"SP : {SP_APPLICATION_ID}")
print("=" * 70)

# Grant on the folder first — covers all notebooks inside it
print(f"\n[1/2] Folder-level grant: {GOLD_FOLDER_PATH}")
try:
    grant_can_run("directories", GOLD_FOLDER_PATH)
except Exception as e:
    print(f"  WARN — folder grant failed: {e}. Will try individual notebooks.")

# Grant on each notebook individually (belt-and-suspenders)
print(f"\n[2/2] Individual notebook grants ({len(GOLD_NOTEBOOKS)} notebooks)")
for nb_path in GOLD_NOTEBOOKS:
    try:
        grant_can_run("notebooks", nb_path)
    except Exception as e:
        print(f"  FAIL — {nb_path}: {e}")

print("\nDone. Re-trigger the ADF pipeline to verify.")
