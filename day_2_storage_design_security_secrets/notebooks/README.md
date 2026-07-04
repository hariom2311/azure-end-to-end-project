# Day 2 Notebooks

Three notebooks to run during Day 2. Import them into Databricks the same way as Day 1.

## How to Import into Databricks

1. Databricks → left menu **Workspace**
2. Right-click any folder → **Import**
3. Select **File** → drag and drop the `.ipynb` file (or browse to it)
4. Click **Import**
5. The notebook opens — attach to `dev-cluster` from the top-right dropdown

---

## Notebooks — Run in This Order

| # | File | Part in DAY2_STORAGE_SECURITY.md | What it does |
|---|---|---|---|
| 1 | `01_create_folder_structure.ipynb` | Part 2 | Creates all 40+ Bronze/Silver/Gold folders inside ADLS Gen2 containers |
| 2 | `02_verify_secrets.ipynb` | Part 3 + Part 7 | Verifies all Key Vault secrets are readable; tests zero-credential storage auth pattern |
| 3 | `03_verify_unity_catalog.ipynb` | Part 10 | Verifies External Locations, Schemas, Volumes — and tests file write via `/Volumes/` path |

---

## Before Running

- Cluster must be **started** (`dev-cluster`)
- Day 1 `00b_connect_storage_no_mount` notebook must have run first (Cells 1–3 — sets up SP OAuth and `abfss()` helper for notebooks 01 and 02)
- Secret scope `kv-ev-scope` must exist (Day 1 Part 6.5)
- For notebook 03: complete Parts 10.1–10.5 in the Day 2 setup guide first (Access Connector, Storage Credential, External Locations, Volumes)

## Notebook 03 Prerequisites (Unity Catalog)

Before running `03_verify_unity_catalog`, confirm these are done in the Azure Portal and Databricks UI:

1. Access Connector `ac-ev-intelligence-dev` created in Azure (Part 10.1)
2. `Storage Blob Data Contributor` role assigned to connector on `evdatalakedev` (Part 10.2)
3. Storage Credential `ac-ev-intelligence-dev` created in Databricks (Part 10.3)
4. External Locations `bronze`, `silver`, `gold` created and tested (Part 10.4)
5. Schemas `bronze`, `silver`, `gold` created in `dbw_ev_intelligence_dev` catalog (Part 10.5)
6. Volumes `bronze_volume`, `silver_volume`, `gold_volume` created (Part 10.5)
