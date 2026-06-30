# Day 1 Notebooks

Three notebooks to run in order during Day 1. Import them into Databricks directly.

## How to Import into Databricks

1. Databricks → left menu **Workspace**
2. Right-click any folder → **Import**
3. Select **File** → drag and drop the `.ipynb` file (or browse to it)
4. Click **Import**
5. The notebook opens — attach to `dev-cluster` from the top-right dropdown

Repeat for each `.ipynb` file below.

---

## Notebooks — Run in This Order

| # | File | Part in DAY1_AZURE_SETUP.md | What it does |
|---|---|---|---|
| 1 | `00_mount_storage.ipynb` | Part 7.2 | Mounts bronze/silver/gold/source containers using SP OAuth |
| 2 | `01_verify_api_auth.ipynb` | Part 7.3 | Tests VoltGrid API login, scans all 18 endpoints, runs noise check |
| 3 | `02_read_source_blob.ipynb` | Part 7.4 | Reads shared source blob data using SAS token |

---

## Before Running

- Cluster must be **started** and Access mode must be **Dedicated**
- Secret scope `kv-ev-scope` must be created (Day 1 Part 6.5)
- All Key Vault secrets must exist (Day 1 Part 4 + 5)
- For `02_read_source_blob.py` — paste the SAS token provided during the session into Cell 1

## Re-running After Cluster Restart

Run `00_mount_storage.py` again after every cluster restart — mounts are not persisted across cluster lifecycles.
