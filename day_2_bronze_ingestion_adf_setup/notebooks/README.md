# Day 2 Notebooks

Two notebooks for Day 2. Import them into Databricks after running the ADF pipelines — they read the data ADF wrote and register it as Delta tables.

> **Run the ADF pipelines first** (`pl_bronze_api_payments` and `pl_bronze_blob_sessions`) before running these notebooks. The notebooks read Delta files that ADF created.

## How to Import into Databricks

1. Databricks → left menu **Workspace**
2. Right-click any folder → **Import**
3. Select **File** → drag and drop the `.ipynb` file (or browse to it)
4. Click **Import**
5. The notebook opens — attach to `dev-cluster` from the top-right dropdown

Repeat for each `.ipynb` file below.

---

## Notebooks — Run in This Order

| # | File | What it does |
|---|---|---|
| 1 | `03_bronze_api_payments.ipynb` | Reads payments Delta from ADLS, creates external + internal Delta tables, writes audit log |
| 2 | `04_bronze_blob_sessions.ipynb` | Reads charging_sessions Delta from ADLS, creates external + internal Delta tables |

Run in order 1 → 2.

---

## Before Running

- Cluster must be **started** — any cluster mode works (Dedicated, Standard, Shared, Serverless)
- Secret scope `kv-ev-scope` must exist (created in Day 1 Part 6.5)
- All Key Vault secrets must exist — see Day 1 and Day 2 prerequisites
- ADF pipeline must have run at least once — Bronze Delta files must exist in ADLS before these notebooks can read them

## Re-running After Cluster Restart

Re-run Cell 1 and Cell 2 of either notebook after every cluster restart — those cells set the SP OAuth Spark config which is per-session and does not persist.
