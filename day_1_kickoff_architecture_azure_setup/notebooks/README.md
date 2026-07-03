# Day 1 Notebooks

Four notebooks for Day 1. Import them into Databricks directly.

> **Storage connection — use the modern direct access approach:**
> - `00b_connect_storage_no_mount.ipynb` — SP OAuth direct access, no mount (works in all cluster modes including Shared and Serverless)

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
| 1 | `00b_connect_storage_no_mount.ipynb` | Part 7.2 | SP OAuth direct access — no mount, works in all cluster modes |
| 2 | `01_verify_api_auth.ipynb` | Part 7.3 | Tests VoltGrid API login, scans all 18 endpoints, runs noise check |
| 3 | `02_read_source_blob.ipynb` | Part 7.4 | Reads shared source blob data using SAS token |

Run in order 1 → 2 → 3.

---

## Before Running

- Cluster must be **started** — any cluster mode works (Dedicated, Standard, Shared, Serverless)
- Secret scope `kv-ev-scope` must be created (Day 1 Part 6.5)
- All Key Vault secrets must exist (Day 1 Part 4 + 5)
- For `02_read_source_blob.ipynb` — SAS token secrets must be added to Key Vault (Day 1 Part 7.4.1)

## Re-running After Cluster Restart

Re-run Cells 1–3 of `00b_connect_storage_no_mount.ipynb` after every cluster restart — the Spark OAuth config is per-session and must be reset each time. Or add `%run "./00b_connect_storage_no_mount"` as Cell 1 in any notebook that accesses ADLS.
