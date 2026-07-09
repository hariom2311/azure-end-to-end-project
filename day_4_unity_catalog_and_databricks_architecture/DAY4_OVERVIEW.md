# Day 4 — Unity Catalog & Databricks Architecture Deep Dive
**Session goal:** Understand the full Databricks + Unity Catalog architecture, every term, every layer, and how each piece shows up in the Databricks UI — so you can navigate confidently and know exactly what you are looking at.

---

## Files in This Directory

| File | What it covers |
|---|---|
| `DAY4_OVERVIEW.md` | This file — index and session goals |
| `01_DATABRICKS_ARCHITECTURE.md` | Full Databricks platform architecture — control plane, data plane, clusters, workspaces |
| `02_UNITY_CATALOG_ARCHITECTURE.md` | Unity Catalog object model — metastore, catalog, schema, table, volume, external location |
| `03_UNITY_CATALOG_VS_NO_CATALOG.md` | Side-by-side comparison: workspace with vs without Unity Catalog — UI differences, what you can and cannot do |
| `04_UI_WALKTHROUGH.md` | Where to find every concept in the Databricks UI — step-by-step navigation with what each screen shows |

---

## What You Will Understand by End of Day 4

- Why Databricks has a **control plane** and a **data plane** — and why this split matters for security
- What a **workspace** is and how multiple workspaces relate to one Unity Catalog **metastore**
- The full **4-level namespace** (`catalog.schema.table`) and where it lives in the UI
- What **External Locations**, **Storage Credentials**, and **Volumes** actually do and why they exist
- What the UI looks like **without** Unity Catalog vs **with** Unity Catalog — specific screens that change
- How to **test** each architectural concept directly in the Databricks UI

---

## How This Fits the Project

```
Day 1   Azure resources provisioned — storage accounts, Key Vault, Databricks workspace
Day 2   ADF pipelines + Unity Catalog External Locations configured
Day 3   Bronze ingestion pipelines (ADF + Databricks Jobs)
Day 4   ← YOU ARE HERE — understand the architecture underneath everything built so far
Day 5+  Silver layer (Delta tables, schema enforcement, deduplication)
Day 6+  Gold layer (aggregations, reporting tables)
Day 7+  Orchestration (ADF triggering Databricks notebooks end-to-end)
```
