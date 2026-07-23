# Day 12 — Gold ADF Pipeline

New pipelines added in this day. No changes to existing day_8 pipelines.

## New files

| File | Purpose |
|---|---|
| `adf_pipeline_json/pl_gold_api_transform_v4.json` | New Gold pipeline — calls Databricks notebook `04_gold_full_incremental_v3` |
| `adf_pipeline_json/pl_bronze_api_master_v5.json` | New master pipeline — extends v4 with Gold step added at the end |

## Pipeline flow (v5)

```
pl_bronze_api_master_v5  (p_load_type = full | incremental)
  │
  ├── act_read_metadata           reads pipeline_metadata_config.json
  ├── act_foreach_entity          Bronze — all 17 entities in parallel
  │     └── pl_bronze_api_ingest_v4
  ├── act_invoke_silver_pipeline  (on ForEach Succeeded)
  │     └── pl_silver_api_transform_v4
  │           └── 04_silver_all_entities_job_params_v4  (Databricks)
  └── act_invoke_gold_pipeline    (on Silver Succeeded)
        └── pl_gold_api_transform_v4
              └── 04_gold_full_incremental_v3  (Databricks)
```

## What changed vs v4

- `pl_bronze_api_master_v4` (day_8) had Bronze + Silver only
- `pl_bronze_api_master_v5` (day_12) adds `act_invoke_gold_pipeline` after Silver
- `pl_gold_api_transform_v4` is a brand new pipeline (did not exist before)

## Import order in ADF

1. `pl_gold_api_transform_v4.json` — import first (master depends on it)
2. `pl_bronze_api_master_v5.json` — import after

## Databricks notebook path

```
/Users/hariomsuryawanshi68258@gmail.com/end-to-end-18-days-project-notebooks/
  gold-ingestion-notebooks/api-response-gold-layer-notebooks/04_gold_full_incremental_v3
```

Upload `day_11_gold_correct/04_gold_full_incremental_v3.ipynb` to that path in Databricks.
