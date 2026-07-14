# Day 8 — ADF Silver Pipeline Setup

## Pipeline structure

```
pl_bronze_api_master_v4                          ← master pipeline (same name, updated)
  │
  ├── act_read_metadata          (Lookup — reads pipeline_metadata_config.json)
  │
  ├── act_foreach_entity         (ForEach — Bronze ingestion, all 17 entities in parallel)
  │       └── act_ingest_entity  (ExecutePipeline → pl_bronze_api_ingest_v4)
  │
  └── act_invoke_silver_pipeline (ExecutePipeline → pl_silver_api_transform_v4)
        dependsOn: act_foreach_entity [Succeeded]
        Parameters:
          p_load_type      = @pipeline().parameters.p_load_type
          p_ingestion_date = @formatDateTime(utcNow(), 'yyyy-MM-dd')

pl_silver_api_transform_v4                       ← Silver pipeline (standalone, can run independently)
  │
  └── act_silver_transform       (DatabricksNotebook → 04_silver_all_entities_job_params_v4)
        baseParameters:
          load_type      = @pipeline().parameters.p_load_type
          ingestion_date = @pipeline().parameters.p_ingestion_date
```

**Why two separate pipelines:**
- `pl_silver_api_transform_v4` can be triggered independently for backfills or reruns without re-ingesting Bronze
- Master invokes it via `ExecutePipeline` — clean separation, same pattern as Bronze child pipeline
- Silver run history appears under its own pipeline in ADF Monitor

---

## Step 1 — Upload v4 notebook to Databricks

1. Databricks → **Workspace** → **Shared**
2. Create folder `silver_transformation` if it doesn't exist
3. **Import** → select `04_silver_all_entities_job_params_v4.ipynb`
4. Confirm path: `/Shared/silver_transformation/04_silver_all_entities_job_params_v4`

---

## Step 2 — Create pl_silver_api_transform_v4 in ADF

1. ADF → **Author** → **Pipelines** → **+** → **New pipeline**
2. Name it exactly: `pl_silver_api_transform_v4`
3. Add two pipeline parameters:

   | Name | Type | Default |
   |---|---|---|
   | `p_load_type` | String | `incremental` |
   | `p_ingestion_date` | String | *(none)* |

4. From the Activities panel → search **Databricks** → drag **Notebook** onto canvas
5. Name the activity: `act_silver_transform`
6. **Azure Databricks tab:**
   | Field | Value |
   |---|---|
   | Databricks linked service | `ls_databricks_dev` |
   | Notebook path | `/Shared/silver_transformation/04_silver_all_entities_job_params_v4` |

7. **Settings tab → Base parameters** → click **+ New** twice:

   | Name | Value |
   |---|---|
   | `load_type` | `@pipeline().parameters.p_load_type` |
   | `ingestion_date` | `@pipeline().parameters.p_ingestion_date` |

   > Both must be set as **Expression** type (toggle the blue `@` button)

8. **Validate** → **Publish all**

---

## Step 3 — Update pl_bronze_api_master_v4 to invoke Silver pipeline

1. ADF → **Author** → open `pl_bronze_api_master_v4`
2. From Activities panel → search **General** → drag **Execute Pipeline** onto canvas
3. Drop it to the right of the `act_foreach_entity` activity
4. Draw a **green success arrow** from `act_foreach_entity` → new Execute Pipeline activity

5. Configure the Execute Pipeline activity:

   **General tab:**
   | Field | Value |
   |---|---|
   | Name | `act_invoke_silver_pipeline` |
   | Timeout | `2:00:00` |

   **Settings tab:**
   | Field | Value |
   |---|---|
   | Invoked pipeline | `pl_silver_api_transform_v4` |
   | Wait on completion | `true` ✓ |

   **Parameters:**
   | Name | Value |
   |---|---|
   | `p_load_type` | `@pipeline().parameters.p_load_type` |
   | `p_ingestion_date` | `@formatDateTime(utcNow(), 'yyyy-MM-dd')` |

   > Set both as Expression type

6. **Validate** → **Publish all**

---

## Step 4 — Test run

1. `pl_bronze_api_master_v4` → **Add trigger** → **Trigger now**
2. Set `p_load_type = incremental` → OK

**Monitor:**
- ADF → Monitor → Pipeline runs
- Expand the master run — you should see:
  - `act_read_metadata` → Succeeded
  - `act_foreach_entity` → Succeeded (17 child runs)
  - `act_invoke_silver_pipeline` → Succeeded → click to see the Silver pipeline's own run

**Verify Silver output in Databricks:**
```python
SILVER_VOLUME = "/Volumes/dbw_ev_intelligence_dev/default/silver-volume"
for entity in ["payments", "sessions", "customers", "weather"]:
    df = spark.read.format("delta").load(f"{SILVER_VOLUME}/api/{entity}")
    print(f"{entity:<25} rows={df.count()}")
```

---

## Run Silver independently (backfill / rerun)

Trigger `pl_silver_api_transform_v4` directly:

1. ADF → Author → `pl_silver_api_transform_v4` → **Add trigger** → **Trigger now**
2. Set parameters:
   | Parameter | Value |
   |---|---|
   | `p_load_type` | `incremental` |
   | `p_ingestion_date` | `2026-07-14` |

This runs only the Databricks notebook — no Bronze ingestion, no metadata lookup.

---

## Common errors

| Error | Cause | Fix |
|---|---|---|
| `Pipeline pl_silver_api_transform_v4 not found` | Pipeline not published yet | Publish the Silver pipeline before running master |
| `Notebook not found at path` | Notebook not uploaded or path typo | Check Databricks Workspace: `/Shared/silver_transformation/` |
| `Parameter 'load_type' was not provided` | baseParameters key typo | Must be exactly `load_type` and `ingestion_date` (no `p_` prefix — these are notebook widget names) |
| `act_invoke_silver_pipeline skipped` | ForEach had a failed entity | Fix the failing Bronze entity run first, then rerun |
| `No Bronze JSON files found` | ingestion_date doesn't match Bronze partition | Check: `dbutils.fs.ls("/Volumes/.../bronze-volume/api/payments/")` |
