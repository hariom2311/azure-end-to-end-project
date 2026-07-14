# Day 8 — ADF Silver Pipeline Setup
**Attach v4 Silver notebook to the master Bronze pipeline**

---

## What we are building

```
pl_bronze_api_master_v4_with_silver
  │
  ├── act_read_metadata          (Lookup — reads pipeline_metadata_config.json)
  │
  ├── act_foreach_entity         (ForEach — Bronze ingestion, all 17 entities in parallel)
  │       └── act_ingest_entity  (ExecutePipeline → pl_bronze_api_ingest_v4, one per entity)
  │
  └── act_silver_transform       (DatabricksNotebook → 04_silver_all_entities_job_params_v4)
        dependsOn: act_foreach_entity [Succeeded]
        Parameters:
          load_type      = @pipeline().parameters.p_load_type
          ingestion_date = @formatDateTime(utcNow(), 'yyyy-MM-dd')
```

Silver only starts after ALL Bronze entities finish successfully.
If any Bronze entity fails, Silver is skipped automatically.

---

## Option A — Add Silver to the existing master pipeline (Recommended)

Edit `pl_bronze_api_master_v4` in ADF and add one new activity after ForEach.

### Step 1 — Upload v4 notebook to Databricks Workspace

1. Databricks → left sidebar → **Workspace** → **Shared**
2. Create folder: **silver_transformation** (if it does not exist)
   - Right-click **Shared** → **Create** → **Folder** → name it `silver_transformation`
3. Import the notebook:
   - Right-click `silver_transformation` → **Import**
   - Select file: `day_8_silver_transformation/04_silver_all_entities_job_params_v4.ipynb`
4. Confirm notebook path:
   ```
   /Shared/silver_transformation/04_silver_all_entities_job_params_v4
   ```

---

### Step 2 — Open the master pipeline in ADF

1. Azure Data Factory → **Author** (pencil icon) → **Pipelines**
2. Open `pl_bronze_api_master_v4`

---

### Step 3 — Add the Databricks Notebook activity

1. In the Activities panel on the left → search **Databricks** → drag **Notebook** onto the canvas
2. Drop it to the right of the ForEach activity

---

### Step 4 — Connect ForEach → Silver (Success dependency)

1. Click the **ForEach** activity (`act_foreach_entity`)
2. Drag the **green arrow** (success connector) from ForEach to the new Notebook activity
   - Green = only runs if ForEach succeeded
   - This means Silver only runs after ALL Bronze entities are done

---

### Step 5 — Configure the Notebook activity

Click the Notebook activity to open its settings panel.

#### General tab
| Field | Value |
|---|---|
| Name | `act_silver_transform` |
| Description | `Silver transformation — all 17 entities via Databricks notebook v4` |
| Timeout | `2:00:00` (2 hours — adjust based on data volume) |
| Retry | `1` |

#### Azure Databricks tab
| Field | Value |
|---|---|
| Databricks linked service | `ls_databricks_dev` |
| Notebook path | `/Shared/silver_transformation/04_silver_all_entities_job_params_v4` |

#### Settings tab → Base parameters
Click **+ New** for each parameter:

| Name | Value | Type |
|---|---|---|
| `load_type` | `@pipeline().parameters.p_load_type` | Expression |
| `ingestion_date` | `@formatDateTime(utcNow(), 'yyyy-MM-dd')` | Expression |

> **Why `formatDateTime(utcNow())`?**
> Bronze ingestion writes files to `ingestion_date=yyyy-MM-dd` partitions today.
> Silver reads the same date partition. Both run in the same pipeline trigger, so `utcNow()` gives the correct folder to read from.

---

### Step 6 — Validate and Publish

1. Click **Validate** (top toolbar) — should show 0 errors
2. Click **Publish all** → **Publish**

---

### Step 7 — Test Run

1. Click **Add trigger** → **Trigger now**
2. Set parameter: `p_load_type = incremental`
3. Click **OK**

**Monitor the run:**
1. ADF → **Monitor** → **Pipeline runs**
2. Click the run to expand activities:
   - `act_read_metadata` → green (Succeeded)
   - `act_foreach_entity` → green (all 17 entities)
   - `act_silver_transform` → green (Databricks notebook completed)
3. Click `act_silver_transform` → **Output** to see the notebook run URL

**Verify Silver in Databricks:**
```python
SILVER_VOLUME = "/Volumes/dbw_ev_intelligence_dev/default/silver-volume"
for entity in ["payments", "sessions", "customers", "weather"]:
    df = spark.read.format("delta").load(f"{SILVER_VOLUME}/api/{entity}")
    print(f"{entity:<25} rows={df.count()}")
```

---

## Option B — Separate Silver pipeline (for independent reruns)

Use `pl_silver_api_transform_v4.json` as a standalone pipeline.

**When to use Option B:**
- Silver failed but Bronze succeeded — rerun Silver only without re-ingesting Bronze
- Backfill Silver for a specific past date

### Step 1 — Import the pipeline JSON

1. ADF → **Author** → Pipelines → **⋮** → **Import from pipeline template** (or paste JSON via Code view)
2. Paste contents of `pl_silver_api_transform_v4.json`
3. Update linked service if your Databricks linked service name differs from `ls_databricks_dev`

### Step 2 — Trigger manually

1. **Add trigger** → **Trigger now**
2. Set parameters:
   | Parameter | Value |
   |---|---|
   | `p_load_type` | `incremental` |
   | `p_ingestion_date` | `2026-07-14` |

---

## Pipeline flow diagram (Option A)

```
Trigger (scheduled / manual)
    │
    ▼  p_load_type = "incremental"
pl_bronze_api_master_v4_with_silver
    │
    ├─► act_read_metadata
    │       └── reads pipeline_metadata_config.json
    │           returns 17 entity configs
    │
    ├─► act_foreach_entity  [dependsOn: act_read_metadata Succeeded]
    │       isSequential: false, batchCount: 20
    │       └── act_ingest_entity (×17 in parallel)
    │               ExecutePipeline → pl_bronze_api_ingest_v4
    │               Parameters: entity_name, api_path, page_size, load_type
    │               Writes: Bronze Volume /api/<entity>/ingestion_date=yyyy-MM-dd/page_N.json
    │
    └─► act_silver_transform  [dependsOn: act_foreach_entity Succeeded]
            DatabricksNotebook
            Path: /Shared/silver_transformation/04_silver_all_entities_job_params_v4
            baseParameters:
              load_type      = incremental
              ingestion_date = 2026-07-14  (today's date via formatDateTime)
            Reads:  Bronze Volume /api/<entity>/ingestion_date=2026-07-14/*.json
            Writes: Silver Volume /api/<entity>/  (Delta MERGE upsert)
```

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `Notebook not found at path` | Notebook not uploaded or path typo | Verify path in Databricks: Workspace → Shared → silver_transformation |
| `Parameter 'load_type' was not provided` | baseParameters key name typo in ADF | Must be exactly `load_type` and `ingestion_date` (match notebook's `_get_job_param` keys) |
| `Linked service not found: ls_databricks_dev` | Linked service name differs in your ADF | Change `ls_databricks_dev` to your actual Databricks linked service name |
| `Silver activity skipped` | ForEach had one or more failed entity runs | Fix the failing Bronze entity first, then rerun |
| `No Bronze JSON files found` for an entity | ingestion_date passed doesn't match Bronze partition | Check Bronze Volume: `dbutils.fs.ls("/Volumes/.../bronze-volume/api/payments/")` |
