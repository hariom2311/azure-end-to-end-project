# 03 — ADF Pipeline: Blob charging_sessions → Bronze Delta
**Day 2 | Step 3 of 4**

Build an ADF pipeline that reads CSV files from the source blob storage (`dataenggdailystorage`) and writes them to the Bronze Delta table in ADLS Gen2.

Files are partitioned by hour: `charging_sessions/YYYY/MM/DD/HH/*.csv`
Each pipeline run reads the **current hour's folder** and appends to the Bronze Delta table.

---

## Pipeline Overview

```
pl_bronze_blob_sessions
│
├── Step 1: Set Variable     Build folder path for current hour
│                            e.g. realtime/charging_sessions/2026/07/04/14/
│
├── Step 2: Copy Activity    Read all CSVs from that folder
│                            Source: ls_source_blob (wasbs://)
│                            Sink:   ls_adls_bronze (Delta)
│
└── Step 3: Set Variable     Store ingestion_date + ingestion_hour
                             (added as extra columns in the Delta table)
```

---

## Folder Structure in Source Blob

```
dataenggdailystorage
└── source/
    └── realtime/
        └── charging_sessions/
            └── 2026/
                └── 07/
                    └── 04/
                        └── 14/
                            ├── sessions_20260704_1400.csv
                            ├── sessions_20260704_1401.csv
                            └── ...
```

Each CSV file has these columns (from Day 1 profiling):
`session_id, vehicle_id, station_id, customer_id, started_at, ended_at, duration_min, energy_kwh, cost_aud, peak_power_kw, connector_type, session_status, payment_id`

---

## Delta Table Target

```
evdatalakedev
└── bronze/
    └── blob/
        └── iot_sessions/         ← Delta table root
            ├── _delta_log/
            ├── ingestion_date=2026-07-04/
            │   └── ingestion_hour=14/
            │       └── part-*.parquet
            └── ...
```

Partitioned by `ingestion_date` and `ingestion_hour` so you can query just one hour or one day efficiently.

---

## Part A — Create Datasets

### Dataset 1: Source Blob CSV (`ds_source_sessions_csv`)

**UI Steps:**

1. ADF Studio → **Author** → **Datasets** → **+ New dataset**
2. Search `Azure Blob Storage` → **Continue**
3. Search `DelimitedText` (CSV) → **Continue**
4. Fill in:
   - **Name:** `ds_source_sessions_csv`
   - **Linked service:** `ls_source_blob`
   - **File path:** click **Browse** — but since it's external, type manually:
     - Container: `source`
     - Directory: leave blank (we'll use parameters)
     - File: `*.csv`
5. Click **OK**
6. **Connection** tab:
   - **Column delimiter:** Comma (,)
   - **Row delimiter:** `\n`
   - **First row as header:** checked
   - **Quote character:** `"`
   - **Escape character:** `\`
7. **Parameters** tab → **+ New**:
   - `p_folder_path` | Type: String | Default: `realtime/charging_sessions/2026/07/04/00`
8. Go back to **Connection** tab → **File path** → **Directory** field → click **Add dynamic content**:
   ```
   @{dataset().p_folder_path}
   ```
9. Click **Publish all**

---

### Dataset 2: Bronze Sessions Delta Sink (`ds_bronze_sessions_delta`)

**UI Steps:**

1. **Datasets** → **+ New dataset**
2. Search `Azure Data Lake Storage Gen2` → **Continue**
3. Search `Delta` → **Delta** → **Continue**
4. Fill in:
   - **Name:** `ds_bronze_sessions_delta`
   - **Linked service:** `ls_adls_bronze`
   - **File path:** `bronze` / `blob/iot_sessions`
5. Click **OK**
6. Click **Publish all**

---

## Part B — Create Pipeline `pl_bronze_blob_sessions`

**UI Steps:**

1. **Author** → **Pipelines** → **+ New pipeline**
2. **Name:** `pl_bronze_blob_sessions`
3. **Parameters** tab → **+ New**:
   - `p_year` | Type: String | Default: (empty — will use current date)
   - `p_month` | Type: String | Default: (empty)
   - `p_day` | Type: String | Default: (empty)
   - `p_hour` | Type: String | Default: (empty)

   > When triggered manually you can pass these. When triggered by schedule, ADF fills them from `@{formatDateTime(pipeline().TriggerTime, 'yyyy')}` etc.

4. **Variables** tab → **+ New**:
   - `v_folder_path` | Type: String
   - `v_ingestion_date` | Type: String
   - `v_ingestion_hour` | Type: String

---

### Step 1 — Set Variable: Build folder path

1. Drag **Set Variable** activity onto canvas
2. **Name:** `act_set_folder_path`
3. **Variable:** `v_folder_path`
4. **Value** (dynamic content):
   ```
   @{concat(
     'realtime/charging_sessions/',
     if(empty(pipeline().parameters.p_year), formatDateTime(utcNow(),'yyyy'), pipeline().parameters.p_year), '/',
     if(empty(pipeline().parameters.p_month), formatDateTime(utcNow(),'MM'), pipeline().parameters.p_month), '/',
     if(empty(pipeline().parameters.p_day), formatDateTime(utcNow(),'dd'), pipeline().parameters.p_day), '/',
     if(empty(pipeline().parameters.p_hour), formatDateTime(utcNow(),'HH'), pipeline().parameters.p_hour)
   )}
   ```

   This resolves to e.g.: `realtime/charging_sessions/2026/07/04/14`

5. Add second **Set Variable**: `act_set_ingestion_date`
   - Variable: `v_ingestion_date`
   - Value: `@{formatDateTime(utcNow(),'yyyy-MM-dd')}`

6. Add third **Set Variable**: `act_set_ingestion_hour`
   - Variable: `v_ingestion_hour`
   - Value: `@{formatDateTime(utcNow(),'HH')}`

---

### Step 2 — Copy Activity: Read CSV → Write Delta

1. Drag **Copy data** activity onto canvas (after the Set Variable activities)
2. **Name:** `act_copy_sessions`

**Source tab:**
- Dataset: `ds_source_sessions_csv`
- Dataset parameters:
  - `p_folder_path`: `@{variables('v_folder_path')}`
- **File path type:** Wildcard
- **Wildcard file name:** `*.csv`

**Sink tab:**
- Dataset: `ds_bronze_sessions_delta`
- **Write behavior:** Append
- **Pre-copy script:** (leave empty)
- **Max concurrent connections:** 4

**Additional columns tab:**

Add 2 extra columns that get injected into every row:

| Name | Value |
|---|---|
| `ingestion_date` | `@{variables('v_ingestion_date')}` |
| `ingestion_hour` | `@{variables('v_ingestion_hour')}` |

These become partition columns in the Delta table.

**Mapping tab:**

| Source column | Destination column | Type |
|---|---|---|
| session_id | session_id | String |
| vehicle_id | vehicle_id | String |
| station_id | station_id | String |
| customer_id | customer_id | String |
| started_at | started_at | String |
| ended_at | ended_at | String |
| duration_min | duration_min | Integer |
| energy_kwh | energy_kwh | Double |
| cost_aud | cost_aud | Double |
| peak_power_kw | peak_power_kw | Double |
| connector_type | connector_type | String |
| session_status | session_status | String |
| payment_id | payment_id | String |
| ingestion_date | ingestion_date | String |
| ingestion_hour | ingestion_hour | String |

3. Connect: `act_set_ingestion_hour` → `act_copy_sessions`

---

## Part C — Trigger the Pipeline

### Manual trigger for a specific hour (UI)

1. Open `pl_bronze_blob_sessions`
2. Click **Add trigger** → **Trigger now**
3. Parameters:
   - `p_year`: `2026`
   - `p_month`: `07`
   - `p_day`: `04`
   - `p_hour`: `06`
4. Click **OK**

### Scheduled trigger — every hour (UI)

1. **Add trigger** → **New/Edit**
2. **Name:** `tr_bronze_blob_sessions_hourly`
3. **Type:** Schedule
4. **Recurrence:** every `1 Hour`
5. **Start time:** set to the next full hour
6. Leave parameters empty — the pipeline uses `utcNow()` automatically
7. Click **OK** → **Publish all**

### CLI trigger

```bash
# Trigger for a specific hour
az datafactory pipeline create-run \
  --resource-group $RG \
  --factory-name $ADF \
  --pipeline-name "pl_bronze_blob_sessions" \
  --parameters '{
    "p_year": "2026",
    "p_month": "07",
    "p_day": "04",
    "p_hour": "06"
  }'
```

### CLI scheduled trigger

```bash
az datafactory trigger create \
  --resource-group $RG \
  --factory-name $ADF \
  --trigger-name "tr_bronze_blob_sessions_hourly" \
  --properties '{
    "type": "ScheduleTrigger",
    "pipelines": [
      {
        "pipelineReference": {
          "referenceName": "pl_bronze_blob_sessions",
          "type": "PipelineReference"
        }
      }
    ],
    "typeProperties": {
      "recurrence": {
        "frequency": "Hour",
        "interval": 1,
        "startTime": "2026-07-04T00:00:00Z",
        "timeZone": "UTC"
      }
    }
  }'

# Start the trigger (it won't fire until started)
az datafactory trigger start \
  --resource-group $RG \
  --factory-name $ADF \
  --trigger-name "tr_bronze_blob_sessions_hourly"
```

---

## Verify in ADLS

After the pipeline runs:

```python
# In Databricks
display(dbutils.fs.ls(abfss("bronze", "blob/iot_sessions/")))
# Expected: _delta_log/, ingestion_date=2026-07-04/ folder

display(dbutils.fs.ls(abfss("bronze", "blob/iot_sessions/ingestion_date=2026-07-04/")))
# Expected: ingestion_hour=06/ folder

df = spark.read.format("delta").load(abfss("bronze", "blob/iot_sessions/"))
print(f"Total rows: {df.count():,}")
display(df.limit(10))
```

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `No files found` | Folder path is wrong — hour does not exist in blob | Run Cell 3 of `02_read_source_blob` notebook to check actual folder structure |
| `403 on source blob` | SAS token expired or missing | Regenerate SAS token, update `source-blob-sas-uri` secret in Key Vault |
| `Schema mismatch` | CSV header differs from mapping | In dataset, enable **First row as header** and re-import schema |
| `Delta write fails` | ADF MI missing role on bronze container | Storage → IAM → add Storage Blob Data Contributor for ADF MI |
| `Duplicate rows on re-run` | Append mode adds rows every run | This is expected in Bronze — Silver layer deduplicates using `session_id` |

---

## Next Step

→ `04_DATABRICKS_BRONZE_TABLES.md` — create internal Delta tables in Databricks for both datasets
