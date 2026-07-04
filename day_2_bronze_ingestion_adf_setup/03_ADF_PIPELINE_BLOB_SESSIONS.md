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
├── Step 1: Set Variable (×3)    Build folder path, ingestion_date, ingestion_hour
│                                e.g. realtime/charging_sessions/2026/07/04/14/
│
├── Step 2: Copy Activity        Read all CSVs from that folder
│                                Source: ls_source_blob (wasbs://)
│                                Sink:   ls_adls_bronze (Delta)
│                                Extra columns: ingestion_date, ingestion_hour injected
│
└── Trigger: runs every hour automatically
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
   - **File path — Container:** `source`
   - **File path — Directory:** leave blank for now (parameter will override this)
   - **File path — File:** `*.csv`
5. Click **OK**
6. **Connection** tab:
   - **Column delimiter:** Comma (`,`)
   - **Row delimiter:** `\n`
   - **First row as header:** checked (ON)
   - **Quote character:** `"`
   - **Escape character:** `\`
7. **Parameters** tab → **+ New**:
   - `p_folder_path` | Type: String | Default: `realtime/charging_sessions/2026/07/04/00`
8. Go back to **Connection** tab → **File path → Directory** field → click **Add dynamic content**:
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
   - **File path — Container:** `bronze`
   - **File path — Directory:** `blob/iot_sessions`
5. Click **OK**
6. Click **Publish all**

---

## Part B — Create Pipeline `pl_bronze_blob_sessions`

**UI Steps:**

1. **Author** → **Pipelines** → **+ New pipeline**
2. **Name:** `pl_bronze_blob_sessions`
3. **Parameters** tab → **+ New** — add 4 optional override parameters:
   - `p_year` | Type: String | Default: (leave empty)
   - `p_month` | Type: String | Default: (leave empty)
   - `p_day` | Type: String | Default: (leave empty)
   - `p_hour` | Type: String | Default: (leave empty)

   > When triggered manually you can pass specific values to backfill a past hour. When triggered by schedule, leave them empty — the pipeline uses `utcNow()` automatically.

4. **Variables** tab → **+ New**:
   - `v_folder_path` | Type: String
   - `v_ingestion_date` | Type: String
   - `v_ingestion_hour` | Type: String

---

### Step 1 — Set Variable: Build folder path

1. Drag **Set Variable** activity onto canvas
2. **Name:** `act_set_folder_path`
3. **Variable:** `v_folder_path`
4. **Value** (dynamic content — paste exactly):
   ```
   @{concat('realtime/charging_sessions/',if(empty(pipeline().parameters.p_year),formatDateTime(utcNow(),'yyyy'),pipeline().parameters.p_year),'/',if(empty(pipeline().parameters.p_month),formatDateTime(utcNow(),'MM'),pipeline().parameters.p_month),'/',if(empty(pipeline().parameters.p_day),formatDateTime(utcNow(),'dd'),pipeline().parameters.p_day),'/',if(empty(pipeline().parameters.p_hour),formatDateTime(utcNow(),'HH'),pipeline().parameters.p_hour))}
   ```

   This resolves to e.g.: `realtime/charging_sessions/2026/07/04/14`

---

### Step 2 — Set Variable: Set ingestion date

1. Add second **Set Variable** activity
2. **Name:** `act_set_ingestion_date`
3. **Variable:** `v_ingestion_date`
4. **Value** (dynamic content):
   ```
   @{formatDateTime(utcNow(),'yyyy-MM-dd')}
   ```

---

### Step 3 — Set Variable: Set ingestion hour

1. Add third **Set Variable** activity
2. **Name:** `act_set_ingestion_hour`
3. **Variable:** `v_ingestion_hour`
4. **Value** (dynamic content):
   ```
   @{formatDateTime(utcNow(),'HH')}
   ```

Connect all 3 Set Variable activities in sequence: `act_set_folder_path` → `act_set_ingestion_date` → `act_set_ingestion_hour`

---

### Step 4 — Copy Activity: Read CSV → Write Delta

1. Drag **Copy data** activity onto canvas
2. **Name:** `act_copy_sessions`
3. Connect: `act_set_ingestion_hour` → `act_copy_sessions`

**Source tab:**
- Dataset: `ds_source_sessions_csv`
- Dataset parameters:
  - `p_folder_path`: `@{variables('v_folder_path')}`
- **File path type:** Wildcard
- **Wildcard folder path:** `@{variables('v_folder_path')}`
- **Wildcard file name:** `*.csv`

**Sink tab:**
- Dataset: `ds_bronze_sessions_delta`
- **Write behavior:** Append
- **Pre-copy script:** (leave empty)
- **Max concurrent connections:** 4

**Additional columns tab:**

Click **+ New** and add these 2 extra columns — they get injected into every row before writing:

| Name | Value |
|---|---|
| `ingestion_date` | `@{variables('v_ingestion_date')}` |
| `ingestion_hour` | `@{variables('v_ingestion_hour')}` |

These become the partition columns in the Delta table.

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

---

## Part C — Trigger the Pipeline

### Manual trigger for a specific hour — UI

1. Open `pl_bronze_blob_sessions`
2. Click **Add trigger** → **Trigger now**
3. Fill in parameters:
   - `p_year`: `2026`
   - `p_month`: `07`
   - `p_day`: `04`
   - `p_hour`: `06`
4. Click **OK**
5. Monitor tab → pipeline run should show green

### Scheduled trigger — every hour (UI)

1. **Add trigger** → **New/Edit**
2. **Name:** `tr_bronze_blob_sessions_hourly`
3. **Type:** Schedule
4. **Recurrence:** every `1 Hour`
5. **Start time:** set to the next full hour in UTC
6. Leave all 4 parameters empty — pipeline uses `utcNow()` automatically
7. Click **OK** → **Publish all**

---

### Manual trigger — CLI

> **CMD / PowerShell users:** The `\` line continuation below is bash syntax and will break in CMD/PowerShell. Use the single-line version to copy-paste directly.

**Single line (CMD / PowerShell):**
```cmd
az datafactory pipeline create-run --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --pipeline-name "pl_bronze_blob_sessions" --parameters "{\"p_year\": \"2026\", \"p_month\": \"07\", \"p_day\": \"04\", \"p_hour\": \"06\"}"
```

**Multi-line (bash / Git Bash only):**
```bash
az datafactory pipeline create-run \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
  --pipeline-name "pl_bronze_blob_sessions" \
  --parameters '{
    "p_year": "2026",
    "p_month": "07",
    "p_day": "04",
    "p_hour": "06"
  }'
```

---

### Create scheduled trigger — CLI

**Single line (CMD / PowerShell):**
```cmd
az datafactory trigger create --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --trigger-name "tr_bronze_blob_sessions_hourly" --properties "{\"type\": \"ScheduleTrigger\", \"pipelines\": [{\"pipelineReference\": {\"referenceName\": \"pl_bronze_blob_sessions\", \"type\": \"PipelineReference\"}}], \"typeProperties\": {\"recurrence\": {\"frequency\": \"Hour\", \"interval\": 1, \"startTime\": \"2026-07-04T00:00:00Z\", \"timeZone\": \"UTC\"}}}"
```

**Multi-line (bash / Git Bash only):**
```bash
az datafactory trigger create \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
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
```

**Start the trigger (CMD / PowerShell):**
```cmd
az datafactory trigger start --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --trigger-name "tr_bronze_blob_sessions_hourly"
```

> The trigger does not fire until explicitly started. After starting, it fires at the next scheduled hour.

---

## Verify in ADLS (Databricks)

After the pipeline runs:

```python
display(dbutils.fs.ls(abfss("bronze", "blob/iot_sessions/")))
```

Expected: `_delta_log/` and `ingestion_date=2026-07-04/` folder.

```python
display(dbutils.fs.ls(abfss("bronze", "blob/iot_sessions/ingestion_date=2026-07-04/")))
```

Expected: `ingestion_hour=06/` folder.

```python
df = spark.read.format("delta").load(abfss("bronze", "blob/iot_sessions/"))
print(f"Total rows: {df.count():,}")
display(df.limit(10))
```

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `No files found` in Copy Activity | Folder path built incorrectly — hour does not exist in source blob | Run Cell 3 of `02_read_source_blob` notebook to check actual folder structure, then adjust parameters |
| `403 Forbidden` on source blob read | SAS token expired or missing `r` + `l` permissions | Regenerate SAS token with `sp=rl`, update `source-blob-sas-uri` secret in Key Vault |
| `Schema mismatch` on Delta write | CSV header order differs from mapping | In Copy Activity → Mapping tab → re-import schema from source |
| `Delta write fails: 403` | ADF MI missing `Storage Blob Data Contributor` on `evdatalakedev` | Day 2 Part 2 — assign the role, wait 2 min, retry |
| `Duplicate rows on re-run` | Append mode adds rows every run | This is expected in Bronze — Silver layer will deduplicate using `session_id` |
| `Trigger not firing` | Trigger created but not started | Run `az datafactory trigger start` or click **Activate** in ADF Studio |

---

## Next Step

→ `04_DATABRICKS_BRONZE_TABLES.md` — create internal Delta tables in Databricks for both datasets
