# Day 5 — Activity Implementation Guide

> **Base pipeline:** `pl_bronze_api_payments` in `adf-datalake-dev-ded`
> Bronze layer must be working (Day 2 pipeline runs successfully) before starting Day 5.
> Each section covers one new activity category with exact steps to implement and test it.

---

## Before You Start

Confirm these prerequisites are in place:

1. `pl_bronze_api_payments` runs successfully and writes `bronze/api/payments/...` to ADLS Gen2
2. The `bronze` container exists in your ADLS Gen2 account
3. You have access to `adf-datalake-dev-ded` Author view

---

## Activity 1 — Mapping Data Flow (Bronze → Silver Transform)

> Build a visual transformation that reads raw Bronze JSON and writes clean typed Parquet to Silver.

### Step 1.1 — Enable Data Flow Debug

1. Open ADF Studio → **Author** → **Data flows** (left sidebar)
2. At the top of any Data Flow canvas, find the **Data Flow Debug** toggle → turn it ON
3. A dialog appears asking which IR and cluster size to use → leave defaults → click **OK**
4. Wait 3–5 minutes for the Spark cluster to start — you'll see a green dot when ready

> Debug mode is only needed during development. Production pipelines run Data Flows without debug mode.

### Step 1.2 — Create the Data Flow

1. **Author** → **Data flows** → **+** → **New data flow**
2. Name: `df_silver_payments`
3. Click **Add Source**

### Step 1.3 — Configure Source

1. Click the `source1` node → rename stream to `src_bronze_payments`
2. **Source settings** tab:
   - **Source type:** Dataset
   - **Dataset:** `ds_bronze_payments_sink` (the Bronze ADLS dataset from Day 2)
3. **Projection** tab → click **Import schema** → ADF reads the Bronze JSON and infers columns
4. Click **Data preview** tab → rows appear → verify you see `id`, `amount`, `status`, `created_at` etc.

### Step 1.4 — Add Filter (remove failed rows)

1. Click **+** on the `src_bronze_payments` node → **Filter**
2. **Output stream name:** `flt_active`
3. **Filter on** → open expression editor → type:
   ```
   notEquals(status, 'failed')
   ```
4. **Data preview** → confirm no rows with `status = 'failed'`

### Step 1.5 — Add Derived Column (compute new columns)

1. Click **+** on `flt_active` → **Derived Column**
2. **Output stream name:** `drc_clean`
3. **Columns** → add three derived columns:

   | Column | Expression | What it does |
   |---|---|---|
   | `amount_aud` | `toDouble(amount)` | Cast string "45.80" to number 45.80 |
   | `created_date` | `toDate(created_at, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'')` | Extract date from timestamp |
   | `ingestion_date` | `currentDate()` | Stamp today's date on every row |

4. **Data preview** → verify `amount_aud` is numeric and `created_date` is a date value

### Step 1.6 — Add Select (keep only needed columns)

1. Click **+** on `drc_clean` → **Select**
2. **Output stream name:** `sel_final`
3. In the column list, keep: `id`, `session_id`, `amount_aud`, `payment_method`, `status`, `created_date`, `ingestion_date`
4. Remove (click the minus): `amount`, `currency`, `updated_at`, `created_at`
5. **Data preview** → only the 7 chosen columns remain

### Step 1.7 — Add Sink (write Silver Parquet)

1. Click **+** on `sel_final` → **Sink**
2. **Output stream name:** `sink_silver`
3. **Sink type:** Dataset → click **+ New**:
   - **Azure Data Lake Storage Gen2** → **Parquet**
   - Name: `ds_silver_payments_sink`
   - Linked service: `ls_adls_bronze`
   - File path: Container `silver`, Directory `api/payments`
   - **Publish**
4. Back in the sink node → **Settings** tab:
   - **File name option:** Output to single file → `payments.parquet`
5. **Data preview** → verify the 7 columns will be written

### Step 1.8 — Add Data Flow Activity to Pipeline

1. **Author** → **Pipelines** → open `pl_bronze_api_payments`
2. Activities panel → **Move & Transform** → drag **Data flow** to canvas
3. Rename to `act_silver_payments`
4. **Wire it:** `act_copy_payments` → `act_silver_payments` (On Success)
5. **Settings tab:**
   - **Data flow:** `df_silver_payments`
   - **Run on:** AutoResolveIntegrationRuntime
   - **Compute type:** General purpose | **Core count:** 8
6. **Publish all** → **Debug**
7. Monitor → click `act_silver_payments` → **Output** tab:
   ```json
   {
     "runStatus": {
       "metrics": {
         "sink_silver": { "rowsWritten": 95, "time": 42 }
       }
     }
   }
   ```
8. Verify: Portal → Storage → `silver` container → `api/payments/payments.parquet` exists

---

## Activity 2 — Databricks Notebook Activity

> Run a Databricks notebook from ADF to do the Bronze → Silver transform using Spark code.

### Step 2.1 — Create the Databricks Linked Service

1. **Manage** → **Linked services** → **+ New** → search **Azure Databricks** → Continue
2. Name: `ls_databricks`
3. **Azure subscription:** select yours
4. **Databricks workspace:** select your workspace (if available)
5. **Select cluster:** New job cluster (recommended — creates and destroys per run)
6. **New cluster version:** select the latest LTS runtime
7. **Node type:** Standard_DS3_v2
8. **Authentication type:** Access token
9. **Access token:** Reference from Key Vault:
   - AKV linked service: `ls_keyvault`
   - Secret name: `databricks-access-token`
10. **Test connection** → Succeeded → **Create**

### Step 2.2 — Add Databricks Notebook Activity

1. Open `pl_bronze_api_payments` → Activities panel → **Databricks** → drag **Notebook** to canvas
2. Rename to `act_run_silver_nb`
3. **Wire it:** `act_copy_payments` → `act_run_silver_nb` (On Success)
4. **Azure Databricks** tab:
   - **Databricks linked service:** `ls_databricks`
5. **Settings** tab:
   - **Notebook path:** `/VoltGrid/silver/process_payments`
   - **Base parameters** → **+ New** for each:

     | Key | Value |
     |---|---|
     | `run_date` | `@variables('v_ingestion_date')` |
     | `source_path` | `@concat('bronze/api/payments/ingestion_date=', variables('v_ingestion_date'), '/')` |

6. **Publish all** → **Debug**
7. Monitor → click `act_run_silver_nb` → **Output** tab → note the `runId` field → click the Databricks run link to view the notebook execution

**Expected output:**
```json
{
  "runId": 123456,
  "runPageUrl": "https://adb-xxx.azuredatabricks.net/...#job/.../run/..."
}
```

**What the notebook does (reference only):**
```python
# /VoltGrid/silver/process_payments
run_date    = dbutils.widgets.get("run_date")
source_path = dbutils.widgets.get("source_path")

df = spark.read.json(
    f"abfss://bronze@youraccount.dfs.core.windows.net/{source_path}"
)
df_clean = (df
    .filter(df.status != "failed")
    .withColumn("amount_aud", df.amount.cast("double"))
    .withColumn("created_date", df.created_at.cast("date"))
)
df_clean.write.mode("overwrite").parquet(
    f"abfss://silver@youraccount.dfs.core.windows.net/api/payments/ingestion_date={run_date}/"
)
```

> If you don't have a Databricks workspace, use the Mapping Data Flow (Activity 1) instead.

---

## Activity 3 — Stored Procedure Activity (Audit Log)

> After every successful Bronze copy, write an audit record to Azure SQL.

### Step 3.1 — Create Linked Service for Azure SQL

1. **Manage** → **Linked services** → **+ New** → **Azure SQL Database**
2. Name: `ls_azure_sql`
3. Fill in your Azure SQL server and database name
4. Authentication: SQL authentication or Managed Identity
5. **Test connection** → **Create**

### Step 3.2 — Create the Audit Table and Stored Procedure

Connect to your Azure SQL database and run:

```sql
-- Create audit table
CREATE TABLE pipeline_audit (
    id              INT IDENTITY PRIMARY KEY,
    pipeline_name   VARCHAR(200),
    run_date        DATE,
    rows_copied     INT,
    status          VARCHAR(50),
    run_timestamp   DATETIME DEFAULT GETDATE()
);

-- Create stored procedure
CREATE PROCEDURE usp_log_pipeline_run
    @pipeline_name  VARCHAR(200),
    @run_date       DATE,
    @rows_copied    INT,
    @status         VARCHAR(50)
AS
BEGIN
    INSERT INTO pipeline_audit (pipeline_name, run_date, rows_copied, status)
    VALUES (@pipeline_name, @run_date, @rows_copied, @status);
END;
```

### Step 3.3 — Add Stored Procedure Activity

1. Open `pl_bronze_api_payments` → Activities panel → **General** → drag **Stored procedure** to canvas
2. Rename to `act_log_audit`
3. **Wire it:** `act_copy_payments` → `act_log_audit` (On Success)
4. **Settings tab:**
   - **Linked service:** `ls_azure_sql`
   - **Stored procedure name:** `[dbo].[usp_log_pipeline_run]`
   - **Stored procedure parameters** → **+ New** for each:

     | Name | Type | Value |
     |---|---|---|
     | `pipeline_name` | String | `@pipeline().pipelineName` |
     | `run_date` | String | `@variables('v_ingestion_date')` |
     | `rows_copied` | Int32 | `@activity('act_copy_payments').output.rowsCopied` |
     | `status` | String | `success` |

5. **Publish all** → **Debug**
6. Monitor → click `act_log_audit` → **Output** tab:
   ```json
   { "returnCode": 0 }
   ```
7. Verify in Azure SQL:
   ```sql
   SELECT * FROM pipeline_audit ORDER BY run_timestamp DESC;
   ```
   Expected:
   ```
   id | pipeline_name           | run_date   | rows_copied | status  | run_timestamp
    1 | pl_bronze_api_payments  | 2026-10-01 | 100         | success | 2026-10-01 08:23:11
   ```

---

## Activity 4 — Validation Activity (Wait for File)

> Pause the pipeline and poll ADLS until the Bronze file appears, then continue to Silver transform.

### Step 4.1 — Create a Dataset for the Expected File

1. **Author** → **Datasets** → **+ New dataset** → **Azure Data Lake Storage Gen2** → **JSON**
2. Name: `ds_bronze_payments_expected`
3. Linked service: `ls_adls_bronze`
4. File path:
   - Container: `bronze`
   - Directory → **Add dynamic content**: `api/payments/ingestion_date=@{dataset().p_run_date}`
   - File: `payments.json`
5. **Parameters** tab → add `p_run_date` (String)
6. **Publish all**

### Step 4.2 — Add Validation Activity

1. Open `pl_bronze_api_payments` → Activities panel → **General** → drag **Validation** to canvas
2. Rename to `act_wait_for_bronze`
3. **Wire it:**
   - `act_copy_payments` → `act_wait_for_bronze` (On Success)
   - `act_wait_for_bronze` → `act_silver_payments` (On Success)
   - Remove the direct arrow from `act_copy_payments` → `act_silver_payments`
4. **Settings tab:**
   - **Dataset:** `ds_bronze_payments_expected`
   - **Dataset properties:** `p_run_date` = `@variables('v_ingestion_date')`
   - **Timeout:** `0.00:10:00` (10 minutes)
   - **Sleep:** `30` (check every 30 seconds)
   - **Minimum size:** `1024` (must be at least 1KB — reject empty files)
5. **Publish all** → **Debug**
6. Monitor → `act_wait_for_bronze` → observe it polling → when file found, status = Succeeded

**Three outcomes to test:**

| Test | How | Expected |
|---|---|---|
| File exists, normal run | Run Debug normally | Succeeds on first poll |
| File is missing | Delete the Bronze file before Debug | Fails after 10-minute timeout |
| File is empty | Write a 0-byte file | Fails with minimum size error |

---

## Activity 5 — Azure Function Activity (Failure Alert)

> When the pipeline fails, call an Azure Function that sends a notification.

### Step 5.1 — Create Linked Service for Azure Function

1. **Manage** → **Linked services** → **+ New** → search **Azure Function**
2. Name: `ls_azure_function`
3. **Function App URL:** your Azure Function App base URL
4. **Function key** → Reference from Key Vault:
   - AKV linked service: `ls_keyvault`
   - Secret name: `function-host-key`
5. **Test connection** → **Create**

### Step 5.2 — Add Azure Function Activity

1. Open `pl_bronze_api_payments` → Activities panel → **Azure** → drag **Azure function** to canvas
2. Rename to `act_alert_on_failure`
3. **Wire it:** `act_copy_payments` → `act_alert_on_failure` with **On Failure** dependency:
   - Draw the arrow from `act_copy_payments` to `act_alert_on_failure`
   - Click the arrow → in the dropdown, change condition from **On Success** to **On Failure**
   - The arrow turns red — this means "run only when the source activity fails"
4. **Settings tab:**
   - **Linked service:** `ls_azure_function`
   - **Function name:** `SendPipelineAlert`
   - **Method:** POST
   - **Body** → **Add dynamic content**:
     ```
     @concat('{"pipeline":"', pipeline().pipelineName,
              '","run_date":"', variables('v_ingestion_date'),
              '","run_id":"', pipeline().RunId,
              '","message":"Bronze copy failed"}')
     ```
5. **Publish all** → **Debug** (normal run — alert does not fire because copy succeeds)

**To test the failure branch:**
1. Temporarily change `act_copy_payments` source URL to an invalid endpoint
2. Debug → Copy fails → `act_alert_on_failure` fires → Monitor shows the Function activity ran
3. Check your Azure Function App logs to confirm the POST was received
4. Restore the correct URL → Publish all

**Monitor — On Failure dependency view:**
```
act_copy_payments     Failed          ← deliberately broken URL
act_alert_on_failure  Succeeded       ← failure alert fired correctly
act_wait_for_bronze   Skipped         ← not reached (copy failed)
act_silver_payments   Skipped         ← not reached
act_log_audit         Skipped         ← not reached
```

---

## Final Pipeline Flow (Day 5 Complete)

```
act_get_username ──┐
                   ├──► act_api_login ──► act_set_token ──► act_set_ingestion_date
act_get_password ──┘                                               │
                                                         act_copy_payments
                                                          │            │
                                                     On Success    On Failure
                                                          │            │
                                              act_wait_for_bronze  act_alert_on_failure
                                                          │         (Azure Function)
                                              act_silver_payments
                                              (Data Flow OR Databricks NB)
                                                          │
                                              act_log_audit
                                              (Stored Procedure → pipeline_audit table)
```

---

## Quick Verification Checklist

| Activity | What to verify in Monitor |
|---|---|
| Mapping Data Flow | `rowsWritten > 0` in Output; Silver Parquet file exists in ADLS |
| Databricks Notebook | `runId` in Output; click run link to see notebook completed in Databricks |
| Stored Procedure | `returnCode: 0` in Output; row appears in `pipeline_audit` table |
| Validation | Status = Succeeded; pipeline continued to next activity |
| Azure Function | Fires only on failure; `statusCode: 200` in Output; Function App logs show request received |
