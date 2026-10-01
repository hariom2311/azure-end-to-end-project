# Day 5 — ADF Advanced Activities

> **Prerequisite:** Day 4 completed. `pl_bronze_api_payments` is running and copying VoltGrid payments to the Bronze layer.
> **Goal:** Cover the remaining ADF activity categories — Move & Transform (Mapping Data Flow), Databricks Notebook, Stored Procedure, Validation, and Azure Function — and wire them into the VoltGrid lakehouse pipeline.

---

## Where We Are in the Lakehouse

```
Day 2–4 built:
  VoltGrid API → [ADF] → ADLS Gen2 Bronze  (raw JSON, partitioned by date)

Day 5 adds:
  Bronze JSON → [Mapping Data Flow] → ADLS Gen2 Silver  (clean Parquet, typed columns)
  Bronze JSON → [Databricks Notebook] → ADLS Gen2 Silver (alternative: Spark)
  Silver land → [Validation Activity] → confirm file arrived before next step
  Each run   → [Stored Procedure] → Azure SQL audit log (rows, status, run_date)
  On failure  → [Azure Function] → email alert or custom logic
```

---

## Part 1: Move & Transform — Mapping Data Flow

### 1.1 What is Mapping Data Flow?

Mapping Data Flow (MDF) is ADF's visual, code-free data transformation engine. You build a transformation graph in the ADF Studio UI — no Spark code required — and ADF compiles it to Spark and runs it on a managed cluster.

```
Data Flow canvas
  Source → Transformations → Sink

Transformations available:
  Filter        → remove rows (WHERE clause)
  Select        → pick/rename columns (SELECT)
  Derived Column → add/compute new columns (new expressions)
  Aggregate     → GROUP BY with SUM, COUNT, AVG
  Join          → LEFT/INNER/FULL join two streams
  Lookup (MDF)  → enrich rows from a reference dataset
  Sort          → ORDER BY
  Flatten       → unnest JSON arrays into rows
  Cast          → change column data types
  Conditional Split → route rows to different sinks based on condition
  Sink          → write output (ADLS, SQL, etc.)
```

**MDF vs Copy Activity:**

| | Copy Activity | Mapping Data Flow |
|---|---|---|
| Purpose | Move data as-is | Transform data |
| Transforms | None (only basic column mapping) | Full Spark transformation graph |
| Code required | No | No (visual) |
| Compute | Azure IR | Spark cluster (3–5 min warm-up) |
| Best for | Raw ingestion (API → Bronze) | Bronze → Silver cleaning |

---

### 1.2 Data Flow — Key Concepts

**Source:** Where data flows in from. Can be a dataset (ADLS JSON, Parquet, Azure SQL, etc.).

**Sink:** Where the transformed data goes. Configured separately from the ADF dataset but uses the same linked services.

**Debug mode:** Turn on the Data Flow Debug slider at the top of the studio. This starts a Spark cluster (takes ~3 min). In debug mode, you can preview data at each transformation step — very useful for developing transforms.

**Integration Runtime:** Data Flows always use Azure Integration Runtime — not Self-hosted IR. The default AutoResolveIntegrationRuntime works for ADLS Gen2.

**Data Flow vs Data Flow Activity:**
- **Data Flow** is the transformation graph — the visual canvas you design
- **Data Flow Activity** is the ADF activity that runs the Data Flow from inside a pipeline — it is what connects your pipeline to the transformation logic

---

### 1.3 Build a Silver-Layer Data Flow for Payments

#### What the Bronze payments JSON looks like (raw from API)

```json
[
  {
    "id": 101,
    "session_id": "S-2001",
    "amount": "45.80",
    "currency": "AUD",
    "payment_method": "credit_card",
    "status": "completed",
    "created_at": "2026-09-28T07:23:11Z",
    "updated_at": "2026-09-28T07:23:45Z"
  }
]
```

#### What the Silver payments Parquet should look like (clean, typed, partitioned)

```
id              INT
session_id      STRING
amount_aud      DOUBLE        ← renamed + cast from string
payment_method  STRING
status          STRING
created_date    DATE          ← extracted from created_at timestamp
ingestion_date  DATE          ← partition column (today's date)
```

Rows with `status = 'failed'` are written to a separate error sink. Active payments go to the main Silver sink.

---

### 1.4 Step-by-Step: Create the Payments Data Flow

#### Step A — Create the Data Flow

1. **Author** → **Data flows** → **+** → **New data flow**
2. Name: `df_silver_payments`
3. Turn on **Data Flow Debug** (top of canvas) — Spark cluster starts (~3 min)

#### Step B — Add Source

1. Click **Add Source** on the canvas
2. **Source settings** tab:
   - **Output stream name:** `src_bronze_payments`
   - **Source type:** Dataset
   - **Dataset:** `ds_bronze_payments_sink` (the same ADLS dataset used as sink in Day 2)
   - *(The Bronze folder contains all the raw JSON files)*
3. **Projection** tab → **Import schema** → ADF infers the columns from the JSON file
4. Click **Data preview** tab → verify rows appear

#### Step C — Add Filter (remove failed payments)

1. Click the **+** after `src_bronze_payments` → **Filter**
2. **Output stream name:** `flt_active`
3. **Filter on** → **Add dynamic content**:
   ```
   notEquals(status, 'failed')
   ```
4. **Data preview** → confirm `status = 'failed'` rows are gone

#### Step D — Add Derived Column (clean and compute columns)

1. Click **+** after `flt_active` → **Derived Column**
2. **Output stream name:** `drc_clean`
3. **Columns** → add each transformation:

   | Column name | Expression |
   |---|---|
   | `amount_aud` | `toDouble(amount)` |
   | `created_date` | `toDate(created_at, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'')` |
   | `ingestion_date` | `currentDate()` |

4. **Data preview** → verify `amount_aud` is a number, `created_date` is a date

#### Step E — Add Select (keep only needed columns, rename)

1. Click **+** after `drc_clean` → **Select**
2. **Output stream name:** `sel_final`
3. Keep columns: `id`, `session_id`, `amount_aud`, `payment_method`, `status`, `created_date`, `ingestion_date`
4. Remove: `amount` (replaced by `amount_aud`), `currency`, `updated_at`, `created_at`

#### Step F — Add Aggregate (optional — row count per date)

1. Click **+** after `sel_final` → **Aggregate**
2. **Output stream name:** `agg_summary`
3. **Group by:** `created_date`
4. **Aggregates:**
   - `total_payments` = `count(id)`
   - `total_amount_aud` = `sum(amount_aud)`

> This stream goes to a separate summary sink. The `sel_final` stream continues to the main sink.

#### Step G — Add Silver Sink

1. Click **+** after `sel_final` → **Sink**
2. **Output stream name:** `sink_silver`
3. **Sink type:** Dataset → create new dataset:
   - **+ New dataset** → **Azure Data Lake Storage Gen2** → **Parquet**
   - Name: `ds_silver_payments_sink`
   - Linked service: `ls_adls_bronze` (or a silver linked service if you have one)
   - File path: `silver` / `api/payments`
4. **Settings** tab:
   - **File name option:** `Output to single file` → `payments.parquet` (for demo; in production use partitioning)
   - **Partition option:** `Set partitioning` → by `ingestion_date`
5. **Publish all**

---

### 1.5 Wire Data Flow into a Pipeline

A Data Flow is not automatically triggered — you run it via a **Data Flow Activity** inside a pipeline.

1. **Author** → **Pipelines** → open `pl_bronze_api_payments` (or create `pl_silver_payments`)
2. Activities panel → **Move & Transform** → drag **Data flow** to canvas
3. Rename to `act_silver_payments_transform`
4. **Settings tab:**
   - **Data flow:** `df_silver_payments`
   - **Run on (Azure IR):** AutoResolveIntegrationRuntime
   - **Compute type:** General purpose
   - **Core count:** 8 (minimum for non-debug runs)
5. **Wire it** after `act_copy_payments` (Bronze must be written before Silver transform runs):
   ```
   act_copy_payments → act_silver_payments_transform
   ```
6. **Publish all** → **Debug**
7. Monitor → click `act_silver_payments_transform` → observe Spark cluster start + transformation run

**Monitor output:**
```json
{
  "runStatus": {
    "metrics": {
      "sink_silver": { "rowsWritten": 95, "time": 42 }
    }
  }
}
```

---

## Part 2: Databricks Notebook Activity

### 2.1 What It Does

The Databricks Notebook Activity runs an Azure Databricks notebook from inside an ADF pipeline. It is the standard way to trigger Spark-based transformations (Bronze → Silver → Gold) when your team uses Databricks for heavy processing.

```
ADF Pipeline
  act_copy_payments (Copy Activity — writes Bronze JSON)
       │
  act_run_silver_nb (Databricks Notebook Activity)
       │
       ▼
  Azure Databricks
    Notebook: /VoltGrid/silver/process_payments
    Reads:    bronze/api/payments/ingestion_date=2026-10-01/*.json
    Writes:   silver/api/payments/ingestion_date=2026-10-01/part-00000.parquet
```

---

### 2.2 Setup — Linked Service for Databricks

Before adding the activity, create a linked service that connects ADF to your Databricks workspace.

1. **Manage** → **Linked services** → **+ New** → search **Azure Databricks**
2. Name: `ls_databricks`
3. **Azure subscription:** select your subscription
4. **Databricks workspace:** select your workspace
5. **Select cluster:** choose between:
   - **Existing interactive cluster** — reuses a running cluster (faster, costs more when idle)
   - **New job cluster** — spins up a cluster just for this job, terminates after (recommended for production)
6. **Authentication:** select **Access token** → store the token in Key Vault → reference with:
   - **AKV linked service:** `ls_keyvault`
   - **Secret name:** `databricks-access-token`
7. **Test connection** → Succeeded → **Create**

---

### 2.3 Add Databricks Notebook Activity to Pipeline

1. Open `pl_bronze_api_payments` → Activities panel → **Databricks** → drag **Notebook** to canvas
2. Rename to `act_run_silver_payments`
3. **Wire it:** `act_copy_payments` → `act_run_silver_payments` (On Success)
4. **Azure Databricks tab:**
   - **Databricks linked service:** `ls_databricks`
5. **Settings tab:**
   - **Notebook path:** `/VoltGrid/silver/process_payments`
   - **Base parameters** — pass pipeline values to the notebook:
     | Key | Value |
     |---|---|
     | `run_date` | `@variables('v_ingestion_date')` |
     | `token` | `@variables('v_token')` |
     | `source_path` | `@concat('bronze/api/payments/ingestion_date=', variables('v_ingestion_date'), '/')` |
6. **Publish all** → **Debug**

**In the Databricks notebook**, these parameters are accessed as:

```python
# Python (Databricks)
run_date    = dbutils.widgets.get("run_date")
source_path = dbutils.widgets.get("source_path")

df = spark.read.json(f"abfss://bronze@youraccount.dfs.core.windows.net/{source_path}")
df_clean = df.filter(df.status != "failed")
df_clean.write.mode("overwrite").parquet(
    f"abfss://silver@youraccount.dfs.core.windows.net/api/payments/ingestion_date={run_date}/"
)
```

**Monitor:** click `act_run_silver_payments` → **Output** tab → Databricks job run ID → follow the link directly to the Databricks run page.

---

### 2.4 Databricks Activity vs Mapping Data Flow

| | Databricks Notebook | Mapping Data Flow |
|---|---|---|
| Code | Python/Scala/SQL notebook | Visual canvas, no code |
| Cluster | Your Databricks workspace | ADF-managed Spark (AutoResolve IR) |
| Control | Full — any Spark operation | Limited to built-in transformations |
| Debugging | Databricks UI | ADF data preview (debug mode) |
| Team | Data engineers who know Spark | Anyone comfortable with ADF UI |
| Cost | Databricks + ADF | ADF only |
| Use when | Complex logic, ML, Delta Lake | Standard ETL: filter, join, aggregate |

---

## Part 3: Stored Procedure Activity

### 3.1 What It Does

Calls a stored procedure in Azure SQL Database (or SQL Managed Instance). Commonly used to:
- Write an audit log after each pipeline run
- Update a watermark table (last successful run timestamp)
- Trigger a database-side post-processing job

---

### 3.2 Setup — Linked Service for Azure SQL

1. **Manage** → **Linked services** → **+ New** → **Azure SQL Database**
2. Name: `ls_azure_sql`
3. **Server:** your Azure SQL server name
4. **Database:** `ev_ops_db` (or your audit database)
5. **Authentication:** SQL authentication or Managed Identity
6. **Test connection** → Create

---

### 3.3 Create the Audit Table and Stored Procedure

Run this in your Azure SQL database:

```sql
-- Audit table
CREATE TABLE pipeline_audit (
    id              INT IDENTITY PRIMARY KEY,
    pipeline_name   VARCHAR(200),
    run_date        DATE,
    rows_copied     INT,
    status          VARCHAR(50),
    run_timestamp   DATETIME DEFAULT GETDATE()
);

-- Stored procedure called by ADF
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

---

### 3.4 Add Stored Procedure Activity to Pipeline

1. Open `pl_bronze_api_payments` → Activities panel → **General** → drag **Stored procedure** to canvas
2. Rename to `act_log_audit`
3. **Wire it:** `act_copy_payments` → `act_log_audit` (On Success)
4. **Settings tab:**
   - **Linked service:** `ls_azure_sql`
   - **Stored procedure name:** `usp_log_pipeline_run`
   - **Stored procedure parameters** → **+ New** for each:

     | Name | Type | Value |
     |---|---|---|
     | `pipeline_name` | String | `@pipeline().pipelineName` |
     | `run_date` | String | `@variables('v_ingestion_date')` |
     | `rows_copied` | Int32 | `@activity('act_copy_payments').output.rowsCopied` |
     | `status` | String | `success` |

5. **Publish all** → **Debug**
6. Monitor → click `act_log_audit` → **Output** tab → `{"returnCode": 0}` = procedure ran successfully
7. Query Azure SQL to verify: `SELECT * FROM pipeline_audit ORDER BY run_timestamp DESC`

**Result in the audit table:**
```
id | pipeline_name            | run_date   | rows_copied | status  | run_timestamp
---+--------------------------+------------+-------------+---------+---------------------
1  | pl_bronze_api_payments   | 2026-10-01 | 100         | success | 2026-10-01 08:23:11
```

**Why use Stored Procedure instead of inserting directly from Copy Activity?**
- Copy Activity can only write to a dataset — it cannot run SQL logic
- The stored procedure can do complex upserts, lookups, and conditionals that Copy Activity cannot
- Keeping audit logic in SQL means it can be tested independently of ADF

---

## Part 4: Validation Activity

### 4.1 What It Does

Validation Activity pauses the pipeline and polls for a file or folder in ADLS Gen2. It keeps polling until:
- The file appears → pipeline continues (Succeeded)
- The timeout is reached → pipeline fails with a timeout error

Think of it as a file-arrival trigger within a pipeline — useful when:
- An upstream team drops a file that your pipeline must wait for
- A Databricks job writes a `_SUCCESS` marker file that signals completion
- You want to confirm Bronze data landed before starting Silver processing

---

### 4.2 Add Validation Activity to Pipeline

#### Scenario: Wait for the Bronze payments file before starting Silver transform

1. Create a dataset pointing to the expected Bronze output file:
   - **Author** → **Datasets** → **+ New dataset** → **Azure Data Lake Storage Gen2** → **JSON**
   - Name: `ds_bronze_payments_check`
   - Linked service: `ls_adls_bronze`
   - File path: Container `bronze`, Directory → **Add dynamic content**: `api/payments/ingestion_date=@{dataset().p_run_date}`, File: `payments.json`
   - **Parameters** tab → add `p_run_date` (String)
   - **Publish all**

2. Open `pl_bronze_api_payments` → Activities panel → **General** → drag **Validation** to canvas
3. Rename to `act_wait_for_bronze`
4. **Wire it:** `act_copy_payments` → `act_wait_for_bronze` → `act_silver_payments_transform`
5. **Settings tab:**
   - **Dataset:** `ds_bronze_payments_check`
   - **Dataset properties:** `p_run_date` = `@variables('v_ingestion_date')`
   - **Timeout:** `0.00:10:00` (10 minutes — if file not there in 10 min, fail)
   - **Sleep:** `30` (check every 30 seconds)
   - **Minimum size:** `1024` (file must be at least 1KB — avoids accepting an empty file)
6. **Publish all** → **Debug**

**Monitor — three outcomes:**

| Outcome | What Monitor shows |
|---|---|
| File appears within timeout | `act_wait_for_bronze` = Succeeded, downstream continues |
| File never appears | `act_wait_for_bronze` = Failed (timeout) — downstream Skipped |
| File appears but size < 1KB | `act_wait_for_bronze` = Failed (minimum size not met) |

**Validation Activity settings explained:**

| Setting | What it means |
|---|---|
| Timeout | Max wait time. Format: `D.HH:MM:SS`. `0.00:10:00` = 10 minutes |
| Sleep | Interval between each poll attempt (seconds) |
| Minimum size | File must be at least this many bytes to be considered valid |

---

## Part 5: Azure Function Activity

### 5.1 What It Does

Azure Function Activity calls an Azure Function (HTTP-triggered) from a pipeline. You write any custom logic in the Function (Python, C#, JavaScript) — ADF just calls it via HTTP and passes parameters.

**Common uses:**
- Send a failure alert email (via SendGrid or Logic App)
- Validate data quality rules that are too complex for ADF expressions
- Trigger an external system or third-party API that needs custom auth
- Write a custom audit record to Cosmos DB or another non-SQL store

---

### 5.2 Setup — Linked Service for Azure Function

1. **Manage** → **Linked services** → **+ New** → **Azure Function**
2. Name: `ls_azure_function`
3. **Function App URL:** your Azure Function App base URL (e.g. `https://ev-alerts.azurewebsites.net`)
4. **Function key:** store the function's host key in Key Vault → reference:
   - **AKV linked service:** `ls_keyvault`
   - **Secret name:** `function-host-key`
5. **Test connection** → Create

---

### 5.3 Add Azure Function Activity to Pipeline

#### Scenario: On pipeline failure, call an Azure Function to send a failure email

1. Open `pl_bronze_api_payments` → Activities panel → **Azure** → drag **Azure function** to canvas
2. Rename to `act_alert_on_failure`
3. **Wire it:** draw a dependency from `act_copy_payments` → `act_alert_on_failure` with **On Failure** (red arrow)
   - Click the dependency arrow → change condition to **Failed**
4. **Settings tab:**
   - **Linked service:** `ls_azure_function`
   - **Function name:** `SendPipelineAlert`
   - **Method:** POST
   - **Body** → **Add dynamic content**:
     ```json
     @concat('{',
       '"pipeline":"', pipeline().pipelineName, '",',
       '"run_date":"', variables('v_ingestion_date'), '",',
       '"run_id":"', pipeline().RunId, '",',
       '"message":"Bronze copy failed for payments endpoint"',
     '}')
     ```
5. **Publish all** → **Debug**

**How the Azure Function receives the call:**

```python
# Azure Function (Python)
import azure.functions as func
import json, logging

def main(req: func.HttpRequest) -> func.HttpResponse:
    body = req.get_json()
    pipeline = body['pipeline']
    run_date = body['run_date']
    message  = body['message']

    logging.info(f"Alert: {pipeline} failed on {run_date}: {message}")
    # Send email via SendGrid or Teams webhook here
    return func.HttpResponse("Alert sent", status_code=200)
```

**Monitor — what to verify:**
- `act_alert_on_failure` is wired with **On Failure** dependency — it only runs when `act_copy_payments` fails
- To test: deliberately break the copy source URL → Debug → Copy fails → Function activity fires → check Function App logs in Azure Portal

---

### 5.4 Azure Function vs Web Activity

| | Web Activity | Azure Function Activity |
|---|---|---|
| Target | Any public HTTP endpoint | Azure Function specifically |
| Auth | Supports MSI, Basic, Client Cert | Function key (stored in Key Vault) |
| Custom code | Not applicable | Yes — full code in the Function |
| Linked service | Not required | Required (`ls_azure_function`) |
| Use when | Calling Key Vault, REST APIs, webhooks | Need custom server-side logic |

---

## Part 6: Full Day 5 Pipeline — End-to-End

Combining all Day 5 activities into one orchestrated flow:

```
pl_ev_daily_ingestion  (Day 5 orchestrator)

┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│  [act_get_username] ──┐                                                  │
│  [act_get_password] ──┴──► [act_api_login] ──► [act_set_token]          │
│                                                       │                  │
│                                             [act_set_ingestion_date]     │
│                                                       │                  │
│                                             [act_copy_payments]  ◄── Copy Activity (Bronze)
│                                              │         │                 │
│                                         On Fail   On Success             │
│                                              │         │                 │
│                              [act_alert_on_failure]    │                 │
│                              Azure Function            │                 │
│                              (sends email)             │                 │
│                                                        │                 │
│                                             [act_wait_for_bronze]        │
│                                             Validation Activity          │
│                                             (poll until file appears)    │
│                                                        │                 │
│                                             [act_silver_transform]       │
│                                             Data Flow OR Databricks NB   │
│                                                        │                 │
│                                             [act_log_audit]              │
│                                             Stored Procedure             │
│                                             (insert into pipeline_audit) │
└──────────────────────────────────────────────────────────────────────────┘
```

**Activities used in Day 5:**
- Data Flow Activity — Bronze JSON → Silver Parquet (visual ETL)
- Databricks Notebook Activity — alternative Spark-based Silver transform
- Stored Procedure Activity — audit log after every run
- Validation Activity — confirm Bronze file exists before Silver starts
- Azure Function Activity — failure alert via HTTP

---

## Activity Categories Recap — All 5 Days

```
ADF Activities Panel
├── General                   ← Day 4 (10 activities)
│   Copy Data, Web, Set Variable, Append Variable, Execute Pipeline,
│   Get Metadata, Delete, Lookup, Wait, Fail
│   + Stored Procedure, Validation   ← Day 5
│
├── Iteration & Conditionals  ← Day 4 (5 activities)
│   ForEach, If Condition, Switch, Until, Filter
│
├── Move & Transform          ← Day 5
│   Data Flow (Mapping Data Flow)
│
├── Azure                     ← Day 5
│   Databricks Notebook, Azure Function
│
└── (Others: HDInsight, Synapse, Azure Batch, Azure ML, etc.)
```

---

## Quick Reference — When to Use Each Day 5 Activity

```
I need to...                                             Use this
───────────────────────────────────────────────────────────────────────
Clean / transform / join data visually (no code)    →  Mapping Data Flow
Run a Databricks/Spark notebook from a pipeline     →  Databricks Notebook
Write an audit log to Azure SQL after each run      →  Stored Procedure
Wait until a file appears before continuing         →  Validation
Custom logic: email alert, external API call        →  Azure Function
```
