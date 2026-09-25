# Day 1 — Azure Data Factory: Core Concepts, Linked Services, Datasets & Copy Activity

## Overview

Before building data pipelines in Azure Data Factory, you need to understand what ADF actually is, why it exists, and how its three fundamental building blocks — Linked Services, Datasets, and Activities — fit together. Day 1 covers everything from provisioning your first ADF instance in the Azure portal to running a Debug pipeline that copies a CSV file from an HTTP source into ADLS Gen2 as Parquet.

**The 3 concepts:**
1. What is Azure Data Factory — why it exists, ETL vs. ELT, ADF vs. Python scripts, and the ADF Studio UI
2. Linked Services and Datasets — saved connections and data shape/location definitions
3. Copy Activity and Debug Run — the data mover, schema mapping, DIUs, and fault tolerance

---

## Concept 1: What is Azure Data Factory?

### The core problem ADF solves

Imagine your company has data sitting in ten different places: a payment processor's HTTP API, an on-premises SQL Server, an Oracle database in a data centre, Salesforce, and six more SaaS tools. Every night, someone needs to pull data from all ten sources, clean it, and load it into the data lake so analysts can run reports in the morning.

Without ADF, a data engineer writes Python scripts for each source, sets up a scheduler (cron or Airflow), handles retries manually, builds monitoring dashboards from scratch, and re-does all of this work every time a new source is added. This works — but it does not scale, it is hard for non-engineers to maintain, and monitoring failures at 3 AM is miserable.

**Azure Data Factory** is Microsoft's managed, cloud-native data integration service. It is a no-code / low-code orchestration platform that:
- Connects to 90+ data sources out of the box (HTTP, SQL, Salesforce, SAP, S3, ADLS, CosmosDB, and more)
- Schedules and orchestrates data movement and transformation pipelines
- Provides a visual drag-and-drop pipeline designer
- Monitors every pipeline run with full logs, retry history, and alerting
- Scales automatically with zero infrastructure to manage

### ETL vs. ELT — and where ADF fits

**ETL (Extract, Transform, Load):** Transform data before loading it into the destination. The transformation happens in a middle-tier compute engine. The destination receives clean, shaped data.

**ELT (Extract, Load, Transform):** Load raw data into the destination first, then transform it inside the destination using its own compute (Spark, SQL). The destination is powerful enough to do the transformation itself.

| | ETL | ELT |
|---|---|---|
| When to transform | Before loading | After loading |
| Where transformation runs | Middle-tier compute (SSIS, Informatica, ADF Data Flow) | Inside the destination (Spark, Synapse SQL, dbt) |
| Best for | Structured, relational targets (data warehouse) | Cloud data lakes (Databricks, Synapse, BigQuery) |
| ADF role | Full ETL engine using Data Flow activity | Orchestration layer — load raw to Bronze, trigger Spark to transform |

**In this course:** We use ADF for ELT. ADF's Copy Activity loads raw data into the Bronze layer of ADLS Gen2. Databricks or Synapse Spark then transforms Bronze → Silver → Gold. ADF orchestrates the whole chain.

### ADF vs. writing Python scripts

A common question: *why use ADF at all? Can't I just write a Python script with `requests` and `pyarrow`?*

| Dimension | Python script | Azure Data Factory |
|---|---|---|
| Time to first pipeline | Fast (minutes) | Slightly slower (UI setup) |
| Connectivity | Write SDK code per source | 90+ connectors built-in |
| Scheduling | External tool (cron, Airflow) | Built-in triggers (schedule, event, tumbling window) |
| Monitoring | Build it yourself | Built-in run history, alerts, Log Analytics |
| Retry logic | Write it yourself | Configurable retry policy per activity |
| Scaling | Manual (add more VMs) | Auto-scale with DIUs |
| Non-engineer friendly | No | Yes — visual designer |
| Cost | Compute cost 24/7 | Serverless — pay only per pipeline run |

**When Python scripts are better:** Simple one-time migrations, highly custom transformation logic, team with strong Python skills and an existing Airflow setup.

**When ADF is better:** Ongoing, scheduled ingestion from many sources, team has mixed technical skills, built-in monitoring is a priority, Azure-native ecosystem.

### The ADF Studio UI — 4 panels

ADF Studio is the browser-based designer at `https://adf.azure.com`. It has four main areas:

```
ADF Studio
├── Author     ← Build pipelines, linked services, datasets, data flows
├── Monitor    ← View all pipeline runs, activity runs, trigger runs, debug history
├── Manage     ← Manage linked services, integration runtimes, git configuration, triggers
└── Learn      ← ADF documentation, templates, tutorials (quick-start gallery)
```

| Panel | What you do here |
|---|---|
| **Author** | Create and edit pipelines. Drag activities onto the canvas. Configure source and sink. Set parameters. Write expressions. |
| **Monitor** | See every pipeline run. Filter by status (Succeeded, Failed, In Progress). Drill into individual activity runs. View input/output JSON. |
| **Manage** | Create Linked Services (connections). Configure Integration Runtime (self-hosted or Azure). Set up triggers. Manage Git repo. |
| **Learn** | ADF template gallery — pre-built pipeline templates for common patterns (HTTP to ADLS, SQL to Parquet, SalesForce to Data Lake). |

---

### Step-by-step: Create an ADF instance in `rg-datalake-dev`

**Step 1 — Open the Azure Portal**
- Go to `https://portal.azure.com` and sign in

**Step 2 — Search for Data Factory**
- In the top search bar, type **"Data factories"**
- Click **"Data factories"** under Services

**Step 3 — Click Create**
- Click **"+ Create"**

**Step 4 — Fill in the Basics tab**

| Field | Value |
|---|---|
| Subscription | Free Trial (or your active subscription) |
| Resource group | `rg-datalake-dev` |
| Region | **Australia East** |
| Name | `adf-datalake-dev-001` (must be globally unique) |
| Version | **V2** (always use V2) |

- Click **"Next: Git configuration"**

**Step 5 — Git configuration tab**
- For this course: check **"Configure Git later"**
- In production you would connect to Azure DevOps or GitHub for source control
- Click **"Next: Networking"**

**Step 6 — Networking tab**
- Connectivity method: **Public endpoint**
- Click **"Next: Advanced"**

**Step 7 — Advanced tab (leave defaults)**
- Click **"Review + create"**

**Step 8 — Review and Create**
- Confirm: Resource group = `rg-datalake-dev`, Region = Australia East, Version = V2
- Click **"Create"**
- Deployment takes approximately 60 seconds

**Step 9 — Launch ADF Studio**
- Click **"Go to resource"**
- On the ADF overview page, click **"Launch Studio"** (the blue button)
- ADF Studio opens in a new tab at `https://adf.azure.com`
- You should see the Author canvas with the 4 panel icons on the left sidebar

> **Checkpoint:** You can see the ADF Studio interface with the pencil icon (Author), monitor icon (Monitor), toolbox icon (Manage), and graduation cap icon (Learn) in the left sidebar. The Author canvas is empty — ready to build.

---

## Concept 2: Linked Services and Datasets

### Linked Services — analogy: saved contacts in your phone

A **Linked Service** is a saved connection definition. It stores the information ADF needs to connect to a data source or destination: the URL, authentication method, credentials (or a reference to Key Vault for the credentials).

**Phone book analogy:** A Linked Service is like a saved contact in your phone. When you want to call your bank, you do not re-enter the phone number every time — you tap "Bank" and the number is already there. Similarly, when you want to connect to your ADLS Gen2 account, you do not re-enter the account name, key, and URL every time — you reference the Linked Service `ls_adls_gen2` and ADF fills in the details.

**Key rule:** Create one Linked Service per source system, not one per pipeline. All pipelines that read from ADLS Gen2 share the same `ls_adls_gen2` Linked Service.

```
Linked Services (created once in Manage panel):
├── ls_adls_gen2         ← connects to stadlsdev001 (ADLS Gen2 storage account)
├── ls_http_source       ← connects to https://data.example.com (HTTP endpoint)
├── ls_sql_payments      ← connects to payments SQL Server database
└── ls_keyvault          ← connects to Azure Key Vault (to retrieve secrets)

Pipelines (reuse existing Linked Services):
├── pl_ingest_http_to_bronze       → uses ls_http_source + ls_adls_gen2
├── pl_ingest_sql_to_bronze        → uses ls_sql_payments + ls_adls_gen2
└── pl_refresh_gold_payments       → uses ls_adls_gen2 + ls_adls_gen2
```

### Datasets — analogy: shipping label on a parcel

A **Dataset** is a description of data — its location (which Linked Service, which container/path) and its shape (file format, delimiter, schema). It does not contain the data itself; it just describes where the data is and what it looks like.

**Shipping label analogy:** A Dataset is like the label on a parcel. The label tells the courier where to pick up the package (source), what is inside (format), and where to deliver it (sink). The courier (Copy Activity) does not need to know the internals — it just reads the label and moves the parcel.

```
Dataset = Linked Service reference + location + format

ds_http_source_csv:
  Linked Service: ls_http_source
  Relative URL:   /datasets/ev-payments/payments.csv
  Format:         DelimitedText (CSV)
  Delimiter:      comma
  First row header: true

ds_adls_bronze_parquet:
  Linked Service: ls_adls_gen2
  Container:      bronze
  Directory:      payments/@{pipeline().parameters.run_date}
  Format:         Parquet
  Compression:    snappy
```

### ADF Expressions

ADF has a built-in expression language used in Dataset paths, activity parameters, and conditions. Expressions are wrapped in `@{ }` or `@` when the entire value is an expression.

```
# Reference a pipeline parameter
@pipeline().parameters.run_date

# Concatenate a dynamic folder path using a parameter
@{concat('bronze/payments/', pipeline().parameters.run_date, '/')}

# Reference a system variable (pipeline run ID)
@pipeline().RunId

# Reference an activity output (e.g., from a Lookup activity)
@activity('LookupSourceList').output.firstRow.table_name

# Conditional expression
@if(equals(pipeline().parameters.env, 'prod'), 'stadlsprod001', 'stadlsdev001')
```

---

### Step-by-step: Create the ADLS Gen2 Linked Service (`ls_adls_gen2`)

**Step 1 — Open the Manage panel**
- In ADF Studio, click the toolbox icon (Manage) in the left sidebar

**Step 2 — Navigate to Linked Services**
- Under "Connections", click **"Linked services"**
- Click **"+ New"**

**Step 3 — Search for ADLS Gen2**
- In the "New linked service" search box, type **"Azure Data Lake Storage Gen2"**
- Click on the **"Azure Data Lake Storage Gen2"** tile
- Click **"Continue"**

**Step 4 — Fill in the Linked Service configuration**

| Field | Value |
|---|---|
| Name | `ls_adls_gen2` |
| Description | Connection to stadlsdev001 ADLS Gen2 bronze/silver/gold lake |
| Connect via integration runtime | AutoResolveIntegrationRuntime |
| Authentication method | Account key |
| Account selection method | From Azure subscription |
| Azure subscription | Your subscription |
| Storage account name | `stadlsdev001` |

**Step 5 — Test the connection**
- Click **"Test connection"** at the bottom
- You should see a green **"Connection successful"** message

**Step 6 — Create**
- Click **"Create"**
- `ls_adls_gen2` now appears in your Linked Services list

> **Checkpoint:** The Linked Services list shows `ls_adls_gen2` with a green status indicator. Test connection succeeded.

---

### Step-by-step: Create the HTTP Linked Service (`ls_http_source`)

**Step 1 — Click "+ New"** in the Linked Services panel

**Step 2 — Search for HTTP**
- Type **"HTTP"** in the search box
- Click the **"HTTP"** tile (generic HTTP connector)
- Click **"Continue"**

**Step 3 — Fill in the configuration**

| Field | Value |
|---|---|
| Name | `ls_http_source` |
| Description | Generic HTTP connector for CSV source data |
| Base URL | `https://raw.githubusercontent.com` (or your actual source URL) |
| Authentication type | Anonymous |
| Enable server certificate validation | Checked |

**Step 4 — Test connection and Create**
- Click **"Test connection"** → should succeed (HTTP 200 from the base URL)
- Click **"Create"**

---

### Step-by-step: Create the source Dataset (`ds_http_source_csv`)

**Step 1 — Go to the Author panel**
- Click the pencil icon (Author) in the left sidebar

**Step 2 — Create a new Dataset**
- In the Factory Resources pane (left side of Author), click the **"+"** next to "Datasets"
- Select **"New dataset"**

**Step 3 — Search for HTTP**
- Type **"HTTP"** in the search
- Select **"HTTP"** → click **"Continue"**

**Step 4 — Select format**
- Select **"DelimitedText"** (CSV)
- Click **"Continue"**

**Step 5 — Configure the Dataset**

| Field | Value |
|---|---|
| Name | `ds_http_source_csv` |
| Linked service | `ls_http_source` |
| Relative URL | `/hariom2311/azure-ev-end-to-end-project/main/payments%20-%2001%20Jan%2026%20-%2017%20Sep%2026.csv` |
| First row as header | Checked |
| Import schema | From connection/store |

- Click **"OK"**

**Step 6 — Verify the Schema tab**
- Click on `ds_http_source_csv` to open it
- Go to the **"Schema"** tab → click **"Import schema"**
- ADF reads the CSV headers and populates the column list automatically

> **Checkpoint:** The Schema tab shows column names imported from the CSV (e.g., `transaction_id`, `amount`, `currency`, `timestamp`). If you see "0 columns", check the Relative URL and Linked Service base URL.

---

### Step-by-step: Create the sink Dataset (`ds_adls_bronze_parquet`)

**Step 1 — Create a new Dataset**
- In the Factory Resources pane, click **"+"** next to Datasets → **"New dataset"**

**Step 2 — Search for ADLS Gen2**
- Type **"Azure Data Lake Storage Gen2"**
- Select **"Azure Data Lake Storage Gen2"** → click **"Continue"**

**Step 3 — Select format**
- Select **"Parquet"**
- Click **"Continue"**

**Step 4 — Configure the Dataset**

| Field | Value |
|---|---|
| Name | `ds_adls_bronze_parquet` |
| Linked service | `ls_adls_gen2` |
| File path — Container | `bronze` |
| File path — Directory | `payments/@{dataset().run_date}` |
| File path — File | `payments_raw.parquet` |
| Import schema | None (sink schema is defined by source mapping) |

**Step 5 — Add a Dataset parameter for `run_date`**
- Go to the **"Parameters"** tab
- Click **"+ New"**
- Name: `run_date`, Type: `String`, Default value: leave blank
- This allows each pipeline run to write to a date-partitioned path (e.g., `bronze/payments/2026-09-25/`)

- Click **"Publish All"** (top bar) to save your work

> **Checkpoint:** The directory path `payments/@{dataset().run_date}` shows in the Connection tab. The Parameters tab shows `run_date` as a String parameter.

---

## Concept 3: Copy Activity and Debug Run

### Copy Activity — analogy: logistics truck

A **Copy Activity** is ADF's data-mover. It picks up data from a source (defined by a Dataset), optionally maps and converts columns, and writes the data to a sink (defined by another Dataset).

**Logistics truck analogy:** Think of Copy Activity as a delivery truck. The source Dataset is the pickup address. The sink Dataset is the delivery address. The Copy Activity is the truck that drives between them. You configure what the truck should do: how fast to drive (DIUs), what to do if it encounters a broken road (fault tolerance), and how to relabel the boxes during transit (schema mapping).

```
Copy Activity
├── Source tab       ← which Dataset to read, query filter, partition options
├── Sink tab         ← which Dataset to write, write behaviour (overwrite, append)
├── Mapping tab      ← rename columns, change data types, skip columns
├── Settings tab     ← DIUs, degree of parallelism, fault tolerance, logging
└── User properties  ← custom key-value metadata (for monitoring)
```

### DIUs — Data Integration Units

**DIUs (Data Integration Units)** are the compute units ADF uses to run a Copy Activity. Each DIU is a bundle of CPU, memory, and network throughput.

| Setting | Behaviour | When to use |
|---|---|---|
| **Auto** (default) | ADF picks the optimal DIU count based on data size | Most cases — ADF is usually right |
| **Manual (e.g., 4 DIUs)** | You fix the DIU count | When you need predictable performance or cost |

More DIUs = higher throughput (copy faster) = higher cost per run. For small files (< 1 GB), `Auto` picks 2–4 DIUs. For large datasets (100+ GB), ADF may auto-scale to 32 DIUs.

### Fault tolerance — skip incompatible rows

When copying heterogeneous data (e.g., a CSV with some malformed rows), Copy Activity can be configured to:

| Option | Behaviour |
|---|---|
| **Fail on first error** (default) | Pipeline fails if any row is incompatible |
| **Skip incompatible rows** | Bad rows are skipped; the copy continues |
| **Log skipped rows** | Skipped rows are written to a log file in ADLS Gen2 |

This is configured in the **Settings** tab of the Copy Activity under "Fault tolerance".

---

### Step-by-step: Build `pl_ingest_http_to_bronze` and add Copy Activity

**Step 1 — Create a new Pipeline**
- In the Author panel, click **"+"** next to "Pipelines" → **"New pipeline"**
- In the Properties pane (right side), set:
  - **Name:** `pl_ingest_http_to_bronze`
  - **Description:** Ingests raw payment CSV from HTTP source into ADLS Gen2 bronze/payments/ as Parquet

**Step 2 — Add a Pipeline Parameter for `run_date`**
- At the bottom of the pipeline canvas, click the **"Parameters"** tab
- Click **"+ New"**

| Field | Value |
|---|---|
| Name | `run_date` |
| Type | String |
| Default value | `2026-09-25` |

**Step 3 — Add a Copy Activity to the canvas**
- In the Activities pane (left of canvas), expand **"Move & transform"**
- Drag **"Copy data"** onto the canvas
- Click on the Copy data activity to select it
- In the General tab at the bottom, set **Name:** `CopyPaymentsHttpToBronze`

**Step 4 — Configure the Source tab**
- Click the **"Source"** tab in the activity properties panel (bottom)

| Field | Value |
|---|---|
| Source dataset | `ds_http_source_csv` |
| Request method | GET |
| Additional headers | leave blank |
| Pagination rules | leave blank (single file) |

**Step 5 — Configure the Sink tab**
- Click the **"Sink"** tab

| Field | Value |
|---|---|
| Sink dataset | `ds_adls_bronze_parquet` |
| Dataset property — run_date | `@pipeline().parameters.run_date` |
| Copy behaviour | None (overwrite if file exists) |

This passes the pipeline's `run_date` parameter into the Dataset, which inserts it into the folder path `bronze/payments/2026-09-25/`.

**Step 6 — Configure the Mapping tab**
- Click the **"Mapping"** tab
- Click **"Import schemas"** — ADF reads both source and sink schemas and shows them side by side
- By default, columns are mapped by name (automatic)

You should see the source CSV columns on the left mapped to matching sink columns on the right.

**Step 7 — Configure the Settings tab**
- Click the **"Settings"** tab

| Field | Value |
|---|---|
| Data integration units | Auto |
| Degree of copy parallelism | Auto |
| Fault tolerance | Skip incompatible rows |
| Enable logging | Checked |
| Log settings — Linked service | `ls_adls_gen2` |
| Log settings — Folder path | `logs/copy-activity-errors/` |

---

### Step-by-step: Configure schema mapping (column rename)

In the **Mapping** tab, you can rename columns between source and sink. For example, if the source CSV has `cust_id` but your Bronze schema standard uses `customer_id`:

**Step 1 — In the Mapping tab, locate the `cust_id` row**

**Step 2 — Under "Destination" column name, click on `cust_id`**
- The field becomes editable
- Change it to `customer_id`

**Step 3 — Click elsewhere to confirm**

The mapping now shows:
```
Source column    →    Sink column
─────────────────────────────────
cust_id          →    customer_id
amount           →    amount
currency         →    currency
timestamp        →    event_timestamp
```

You can also change data types in the "Type" dropdown next to each sink column — for example, converting a `String` `timestamp` column to `Timestamp` type in Parquet.

---

### Step-by-step: Debug run and verify output

**Step 1 — Click Debug**
- At the top of the pipeline canvas, click **"Debug"**
- ADF prompts you to enter parameter values for the run
- Set `run_date`: `2026-09-25`
- Click **"OK"**

**Step 2 — Watch the activity run**
- The pipeline canvas shows the activity turn yellow (running) then green (succeeded) or red (failed)
- At the bottom, the **"Output"** tab shows the run status in real time

**Step 3 — Inspect the Copy Activity output**
- Click the glasses icon (View details) on the activity run row in the Output tab
- You will see the Copy Activity details:

```
Data read:       2.1 MB (from HTTP source)
Data written:    0.8 MB (Parquet compressed with Snappy)
Files read:      1
Files written:   1
Rows copied:     14,230
Duration:        00:00:08
DIUs used:       2
Throughput:      0.26 MB/s
```

**Step 4 — Verify the Parquet file in ADLS Gen2**
- Go to the Azure portal → `stadlsdev001` → Containers → `bronze`
- Navigate to: `bronze/payments/2026-09-25/`
- You should see `payments_raw.parquet`
- Click on the file → **"View/Edit"** (or download and open in a Parquet viewer)

> **Checkpoint:** The file `payments_raw.parquet` exists at `bronze/payments/2026-09-25/payments_raw.parquet`. The Parquet file size should be smaller than the source CSV (due to Snappy compression). If the file does not appear, check the Copy Activity error output and review the sink Dataset path configuration.

**Step 5 — Publish All**
- Back in ADF Studio, click **"Publish All"** in the top bar
- A diff panel shows all unsaved changes (pipeline, datasets, linked services)
- Click **"Publish"**
- ADF saves all resources to the Data Factory

> **Checkpoint:** After Publish All, a green success banner appears. All resources are now permanently saved. The Debug run only tested the pipeline — Publish All makes your changes live and available to triggers.

---

## Summary

| Concept | Key takeaway |
|---|---|
| Azure Data Factory | Managed cloud-native data integration service — orchestrates data movement from 90+ sources with built-in scheduling, monitoring, and retry |
| ETL vs. ELT | ADF supports both; in modern data lakes, ADF is the ELT orchestrator (load raw to Bronze, trigger Spark to transform) |
| ADF vs. Python | ADF wins on connectivity, scheduling, monitoring, and non-engineer accessibility; Python wins on custom logic and existing Airflow setups |
| ADF Studio panels | Author (build), Monitor (observe), Manage (configure connections), Learn (templates) |
| Linked Service | Saved connection — one per source system; stores auth credentials; referenced by Datasets and Activities |
| Dataset | Data shape + location — references a Linked Service, adds file path, format, and schema; analogous to a shipping label |
| ADF Expression | `@pipeline().parameters.run_date` — used to make paths, values, and conditions dynamic at runtime |
| Copy Activity | The data mover — reads from source Dataset, applies column mapping, writes to sink Dataset |
| Schema Mapping | Rename or retype columns between source and sink in the Copy Activity Mapping tab |
| DIUs | Compute units for Copy Activity — Auto lets ADF optimise; manual control for cost predictability |
| Fault tolerance | Skip incompatible rows + log them to ADLS Gen2 — prevents one bad row from failing the entire pipeline run |
| Debug run | Runs the pipeline immediately in test mode with parameter values you enter — does not require a Trigger or Publish |
| Publish All | Saves all authored changes (pipelines, datasets, linked services) to the live Data Factory |
