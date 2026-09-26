# Day 1 — Azure Data Factory: Introduction & Key Terminologies

## Overview

Azure Data Factory (ADF) is Microsoft's cloud-native data integration service. Before writing a single pipeline, you need to understand what ADF is, what every term means, and how all the pieces connect. Day 1 is a complete introduction — every important ADF concept explained with analogies, plus hands-on steps to create and test each one in the Azure portal.

**The 3 concepts:**
1. What is ADF — why it exists, core architecture, and the Studio UI
2. ADF Terminologies — every building block explained: Linked Services, Datasets, Activities, Pipelines, Triggers, Integration Runtime, Parameters, Expressions, Data Flows, Git Integration, Monitoring
3. Hands-on Walkthrough — create a Linked Service, Dataset, Pipeline, Schedule Trigger, enable Git, and monitor a run using the VoltGrid EV API as the real data source

---

## Concept 1: What is Azure Data Factory?

### The problem ADF solves

Imagine your company has data sitting in 10 different places: a REST API, an on-premises SQL Server, Salesforce, an FTP server, and 6 more SaaS tools. Every night, someone needs to pull data from all 10 sources, clean it, and load it into the data lake so analysts can query it in the morning.

Without ADF:
- A data engineer writes Python scripts for each source
- Sets up cron jobs or Airflow to schedule them
- Writes retry logic, monitoring, and alerting from scratch
- Rebuilds all of this each time a new source is added

**Azure Data Factory** handles all of this out of the box:

| | Python Script | Azure Data Factory |
|---|---|---|
| Data source connectivity | Write SDK code per source | 90+ built-in connectors |
| Scheduling | External (cron / Airflow) | Built-in Schedule / Event / Tumbling Window triggers |
| Monitoring | Build it yourself | Built-in run history, alerts, Log Analytics |
| Retry logic | Write it yourself | Configurable per activity |
| Scaling | Manual | Auto-scales with DIUs |
| Cost model | Pay for compute 24/7 | Serverless — pay only per pipeline run |
| Non-engineer friendly | No | Yes — visual drag-and-drop designer |

### ETL vs. ELT — where ADF fits

**ETL (Extract → Transform → Load):** Transform data in a middle layer before loading. Old approach — used when the destination was a slow relational data warehouse.

**ELT (Extract → Load → Transform):** Load raw data first, then transform it inside the powerful destination (Spark, Synapse SQL). Modern approach for cloud data lakes.

```
ETL:  Source → [Transform in middle] → Destination (clean data)
ELT:  Source → [Load raw] → Destination → [Transform inside destination]
```

**ADF in this course:** We use ADF for **ELT**. ADF's Copy Activity loads raw data from the VoltGrid API into the Bronze layer of ADLS Gen2. Databricks or Synapse Spark then transforms Bronze → Silver → Gold. ADF orchestrates the whole chain.

### The ADF Studio UI — 4 panels

ADF Studio is the browser-based designer at `https://adf.azure.com`.

```
ADF Studio
├── ✏️  Author   — Build pipelines, datasets, linked services, data flows
├── 📺  Monitor  — View all pipeline runs, activity runs, trigger runs
├── 🔧  Manage   — Linked services, integration runtimes, triggers, Git config
└── 🎓  Learn    — Template gallery, tutorials, quick-start guides
```

| Panel | What you do here |
|---|---|
| **Author** | Create pipelines. Drag activities onto the canvas. Write expressions. Set parameters. |
| **Monitor** | See every run. Filter by status. Drill into individual activity runs and view logs. |
| **Manage** | Create connections (Linked Services). Configure Integration Runtime. Manage triggers. |
| **Learn** | Browse pre-built pipeline templates — HTTP to ADLS, SQL to Parquet, Salesforce to lake. |

---

## Concept 2: ADF Terminologies — Every Building Block

### 2.1 Linked Service

**What it is:** A saved connection definition — stores the URL, authentication method, and credentials (or a Key Vault reference) for a data source or destination.

**Analogy:** A saved contact in your phone. You do not re-enter the bank's number every time you call — you tap "Bank". Similarly, ADF does not re-enter connection details every pipeline — it references the saved Linked Service.

**Key rule:** One Linked Service per source system. All pipelines that connect to the same REST API share one Linked Service.

```
Linked Services (configured once in Manage panel):
├── ls_voltgrid_api     ← REST API: https://your-app.vercel.app
├── ls_adls_gen2        ← ADLS Gen2: stadlsdev001
├── ls_sql_server       ← Azure SQL Database: payments-db.database.windows.net
└── ls_keyvault         ← Azure Key Vault: kv-ev-intelligence-dev
```

**Supported authentication methods:**
| Method | When to use |
|---|---|
| Anonymous | Public HTTP endpoints |
| Account key | ADLS Gen2 or Blob Storage (dev only) |
| Service Principal | Production — app identity, no user involved |
| Managed Identity | Best practice — no secrets at all, Azure handles auth |
| Username/Password | Legacy sources (FTP, SFTP, basic HTTP) |

---

### 2.2 Dataset

**What it is:** A description of the data — its location (which Linked Service, which path) and its shape (file format, schema, delimiter). A Dataset does NOT contain data — it is a pointer.

**Analogy:** A shipping label on a parcel. The label says where to pick it up (source), what format the contents are in, and where to deliver it. The delivery driver (Copy Activity) reads the label and moves the parcel.

```
Dataset = Linked Service + Location + Format

ds_voltgrid_payments_json:
  Linked Service: ls_voltgrid_api
  Relative URL:   /api/db/payments/
  Format:         JSON
  Auth header:    Token {token}    ← passed at runtime via pipeline parameter

ds_adls_bronze_parquet:
  Linked Service: ls_adls_gen2
  Container:      bronze
  Directory:      payments/@{dataset().run_date}
  Format:         Parquet (Snappy compression)
```

**Common Dataset formats:**
| Format | Use case |
|---|---|
| DelimitedText (CSV) | Simple flat files |
| JSON | REST API responses |
| Parquet | Bronze/Silver/Gold lake storage |
| Delta | Delta Lake tables |
| Binary | Files you don't want ADF to inspect (just move as-is) |

---

### 2.3 Activity

**What it is:** One unit of work inside a pipeline. ADF has three categories of activities:

#### Data Movement Activities
| Activity | What it does |
|---|---|
| **Copy Activity** | Reads from a source Dataset, writes to a sink Dataset. The workhorse of ADF. |

#### Data Transformation Activities
| Activity | What it does |
|---|---|
| **Data Flow** | No-code Spark transformations — filter, join, aggregate, pivot, lookup, split |
| **Databricks Notebook** | Run a Databricks notebook (Python, Scala, SQL) |
| **HDInsight Hive/Spark** | Run Spark or Hive jobs on HDInsight |
| **Azure Function** | Trigger a serverless function |

#### Control Flow Activities
| Activity | What it does |
|---|---|
| **ForEach** | Loop over an array — run inner activities for each item |
| **If Condition** | Branch: if true → do A, else → do B |
| **Switch** | Multi-branch: route based on a value (like a switch statement) |
| **Lookup** | Query a database/file and return the result for use downstream |
| **Wait** | Pause the pipeline for N seconds |
| **Execute Pipeline** | Call another pipeline (modular design) |
| **Until** | Loop until a condition is true (e.g., wait for a file to arrive) |
| **Web Activity** | Call any HTTP endpoint — great for Slack/Teams alerts on failure |
| **Fail** | Explicitly fail the pipeline with a custom message |
| **Get Metadata** | Get file properties: size, existence, column count, last modified |

---

### 2.4 Pipeline

**What it is:** A logical grouping of activities with defined execution order and dependencies. A pipeline is the workflow.

**Analogy:** A recipe. Individual activities are the steps ("chop onions", "boil water"). The pipeline is the full recipe — it defines the order and what happens if a step fails.

```
Pipeline: pl_ingest_voltgrid_to_bronze
├── Get Token (Web Activity)     → calls POST /api/auth/login/ → returns token
├── Copy Payments (Copy Activity) → On Success of Get Token
│     Source: ls_voltgrid_api → /api/db/payments/?page=1
│     Sink:   ls_adls_gen2   → bronze/payments/2026-01-15/
└── Alert on Failure (Web Activity) → On Failure of Copy Payments → POST to Teams webhook
```

**Activity dependency types:**
| Dependency | When the next activity runs |
|---|---|
| **On Success** | Only if the previous activity succeeded (green) |
| **On Failure** | Only if the previous activity failed (red) |
| **On Completion** | Always — regardless of success or failure |
| **On Skipped** | Only if the previous activity was skipped |

---

### 2.5 Trigger

**What it is:** Defines when a pipeline runs. Without a trigger, a pipeline only runs when you manually click "Debug" or "Trigger Now".

**Analogy:** An alarm clock. The pipeline is what to do when you wake up. The trigger is the alarm that wakes you.

#### Schedule Trigger
Runs at fixed times — like a cron job.
```
Trigger: trg_daily_ingestion
Type:    Schedule
Start:   2026-01-15 02:00 AM UTC
Recurrence: Every 1 Day
```
**Limitation:** Does not track which runs completed. If Monday's run fails, it will not automatically rerun Monday's data — it just fires again on Tuesday.

#### Tumbling Window Trigger
Like a schedule trigger but **backfill-aware** — it tracks which time windows have been processed and reruns any that failed.
```
Trigger: trg_tumbling_payments
Type:    Tumbling Window
Frequency: Every 1 Day
Start: 2026-01-01 (past date — triggers backfill for every day since then)
```
Use for: pipelines that process one day/hour of data and must have complete, gap-free history.

#### Storage Event Trigger
Fires when a blob is created or deleted in Azure Storage.
```
Trigger: trg_on_file_arrival
Type:    Storage Event
Storage account: stadlsdev001
Container:       landing-zone
Blob path begins with: incoming/payments/
Event:   Blob created
```
Use for: file-landing patterns — pipeline runs the moment a file drops into a folder.

#### Manual Trigger
Run the pipeline on demand via "Trigger Now" in the Studio. Useful for testing and ad-hoc runs.

---

### 2.6 Integration Runtime (IR)

**What it is:** The compute infrastructure that executes ADF activities. It is the engine — ADF Studio is the cockpit.

```
Integration Runtime Types:
├── Azure IR (AutoResolveIntegrationRuntime)
│     ← Default. Managed by Microsoft. No setup needed.
│     ← Used for: cloud-to-cloud (REST API → ADLS Gen2, Azure SQL → ADLS Gen2)
│
├── Self-Hosted IR (SHIR)
│     ← Software installed on YOUR machine or VM.
│     ← Used for: on-premises SQL Server, Oracle, SAP inside a private network
│
└── Azure-SSIS IR
      ← Runs legacy SQL Server Integration Services (SSIS) packages in Azure
      ← Used for: migrating old SSIS ETL jobs to cloud without rewriting them
```

| | Azure IR | Self-Hosted IR |
|---|---|---|
| Setup | None — fully managed | Install agent on a machine in your network |
| Source location | Public internet or Azure PaaS | On-premises or private VNet |
| Cost | Included in Copy Activity cost | Cost of the VM running the agent |
| Use case | REST API, ADLS Gen2, Azure SQL | On-prem SQL Server, Oracle, SAP |

---

### 2.7 Parameters and Expressions

**Parameters** make pipelines reusable. Instead of hardcoding `2026-01-15` in the sink path, you pass it as a parameter at runtime.

**Pipeline parameter example:**
```
Parameter: run_date (String, default: "2026-01-15")
Used in sink path: bronze/payments/@{pipeline().parameters.run_date}/
```

**Expressions** are ADF's built-in formula language — wrapped in `@{}` or `@` when the whole value is an expression.

```
# Reference a pipeline parameter
@pipeline().parameters.run_date

# Build a dynamic folder path
@{concat('bronze/payments/', pipeline().parameters.run_date, '/')}

# Today's date as a string
@{formatDateTime(utcnow(), 'yyyy-MM-dd')}

# Reference a trigger's scheduled time
@{formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')}

# Reference output from a previous activity (e.g. Web Activity that fetched a token)
@activity('GetToken').output.token

# Conditional — use prod account in prod, dev account in dev
@if(equals(pipeline().parameters.env, 'prod'), 'stadlsprod001', 'stadlsdev001')
```

**Dataset parameters** — similar to pipeline parameters but scoped to the dataset:
```
Dataset: ds_adls_bronze_parquet
Parameter: run_date (String)
Directory field: payments/@{dataset().run_date}
```

---

### 2.8 Data Flow

**What it is:** A no-code Spark transformation designer built into ADF. Used for complex transformations that need more than column renaming — filtering, joining, aggregating, splitting, pivoting.

**Copy Activity vs Data Flow:**
| | Copy Activity | Data Flow |
|---|---|---|
| Purpose | Move data as-is (minor mapping only) | Transform, enrich, reshape data |
| Transformations | Column rename + type cast only | Filter, join, aggregate, pivot, lookup, split |
| Underlying engine | ADF managed movement | Apache Spark (auto-provisioned) |
| Performance | Very fast (optimised bulk copy) | Spark overhead — slower for small datasets |
| Cost | Charged per DIU-hour (cheap) | Charged per vCore-hour (more expensive) |
| Code required | None | None (visual) or expression language |

**Rule of thumb:**
- **Bronze ingestion:** Copy Activity — just move the raw data, no changes
- **Silver transformation:** Data Flow or Databricks Notebook — clean, join, aggregate

---

### 2.9 Git Integration

**What it is:** ADF can connect to a Git repository (Azure DevOps or GitHub) for source control of your pipeline definitions.

**Why it matters:**
- All pipelines, datasets, linked services are stored as JSON files in Git
- Changes go through pull requests — peer review before they hit production
- Full history: who changed what and when
- Branch-based development: build in a feature branch, merge to main, publish to prod

```
Without Git:                    With Git:
─────────────────               ─────────────────────────────────────────────
Edit in Studio → Publish        Edit in Studio → Commit to branch → PR review
No history                      Full git history
No review                       Code review before production
Shared dev/prod ADF             Separate dev ADF (feature branch) + prod ADF (main branch)
```

**How Git integration works in ADF:**
- Each resource (pipeline, dataset, linked service) is stored as a `.json` file in the repo
- **Publish** in ADF Studio = push to the `adf_publish` branch (ARM template for the live factory)
- You work in a collaboration branch (e.g., `main` or `dev`)
- Developers create feature branches → PR → merge → Publish from collaboration branch

---

### 2.10 Monitoring

**What it is:** The Monitor panel shows the complete run history for your Data Factory.

**Levels of monitoring:**
```
Pipeline Runs   ← Did the whole pipeline succeed or fail? How long did it take?
    └── Activity Runs  ← Which specific activity failed? What was the error?
            └── Input/Output JSON  ← What exactly did the activity read/write?
```

**Monitor panel sections:**
| Section | What you see |
|---|---|
| Pipeline runs | All pipeline executions — status, duration, trigger name |
| Activity runs | Drill into a pipeline run — see each activity's status, duration, rows copied |
| Trigger runs | All trigger executions — when each trigger fired and whether it succeeded |
| Debug runs | Runs from clicking "Debug" — separate from production trigger runs |

**Alerting:**
- ADF integrates with Azure Monitor → set up alert rules for "Failed pipeline runs > 0"
- Alert action: email, SMS, Teams webhook, PagerDuty
- Also configure directly in ADF: pipeline → **On Failure** → Web Activity → POST to webhook

**Key metrics visible per Copy Activity run:**
```
Data read:       12.5 MB
Data written:    4.2 MB (Parquet compressed)
Rows copied:     8,421
Duration:        00:00:14
DIUs used:       2
Throughput:      0.89 MB/s
```

---

### 2.11 DIUs (Data Integration Units)

**What it is:** The compute unit for Copy Activity. Each DIU is a bundle of CPU, memory, and network capacity. More DIUs = more parallel threads = faster copy.

| Setting | Behaviour | When to use |
|---|---|---|
| Auto (default) | ADF picks 2–32 DIUs based on data size | Most cases |
| Manual (e.g. 32) | You fix the DIU count | When you need predictable cost or performance |

For small REST API responses (< 100 MB): Auto picks 2 DIUs — cheap and fast.
For large table exports (100+ GB): Auto may scale to 32 DIUs — faster but more expensive.

---

### 2.12 Fault Tolerance

**What it is:** What happens when the Copy Activity encounters a bad row.

| Setting | Behaviour |
|---|---|
| Fail on first error (default) | Pipeline fails if any incompatible row is encountered |
| Skip incompatible rows | Bad rows are logged and skipped — copy continues |
| Enable logging | Skipped rows written to an ADLS Gen2 path for review |

Always enable logging when skipping rows — silent discard hides data quality problems.

---

## Concept 3: Hands-on Walkthrough

### What we will build

We will build a real ADF pipeline that:
1. Calls the **VoltGrid EV API** to get a Bearer token
2. Uses that token to fetch **payments data** from `/api/db/payments/`
3. Writes the raw JSON response to **ADLS Gen2 bronze** layer
4. Runs on a **daily schedule** at 2am UTC
5. Monitored via the **Monitor panel**
6. Source-controlled with **Git integration**

```
VoltGrid API                    ADF Pipeline                     ADLS Gen2
─────────────                   ──────────────────────────────   ─────────────────────────
POST /api/auth/login/  →  Web Activity (GetToken)
GET  /api/db/payments/ →  Copy Activity (CopyPayments)    →  bronze/payments/2026-01-15/
                           On Failure →
                          Web Activity (AlertOnFailure)
```

**Pre-requisite:** ADLS Gen2 storage account `stadlsdev001` with a `bronze` container (created in ADLS Day 1).

---

### Step-by-step: Create ADF Instance

**Step 1 — Search for Data Factory in the portal**
- Go to `https://portal.azure.com`
- In the top search bar, type **"Data factories"**
- Click **"Data factories"** under Services

**Step 2 — Create**
- Click **"+ Create"**

**Step 3 — Fill in Basics tab**

| Field | Value |
|---|---|
| Subscription | Free Trial |
| Resource group | `rg-datalake-dev` |
| Region | Australia East |
| Name | `adf-datalake-dev` (must be globally unique — add numbers if taken) |
| Version | **V2** |

**Step 4 — Git configuration tab**
- For now: check **"Configure Git later"** (we will set it up in a later step)
- Click **"Review + create"** → **"Create"**
- Deployment: ~60 seconds

**Step 5 — Launch Studio**
- Click **"Go to resource"** → click **"Launch Studio"**
- ADF Studio opens in a new tab

> **Checkpoint:** You see the ADF Studio with 4 icons in the left sidebar (pencil, TV, toolbox, graduation cap). The Author canvas is blank and ready.

---

### Step-by-step: Create Linked Services

#### A — ADLS Gen2 Linked Service

**Step 1** — Click **🔧 Manage** → **Linked services** → **+ New**

**Step 2** — Search **"Azure Data Lake Storage Gen2"** → Continue

**Step 3** — Configure:

| Field | Value |
|---|---|
| Name | `ls_adls_gen2` |
| Description | Connects to stadlsdev001 ADLS Gen2 data lake |
| Authentication method | Account key |
| Storage account name | `stadlsdev001` |

**Step 4** — Click **"Test connection"** → confirm "Connection successful" → **"Create"**

---

#### B — VoltGrid API Linked Service (HTTP)

**Step 1** — **+ New** → search **"HTTP"** → Continue

**Step 2** — Configure:

| Field | Value |
|---|---|
| Name | `ls_voltgrid_api` |
| Description | VoltGrid EV platform REST API |
| Base URL | `https://your-app.vercel.app` |
| Authentication type | Anonymous |

> We use Anonymous at the Linked Service level because authentication is handled inside the pipeline — we call `POST /api/auth/login/` first to get a token, then pass it as a header in the Copy Activity. This is the correct pattern for token-based REST APIs.

**Step 3** — **"Test connection"** → **"Create"**

---

### Step-by-step: Create Datasets

#### A — VoltGrid Payments Source Dataset

**Step 1** — Click **✏️ Author** → **"+"** next to Datasets → **New dataset**

**Step 2** — Search **"HTTP"** → select **HTTP** → Continue → select **JSON** → Continue

**Step 3** — Configure:

| Field | Value |
|---|---|
| Name | `ds_voltgrid_payments` |
| Linked service | `ls_voltgrid_api` |
| Relative URL | `/api/db/payments/` |
| Request method | GET |
| Import schema | None (schema varies per page, defined in mapping) |

**Step 4** — Go to **Parameters** tab → **+ New**:
- Name: `auth_token`, Type: `String`

**Step 5** — Go back to **Connection** tab → **Additional headers** → **+ New**:
- Header name: `Authorization`
- Header value: `@{concat('Token ', dataset().auth_token)}`

This injects the Bearer token from the pipeline into every request this dataset makes.

**Step 6** — Click **"OK"**

---

#### B — ADLS Gen2 Bronze Sink Dataset

**Step 1** — **"+"** next to Datasets → **New dataset**

**Step 2** — Search **"Azure Data Lake Storage Gen2"** → Continue → select **JSON** → Continue

**Step 3** — Configure:

| Field | Value |
|---|---|
| Name | `ds_adls_bronze_json` |
| Linked service | `ls_adls_gen2` |
| File path — Container | `bronze` |
| File path — Directory | `payments/@{dataset().run_date}` |
| File path — File | `payments_raw.json` |

**Step 4** — Go to **Parameters** tab → **+ New**:
- Name: `run_date`, Type: `String`

**Step 5** — Click **"OK"** → **"Publish All"**

---

### Step-by-step: Build the Pipeline

**Step 1 — Create pipeline**
- **✏️ Author** → **"+"** next to Pipelines → **New pipeline**
- Name: `pl_ingest_voltgrid_payments_to_bronze`
- Description: Fetches VoltGrid payment data from REST API and writes to ADLS Gen2 bronze layer

**Step 2 — Add pipeline parameters**
- Click the canvas background → **Parameters** tab at the bottom → **+ New**:

| Name | Type | Default |
|---|---|---|
| `run_date` | String | `2026-01-15` |
| `api_username` | String | `voltgrid_demo` |
| `api_password` | String | `EVcharge@AU2025` |

---

**Step 3 — Add Web Activity to get token**
- From Activities pane, expand **"General"** → drag **"Web"** onto canvas
- Name: `GetAuthToken`
- **Settings** tab:

| Field | Value |
|---|---|
| URL | `https://your-app.vercel.app/api/auth/login/` |
| Method | POST |
| Headers | `Content-Type: application/json` |
| Body | `@{concat('{"username":"', pipeline().parameters.api_username, '","password":"', pipeline().parameters.api_password, '"}')}` |

This calls the VoltGrid login endpoint and the output JSON contains `{"token": "abc123..."}`. The token is referenced downstream as `@activity('GetAuthToken').output.token`.

---

**Step 4 — Add Copy Activity → connect On Success from Web Activity**
- Drag **"Copy data"** onto canvas
- Drag the **green arrow** from `GetAuthToken` to the Copy Activity → dependency is **On Success**
- Name: `CopyPaymentsToBronze`

**Source tab:**

| Field | Value |
|---|---|
| Source dataset | `ds_voltgrid_payments` |
| Dataset property — auth_token | `@activity('GetAuthToken').output.token` |
| Request method | GET |
| Additional headers | leave blank (already in dataset) |

**Sink tab:**

| Field | Value |
|---|---|
| Sink dataset | `ds_adls_bronze_json` |
| Dataset property — run_date | `@pipeline().parameters.run_date` |
| File extension | `.json` |

**Settings tab:**

| Field | Value |
|---|---|
| Data integration units | Auto |
| Fault tolerance | Skip incompatible rows |
| Enable logging | Checked → `ls_adls_gen2` → `logs/copy-errors/` |

---

**Step 5 — Add Web Activity for failure alert → connect On Failure**
- Drag another **"Web"** onto canvas
- Drag the **red arrow** from `CopyPaymentsToBronze` to this activity → dependency **On Failure**
- Name: `AlertOnFailure`
- **Settings** tab:

| Field | Value |
|---|---|
| URL | Your Teams/Slack webhook URL (or `https://httpbin.org/post` for testing) |
| Method | POST |
| Body | `@{concat('{"text":"ADF Pipeline FAILED: ', pipeline().Pipeline, ' at ', utcnow(), '"}')}`  |

---

**Step 6 — Debug run**
- Click **"Debug"** at the top of the canvas
- In the Parameters dialog, confirm `run_date = 2026-01-15`
- Click **"OK"**
- Watch each activity turn yellow (running) → green (succeeded)
- Click the glasses icon on `CopyPaymentsToBronze` to see rows copied, data written, DIUs used

> **Checkpoint:** `GetAuthToken` succeeds with a token in the output. `CopyPaymentsToBronze` succeeds. Navigate to `stadlsdev001` → `bronze/payments/2026-01-15/` — you should see `payments_raw.json`.

**Step 7 — Publish All**
- Click **"Publish All"** → **"Publish"**
- All resources (linked services, datasets, pipeline) are now saved to the live factory

---

### Step-by-step: Create a Schedule Trigger

**Step 1** — With `pl_ingest_voltgrid_payments_to_bronze` open, click **"Add trigger"** → **"New/Edit"**

**Step 2** — Click **"+ New"**

**Step 3** — Configure:

| Field | Value |
|---|---|
| Name | `trg_daily_payments` |
| Type | Schedule |
| Start date | Today |
| Time | 02:00 AM UTC |
| Recurrence | Every 1 Day |
| End | No end |

**Step 4** — Set trigger parameters:

| Parameter | Value |
|---|---|
| `run_date` | `@{formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')}` |
| `api_username` | `voltgrid_demo` |
| `api_password` | `EVcharge@AU2025` |

**Step 5** — Click **"OK"** → **"Publish All"**

> **Checkpoint:** Go to **🔧 Manage → Triggers** — confirm `trg_daily_payments` shows as **Started** (green).

---

### Step-by-step: Enable Git Integration

**Step 1** — Go to **🔧 Manage → Git configuration** → **"Configure"**

**Step 2** — Select repository type: **GitHub**

**Step 3** — Configure:

| Field | Value |
|---|---|
| GitHub account | your GitHub username |
| Repository name | `azure-ev-end-to-end-project` |
| Collaboration branch | `main` |
| Publish branch | `adf_publish` |
| Root folder | `/azure-data-factory-service/adf-resources` |

**Step 4** — Click **"Apply"**

After Git is connected:
- Every pipeline/dataset/linked service has a **"Save"** button that commits to the collaboration branch
- **"Publish All"** deploys from the collaboration branch to the live factory
- You can see pipeline JSON files in your GitHub repo under the root folder

---

### Step-by-step: Monitor a Pipeline Run

**Step 1** — Click the **📺 Monitor** icon

**Step 2** — Go to **"Pipeline runs"**
- Find `pl_ingest_voltgrid_payments_to_bronze` in the list
- Status: Succeeded / Failed / In Progress
- Duration and trigger name shown

**Step 3** — Click on the pipeline run row to drill into **Activity runs**
- You see each activity: `GetAuthToken`, `CopyPaymentsToBronze`, etc.
- Status, duration, and start time per activity

**Step 4** — Click the glasses icon (👓) on `CopyPaymentsToBronze`
- **Input:** source URL, dataset name, auth header presence
- **Output:** rows copied, data read (MB), data written (MB), DIUs used, throughput (MB/s)
- **Error** (if failed): full error message with error code

**Step 5 — Set up an email alert**
- Go to the Azure portal → `adf-datalake-dev` → **Monitoring → Alerts → + New alert rule**
- Condition: Signal = **"Failed pipeline runs"** metric > 0
- Action group: add your email
- This sends an email whenever any pipeline fails — no code needed

---

## Summary

| Term | One-line definition |
|---|---|
| **Linked Service** | Saved connection credentials — one per source system |
| **Dataset** | Data shape + location — pointer, not the data itself |
| **Activity** | One unit of work — Copy, Data Flow, Web, ForEach, Lookup, etc. |
| **Pipeline** | Ordered workflow of activities with dependencies |
| **Trigger** | Alarm clock for a pipeline — Schedule, Tumbling Window, or Event |
| **Integration Runtime** | Compute engine — Azure IR (cloud) or Self-Hosted IR (on-premises) |
| **Parameter** | Variable passed to a pipeline at runtime — makes pipelines reusable |
| **Expression** | `@{}` dynamic formula — `@{concat(...)}`, `@{formatDateTime(...)}` |
| **Data Flow** | No-code Spark transformation inside ADF |
| **Git Integration** | Source control for pipeline definitions — version history + PR review |
| **Monitor** | Run history panel — pipeline runs → activity runs → input/output |
| **DIU** | Data Integration Unit — compute for Copy Activity (more = faster) |
| **Fault Tolerance** | Skip bad rows + log them instead of failing the whole pipeline |
| **Publish All** | Deploy all saved changes from the Studio to the live factory |
| **Debug** | Manual one-off run in test mode — does not require a trigger |
