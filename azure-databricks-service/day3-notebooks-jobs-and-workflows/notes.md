# Day 3 — Azure Databricks: Workspace, Notebooks, Jobs & Workflows

> **Goal:** Understand every section of the Databricks workspace, create and run notebooks with simple Spark code, and build a Databricks Job (Workflow) that runs a notebook on a schedule.
> **Context:** In the VoltGrid project, data engineers write transformation notebooks and then wrap them in Databricks Workflows so they run automatically each night — no manual triggering needed.

---

## Part 1: Workspace — Every Section Explained

The Databricks workspace is the web UI you land on after clicking **Launch Workspace** from the Azure Portal. The left sidebar is your main navigation. Each section has a specific purpose.

```
Left Sidebar (top to bottom)
├── Home
├── Workspace
├── Repos           (removed from notes — covered separately)
├── Data (Catalog)
├── Compute
├── Workflows
├── Delta Live Tables
├── Marketplace
└── Settings
```

### 1.1 Home

Your personal landing page inside the workspace.

```
What you see:
  - Recently opened notebooks
  - Pinned notebooks or dashboards
  - Quick links: Create notebook, Create cluster, Import data
  - Notifications and workspace announcements
```

- Home is personal — each user sees their own recently used items
- Use it as a shortcut back to notebooks you open frequently
- The **+** button in the top right opens a quick-create menu (notebook, cluster, job)

### 1.2 Workspace (Folder Tree)

The Workspace section is a shared folder tree — like a file system — where all notebooks and files live.

```
Workspace/
├── Shared/                 ← visible to all users in the workspace
│     ├── voltgrid/
│     │   ├── bronze/
│     │   │   └── ingest_payments   (notebook)
│     │   ├── silver/
│     │   │   └── transform_payments (notebook)
│     │   └── gold/
│     │       └── agg_revenue        (notebook)
│
└── Users/
      └── hariom@simformsolutions.com/   ← your personal folder
            └── scratch/
                  └── test_notebook
```

**Workspace operations:**

| Action | How |
|---|---|
| Create notebook | Right-click folder → Create → Notebook |
| Create folder | Right-click folder → Create → Folder |
| Import notebook | Right-click folder → Import → Upload .ipynb or .py |
| Export notebook | Right-click notebook → Export → Source / HTML / IPython |
| Move | Drag and drop OR right-click → Move |
| Clone | Right-click → Clone — creates a copy in a new location |
| Delete | Right-click → Move to Trash (recoverable for 30 days) |
| Permissions | Right-click → Permissions → grant Can View / Can Run / Can Edit / Can Manage |

**Notebook permissions:**

```
Can View     → read the notebook, cannot run it
Can Run      → run the notebook, cannot edit
Can Edit     → edit and run
Can Manage   → edit, run, delete, change permissions (owner-level)
```

### 1.3 Data (Catalog)

The Data section is the Unity Catalog browser — it shows all databases, schemas, tables, and volumes your workspace can see.

```
Data (Catalog)
├── hive_metastore          ← legacy metastore (pre-Unity Catalog)
│     └── default           ← default database
│
└── voltgrid_catalog        ← Unity Catalog catalog
      ├── bronze_schema
      │     └── payments_raw    (table)
      ├── silver_schema
      │     └── payments_clean  (table)
      └── gold_schema
            └── revenue_summary (table)
```

- Click any table → **Schema** tab shows column names and types
- **Sample Data** tab shows the first 1000 rows (read from Delta)
- **History** tab shows Delta transaction log (who wrote when)
- **Details** tab shows table location (ADLS path), format, partitioning

### 1.4 Compute

You covered this in depth in Day 2. Quick recap:

```
Compute
├── All-Purpose Clusters  ← clusters for interactive notebooks
├── Job Clusters          ← clusters created by jobs (visible only in job runs)
├── SQL Warehouses        ← for Databricks SQL / BI workloads
└── Pools                 ← pre-warmed VM pools
```

Each cluster row shows: name, state (Running / Terminated), runtime version, size, creator.

### 1.5 Workflows (Jobs)

This is where you define and run **Databricks Jobs** — automated tasks that run notebooks or scripts on a schedule. Covered in detail in Part 4.

```
Workflows
├── Jobs             ← list of all defined jobs
├── Job runs         ← history of all past job runs (success / failure / running)
└── Delta Live Tables pipelines ← streaming/batch DLT pipelines
```

### 1.6 Delta Live Tables (DLT)

DLT is Databricks' declarative pipeline framework — you write `@dlt.table` decorated Python functions and Databricks handles dependencies, retries, and data quality checks.

- Covered in a later day (Day 6+)
- For now: know it exists under the Workflows section and is separate from regular jobs

### 1.7 Marketplace

The Databricks Marketplace is a data + solutions hub where you can:
- Browse publicly available datasets (e.g. US Census, financial data)
- Install partner solutions (dbt, Fivetran connectors)
- Publish your own datasets to share with other organisations

For VoltGrid dev work: not commonly used — just know it exists.

### 1.8 Settings

```
Settings (gear icon, bottom-left)
├── Workspace settings    ← admin: cluster creation, DBFS browser, Unity Catalog
├── Admin Console         ← user/group management, access control
├── Compute (policies)    ← cluster policies (covered Day 2)
├── Linked accounts       ← Git provider connection (removed from scope)
├── Developer             ← personal access tokens (PAT)
│     └── Access tokens   → Generate a PAT for CLI or REST API access
└── Notifications         ← email alerts for job failures
```

**Generating a Personal Access Token (PAT):**

1. Settings → **Developer** → **Access tokens** → **Generate new token**
2. Name: `voltgrid-cli-token`
3. Lifetime: 90 days
4. Copy and store the token — it is shown only once
5. Used with: Databricks CLI, REST API, ADF Databricks Linked Service

---

## Part 2: Notebooks — What They Are

A Databricks notebook is an interactive document made of **cells**. Each cell contains code (Python, SQL, Scala, or R) or markdown text. You run cells one by one or all at once, and the output appears directly below the cell.

```
Notebook structure:
  ┌─────────────────────────────────────────┐
  │  Cell 1 — Python (import libraries)     │
  │  > spark.version                        │
  │  Output: '3.5.0'                        │
  ├─────────────────────────────────────────┤
  │  Cell 2 — SQL (query a table)           │
  │  > %sql SELECT count(*) FROM payments   │
  │  Output: 15234                          │
  ├─────────────────────────────────────────┤
  │  Cell 3 — Markdown (section header)     │
  │  > %md ## Results                       │
  │  Output: rendered "Results" heading     │
  └─────────────────────────────────────────┘
```

### 2.1 Notebook Languages

When you create a notebook you pick a **default language**. Every cell runs in that language unless you override it with a magic command.

| Language | Use case |
|---|---|
| Python | Data engineering, ML, Spark DataFrame API |
| SQL | Querying Delta tables, ad-hoc exploration |
| Scala | When Java/JVM performance is needed |
| R | Statistical analysis |

**VoltGrid project default:** Python — all Bronze → Silver → Gold notebooks use PySpark.

### 2.2 Magic Commands

Magic commands start with `%` and override the default language for one cell:

| Command | What it does |
|---|---|
| `%python` | Run this cell in Python |
| `%sql` | Run this cell in SQL |
| `%scala` | Run this cell in Scala |
| `%r` | Run this cell in R |
| `%md` | Render this cell as Markdown (documentation) |
| `%sh` | Run this cell as a shell (bash) command |
| `%fs` | Shortcut for `dbutils.fs` commands (DBFS file operations) |
| `%run` | Run another notebook from this cell |
| `%pip` | Install a Python library in this cluster session |

Examples:

```python
# %sql cell — runs SQL even though notebook default is Python
%sql
SELECT payment_status, count(*) as total
FROM voltgrid_catalog.silver_schema.payments_clean
GROUP BY payment_status
```

```python
# %md cell — renders as formatted text
%md
## VoltGrid Silver Transform
This notebook reads Bronze payments JSON and writes Silver Delta.
```

```python
# %sh cell — shell command on the driver node
%sh
ls /tmp
```

```python
# %fs cell — list DBFS or ADLS path
%fs ls abfss://bronze@evdatalakedev.dfs.core.windows.net/
```

### 2.3 Notebook Toolbar

```
Notebook toolbar (top of the page):
  [Notebook name]  [Language selector]  [Cluster: voltgrid-dev-shared ▼]
  
  Run All | Clear Output | Schedule | Edit | View | ...

  Run All         → runs every cell from top to bottom in order
  Clear Output    → removes all cell outputs (notebook size shrinks)
  Schedule        → creates a Job that runs this notebook (shortcut)
  Edit            → toggle edit/view mode
```

### 2.4 Cell Operations

```
Per-cell controls (hover over a cell):
  ▶ Run       → run this single cell (Shift + Enter)
  + (above)   → insert a new cell above this one
  + (below)   → insert a new cell below this one
  ↑ / ↓       → move cell up or down
  ⋮ (menu)    → Cut, Copy, Paste, Delete cell
  
Keyboard shortcuts:
  Shift + Enter   → run cell and move to next
  Ctrl + Enter    → run cell and stay
  Esc             → exit cell edit mode (command mode)
  A               → insert cell above (command mode)
  B               → insert cell below (command mode)
  DD              → delete current cell (command mode)
  M               → convert cell to Markdown (command mode)
```

### 2.5 dbutils — Databricks Utilities

`dbutils` is a built-in Python object available in all Databricks notebooks. It provides utilities for working with files, secrets, and notebook flow.

```python
# dbutils.fs — file system operations
dbutils.fs.ls("dbfs:/")                    # list DBFS root
dbutils.fs.ls("abfss://bronze@...")       # list ADLS container
dbutils.fs.mkdirs("dbfs:/tmp/voltgrid")   # create a directory
dbutils.fs.cp("source", "destination")    # copy a file
dbutils.fs.rm("dbfs:/tmp/old/", True)     # delete recursively

# dbutils.secrets — read secrets without printing
val = dbutils.secrets.get(scope="voltgrid-kv", key="sp-client-id")

# dbutils.notebook — notebook flow control
dbutils.notebook.run("./silver_transform", timeout_seconds=600,
                     arguments={"env": "dev"})
dbutils.notebook.exit("SUCCESS")          # exit with a return value

# dbutils.widgets — parameterise notebooks
dbutils.widgets.text("env", "dev", "Environment")
env = dbutils.widgets.get("env")
```

---

## Part 3: Creating a Notebook — Step by Step

### Step 1 — Navigate to Workspace

1. Left sidebar → **Workspace**
2. Expand **Shared** → navigate to your team folder (e.g. `voltgrid`)
3. If the folder does not exist: right-click **Shared** → **Create** → **Folder** → name it `voltgrid`

### Step 2 — Create the Notebook

1. Right-click the `voltgrid` folder → **Create** → **Notebook**
2. Fill in:

   | Field | Value |
   |---|---|
   | Name | `day3_payments_summary` |
   | Default language | `Python` |
   | Cluster | `voltgrid-dev-shared` (select from dropdown) |

3. Click **Create** — the notebook opens immediately

### Step 3 — Attach to Cluster

At the top of the notebook, you will see:
```
Detached ▼   or   voltgrid-dev-shared ▼
```

- If the notebook shows **Detached**, click it → select `voltgrid-dev-shared` from the list
- If the cluster is **Terminated**, click the cluster name → **Start** — wait ~3 minutes
- The cluster indicator turns green when connected

### Step 4 — Write the Notebook (Cell by Cell)

**Cell 1 — Markdown header (documentation cell)**
```python
%md
# Day 3 — Payments Summary Notebook
This notebook reads Bronze payments data from ADLS Gen2 and shows a simple summary.
- Source: `abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/`
- Output: printed summary to screen
```

Run: `Shift + Enter` — the cell renders as formatted text.

**Cell 2 — Check Spark is ready**
```python
print(f"Spark version: {spark.version}")
print(f"Cluster: {spark.sparkContext.appName}")
```

Expected output:
```
Spark version: 3.5.0
Cluster: voltgrid-dev-shared
```

**Cell 3 — Create a simple DataFrame (no external data needed)**
```python
# Create sample payments data inline — no ADLS connection needed for this demo
data = [
    (1, "completed", 250.00, "2024-01-15"),
    (2, "failed",    100.00, "2024-01-15"),
    (3, "completed", 430.50, "2024-01-16"),
    (4, "completed",  75.00, "2024-01-16"),
    (5, "pending",   200.00, "2024-01-17"),
    (6, "failed",    310.00, "2024-01-17"),
]

columns = ["payment_id", "status", "amount", "payment_date"]

df = spark.createDataFrame(data, columns)
df.show()
```

Expected output:
```
+----------+---------+------+------------+
|payment_id|   status|amount|payment_date|
+----------+---------+------+------------+
|         1|completed|250.0 |  2024-01-15|
|         2|   failed|100.0 |  2024-01-15|
|         3|completed|430.5 |  2024-01-16|
|         4|completed| 75.0 |  2024-01-16|
|         5|  pending|200.0 |  2024-01-17|
|         6|   failed|310.0 |  2024-01-17|
+----------+---------+------+------------+
```

**Cell 4 — Show schema**
```python
df.printSchema()
```

**Cell 5 — Summary by status using SQL magic**
```python
# Register as a temp view so we can query it with SQL
df.createOrReplaceTempView("payments_temp")
```

**Cell 6 — SQL query on the temp view**
```sql
%sql
SELECT
    status,
    COUNT(*)        AS total_payments,
    SUM(amount)     AS total_amount,
    AVG(amount)     AS avg_amount
FROM payments_temp
GROUP BY status
ORDER BY total_amount DESC
```

Expected output: a formatted table showing counts and totals by status.

**Cell 7 — Filter completed payments**
```python
from pyspark.sql.functions import col

completed = df.filter(col("status") == "completed")
print(f"Completed payment count: {completed.count()}")
print(f"Total completed amount: {completed.agg({'amount': 'sum'}).collect()[0][0]}")
```

**Cell 8 — Exit with a result (used when this notebook is called by a job)**
```python
dbutils.notebook.exit("Notebook completed successfully")
```

### Step 5 — Run All Cells

Click **Run All** in the toolbar → Databricks runs every cell from top to bottom. Watch the status indicator next to each cell:
- **Running** (spinner) → in progress
- **Success** (green check) → cell finished
- **Failed** (red X) → error, fix and re-run

### Step 6 — Save and Name the Notebook

Databricks auto-saves every few seconds. To rename: click the notebook name at the top → type new name → Enter.

---

## Part 4: Databricks Jobs (Workflows) — What They Are

A **Databricks Job** (called a Workflow in the UI) is a way to automate running one or more notebooks on a schedule or on-demand. Think of it as a cron job that runs your Spark code.

```
Job = what to run + when to run it + what compute to use

  What:    a notebook (or Python script / JAR / dbt project)
  When:    on a schedule (cron), on-demand (manual trigger), or triggered by an event
  Compute: a job cluster (created fresh for each run) or an existing cluster
```

### 4.1 Job vs Notebook (Manual Run)

| | Manual notebook run | Databricks Job |
|---|---|---|
| Trigger | You click Run All | Automatic (scheduled or API) |
| Compute | Your dev cluster (billing continues) | Job cluster (auto-terminates) |
| Monitoring | Watch the notebook cells | Job run history, email alerts |
| Retry on failure | Manual | Automatic (configurable retries) |
| Multi-step | One notebook at a time | Multiple notebooks in sequence or parallel |
| Production use | No — manual and attached to a user | Yes — runs as a service principal |

### 4.2 Job Run Lifecycle

```
Job triggered (schedule or manual)
      │
      ▼
Job Cluster created (PENDING ~3–5 min or ~30s from pool)
      │
      ▼
Notebook executes (RUNNING)
      │
      ├── Notebook succeeds → dbutils.notebook.exit("...") → SUCCEEDED
      │
      └── Notebook throws exception → FAILED → retry (if configured)
      │
      ▼
Job Cluster auto-terminates
```

---

## Part 5: Creating a Databricks Job — Step by Step

We will create a job that runs `day3_payments_summary` every night at midnight.

### Step 1 — Navigate to Workflows

Left sidebar → **Workflows** → **+ Create job**

### Step 2 — Name the Job

At the top, click the default name `Untitled` → type: `voltgrid_payments_summary_job`

### Step 3 — Configure the First Task

You will see a task panel on the right side or in the centre:

| Field | Value | Notes |
|---|---|---|
| Task name | `run_payments_summary` | Snake case, descriptive |
| Type | `Notebook` | Other options: Python script, JAR, dbt, Spark submit |
| Source | `Workspace` | Choose Workspace for notebooks in the folder tree |
| Path | Browse to: `Shared/voltgrid/day3_payments_summary` | Click the folder icon to browse |
| Cluster | `New job cluster` | Recommended — creates a fresh cluster per run |

**Configure the new job cluster:**

Click **Edit** next to the cluster dropdown:

| Field | Value |
|---|---|
| Cluster name | auto-generated (leave it) |
| Databricks runtime | `15.4 LTS` |
| Worker type | `Standard_D4s_v3` |
| Workers | `1` (fixed — small job, small data) |
| Auto-termination | automatic on job completion |
| Enable Photon | checked |

Click **Confirm** to save the cluster config.

### Step 4 — Set a Schedule

1. Click **Add trigger** (or the **Triggers** tab at the top of the job)
2. **Trigger type:** `Scheduled`
3. **Schedule:** choose `Every day at 00:00 (midnight)`
   - Or enter cron expression manually: `0 0 * * *`
4. **Timezone:** `UTC` (use UTC for production jobs)
5. Click **Save**

```
Cron syntax (5 fields):
  minute  hour  day-of-month  month  day-of-week
    0       0        *          *        *
  = "at 00:00 every day"
```

### Step 5 — Configure Notifications (Optional)

1. Click **Add notification** (in the job settings)
2. Events to notify on:
   - ☑ On start
   - ☑ On success
   - ☑ On failure
3. Email: your team email or personal email
4. Click **Save**

### Step 6 — Set Retries (Optional)

1. Click the task (`run_payments_summary`) → look for **Retries**
2. **Max retries:** `2`
3. **Retry interval:** `5 minutes`

This means: if the notebook fails, Databricks will try again 2 more times before marking the job as failed.

### Step 7 — Save and Run the Job

1. Click **Create** / **Save** (top right)
2. To test: click **Run now** → the job starts immediately regardless of the schedule
3. Watch the run under **Job runs** tab

### Step 8 — Monitor the Job Run

1. **Workflows** → click your job → **Runs** tab
2. You see a list of all runs — each row shows:
   - Run ID
   - Start time and duration
   - Status: Running / Succeeded / Failed
3. Click a specific run → drill into the task:
   - **Logs** tab → full notebook output (all cell outputs)
   - **Spark UI** tab → Spark job details, DAG, stages, tasks
   - **Ganglia** tab → cluster resource usage (CPU, memory)

```
Job run monitoring panel:
  ┌────────────────────────────────────────────────────────┐
  │  Job: voltgrid_payments_summary_job                    │
  │  Run #1 — Started 2024-01-17 00:00:02 UTC              │
  │  Duration: 4m 32s                                      │
  │  Status: SUCCEEDED                                     │
  │                                                        │
  │  Task: run_payments_summary                            │
  │  Cluster: job-cluster-1234 (auto-terminated)           │
  │  Output: "Notebook completed successfully"             │
  └────────────────────────────────────────────────────────┘
```

---

## Part 6: Multi-Task Jobs (Pipelines in Databricks)

A Databricks Job can contain **multiple tasks** that run in sequence or in parallel. This is what makes it a "pipeline" — each task is a step in the data flow.

### 6.1 Why Multi-Task Jobs

```
Single-task job:
  run_bronze_ingest → done

Multi-task job (pipeline):
  run_bronze_ingest
        │ on success
        ▼
  run_silver_transform
        │ on success
        ▼
  run_gold_aggregation
```

Each task runs in its own cluster (created and terminated automatically). If `run_silver_transform` fails, `run_gold_aggregation` does not run.

### 6.2 Creating a Multi-Task Job

We will create a 3-task VoltGrid pipeline job.

#### Step 1 — Create the job

**Workflows** → **+ Create job**

Name: `voltgrid_pipeline_daily`

#### Step 2 — Add Task 1 (Bronze Ingest)

| Field | Value |
|---|---|
| Task name | `bronze_ingest` |
| Type | `Notebook` |
| Path | `Shared/voltgrid/day3_payments_summary` (use this for demo) |
| Cluster | `New job cluster` → 1 worker, D4s_v3, DBR 15.4 LTS |

#### Step 3 — Add Task 2 (Silver Transform)

Click **+ Add task** (the + button that appears below task 1 or in the toolbar):

| Field | Value |
|---|---|
| Task name | `silver_transform` |
| Type | `Notebook` |
| Path | `Shared/voltgrid/day3_payments_summary` (same notebook for demo) |
| Depends on | `bronze_ingest` (select from dropdown) |
| Cluster | `New job cluster` → same config |

The **Depends on** field creates the arrow: silver_transform runs ONLY after bronze_ingest succeeds.

#### Step 4 — Add Task 3 (Gold Aggregation)

| Field | Value |
|---|---|
| Task name | `gold_aggregation` |
| Type | `Notebook` |
| Path | `Shared/voltgrid/day3_payments_summary` (same notebook for demo) |
| Depends on | `silver_transform` |
| Cluster | `New job cluster` → same config |

#### Step 5 — View the DAG

After adding all 3 tasks, you see a visual DAG (Directed Acyclic Graph):

```
  [bronze_ingest] ──► [silver_transform] ──► [gold_aggregation]
```

This is the same concept as ADF pipeline activities — tasks connected by dependencies.

#### Step 6 — Add a Schedule

Same as before: **Add trigger** → Scheduled → daily at 01:00 UTC (after midnight data lands in Bronze).

#### Step 7 — Run and Monitor

**Run now** → each task runs in sequence:
- `bronze_ingest` starts → cluster spins up → notebook runs → cluster terminates
- `silver_transform` starts → new cluster → runs → terminates
- `gold_aggregation` starts → new cluster → runs → terminates

Total pipeline time = sum of all task times + cluster startup per task.

---

## Part 7: Passing Parameters to Notebooks in a Job

When a job runs a notebook, you can pass parameters to it. In the notebook, you read the parameter with `dbutils.widgets`.

### Step 1 — Add a Widget to the Notebook

In `day3_payments_summary`, add this as Cell 1 (before other cells):

```python
# Widget: reads parameter from job run OR from the widget UI at the top of the notebook
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("run_date", "2024-01-17", "Run Date")

env = dbutils.widgets.get("environment")
run_date = dbutils.widgets.get("run_date")

print(f"Running for environment: {env}")
print(f"Run date: {run_date}")
```

When you run the notebook manually, a widget UI appears at the top — you can type values in the boxes. When the job runs the notebook, it injects the values automatically.

### Step 2 — Pass Parameters in the Job

1. Open the job → click the task (`run_payments_summary`)
2. Scroll to **Parameters** section
3. Add:

   | Key | Value |
   |---|---|
   | `environment` | `prod` |
   | `run_date` | `{{job.start_time.date}}` |

`{{job.start_time.date}}` is a Databricks dynamic value — it injects the actual run date automatically.

---

## Part 8: ADF + Databricks Jobs Integration (Overview)

In the VoltGrid project, Azure Data Factory triggers Databricks notebooks instead of running them on a schedule inside Databricks itself. This gives ADF full control over the pipeline sequence.

```
ADF Pipeline
  │
  ├── Copy Activity (Bronze ingest from API)
  │         ↓ on success
  └── Databricks Notebook Activity
            ↓ triggers notebook
      Databricks: silver_transform notebook
            ↓ on completion
      ADF: Gold aggregation Notebook Activity
```

**Two ways to trigger Databricks from ADF:**

| Method | ADF Activity | Cluster used | Best for |
|---|---|---|---|
| Run notebook directly | Databricks Notebook | Existing cluster or new job cluster | Simple notebook execution |
| Trigger a Databricks Job | Web Activity (REST API call) | Job cluster defined in the job | Complex multi-task jobs |

For Day 3 you should know the pattern exists. The detailed ADF–Databricks integration is covered in the ADF day 7 curriculum.

---

## Quick Reference — Day 3 Terminologies

```
Term                   Definition
──────────────────────────────────────────────────────────────────────────
Notebook               Interactive document of code cells — the primary
                       development tool in Databricks

Cell                   A single unit in a notebook — contains code or markdown

Magic Command          % prefix overrides cell language (%sql, %md, %sh, %fs)

Default Language       Language set when the notebook is created — applies
                       to all cells without a magic command

dbutils                Built-in Python object for file ops, secrets, notebook
                       flow control, and widgets

Temp View              A virtual table registered from a DataFrame, queryable
                       with %sql — exists for the lifetime of the cluster session

Widget                 A parameterised input to a notebook — text box, dropdown,
                       multiselect — can be passed from a job

Job (Workflow)         An automated, scheduled run of one or more notebooks
                       or scripts on Databricks compute

Task                   A single step inside a job — one notebook, one cluster

Job Cluster            A fresh cluster created by a job, auto-terminates when done

Task Dependency        "Depends on" link between tasks — controls execution order

DAG                    Directed Acyclic Graph — the visual representation of
                       task dependencies in a multi-task job

Trigger                What starts a job: a schedule (cron), manual, or API call

Job Run                One execution instance of a job — has its own run ID,
                       logs, and duration

Cron Expression        5-field time pattern (minute hour day month weekday)
                       used to schedule jobs — e.g. `0 0 * * *` = midnight daily

Retry                  Automatic re-execution of a failed task N times with
                       a delay between attempts

Notebook exit value    Value returned by dbutils.notebook.exit("...") — visible
                       in the job run output and can be used by downstream tasks

PAT                    Personal Access Token — used to authenticate with the
                       Databricks REST API or CLI
```
