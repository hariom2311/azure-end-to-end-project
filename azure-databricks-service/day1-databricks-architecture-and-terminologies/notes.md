# Day 1 — Azure Databricks: Architecture & Terminologies

> **Goal:** Understand what Azure Databricks is, how it is architecturally composed, and what every term means before writing a single line of code.
> **Context:** In the VoltGrid EV project, Databricks is the Silver and Gold layer compute engine — it reads Bronze JSON from ADLS Gen2, transforms it, and writes clean Parquet back to ADLS.

---

## Part 1: What Is Azure Databricks?

Azure Databricks is a **cloud-native, managed Apache Spark platform** built jointly by Microsoft and Databricks Inc. It runs on Azure infrastructure but is managed by Databricks — you get the full power of distributed Spark without having to install, configure, or maintain a Spark cluster yourself.

```
What it is:
  A managed Spark service optimised for data engineering, data science, and ML

What it is NOT:
  - Not a storage service (it reads/writes from ADLS, Azure SQL, etc.)
  - Not a scheduler (pipelines are orchestrated by ADF or Databricks Workflows)
  - Not a database (data lives in ADLS, Delta Lake is the storage format, not the compute)

Where it sits in the VoltGrid lakehouse:
  ADLS Gen2 Bronze (raw JSON)
       │
  Azure Databricks (Spark compute)
       │ transforms, cleans, joins, aggregates
  ADLS Gen2 Silver (clean Parquet / Delta)
       │
  Azure Databricks (Spark compute)
       │ business aggregations
  ADLS Gen2 Gold (business-ready Delta tables)
       │
  Power BI / Azure Synapse (reporting)
```

**Why Databricks instead of plain Spark?**

| Feature | Plain Apache Spark | Azure Databricks |
|---|---|---|
| Cluster management | Manual — install, configure, scale | Fully managed — one click |
| Notebook interface | Third-party (Zeppelin, Jupyter) | Built-in, collaborative, versioned |
| Delta Lake | External add-on | First-class, built-in |
| Unity Catalog | Not available | Built-in governance |
| Auto-scaling | Manual or complex config | Native, automatic |
| Security | Complex network + auth config | Azure AD + VNet injection |
| MLflow | Separate install | Built-in |

---

## Part 2: Databricks Architecture — Two Planes

The most important architectural concept in Azure Databricks is the **two-plane model**. Everything in Databricks runs in one of two planes:

```
┌─────────────────────────────────────────────────────────────────────┐
│                     CONTROL PLANE (Databricks-managed)               │
│                                                                       │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐               │
│  │  Web UI /   │  │  REST API    │  │  Job         │               │
│  │  Notebooks  │  │  (clusters,  │  │  Scheduler   │               │
│  │  IDE        │  │   jobs, etc) │  │  (Workflows) │               │
│  └─────────────┘  └──────────────┘  └──────────────┘               │
│                                                                       │
│  Hosted in Databricks' own Azure subscription                         │
│  Your code and metadata are stored here (notebook content,           │
│  job definitions, cluster configs)                                    │
└────────────────────────────────────┬────────────────────────────────┘
                                     │  secure channel (HTTPS/443)
┌────────────────────────────────────▼────────────────────────────────┐
│                     DATA PLANE (Your Azure subscription)             │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                  Azure Virtual Network (VNet)                │    │
│  │                                                              │    │
│  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │    │
│  │  │  Driver Node │    │ Worker Node 1│    │ Worker Node 2│  │    │
│  │  │  (Master)    │◄──►│  (Executor) │    │  (Executor)  │  │    │
│  │  └──────────────┘    └──────────────┘    └──────────────┘  │    │
│  │         │                                                    │    │
│  │         ▼                                                    │    │
│  │  ┌──────────────────────────────────────────────────────┐   │    │
│  │  │            Azure Data Lake Storage Gen2              │   │    │
│  │  │   (Bronze / Silver / Gold containers)                │   │    │
│  │  └──────────────────────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                       │
│  Hosted in YOUR Azure subscription — your VMs, your network,         │
│  your storage. Databricks does NOT see your data.                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Control Plane
- Managed entirely by Databricks
- Hosts the web UI, REST API, job scheduler, notebook storage, cluster config
- Runs in Databricks' own Azure subscription in the same region as your workspace
- Your data **never** flows through the Control Plane — only metadata and code

### Data Plane
- Runs in **your** Azure subscription
- The actual Spark cluster VMs (Driver + Workers) are here
- Your ADLS Gen2, Azure SQL, and other data sources are here
- Databricks provisions the VMs on your behalf when you create a cluster

**Why this matters:** Your data stays in your subscription at all times. Databricks cannot access your data directly — it only sends orchestration commands to your VMs through a secure channel.

---

## Part 3: Workspace

A **Workspace** is the top-level organisational unit in Databricks. Think of it like an Azure resource — you create one Databricks Workspace per environment (dev / staging / prod).

```
Azure Resource Group: rg-ev-intelligence-dev
  └── Azure Databricks Workspace: adb-ev-intelligence-dev
        ├── Notebooks
        ├── Clusters
        ├── Jobs / Workflows
        ├── Repos (Git)
        ├── Delta Tables (Unity Catalog)
        └── Users / Groups / Permissions
```

**What lives inside a Workspace:**
- All notebooks, files, and folders
- All cluster definitions
- All job / workflow definitions
- All secrets (via Secret Scopes)
- All users and their permissions
- All Unity Catalog objects (databases, tables, views)

**Workspace URL format:**
```
https://adb-<workspace-id>.<region>.azuredatabricks.net
```

---

## Part 4: Cluster Architecture

A **Cluster** is a set of virtual machines that run Spark. Every notebook, job, or query runs on a cluster. When you run a cell in a notebook, Databricks sends it to the cluster's Driver Node, which distributes work across Worker Nodes.

### 4.1 Cluster Components

```
┌─────────────────────────────────────────────────────────────────┐
│                         SPARK CLUSTER                           │
│                                                                  │
│  ┌───────────────────────────────────┐                          │
│  │           DRIVER NODE             │                          │
│  │  • Runs the SparkContext           │                          │
│  │  • Receives your code (notebook)  │                          │
│  │  • Creates execution plan (DAG)   │                          │
│  │  • Distributes tasks to workers   │                          │
│  │  • Collects results               │                          │
│  │  • Hosts Spark UI (port 4040)     │                          │
│  └──────────────┬────────────────────┘                          │
│                 │  distributes tasks                            │
│    ┌────────────▼──────┐  ┌─────────────────┐                  │
│    │   WORKER NODE 1   │  │  WORKER NODE 2  │  ...             │
│    │  ┌─────────────┐  │  │ ┌─────────────┐ │                  │
│    │  │ Executor 1  │  │  │ │ Executor 2  │ │                  │
│    │  │ ┌─────────┐ │  │  │ │ ┌─────────┐ │ │                  │
│    │  │ │ Task 1  │ │  │  │ │ │ Task 2  │ │ │                  │
│    │  │ │ Task 2  │ │  │  │ │ │ Task 3  │ │ │                  │
│    │  │ └─────────┘ │  │  │ │ └─────────┘ │ │                  │
│    │  └─────────────┘  │  │ └─────────────┘ │                  │
│    └───────────────────┘  └─────────────────┘                  │
└─────────────────────────────────────────────────────────────────┘
```

**Driver Node:**
- The "brain" of the cluster
- Runs `SparkContext` — the entry point to all Spark functionality
- Translates your Python/Scala/SQL code into a DAG (Directed Acyclic Graph) of stages and tasks
- Sends tasks to Executors and collects results
- Hosts the Spark Web UI at port 4040

**Worker Node:**
- The "muscle" — does the actual data processing
- Each Worker Node runs one or more **Executors**
- Workers communicate with the Driver but not with each other directly

**Executor:**
- A JVM process running on a Worker Node
- Runs Tasks — the smallest unit of work
- Has its own memory and CPU allocation
- Each Executor processes one partition of data at a time

### 4.2 Cluster Types

| Type | Description | Use case |
|---|---|---|
| **All-purpose cluster** | Long-running, interactive, shared | Notebook development, ad-hoc queries |
| **Job cluster** | Created for a single job, terminated after | Scheduled production jobs |
| **SQL Warehouse** (formerly SQL Endpoint) | Optimised for SQL queries, serverless option | BI tools, SQL analytics, Databricks SQL |

### 4.3 Cluster Modes

| Mode | Workers | Use case |
|---|---|---|
| **Standard** | 1+ workers, driver separate | Collaborative notebooks, shared compute |
| **Single Node** | Driver only, no workers | Local dev, small datasets, ML model training |
| **High Concurrency** | Multiple users share a cluster | Many users, strong isolation between workloads |

### 4.4 Databricks Runtime

The **Databricks Runtime (DBR)** is the software stack running on each cluster node. It includes:
- Apache Spark (specific version)
- Delta Lake libraries
- Python, R, Scala, Java
- Pre-installed ML libraries (MLflow, scikit-learn, TensorFlow, PyTorch) in ML Runtime variants
- Photon (vectorised query engine) in Photon Runtime

**Runtime naming convention:**
```
14.3 LTS (includes Apache Spark 3.5.0, Scala 2.12)
 │   │
 │   └── Long-Term Support — stable, supported for ~2 years
 └────── Databricks Runtime version

14.3 LTS ML — same as above but includes ML libraries
14.3 LTS Photon — same as above but with Photon engine
```

Always choose an **LTS** runtime for production clusters — it has a longer support window and is more stable than non-LTS versions.

---

## Part 5: Notebook

A **Notebook** is the primary interface for writing and running code in Databricks. It is a document that combines code cells, markdown cells, and output — similar to Jupyter Notebook but with collaborative, cloud-native features.

### 5.1 Key Notebook Concepts

**Supported languages per notebook:**
- Each notebook has a default language set at creation (Python, Scala, SQL, R)
- You can override the language per cell using magic commands:
  ```
  %python   → run this cell as Python
  %scala    → run this cell as Scala
  %sql      → run this cell as SQL
  %r        → run this cell as R
  %md       → render this cell as Markdown
  %sh       → run this cell as shell command
  %fs       → run DBFS (Databricks File System) commands
  %run      → execute another notebook and inherit its variables
  ```

**Cell execution:**
- Cells run sequentially by default (top to bottom)
- Each cell's output appears directly below it
- `Shift + Enter` runs current cell and moves to the next
- `Ctrl + Enter` runs current cell and stays

**Notebook state:**
- All variables, DataFrames, and imports are shared within a notebook session
- The kernel (Python/Scala runtime) stays alive as long as the cluster is running and the notebook is attached

### 5.2 Widgets (Parameters)

Widgets let you pass parameters into a notebook — from the UI or from ADF:

```python
# Declare a widget
dbutils.widgets.text("run_date", "2026-10-01", "Run Date")

# Read the value
run_date = dbutils.widgets.get("run_date")
print(run_date)  # → "2026-10-01"
```

When ADF calls the notebook via Databricks Activity, it passes `base_parameters` which are received as widget values.

---

## Part 6: Apache Spark Core Concepts

Understanding Spark's execution model is essential to writing efficient Databricks notebooks.

### 6.1 RDD (Resilient Distributed Dataset)

The foundational Spark data structure. An RDD is:
- **Resilient** — fault-tolerant; if a partition is lost, Spark recomputes it from the lineage
- **Distributed** — data is split across multiple nodes
- **Dataset** — a collection of records

In modern Spark (2.0+), you almost never use RDDs directly — DataFrames and Datasets provide a higher-level API that is optimised by the Catalyst query engine. RDD knowledge is useful for understanding Spark internals.

### 6.2 DataFrame

A **DataFrame** is a distributed collection of data organised into named columns — like a table in a database or a pandas DataFrame, but distributed across the cluster.

```python
# Create a DataFrame by reading from ADLS
df = spark.read.json(
    "abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/"
)

# Show schema
df.printSchema()
# root
#  |-- id: long (nullable = true)
#  |-- amount: string (nullable = true)
#  |-- status: string (nullable = true)
#  |-- created_at: string (nullable = true)

# Show first 5 rows
df.show(5)
```

DataFrames are **immutable** — every transformation creates a new DataFrame. The original is never modified.

### 6.3 Dataset

A typed version of DataFrame — available in Scala and Java, not in Python (Python DataFrames are always untyped at runtime). In PySpark, DataFrame = Dataset[Row].

### 6.4 Transformations vs Actions

**This is the most critical Spark concept.** Spark is **lazy** — it does not execute anything until you call an Action.

```
Transformations (lazy — build a plan, don't execute):
  df.filter(...)     → no data is read yet
  df.select(...)     → no data is read yet
  df.groupBy(...)    → no data is read yet
  df.join(...)       → no data is read yet
  df.withColumn(...) → no data is read yet

Actions (trigger actual execution):
  df.show()          → NOW Spark reads data and executes the plan
  df.count()         → NOW Spark reads data
  df.collect()       → NOW Spark reads data and returns to Driver
  df.write.parquet() → NOW Spark reads data and writes output
```

**Why lazy evaluation?**
Spark collects all transformations first, then optimises the full plan using Catalyst before executing. This produces a much more efficient execution plan than running each step immediately.

### 6.5 DAG (Directed Acyclic Graph)

When you trigger an Action, Spark builds a **DAG** of all the transformations needed to compute the result. The DAG shows the dependency order — which operations must complete before others can start.

```
DAG for: df.filter().groupBy().agg().write()

Read JSON ──► Filter ──► GroupBy ──► Aggregate ──► Write Parquet
```

You can visualise the DAG in the Spark UI (accessible from the cluster page → Spark UI → click any running job → DAG Visualisation).

### 6.6 Job, Stage, Task

When Spark executes a DAG, it breaks it into:

```
JOB
└── Created by one Action (e.g., df.write())
    └── STAGES
        └── A stage boundary occurs at a SHUFFLE (data redistribution across nodes)
            └── TASKS
                └── One task per partition per stage
                    Each task processes one chunk of data on one Executor
```

**Example:**
```
df = spark.read.json(...)          # read 4 partitions → 4 tasks in Stage 1
   .filter(df.status == 'active')  # no shuffle → still Stage 1
   .groupBy('date')                # SHUFFLE (all records for each date must go to same node)
   .agg(count('id'))               # Stage 2 — 4 tasks (one per output partition)
   .write.parquet(...)             # triggers Job with 2 Stages, 8 Tasks total
```

### 6.7 Partition

A **partition** is a chunk of data that one Task processes. Spark reads large files and splits them into partitions so multiple Executors can work in parallel.

```
payments.json (1GB file)
  → Spark splits into N partitions
  → Each partition processed by one Task on one Executor
  → All Tasks run in parallel across Workers

Default partition size: ~128MB
Default partitions after shuffle: spark.sql.shuffle.partitions = 200
```

**Why partitioning matters:**
- Too few partitions → not enough parallelism → slow
- Too many partitions → overhead of managing small tasks → also slow
- Right number → depends on data size and cluster size

### 6.8 Shuffle

A **shuffle** is the most expensive Spark operation. It redistributes data across all Worker Nodes — all records with the same key must end up on the same Executor for operations like `groupBy`, `join`, `distinct`.

```
Before shuffle:
  Worker 1: [payments: date=2026-10-01, date=2026-10-02]
  Worker 2: [payments: date=2026-10-01, date=2026-10-03]

Shuffle (groupBy date):
  Worker 1: [all date=2026-10-01 records from both workers]
  Worker 2: [all date=2026-10-02 records from Worker 1]
  Worker 3: [all date=2026-10-03 records from Worker 2]
```

Shuffles involve network I/O and disk I/O — minimize them by using partition-friendly operations and caching.

---

## Part 7: Delta Lake

**Delta Lake** is the open-source storage layer that brings ACID transactions and versioning to data lake files (Parquet). It is the default storage format for Databricks tables and the foundation of the Lakehouse architecture.

### 7.1 What Delta Lake Adds Over Plain Parquet

| Feature | Plain Parquet | Delta Lake |
|---|---|---|
| ACID transactions | No | Yes |
| Schema enforcement | No | Yes — rejects incompatible writes |
| Schema evolution | No | Yes — `mergeSchema` option |
| Time travel | No | Yes — query historical versions |
| DML (UPDATE, DELETE, MERGE) | No | Yes |
| Audit log | No | Yes — Delta transaction log |
| Streaming + batch unified | No | Yes |

### 7.2 Delta Table Structure

A Delta table is a folder in ADLS Gen2 that contains:

```
silver/api/payments/
  ├── _delta_log/                    ← Delta transaction log (JSON files)
  │     ├── 00000000000000000000.json  ← commit 0: initial write
  │     ├── 00000000000000000001.json  ← commit 1: append
  │     ├── 00000000000000000002.json  ← commit 2: UPDATE
  │     └── ...
  ├── part-00000-abc123.snappy.parquet  ← actual data
  ├── part-00001-def456.snappy.parquet
  └── ...
```

The `_delta_log` folder is what makes a folder a Delta table. Every write operation (INSERT, UPDATE, DELETE, MERGE) appends a new JSON file to the log. Spark reads the log to determine which Parquet files are current (vs. deleted by an UPDATE).

### 7.3 Delta Lake Key Operations

**Read:**
```python
df = spark.read.format("delta").load(
    "abfss://silver@evdatalakedev.dfs.core.windows.net/api/payments/"
)
# Or using SQL:
# spark.sql("SELECT * FROM silver.payments")
```

**Write (overwrite):**
```python
df.write.format("delta").mode("overwrite").save(
    "abfss://silver@evdatalakedev.dfs.core.windows.net/api/payments/"
)
```

**Append:**
```python
df.write.format("delta").mode("append").save(...)
```

**MERGE (upsert):**
```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "abfss://silver@.../api/payments/")

target.alias("t").merge(
    df.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()
```

**Time Travel:**
```python
# Read version 3 of the table
df_v3 = spark.read.format("delta").option("versionAsOf", 3).load("abfss://silver@.../api/payments/")

# Read as of a timestamp
df_yesterday = spark.read.format("delta").option("timestampAsOf", "2026-09-30").load(...)
```

**VACUUM (cleanup old files):**
```python
# Remove files older than 7 days (default retention)
spark.sql("VACUUM silver.payments RETAIN 168 HOURS")
```

### 7.4 Delta Lake Key Terms

**Delta Transaction Log:** The `_delta_log` directory that records every change to the table as a series of JSON commit files. This is what enables ACID transactions and time travel.

**Checkpoint:** Every 10 commits, Delta creates a Parquet checkpoint file that consolidates the JSON log — prevents the log from growing indefinitely and speeds up reads.

**OPTIMIZE:** Compacts small Parquet files into larger ones — improves read performance:
```sql
OPTIMIZE silver.payments ZORDER BY (created_date)
```

**Z-ORDER:** A multi-dimensional clustering technique that co-locates related rows in the same files — dramatically speeds up queries that filter on those columns.

**VACUUM:** Removes old Parquet data files that are no longer referenced by the transaction log — reclaims storage space.

---

## Part 8: DBFS (Databricks File System)

**DBFS** is a distributed file system abstraction layer that maps to the underlying cloud storage. It provides a POSIX-like path interface (`/dbfs/...`) over ADLS Gen2, Azure Blob Storage, or the Databricks managed storage.

```
DBFS path:          /databricks/datasets/payments/
Underlying storage: abfss://dbfs-root@<workspace-storage>.dfs.core.windows.net/...

ADLS mount:
  /mnt/bronze/  →  abfss://bronze@evdatalakedev.dfs.core.windows.net/
  /mnt/silver/  →  abfss://silver@evdatalakedev.dfs.core.windows.net/
```

**DBFS vs direct ADLS paths:**

| | DBFS mount (`/mnt/`) | Direct ABFSS path |
|---|---|---|
| Syntax | `/mnt/bronze/api/payments/` | `abfss://bronze@account.dfs.core.windows.net/api/payments/` |
| Auth | Set up once at mount time | Per-session or via Unity Catalog |
| Recommended? | Legacy — use direct paths with Unity Catalog instead | Yes — modern approach |

**dbutils.fs commands:**
```python
dbutils.fs.ls("/mnt/bronze/api/payments/")     # list files
dbutils.fs.mkdirs("/mnt/silver/api/payments/") # create directory
dbutils.fs.cp("source", "dest")                # copy
dbutils.fs.rm("path", recurse=True)            # delete
dbutils.fs.head("path")                        # read first N bytes
```

Or using `%fs` magic:
```
%fs ls /mnt/bronze/api/payments/
```

---

## Part 9: SparkSession and SparkContext

### SparkSession
The **SparkSession** is the entry point to all Spark functionality in a notebook. In Databricks, `spark` is pre-created automatically — you never need to initialise it.

```python
# Already available in every Databricks notebook:
spark  # → SparkSession

# Read data
df = spark.read.json(...)

# Run SQL
result = spark.sql("SELECT * FROM silver.payments WHERE status = 'active'")

# Access configuration
spark.conf.set("spark.sql.shuffle.partitions", "50")
```

### SparkContext
The lower-level entry point (pre-DataFrames era). Also pre-created in Databricks as `sc`.

```python
sc  # → SparkContext

# Used for RDD operations
rdd = sc.parallelize([1, 2, 3, 4, 5])
```

In modern PySpark, you almost always use `spark` (SparkSession), not `sc` (SparkContext) directly.

---

## Part 10: dbutils

**dbutils** is a Databricks utility library pre-loaded in every notebook. It provides file system operations, secret access, widget management, and notebook chaining.

### Key dbutils modules:

**dbutils.fs** — file system operations:
```python
dbutils.fs.ls("/mnt/bronze/")          # list directory
dbutils.fs.cp("src", "dst")            # copy
dbutils.fs.rm("path", recurse=True)    # delete
dbutils.fs.mkdirs("path")              # create folder
```

**dbutils.secrets** — read secrets without exposing them in code:
```python
token = dbutils.secrets.get(scope="kv-ev-scope", key="voltgrid-token")
# token is never printed in notebook output — it appears as [REDACTED]
```

**dbutils.widgets** — parameterise notebooks:
```python
dbutils.widgets.text("run_date", "2026-10-01")
run_date = dbutils.widgets.get("run_date")
```

**dbutils.notebook** — call another notebook and get its result:
```python
result = dbutils.notebook.run(
    "/VoltGrid/silver/process_payments",
    timeout_seconds=3600,
    arguments={"run_date": "2026-10-01"}
)
```

**dbutils.jobs** — access job context info:
```python
run_id = dbutils.jobs.taskValues.get(taskKey="act_copy", key="rows_copied")
```

---

## Part 11: Unity Catalog

**Unity Catalog** is Databricks' unified governance and data catalogue for the Lakehouse. It provides a three-level namespace for all data objects and centralised access control.

### 11.1 Three-Level Namespace

```
CATALOG
  └── SCHEMA (DATABASE)
        └── TABLE / VIEW / VOLUME / FUNCTION

Example for VoltGrid:
  ev_catalog
    ├── bronze
    │     ├── payments        ← raw ingested table
    │     ├── sessions
    │     └── customers
    ├── silver
    │     ├── payments        ← cleaned, typed
    │     ├── sessions
    │     └── customers
    └── gold
          ├── daily_revenue
          └── station_utilisation
```

**SQL syntax:**
```sql
-- Full three-part name
SELECT * FROM ev_catalog.silver.payments;

-- Set defaults to avoid typing catalog and schema each time
USE CATALOG ev_catalog;
USE SCHEMA silver;
SELECT * FROM payments;  -- resolves to ev_catalog.silver.payments
```

### 11.2 Unity Catalog Objects

| Object | Description |
|---|---|
| **Catalog** | Top-level namespace — like a database server |
| **Schema** | Second level — like a database or schema in SQL Server |
| **Table** | A Delta table (managed or external) |
| **View** | A saved SQL query — no data stored |
| **Volume** | Unstructured file storage governed by Unity Catalog |
| **External Location** | A registered ADLS path — allows Unity Catalog to govern access to ADLS |
| **Storage Credential** | Azure Managed Identity or Service Principal used to access ADLS |
| **Function** | User-defined SQL or Python function registered in Unity Catalog |

### 11.3 Managed vs External Tables

| | Managed Table | External Table |
|---|---|---|
| Data location | Unity Catalog managed storage | Your ADLS Gen2 path |
| `DROP TABLE` behaviour | Deletes data files | Only removes table definition — data stays |
| Use when | Table owned entirely by Databricks | Data shared with ADF, Synapse, or other tools |

---

## Part 12: Databricks Workflows (Jobs)

A **Workflow** (formerly Job) is a scheduled or triggered execution of one or more tasks. Think of it as Databricks' own pipeline orchestrator — an alternative to ADF for pure Databricks workloads.

### 12.1 Workflow Components

```
Workflow: wf_ev_daily_silver
  ├── Task 1: nb_bronze_to_silver_payments  (Notebook task)
  │     Depends on: nothing (runs first)
  │     Cluster: job_cluster_small
  │
  ├── Task 2: nb_bronze_to_silver_sessions  (Notebook task)
  │     Depends on: nothing (runs in parallel with Task 1)
  │     Cluster: job_cluster_small
  │
  └── Task 3: nb_gold_daily_revenue         (Notebook task)
        Depends on: Task 1 AND Task 2 (waits for both)
        Cluster: job_cluster_medium
```

### 12.2 Task Types

| Task type | Description |
|---|---|
| **Notebook** | Run a Databricks notebook |
| **Python script** | Run a `.py` file stored in DBFS or Repos |
| **JAR** | Run a compiled Scala/Java JAR |
| **SQL** | Run a SQL query or file |
| **dbt** | Run a dbt project |
| **Pipeline (DLT)** | Run a Delta Live Tables pipeline |
| **Spark Submit** | Submit a raw Spark application |

### 12.3 Trigger Types

| Trigger | Description |
|---|---|
| Manual | Run on demand |
| Scheduled | Cron expression (e.g., every day at 2am) |
| File arrival | Trigger when a file lands in ADLS |
| Continuous | Streaming pipelines — restart automatically on failure |

---

## Part 13: Delta Live Tables (DLT)

**Delta Live Tables** is a declarative pipeline framework in Databricks. Instead of writing imperative Spark code (`read → transform → write`), you declare what each table should contain and Databricks manages dependencies, ordering, and error handling automatically.

```python
# DLT pipeline — declarative style
import dlt

@dlt.table(
    name="silver_payments",
    comment="Cleaned payments from Bronze"
)
def silver_payments():
    return (
        dlt.read("bronze_payments")
           .filter("status != 'failed'")
           .withColumn("amount_aud", col("amount").cast("double"))
    )

@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue aggregation"
)
def gold_daily_revenue():
    return (
        dlt.read("silver_payments")
           .groupBy("created_date")
           .agg(sum("amount_aud").alias("total_revenue"))
    )
```

DLT automatically:
- Determines the execution order (`bronze_payments` before `silver_payments` before `gold_daily_revenue`)
- Handles data quality checks (Expectations)
- Retries failed tasks
- Tracks lineage

---

## Part 14: MLflow

**MLflow** is the open-source ML lifecycle management platform built into Databricks. It tracks experiments, models, parameters, and metrics across ML training runs.

```python
import mlflow

with mlflow.start_run():
    mlflow.log_param("learning_rate", 0.01)
    mlflow.log_param("max_depth", 5)

    # ... train model ...

    mlflow.log_metric("accuracy", 0.94)
    mlflow.log_metric("rmse", 0.12)

    mlflow.sklearn.log_model(model, "ev-charge-predictor")
```

MLflow components:
- **Tracking:** log parameters, metrics, and artefacts for each run
- **Projects:** package ML code for reproducible runs
- **Models:** standardised model packaging format
- **Registry:** version control for models — staging → production promotion

---

## Part 15: Complete Terminologies Reference

| Term | Definition |
|---|---|
| **Workspace** | Top-level Databricks resource — contains all notebooks, clusters, jobs |
| **Control Plane** | Databricks-managed layer hosting the UI, API, and job scheduler |
| **Data Plane** | Your Azure subscription — where cluster VMs and your data live |
| **Cluster** | A set of VMs (Driver + Workers) that run Spark |
| **Driver Node** | Master node — coordinates execution, hosts SparkContext |
| **Worker Node** | Slave nodes — run Executors that process data |
| **Executor** | JVM process on a Worker — runs Tasks |
| **Task** | Smallest unit of work — processes one data partition |
| **Partition** | A chunk of data assigned to one Task |
| **Shuffle** | Redistributing data across Workers — expensive, causes stage boundaries |
| **DAG** | Directed Acyclic Graph — the execution plan Spark builds from your transformations |
| **Job** | A triggered execution created by one Action |
| **Stage** | A group of Tasks that can run without a shuffle; separated by shuffle boundaries |
| **RDD** | Resilient Distributed Dataset — low-level Spark data structure |
| **DataFrame** | High-level distributed table with named columns — the standard PySpark abstraction |
| **Transformation** | Lazy operation on a DataFrame (filter, select, join) — no execution until Action |
| **Action** | Triggers actual computation (show, count, write, collect) |
| **SparkSession** | Entry point to Spark — pre-created as `spark` in every Databricks notebook |
| **SparkContext** | Lower-level entry point — pre-created as `sc` |
| **DBR (Databricks Runtime)** | Software stack on each cluster: Spark + Python + Delta Lake + libraries |
| **LTS** | Long-Term Support runtime — stable, recommended for production |
| **Notebook** | Interactive document with code + output + markdown cells |
| **Magic command** | Cell-level language or utility override (`%python`, `%sql`, `%fs`, `%run`) |
| **Widget** | Notebook parameter — input value from UI or ADF |
| **dbutils** | Databricks utility library — file ops, secrets, widgets, notebook chaining |
| **DBFS** | Databricks File System — abstraction layer over cloud storage |
| **Delta Lake** | Storage layer adding ACID transactions and versioning to Parquet files |
| **Delta Table** | A folder of Parquet files managed by the Delta transaction log |
| **Delta Transaction Log** | `_delta_log/` directory recording every change to a Delta table |
| **Checkpoint** | Parquet snapshot of the Delta log created every 10 commits |
| **OPTIMIZE** | Compacts small Delta files into larger ones for faster reads |
| **Z-ORDER** | Multi-dimensional clustering in Delta OPTIMIZE — speeds up filtered queries |
| **VACUUM** | Removes old Delta files no longer referenced by the log |
| **Time Travel** | Query a Delta table as it existed at a previous version or timestamp |
| **MERGE** | Upsert operation in Delta — update matching rows, insert new ones |
| **Unity Catalog** | Databricks' governance layer — three-level namespace + access control |
| **Catalog** | Top level of Unity Catalog namespace |
| **Schema** | Second level of Unity Catalog namespace (same as DATABASE) |
| **Managed Table** | Table whose data is owned by Unity Catalog — `DROP TABLE` deletes data |
| **External Table** | Table pointing to your ADLS path — `DROP TABLE` keeps data |
| **Volume** | Unstructured file area governed by Unity Catalog |
| **External Location** | A registered ADLS path in Unity Catalog |
| **Storage Credential** | Managed Identity or Service Principal giving Unity Catalog ADLS access |
| **Workflow** | Databricks job — scheduled or triggered pipeline of tasks |
| **Job Cluster** | Auto-created cluster for a single job run, terminated after |
| **All-Purpose Cluster** | Persistent, shared cluster for interactive development |
| **SQL Warehouse** | Compute optimised for SQL queries — used with Databricks SQL |
| **Photon** | Databricks' vectorised query engine — written in C++, faster than standard Spark |
| **Delta Live Tables (DLT)** | Declarative pipeline framework — define what tables contain, DLT manages execution |
| **MLflow** | ML lifecycle platform — experiment tracking, model registry |
| **Repos** | Git integration — sync notebooks with GitHub/Azure DevOps |
| **Secret Scope** | Named collection of secrets — backed by Azure Key Vault or Databricks |
| **ABFSS** | Azure Blob File System Secure — URI scheme for ADLS Gen2 (`abfss://container@account.dfs.core.windows.net/`) |
| **Auto-scaling** | Cluster automatically adds/removes Worker Nodes based on workload |
| **Auto-termination** | Cluster shuts down automatically after N minutes of inactivity |
| **Spot/Preemptible VMs** | Cheaper VMs for Workers — can be reclaimed by Azure; use Standard VMs for Driver |
| **Instance Pool** | Pre-warmed VMs shared across clusters — reduces cluster start time |
| **Photon Runtime** | DBR variant with the Photon C++ query engine for faster SQL |
| **Catalyst Optimizer** | Spark's query optimiser — rewrites and optimises DAG plans |
| **Tungsten** | Spark's execution engine — binary processing, code generation |
| **Adaptive Query Execution (AQE)** | Runtime plan optimisation based on actual data statistics |
| **Broadcast Join** | Sends a small table to all Workers to avoid shuffle — use for small reference tables |
| **Streaming** | Processing data in real time using Spark Structured Streaming |
| **Trigger** | Controls how often a streaming query processes new data (once, continuous, interval) |
| **Checkpoint Location** | ADLS path where Structured Streaming saves its state for fault tolerance |
