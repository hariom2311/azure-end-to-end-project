# 01 — Databricks Platform Architecture
**Day 4 | What Databricks actually is under the hood**

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DATABRICKS CONTROL PLANE                             │
│                    (Managed by Databricks Inc. — Azure West US)             │
│                                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌────────────────┐  │
│  │  Workspace   │  │  Jobs /      │  │   Unity      │  │  Databricks    │  │
│  │  UI (Web)    │  │  Workflows   │  │   Catalog    │  │  REST API      │  │
│  │              │  │  Scheduler   │  │   Metastore  │  │                │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  └────────────────┘  │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  Cluster Manager — provisions, autoscales, terminates clusters       │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                              │  secure channel (TLS)
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         YOUR DATA PLANE                                     │
│              (Runs inside YOUR Azure subscription — Central India)          │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    Virtual Network (VNet)                            │   │
│  │                                                                      │   │
│  │   ┌─────────────────────────────────────────────────────────────┐   │   │
│  │   │  Managed Resource Group (auto-created by Databricks)        │   │   │
│  │   │                                                             │   │   │
│  │   │  ┌─────────────────┐    ┌─────────────────┐                │   │   │
│  │   │  │  Driver Node    │    │  Worker Node(s) │                │   │   │
│  │   │  │  (Spark master) │◄──►│  (Spark workers)│                │   │   │
│  │   │  │                 │    │                 │                │   │   │
│  │   │  │  dev-cluster    │    │  (auto-scale    │                │   │   │
│  │   │  │                 │    │   or fixed)     │                │   │   │
│  │   │  └─────────────────┘    └─────────────────┘                │   │   │
│  │   │                                                             │   │   │
│  │   └─────────────────────────────────────────────────────────────┘   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────────┐    │
│  │  ADLS Gen2       │   │  Azure Key Vault  │   │  Azure Data Factory  │    │
│  │  (evdatalakedev) │   │  (kv-ev-intel..)  │   │  (adf-ev-intel..)   │    │
│  │  bronze/silver/  │   │                  │   │                      │    │
│  │  gold containers │   │                  │   │                      │    │
│  └──────────────────┘   └──────────────────┘   └──────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Every Term Explained

### Control Plane
**What it is:** The brain of Databricks — hosted and managed entirely by Databricks Inc. on their own Azure infrastructure (typically West US). You never see, pay for, or manage these servers directly.

**What runs here:**
- The web UI you open in your browser
- The job scheduler that fires your hourly blob migration job
- The cluster manager that provisions VMs in your subscription when a cluster starts
- The Unity Catalog metadata store (table definitions, permissions, lineage)
- The Databricks REST API

**Why this split exists:** Databricks keeps the orchestration logic on their side so they can update it without touching your data. Your actual data never leaves your subscription — only instructions and metadata flow through the control plane.

**In our project:** When you click "Run now" on `job_bronze_charging_sessions_hourly`, the control plane receives that instruction, talks to your Azure subscription, provisions compute (or uses an existing cluster), and orchestrates the notebook run. The data being copied never passes through Databricks infrastructure — it goes directly from the source blob to your Bronze Volume inside your VNet.

---

### Data Plane
**What it is:** Everything that actually runs inside your Azure subscription. This is where your data lives, where Spark executes, and where your storage accounts sit.

**What runs here:**
- Your Databricks clusters (VMs provisioned in your subscription)
- Your ADLS Gen2 storage (Bronze/Silver/Gold containers)
- Your Key Vault
- Your ADF instance

**Why this matters:** Your raw payment data, charging session CSVs, and Delta tables never leave your Azure subscription. Databricks infrastructure sees metadata and instructions — not your actual rows.

**In our project:** `dev-cluster` runs in VMs inside your Azure Resource Group `rg-ev-intelligence-dev`. When a notebook copies files from source blob to Bronze Volume, both endpoints are inside your subscription.

---

### Workspace
**What it is:** A logical boundary inside Databricks that groups together notebooks, clusters, jobs, and users. Think of it as a "project environment."

**In Azure terms:** Each workspace corresponds to one Azure resource — `dbw-ev-intelligence-dev` in your subscription. The Azure resource just holds the URL and configuration; the actual workspace objects (notebooks, jobs) are stored in the control plane.

**In our project:** You have one workspace: `dbw-ev-intelligence-dev`. Everything you build (notebooks, jobs, clusters) lives inside this workspace.

**URL format:** `https://adb-<workspace-id>.azuredatabricks.net`

**What you see in the UI:** Left sidebar with Workspace, Catalog, Workflows, Compute, Data tabs.

---

### Cluster
**What it is:** A set of virtual machines (one driver + one or more workers) running Apache Spark. Notebooks and jobs run code ON a cluster — the cluster is the actual compute engine.

**Driver node:** The Spark master. Coordinates work, runs your Python/Scala/SQL code that isn't distributed, manages the DAG (Directed Acyclic Graph) of computation.

**Worker node(s):** Spark executors. Do the actual distributed data processing — reading files, applying transformations, writing output.

**Two cluster types in Databricks:**

| Type | Name | Best for |
|---|---|---|
| All-Purpose (Interactive) | `dev-cluster` in our project | Notebooks, development, ad-hoc queries |
| Job Cluster | Created fresh per Job run | Production scheduled jobs — cold start each run, auto-terminated after |

**In our project:** `dev-cluster` is an All-Purpose cluster. It stays running (or auto-terminates after idle time) and is shared by all notebooks you run interactively. The hourly blob migration job also uses it — avoiding cold-start latency.

**Auto-termination:** After N minutes of inactivity (default 120 min), the cluster shuts down automatically to save cost. The cluster manager in the control plane detects idle time and sends a terminate command to your subscription.

**Why clusters are in YOUR subscription:** Because the compute is billed directly to you via Azure — Databricks doesn't pay for your VMs, you do. The cluster manager in the control plane just orchestrates when to start and stop them.

---

### Spark
**What it is:** The distributed computing engine that runs on your cluster. When you write `spark.read.csv(...)` in a notebook, Spark splits the work across all worker nodes in parallel.

**In our project:** Used in the Bronze migration notebook to read CSVs and verify schema. In Silver layer (coming later), Spark will read all Bronze CSVs, apply schema, deduplicate, and write Delta tables — processing millions of rows in parallel across workers.

**Why Spark and not plain Python:** A single-node Python script can read one file at a time. Spark on a 4-node cluster reads 4 files simultaneously and merges results. At scale (hundreds of GB of data), this is the difference between 2 minutes and 2 hours.

---

### Notebook
**What it is:** An interactive document inside a workspace that mixes code cells (Python, SQL, Scala, R) with output cells. Runs on a connected cluster.

**In our project:**
- `01_bronze_blob_charging_sessions.ipynb` — manual v1 migration
- `02_bronze_blob_charging_sessions_v2.ipynb` — scheduled hourly v2 migration

**How to view in UI:** Left sidebar → **Workspace** → navigate to the folder → click the notebook name.

---

### Job / Workflow
**What it is:** A scheduled or triggered run of one or more notebooks (or JARs, Python scripts, SQL queries). The Workflows scheduler in the control plane fires jobs based on a cron expression and records results.

**In our project:** `job_bronze_charging_sessions_hourly` — fires every hour, runs the v2 migration notebook on `dev-cluster`.

**How to view in UI:** Left sidebar → **Workflows** → click the job name → **Run history** tab shows every past run with timestamps, duration, and success/failure.

---

### DBFS (Databricks File System)
**What it is:** A virtual filesystem abstraction that maps paths like `/dbfs/...` to actual cloud storage. In legacy Databricks (without Unity Catalog), everything was accessed through DBFS mount points.

**In our project:** We do NOT use DBFS mounts. We use Unity Catalog Volumes and `abfss://` paths directly — this is the modern, recommended approach. DBFS still exists but is being deprecated for data storage.

---

### Delta Lake
**What it is:** An open-source storage format built on top of Parquet files that adds:
- **ACID transactions** — no partial writes; a write either fully succeeds or fully rolls back
- **Time travel** — query data as it was at any previous point in time (`VERSION AS OF 3`)
- **Schema enforcement** — rejects writes that don't match the table schema
- **MERGE (upsert)** — update existing rows AND insert new ones in one operation

**In our project:** Bronze is currently raw JSON/CSV files. Silver layer (Day 5+) will write Delta tables — enabling MERGE for deduplication and schema enforcement.

**Why Delta and not plain Parquet:** Plain Parquet has no ACID guarantees — if a write fails halfway, you get corrupt data. Delta's transaction log (`_delta_log/`) records every operation atomically. This is critical when multiple pipelines write to the same table concurrently.

---

### Secret Scope
**What it is:** A Databricks abstraction that links a Key Vault to a workspace so notebooks can call `dbutils.secrets.get(scope, key)` without hardcoding credentials.

**In our project:** `kv-ev-scope` links to `kv-ev-intelligence-dev`. When the migration notebook calls:
```python
dbutils.secrets.get(scope="kv-ev-scope", key="source-sas-token")
```
Databricks fetches the secret value from Key Vault at runtime — the value is never stored in the notebook or visible in output.

**How to view in UI:** Settings → Secret scopes (accessible via URL: `https://<workspace-url>#secrets/createScope`). Secret scope values are NEVER visible in the UI — only the scope name and key names.
