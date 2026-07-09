# 01 — Databricks Platform Architecture
**Day 4 | What Databricks actually is under the hood**

---

## What Is Databricks?

Databricks is a **unified data intelligence platform** built on top of Apache Spark. It gives you a managed environment where you can ingest, process, transform, and analyse large-scale data — combining a data warehouse, a data lake, and a machine learning platform into one product (called a **Lakehouse**).

Founded in 2013 by the creators of Apache Spark, Databricks sits between raw cloud infrastructure (ADLS, S3, GCS) and the tools that consume data (Power BI, ML models, dashboards). It handles the hard parts: distributed compute, cluster lifecycle, scheduling, governance, and optimised query execution.

**In one line:** Databricks is a managed Apache Spark environment with governance (Unity Catalog), orchestration (Workflows), and SQL analytics built on top — running inside your own cloud subscription.

---

## How Databricks Compares to Its Alternatives

### The Landscape

```
                  ┌──────────────────────────────────────────────────────┐
                  │          DATA PLATFORM LANDSCAPE                     │
                  │                                                      │
  PURE WAREHOUSE  │  Snowflake ─── Azure Synapse ─── BigQuery            │
  (SQL-first,     │       ↑              ↑               ↑               │
   closed storage)│    proprietary    hybrid          GCS-native         │
                  │                                                      │
  LAKEHOUSE       │  Databricks ─────────────────── Apache Iceberg       │
  (open storage,  │  (open Delta Lake, any cloud)   (open table format,  │
   compute+govern)│                                  needs own compute)  │
                  │                                                      │
  PURE STREAMING  │  Azure Stream Analytics ─── Confluent (Kafka)        │
                  │                                                      │
                  └──────────────────────────────────────────────────────┘
```

---

### Databricks vs Snowflake

Snowflake is the most common alternative. Both are enterprise data platforms, but they are built on fundamentally different philosophies.

| Dimension | Databricks | Snowflake |
|---|---|---|
| **Storage format** | Open — Delta Lake (Parquet + transaction log). Your files on ADLS/S3/GCS. You own the data. | Proprietary. Data loaded INTO Snowflake's internal storage. You don't own the raw files. |
| **Compute model** | You bring clusters (VMs in YOUR subscription). You pay Azure for the VMs directly. | Snowflake manages "virtual warehouses" — compute is inside Snowflake's cloud, billed per second via Snowflake credits. |
| **Primary language** | Python (PySpark), SQL, Scala, R | SQL-first. Python supported via Snowpark but SQL is the primary interface. |
| **ML / AI** | First-class. MLflow built in. Train models, serve endpoints, run LLMs on the same platform. | Not native. Snowflake ML exists but is limited — ML teams typically export data to external tools. |
| **Streaming data** | Native via Spark Structured Streaming + Delta Live Tables. Real-time ingestion into Delta tables. | Limited. Snowpipe for micro-batch near-real-time. Not designed for true streaming. |
| **Schema on read vs write** | Both. Bronze = schema on read (raw CSV). Silver = Delta enforces schema on write. | Schema on write always. Data must match table schema at load time. |
| **Data governance** | Unity Catalog — fine-grained column/row level, lineage, external locations | Snowflake's own access controls. Horizon (governance layer) — similar capability but closed. |
| **Cost model** | Compute = Azure VM cost (billed by Azure, not Databricks). Storage = ADLS cost (cheap). Databricks charges a DBU (Databricks Unit) on top per node. | Per credit — storage + compute bundled. Can be expensive at scale. Easier to predict. |
| **Vendor lock-in** | Low. Delta Lake is open source. If you stop using Databricks, your Parquet files on ADLS are still readable by Spark, Athena, BigQuery, etc. | High. Data inside Snowflake can be unloaded but the format is proprietary. |
| **Best for** | Large-scale ETL, ML, mixed workloads (SQL + Python + streaming). Teams with data engineers who write code. | Pure SQL analytics, BI reporting, business analysts. Teams that want a managed warehouse without writing Spark code. |
| **In our EV project** | Ingesting CSV from blob, transforming with PySpark, writing Delta tables — ideal Databricks use case. | Could handle the SQL analytics layer (Gold) but not the ingestion or ML components without additional tools. |

**When to choose Snowflake over Databricks:** If your team is 100% SQL-focused, you have no ML requirements, and you want zero infrastructure management — Snowflake is simpler to operate. It handles query optimisation, storage, and warehousing automatically without you managing VMs or clusters.

**When to choose Databricks over Snowflake:** When you need Python + SQL together, when you're building ML models on the same data, when you have streaming workloads, or when you need to keep costs low at large scale (ADLS storage is cheaper than Snowflake storage).

---

### Databricks vs Azure Synapse Analytics

Azure Synapse is Microsoft's answer to the same space — a hybrid warehouse + Spark platform.

| Dimension | Databricks | Azure Synapse Analytics |
|---|---|---|
| **Spark engine** | Optimised Databricks Runtime (DBR) — proprietary enhancements on top of open-source Spark. Photon engine for vectorised execution. | Standard open-source Spark via "Spark pools". No Photon. Generally slower for complex Spark workloads. |
| **SQL engine** | Databricks SQL Warehouse — serverless SQL on Delta tables. | Dedicated SQL Pool (formerly SQL DW) — provisioned T-SQL warehouse. Serverless SQL Pool for ad-hoc queries on ADLS files. |
| **Integration with Azure** | Works well but is a third-party product — some extra setup (Access Connectors, linked services). | Native to Azure — shares the same portal, IAM roles, no extra auth layers for ADLS access. |
| **Unity Catalog** | Full Unity Catalog — metastore, external locations, column/row security, lineage. | Purview + Synapse access controls — separate governance system, not unified with Spark. |
| **Delta Lake** | Delta Lake is Databricks' core format — fully optimised. | Synapse supports Delta Lake but it is not the primary format. Parquet and dedicated SQL pools are more common. |
| **Cost** | DBU cost on top of Azure VM cost. For heavy Spark workloads, Databricks is often cheaper because its optimised runtime does more per CPU. | Dedicated SQL Pool is expensive per DWU (billed hourly even when idle unless paused). Serverless is cheaper. |
| **MLflow / ML** | MLflow built into Databricks. Model serving, experiment tracking, feature store native. | Azure ML is the Microsoft ML platform — separate service, separate UI. Not integrated into Synapse. |
| **Best for** | Teams who want the best Spark performance, Unity Catalog governance, and ML capabilities — and are willing to pay the DBU premium. | Teams already deep in the Microsoft ecosystem who want minimum friction with Azure IAM, Azure DevOps, Power BI — and whose workloads are SQL-heavy. |
| **In our EV project** | We use Databricks — it gives us the best Spark runtime for the ingestion jobs, Unity Catalog for governance, and the path to ML (charging pattern prediction, anomaly detection) later in the project. | Synapse would have been a viable alternative for the SQL and ingestion parts, but ML integration is weaker. |

---

### Databricks vs Google BigQuery

BigQuery is Google Cloud's managed data warehouse.

| Dimension | Databricks | BigQuery |
|---|---|---|
| **Cloud** | Any cloud (Azure, AWS, GCP). Multi-cloud with Unity Catalog. | Google Cloud only (though BigQuery Omni extends to S3/Azure via Anthos — limited). |
| **Storage** | Your ADLS/S3/GCS. Open Delta Lake format. | Google's Colossus storage. Data loaded into BigQuery is stored in Google's proprietary Capacitor format. |
| **Compute** | You manage clusters (or use serverless SQL Warehouse). | Fully serverless — no clusters, no provisioning. BigQuery auto-scales invisibly. |
| **SQL dialect** | ANSI SQL + Spark SQL extensions | Standard SQL (BigQuery dialect) — some differences from ANSI |
| **Python / ML** | Native PySpark, MLflow, model serving | BigQuery ML for in-warehouse ML. Python via notebooks (Colab integration). Not as comprehensive as Databricks for complex ML. |
| **Streaming** | Spark Structured Streaming, Delta Live Tables | BigQuery Storage Write API for streaming inserts — very capable, but SQL-centric. |
| **Cost model** | Compute = VM cost + DBU. Storage = ADLS (cheap flat rate). | On-demand: $5/TB scanned. Flat-rate / reservations: capacity-based pricing. No separate compute cost — pay per query. |
| **Best for** | Code-heavy teams, ML use cases, open format, multi-cloud. | Fully serverless SQL analytics at scale with minimal ops overhead, especially in GCP environments. |

---

### One-Line Summary of When to Use Each

| Platform | Use it when... |
|---|---|
| **Databricks** | You write code (Python + SQL), need ML, have streaming data, want open storage format (Delta), and need governance at scale |
| **Snowflake** | Your team is SQL-only, you want zero infrastructure management, and you don't need ML or streaming on the same platform |
| **Azure Synapse** | You're already all-in on Azure, your workload is SQL-heavy, and you want native Azure IAM without extra setup |
| **BigQuery** | You're on Google Cloud and want serverless SQL with no cluster management |
| **Apache Spark on its own** | You want full control and will manage clusters yourself (no governance, no UI, no support) |

---

## What Makes Databricks Unique (The Lakehouse Idea)

Traditional architectures separated data lakes (cheap storage, no structure, no transactions) from data warehouses (expensive, structured, ACID transactions). You needed both — a lake for raw data and a warehouse for clean analytics.

```
OLD ARCHITECTURE (two systems, data copied between them):
  Raw data → Data Lake (S3/ADLS) → ETL job → Data Warehouse (Redshift/Synapse)
                ↑                                      ↑
          cheap, flexible,                    expensive, structured,
          no transactions                     ACID, queryable

DATABRICKS LAKEHOUSE (one system):
  Raw data → Bronze (Delta Lake on ADLS) → Silver (Delta) → Gold (Delta)
                                    ↑
                  Delta Lake gives you: ACID + schema + time travel + cheap storage
                  Same files work for: Python ML, SQL queries, streaming ingestion
```

Delta Lake collapses the two systems into one by adding a transaction log (`_delta_log/`) on top of Parquet files — giving you warehouse-grade reliability with data lake storage costs. This is the foundation of everything in our EV project.

---

## Full Architecture Diagram (Control Plane + Unity Catalog + Data Plane)

```
╔═════════════════════════════════════════════════════════════════════════════╗
║                       DATABRICKS CONTROL PLANE                             ║
║                 (Managed by Databricks Inc. — Azure West US)               ║
║                                                                             ║
║  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────────────┐   ║
║  │  Workspace   │  │  Jobs /      │  │       Databricks REST API      │   ║
║  │  UI (Web)    │  │  Workflows   │  └────────────────────────────────┘   ║
║  │              │  │  Scheduler   │                                        ║
║  └──────────────┘  └──────────────┘                                        ║
║                                                                             ║
║  ┌──────────────────────────────────────────────────────────────────────┐  ║
║  │  Cluster Manager — provisions, autoscales, terminates clusters       │  ║
║  └──────────────────────────────────────────────────────────────────────┘  ║
║                                                                             ║
║  ╔════════════════════════════════════════════════════════════════════╗     ║
║  ║              UNITY CATALOG METASTORE                              ║     ║
║  ║        (account-level — shared across all workspaces)             ║     ║
║  ║                                                                   ║     ║
║  ║  ┌──────────────────────────────────────────────────────────┐    ║     ║
║  ║  │  CATALOG: dbw_ev_intelligence_dev                        │    ║     ║
║  ║  │                                                          │    ║     ║
║  ║  │  ┌───────────────────────────────────────────────────┐  │    ║     ║
║  ║  │  │  SCHEMA: default                                  │  │    ║     ║
║  ║  │  │                                                   │  │    ║     ║
║  ║  │  │  ┌──────────────┐  ┌──────────────┐              │  │    ║     ║
║  ║  │  │  │    TABLE     │  │    VOLUME    │              │  │    ║     ║
║  ║  │  │  │pipeline_audit│  │bronze-volume │              │  │    ║     ║
║  ║  │  │  │  (managed)   │  │  (external)  │              │  │    ║     ║
║  ║  │  │  └──────────────┘  └──────┬───────┘              │  │    ║     ║
║  ║  │  └─────────────────────────── │ ────────────────────┘  │    ║     ║
║  ║  └───────────────────────────────│──────────────────────────┘    ║     ║
║  ║                                  │ maps to                       ║     ║
║  ║  ┌─────────────────────────────────────────────────────────┐     ║     ║
║  ║  │  EXTERNAL LOCATIONS                                     │     ║     ║
║  ║  │  evdatalakedev-bronze → abfss://bronze@evdatalakedev... │     ║     ║
║  ║  │  evdatalakedev-silver → abfss://silver@evdatalakedev... │     ║     ║
║  ║  │  evdatalakedev-gold   → abfss://gold@evdatalakedev...   │     ║     ║
║  ║  └──────────────────────────────────┬──────────────────────┘     ║     ║
║  ║                                     │ uses                        ║     ║
║  ║  ┌──────────────────────────────────▼──────────────────────┐     ║     ║
║  ║  │  STORAGE CREDENTIAL                                     │     ║     ║
║  ║  │  cred-ev-intelligence-dev                               │     ║     ║
║  ║  │  → wraps Managed Identity of ac-ev-intelligence-dev     │     ║     ║
║  ║  └─────────────────────────────────────────────────────────┘     ║     ║
║  ╚════════════════════════════════════════════════════════════════════╝     ║
╚═════════════════════════════════════════════════════════════════════════════╝
                              │  secure channel (TLS)
                              │  UC enforces permissions on every data access
                              ▼
╔═════════════════════════════════════════════════════════════════════════════╗
║                         YOUR DATA PLANE                                    ║
║              (Runs inside YOUR Azure subscription — Central India)         ║
║                                                                             ║
║  ┌──────────────────────────────────────────────────────────────────────┐  ║
║  │                    Virtual Network (VNet)                            │  ║
║  │                                                                      │  ║
║  │   ┌─────────────────────────────────────────────────────────────┐   │  ║
║  │   │  Managed Resource Group (auto-created by Databricks)        │   │  ║
║  │   │                                                             │   │  ║
║  │   │  ┌──────────────────┐    ┌──────────────────┐              │   │  ║
║  │   │  │   Driver Node    │    │  Worker Node(s)  │              │   │  ║
║  │   │  │  (Spark master)  │◄──►│  (Spark workers) │              │   │  ║
║  │   │  │   dev-cluster    │    │  (auto-scale)    │              │   │  ║
║  │   │  └──────────────────┘    └──────────────────┘              │   │  ║
║  │   └─────────────────────────────────────────────────────────────┘   │  ║
║  └──────────────────────────────────────────────────────────────────────┘  ║
║                                                                             ║
║  ┌──────────────────────────────────────────────────────────────────────┐  ║
║  │                    ADLS Gen2 — evdatalakedev                        │  ║
║  │                                                                      │  ║
║  │   bronze/  ── audit/pipeline_audit.csv                              │  ║
║  │           ── realtime/charging_sessions/YYYY/MM/DD/HH/*.csv         │  ║
║  │           ── api/payments/raw/YYYY/MM/DD/*.json                     │  ║
║  │                                                                      │  ║
║  │   silver/  ── (Delta tables — Day 5+)                               │  ║
║  │   gold/    ── (aggregated Delta tables — Day 6+)                    │  ║
║  └──────────────────────────────────────────────────────────────────────┘  ║
║                                                                             ║
║  ┌────────────────────────┐   ┌──────────────────────────────────────────┐ ║
║  │  Azure Key Vault       │   │  Access Connector (ac-ev-intelligence-dev)│ ║
║  │  kv-ev-intelligence-dev│   │  Managed Identity → has Storage Blob     │ ║
║  │  Secrets:              │   │  Data Contributor role on evdatalakedev  │ ║
║  │  source-sas-token      │   │  Used by UC Storage Credential           │ ║
║  │  source-storage-account│   └──────────────────────────────────────────┘ ║
║  └────────────────────────┘                                                 ║
║                                                                             ║
║  ┌──────────────────────────────────────────────────────────────────────┐  ║
║  │  Azure Data Factory — adf-ev-intelligence-dev                        │  ║
║  │  Pipelines: pl_bronze_api_payments_v3                                │  ║
║  │  Linked Services: ls_adls_bronze, ls_payments_api                    │  ║
║  └──────────────────────────────────────────────────────────────────────┘  ║
╚═════════════════════════════════════════════════════════════════════════════╝

Request flow example — notebook reads from Bronze Volume:
  1. Notebook: dbutils.fs.ls("/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/")
  2. Cluster → UC checks: does this user have READ VOLUME on bronze-volume?
  3. UC resolves External Location → evdatalakedev-bronze → abfss://bronze@evdatalakedev...
  4. UC uses Storage Credential → ac-ev-intelligence-dev Managed Identity
  5. Azure IAM confirms: identity has Storage Blob Data Contributor on evdatalakedev
  6. ADLS returns file list → notebook receives results
  (Your storage key is NEVER in the notebook or visible in any log)
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
