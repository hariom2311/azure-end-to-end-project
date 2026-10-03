# Day 2 — Azure Databricks: Workspace Setup, Clusters & Configuration

> **Goal:** Set up an Azure Databricks workspace from scratch, understand every configuration option, and learn how to create and manage clusters correctly.
> **Context:** In the VoltGrid project, the Databricks workspace is where all Bronze → Silver → Gold transformations run. Getting the workspace and cluster configuration right determines cost, performance, and security for the entire lakehouse.

---

## Part 1: Azure Databricks Account vs Workspace

Before touching any configuration, you must understand the two-level hierarchy.

```
Azure Databricks Account  (one per organisation)
  │
  ├── Workspace A  (dev)
  │     ├── Clusters
  │     ├── Notebooks
  │     ├── Jobs / Workflows
  │     └── Unity Catalog (shared via account)
  │
  ├── Workspace B  (staging)
  │
  └── Workspace C  (prod)
```

### Account Level
- The **Databricks Account** is the top-level entity — it is created when you first subscribe to Azure Databricks
- Managed at `https://accounts.azuredatabricks.net`
- Controls: Unity Catalog metastore, user/group management (via Azure AD sync), billing, account-level policies
- One account can have **many workspaces** across different regions and subscriptions

### Workspace Level
- A **Workspace** is the working environment — where you create clusters, notebooks, jobs
- Each workspace lives inside an Azure Resource Group and maps to a specific Azure region
- URL format: `https://adb-<workspace-id>.<region>.azuredatabricks.net`
- Controls: cluster policies, access modes, secrets, ADLS mounts

**In the VoltGrid project:**
- One workspace per environment: `dbw-ev-dev`, `dbw-ev-staging`, `dbw-ev-prod`
- All workspaces share the same Unity Catalog metastore (account-level)
- Dev workspace has relaxed cluster policies; prod workspace enforces cluster policies with auto-termination and max DBUs

---

## Part 2: Creating an Azure Databricks Workspace (Step by Step)

### Step 1 — Create via Azure Portal

1. Go to **portal.azure.com** → search **Azure Databricks** → **+ Create**
2. Fill in the **Basics** tab:

   | Field | Value | What it means |
   |---|---|---|
   | Subscription | your Azure subscription | Billing target |
   | Resource Group | `rg-ev-dev` | Logical container for this workspace |
   | Workspace name | `dbw-ev-dev` | Name shown in Azure Portal and Databricks URL |
   | Region | Australia East (or your region) | Where the control plane and VMs run |
   | Pricing Tier | **Premium** | Required for Unity Catalog, RBAC, cluster policies |

   > Always choose **Premium** tier for production and any project using Unity Catalog. Standard tier does not support Unity Catalog or cluster policies.

3. **Networking** tab:
   - **Deploy Azure Databricks workspace in your own VNet:** Yes (for production) / No (for dev)
   - For dev: leave defaults — Databricks creates and manages its own VNet
   - For prod: use VNet injection — see Part 6

4. **Review + Create** → **Create**
   - Deployment takes ~3–5 minutes
   - Creates: Databricks workspace, managed resource group (`databricks-rg-<name>`), VNet, NSG, Storage Account (for DBFS)

### Step 2 — Open the Workspace

1. Portal → **Resource Groups** → `rg-ev-dev` → `dbw-ev-dev` → **Launch Workspace**
2. Or navigate directly to the workspace URL
3. First time: Azure AD SSO — you log in with your Azure AD account

### Step 3 — Explore the Workspace UI

```
Left sidebar
├── Home          → your personal folder (notebooks you created)
├── Workspace     → shared folder tree (all users, all notebooks)
├── Repos         → Git-connected code repositories
├── Data (Catalog)→ Unity Catalog: catalogs, schemas, tables, volumes
├── Compute       → All clusters and SQL warehouses
│     ├── All-purpose clusters
│     ├── Job clusters (created by workflows, visible here)
│     └── SQL Warehouses
├── Workflows     → Databricks Jobs and Pipelines (DLT)
├── Delta Live Tables → streaming pipelines
├── Marketplace   → partner integrations (dbt, Fivetran, etc.)
└── Settings      → workspace admin settings
```

---

## Part 3: Cluster Types — Which One to Use

A cluster is the set of virtual machines that run your Spark code. Choosing the wrong type wastes money or causes failures.

```
Cluster Types in Azure Databricks
├── All-Purpose Cluster
│     Used for: interactive notebooks, development, exploration
│     Started by: manually by a user
│     Billed when: running (even if idle)
│     Shared by: multiple users simultaneously
│
├── Job Cluster
│     Used for: scheduled workflows, ADF Databricks Activity
│     Started by: a Databricks Job or ADF pipeline — automatically
│     Terminated by: automatically when job finishes
│     Billed for: only the duration of the job
│     NOT visible in Compute tab (only in job run history)
│
└── SQL Warehouse (formerly SQL Analytics)
      Used for: SQL queries, BI dashboards, Databricks SQL
      Technology: Photon engine (vectorised, not Spark)
      Billed by: DBU/hour, scales automatically
      Used with: Databricks SQL editor, Power BI connector
```

### All-Purpose vs Job Cluster — The Key Decision

| | All-Purpose Cluster | Job Cluster |
|---|---|---|
| Startup time | Already running (if not auto-terminated) | 3–5 min cold start per job |
| Cost | Expensive if left running | Cost-efficient — billed per job |
| Use for | Development, notebooks, exploration | Production scheduled jobs |
| Multi-user | Yes — notebook sharing | No — one job only |
| ADF integration | Works but wasteful | Recommended for ADF Notebook Activity |
| Auto-termination | Configurable | Always terminates on job completion |

**VoltGrid pattern:**
- Dev: one shared all-purpose cluster for notebook development
- Prod: job clusters created per pipeline run via ADF or Databricks Workflows

---

## Part 4: Cluster Configuration — Every Setting Explained

When you click **Compute** → **Create cluster**, you see this configuration form:

### 4.1 Cluster Name
Name your cluster clearly. Convention: `<team>-<purpose>-<env>`
- `voltgrid-dev-shared` — shared all-purpose for development
- `voltgrid-silver-job` — job cluster for Silver transform

### 4.2 Cluster Mode / Access Mode

This is the most important security setting:

```
Access Modes (Unity Catalog era)
├── Single User
│     One user only — full isolation
│     Supports: Python, SQL, Scala, R
│     Required for: ML workloads with certain libraries
│     Unity Catalog: full support
│
├── Shared
│     Multiple users share the cluster simultaneously
│     Supports: Python, SQL only (no Scala/R)
│     Unity Catalog: full support
│     Best for: cost-efficient shared dev clusters
│
└── No Isolation Shared  (legacy, avoid in new setups)
      All users, no isolation between sessions
      Unity Catalog: NOT supported
      Avoid for new workspaces
```

**Choosing access mode:**
- Development (team shares a cluster) → **Shared** access mode
- Production job runs → **Single User** access mode (service principal)
- ML notebooks with custom libraries → **Single User** access mode

### 4.3 Databricks Runtime Version

The runtime includes: Apache Spark + Delta Lake + Python + pre-installed libraries

```
Runtime naming: DBR X.Y (LTS)

Example: DBR 15.4 LTS
  Apache Spark: 3.5.0
  Scala: 2.12
  Python: 3.11
  Delta Lake: 3.2.0

LTS = Long Term Support — stable, patched for ~2 years
      Always use LTS for production workloads

ML Runtime: DBR 15.4 LTS ML
  Everything in standard DBR +
  MLflow 2.x, TensorFlow, PyTorch, scikit-learn, XGBoost
  Use only when you need ML libraries — larger image, slower start
```

**Runtime selection guide:**

| Use case | Recommended runtime |
|---|---|
| Data engineering (Bronze→Silver→Gold) | Latest LTS (e.g. DBR 15.4 LTS) |
| Machine learning / model training | DBR 15.4 LTS ML |
| Delta Live Tables pipelines | DLT runtime (auto-selected) |
| Legacy workloads | Match the runtime used when originally developed |

**VoltGrid project:** Use DBR 15.4 LTS (or latest LTS at time of setup). Upgrade runtime during maintenance windows, not during active development.

### 4.4 Node Type (VM Size)

Each worker and driver node is an Azure VM. You pick the VM size:

```
VM Family categories in Databricks:
├── Memory-Optimised  → Standard_E series (E4s_v3, E8s_v3, E16s_v3)
│     Best for: large joins, shuffles, wide DataFrames in memory
│     VoltGrid Silver layer: joins payments + sessions + customers
│
├── Compute-Optimised → Standard_F series (F4s_v2, F8s_v2)
│     Best for: CPU-intensive transformations, parsing, UDFs
│
├── General Purpose   → Standard_D series (D4s_v3, D8s_v3, D16s_v3)
│     Best for: balanced workloads, development, small-medium data
│     VoltGrid dev cluster: D4s_v3 (4 vCPU, 16 GB RAM)
│
└── Storage-Optimised → Standard_L series
      Best for: very large shuffle spill to local disk
```

**Choosing node type for VoltGrid:**
- Dev cluster: `Standard_D4s_v3` (4 vCPU / 16 GB RAM) — cost-effective
- Silver transform: `Standard_E8ds_v4` (8 vCPU / 64 GB RAM) — memory for joins
- Gold aggregations: `Standard_D8s_v3` (8 vCPU / 32 GB RAM) — balanced

### 4.5 Driver Node vs Worker Nodes

```
Cluster = 1 Driver + N Workers

Driver Node:
  - Runs the SparkContext and your notebook/job code
  - Coordinates task distribution to workers
  - Collects results
  - Should be same size as or larger than workers for complex DAGs

Worker Nodes:
  - Execute the actual Spark tasks in parallel
  - Each worker runs multiple executor threads
  - More workers = more parallelism = faster processing of large data
```

### 4.6 Cluster Size — Fixed vs Auto-Scaling

**Fixed size:**
```
Min workers = Max workers = N
  Always runs exactly N workers
  Predictable cost
  Good when: data size is consistent and known
```

**Auto-scaling:**
```
Min workers: 2   Max workers: 8
  Databricks monitors task queue and CPU
  Scales UP when tasks are queued (adds workers)
  Scales DOWN when workers are idle (removes workers)
  Good when: data volume varies, want to minimise cost

Setting:
  ☑ Enable autoscaling
  Min workers: 2
  Max workers: 8
```

**VoltGrid recommendation:**
- Dev: fixed 1–2 workers (cost control during development)
- Prod Silver job: auto-scale 2–8 workers (payment data volume varies by day)

### 4.7 Auto-Termination

Automatically stops the cluster after N minutes of inactivity (no running commands or notebook cells).

```
Setting: Terminate after __ minutes of inactivity
Recommended values:
  Dev cluster:  30–60 minutes  (stops when nobody is working)
  Prod cluster: Managed by job — terminates on job completion
```

**Critical:** Always set auto-termination on all-purpose clusters. A cluster with no auto-termination left running overnight costs money for zero work done.

### 4.8 Spot Instances (Azure Spot VMs)

Spot instances use Azure's spare compute capacity at 60–90% discount. They can be evicted with 30-second notice if Azure needs the capacity back.

```
Spot configuration:
  ☑ Use spot instances for workers
  ☐ Use spot instances for driver  (never use spot for driver — eviction kills the job)

On-demand vs Spot cost example (Standard_E8ds_v4):
  On-demand:  ~$0.50/hour per node
  Spot:       ~$0.10–0.20/hour per node
```

**When to use spot:**
- Batch jobs where retry-on-eviction is acceptable (idempotent jobs with checkpointing)
- Dev clusters (losing a cluster is inconvenient, not catastrophic)

**When NOT to use spot:**
- Jobs with strict SLAs (payment data must be ready by 6am)
- Long-running jobs without checkpointing (eviction loses all progress)

### 4.9 Photon Acceleration

Photon is Databricks' vectorised query engine — a native C++ implementation of Spark SQL operations. It runs alongside Spark and speeds up SQL queries and DataFrame operations automatically.

```
☑ Enable Photon Acceleration

Speedup: 2–8x faster on SQL-heavy workloads
Cost: ~10% additional DBU cost
When to enable: always for data engineering and SQL workloads
When to skip: pure Python UDF workloads (Photon doesn't run Python UDFs)
```

---

## Part 5: Databricks Runtime Unit (DBU) — Cost Model

DBU (Databricks Runtime Unit) is how Databricks charges for compute — on top of the Azure VM cost.

```
Total cost = Azure VM cost + Databricks DBU cost

DBU rate depends on:
  - Cluster type (All-Purpose > Job > SQL Warehouse)
  - Tier (Premium > Standard)
  - Runtime (ML runtime costs more DBUs)
  - Region

Example (approximate):
  All-Purpose cluster, Premium tier:
    Standard_D4s_v3 = 0.75 DBU/hour per node
    If 4 nodes → 3 DBU/hour
    At $0.55/DBU → ~$1.65/hour in DBUs
    Plus Azure VM cost: ~$0.80/hour
    Total: ~$2.45/hour for a 4-node cluster

  Job cluster, Premium tier:
    Same VM: 0.375 DBU/hour per node (half the DBU rate of All-Purpose)
    → Production jobs cost ~half the DBU rate of dev clusters
```

**Key insight:** Always use Job clusters for production scheduled work. The DBU rate for Job clusters is roughly half the All-Purpose rate for identical VM sizes.

---

## Part 6: Instance Pools — Pre-Warmed VMs

Instance pools keep a set of Azure VMs running and idle, ready to be attached to a cluster. When a cluster starts, it draws VMs from the pool instead of provisioning new ones from scratch.

```
Without pool:                  With pool:
  Job starts                     Job starts
  → Wait for Azure VM            → VMs already running in pool
    provisioning (~3–5 min)      → Cluster ready in ~30–60 seconds
  → Spark runtime init
  → Job runs
```

### Creating an Instance Pool (Step by Step)

1. **Compute** → **Pools** tab → **Create pool**
2. Configure:

   | Setting | Value | Purpose |
   |---|---|---|
   | Pool name | `voltgrid-pool-dev` | Descriptive name |
   | Min idle instances | `2` | Always keep 2 VMs warm |
   | Max capacity | `10` | Never exceed 10 VMs total |
   | Idle instance auto-termination | `60 minutes` | Release idle VMs after 1 hour |
   | Node type | `Standard_D4s_v3` | VM size for pool VMs |
   | Databricks runtime | Latest LTS | Pre-installed runtime |

3. **Create** → pool starts warming up immediately

### Using a Pool in a Cluster

1. **Create cluster** → **Node type** section → **Instance pool** → select `voltgrid-pool-dev`
2. The cluster now draws VMs from the pool — cold start becomes warm start (~30s vs 3–5 min)

**Pool economics:**
- Min idle instances (2) are billed at the Azure VM rate even when idle — no DBUs for idle pool instances
- When a cluster uses pool instances, normal DBU billing resumes
- Net saving: fast cluster starts without paying DBUs for idle time

---

## Part 7: Cluster Policies — Enforcing Standards

Cluster policies are admin-defined rules that restrict what users can configure when creating a cluster. They prevent cost overruns and enforce security standards.

```
Without policy:                With policy:
  Any user can create:           Policy enforces:
  - 50-node cluster              - Max 8 workers
  - No auto-termination          - Auto-terminate after 60 min
  - Spot: off (expensive)        - Spot: on for workers
  - Any VM size                  - Only D4s or E8s
  - Any runtime                  - Only LTS runtimes
```

### Creating a Cluster Policy (Admin only)

1. **Settings** (gear icon) → **Compute** → **Cluster Policies** → **Create policy**
2. Name: `voltgrid-dev-policy`
3. Policy JSON — example for VoltGrid dev:

```json
{
  "autotermination_minutes": {
    "type": "fixed",
    "value": 60,
    "hidden": false
  },
  "node_type_id": {
    "type": "allowlist",
    "values": ["Standard_D4s_v3", "Standard_D8s_v3", "Standard_E8ds_v4"],
    "defaultValue": "Standard_D4s_v3"
  },
  "spark_version": {
    "type": "regex",
    "pattern": ".*-scala2.12",
    "defaultValue": "15.4.x-scala2.12"
  },
  "num_workers": {
    "type": "range",
    "minValue": 1,
    "maxValue": 8
  },
  "azure_use_spot_instances": {
    "type": "fixed",
    "value": "true",
    "hidden": false
  },
  "custom_tags.team": {
    "type": "fixed",
    "value": "voltgrid",
    "hidden": false
  }
}
```

4. **Assign** the policy to users/groups under **Permissions**

**Policy attribute types:**

| Type | Meaning |
|---|---|
| `fixed` | Value is locked — user cannot change it |
| `allowlist` | User picks from a list of allowed values |
| `range` | User can set a value within min/max bounds |
| `regex` | Value must match the pattern |
| `forbidden` | Setting is completely hidden from the user |

---

## Part 8: Workspace Configuration — Admin Settings

### 8.1 Admin Console

1. **Settings** → **Admin Console** (or **Settings** → **Workspace settings**)
2. Key settings:

   | Setting | Recommended | Why |
   |---|---|---|
   | Allow cluster creation | Admins only in prod; all in dev | Prevent uncontrolled cluster sprawl |
   | Allow public clusters | Off in prod | Security — clusters must use private endpoints |
   | Enable enhanced security monitoring | On | Audit log all access events |
   | Unity Catalog | On | Centralised governance |
   | DBFS browser | Off in prod | Security — direct DBFS access bypasses Unity Catalog |

### 8.2 Git Integration (Repos)

Connect Databricks Repos to your Git provider so notebooks are version-controlled.

1. **Settings** → **Linked accounts** → **Git provider** → **Azure DevOps Services** or **GitHub**
2. Generate a personal access token (PAT) in your Git provider
3. Enter the PAT in Databricks → **Save**

Now in **Repos** → **+ Add Repo** → paste your repo URL → Databricks clones it.

```
Repo structure (VoltGrid):
  azure-ev-end-to-end-project/
  ├── databricks/
  │   ├── bronze/
  │   │   └── ingest_payments.py
  │   ├── silver/
  │   │   └── transform_payments.py
  │   └── gold/
  │       └── agg_revenue.py
```

### 8.3 Secrets — Databricks Secret Scope

Instead of hardcoding credentials in notebooks, store them in secret scopes.

**Two types of secret scope:**

```
Type 1: Databricks-managed scope
  Secrets stored in Databricks' encrypted store
  Created via CLI: databricks secrets create-scope --scope voltgrid

Type 2: Azure Key Vault-backed scope (recommended)
  Secrets stored in Azure Key Vault
  Databricks reads them via the scope at runtime
  Created in: https://<workspace-url>#secrets/createScope
```

**Create an AKV-backed secret scope (step by step):**

1. Navigate to: `https://<your-workspace-url>#secrets/createScope`
2. Fill in:
   - **Scope name:** `voltgrid-kv`
   - **Manage Principal:** `All Users` (for dev) or `Creators` (for prod)
   - **DNS Name:** `https://key-vault-session-ded.vault.azure.net/`
   - **Resource ID:** (from Key Vault Properties → Resource ID)
3. Click **Create**

**Use in a notebook:**
```python
# In Databricks notebook — never prints or logs the value
username = dbutils.secrets.get(scope="voltgrid-kv", key="voltgrid-username")
password = dbutils.secrets.get(scope="voltgrid-kv", key="voltgrid-password")
```

### 8.4 ADLS Gen2 Access — Service Principal Mount

To read/write ADLS Gen2 from Databricks notebooks, configure access.

**Option A: OAuth with Service Principal (recommended)**

```python
# Set Spark config for ADLS Gen2 access
spark.conf.set(
    "fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    "fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    "fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net",
    dbutils.secrets.get(scope="voltgrid-kv", key="sp-client-id")
)
spark.conf.set(
    "fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net",
    dbutils.secrets.get(scope="voltgrid-kv", key="sp-client-secret")
)
spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net",
    "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
)

# Read Bronze payments
df = spark.read.json("abfss://bronze@<storage-account>.dfs.core.windows.net/api/payments/")
df.show(5)
```

**Option B: Unity Catalog (recommended for new workspaces)**
- Configure Unity Catalog external location pointing to ADLS
- Grant `READ FILES` / `WRITE FILES` on the external location
- Access via `spark.read.format("delta").load("abfss://...")` — no manual Spark config needed

---

## Part 9: Cluster Lifecycle — States

```
Cluster states:
  PENDING    → VM is being provisioned (3–5 min cold, ~30s from pool)
  RUNNING    → Cluster is up and accepting commands
  TERMINATING→ Shutdown in progress (saves logs, releases VMs)
  TERMINATED → Fully stopped — no cost (except pool idle VMs)
  ERROR      → Failed to start — check event log for reason

Transitions:
  Create cluster → PENDING → RUNNING
  Auto-terminate → RUNNING → TERMINATING → TERMINATED
  User restarts  → TERMINATED → PENDING → RUNNING
```

**Cluster event log:** Click any cluster → **Event log** tab → see every start, stop, scale, error with timestamps. Essential for debugging unexpected terminations.

---

## Part 10: Full Cluster Creation — Step by Step (VoltGrid Dev)

Here is the complete sequence to create the VoltGrid shared development cluster:

### Step 1 — Navigate to Compute

1. Workspace → left sidebar → **Compute** → **+ Create compute**

### Step 2 — Basic Settings

| Field | Value |
|---|---|
| Cluster name | `voltgrid-dev-shared` |
| Policy | `voltgrid-dev-policy` (if created) or Unrestricted |
| Access mode | `Shared` |
| Databricks runtime | `15.4 LTS` (or latest LTS) |

### Step 3 — Auto-Scaling

| Field | Value |
|---|---|
| Enable autoscaling | ☑ checked |
| Min workers | `1` |
| Max workers | `4` |
| Worker type | `Standard_D4s_v3` |
| Driver type | `Standard_D4s_v3` (same as worker) |

### Step 4 — Advanced Options

1. Click **Advanced options** to expand
2. **Spark** tab:
   - Spark config (add to text box):
     ```
     spark.databricks.delta.preview.enabled true
     spark.sql.adaptive.enabled true
     spark.sql.adaptive.coalescePartitions.enabled true
     ```
3. **Environment variables** tab:
   - `ENVIRONMENT=dev`
4. **Tags** tab — add:
   - `team` = `voltgrid`
   - `environment` = `dev`
   - `cost-centre` = `data-engineering`

### Step 5 — Auto-Termination

| Field | Value |
|---|---|
| Terminate after | `60` minutes of inactivity |

### Step 6 — Photon

| Field | Value |
|---|---|
| Enable Photon acceleration | ☑ checked |

### Step 7 — Create

Click **Create compute** → cluster enters PENDING state → wait 3–5 minutes → RUNNING.

### Step 8 — Verify

1. Click the cluster name → **Configuration** tab → confirm all settings
2. Click **Event log** tab → should show `Cluster launched` event
3. Open any notebook → attach to `voltgrid-dev-shared` → run `spark.version` → confirms Spark is running

---

## Quick Reference — All Day 2 Terminologies

```
Term                   Definition
──────────────────────────────────────────────────────────────────────────
Account                Top-level Databricks entity — manages workspaces,
                       Unity Catalog, users across an organisation

Workspace              A Databricks working environment — notebooks,
                       clusters, jobs for one team/environment

DBU                    Databricks Runtime Unit — billing unit on top of VM cost

All-Purpose Cluster    Long-running interactive cluster shared by users

Job Cluster            Single-job cluster, auto-terminates — used in production

SQL Warehouse          Photon-powered cluster for SQL-only workloads / BI

Access Mode            Security isolation: Single User / Shared / No Isolation

Databricks Runtime     Versioned bundle: Spark + Delta + Python + libraries

LTS                    Long Term Support — stable runtime, use in production

DBR                    Databricks Runtime (version prefix e.g. DBR 15.4)

Driver Node            The VM that runs your code and coordinates Spark

Worker Node            VMs that execute Spark tasks in parallel

Executor               JVM process on a worker node that runs Spark tasks

Instance Pool          Pre-warmed VMs — reduces cluster cold-start time

Cluster Policy         Admin rules restricting cluster configuration

Auto-Termination       Automatic cluster shutdown after N idle minutes

Auto-Scaling           Dynamic worker count based on load (min → max)

Photon                 Databricks vectorised query engine — faster SQL/DF ops

Spot Instance          Discounted Azure VM — can be evicted with 30s notice

Secret Scope           Named collection of secrets — Databricks or AKV-backed

dbutils.secrets        Python API to read secrets without printing values

DBFS                   Databricks File System — legacy distributed FS on Blob Storage

ADLS Gen2              Azure Data Lake Storage — recommended external storage

abfss://               URI scheme for ADLS Gen2 access from Databricks

Unity Catalog          Centralised governance layer — tables, schemas, users

Repo                   Git-connected folder in workspace for versioned notebooks

Cluster Mode           Legacy term for Access Mode (pre-Unity Catalog workspaces)

Spark Config           Key-value settings passed to the SparkSession at startup

Tags                   Azure/Databricks metadata — used for cost allocation
```
