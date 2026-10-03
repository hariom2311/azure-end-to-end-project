# Day 2 — Interview Solutions: Workspace Setup & Clusters

---

## Workspace & Account

**A1**
A **Databricks Account** is the top-level entity — it manages workspaces, Unity Catalog, users, and billing across an organisation. One account maps to one organisation and is accessed at `https://accounts.azuredatabricks.net`.

A **Workspace** is a working environment within an account — it contains clusters, notebooks, and jobs for a specific team or environment. Each workspace has its own URL (`https://adb-<id>.<region>.azuredatabricks.net`).

One account can have **many workspaces** (dev, staging, prod, team-specific). A workspace belongs to **exactly one account** — there is no shared-workspace-across-accounts concept.

---

**A2**
Recommended: **separate workspaces per environment, not per team**.

```
Account
├── dbw-ev-dev      (all teams share this for development)
├── dbw-ev-staging  (pre-production validation)
└── dbw-ev-prod     (production only — restricted access)
```

Within each workspace, teams are separated by Unity Catalog (catalog-level isolation) and cluster policies (each team gets its own policy). This is better than per-team workspaces because:
- Unity Catalog (shared via account) can govern cross-team data access consistently
- Fewer workspaces = lower admin overhead
- Staging catches issues before they hit prod regardless of which team introduced them

---

**A3**
Unity Catalog requires **Premium pricing tier**. Standard tier does not support Unity Catalog, cluster policies, or table-level access control.

Fix: The workspace pricing tier cannot be changed after creation — you must create a new workspace with Premium tier and migrate notebooks and data references. To avoid this, always deploy new workspaces with Premium tier from the start.

---

**A4**
The engineer is partially wrong. Disabling DBFS browser prevents users from **browsing DBFS through the Databricks UI** (the Data → DBFS tab in older workspaces). It does NOT prevent programmatic DBFS access from notebooks (`dbutils.fs.ls("dbfs:/...")` still works).

What disabling DBFS browser actually achieves: it removes the visual browser that some users rely on to navigate raw DBFS paths, encouraging them to use Unity Catalog instead. In production, this is a good practice because direct DBFS access bypasses Unity Catalog's fine-grained access controls and auditing.

---

## Cluster Types

**A5**
- **All-Purpose cluster:** Long-running, shared by multiple users for interactive notebook development — started manually
- **Job cluster:** Created automatically by a scheduled Databricks Job or ADF pipeline, runs one job, terminates on completion — used in production
- **SQL Warehouse:** Photon-powered, SQL-only compute for BI dashboards and Databricks SQL editor — billed by DBU/hour with auto-scaling

---

**A6**
Use a **Job cluster**. Reasons:
- The job runs on a fixed schedule — no human interaction needed during the run
- Job clusters auto-terminate when the job finishes — no idle cost
- Job cluster DBU rate is ~half the All-Purpose rate for the same VM size
- The job is isolated — no contention from other users

After the job finishes, the cluster **automatically terminates**. You can verify this in Databricks Workflows → Job run history → Cluster column shows the cluster ID, and the cluster is in TERMINATED state in Compute.

---

**A7**
For identical VM sizes, Job clusters are billed at approximately **half the DBU rate** of All-Purpose clusters (e.g., 0.375 DBU/hour per node vs 0.75 DBU/hour per node for the same `Standard_D4s_v3`).

Databricks charges more for All-Purpose because they are designed for interactive, multi-user use — the control plane handles concurrent notebook sessions, real-time output streaming, and collaborative features. Job clusters are single-tenant, single-purpose, and do not carry the overhead of interactive session management.

---

**A8**
Most likely cause: **resource contention** — other teams are running notebooks on the same All-Purpose cluster during the day. When the nightly job starts at 2am, it may share the cluster with late-running notebooks or early-morning jobs from other teams, causing Spark task scheduling delays and executor CPU contention.

Fix: Use a **dedicated Job cluster** for the nightly pipeline. The cluster is created fresh for each run (or uses a pool), runs without sharing, and terminates on completion. This eliminates contention and makes run times deterministic.

---

## Cluster Configuration

**A9**
- **Single User:** One user (or service principal) only. Full isolation — their Spark session is exclusive. Supports Python, SQL, Scala, and R. Required for ML workloads with certain native libraries.
- **Shared:** Multiple users share the cluster simultaneously. Each user gets an isolated Python REPL but shares the underlying JVM and executor memory. Supports Python and SQL only — **Scala and R are not supported** in Shared mode.

**Scala** requires Single User access mode because Shared mode prevents arbitrary JVM code execution to maintain isolation between users on the same cluster.

---

**A10**
Choose **Databricks Runtime ML** (e.g., `15.4 LTS ML`). This runtime includes:
- PyTorch, TensorFlow, Keras
- scikit-learn, XGBoost, LightGBM
- MLflow (pre-configured to auto-log experiments)
- CUDA libraries for GPU-enabled VM types

Using the standard DBR would require manually installing these ML libraries on every cluster start, which is error-prone and slower. The ML runtime pre-bundles them with tested, compatible versions.

---

**A11**
Photon is Databricks' vectorised query engine — written in C++ — that runs SQL and DataFrame operations faster than the standard JVM-based Spark engine. It processes data column-by-column (vectorised) rather than row-by-row, enabling better CPU cache utilisation.

**Photon accelerates:**
- SQL queries (`SELECT`, `JOIN`, `GROUP BY`, `ORDER BY`, window functions)
- DataFrame operations that compile to Spark SQL (`.filter()`, `.groupBy()`, `.join()`)
- Delta Lake reads and writes

**Photon does NOT accelerate:**
- Python UDFs (User-Defined Functions) — these run in the Python interpreter, not in Photon's engine
- pandas operations on the driver node
- RDD operations (low-level Spark API)

---

**A12**
Two options:

**Option 1 — Add more workers:**
Scale from 4 to 8 workers. Each worker handles half the tasks → roughly 2x speedup → ~10 minutes. Trade-off: doubles the VM and DBU cost for the job duration.

**Option 2 — Switch to a larger VM with more memory:**
Upgrade from `Standard_E8ds_v4` (8 vCPU / 64 GB) to `Standard_E16ds_v4` (16 vCPU / 128 GB). More cores per node = more parallel tasks. Trade-off: higher cost per node, but fewer nodes needed → potentially better Spark shuffle efficiency.

The right choice depends on the bottleneck: if the job is CPU-bound, more cores help. If it's shuffle/join-bound, more memory per node reduces spill to disk.

---

**A13**
Autoscaling behaviour:

```
0–5 min:   Heavy load → Databricks detects queued tasks → scales up
           Workers grow from 2 → 6 → 10 (max) over ~2–3 minutes
5–30 min:  Light load → workers are idle
           Databricks scales down after idle timeout → back to 2 workers
```

Cost profile:
- First 5 minutes: billed for 10 workers (at max)
- Minutes 5–30: billed for 2 workers (min) as cluster scales down
- Much cheaper than running 10 workers for the full 30 minutes

Key insight: scale-down is not instant — there is a configurable idle timeout (default 2–3 minutes of idle per worker). So you pay for slightly more than 5 minutes at max workers, but far less than the fixed-size alternative.

---

**A14**
**Driver node:** Runs the SparkContext, your application code, and all the coordination logic. It distributes tasks to workers and collects results. Only one driver per cluster.

**Worker nodes:** Execute the actual Spark tasks (map, filter, join, shuffle). Many workers run in parallel.

If the driver is undersized:
- **Out of memory (OOM) on driver:** when you call `.collect()`, `.toPandas()`, or display large DataFrames — all data is pulled to the driver for these operations
- **Slow DAG planning:** complex queries with many stages take longer to plan
- **Notebook UI lag:** the driver also serves notebook output back to the browser — undersized driver causes slow cell output

Fix: make the driver the same size as or larger than workers, or avoid collecting large DataFrames to the driver (use `.write` instead of `.collect()`).

---

**A15**
**No, the cluster does not auto-terminate during an active cell run.** Auto-termination only triggers when the cluster is genuinely idle — meaning no notebook commands, no Spark jobs, no streaming queries are running.

A cell executing for 3 hours is actively running — Spark tasks are in progress. The auto-termination timer only starts counting after the last command finishes and the cluster has nothing to do.

---

**A16**
What happened: Azure reclaimed the Spot VM(s) — a Spark executor running on that VM was lost, causing a `SparkException: Lost executor`. Azure evicts Spot VMs with 30 seconds notice when it needs capacity.

Fix without abandoning Spot:
1. **Use Spot for workers only, On-Demand for the driver.** The driver is never on Spot — its eviction kills the entire job. Workers being evicted triggers Spark task retry (Spark automatically retries lost tasks up to 4 times by default).
2. **Enable Delta Lake checkpointing / idempotent writes.** Ensure the job writes using `MERGE` or `overwrite` mode so a retry from failure produces the same result. This makes the job resilient to executor loss.

With these two changes, Spot is safe for production batch jobs.

---

## Instance Pools

**A17**
An instance pool is a set of pre-provisioned, idle Azure VMs managed by Databricks. When a cluster needs to start, it draws VMs from the pool instead of provisioning new ones from Azure — reducing cold-start time from 3–5 minutes to 30–60 seconds.

Problem it solves: **cluster cold-start latency**. Job clusters that start and stop per run normally wait 3–5 minutes for Azure to provision VMs. With a pool of idle VMs, this wait is eliminated.

---

**A18**
The 3 additional VMs are provisioned **fresh from Azure** — standard VM provisioning (~3–5 minutes). The pool only guaranteed 2 idle VMs. When demand exceeds idle capacity, the pool requests additional VMs from Azure just like a normal cluster would.

Key point: the pool's `Max capacity` setting caps the total VMs the pool can manage at once. If the pool is at max capacity, the cluster creation fails or queues.

---

**A19**
Idle pool VMs are billed at **Azure VM rates only — no DBU charges**. DBU billing begins only when a cluster is actively using pool VMs (i.e., the cluster is in RUNNING state and has attached to those pool VMs).

This is the economic appeal of pools: you pay Azure VM prices (~$0.10–0.50/hour per VM) to keep VMs warm instead of paying the full DBU + VM rate for a running cluster with no work to do.

---

## Cluster Policies

**A20**
A cluster policy is a JSON configuration that workspace admins define to restrict and standardise how users create clusters. It can lock specific settings (like auto-termination), limit choices (like allowed VM types), or set required tags.

Only **workspace admins** can create and manage cluster policies. Regular users can only select and use policies that have been assigned to them.

---

**A21**
The user **cannot change auto-termination**. The `"type": "fixed"` attribute means the value is locked — the field appears greyed out in the cluster creation UI. Any attempt to change it programmatically via the API would also be rejected by the policy engine.

The user must use a different policy (if one exists with more flexibility) or ask the admin to update the policy.

---

**A22**
- `"type": "fixed"` — the setting is locked at the specified value but **visible** to the user (they can see it is 60 minutes, just can't change it). Use for: enforcing required settings that users should know about, like auto-termination.

- `"type": "forbidden"` — the setting is completely **hidden** from the user. They don't see it in the UI at all. Use for: advanced settings that users should never need to touch (e.g., internal Databricks configuration flags, or disabling public internet access completely without confusing users).

---

## Secrets & Security

**A23**
A Databricks secret scope is a named collection of secrets accessible from notebooks via `dbutils.secrets.get(scope, key)`. Values are never displayed in notebook output (Databricks masks them as `[REDACTED]`).

Two types:
- **Databricks-managed scope:** Secrets stored in Databricks' internal encrypted store. Simpler to set up but secrets live inside Databricks.
- **Azure Key Vault-backed scope (recommended):** Secrets stored in Azure Key Vault. Databricks reads them at runtime via the scope. All secret rotation, auditing, and access control happens in Key Vault — the standard enterprise approach.

AKV-backed is recommended because it centralises secrets in one place (Key Vault is also used by ADF, Azure Functions, and other services in the VoltGrid stack), and Key Vault audit logs capture every access.

---

**A24**
The notebook output shows `[REDACTED]` — not the actual password value.

Databricks' runtime intercepts any `print()` or display output that contains a secret value and replaces it with `[REDACTED]`. This prevents secrets from appearing in:
- Notebook cell output
- Databricks logs
- Exported notebooks

Even if the engineer assigns the secret to a variable and prints the variable, the output is redacted. The only way to "see" the secret value is to use it — pass it to an HTTP call, a Spark config, etc.

---

**A25**
The colleague is **partially right, but largely wrong**. When you set a value via `spark.conf.set(...)` using a secret:

1. `spark.conf.get("fs.azure.account.oauth2.client.secret.myaccount.dfs.core.windows.net")` **does** return the raw value — it is stored in the Spark config dictionary on the driver
2. However, this is accessible **only from within the same Spark session** — other users on a Shared cluster cannot read another user's Spark config
3. In notebook output, if someone were to `print(spark.conf.get(...))`, the value would appear as plaintext — so you should **never print Spark config values that contain secrets**

The correct fix: use Unity Catalog external locations (which handle credentials server-side) so the secret never needs to be in the Spark config at all. For workspaces still using Spark config, ensure cluster access mode is Single User so no other user shares the same SparkSession.

---

## ADLS Access & Configuration

**A26**
The URI scheme is `abfss://` (Azure Blob File System Secure — always use HTTPS).

Format: `abfss://<container>@<storage-account>.dfs.core.windows.net/<path>`

For the `bronze` container in `evdatalakedev`:
```
abfss://bronze@evdatalakedev.dfs.core.windows.net/
```

To read payments:
```
abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/
```

---

**A27**
Most likely cause: the **Service Principal does not have the required RBAC role on the new cluster's storage access path**.

The original cluster `voltgrid-dev-shared` may have had Spark config set at the cluster level (in Advanced → Spark config) giving it ADLS access. The new cluster has none of this config — `spark.conf.set(...)` calls are per-session and are not inherited from other clusters.

Fix: either add the same Spark config to the new cluster, or configure Unity Catalog external locations so all clusters in the workspace access ADLS through the same governed path without per-cluster Spark config.

---

**A28**
| | Spark config | Unity Catalog external location |
|---|---|---|
| Where credentials live | In Spark session (per cluster) | In Unity Catalog (account-level) |
| Setup required | Per cluster or per notebook | Once, at account level |
| Auditing | Not audited | Every read/write logged in Unity Catalog audit log |
| Access control | Anyone with cluster access | Fine-grained: catalog, schema, table-level grants |
| Credential rotation | Update Spark config in all clusters | Update the external location credential once |

**Unity Catalog external locations are better for production** because:
1. Credentials are managed centrally — rotation affects all clusters automatically
2. Access is governed by `GRANT` statements — you control who can read which tables/paths
3. All accesses are audited in Unity Catalog's system tables
4. No secrets ever appear in cluster config or notebook Spark config

---

## Mixed / Senior

**A29**
Complete VoltGrid Databricks setup:

```
Account: databricks-ev (Premium tier)
Unity Catalog metastore: ev-metastore (Australia East)
  Bound to all workspaces

Workspaces:
  dbw-ev-dev     (dev)     Premium, Australia East, VNet managed by Databricks
  dbw-ev-prod    (prod)    Premium, Australia East, VNet injection (private endpoints)

Clusters per workspace:

  DEV:
    Type: All-Purpose, Shared access mode
    Name: voltgrid-dev-shared
    VM: Standard_D4s_v3, autoscale 1-4, auto-terminate 60 min
    Pool: voltgrid-pool-dev (1 idle VM)
    Policy: voltgrid-dev-policy (enforces auto-terminate, max 4 workers, allowed VMs)

  PROD:
    Type: Job clusters (created per workflow run)
    Access mode: Single User (service principal: sp-voltgrid-prod)
    Silver VM: Standard_E8ds_v4, autoscale 2-8, spot workers
    Gold VM: Standard_D8s_v3, autoscale 2-4, spot workers
    Pool: voltgrid-pool-prod (2 idle VMs — one per job type)
    Policy: voltgrid-prod-policy (enforces single user, spot, mandatory tags)

Secret scope: voltgrid-kv (AKV-backed, pointing to key-vault-session-ded)
ADLS access: Unity Catalog external locations (not Spark config)
  External location: ev-bronze-loc → abfss://bronze@evdatalakedev.dfs.core.windows.net/
  External location: ev-silver-loc → abfss://silver@evdatalakedev.dfs.core.windows.net/
  Credential: sp-voltgrid-prod (service principal with Storage Blob Data Contributor on ADLS)
```

Justifications:
- Separate workspaces for dev/prod: isolate production from experimental dev work
- VNet injection in prod: private endpoints, no public internet traffic
- Shared access mode in dev: cost sharing, one cluster for whole team
- Single User in prod: isolation, no cross-job interference on job clusters
- Job clusters in prod: half DBU rate, auto-terminate, no idle cost
- Pools: eliminate cold-start latency for time-sensitive morning jobs
- AKV-backed secrets: centralised rotation, audit trail, reused across ADF + Databricks

---

**A30**
The behaviour differs because of **access mode isolation rules**:

In **Shared** access mode, Databricks enforces a security boundary between users sharing the same cluster. `%pip install` modifies the Python environment for the entire cluster — allowing one user to `pip install` would affect all other users' sessions and could introduce incompatible library versions or malicious packages. Databricks blocks `%pip install` for non-admin users in Shared mode for this reason.

In **Single User** mode, the user has exclusive control over the cluster — there are no other sessions to affect, so `%pip install` is unrestricted.

**Two ways to give the engineer access to `requests` on the shared cluster:**

1. **Install `requests` as a cluster library (admin action):** In the cluster configuration → **Libraries** tab → **Install** → PyPI → `requests`. This installs it at the cluster level for all users. Since `requests` is a standard, safe library, this is appropriate.

2. **Use init scripts (admin action):** Add a cluster init script that runs `pip install requests` at cluster startup — applies before any user connects. Used when libraries cannot be installed via the Libraries tab (e.g., custom `.whl` files or system-level packages).