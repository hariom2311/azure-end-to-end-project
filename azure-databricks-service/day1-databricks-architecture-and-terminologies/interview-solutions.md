# Day 1 — Interview Solutions: Databricks Architecture & Terminologies

---

## Architecture & Two-Plane Model

**A1**
The two-plane model divides Databricks into two layers:

**Control Plane** (Databricks-managed, Databricks' Azure subscription):
- Web UI, notebooks, REST API, job scheduler, cluster configuration storage
- Notebook content and job definitions are stored here

**Data Plane** (Customer-managed, your Azure subscription):
- The actual Spark cluster VMs (Driver + Worker Nodes)
- Your ADLS Gen2 storage, Azure SQL, and all data sources
- The virtual network (VNet) your clusters run inside

Security significance: your data never flows through the Control Plane. Databricks sends only orchestration commands (start cluster, submit code) through a secure channel. All data reads and writes happen between the cluster VMs in your Data Plane and your storage — entirely within your subscription. This satisfies most enterprise data residency requirements.

---

**A2**
The statement is inaccurate. When you delete an Azure Databricks Workspace, the following is deleted:
- All notebook content (stored in the Control Plane)
- All cluster configurations and job definitions
- The Azure Databricks resource itself

What is **not** deleted:
- Data in ADLS Gen2, Azure SQL, or any other data store (these are separate Azure resources)
- Delta tables stored in ADLS — they survive workspace deletion
- External tables' data files

This is because the Data Plane (your storage) is independent of the Databricks workspace. Only the Databricks metadata and notebooks are in Databricks-managed storage.

---

**A3**
Yes — this is exactly what the two-plane model enables. All data processing (Spark cluster VMs) and all data storage (ADLS Gen2) run in your Azure subscription (the Data Plane). Databricks in the Control Plane only sends orchestration commands — no data flows out of your subscription.

Additionally, you can use **VNet Injection** to deploy the Databricks cluster VMs into your own VNet, locking down all outbound traffic via network security groups and private endpoints. This ensures even the cluster-to-storage communication stays within your network.

---

**A4**
- **Data Plane VMs** — you pay for these. Each Worker Node and the Driver Node are Azure VMs in your subscription billed by Azure (VM + storage + networking costs).
- **Control Plane** — Databricks charges a **DBU (Databricks Unit)** fee per core-hour of cluster runtime. This covers the managed service cost.

So for a running cluster you pay both: Azure VM cost (Data Plane) + Databricks DBU cost (Control Plane). When the cluster is stopped, both charges stop — the DBU fee and the VM cost.

---

## Cluster Architecture

**A5**
The Driver Node is the "brain" of the cluster. It:
- Hosts the `SparkContext` — the entry point to all Spark operations
- Receives code from notebooks or jobs
- Builds the DAG (execution plan) for each Action
- Distributes Tasks to Executors on Worker Nodes
- Collects results from Workers and returns them to the notebook

If the Driver Node crashes, **the entire Spark application fails**. All running jobs terminate, all in-memory DataFrames are lost, and the notebook connection drops. Worker Nodes cannot continue without the Driver — they cannot coordinate among themselves.

This is why you should always use a reliable (Standard/On-Demand) VM for the Driver even when using Spot VMs for Workers.

---

**A6**
| | All-Purpose Cluster | Job Cluster |
|---|---|---|
| Lifetime | Persistent (stays running) | Created for one job run, terminated after |
| Cost | Higher — runs even when idle | Lower — only billed during job execution |
| Shared | Yes — multiple notebooks/users | No — dedicated to one job run |
| Start time | Near instant (already running) | 3–5 minute cold start per run |

**For a nightly scheduled Silver transformation:** use a **Job Cluster**. Reasons:
1. The job runs once per night — paying for an idle cluster all day wastes money
2. The job cluster starts fresh with no contention from other users
3. The cluster auto-terminates after the job, so no risk of forgotten running clusters

Use an All-Purpose Cluster for development and interactive notebooks during business hours.

---

**A7**
With 4 Worker Nodes, each with 4 cores and 16 GB RAM:
- Executors: typically **1 Executor per Worker Node** by default in Databricks = **4 Executors**
- Memory per Executor: **16 GB** (minus OS overhead, so roughly 12–14 GB usable by Spark)
- Cores per Executor: 4 cores → can run **4 Tasks concurrently per Executor**
- Total concurrent Tasks: 4 Workers × 4 cores = **16 Tasks running simultaneously**

Note: Databricks can configure multiple Executors per Worker for fine-grained resource allocation, but the single-Executor-per-Worker model is the default.

---

**A8**
Two likely causes:

1. **Resource contention.** With 5 data scientists on the same cluster, their notebooks consume memory and CPU. The nightly job competes for Executor slots with interactive queries. A slow Shuffle might be waiting for cores that are occupied by other users' tasks.

2. **No auto-scaling or insufficient cluster size.** If the cluster was sized for interactive low-concurrency work, a large batch job that reads all payments data will fill up the Executors and queue tasks.

**Fix:** Move the nightly job to a **dedicated Job Cluster** with a size appropriate for the data volume. Never share production batch jobs with interactive development clusters.

---

**A9**
Databricks Runtime (DBR) is the pre-built software stack installed on every cluster node. It includes Apache Spark (specific version), Delta Lake, Python/Scala/Java/R runtimes, and pre-installed libraries.

**LTS = Long-Term Support.** A LTS runtime is a specific DBR version that Databricks commits to supporting with security patches and bug fixes for approximately 2 years (vs. non-LTS versions which are supported for 6 months).

Use LTS for production because:
- Stability — well-tested, known issues fixed
- Longer support window — less frequent forced upgrades
- Predictable behaviour — non-LTS runtimes may have experimental features

---

## Spark Execution Model

**A10**
**Transformations** are lazy operations that build a plan but do not execute immediately:
- `df.filter(df.status == 'active')` — describes which rows to keep
- `df.select('id', 'amount')` — describes which columns to keep
- `df.groupBy('date').agg(count('id'))` — describes the aggregation

**Actions** trigger actual computation:
- `df.show()` — read data, compute result, display in notebook
- `df.count()` — read data, count rows, return integer to Driver
- `df.write.parquet(...)` — read data, write files to storage
- `df.collect()` — read all data and return to Driver as a Python list

**Why it matters:** Spark collects all transformations first, then hands the complete plan to the Catalyst optimizer, which rewrites and optimises it (e.g., pushing filters as early as possible, reordering joins). If Spark executed each step immediately, it could not perform cross-step optimisations.

---

**A11**
Lazy evaluation means Spark does nothing when you call a Transformation. It records the operation in a **lineage graph** (the DAG) — a description of what needs to happen — but does not read any data or do any computation.

Between Transformation and Action, Spark:
1. Adds each Transformation to the DAG
2. When an Action is called, passes the complete DAG to the **Catalyst Optimizer**
3. Catalyst rewrites the plan — pushes down filters, reorders joins, eliminates redundant steps
4. Catalyst generates optimised physical execution code (via Tungsten)
5. Spark executes the optimised plan

The result: `df.filter(...).select(...).groupBy(...).agg(...)` runs as one optimised pass over the data, not as four sequential passes.

---

**A12**
**3 Spark Jobs** are triggered:

1. `df_clean.count()` → Action → 1 Job (reads file, applies filter, counts)
2. `df_clean.show(10)` → Action → 1 Job (reads file again, applies filter, takes 10 rows)
3. `df_clean.write.parquet(...)` → Action → 1 Job (reads file a third time, applies filter, writes output)

`spark.read.json(...)` and `.filter(...)` are Transformations — no Jobs created.

**Performance issue:** the source file is read three times. Fix: cache `df_clean` after the filter:
```python
df_clean = df.filter(df.status == 'active').cache()
df_clean.count()    # triggers caching (first read)
df_clean.show(10)   # reads from cache (fast)
df_clean.write...   # reads from cache (fast)
```

---

**A13**
A shuffle is when Spark redistributes data across all Worker Nodes so that all records with the same key end up on the same Executor. It is required by operations that need to group data globally: `groupBy`, `join`, `distinct`, `orderBy`.

VoltGrid example — `groupBy('created_date').agg(sum('amount'))`:
- Before shuffle: Worker 1 has payments from 2026-10-01 and 2026-10-02; Worker 2 also has payments from both dates
- The groupBy requires all 2026-10-01 records to be on the same Worker and all 2026-10-02 records on the same Worker
- Shuffle: Worker 1 sends its 2026-10-02 records to Worker 2 and Worker 2 sends its 2026-10-01 records to Worker 1

Why expensive:
1. **Network I/O** — data is serialised and sent over the network
2. **Disk I/O** — Spark spills shuffle data to disk if it doesn't fit in memory
3. **Stage barrier** — no downstream task can start until all upstream tasks complete (global synchronisation point)

---

**A14**
**Job:** Created by one Action. Represents the complete computation needed to produce the Action's result.

**Stage:** A Job is divided into Stages at **shuffle boundaries**. All Tasks within a Stage can run in parallel — no data movement between Workers is needed within a Stage. A new Stage begins wherever a shuffle is required.

**Task:** The smallest unit. One Task processes one Partition on one Executor. All Tasks within a Stage run in parallel (constrained only by available Executor slots).

A new Stage is created when Spark needs to shuffle data — specifically when operations like `groupBy`, `join`, `distinct`, or `repartition` require all records with the same key to be on the same Executor.

---

**A15**
**1000 Tasks** — one Task per partition. `count()` is a single-stage operation (no shuffle required — each Task counts its own partition, the Driver sums the results). With 1000 partitions, Spark creates 1000 Tasks to count in parallel.

---

**A16**
Spark reads the source file **three times** — once per `show()` call. Each call is a new Action that triggers re-execution of the full lineage from source.

Fix: **cache the DataFrame** in memory after reading:
```python
df = spark.read.json("/mnt/bronze/payments/").cache()
df.show(5)   # first call: reads from disk, caches in memory
df.show(5)   # second call: reads from cache
df.show(5)   # third call: reads from cache
```

Trade-off: caching uses cluster memory. If the DataFrame is larger than available memory, Spark spills to disk (slower) or evicts cached partitions. Cache only DataFrames that are reused multiple times and fit comfortably in memory. Call `.unpersist()` when done to free memory.

---

## Delta Lake

**A17**
Delta Lake is an open-source storage layer that sits on top of Parquet files and adds database-like features to a data lake. It stores data as Parquet files plus a transaction log (`_delta_log/`).

Four features it adds over plain Parquet:
1. **ACID transactions** — writes are atomic (all-or-nothing); concurrent readers always see a consistent state
2. **Time Travel** — query any previous version of the table using `versionAsOf` or `timestampAsOf`
3. **DML support** — `UPDATE`, `DELETE`, `MERGE` (upsert) operations not possible on plain Parquet
4. **Schema enforcement** — Delta rejects writes whose schema does not match the table's schema, preventing data corruption

---

**A18**
The `_delta_log/` directory is Delta Lake's transaction log. It contains a series of JSON commit files (one per write operation):
- `00000000000000000000.json` — commit 0 (initial table creation)
- `00000000000000000001.json` — commit 1 (an append)
- `00000000000000000002.json` — commit 2 (an UPDATE)

Each JSON file records:
- Which Parquet files were added (`add` entries)
- Which Parquet files were removed/logically deleted (`remove` entries)
- Schema and metadata

ACID transactions work because: when Spark writes, it writes new Parquet files first, then atomically appends a new JSON commit file. If the process crashes before the commit file is written, the new Parquet files are invisible to readers (no commit = not part of the table). Concurrent readers always read based on the latest committed log entry, so they never see partial writes.

---

**A19**
Use Delta Lake **Time Travel** to read the table as it was at 8:55 AM, then overwrite the current (bad) data with the good historical data:

```python
from delta.tables import DeltaTable

# Read the table as it was at 8:55 AM
df_good = spark.read.format("delta") \
    .option("timestampAsOf", "2026-10-01 08:55:00") \
    .load("abfss://silver@evdatalakedev.dfs.core.windows.net/api/payments/")

# Overwrite the current bad data with the good historical version
df_good.write.format("delta").mode("overwrite") \
    .save("abfss://silver@evdatalakedev.dfs.core.windows.net/api/payments/")
```

Or using SQL:
```sql
RESTORE TABLE silver.payments TO TIMESTAMP AS OF '2026-10-01 08:55:00'
```

`RESTORE` is simpler and is the recommended approach — it adds a new commit to the log restoring the table to the previous state without rewriting all the files.

---

**A20**
**OPTIMIZE:** Compacts many small Parquet files into fewer, larger files. Improves read performance because Spark needs to open fewer files. Does not delete any data — it rewrites the data into larger files and marks the old small files as "removed" in the Delta log (but keeps them for time travel).

**VACUUM:** Removes old Parquet files that are no longer referenced by the Delta transaction log — i.e., files that were superseded by OPTIMIZE, UPDATE, or DELETE operations. Frees storage space. Deletes data permanently — after VACUUM, time travel before the retention window is not possible.

**Can you run VACUUM immediately after table creation?**
No — the default retention period is **7 days (168 hours)**. VACUUM with the default settings will not delete anything newer than 7 days. Running it immediately is safe (it won't delete the newly created files) but pointless. Never set `RETAIN 0 HOURS` unless you are certain you don't need time travel — it will delete all historical versions.

---

**A21**
| | Managed Table | External Table |
|---|---|---|
| Data stored in | Unity Catalog managed storage | Your specified ADLS path |
| `DROP TABLE` effect | **Deletes the data files** | Only removes the table definition — data stays |

For VoltGrid Silver payments: use an **External Table** pointing to `abfss://silver@evdatalakedev.dfs.core.windows.net/api/payments/`.

Reason: the data in ADLS needs to remain accessible even if the Databricks workspace is changed or the table is dropped. ADF also writes to this ADLS path directly (Copy Activity → Bronze layer), and external tables let multiple tools share the same data without Databricks owning it.

---

**A22**
The old Parquet files are still physically present — Delta Lake uses **soft deletion**. When you overwrite a Delta table, Spark writes the new Parquet files first, then adds a commit to `_delta_log/` that marks the old files as "removed" and the new files as "added." The old files are not physically deleted — they remain for time travel.

When Spark reads the table, it reads the `_delta_log/` to find which files are currently active (i.e., added but not yet removed), and reads only those. The old files are invisible to reads even though they exist in storage.

The old files are only physically deleted when you run `VACUUM` after the retention period (default 7 days).

---

## Unity Catalog & Workspace

**A23**
Unity Catalog uses a three-level namespace: **Catalog → Schema → Table**

```
CATALOG  →  SCHEMA   →  TABLE
ev_catalog  silver      payments
```

Fully-qualified table name for VoltGrid Silver payments:
```sql
ev_catalog.silver.payments
```

Usage:
```sql
SELECT * FROM ev_catalog.silver.payments WHERE status = 'active';
```

---

**A24**
**Unity Catalog Table:** Structured data with a defined schema — columns, data types, Delta/Parquet format. Supports SQL queries, DataFrame reads, MERGE, and all Spark operations.

**Unity Catalog Volume:** Unstructured or semi-structured file storage — JSON, CSV, images, model files, anything. No schema enforcement. Governed by Unity Catalog for access control but accessed via file path, not SQL.

Use Volume when:
- Storing ML model artefacts, raw uploaded files, or binary data
- The data does not fit a tabular schema
- You need governed file storage alongside your Delta tables

Use Table when:
- Data is structured and will be queried with SQL or DataFrames
- You need ACID transactions, schema enforcement, or time travel

---

**A25**
**Managed Table:** Running `DROP TABLE ev_catalog.silver.payments` on a Managed Table **permanently deletes the underlying Parquet/Delta data files** from Databricks-managed storage. The data is gone — no recovery possible.

**External Table:** Running `DROP TABLE` on an External Table only removes the table definition (metadata) from Unity Catalog. The actual data files in ADLS Gen2 (`abfss://silver@evdatalakedev.dfs.core.windows.net/api/payments/`) are **not deleted**. You can recreate the table definition and query the data again by pointing a new table at the same ADLS path.

This is why External Tables are preferred for production data in the lakehouse — they decouple the table definition lifecycle from the data lifecycle.

---

## dbutils and Notebooks

**A26**
Four main dbutils modules:

1. **dbutils.fs** — file system operations: `ls()`, `cp()`, `rm()`, `mkdirs()`, `head()`. Used to navigate and manipulate files in DBFS and mounted ADLS paths.

2. **dbutils.secrets** — read secrets from a Secret Scope (backed by Azure Key Vault or Databricks). `dbutils.secrets.get(scope, key)`. Secret values are never printed in notebook output — shown as `[REDACTED]`.

3. **dbutils.widgets** — parameterise notebooks. `text()`, `dropdown()`, `get()`. Used to receive parameters from ADF Databricks Activity or from the notebook UI.

4. **dbutils.notebook** — chain notebooks. `run(path, timeout, arguments)` runs another notebook and returns its exit value. `exit(value)` returns a value from the current notebook to the caller.

---

**A27**
```python
# In the calling notebook:
result = dbutils.notebook.run(
    "/VoltGrid/silver/process_payments",
    timeout_seconds=3600,
    arguments={"run_date": "2026-10-01"}
)
print(result)   # prints whatever process_payments returns via dbutils.notebook.exit()
```

In the called notebook (`/VoltGrid/silver/process_payments`):
```python
# Receive the parameter
dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

# ... do the work ...

# Return a result to the caller
dbutils.notebook.exit(f"Processed {rows_written} rows for {run_date}")
```

The `result` variable in the calling notebook will contain the string returned by `dbutils.notebook.exit()`.

---

**A28**
Storing a password in a notebook cell is a serious security problem:
1. **Notebook content is stored in the Control Plane** — anyone with workspace access can see it
2. **Git integration** — if notebooks are committed to a repository, the password is in version control history forever, even if later deleted
3. **Notebook output** — if the cell is run and output is saved, the password appears in the saved output

**Correct approach:** Store the secret in Azure Key Vault, create a Databricks Secret Scope backed by Key Vault, then read it at runtime:

```python
# In Key Vault: secret name = "voltgrid-api-password", value = "EVcharge@AU2025"
# In Databricks: secret scope name = "kv-ev-scope"

password = dbutils.secrets.get(scope="kv-ev-scope", key="voltgrid-api-password")
# password is now usable in code but will never be printed — shown as [REDACTED]
```

---

## Workflows & Delta Live Tables

**A29**
A Databricks Workflow is Databricks' built-in job orchestrator — a directed graph of tasks (notebooks, Python scripts, SQL, jars) that run on a schedule or trigger.

**Databricks Workflows vs ADF:**

| | Databricks Workflows | ADF |
|---|---|---|
| Best for | Pure Databricks workloads (notebook → notebook → notebook) | Multi-service orchestration (API copy → Databricks → SQL proc → alert) |
| Compute | Databricks clusters | ADF IR + Databricks cluster per activity |
| Non-Databricks activities | Limited (SQL, dbt, HTTP) | Full (Copy, Stored Procedure, Web, Delete, ForEach, etc.) |
| Monitoring | Databricks Workflow UI | ADF Monitor panel |
| Cost | DBU per task run | ADF activity runs + DBU |

Use Databricks Workflows when the entire pipeline is Spark/Delta notebooks. Use ADF when the pipeline includes non-Databricks steps (REST API ingestion, SQL stored procedures, file movement).

---

**A30**
Delta Live Tables (DLT) is a declarative pipeline framework. Instead of writing imperative code (`read → transform → write`), you decorate Python functions with `@dlt.table` to declare what each table should contain. DLT infers the dependency order, manages execution, handles retries, and tracks data quality.

**Advantage over standard PySpark notebooks:**
- Automatic dependency management — DLT determines which table to compute first; you don't manage `dependsOn` chains
- Built-in data quality (Expectations) — `@dlt.expect("valid_amount", "amount > 0")` tracks violations without stopping the pipeline
- Continuous streaming pipelines — DLT handles restart logic and state management automatically

**Limitation vs standard PySpark notebooks:**
- Less flexibility — DLT pipelines can only produce Delta tables and follow the declarative pattern; complex imperative logic (dynamic table names, conditional branching, external API calls within the transform) is harder or impossible
- Debugging is harder — you cannot run individual cells like in a notebook; the entire pipeline runs as a unit, making step-by-step iteration slower during development
