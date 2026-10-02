# Day 1 — Practice Exercises: Databricks Architecture & Terminologies

> **Goal:** Explore the Databricks workspace, understand the UI, create your first cluster, run your first notebook, and observe Spark execution concepts in action.
> No external API or complex data pipeline needed — all exercises use Databricks built-in sample data or inline data.

---

## Before You Start

You need:
- Access to an Azure Databricks workspace (URL: `https://adb-<id>.<region>.azuredatabricks.net`)
- Permission to create clusters and notebooks
- The workspace should have ADLS Gen2 access configured (optional for some exercises)

---

## Exercise 1 — Explore the Workspace UI

**Goal:** Understand what each section of the Databricks workspace does.

### Steps

1. Open your Databricks workspace URL in a browser
2. Review the left sidebar — identify each icon:
   - **Home** → your personal notebook folder
   - **Workspace** → all notebooks and folders (shared workspace)
   - **Repos** → Git-connected repositories
   - **Data** / **Catalog** → Unity Catalog tables and schemas
   - **Compute** → clusters and SQL warehouses
   - **Workflows** → scheduled jobs and pipelines
   - **SQL Editor** → run SQL queries (uses SQL Warehouse)
   - **Marketplace** → Databricks-curated datasets and models

3. Navigate to **Compute** → observe any existing clusters
   - Note the cluster status (Running / Terminated)
   - Click a cluster → read the **Configuration** tab:
     - Databricks Runtime version
     - Worker type and count
     - Auto-termination setting

4. Navigate to **Catalog** (Unity Catalog) → expand the default catalog → observe existing schemas and tables

**Answer these questions after exploring:**
- What Databricks Runtime version is used on the cluster?
- Is the cluster an All-Purpose or Job cluster?
- What is the auto-termination timeout?

---

## Exercise 2 — Create a Cluster

**Goal:** Create an All-Purpose cluster for development.

### Steps

1. **Compute** → **+ Create compute**
2. Configure:
   - **Cluster name:** `dev-cluster-yourname`
   - **Policy:** Unrestricted (or the policy your workspace allows)
   - **Cluster mode:** Single Node (sufficient for exercises today)
   - **Databricks Runtime:** pick the latest LTS version (e.g., `14.3 LTS`)
   - **Node type:** Standard_DS3_v2 (or smallest available)
   - **Auto-termination:** 30 minutes
3. Click **Create compute**
4. Wait for the cluster to reach **Running** state (~3–5 minutes)

**Observe while waiting:**
- The cluster goes through states: Pending → Running
- The cluster URL includes the workspace region (matches the Data Plane region)

**After cluster starts:**
- Click **Spark UI** → this opens the Spark Web UI (port 4040 equivalent)
- Navigate to **Jobs** tab → empty (no jobs run yet)
- Navigate to **Executors** tab → observe the Driver executor listed

---

## Exercise 3 — Create a Notebook and Understand Cell Execution

**Goal:** Write and run your first cells, observe lazy evaluation and Actions vs Transformations.

### Steps

1. **Workspace** → your user folder → **+ Create** → **Notebook**
2. Name: `day1_spark_concepts` | Language: Python | Cluster: `dev-cluster-yourname`
3. Run the following cells one at a time (Shift+Enter):

**Cell 1 — Check SparkSession:**
```python
print(type(spark))
print(spark.version)
```
Expected: prints `<class 'pyspark.sql.session.SparkSession'>` and the Spark version.

**Cell 2 — Create a DataFrame (Transformation — no execution yet):**
```python
# Using Databricks built-in sample data
df = spark.read.csv(
    "/databricks-datasets/flights/departuredelays.csv",
    header=True,
    inferSchema=True
)

# Add a filter (still a Transformation — no data read yet)
df_delayed = df.filter(df.delay > 60)

print("DataFrame created — but no data read yet!")
print(f"Type: {type(df_delayed)}")
```
Observe: this runs instantly. No Spark job appears in the Spark UI yet.

**Cell 3 — Trigger an Action:**
```python
# count() is an Action — this triggers actual Spark execution
count = df_delayed.count()
print(f"Flights delayed > 60 minutes: {count}")
```
Observe: this takes a few seconds. Go to Spark UI → Jobs → you'll see a new Job appear.

**Cell 4 — Another Action (reads source again):**
```python
df_delayed.show(5)
```
Observe: another Job appears in Spark UI — the source file was read again.

**Cell 5 — Cache and reuse:**
```python
# Cache the filtered DataFrame
df_delayed.cache()

# First action after cache: reads from disk, stores in memory
print(f"Cached count: {df_delayed.count()}")

# Second action: reads from cache — much faster
df_delayed.show(5)
print("This time it read from cache, not disk!")
```
Compare the job durations in Spark UI — the cached run is significantly faster.

**Cell 6 — Inspect the DataFrame schema:**
```python
df_delayed.printSchema()
print(f"Number of partitions: {df_delayed.rdd.getNumPartitions()}")
```

---

## Exercise 4 — Observe DAG, Stages, and Tasks in Spark UI

**Goal:** Visually understand how Spark breaks your code into Jobs, Stages, and Tasks.

### Steps

**Cell 1 — Run a multi-step transformation with a shuffle:**
```python
from pyspark.sql.functions import count, avg

# Chain of transformations ending in an Action
df_summary = (
    df                                          # read
    .filter(df.delay > 0)                       # transformation (no shuffle)
    .groupBy("origin")                          # transformation (SHUFFLE required)
    .agg(
        count("*").alias("total_flights"),
        avg("delay").alias("avg_delay")
    )
    .orderBy("avg_delay", ascending=False)      # transformation (SHUFFLE required)
)

# Action — triggers everything
df_summary.show(10)
```

**After running:**
1. Go to **Spark UI** → **Jobs** → click the latest job
2. Click **DAG Visualization** → observe the execution graph:
   - Boxes = Stages
   - Arrows = data flow
   - Stage boundary where `groupBy` and `orderBy` cause shuffles
3. Click **Stages** tab → observe 2 or 3 stages
4. Click into a Stage → observe Tasks (one per partition)

**Questions to answer:**
- How many Stages did this Job have?
- Where was the stage boundary (which operation caused a shuffle)?
- How many Tasks ran in each Stage?

---

## Exercise 5 — Delta Lake: Create, Update, Time Travel

**Goal:** Create a Delta table, modify it, and use Time Travel to see historical versions.

### Steps

**Cell 1 — Create a Delta table from inline data:**
```python
from pyspark.sql import Row

# Create a simple DataFrame
data = [
    Row(id=1, name="payments",  status="active",  row_count=1000),
    Row(id=2, name="sessions",  status="active",  row_count=850),
    Row(id=3, name="customers", status="inactive", row_count=500),
]
df_config = spark.createDataFrame(data)

# Write as Delta format
df_config.write.format("delta").mode("overwrite").saveAsTable("default.pipeline_config")
print("Delta table created!")
```

**Cell 2 — Read and verify:**
```python
spark.sql("SELECT * FROM default.pipeline_config").show()
```

**Cell 3 — View Delta table history:**
```python
spark.sql("DESCRIBE HISTORY default.pipeline_config").show(truncate=False)
```
Observe: version 0 = initial write. Each write creates a new version.

**Cell 4 — Update a row (creates version 1):**
```python
spark.sql("""
    UPDATE default.pipeline_config
    SET row_count = 1200
    WHERE name = 'payments'
""")
print("Updated payments row_count to 1200")
spark.sql("SELECT * FROM default.pipeline_config").show()
```

**Cell 5 — Check history again:**
```python
spark.sql("DESCRIBE HISTORY default.pipeline_config").show(truncate=False)
```
Now you see version 0 (initial) and version 1 (update).

**Cell 6 — Time Travel: read as of version 0:**
```python
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .table("default.pipeline_config")

print("Version 0 (original):")
df_v0.show()

df_current = spark.sql("SELECT * FROM default.pipeline_config")
print("Current version:")
df_current.show()
```
Observe: version 0 shows `row_count = 1000` for payments; current shows `1200`.

**Cell 7 — Inspect the Delta log files:**
```python
# See the physical files behind the Delta table
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/pipeline_config"))
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/pipeline_config/_delta_log/"))
```
Observe: `_delta_log/` directory with JSON commit files (00000...json, 00001...json).

---

## Exercise 6 — dbutils: Secrets, Widgets, and File System

**Goal:** Use dbutils for the three most common production tasks.

### Step 6.1 — File system operations

```python
# List the Databricks sample datasets
display(dbutils.fs.ls("/databricks-datasets/"))

# List a specific directory
display(dbutils.fs.ls("/databricks-datasets/flights/"))

# Read the first 200 bytes of a file
print(dbutils.fs.head("/databricks-datasets/flights/departuredelays.csv", 200))
```

### Step 6.2 — Widgets (parameters)

```python
# Declare a text widget
dbutils.widgets.text("run_date", "2026-10-01", "Run Date")
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")

# Read widget values
run_date = dbutils.widgets.get("run_date")
environment = dbutils.widgets.get("environment")

print(f"Run date: {run_date}")
print(f"Environment: {environment}")
```

After running, observe the widget input boxes appear at the top of the notebook. Change the value in the UI and re-run — the new value is picked up.

### Step 6.3 — Secrets (if a Secret Scope is configured)

```python
# List available secret scopes
dbutils.secrets.listScopes()
```

```python
# Read a secret (replace with your scope and key names)
# secret = dbutils.secrets.get(scope="kv-ev-scope", key="voltgrid-username")
# print(secret)   # prints [REDACTED] — never exposed in output
```

---

## Exercise 7 — Unity Catalog: Three-Level Namespace

**Goal:** Query tables using the three-level namespace and understand schema/catalog organisation.

### Steps

**Cell 1 — List catalogs:**
```sql
%sql
SHOW CATALOGS;
```

**Cell 2 — List schemas in a catalog:**
```sql
%sql
SHOW SCHEMAS IN main;
```
(Replace `main` with your catalog name)

**Cell 3 — Create a schema and table:**
```sql
%sql
CREATE SCHEMA IF NOT EXISTS main.ev_demo;

CREATE TABLE IF NOT EXISTS main.ev_demo.sample_payments (
    id          INT,
    amount      DOUBLE,
    status      STRING,
    created_date DATE
) USING DELTA;

INSERT INTO main.ev_demo.sample_payments VALUES
    (1, 45.80, 'completed', '2026-10-01'),
    (2, 23.50, 'failed',    '2026-10-01'),
    (3, 67.00, 'completed', '2026-10-01');
```

**Cell 4 — Query with full three-part name:**
```sql
%sql
SELECT * FROM main.ev_demo.sample_payments
WHERE status = 'completed';
```

**Cell 5 — Use USE to set defaults:**
```sql
%sql
USE CATALOG main;
USE SCHEMA ev_demo;

-- Now this resolves to main.ev_demo.sample_payments
SELECT COUNT(*) as total FROM sample_payments;
```

**Cell 6 — View table details:**
```sql
%sql
DESCRIBE EXTENDED main.ev_demo.sample_payments;
```
Observe: table type (MANAGED or EXTERNAL), location (ADLS path), Delta format, table properties.

---

## Exercise 8 — Cluster Termination and Cost Awareness

**Goal:** Understand auto-termination and its importance for cost management.

### Steps

1. Go to **Compute** → click your `dev-cluster-yourname` cluster
2. Note the **Auto-termination** setting (you set it to 30 min in Exercise 2)
3. Check **Last activity** — this resets every time a cell runs
4. Manually terminate the cluster now: **Terminate** button → confirm

**What to observe:**
- Status changes from Running → Terminating → Terminated
- All notebook connections to this cluster are dropped
- Spark jobs no longer run

5. Start the cluster again and observe the cold-start time

**Cost awareness:**
- An All-Purpose cluster on Standard_DS3_v2 costs approximately $0.20–0.40/hour in Azure (VM cost + DBU)
- Auto-termination after 30 min prevents paying for idle clusters overnight
- A forgotten running cluster over a weekend = ~48 hours × cost = significant unexpected spend

---

## Summary Checklist

After completing all exercises, you should be able to:

| Concept | Verified? |
|---|---|
| Navigate workspace: Compute, Notebooks, Catalog, Workflows | |
| Create an All-Purpose cluster with LTS runtime | |
| Understand that Transformations are lazy (no Spark job until Action) | |
| Observe Jobs, Stages, and Tasks in the Spark UI | |
| Identify shuffle boundaries in a DAG visualisation | |
| Create a Delta table and see the `_delta_log` directory | |
| Run a Delta UPDATE and verify a new version was created | |
| Use Time Travel to query a previous Delta version | |
| Use dbutils.fs to list and read files | |
| Use dbutils.widgets to pass parameters to a notebook | |
| Query tables using the three-part Unity Catalog name | |
| Terminate a cluster and understand cost implications | |
