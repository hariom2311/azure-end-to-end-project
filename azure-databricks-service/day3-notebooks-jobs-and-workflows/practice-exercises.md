# Day 3 — Practice Exercises: Notebooks, Jobs & Workflows

> **Goal:** Create a notebook with simple Spark code, wrap it in a Databricks Job, build a multi-task pipeline, and pass parameters between a job and a notebook.
> All exercises use the VoltGrid naming convention.

---

## Before You Start

You need:
- The `voltgrid-dev-shared` cluster from Day 2 (create it if not done)
- Access to the Databricks workspace (`dbw-ev-dev`)
- The `Shared/voltgrid` folder in the workspace (create it if missing)

---

## Exercise 1 — Explore the Workspace Sections

**Goal:** Navigate every section of the workspace sidebar so you know what is where.

### Steps

1. Open your Databricks workspace URL

2. Click each item on the left sidebar and note what you see:
   - **Home** — your recently opened notebooks, quick create buttons
   - **Workspace** — folder tree (Shared + Users)
   - **Data** — catalog browser (if Unity Catalog is enabled)
   - **Compute** — your `voltgrid-dev-shared` cluster from Day 2
   - **Workflows** — (currently empty — we will fill this in exercises 4–6)
   - **Settings** — workspace settings, admin console, access tokens

3. In **Workspace** → right-click **Shared** → **Create** → **Folder**
   - Name: `voltgrid`

4. In **Settings** → **Developer** → **Access tokens** → **Generate new token**
   - Name: `day3-test-token`
   - Lifetime: 7 days
   - Copy the token value (paste it into a notepad temporarily — we will use it in a later exercise)

**What to verify:** You can navigate to every section and the `Shared/voltgrid` folder exists.

---

## Exercise 2 — Create a Notebook and Run Basic Commands

**Goal:** Create a notebook, attach it to a cluster, and run Python and SQL cells.

### Steps

1. **Workspace** → right-click `Shared/voltgrid` → **Create** → **Notebook**
   - Name: `ex2_first_notebook`
   - Default language: `Python`
   - Attach to: `voltgrid-dev-shared`
   - Click **Create**

2. **Cell 1** — Run a markdown header:
   ```python
   %md
   # Exercise 2 — First Notebook
   Testing basic Spark commands and SQL magic.
   ```
   Run: `Shift + Enter`

3. **Cell 2** — Check Spark version:
   ```python
   print(f"Spark version: {spark.version}")
   print(f"Python version: {spark.conf.get('spark.databricks.python.worker.reuse', 'n/a')}")
   print("Cluster connected successfully!")
   ```
   Run: `Shift + Enter`
   Expected: `Spark version: 3.5.x`

4. **Cell 3** — Create a small DataFrame:
   ```python
   data = [
       (1, "Alice",  "Engineering",  95000),
       (2, "Bob",    "Marketing",    72000),
       (3, "Carol",  "Engineering",  98000),
       (4, "David",  "HR",           65000),
       (5, "Eve",    "Marketing",    80000),
   ]
   columns = ["id", "name", "department", "salary"]

   df = spark.createDataFrame(data, columns)
   df.show()
   ```

5. **Cell 4** — Register as temp view and query with SQL:
   ```python
   df.createOrReplaceTempView("employees")
   ```

6. **Cell 5** — SQL magic cell:
   ```sql
   %sql
   SELECT department, COUNT(*) AS headcount, AVG(salary) AS avg_salary
   FROM employees
   GROUP BY department
   ORDER BY avg_salary DESC
   ```

7. **Cell 6** — Shell command:
   ```sh
   %sh
   echo "Driver hostname: $(hostname)"
   echo "Current user: $(whoami)"
   ```

8. **Cell 7** — List DBFS root:
   ```python
   %fs ls dbfs:/
   ```

9. Click **Run All** in the toolbar

**What to verify:** All 7 cells run without errors. The SQL cell shows a table with department averages.

---

## Exercise 3 — Use dbutils and Widgets

**Goal:** Use `dbutils.fs`, `dbutils.secrets`, and notebook widgets.

### Steps

1. Open `ex2_first_notebook` → add new cells at the bottom (click + below the last cell)

2. **New Cell** — test dbutils.fs:
   ```python
   # List DBFS /tmp directory
   files = dbutils.fs.ls("dbfs:/tmp/")
   for f in files:
       print(f.name, f.size)
   ```

3. **New Cell** — write a small file to DBFS and read it back:
   ```python
   # Write text to DBFS
   dbutils.fs.put("dbfs:/tmp/voltgrid_test.txt",
                  "Hello from Day 3 exercise!", overwrite=True)

   # Read it back
   content = dbutils.fs.head("dbfs:/tmp/voltgrid_test.txt")
   print("File content:", content)
   ```

4. **New Cell** — add a widget to parameterise the notebook:
   ```python
   # This creates a text box at the top of the notebook
   dbutils.widgets.text("run_env", "dev", "Run Environment")
   run_env = dbutils.widgets.get("run_env")
   print(f"Running in environment: {run_env}")
   ```
   After running this cell: a widget text box appears at the top of the notebook. Change the value to `staging` in the box and re-run the cell — `run_env` changes.

5. **New Cell** — test notebook exit value:
   ```python
   result_message = f"Exercise 3 completed in {run_env}"
   dbutils.notebook.exit(result_message)
   ```
   In manual runs, `dbutils.notebook.exit` just prints the value. In a job run, this value is captured in the job run output.

6. **Run All** (from Cell 1 so widgets are initialised before they are read)

**What to verify:**
- File `voltgrid_test.txt` is created and read back correctly
- Widget text box appears at the top of the notebook
- Exit value prints at the bottom

---

## Exercise 4 — Create a Single-Task Job

**Goal:** Schedule `ex2_first_notebook` to run automatically.

### Steps

1. **Workflows** (left sidebar) → **+ Create job**

2. Name the job: click `Untitled` at the top → type `voltgrid_ex4_daily_job`

3. Configure the task:
   | Field | Value |
   |---|---|
   | Task name | `run_first_notebook` |
   | Type | `Notebook` |
   | Source | `Workspace` |
   | Path | Browse to `Shared/voltgrid/ex2_first_notebook` |
   | Cluster | `New job cluster` |

4. Configure the job cluster (click **Edit** next to cluster):
   | Field | Value |
   |---|---|
   | Runtime | `15.4 LTS` |
   | Worker type | `Standard_D4s_v3` |
   | Workers | `1` |
   Click **Confirm**

5. Add a schedule:
   - Click **Add trigger**
   - Type: `Scheduled`
   - Schedule: `Every day at 08:00 AM`
   - Timezone: `UTC`
   - Click **Save**

6. Click **Create** (or **Save job**)

7. Click **Run now** to trigger a test run immediately

8. Watch the run:
   - **Workflows** → `voltgrid_ex4_daily_job` → **Runs** tab
   - Click the run row to open the run detail
   - Wait for the task status to turn green (Succeeded)
   - Click the task → **Logs** tab → you should see all cell outputs

**What to verify:** Run shows SUCCEEDED status. The Logs tab shows output from all cells including `Spark version: 3.5.x` and the SQL result table.

---

## Exercise 5 — Add Notifications and Retries

**Goal:** Configure the job to send email alerts and retry on failure.

### Steps

1. Open `voltgrid_ex4_daily_job` → **Edit** (top right)

2. **Notifications** section → **Add notification**:
   - On failure → email: your email address
   - Click **Save**

3. Click the task `run_first_notebook` to expand the task settings

4. Find **Retries** (in the task panel):
   - Max retries: `2`
   - Retry interval: `2 minutes`
   - Click **Save**

5. **Run now** again → the run should still succeed (no failure to test retries, but the config is in place)

6. To simulate a failure: open `ex2_first_notebook` → add a new last cell:
   ```python
   raise Exception("Simulated failure for retry test")
   ```
   Save the notebook → go back to the job → **Run now**

   The job will fail → wait 2 minutes → Databricks retries → fails again → retries → fails → marks as FAILED (after 2 retries exhausted).

7. **Important:** Remove the exception cell from `ex2_first_notebook` when done:
   - Open the notebook → click the exception cell → press `DD` to delete it

**What to verify:** The **Runs** tab shows a FAILED run with 3 attempts (1 original + 2 retries). Each attempt is listed separately.

---

## Exercise 6 — Create a Multi-Task Pipeline Job

**Goal:** Build a 3-task pipeline job where each task depends on the previous one.

### Steps

#### Prepare: Create 3 notebooks

1. In `Shared/voltgrid`, create 3 notebooks:
   - `bronze_notebook`
   - `silver_notebook`
   - `gold_notebook`

2. **bronze_notebook** — paste this content (replace any default cell):
   ```python
   %md
   ## Bronze Layer — Ingest
   ```
   ```python
   print("Bronze: Starting data ingestion...")

   data = [(i, f"payment_{i}", round(100 + i * 10.5, 2)) for i in range(1, 11)]
   bronze_df = spark.createDataFrame(data, ["id", "ref", "amount"])
   bronze_df.createOrReplaceTempView("bronze_payments")

   print(f"Bronze: Ingested {bronze_df.count()} records")
   dbutils.notebook.exit("Bronze completed")
   ```

3. **silver_notebook** — paste:
   ```python
   %md
   ## Silver Layer — Transform
   ```
   ```python
   print("Silver: Starting transformation...")

   # In a real job this reads from Delta — for demo we recreate the data
   data = [(i, f"payment_{i}", round(100 + i * 10.5, 2)) for i in range(1, 11)]
   bronze_df = spark.createDataFrame(data, ["id", "ref", "amount"])

   # Simple transform: add a status column
   from pyspark.sql.functions import when, col
   silver_df = bronze_df.withColumn(
       "status",
       when(col("amount") > 150, "high").otherwise("normal")
   )
   silver_df.show()
   print(f"Silver: Transformed {silver_df.count()} records")
   dbutils.notebook.exit("Silver completed")
   ```

4. **gold_notebook** — paste:
   ```python
   %md
   ## Gold Layer — Aggregate
   ```
   ```python
   print("Gold: Starting aggregation...")

   data = [(i, f"payment_{i}", round(100 + i * 10.5, 2)) for i in range(1, 11)]
   bronze_df = spark.createDataFrame(data, ["id", "ref", "amount"])

   from pyspark.sql.functions import when, col, count, sum as _sum, avg
   silver_df = bronze_df.withColumn(
       "status",
       when(col("amount") > 150, "high").otherwise("normal")
   )

   gold_df = silver_df.groupBy("status").agg(
       count("id").alias("total_payments"),
       _sum("amount").alias("total_amount"),
       avg("amount").alias("avg_amount")
   )
   gold_df.show()
   print("Gold: Aggregation complete")
   dbutils.notebook.exit("Gold completed")
   ```

#### Create the multi-task job

5. **Workflows** → **+ Create job**

6. Name: `voltgrid_pipeline_daily`

7. **Task 1:**
   | Field | Value |
   |---|---|
   | Task name | `bronze_ingest` |
   | Type | `Notebook` |
   | Path | `Shared/voltgrid/bronze_notebook` |
   | Cluster | New job cluster, D4s_v3, 1 worker, DBR 15.4 LTS |

8. **Add Task 2:** click **+ Add task**
   | Field | Value |
   |---|---|
   | Task name | `silver_transform` |
   | Type | `Notebook` |
   | Path | `Shared/voltgrid/silver_notebook` |
   | Depends on | `bronze_ingest` |
   | Cluster | New job cluster, same config |

9. **Add Task 3:** click **+ Add task**
   | Field | Value |
   |---|---|
   | Task name | `gold_aggregation` |
   | Type | `Notebook` |
   | Path | `Shared/voltgrid/gold_notebook` |
   | Depends on | `silver_transform` |
   | Cluster | New job cluster, same config |

10. Verify the DAG view shows: `bronze_ingest → silver_transform → gold_aggregation`

11. Add schedule: daily at 01:00 AM UTC

12. Click **Save job**

13. **Run now**

14. Monitor: **Runs** tab → click the run → you see all 3 tasks in the DAG. Watch them turn green one by one.

**What to verify:**
- All 3 tasks complete with SUCCEEDED status
- Each task shows its `dbutils.notebook.exit(...)` value in the output
- Tasks run in order: bronze → silver → gold (silver does not start until bronze is done)

---

## Exercise 7 — Pass Parameters from Job to Notebook

**Goal:** Configure the job to pass `run_date` and `environment` parameters into a notebook via widgets.

### Steps

1. Open `bronze_notebook` → add a cell **at the very top** (before other cells):
   ```python
   dbutils.widgets.text("run_date",    "2024-01-17", "Run Date")
   dbutils.widgets.text("environment", "dev",         "Environment")

   run_date = dbutils.widgets.get("run_date")
   environment = dbutils.widgets.get("environment")

   print(f"Job run date: {run_date}")
   print(f"Environment: {environment}")
   ```

2. Open `voltgrid_pipeline_daily` job → click **Edit**

3. Click the `bronze_ingest` task → find **Parameters** section → **Add parameter**:
   | Key | Value |
   |---|---|
   | `run_date` | `{{job.start_time.iso_date}}` |
   | `environment` | `prod` |

4. **Save** the job

5. **Run now** → open the run → click `bronze_ingest` task → **Logs**

   You should see at the top:
   ```
   Job run date: 2024-01-17
   Environment: prod
   ```

**What to verify:** The parameters are injected by the job and printed in the notebook output. Changing the value in the job config changes what the notebook sees.

---

## Final Architecture — VoltGrid Day 3 Setup

```
Workspace: dbw-ev-dev
  │
  ├── Shared/voltgrid/
  │     ├── ex2_first_notebook      ← Exercise 2 (general Spark practice)
  │     ├── bronze_notebook         ← Exercise 6 Task 1
  │     ├── silver_notebook         ← Exercise 6 Task 2
  │     └── gold_notebook           ← Exercise 6 Task 3
  │
  └── Workflows (Jobs):
        ├── voltgrid_ex4_daily_job         ← single-task, daily 08:00 UTC
        └── voltgrid_pipeline_daily        ← 3-task pipeline, daily 01:00 UTC
              bronze_ingest → silver_transform → gold_aggregation
```

---

## Quick Verification Checklist

| Task | How to verify |
|---|---|
| Workspace folder created | Workspace → Shared → voltgrid folder visible |
| Notebook runs all cells | Run All → all cells show green check |
| SQL magic works | %sql cell shows a result table |
| Widget created | Text box appears at top of notebook |
| dbutils.fs works | File written to DBFS and read back |
| Single-task job runs | Workflows → job → Runs tab → SUCCEEDED |
| Retries configured | Job task settings → Retries = 2 |
| Multi-task job DAG | Job canvas shows 3 connected task boxes |
| Tasks run in order | bronze → silver → gold in the run timeline |
| Parameters passed | Bronze notebook output shows run_date and environment values |
