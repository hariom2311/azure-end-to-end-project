# Day 7 — Practice Exercises: ADF + Databricks Integration

> Complete these exercises in order. Each builds on the previous one.
> Resources: `adf-ev-dev`, `dbw-ev-dev`, `stadlsdev001`, `stblobdev001`.

---

## Exercise 1 — Generate a PAT Token and Create an Access Token Linked Service

**Objective:** Set up the simplest possible ADF → Databricks connection.

**Steps:**
1. Open `dbw-ev-dev` Databricks workspace
2. Go to User Settings → Developer → Access tokens
3. Generate a token with comment `exercise-1-pat`, lifetime 30 days
4. Open `adf-ev-dev` → ADF Studio → Manage → Linked services
5. Create a new Azure Databricks Linked Service named `ls_databricks_exercise1`
6. Authentication: Access token — paste the token you just generated
7. Cluster: New job cluster, `Standard_D4s_v3`, 1 worker
8. Click Test connection — confirm it succeeds
9. Apply and save

**Verify:** The Linked Service appears in the list with a green status indicator.

---

## Exercise 2 — Create and Run a Simple ADF Notebook Pipeline

**Objective:** Trigger a notebook from ADF and capture the output.

**Steps:**
1. In Databricks workspace → `Shared/day7-adf-practice/` → create notebook `exercise2_notebook`
2. Add Cell 1:
   ```python
   dbutils.widgets.text("batch_date", "2024-01-01", "Batch Date")
   batch_date = dbutils.widgets.get("batch_date")
   print(f"Processing date: {batch_date}")
   ```
3. Add Cell 2:
   ```python
   result = f"Processed batch_date={batch_date}"
   dbutils.notebook.exit(result)
   ```
4. In ADF Studio → Author → create pipeline `pl_exercise2`
5. Add a Databricks Notebook activity
6. Linked service: `ls_databricks_exercise1`
7. Notebook path: `/Shared/day7-adf-practice/exercise2_notebook`
8. Base parameter: `batch_date` = `2024-01-15`
9. Click Debug → OK
10. When it completes, click the glasses icon on the activity
11. Find `runOutput` in the JSON

**Verify:** `runOutput` says `Processed batch_date=2024-01-15`

---

## Exercise 3 — Use ADF Dynamic Expressions as Parameters

**Objective:** Pass ADF system variables (pipeline name, run ID) to the notebook.

**Steps:**
1. Open the `exercise2_notebook` from Exercise 2
2. Update Cell 1 to also read `pipeline_name` and `run_id` widgets:
   ```python
   dbutils.widgets.text("batch_date",     "2024-01-01", "Batch Date")
   dbutils.widgets.text("pipeline_name",  "unknown",    "Pipeline Name")
   dbutils.widgets.text("run_id",         "unknown",    "Run ID")

   batch_date    = dbutils.widgets.get("batch_date")
   pipeline_name = dbutils.widgets.get("pipeline_name")
   run_id        = dbutils.widgets.get("run_id")

   print(f"batch_date   : {batch_date}")
   print(f"pipeline_name: {pipeline_name}")
   print(f"run_id       : {run_id}")
   ```
3. Update Cell 2 exit string to include pipeline_name and run_id
4. In `pl_exercise2` (the ADF pipeline):
   - Add two more Base parameters:
     - `pipeline_name` = `@pipeline().Pipeline`
     - `run_id` = `@pipeline().RunId`
5. Debug → view Output → find `runOutput`

**Verify:** `runOutput` contains the actual pipeline name `pl_exercise2` and a real GUID for `run_id`.

---

## Exercise 4 — Add a Dependency Arrow Between Two Activities

**Objective:** Chain a Copy Data activity → Notebook activity with a success arrow.

**Steps:**
1. Open `pl_exercise2`
2. Drag a **Copy data** activity onto the canvas (from Activities → Move & Transform)
3. Name it `Dummy Copy`
4. Configure it minimally (you do not need a real source — for this exercise, the goal is the arrow)
5. Draw the green arrow from `Dummy Copy` → `Run Notebook`
6. Click the arrow — confirm the condition is `Succeeded`
7. Save the pipeline
8. Click Debug — notice the Output tab shows BOTH activities

**Verify:** The output shows `Dummy Copy` first, then `Run Notebook`. If `Dummy Copy` had failed, `Run Notebook` would have been skipped.

---

## Exercise 5 — Set Up System-Assigned Managed Identity Auth

**Objective:** Remove the PAT token dependency by using ADF's own managed identity.

**Steps:**
1. Azure Portal → `adf-ev-dev` → Properties → copy the managed identity **Object ID**
2. Azure Portal → `dbw-ev-dev` → Access control (IAM)
3. Add role assignment:
   - Role: `Contributor`
   - Assign to: Managed identity → `adf-ev-dev` (Data factory V2)
4. Wait 2 minutes for propagation
5. ADF Studio → Manage → Linked services → + New → Azure Databricks
6. Name: `ls_databricks_smi`
7. Authentication: `System-assigned managed identity`
8. Cluster: New job cluster, `Standard_D4s_v3`, 1 worker
9. Test connection → Apply
10. In `pl_exercise2`, switch the activity's Linked Service from `ls_databricks_exercise1` to `ls_databricks_smi`
11. Debug → confirm it still works

**Verify:** The pipeline runs successfully using managed identity — no token involved.

---

## Exercise 6 — Schedule the Pipeline

**Objective:** Configure the pipeline to run daily at a fixed time.

**Steps:**
1. Open `pl_exercise2` in ADF Studio
2. Click **Add trigger** → **New/Edit**
3. Create a new trigger:
   - Name: `trigger_exercise6_daily`
   - Type: Schedule
   - Recurrence: Every 1 Day
   - Time: 9:00 AM (your timezone)
4. Click OK → OK
5. Click **Publish all** (the trigger must be published to become active)
6. Go to **Monitor** (left sidebar, clock icon) → **Trigger runs** to see the trigger listed

**Verify:** The trigger appears in the Trigger runs list with status `Waiting`.

> To cancel (avoid unnecessary runs): go to Manage → Triggers → find your trigger → click the toggle to **Disable** it.

---

## Exercise 7 — Full End-to-End: Read from ADLS in the Notebook

**Objective:** ADF triggers a notebook that reads data from `stadlsdev001` and returns a summary.

**Steps:**
1. Databricks workspace → create notebook `Shared/day7-adf-practice/exercise7_read_adls`
2. Add Cell 1:
   ```python
   dbutils.widgets.text("container", "bronze", "Container")
   container = dbutils.widgets.get("container")
   path = f"abfss://{container}@stadlsdev001.dfs.core.windows.net/"
   print(f"Reading from: {path}")
   ```
3. Add Cell 2:
   ```python
   try:
       files = dbutils.fs.ls(path)
       file_count = len(files)
       print(f"Found {file_count} items")
   except Exception as e:
       file_count = 0
       print(f"Error: {e}")
   ```
4. Add Cell 3:
   ```python
   dbutils.notebook.exit(f"container={container} files_found={file_count}")
   ```
5. In ADF Studio → create pipeline `pl_exercise7_read_adls`
6. Add a Databricks Notebook activity:
   - Notebook path: `/Shared/day7-adf-practice/exercise7_read_adls`
   - Base parameter: `container` = `bronze`
7. Debug → view output → confirm `runOutput` shows file count

**Verify:** `runOutput` says `container=bronze files_found=<N>` — N being the number of files in your bronze container.

---
