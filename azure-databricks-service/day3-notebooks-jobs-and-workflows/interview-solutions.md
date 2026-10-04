# Day 3 — Interview Solutions: Notebooks, Jobs & Workflows

---

## Workspace

**A1**
- `Shared/` is visible to all users in the workspace. Any user can see, and (depending on permissions) open and run notebooks stored here. It is the right place for team notebooks and production code.
- `Users/` contains a personal sub-folder for each user (named after their email). Only that user can see their personal folder by default — other users cannot browse it unless they are given explicit access.

One sentence: Shared is the team library; Users is each engineer's personal scratch space.

---

**A2**
The problem: the notebook is in Alice's personal folder. If Alice leaves the company or her account is deactivated, the job path breaks. Also, other users cannot see or inspect the notebook.

Fix:
1. Move the notebook to a shared location: right-click → **Move** → `Shared/voltgrid/transform.py`
2. Set permissions on the notebook: right-click → **Permissions** → give the service principal or team group `Can Run` or `Can Edit`
3. Update the job to reference the new path

General rule: production notebooks must always live in `Shared/`, never in a personal folder.

---

**A3**
In Databricks, notebook-level permissions:
1. Right-click the notebook (or folder) → **Permissions**
2. Add the group `QA Team`
3. Grant **Can View** permission

**Can View** allows: opening the notebook and reading cells. It does NOT allow running cells or editing. The QA team can see the code but cannot execute it or change it.

---

**A4**
A PAT (Personal Access Token) is a bearer token that authenticates API calls and CLI commands to the Databricks workspace. It is generated per user under **Settings → Developer → Access tokens**.

Used in:
- Databricks CLI (`databricks configure --token`)
- REST API calls (Authorization: Bearer <token>)
- ADF Databricks Linked Service (if using PAT auth instead of Service Principal)
- CI/CD pipelines that deploy notebooks or jobs

Security risk of no expiry: if the token is leaked (committed to Git, shared in Slack, logged), an attacker has permanent access to the workspace with that user's permissions. There is no automatic revocation. Always set a short expiry (30–90 days) and rotate regularly.

---

## Notebooks

**A5**
Add `%sql` as the first line of the cell:
```sql
%sql
SELECT * FROM payments_clean WHERE status = 'completed'
```
The `%sql` magic command tells Databricks to run that cell in SQL, regardless of the notebook's default language.

---

**A6**
- `%fs` is a magic command — a shortcut used inside a notebook cell. It wraps `dbutils.fs` calls with a simpler syntax. Example: `%fs ls dbfs:/tmp/` runs in a dedicated cell.
- `dbutils.fs` is a Python object — it can be called inside Python code, assigned to variables, used in conditions and loops.

```python
# Use %fs for quick one-off inspection in a cell:
%fs ls abfss://bronze@evdatalakedev.dfs.core.windows.net/

# Use dbutils.fs when the result needs to be used in code:
files = dbutils.fs.ls("abfss://bronze@evdatalakedev.dfs.core.windows.net/")
for f in files:
    print(f.name, f.size)
```

---

**A7**
Two possible causes:

1. **The temp view does not exist in this session.** `createOrReplaceTempView` creates a view that exists only for the current SparkSession. If the cell that created the view was not run in this cluster session, or the cluster was restarted since the view was created, the view is gone. Fix: run the cell that creates the view first.

2. **The table does not exist in the metastore.** If `payments_clean` is a Unity Catalog or Hive metastore table (not a temp view), it may not be accessible from this cluster (wrong catalog, wrong permissions, or it was never created). Fix: check the Data section → browse for `payments_clean` → ensure the cluster has access.

---

**A8**
`df.createOrReplaceTempView("my_view")` registers a Spark DataFrame as a virtual SQL table named `my_view`. It exists only in the current SparkSession — it is not written to disk, it is not stored in the metastore, and it has no schema definition in Unity Catalog.

The view disappears when:
- The cluster is restarted or terminated
- The SparkSession is reset (e.g., `spark.catalog.dropTempView("my_view")`)
- The notebook is detached and reattached (new SparkSession started)

It does NOT persist across cluster sessions. For a permanent table, use `df.write.format("delta").saveAsTable("my_permanent_table")`.

---

**A9**
- Cells 1 to 4 ran successfully and have outputs
- Cell 5 failed — it shows an error output (the exception traceback)
- Cells 6 to 10 did NOT run and have no output (Databricks stops on failure when using Run All)

To fix: correct the error in cell 5 → click the ▶ button on cell 5 to run just that cell → then **Run All Below** to continue from cell 5 onward.

---

**A10**
`%run ./helper_notebook` executes the notebook at the relative path `helper_notebook` in the same directory. Its code runs in the same SparkSession, so any variables, functions, or DataFrames defined in `helper_notebook` become available in the calling notebook immediately after the `%run` cell completes.

Difference between `%run` and `dbutils.notebook.run()`:

| | `%run` | `dbutils.notebook.run()` |
|---|---|---|
| Returns | Nothing (variables in scope) | A string (exit value) |
| Scope | Shares SparkSession and variables | Runs in isolated scope — variables not shared |
| Use case | Loading helper functions / libraries | Running a sub-notebook as a step (like a function call) |
| Error handling | Stops current notebook on failure | Can be wrapped in try/except |

---

**A11**
a) **Manual run:** `dbutils.notebook.exit("DONE")` stops the notebook execution and prints the exit value below the cell. It is like a `return` statement for the notebook.

b) **Job run:** The exit value string (`"DONE"`) is captured by the job framework. It is displayed in the job run output panel under the task details. If the notebook is called via `dbutils.notebook.run("child", ...)`, the parent notebook receives this value as the return value.

---

**A12**
**In the browser (manual run):**
When Cell 1 (`dbutils.widgets.text(...)`) runs, a text input box appears at the top of the notebook with default value `"dev"`. The user can change the value in the UI. When Cell 2 (`dbutils.widgets.get(...)`) runs, it reads whatever is currently in the text box.

**When a Job runs the notebook:**
The text box UI is not rendered (no browser). The job configuration's **Parameters** section provides the values. If the job passes `{"env": "prod"}`, then `dbutils.widgets.get("env")` returns `"prod"`. If the job passes no parameters, the widget uses the default value (`"dev"`).

---

**A13**
`%sh pip install great_expectations` installs the library on the driver node only, and only for the current Python process. When the cluster restarts, the VM is rebuilt from the base runtime image — the manually installed library is gone.

Correct ways to install libraries persistently:

1. **%pip magic (recommended for session):**
   ```python
   %pip install great_expectations
   ```
   This installs on all nodes for the current session. It persists for the session but not across cluster restarts.

2. **Cluster libraries (persistent across restarts):**
   Compute → click the cluster → **Libraries** tab → **Install new** → PyPI → `great_expectations` → **Install**
   This installs every time the cluster starts.

3. **requirements.txt via init script:** For full control, attach an init script to the cluster that runs `pip install` during startup.

---

## dbutils

**A14**
```python
# a) List ADLS Silver container
dbutils.fs.ls("abfss://silver@evdatalakedev.dfs.core.windows.net/")

# b) Delete a DBFS folder recursively
dbutils.fs.rm("dbfs:/tmp/old_data/", True)
# True = recurse (deletes all contents inside the folder)
```

---

**A15**
```python
# Prerequisite: AKV-backed secret scope named "voltgrid-kv" must exist
# The key "db-password" must be in the Key Vault

password = dbutils.secrets.get(scope="voltgrid-kv", key="db-password")

# Safe to use the value — it will show as [REDACTED] if printed
# Pass it to a connection without printing
jdbc_url = f"jdbc:sqlserver://myserver.database.windows.net;password={password}"
```

Note: even `print(password)` in a notebook shows `[REDACTED]` — Databricks masks secrets in output automatically.

---

**A16**
The string comes from `dbutils.notebook.exit("some string")` in the **child notebook**.

```python
# child notebook (child.py):
result = "Processed 5000 rows"
dbutils.notebook.exit(result)

# parent notebook:
output = dbutils.notebook.run("./child", timeout_seconds=300, arguments={"env": "prod"})
print(output)  # prints: "Processed 5000 rows"
```

If the child notebook does not call `dbutils.notebook.exit(...)`, the return value is an empty string. If the child throws an exception, `dbutils.notebook.run` raises an exception in the parent.

---

## Databricks Jobs

**A17**
| | Manual notebook run | Databricks Job |
|---|---|---|
| Who triggers it | A human clicks Run All | Scheduler, API, or event |
| Compute | All-purpose cluster (billed while running, even idle) | Job cluster (auto-terminates, cheaper DBU rate) |
| Monitoring | Watch cells in the browser | Job run history, alerts, logs |
| User dependency | Requires a logged-in user | Runs as service principal, unattended |
| Production suitable | No | Yes |

---

**A18**
Recommended: **new job cluster** for each job run.

Reasons:
- Auto-terminates when the job finishes → no idle billing
- DBU rate is ~half the all-purpose rate for the same VM
- Isolated — no resource contention with interactive users
- Configuration is version-controlled inside the job definition

Using an existing all-purpose cluster is acceptable only for quick ad-hoc test runs — never for production scheduled jobs.

---

**A19**
`0 2 * * *` means: at minute 0 of hour 2, every day of the month, every month, every day of the week → **every day at 2:00 AM UTC**.

To run every Monday at 6:30 AM:
```
30 6 * * 1
```
Fields: minute=30, hour=6, day=*, month=*, weekday=1 (Monday).

---

**A20**
The job cluster is a **new cluster** created for the job run. It does not inherit Spark configuration from the dev cluster.

The dev cluster has ADLS Gen2 OAuth credentials in its Spark config (set in the cluster's advanced Spark config, or set by a notebook cell that ran once on that cluster). The job cluster starts with a blank Spark config — those credentials are not there.

Fix options:
1. Add the Spark config entries to the job cluster configuration (cluster definition → Spark config section)
2. Set the Spark config at the start of the notebook using `dbutils.secrets` to read credentials:
   ```python
   spark.conf.set("fs.azure.account.oauth2.client.secret...", 
                  dbutils.secrets.get("voltgrid-kv", "sp-client-secret"))
   ```
3. Use Unity Catalog external locations — no manual Spark config needed

---

**A21**
With `Max retries: 3` and `Retry interval: 5 minutes`:
- Attempt 1: fails immediately
- Wait 5 minutes → Attempt 2: fails
- Wait 5 minutes → Attempt 3: fails
- Wait 5 minutes → Attempt 4 (final): fails

Total attempts: **4** (1 original + 3 retries)
Total time: at least 15 minutes of waiting (3 × 5 min intervals) plus the duration of each attempt

After 4 failures, the job is marked as **FAILED** and no more retries occur.

---

**A22**
**Logs tab:** shows the full notebook output — every cell's stdout, stderr, and display output, exactly as you would see if running the notebook manually. Also includes any `print()` statements and `dbutils.notebook.exit()` value.

**Spark UI tab:** opens the Apache Spark web UI for that job run cluster — shows:
- Jobs (Spark jobs triggered within the notebook)
- Stages (map/reduce stages)
- Tasks (individual partition-level tasks)
- Storage (cached RDDs/DataFrames)
- Executors (per-node metrics: CPU, memory, shuffle)

Logs = what your code said. Spark UI = how Spark executed it.

---

**A23**
It depends on the job's **Concurrent run policy**:

- **Allow concurrent runs (default):** the 1 AM run starts even though the midnight run is still going. Both run simultaneously on separate clusters. This can cause data conflicts (two jobs writing to the same Delta table at the same time).

- **Skip new run if already running:** the 1 AM run is skipped. The midnight run continues. Safe for most data pipelines.

- **Wait for the current run to finish:** the 1 AM run is queued and starts as soon as the midnight run completes.

Best practice for data pipelines: set to **Skip** or **Wait** — never allow concurrent runs when both jobs write to the same output path.

---

## Multi-Task Jobs (Pipelines)

**A24**
"Depends on" creates an execution dependency between two tasks. Task B "depends on" Task A means:
- Task B does not start until Task A has completed
- If Task A fails, Task B does not run (it is marked as SKIPPED or UPSTREAM_FAILED)
- If Task A succeeds, Task B starts immediately after

It is the equivalent of ADF's "On success" dependency condition between activities.

---

**A25**
Task C does not run. It is marked as **UPSTREAM_FAILED** or **SKIPPED** because its dependency (Task B) did not succeed.

The job overall is marked as **FAILED** because at least one task failed.

To fix: investigate and fix Task B → re-run the job from Task B (if Databricks allows partial re-run) or re-run the entire job.

---

**A26**
DAG:
```
        [Task A]
       /         \
  [Task B]     [Task C]
```

Task B and Task C both depend on Task A but not on each other — they run **in parallel** immediately after Task A completes. This is the recommended pattern for independent Silver transforms (e.g., silver_payments and silver_sessions can process simultaneously after the same bronze ingest).

---

**A27**
Use an **Instance Pool** for the job cluster.

A pool keeps pre-warmed VMs ready. When the job cluster is created, it draws from the pool instead of provisioning new Azure VMs — startup drops from 3–5 minutes to ~30–60 seconds.

Steps:
1. Create a pool: Compute → Pools → Create pool → `voltgrid-pool-prod` → min idle = 2, D4s_v3
2. In the job's cluster config: Node type → Instance pool → select `voltgrid-pool-prod`

Now each task's cluster starts in ~30 seconds from the pool instead of 5 minutes from scratch. For a 3-task pipeline, this saves ~12–13 minutes of startup overhead.

---

## Parameters & Integration

**A28**
`{{job.start_time.iso_date}}` is a Databricks dynamic value (job parameter substitution). At runtime, Databricks replaces it with the actual start date of the job run in ISO format, e.g., `2024-01-17`.

The notebook reads it with:
```python
dbutils.widgets.text("run_date", "", "Run Date")
run_date = dbutils.widgets.get("run_date")
# run_date is now "2024-01-17"
```

Other dynamic values:
- `{{job.id}}` — the job ID
- `{{run_id}}` — the run ID
- `{{job.start_time.epoch_milliseconds}}` — Unix timestamp in ms

---

**A29**
- `dbutils.widgets.text("key", "default", "label")` **creates** the widget — it registers it with a default value and makes the text box appear in the notebook UI. This must be called before `get()`.
- `dbutils.widgets.get("key")` **reads** the current value of the widget — either what the user typed in the UI or what the job injected.

If `get()` is called without `text()` first (i.e., the widget was never created), Databricks raises:
```
com.databricks.dbutils_v1.widgets.WidgetNotFoundException: No widget named "key" found
```

Order matters: always define widgets before reading them. A common pattern is to put all widget definitions in Cell 1 of the notebook.

---

**A30**
**Option 1: ADF Databricks Notebook Activity**

ADF has a built-in **Databricks Notebook Activity**. After the Bronze Copy Activity succeeds:
- Add a Databricks Notebook Activity → point to `silver_transform` notebook
- After Silver succeeds → add another Databricks Notebook Activity → point to `gold_aggregation`

The ADF pipeline controls the sequence: Copy → Silver → Gold.

**Option 2: ADF Web Activity → Databricks Jobs API**

ADF Web Activity calls the Databricks REST API:
```
POST https://<workspace-url>/api/2.1/jobs/run-now
Body: {"job_id": 12345}
```
This triggers the Databricks `voltgrid_pipeline_daily` multi-task job. ADF waits for the API response (or polls for completion). Silver and Gold are handled by the Databricks job internally.

**Recommendation: Option 1 (Databricks Notebook Activity)** for most teams.

Reason: It is simpler to configure, uses native ADF integration, and the full pipeline is visible in ADF Monitor. No REST API setup or polling logic needed.

Option 2 is better when: the Silver+Gold logic is complex (many tasks, retries, parallel branches) and is better managed inside Databricks Workflows rather than ADF. Use Option 2 when the ADF pipeline is purely an orchestrator and Databricks owns the compute details.
