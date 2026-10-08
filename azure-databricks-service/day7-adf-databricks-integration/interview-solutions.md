# Day 7 — Interview Solutions: ADF + Databricks Integration

---

## ADF Linked Service & Authentication

**A1**
A Linked Service is a connection object in ADF that holds the endpoint URL of an external resource and the credentials to authenticate against it. For Databricks, it stores: (1) the Databricks workspace URL (resolved from the subscription+workspace selection), and (2) the authentication credential — either a PAT token, or a reference to a managed identity.

---

**A2**

| Option | What makes it different |
|---|---|
| Access token | Uses a Databricks Personal Access Token (PAT) — a static secret you generate and paste in |
| System-assigned managed identity | Uses ADF's built-in Azure identity — no secret to manage; identity tied to the ADF resource |
| User-assigned managed identity | Uses an independent Azure managed identity resource you create and assign to ADF |

---

**A3**
PAT tokens have an expiry date. After 3 months (or whatever lifetime you set), the token expired and ADF can no longer authenticate. Fix: generate a new PAT token in Databricks (User Settings → Developer → Access tokens), go to ADF → Manage → Linked services → edit `ls_databricks_*` → paste the new token → Test connection → Apply → Publish. No downtime: the new token takes effect on the next pipeline run.

**Better long-term fix:** Switch to system-assigned managed identity — no expiry, no rotation needed.

---

**A4**
Using a personal PAT token means: (1) if the engineer leaves the company and their account is disabled, the ADF pipelines will break immediately; (2) the token is tied to the individual's permissions — if their access is reduced, pipelines fail; (3) tokens must be manually rotated before they expire, and one person owns that rotation. Best practice: create a service account or use managed identity for ADF connections.

---

**A5**
A system-assigned managed identity is an identity in Azure Active Directory (Entra ID) that Azure automatically creates and manages for a specific resource — in this case, ADF. It is tied to the ADF resource's lifecycle: when the ADF resource is deleted, the managed identity is automatically deleted with it.

---

**A6**
Test connection checks that ADF can reach the Databricks workspace endpoint and authenticate. The Notebook activity fails because the managed identity has not been granted permission on the specific notebook or folder. Solution: either put the notebook in `Shared/` (accessible by default), or go to the notebook → right-click → Permissions → grant the ADF managed identity `Can Run` permission.

---

**A7**

| | System-assigned | User-assigned |
|---|---|---|
| Created | Automatically when ADF is provisioned | Manually as a standalone Azure resource |
| Scope | Tied to one ADF resource | Can be assigned to multiple resources |
| Lifecycle | Deleted when ADF is deleted | Independent — persists when ADF is deleted |
| Sharing | Cannot share across ADF instances | Can be shared across ADF, VM, Functions, etc. |

**Use user-assigned when:** multiple ADF instances, VMs, or other services all need access to the same Databricks workspace — create one identity, grant it once, assign to all.

---

**A8**
With five system-assigned identities, you must grant the Contributor role five times on the Databricks workspace (once per identity). If the workspace access policy changes, you update five entries. With one user-assigned identity shared across all five ADF instances: grant the role once, manage one identity, one rotation point, one audit trail. Easier to manage at scale.

---

**A9**
1. Azure Portal → search `dbw-ev-dev` → click the Databricks workspace resource
2. Left menu → **Access control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. **Role tab:** search `Contributor` → select it → **Next**
5. **Members tab:** Assign access to `Managed identity` → **+ Select members** → find the ADF managed identity → **Select**
6. **Review + assign** → **Review + assign**

---

**A10**
The Test connection will fail with an error like `Authorization failed` or `The client does not have permission to perform action`. ADF's identity can reach the Databricks endpoint but cannot authenticate because it has no role on the workspace resource. Fix: grant the user-assigned managed identity the `Contributor` role on `dbw-ev-dev` via IAM.

---

## Notebook Activity & Parameters

**A11**
A Base Parameter is a key-value pair you define in the ADF Notebook Activity → Settings tab. ADF passes it to the notebook as a widget. The notebook reads it with:

```python
dbutils.widgets.text("key", "default_value", "label")
value = dbutils.widgets.get("key")
```

The `dbutils.widgets.text()` call declares the widget; `dbutils.widgets.get()` reads the current value. When ADF runs the notebook with a Base Parameter, the ADF-supplied value overrides the default.

---

**A12**

```python
# Declare the widget (default is used only when running notebook manually)
dbutils.widgets.text("batch_date", "2024-01-01", "Batch Date")

# Read the value — ADF supplies the actual date at runtime
batch_date = dbutils.widgets.get("batch_date")

print(f"Processing date: {batch_date}")
```

`@formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')` is an ADF expression that resolves before the notebook runs — the notebook receives the already-formatted date string like `2024-01-15`, not the expression.

---

**A13**
`env` holds `prod` — the value ADF passed in. Here is why: `dbutils.widgets.text("env", "dev", "Environment")` DECLARES the widget with a default. When ADF passes `env = prod` as a Base Parameter, it overrides the default. `dbutils.widgets.get("env")` then returns `prod`. The line `env = "dev"` — if written AFTER the widget declaration — would reassign the Python variable to `"dev"`, overwriting the widget-read value. But `dbutils.widgets.get("env")` always returns what ADF supplied (`prod`) until you explicitly reassign the variable.

**The safe pattern:** read the widget immediately and do not re-assign the variable.

---

**A14**
`dbutils.notebook.exit("DONE: 500 rows")` terminates the notebook immediately and sends the string `DONE: 500 rows` back to ADF as the activity's return value. In ADF, it appears in:
- Activity Output tab → click the glasses icon → `"runOutput": "DONE: 500 rows"`

ADF expression to use in a downstream activity:
```
@activity('Run Notebook').output.runOutput
```

---

**A15**
When Cell 3 executes `dbutils.notebook.exit("partial")`, the notebook exits immediately. Cells 4 and 5 do NOT run. This is by design — `dbutils.notebook.exit()` is a hard stop. ADF receives `"partial"` as `runOutput` and marks the activity as Succeeded (exit does not mean failure; failure is an uncaught exception). Use this for early-exit on empty input: check at the top if there's nothing to process, exit early, skip the rest.

---

**A16**
ADF will fail to evaluate the expression `@pipeline().parameters.environment` at runtime because the pipeline has no parameter named `environment`. The activity will fail with an expression evaluation error before the notebook even starts. Fix: either add a pipeline parameter named `environment` (Author → pipeline → Parameters tab → + New), or change the Base Parameter value to a static string like `dev`.

---

## Cluster Configuration

**A17**

| | New job cluster | Existing interactive cluster |
|---|---|---|
| Startup time | 3–5 minutes per run | ~10 seconds (cluster already running) |
| Cost | Pay only during the run | Cluster billed hourly even when idle |
| Isolation | Each run is isolated | Shared — multiple jobs compete for resources |
| Recommended for | Production scheduled jobs | Development / fast iteration |

**New job cluster is recommended for production** because: isolated execution, controlled cost (pay per run), cluster terminates after the job, no resource contention with other notebooks.

---

**A18**
Running every 5 minutes with `New job cluster` means 5 minutes startup overhead for a cluster that might run a 30-second notebook. That is 5 minutes wasted per run. At 5-min intervals, the cluster is starting a new one before the previous run's cluster is even ready — 12 runs per hour × 5-min startup = effectively the cluster never has idle time, but you're paying for 12 full cluster lifetimes per hour.

**Fix option 1:** Use `Existing interactive cluster` — instant start, always on, amortize the cost.
**Fix option 2:** Increase schedule interval to every 1 hour if near-real-time is not needed.
**Fix option 3:** Redesign the pipeline — accumulate data and process in larger batches less frequently.

---

**A19**
All 10 ADF runs share the same cluster's memory and CPU. If the cluster is undersized, jobs compete for executors. Spark tasks queue up. Jobs that normally take 2 minutes might take 10–15 minutes because they are waiting for executor slots. There is also a risk of out-of-memory errors if 10 notebooks each load large datasets. For production, use `New job cluster` per ADF run for isolation, or use a `Shared` access mode cluster with proper autoscaling.

---

## Pipeline Design & Scheduling

**A20**

| Arrow color | Condition | Example use case |
|---|---|---|
| Green | On Success | Run downstream transform only after data is loaded successfully |
| Red | On Failure | Trigger an error-handler notebook or send alert email when a step fails |
| Blue | On Completion | Always log run metadata regardless of success or failure |
| Yellow | On Skipped | Handle skipped activities in conditional pipelines |

**Red arrow real use case:** Copy Data activity fails → red arrow → Databricks Error Handler notebook runs → sends alert and writes failure record to error log table.

---

**A21**
Activity B fails → ADF evaluates the green arrow from B to C. Since B failed (not succeeded), the success condition is not met → Activity C is **skipped**. Final pipeline status: **Failed** (because at least one activity failed).

---

**A22**
Activity A succeeds. ADF evaluates both outgoing arrows from A:
- Red arrow (A → B): condition is "A failed" — A succeeded, so condition NOT met → B is skipped
- Green arrow (A → C): condition is "A succeeded" — A succeeded → C runs

Result: **Only C runs**. B is skipped. Pipeline status: Succeeded (C succeeded, B was just skipped).

---

**A23**
You must click **Publish all** in ADF Studio. Without publishing, the trigger exists only as a draft — it is not deployed to the ADF service and will never fire. If you forget this step, the trigger appears in the Manage → Triggers list but shows as a draft; it never runs. After clicking Publish all, the trigger becomes active and will fire on schedule.

---

## Monitoring & Troubleshooting

**A24**
Two most likely causes:

**Cause 1: Cluster startup time**
New job cluster takes 3–5 minutes to provision. If the cluster is slow to start (e.g. VM capacity constraints in the region), it can take 10–20+ minutes. To diagnose: go to Databricks workspace → Workflows → Job runs → find the ADF-triggered run → look at the timeline to see how long the cluster spent in "Pending" state vs "Running" state.

**Cause 2: Notebook running a long operation**
The notebook itself is slow — large data read, heavy transformation, network I/O to storage. To diagnose: open the job run in Databricks → click the run → see each cell's execution time. Identify which cell is taking long. Check Spark UI for long-running tasks.

---

**A25**
Full design:

**Authentication method:** System-assigned managed identity (no token management, production-grade)

**Step 1 — Linked Service:**
- Name: `ls_databricks_prod`
- Authentication: System-assigned managed identity
- Cluster: New job cluster (isolated per run)
- Runtime: 15.4 LTS, Standard_D4s_v3, 2 workers (autoscale min 2 max 4)

**Step 2 — Databricks notebook** (`Shared/pipelines/daily_transform`):
```python
# Cell 1: Read parameters
dbutils.widgets.text("batch_date", "", "Batch Date")
batch_date = dbutils.widgets.get("batch_date")

# Cell 2: Read CSV from volume
df = spark.read.option("header","true").csv(
    f"/Volumes/dev_catalog/bronze/blob_landing/data_{batch_date}.csv"
)

# Cell 3: Transform and write Delta
df_clean = df.dropna()
df_clean.write.format("delta").mode("overwrite").save(
    f"abfss://bronze@stadlsdev001.dfs.core.windows.net/daily/{batch_date}/"
)
row_count = df_clean.count()

# Cell 4: Return row count
dbutils.notebook.exit(str(row_count))
```

**Step 3 — Error handler notebook** (`Shared/pipelines/error_handler`):
```python
dbutils.widgets.text("batch_date", "", "Batch Date")
dbutils.widgets.text("error_msg", "", "Error Message")
batch_date = dbutils.widgets.get("batch_date")
error_msg  = dbutils.widgets.get("error_msg")
print(f"PIPELINE FAILED: batch_date={batch_date}, error={error_msg}")
# Could also write to error log table here
```

**Step 4 — Pipeline `pl_daily_transform`:**
- Activity 1: Databricks Notebook (`Run Daily Transform`)
  - Linked service: `ls_databricks_prod`
  - Notebook: `/Shared/pipelines/daily_transform`
  - Base parameters: `batch_date` = `@formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')`
- Activity 2: Databricks Notebook (`Run Error Handler`)
  - Connected to Activity 1 with a **red arrow** (On Failure)
  - Notebook: `/Shared/pipelines/error_handler`
  - Base parameters: `batch_date` = `@formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')`

**Step 5 — Read row count downstream:**
Add a Set Variable or Web activity after Activity 1 (with green arrow):
```
@activity('Run Daily Transform').output.runOutput
```
This gives the row count string returned by `dbutils.notebook.exit(str(row_count))`.

**Step 6 — Schedule trigger:**
- Name: `trigger_daily_7am`
- Type: Schedule
- Recurrence: Every 1 Day at 07:00 AM IST (UTC+05:30)
- Start date: today
- Click OK → **Publish all** to activate

---
