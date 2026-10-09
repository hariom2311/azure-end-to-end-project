# Day 7 — ADF + Azure Databricks Integration

> **Goal:** Connect Azure Data Factory to a Databricks workspace using all three authentication methods available in the ADF Linked Service form, then trigger Databricks notebooks from ADF pipelines. Every step is shown as it appears in the actual Azure UI.
>
> **Resources used in this project:**
> - ADF instance: `adf-ev-dev` (Resource Group: `data-engineering-daily-grp`)
> - Databricks workspace: `dbw-ev-dev`
> - Storage: `stadlsdev001` (ADLS), `stblobdev001` (Blob)
> - Subscription ID: `81dd57e1-876a-4fcc-8778-e06f68c13228`
> - Tenant ID: `c8fe40ce-7c95-4958-8992-21dfb0ea6c3c`

---

## Part 1: How ADF Connects to Databricks — Overview

When you add a Databricks Notebook Activity to an ADF pipeline, ADF needs a **Linked Service** that tells it how to reach the Databricks workspace and how to authenticate.

```
Azure Data Factory (adf-ev-dev)
  └── Pipeline
        └── Databricks Notebook Activity
              ├── Linked Service  ← authenticates ADF → Databricks
              │     └── Authentication method (choose one):
              │           ├── Option 1: Access token (PAT)
              │           ├── Option 2: System-assigned managed identity
              │           └── Option 3: User-assigned managed identity
              ├── Notebook path   ← path inside Databricks workspace
              ├── Cluster config  ← New job cluster (auto-provisioned per run)
              └── Base parameters ← key-value pairs passed to notebook as widgets
```

**Which method to use:**

| Method | Best for | Requires |
|---|---|---|
| Access token | Quick setup, dev/testing | PAT token from Databricks |
| System-assigned managed identity | Production, no secrets to rotate | ADF identity granted Contributor role on Databricks workspace |
| User-assigned managed identity | Multi-resource reuse, org-wide identity | Custom managed identity assigned to ADF, granted Contributor |

> **Cluster:** All three methods support **New job cluster** — ADF auto-provisions a fresh cluster per run and terminates it after. This is the recommended production setup. No existing running cluster is required.

> **Finding the Linked Service in ADF:** In the New Linked Service search, type `Databricks` and look under the **Compute** category. Select **Azure Databricks** from there. This is the correct connector for running notebooks.

---

## Part 2: Prepare the Databricks Notebook

Create the notebook that ADF will trigger. This notebook reads parameters that ADF passes in, does some work, and returns a result.

### Step 1 — Open Databricks Workspace

1. Azure Portal → search `dbw-ev-dev` → click it
2. Click **Launch Workspace**
3. You are now inside the Databricks workspace

### Step 2 — Create the Notebook

1. Left sidebar → **Workspace**
2. Click **Workspace** in the left tree → navigate to `Shared`
3. Right-click `Shared` → **Create** → **Folder**
4. Folder name: `day7-adf-practice`
5. Click **Create**
6. Right-click the new folder → **Create** → **Notebook**
7. Name: `adf_triggered_notebook`
8. Default language: `Python`
9. Click **Create**

> **Why Shared/?** ADF's managed identity and Service Principal both have access to `Shared/` by default. If you put the notebook in `Users/your-email/`, you must grant explicit permissions to the ADF identity — more steps.

### Step 3 — Add Code to the Notebook

**Cell 1 — Read ADF parameters:**

```python
# ADF passes key-value pairs as Base Parameters → they arrive as notebook widgets
dbutils.widgets.text("adf_pipeline_name", "unknown", "ADF Pipeline Name")
dbutils.widgets.text("adf_run_id",        "unknown", "ADF Run ID")
dbutils.widgets.text("env",               "dev",     "Environment")

pipeline_name = dbutils.widgets.get("adf_pipeline_name")
run_id        = dbutils.widgets.get("adf_run_id")
env           = dbutils.widgets.get("env")

print(f"Triggered by pipeline : {pipeline_name}")
print(f"ADF Run ID            : {run_id}")
print(f"Environment           : {env}")
```

**Cell 2 — Do some processing work:**

```python
# Simulate data processing
data = [(i, f"record_{i}", i * 100) for i in range(1, 6)]
df = spark.createDataFrame(data, ["id", "label", "value"])
df.show()

row_count = df.count()
print(f"Processed {row_count} records in [{env}] environment")
```

**Cell 3 — Return result to ADF:**

```python
# ADF captures this string as activity output: runOutput
result = f"SUCCESS: {row_count} records processed in {env} | pipeline={pipeline_name}"
dbutils.notebook.exit(result)
```

Note the full notebook path — you will need it when configuring the ADF activity:
```
/Shared/day7-adf-practice/adf_triggered_notebook
```

---

## Part 3: Authentication Method 1 — Access Token (PAT)

This is the simplest method. You generate a Personal Access Token (PAT) in Databricks and paste it into the ADF Linked Service.

### 3.1 Generate a PAT Token in Databricks

1. Inside the Databricks workspace, click your **username** (top right corner)
2. Click **User Settings**
3. In the left menu, click **Developer**
4. Click **Access tokens**
5. Click **Generate new token**

The **Generate new token** dialog appears with two sections:

**Scope checkboxes** (select which APIs this token can call):

The form shows a scrollable list of scopes. For ADF to trigger notebooks and attach to a cluster, check these scopes:

| Scope | Why needed |
|---|---|
| `clusters` | ADF attaches to the existing cluster |
| `jobs` | ADF submits the notebook as a job run |
| `command-execution` | ADF executes commands on the cluster |

> **Simplest option for dev/testing:** check **`all APIs (not recommended)`** — the token can call everything. This is marked "not recommended" because it grants full access; use specific scopes in production.

**Comment field** (text box at the bottom of the dialog):

| Field | Value |
|---|---|
| Comment | `adf-linked-service` |

> There is no "Lifetime (days)" field visible in the current UI — the token does not expire unless your workspace admin has configured a maximum lifetime policy.

6. After selecting scopes and entering a comment, click **Generate**
7. **Copy the token immediately** — it is shown only once. If you close this dialog without copying, you must generate a new one.

The token looks like: `dapi<32-character-hex-string>`

### 3.2 Create the Linked Service in ADF — Access Token

1. Azure Portal → search `adf-ev-dev` → click it
2. Click **Launch studio** (or **Open Azure Data Factory Studio**)
3. Left sidebar → click **Manage** (wrench/toolbox icon)
4. Under **Connections** → click **Linked services**
5. Click **+ New**
6. In the search box type `Databricks`
7. Look under the **Compute** category in the results — select **Azure Databricks** → click **Continue**

Fill in the form:

**Name:**
```
ls_databricks_pat
```

**Connect via integration runtime:**
- Leave as `AutoResolveIntegrationRuntime`

**Authentication method:**
- Select `Access token` from the dropdown

**Account selection method:**
- Click `From Azure subscription`

| Field | Value |
|---|---|
| Azure subscription | select `81dd57e1-876a-4fcc-8778-e06f68c13228` (DataEngineeringDaily) |
| Databricks workspace | select `ev-project-workspace` |

After selecting the workspace, the form auto-fills:
- **Databricks Workspace URL** — shown read-only (e.g. `https://adb-7405612713187126.6.azuredatabricks.net`)

**Access token field:**

Two sub-options appear side by side:

| Option | When to use |
|---|---|
| **Access token** (tab) | Paste the PAT token directly into the form |
| **Azure Key Vault** (tab) | Reference the token stored as a Key Vault secret |

Click the **Access token** tab and paste your PAT token in the field.

**Select cluster:**
- Select `New job cluster`

ADF auto-provisions a fresh cluster each time the pipeline runs and terminates it after the notebook finishes. No pre-existing running cluster is needed.

| Field | Value |
|---|---|
| Databricks runtime version | `15.4 LTS (Scala 2.12, Spark 3.5.0)` |
| Worker node type | `Standard_D4s_v3` |
| Driver node type | `Standard_D4s_v3` |
| Workers | `1` |

8. Click **Test connection** → wait for `Connection successful`
9. Click **Apply**

---

## Part 4: Authentication Method 2 — System-Assigned Managed Identity

No tokens, no secrets. ADF has its own identity in Azure Active Directory (Entra ID). You grant that identity access to the Databricks workspace.

### 4.1 Understand What System-Assigned Managed Identity Is

```
Azure Data Factory (adf-ev-dev)
  └── System-assigned managed identity
        ├── Automatically created when ADF is provisioned
        ├── Has its own Object ID in Azure AD / Entra ID
        ├── Tied to the ADF resource — deleted when ADF is deleted
        └── Cannot be shared with other resources
```

ADF's managed identity needs permission to access the Databricks workspace. There are **two things** required — an Azure IAM role AND a Databricks workspace-level permission.

### 4.2 Find ADF's Managed Identity Object ID

The form auto-fills this after you select the workspace (visible as **Managed identity object ID** on the form). But you can also find it here:

1. Azure Portal → search `adf-ev-dev` → click it
2. Left menu → **Properties** (under Settings)
3. Find **Managed Identity** section → copy the **Object ID**

The Object ID shown on the form is: `3d986697-f629-478e-a50b-f77716732feb`

### 4.3 Grant ADF Identity Access to Databricks Workspace

Grant the ADF system-assigned managed identity the **Contributor** role on the Databricks workspace resource via Azure Portal IAM:

1. Azure Portal → search `ev-project-workspace` → click the Databricks workspace resource
2. Left menu → **Access control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. **Role tab:**
   - Click **Job function roles** tab
   - Search `Contributor`
   - Select **Contributor** → click **Next**
5. **Members tab:**
   - **Assign access to:** `Managed identity`
   - Click **+ Select members**
   - **Subscription:** select your subscription
   - **Managed identity:** `Data factory (V2)`
   - From the list, select `adf-ev-dev`
   - Click **Select**
6. Click **Review + assign** → **Review + assign** again

Wait 1–2 minutes for the role assignment to propagate before clicking Test connection.

### 4.4 Create the Linked Service — System-Assigned Managed Identity

1. ADF Studio → **Manage** → **Linked services** → **+ New**
2. Search `Databricks` → under the **Compute** category, select **Azure Databricks** → **Continue**

Fill in the form:

**Name:**
```
ls_databricks_system_mi
```

**Authentication method:**
- Select `System-assigned managed identity`

**Account selection method:**
- `From Azure subscription`

| Field | Value |
|---|---|
| Azure subscription | `81dd57e1-876a-4fcc-8778-e06f68c13228` (DataEngineeringDaily) |
| Databricks workspace | select `ev-project-workspace` |

After selecting the workspace, the form auto-fills three read-only fields:

| Read-only field | What it shows |
|---|---|
| **Databricks Workspace URL** | `https://adb-7405612713187126.6.azuredatabricks.net` |
| **Managed identity name** | `adf-datalake-dev-ded` (ADF's own system-assigned identity name) |
| **Managed identity object ID** | `3d986697-f629-478e-a50b-f77716732feb` (same value used in Step 4.3 IAM) |

> The form also shows a reminder: *"Grant Data Factory service managed identity access to your Azure Databricks Delta Lake."* This is just Microsoft's internal label — you have already done this in Step 4.3 via Contributor role.

**Select cluster:**
- Select `New job cluster`

| Field | Value |
|---|---|
| Databricks runtime version | `15.4 LTS (Scala 2.12, Spark 3.5.0)` |
| Worker node type | `Standard_D4s_v3` |
| Driver node type | `Standard_D4s_v3` |
| Workers | `1` |

3. Click **Test connection** → wait for `Connection successful`
   - If it fails with `403`: the Contributor role from Step 4.3 has not propagated yet — wait 2 minutes and retry
4. Click **Apply**

---

## Part 5: Authentication Method 3 — User-Assigned Managed Identity

A user-assigned managed identity is an independent Azure resource you create separately. You can assign it to multiple resources (ADF, VM, Function App). It persists even if ADF is deleted.

### 5.1 Understand What User-Assigned Managed Identity Is

```
User-Assigned Managed Identity (mi-adf-databricks)
  ├── Independent Azure resource — has its own Object ID in Entra ID
  ├── Can be assigned to: ADF, VMs, Function Apps, Logic Apps, etc.
  ├── Persists when the ADF resource is deleted
  └── Useful for org-wide identity shared across multiple pipelines/resources
```

### 5.2 Create the User-Assigned Managed Identity

1. Azure Portal → click **+ Create a resource** (top left)
2. Search `managed identity`
3. Select **User Assigned Managed Identity** → click **Create**
4. Fill in:
   - **Subscription:** `81dd57e1-876a-4fcc-8778-e06f68c13228`
   - **Resource group:** `data-engineering-daily-grp`
   - **Region:** same as your ADF and Databricks (e.g. `East US`)
   - **Name:** `mi-adf-databricks`
5. Click **Review + create** → **Create**
6. Wait for deployment → click **Go to resource**
7. On the resource page, copy:
   - **Client ID** (also called Application ID) — you need this in ADF

### 5.3 Assign the Identity to ADF

1. Azure Portal → search `adf-ev-dev` → click it
2. Left menu → **Identity** (under Settings)
3. Click the **User assigned** tab
4. Click **+ Add**
5. Search for `mi-adf-databricks` → select it → click **Add**

ADF can now use this identity when making requests.

### 5.4 Grant the User-Assigned Identity Access to Databricks Workspace

Grant the user-assigned managed identity the **Contributor** role on the Databricks workspace resource — same approach as Part 4.3:

1. Azure Portal → search `ev-project-workspace` → click the Databricks workspace resource
2. Left menu → **Access control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. **Role tab:** search `Contributor` → select **Contributor** → click **Next**
5. **Members tab:**
   - **Assign access to:** `Managed identity`
   - Click **+ Select members**
   - **Managed identity:** `User-assigned managed identity`
   - Select `mi-adf-databricks` → click **Select**
6. Click **Review + assign** → **Review + assign** again

Wait 1–2 minutes for the role assignment to propagate.

### 5.5 Create the Linked Service — User-Assigned Managed Identity

1. ADF Studio → **Manage** → **Linked services** → **+ New**
2. Search `Databricks` → under the **Compute** category, select **Azure Databricks** → **Continue**

Fill in the form:

**Name:**
```
ls_databricks_user_mi
```

**Authentication method:**
- Select `User-assigned managed identity`

**Account selection method:**
- `From Azure subscription`

| Field | Value |
|---|---|
| Azure subscription | `81dd57e1-876a-4fcc-8778-e06f68c13228` |
| Databricks workspace | select `ev-project-workspace` |

**Credentials:**

A Credential object in ADF wraps the user-assigned managed identity. If you do not have one yet:
1. Click **+ New** next to the Credentials dropdown
2. **Name:** `cred-mi-adf-databricks`
3. **Type:** `User-assigned managed identity`
4. **Managed identity resource:** select `mi-adf-databricks`
5. Click **Create**

Then select `cred-mi-adf-databricks` from the Credentials dropdown.

**Workspace resource ID:**

This field is NOT auto-filled for user-assigned MI — paste it manually.

To find it:
1. Azure Portal → search `ev-project-workspace` → click the Databricks workspace resource
2. Left menu → **Properties**
3. Copy the **Resource ID**:
   ```
   /subscriptions/81dd57e1-876a-4fcc-8778-e06f68c13228/resourceGroups/data-engineering-daily-grp/providers/Microsoft.Databricks/workspaces/ev-project-workspace
   ```
4. Paste it into the **Workspace resource ID** field

**Select cluster:**
- Select `New job cluster`

| Field | Value |
|---|---|
| Databricks runtime version | `15.4 LTS (Scala 2.12, Spark 3.5.0)` |
| Worker node type | `Standard_D4s_v3` |
| Driver node type | `Standard_D4s_v3` |
| Workers | `1` |

3. Click **Test connection** → wait for `Connection successful`
   - If it fails with `403`: Contributor role from Step 5.4 has not propagated yet — wait 2 minutes and retry
4. Click **Apply**

---

## Part 6: Create an ADF Pipeline with a Databricks Notebook Activity

Now that you have at least one Linked Service, create a pipeline that triggers the notebook.

### Step 1 — Create a New Pipeline

1. ADF Studio → left sidebar → **Author** (pencil icon)
2. Under **Pipelines** section → click **⋮** (three dots) → **New pipeline**
   - Or click **+** next to Pipelines → **New pipeline**
3. Pipeline name: `pl_day7_notebook_demo`

### Step 2 — Add a Databricks Notebook Activity

1. In the **Activities** panel (left side of the canvas), find the **Databricks** section
2. Expand it → drag **Notebook** onto the canvas

The activity appears as a blue box. Click it to select it.

### Step 3 — Configure the Activity (Three Tabs at the Bottom)

**General tab:**

| Field | Value |
|---|---|
| Name | `Run Notebook` |
| Timeout | `00:30:00` |
| Retry | `1` |

**Azure Databricks tab:**

| Field | Value |
|---|---|
| Databricks linked service | select `ls_databricks_pat` (or whichever method you set up) |

**Settings tab:**

| Field | Value |
|---|---|
| Notebook path | `/Shared/day7-adf-practice/adf_triggered_notebook` |

Click **Browse** to pick the notebook from the workspace file tree — or type the path directly.

**Base parameters** (still in Settings tab):

Click **+ New** for each parameter:

| Name | Value |
|---|---|
| `adf_pipeline_name` | `@pipeline().Pipeline` |
| `adf_run_id` | `@pipeline().RunId` |
| `env` | `dev` |

> `@pipeline().Pipeline` and `@pipeline().RunId` are ADF dynamic expressions — they resolve to the actual pipeline name and run ID at runtime.

### Step 4 — Save the Pipeline

Click **Save all** (floppy disk icon, top left) or press `Ctrl+S`.

---

## Part 7: Run the Pipeline and Verify Output

### 7.1 Debug Run (Test Without Publishing)

1. In the pipeline canvas, click **Debug** (triangle with bug icon, top toolbar)
2. A dialog may appear asking for parameter values — click **OK**
3. The **Output** panel appears at the bottom

Watch the activity:
- **Blue spinning circle** = cluster is starting (3–5 min) or notebook is running
- **Green checkmark** = activity succeeded
- **Red X** = activity failed

### 7.2 View the runOutput

When the activity turns green:
1. In the **Output** tab at the bottom, find the `Run Notebook` row
2. Click the **glasses icon** (Output) on the right side of the row

You see the full JSON output:

```json
{
  "runOutput": "SUCCESS: 5 records processed in dev | pipeline=pl_day7_notebook_demo",
  "effectiveIntegrationRuntime": "AutoResolveIntegrationRuntime",
  "executionDuration": 318,
  "durationInQueue": {
    "integrationRuntimeQueue": 0
  }
}
```

`runOutput` is the exact string the notebook returned via `dbutils.notebook.exit(result)`.

### 7.3 Reference runOutput in a Downstream Activity

If you have a second activity (e.g. a Send Email or another Notebook) that needs the notebook's output:

```
@activity('Run Notebook').output.runOutput
```

Paste this ADF expression in any downstream activity field that accepts dynamic content.

### 7.4 Verify in Databricks Job Runs

1. Databricks workspace → left sidebar → **Workflows**
2. Click **Job runs** tab
3. Find the run with source `ADF` — it shows:
   - Which ADF pipeline triggered it
   - Start time, duration
   - Status (Succeeded / Failed)
4. Click the run → click **Logs** to see all notebook cell output

---

## Part 8: Add a Dependency Arrow Between Activities

A dependency arrow controls whether the next activity runs based on the result of the previous one.

### Step 1 — Add a Copy Activity Before the Notebook

1. In the Activities panel → **Move & Transform** → drag **Copy data** onto the canvas
2. Place it to the LEFT of the Notebook activity

### Step 2 — Draw the Arrow

Hover over the Copy activity — a small green circle appears on the right edge. Drag from that circle to the Notebook activity. Release on top of the Notebook box.

A **green arrow** is drawn = "Run Notebook only if Copy Data **succeeded**".

### Step 3 — Change Arrow Type (Optional)

Click the arrow to select it → a small panel appears showing the condition:

| Arrow color | Condition | When to use |
|---|---|---|
| Green | On Success | Run next activity only if this one succeeded |
| Red | On Failure | Run next activity only if this one failed (error handling) |
| Blue | On Completion | Run next activity always, regardless of success/failure |
| Yellow | On Skipped | Run next activity only if this one was skipped |

Right-click the arrow → **Add success condition** / **Add failure condition** to change it.

---

## Part 9: Schedule the Pipeline

To run the pipeline daily (e.g. at 6 AM):

1. In the pipeline canvas, click **Add trigger** (top toolbar, lightning bolt icon)
2. Click **New/Edit**
3. Click **+ New** to create a new trigger

Fill in:

| Field | Value |
|---|---|
| Name | `trigger_daily_6am` |
| Type | `Schedule` |
| Start date | today's date |
| Time zone | `UTC+05:30 Chennai, Kolkata, Mumbai, New Delhi` |
| Recurrence | Every `1` `Day` |
| Execute at these times (hours) | `6` |
| Execute at these times (minutes) | `0` |

4. Click **OK**
5. Click **OK** again on the pipeline trigger dialog
6. Click **Publish all** (top toolbar) to activate the trigger

> **Important:** Triggers only activate AFTER you click **Publish all**. Debug runs do not trigger schedules.

---

## Part 10: New Job Cluster vs Existing Cluster

All three auth methods support both cluster options. **New job cluster** is recommended for production.

| Option | What happens | Startup time | Cost |
|---|---|---|---|
| **New job cluster** | ADF creates a fresh cluster per run, terminates after | 3–5 minutes | Pay only during the run |
| Existing interactive cluster | ADF attaches to a cluster already running | ~10 seconds | Cluster billed hourly even when idle |

### New Job Cluster — How it Works

When you select `New job cluster` in the Linked Service form:
- ADF provisions a cluster automatically when the pipeline runs
- The notebook executes on that cluster
- ADF terminates the cluster when the notebook finishes
- No manual cluster management needed

### Cluster Access Mode for New Job Cluster

The new job cluster ADF creates uses **Single User** access mode by default. This is fine — the cluster is dedicated to that one run and terminated after.

For an existing cluster used by multiple ADF runs or users:

| Access Mode | Works with ADF? | Notes |
|---|---|---|
| Single User | Yes — if assigned to the ADF identity | ADF's identity must match the cluster's assigned user |
| Shared | Yes — any identity can use it | Best for shared/team clusters |
| No isolation shared | Yes (legacy) | Avoid — no Unity Catalog support |

---

## Part 11: Where Service Principal Fits

You may wonder: where does a Service Principal (App Registration) get used if the ADF UI only shows managed identity options?

**Service Principal IS available** but not through the three options in the screenshot you saw. It appears under a different approach:

```
ADF Linked Service for Databricks
  ├── Access token          ← what the UI shows
  ├── System-assigned MI    ← what the UI shows
  ├── User-assigned MI      ← what the UI shows
  └── (Service Principal via API / ARM template / Terraform — not shown in UI dropdown)
```

**Where Service Principal IS used in this project:**

| Use case | Where SP is used |
|---|---|
| Key Vault access (legacy) | spark.conf.set with client ID and secret |
| Databricks secret scope backed by Key Vault | Secret scope creation references SP |
| ADF Linked Service (Terraform/ARM) | `servicePrincipalId` + `servicePrincipalKey` in JSON |
| Databricks CLI auth | `databricks configure --aad-token` with SP token |
| Unity Catalog (Azure) | NOT used — Azure uses Access Connector (managed identity) |

**In the UI today (as you saw):** the ADF → Databricks Linked Service form shows only the three managed identity / access token methods. If your company policy requires Service Principal, it is configured via:
1. ADF Linked Service JSON (Edit → view JSON → add `servicePrincipalId` and `servicePrincipalKey` fields)
2. Terraform / Bicep ARM templates

For this project, use the Access token (PAT) method for simplicity.

---

## Quick Reference — Day 7

```
Term                             Definition
────────────────────────────────────────────────────────────────────────────────
Linked Service                   Connection object in ADF that holds endpoint
                                 + auth config for an external resource

Access Token (PAT)               Personal Access Token generated from Databricks
                                 User Settings → Developer → Access tokens

System-assigned managed identity ADF's built-in identity in Entra ID, tied
                                 to the ADF resource lifecycle

User-assigned managed identity   An independent Entra ID identity resource
                                 that can be shared across multiple Azure services

Contributor role                 Azure RBAC role granting read/write access
                                 on the Databricks workspace resource

Base Parameters                  Key-value pairs in ADF Notebook Activity →
                                 Settings tab that become notebook widgets

dbutils.widgets.get("key")       Reads an ADF Base Parameter value in Python
                                 (or dbutils.widgets.text() to declare first)

dbutils.notebook.exit("value")   Sends a string result back to ADF

runOutput                        The value from dbutils.notebook.exit(), found
                                 in activity Output JSON → key "runOutput"

New job cluster                  ADF auto-provisions a cluster per run (3–5 min
                                 startup), terminates it after — all three auth
                                 methods support this option; recommended for prod

Existing interactive cluster     ADF attaches to an already-running cluster;
                                 near-instant start but billed even when idle

Workspace resource ID            Azure resource ID of the Databricks workspace —
                                 required field for user-assigned MI auth;
                                 auto-filled for system-assigned MI and access token

Azure Databricks (Compute)       The correct ADF Linked Service connector for
                                 running notebooks — found under Compute category
                                 in the New Linked Service search

@pipeline().Pipeline             ADF dynamic expression → current pipeline name
@pipeline().RunId                ADF dynamic expression → current run's GUID

Dependency arrow                 Green/red/blue/yellow arrow between activities
                                 controlling execution order and conditions
```
