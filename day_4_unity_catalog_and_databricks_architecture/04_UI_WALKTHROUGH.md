# 04 — UI Walkthrough: Testing Every Architectural Concept in Databricks
**Day 4 | Step-by-step navigation guide — open Databricks UI and follow along**

---

## How to Use This Guide

Every section below is a self-contained test you can run directly in the Databricks UI. No notebook code is required unless stated. Each test confirms one architectural concept is correctly set up in our project.

**URL to open:** `https://adb-<workspace-id>.azuredatabricks.net` — your Databricks workspace URL.

---

## Test 1 — Verify the Unity Catalog Metastore Is Attached

**What you're testing:** The workspace is linked to a Unity Catalog metastore (not legacy Hive only).

**Steps:**
1. Open your Databricks workspace
2. Look at the left sidebar — do you see a **Catalog** tab (not "Data")?
   - YES — Unity Catalog is active
   - NO — workspace is on legacy mode or UC is not yet enabled
3. Click **Catalog** in the left sidebar
4. In the Catalog tree (top left panel), you should see the metastore name at the top: `dbw_ev_intelligence_dev`

**What you see if correctly set up:**
```
Catalog browser (left panel)
├── dbw_ev_intelligence_dev  ← metastore name shown at top
│     └── default
│           ├── Tables
│           └── Volumes
└── main  ← default catalog every metastore has
```

**To see metastore details:**
- Admin settings (top right user icon → Settings) → **Metastore** tab
- Shows: Metastore ID, region (Central India), delta sharing config
- If you don't see a Metastore tab in settings, you don't have admin rights — ask the workspace admin

---

## Test 2 — Browse the Catalog Object Tree

**What you're testing:** The 4-level namespace — catalog → schema → table/volume exists as expected.

**Steps:**
1. Left sidebar → **Catalog**
2. In the left panel tree, click **dbw_ev_intelligence_dev** (expand)
3. Click **default** (expand) — you should see:
   - **Tables** section
   - **Volumes** section
4. Under Tables, click **pipeline_audit** (if it exists)
   - You'll see: Schema tab, Sample Data tab, Details tab, Permissions tab, Lineage tab

**What each tab shows:**

**Schema tab:**
```
Column name       Type      Nullable
pipeline_name     STRING    true
load_type         STRING    true
watermark_value   STRING    true
ingestion_date    STRING    true
total_pages       STRING    true
status            STRING    true
pipeline_run_id   STRING    true
run_timestamp     STRING    true
```

**Details tab:**
```
Owner:           your user account
Table type:      MANAGED or EXTERNAL
Location:        abfss://... (if external) or UC-managed path (if managed)
Created:         timestamp
Last modified:   timestamp
```

**Sample Data tab:**
- Click it → Databricks runs a `SELECT * FROM ... LIMIT 20` automatically and shows results in a grid
- No notebook required — this is a UC feature

---

## Test 3 — Browse Volume Files Without a Notebook

**What you're testing:** The Bronze Volume is correctly registered and files are visible via the UI.

**Steps:**
1. Catalog → **dbw_ev_intelligence_dev** → **default** → **Volumes**
2. Click **bronze-volume**
3. You'll see a **Details** tab showing:
   ```
   Volume type:    External
   Storage location: abfss://bronze@evdatalakedev.dfs.core.windows.net/
   Owner:          your user
   ```
4. Click **Browse** (or the folder icon) — you'll see the root of the Bronze container:
   ```
   audit/
   realtime/
   api/
   ```
5. Navigate into: `realtime/` → `charging_sessions/` → `2026/` → `07/` → any day → any hour
6. You should see the CSV files copied by the migration notebook

**What this proves:** The External Volume correctly maps to your ADLS Bronze container. The Access Connector credential is working. You can verify job output without opening a notebook.

**If you see "Access Denied":** Your user doesn't have `READ VOLUME` on bronze-volume. Either grant it:
```sql
GRANT READ VOLUME ON VOLUME dbw_ev_intelligence_dev.default.`bronze-volume` TO `your.email@company.com`;
```
or ask the workspace admin to do so.

---

## Test 4 — Verify External Locations

**What you're testing:** All three ADLS containers (bronze/silver/gold) are registered as External Locations and the connection is healthy.

**Steps:**
1. Left sidebar → **Catalog**
2. In the top sub-navigation (horizontal tabs below the search bar), look for **External Data**
3. Click **External Locations**
4. You should see a list like:
   ```
   Name                   URL                                              Status
   evdatalakedev-bronze   abfss://bronze@evdatalakedev.dfs.core.windows.net/   ✓
   evdatalakedev-silver   abfss://silver@evdatalakedev.dfs.core.windows.net/   ✓
   evdatalakedev-gold     abfss://gold@evdatalakedev.dfs.core.windows.net/     ✓
   ```
5. Click **evdatalakedev-bronze** → on the right panel, click **Test connection**
6. You should see: `"Connection test succeeded"` — green checkmark

**If test fails:**
- The Access Connector's managed identity no longer has `Storage Blob Data Contributor` on the ADLS account
- Go to Azure Portal → `evdatalakedev` storage account → IAM → Role assignments → check `ac-ev-intelligence-dev` has the role

---

## Test 5 — Verify Storage Credential

**What you're testing:** The Storage Credential wrapping the Access Connector identity is registered and linked to External Locations.

**Steps:**
1. Catalog → **External Data** → **Credentials** tab
2. Click **cred-ev-intelligence-dev**
3. Check:
   ```
   Credential type:    Azure Managed Identity
   Access Connector:   /subscriptions/<id>/resourceGroups/rg-ev-intelligence-dev/providers/
                       Microsoft.Databricks/accessConnectors/ac-ev-intelligence-dev
   Used by locations:  evdatalakedev-bronze, evdatalakedev-silver, evdatalakedev-gold
   ```

**What this confirms:** One credential backs all three External Locations. The Access Connector (`ac-ev-intelligence-dev`) is the Azure resource with a managed identity — that identity has been granted storage roles in Azure IAM.

---

## Test 6 — Verify Cluster Security Mode

**What you're testing:** The `dev-cluster` is in a mode compatible with Unity Catalog enforcement.

**Steps:**
1. Left sidebar → **Compute**
2. Click **dev-cluster** (or the cluster your notebooks run on)
3. Scroll down to **Advanced options** → **Security**
4. Check **Access mode**:
   - Should be `Single User` (your email shown below it) or `Shared`
   - NOT `No Isolation Shared` — that mode bypasses UC permissions

**What Single User means:** Only YOUR account can attach to this cluster and run notebooks on it. UC enforces your permissions, not a shared pool's permissions. When the hourly job runs on this cluster, it inherits your account's access to the Bronze Volume.

**To change access mode** (only when cluster is terminated):
- Edit cluster → Access mode → select Single User → restart cluster

---

## Test 7 — Run a SQL Query Against a Unity Catalog Table

**What you're testing:** You can query a UC table via SQL from the workspace without writing a notebook.

**Steps:**
1. Left sidebar → **SQL Editor** (or search "SQL Editor" in the sidebar)
2. At the top, select a warehouse or cluster:
   - If you have a SQL Warehouse: select it
   - If not: select `dev-cluster` from the "Run on" dropdown
3. Set the catalog and schema defaults at the top dropdowns:
   ```
   Catalog: dbw_ev_intelligence_dev
   Schema:  default
   ```
4. In the SQL editor, type:
   ```sql
   SELECT * FROM pipeline_audit ORDER BY run_timestamp DESC LIMIT 10;
   ```
5. Click **Run** (or Ctrl+Enter)
6. Results appear in the grid below — no cluster attachment, no notebook

**What this proves:** Unity Catalog makes tables queryable from anywhere in the workspace using just SQL — not just from Python notebooks. The 3-level namespace means the same query works from any workspace attached to this metastore.

---

## Test 8 — View a Databricks Job and Its Run History

**What you're testing:** The hourly blob migration job is correctly scheduled and you can inspect past runs.

**Steps:**
1. Left sidebar → **Workflows**
2. Find `job_bronze_charging_sessions_hourly` in the list
3. Check the **Status** column — should show `Active` (green dot)
4. Check **Last run** column — shows when it last fired
5. Click the job name to open it
6. Click the **Runs** tab (or **Run history** tab)
7. Click any past run row
8. You'll see the task execution tree:
   ```
   task_copy_hourly   [Succeeded]   2m 14s
   ```
9. Click the task → **Logs** tab → switch between Stdout / Stderr / Driver log
10. Stdout will show Cell 8's summary:
    ```
    Hours copied: 1 | Hours skipped: 2 | Files copied: 1
    ```

**If job shows "Paused":** Click the three-dot menu → **Resume** to activate scheduling.

**Next scheduled run:** Shown on the job's overview page — confirms cron `0 * * * *` is interpreted correctly as UTC.

---

## Test 9 — Trigger a Manual Job Run

**What you're testing:** The job configuration is correct — notebook path, cluster, notebook code all work end-to-end.

**Steps:**
1. Workflows → `job_bronze_charging_sessions_hourly`
2. Click **Run now** (top right blue button)
3. A new row appears in **Active runs** — click it immediately
4. Watch task progress in real time:
   ```
   task_copy_hourly   [Running]   0:14
   ```
5. Click the task → output streams live as cells execute
6. Expected output (steady state — previous hour already loaded):
   ```
   SKIP (already in Bronze): INCREMENTAL — 2026/07/08/09
   SKIP (source not found):  INCREMENTAL — 2026/07/08/08
   SKIP (already in Bronze): INCREMENTAL — 2026/07/08/07
   INFO: Nothing to copy — all hours in window already loaded or source data not yet available.
   ```
7. Run status: **Succeeded** (even though nothing was copied — exiting cleanly is not a failure)

**If run fails:**
- Click the failed task → **Logs** tab → read the error message
- Common causes: cluster stopped (restart it), secret scope missing, Volume permissions

---

## Test 10 — View Table Permissions

**What you're testing:** The permissions model is working and you can see who has access to a table.

**Steps:**
1. Catalog → `dbw_ev_intelligence_dev` → `default` → Tables → **pipeline_audit**
2. Click the **Permissions** tab
3. You should see at least your own account with ALL PRIVILEGES (as the table owner)
4. To grant access to another user:
   - Click **Grant** button
   - Search for the user or group name
   - Select privilege: SELECT (read-only), MODIFY (read+write), ALL PRIVILEGES
   - Click **Grant**
5. To verify a permission works, open a new incognito browser window, log in as that user, and run:
   ```sql
   SELECT * FROM dbw_ev_intelligence_dev.default.pipeline_audit;
   ```

**What to test with column-level denial:**
```sql
-- From your admin account in SQL Editor:
DENY SELECT ON TABLE dbw_ev_intelligence_dev.default.pipeline_audit (run_timestamp)
  TO `testuser@company.com`;
```
Then as that user, `SELECT * FROM pipeline_audit` will fail. `SELECT pipeline_name FROM pipeline_audit` will succeed. This is column-level security in action.

---

## Test 11 — View Data Lineage

**What you're testing:** Unity Catalog tracks which pipelines wrote to which tables automatically.

**Steps:**
1. Catalog → `dbw_ev_intelligence_dev` → `default` → Tables → **pipeline_audit**
2. Click the **Lineage** tab
3. You'll see a graph showing:
   - Left side (upstream): the ADF pipeline or notebook that last wrote to this table
   - Right side (downstream): anything that reads from this table
4. Click any node in the graph to navigate to that resource

**Note:** Lineage is populated as pipelines run — if the table was just created or ADF hasn't run yet, the lineage graph may be empty. After ADF pipeline `pl_bronze_api_payments_v3` runs and writes a row via the Copy Activity, lineage shows it automatically.

---

## Test 12 — Verify Secret Scope (Without Revealing the Secret)

**What you're testing:** The `kv-ev-scope` secret scope is configured and the secrets exist.

**Steps (requires a running cluster):**
1. Open any notebook attached to `dev-cluster`
2. In a cell, run:
   ```python
   # List all secret scope names (no values shown)
   print(dbutils.secrets.listScopes())
   ```
   Output should include: `[SecretScope(name='kv-ev-scope')]`

3. List keys in the scope:
   ```python
   print(dbutils.secrets.list("kv-ev-scope"))
   ```
   Output should include:
   ```
   [SecretMetadata(key='source-storage-account'),
    SecretMetadata(key='source-container'),
    SecretMetadata(key='source-sas-token')]
   ```

4. Try to get a secret value:
   ```python
   val = dbutils.secrets.get(scope="kv-ev-scope", key="source-storage-account")
   print(val)
   ```
   Output: `[REDACTED]` — the value is returned to the variable but masked in notebook output.
   Assign it to a Spark config to use it — never `print()` directly (though even if you do, the notebook output masks it).

**What this confirms:** The secret scope is connected to Key Vault and the three secrets are present. Any notebook on any cluster in this workspace can access these secrets without knowing the actual values.

---

## Test 13 — Verify Control Plane vs Data Plane Split (Conceptual Test)

**What you're testing:** Understanding what happens inside your subscription vs. what Databricks manages.

**Steps in Azure Portal (not Databricks UI):**
1. Open [portal.azure.com](https://portal.azure.com)
2. Navigate to Resource Group `rg-ev-intelligence-dev`
3. You will see YOUR resources:
   ```
   dbw-ev-intelligence-dev    (Databricks Workspace resource — just a shell)
   evdatalakedev              (ADLS Gen2 — your data is here)
   kv-ev-intelligence-dev     (Key Vault — your secrets)
   adf-ev-intelligence-dev    (ADF — your pipelines)
   ac-ev-intelligence-dev     (Access Connector — Databricks ADLS auth)
   ```
4. Notice: NO VMs, no cluster nodes listed here. Where are they?

**Answer:** Databricks cluster VMs are created in a **managed resource group** (auto-created by Databricks, named something like `databricks-rg-<workspace-name>-<id>`). Search for it in your subscription — you'll find it. Inside it you'll see:
```
Virtual Machines (when cluster is running)
Network Security Groups
Virtual Network
Storage Account (for DBFS root — less relevant when using UC Volumes)
```

**What this proves:** Your data (`evdatalakedev` ADLS) lives in YOUR resource group. The compute (VMs) lives in a Databricks-managed sub-resource group inside your subscription. The control plane (scheduler, Catalog UI, REST API) lives in Databricks' own Azure tenant — you never see those VMs at all.

---

## Quick Reference: Where to Find Everything

| Concept | Where in Databricks UI |
|---|---|
| Metastore name | Catalog tab → top of left tree |
| Catalog | Catalog → expand tree → first level |
| Schema | Catalog → expand catalog → second level |
| Table schema | Catalog → table → Schema tab |
| Table permissions | Catalog → table → Permissions tab |
| Table lineage | Catalog → table → Lineage tab |
| Sample table data | Catalog → table → Sample Data tab |
| Volume files | Catalog → Volumes → volume name → Browse |
| External Locations | Catalog → External Data → External Locations |
| Storage Credentials | Catalog → External Data → Credentials |
| Test ADLS connection | Catalog → External Locations → click location → Test connection |
| Cluster access mode | Compute → cluster → Advanced → Security → Access mode |
| Secret scopes | `dbutils.secrets.listScopes()` in any notebook |
| Job schedule | Workflows → job → Schedules & Triggers tab |
| Job run history | Workflows → job → Runs tab |
| Job run output | Workflows → job → Runs tab → click run → click task → Logs |
| SQL query | SQL Editor (left sidebar) — no notebook needed |
| Admin/Metastore settings | User icon (top right) → Settings → Metastore tab |
