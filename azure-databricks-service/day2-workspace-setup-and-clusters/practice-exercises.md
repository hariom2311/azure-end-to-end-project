# Day 2 — Practice Exercises: Workspace Setup & Cluster Configuration

> **Goal:** Configure your Databricks workspace from scratch, create clusters with the correct settings, set up secrets, and connect to ADLS Gen2.
> All exercises use the VoltGrid project naming convention.

---

## Before You Start

You need:
- An Azure subscription with Contributor or Owner access
- An Azure Databricks workspace already deployed (or permission to create one)
- The `key-vault-session-ded` Key Vault from the VoltGrid project (optional — needed for secrets exercises)

---

## Exercise 1 — Explore Workspace Hierarchy

**Goal:** Understand the Account vs Workspace distinction by navigating both UIs.

### Steps

1. Open your Databricks workspace URL → note the format: `https://adb-<id>.<region>.azuredatabricks.net`

2. In the workspace, go to: **Settings** → **Admin Console** → note which settings are workspace-level

3. Open the Databricks Account Console:
   - Navigate to `https://accounts.azuredatabricks.net`
   - Log in with your Azure AD account
   - Note: you see **Workspaces**, **Users**, **Groups**, **Unity Catalog** tabs — these are account-level

4. Back in your workspace → **Settings** → **Workspace settings** → find these and note the current value:
   - Allow cluster creation
   - DBFS browser enabled
   - Unity Catalog enabled

**What to verify:** You can see the same user in both the workspace **Users** section and the account **Users** section. Account-level = controls everything; workspace-level = controls one environment.

---

## Exercise 2 — Create a Shared Development Cluster

**Goal:** Create a correctly configured all-purpose cluster for development.

### Steps

1. Left sidebar → **Compute** → **+ Create compute**

2. Fill in the **Basic** section:
   - **Cluster name:** `voltgrid-dev-shared`
   - **Policy:** Unrestricted (or your policy if one exists)
   - **Access mode:** `Shared`
   - **Databricks runtime version:** select `15.4 LTS` (or latest LTS — look for the LTS badge)

3. **Worker type:**
   - Node type: `Standard_D4s_v3`
   - ☑ Enable autoscaling → Min: `1`, Max: `4`

4. **Driver type:** `Standard_D4s_v3` (same as worker)

5. **Auto termination:** `60` minutes

6. **Photon acceleration:** ☑ Enable

7. Click **Advanced options** to expand:
   - **Spark** tab → add to Spark config:
     ```
     spark.sql.adaptive.enabled true
     spark.sql.adaptive.coalescePartitions.enabled true
     ```
   - **Tags** tab → add:
     - Key: `team` Value: `voltgrid`
     - Key: `environment` Value: `dev`

8. Click **Create compute**

9. Wait for cluster to reach **Running** state (~3–5 minutes)

10. Click the cluster name → **Event log** tab → confirm you see:
    ```
    Cluster launched
    Cluster running
    ```

**Verify:** Click **Configuration** tab → confirm Access mode = Shared, Runtime = 15.4 LTS, Autoscaling enabled.

---

## Exercise 3 — Attach a Notebook and Run First Commands

**Goal:** Confirm the cluster works by running basic Spark and dbutils commands.

### Steps

1. Left sidebar → **Workspace** → **+** → **Notebook**
2. Name: `day2_cluster_verify` | Language: `Python` | Attach to: `voltgrid-dev-shared`

3. In Cell 1 — check Spark version:
   ```python
   print(f"Spark version: {spark.version}")
   print(f"Scala version: {sc._jvm.scala.util.Properties.versionString()}")
   ```
   Expected: `Spark version: 3.5.x`

4. In Cell 2 — check cluster info:
   ```python
   print(spark.sparkContext.appName)
   print(spark.sparkContext.master)
   ```

5. In Cell 3 — test parallelism:
   ```python
   # Create a simple RDD and count
   rdd = sc.parallelize(range(1, 1001))
   print(f"Sum 1-1000: {rdd.sum()}")
   print(f"Partitions: {rdd.getNumPartitions()}")
   ```
   Expected: `Sum 1-1000: 500500`

6. In Cell 4 — list DBFS root:
   ```python
   display(dbutils.fs.ls("dbfs:/"))
   ```
   You should see the DBFS root directories including `FileStore`, `user`, `tmp`.

7. In Cell 5 — Spark config verification:
   ```python
   print(spark.conf.get("spark.sql.adaptive.enabled"))
   print(spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"))
   ```
   Expected: `true` for both (from the cluster Spark config we set).

---

## Exercise 4 — Create an Instance Pool

**Goal:** Create a pre-warmed VM pool to speed up cluster starts.

### Steps

1. Left sidebar → **Compute** → **Pools** tab → **Create pool**

2. Configure:
   | Field | Value |
   |---|---|
   | Pool name | `voltgrid-pool-dev` |
   | Min idle instances | `1` |
   | Max capacity | `6` |
   | Idle instance auto-termination | `30` minutes |
   | Node type | `Standard_D4s_v3` |

3. Click **Create**

4. Wait ~2 minutes → pool shows status **Running** with 1 idle instance

5. Now create a second cluster **using the pool**:
   - **Compute** → **+ Create compute**
   - Name: `voltgrid-pool-cluster-test`
   - Access mode: `Single User` → select your user
   - Runtime: `15.4 LTS`
   - **Node type** → click **Instance pool** radio → select `voltgrid-pool-dev`
   - Min workers: `1`, Max workers: `2`
   - **Create compute**

6. Observe: this cluster starts in **~30–60 seconds** instead of 3–5 minutes — the VM was already running in the pool

7. After verifying, terminate `voltgrid-pool-cluster-test` (we don't need two clusters running)

**What to observe:** The **Event log** of the pool cluster shows `Cluster launched` almost immediately compared to the 3–5 minute launch in Exercise 2.

---

## Exercise 5 — Create a Cluster Policy (Admin Required)

**Goal:** Create a policy that enforces auto-termination and limits VM choices.

> Skip this exercise if you do not have workspace admin access. Ask your admin to apply this policy.

### Steps

1. Left sidebar → **Settings** → **Compute** → **Cluster policies** → **Create policy**

2. Name: `voltgrid-dev-policy`

3. Paste this policy JSON into the definition box:
   ```json
   {
     "autotermination_minutes": {
       "type": "fixed",
       "value": 60,
       "hidden": false
     },
     "node_type_id": {
       "type": "allowlist",
       "values": [
         "Standard_D4s_v3",
         "Standard_D8s_v3",
         "Standard_E8ds_v4"
       ],
       "defaultValue": "Standard_D4s_v3"
     },
     "num_workers": {
       "type": "range",
       "minValue": 1,
       "maxValue": 8
     },
     "autoscale.min_workers": {
       "type": "range",
       "minValue": 1,
       "maxValue": 4
     },
     "autoscale.max_workers": {
       "type": "range",
       "minValue": 2,
       "maxValue": 8
     },
     "custom_tags.team": {
       "type": "fixed",
       "value": "voltgrid",
       "hidden": false
     },
     "custom_tags.environment": {
       "type": "fixed",
       "value": "dev",
       "hidden": false
     }
   }
   ```

4. Click **Create**

5. Assign to users:
   - Click the policy → **Permissions** tab
   - Add group `data-engineers` with **Can Use** permission

6. **Verify:** Open a new cluster creation form → select Policy = `voltgrid-dev-policy` → observe that:
   - Auto-termination is locked at 60 min (greyed out)
   - Node type only offers the 3 allowed VMs
   - Tags are pre-filled and locked

---

## Exercise 6 — Configure Secret Scope (Azure Key Vault-Backed)

**Goal:** Create a secret scope that reads secrets from the VoltGrid Key Vault.

### Steps

1. Navigate to this URL (replace with your workspace URL):
   ```
   https://<your-workspace-url>#secrets/createScope
   ```

2. Fill in the form:
   | Field | Value |
   |---|---|
   | Scope name | `voltgrid-kv` |
   | Manage Principal | `All Users` |
   | DNS Name | `https://key-vault-session-ded.vault.azure.net/` |
   | Resource ID | *(copy from Azure Portal → Key Vault → Properties → Resource ID)* |

3. Click **Create**

4. Verify in a notebook — open `day2_cluster_verify` → add a new cell:
   ```python
   # List secrets in the scope (shows secret names only, never values)
   secrets = dbutils.secrets.list(scope="voltgrid-kv")
   for s in secrets:
       print(s.key)
   ```
   Expected output — lists the key names (e.g., `voltgrid-username`, `voltgrid-password`)

5. Read a secret:
   ```python
   # Value is REDACTED in notebook output — Databricks hides it automatically
   username = dbutils.secrets.get(scope="voltgrid-kv", key="voltgrid-username")
   print(f"Username length: {len(username)}")  # prints length, not value
   print("Secret retrieved successfully")
   ```
   The `print(username)` would show `[REDACTED]` in the notebook output — Databricks protects secret values.

---

## Exercise 7 — Connect to ADLS Gen2 and Read Bronze Data

**Goal:** Configure ADLS Gen2 access from Databricks and read the Bronze payments data.

### Steps

1. Open `day2_cluster_verify` notebook → add a new cell:

   ```python
   # Configure ADLS Gen2 access using Service Principal credentials from Key Vault
   storage_account = "<your-storage-account-name>"   # e.g. evdatalakedev

   spark.conf.set(
       f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
       "OAuth"
   )
   spark.conf.set(
       f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
       "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
   )
   spark.conf.set(
       f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
       dbutils.secrets.get(scope="voltgrid-kv", key="sp-client-id")
   )
   spark.conf.set(
       f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
       dbutils.secrets.get(scope="voltgrid-kv", key="sp-client-secret")
   )
   spark.conf.set(
       f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
       dbutils.secrets.get(scope="voltgrid-kv", key="sp-tenant-endpoint")
   )

   print("ADLS config set")
   ```

2. Add a new cell — list Bronze container:
   ```python
   bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
   files = dbutils.fs.ls(bronze_path)
   for f in files:
       print(f.path, f.size)
   ```

3. Add a new cell — read Bronze payments JSON:
   ```python
   payments_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/api/payments/"
   df = spark.read.option("multiline", "true").json(payments_path)
   print(f"Schema:")
   df.printSchema()
   print(f"Row count: {df.count()}")
   display(df.limit(5))
   ```

**Expected output:** Schema shows `id`, `amount`, `status`, `session_id`, `created_at` columns. Row count matches what ADF copied.

---

## Exercise 8 — Compare Cluster Access Modes

**Goal:** Observe the difference between Single User and Shared access modes.

### Steps

1. In your existing `voltgrid-dev-shared` cluster (Shared mode), run:
   ```python
   # This works in Shared mode — Python is fully supported
   import subprocess
   result = subprocess.run(["pip", "install", "faker"], capture_output=True, text=True)
   print(result.stdout)
   ```
   This will likely fail or be restricted — Shared mode restricts library installation to admins.

2. Create a **Single User** cluster:
   - Name: `voltgrid-single-user-test`
   - Access mode: `Single User` → your username
   - Runtime: `15.4 LTS`
   - Workers: `1` (fixed)
   - Auto-terminate: `30` minutes

3. Attach a notebook to `voltgrid-single-user-test` → run:
   ```python
   # Single user mode: full control — install anything
   %pip install faker
   ```
   Then:
   ```python
   from faker import Faker
   fake = Faker()
   print(fake.name())   # works — library installed successfully
   ```

4. Terminate `voltgrid-single-user-test` when done

**Key observation:**
- Shared mode: multiple users, restricted library access, ideal for cost sharing
- Single User mode: full control, isolated session, required for ML or custom libraries

---

## Exercise 9 — Monitor Cluster Cost with Tags

**Goal:** Understand how tags enable cost tracking.

### Steps

1. In Azure Portal → **Cost Management + Billing** → **Cost analysis**
2. Add filter: **Tag** → `team` = `voltgrid`
3. You will see all Azure resources tagged with `voltgrid` — including the Databricks-managed VMs

4. Back in Databricks → **Compute** → click `voltgrid-dev-shared` → **Configuration** tab → scroll to **Tags**
5. Confirm tags: `team=voltgrid`, `environment=dev`

6. These tags propagate to:
   - Azure VM costs in Cost Management
   - Databricks usage reports (under Account Console → Billing)

**Why this matters:** Without tags, you cannot split Databricks costs across teams or projects. The `cost-centre` tag routes charges to the correct budget in finance reporting.

---

## Final Cluster Architecture — VoltGrid Reference

```
Clusters in VoltGrid:

  DEV WORKSPACE (dbw-ev-dev)
  ├── voltgrid-dev-shared        All-Purpose  Shared    D4s_v3  1-4 workers  Auto-term 60m
  └── (pool: voltgrid-pool-dev)  Pool         —         D4s_v3  1 idle

  PROD WORKSPACE (dbw-ev-prod)
  ├── Job clusters (created per run by Databricks Workflows / ADF)
  │   ├── silver-payments-job    Job          Single    E8ds_v4  2-8 workers (spot)
  │   ├── silver-sessions-job    Job          Single    E8ds_v4  2-8 workers (spot)
  │   └── gold-revenue-job       Job          Single    D8s_v3   2-4 workers (spot)
  └── (pool: voltgrid-pool-prod) Pool         —         E8ds_v4  2 idle
```

---

## Quick Verification Checklist

| Task | How to verify |
|---|---|
| Workspace created | Portal → Resource Groups → `rg-ev-dev` → `dbw-ev-dev` exists |
| Cluster running | Compute → `voltgrid-dev-shared` → status = Running (green dot) |
| Correct runtime | Cluster config → Databricks runtime = 15.4 LTS |
| Auto-termination set | Cluster config → Terminate after 60 minutes |
| Photon enabled | Cluster config → Photon acceleration = enabled |
| Tags applied | Cluster config → Tags → team=voltgrid, environment=dev |
| Secret scope works | Notebook: `dbutils.secrets.list("voltgrid-kv")` → shows key names |
| ADLS readable | Notebook: `dbutils.fs.ls("abfss://bronze@...")` → shows files |
| Pool created | Compute → Pools → voltgrid-pool-dev → Running |
