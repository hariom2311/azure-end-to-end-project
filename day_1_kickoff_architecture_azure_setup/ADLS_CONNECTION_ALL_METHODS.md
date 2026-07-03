# All Ways to Connect Databricks to ADLS Gen2
**Reference guide — every method, every step**

---

## Which Method Should I Use?

| Method | Use when | Cluster mode | Credentials |
|---|---|---|---|
| [Method 1 — SP OAuth Direct](#method-1--service-principal-oauth-direct-access) | **This project — use this** | Any | SP Client ID + Secret |
| [Method 2 — SAS Token](#method-2--sas-token) | Accessing someone else's storage | Any | SAS token string |
| [Method 3 — Storage Access Key](#method-3--storage-account-access-key) | Quick local dev test only | Any | Account key |
| [Method 4 — Mount with SP OAuth](#method-4--mount-with-service-principal-oauth-legacy) | Legacy — avoid new code | Dedicated only | SP Client ID + Secret |
| [Method 5 — Managed Identity](#method-5--managed-identity-msi) | Production / no-credential setups | Any | None — Azure handles it |
| [Method 6 — Unity Catalog](#method-6--unity-catalog-storage-credential) | Enterprise / shared workspace | Any | Configured once by admin |

> **For this project (EV Intelligence):** Use **Method 1**. Methods 2 and 4 are also used in specific places — noted below.

---

## Method 1 — Service Principal OAuth Direct Access

**Used in this project for:** connecting to your own ADLS `evdatalakedev`
**File:** `00b_connect_storage_no_mount.ipynb`

### What it is
Databricks presents the Service Principal's Client ID + Secret to Azure Entra ID, receives a short-lived OAuth token, and uses that token to access storage. No mount. No access key. Works on any cluster mode.

### What you need before starting
- [ ] Service Principal created with `sp-client-id`, `sp-client-secret`, `sp-tenant-id`
- [ ] SP assigned `Storage Blob Data Contributor` role on `evdatalakedev`
- [ ] All 4 secrets stored in Key Vault: `adls-account-name`, `sp-client-id`, `sp-client-secret`, `sp-tenant-id`
- [ ] Secret scope `kv-ev-scope` created in Databricks

### Step 1 — Add secrets to Key Vault (if not done already)

**Via Portal:**
1. Portal → **Key vaults** → `kv-ev-intelligence-dev` → **Secrets** → **+ Generate/Import**
2. Add each secret:

| Secret Name | Value |
|---|---|
| `adls-account-name` | `evdatalakedev` |
| `sp-client-id` | Application (client) ID of your SP |
| `sp-client-secret` | Client secret value of your SP |
| `sp-tenant-id` | Directory (tenant) ID |

**Via CLI:**
```cmd
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "adls-account-name" --value "evdatalakedev"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-client-id"      --value "<your-app-id>"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-client-secret"  --value "<your-secret>"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-tenant-id"      --value "<your-tenant-id>"
```

### Step 2 — Assign SP the storage role (if not done already)

**Via Portal:**
1. Portal → **Storage accounts** → `evdatalakedev` → **Access Control (IAM)**
2. Click **+ Add** → **Add role assignment**
3. Role: `Storage Blob Data Contributor` → **Next**
4. Members → **+ Select members** → search `sp-ev-intelligence-dev` → **Select**
5. Click **Review + assign**

**Via CLI:**
```cmd
az role assignment create --assignee-object-id <SP-Object-ID> --assignee-principal-type ServicePrincipal --role "Storage Blob Data Contributor" --scope /subscriptions/<sub-id>/resourceGroups/rg-ev-intelligence-dev/providers/Microsoft.Storage/storageAccounts/evdatalakedev
```

### Step 3 — Create a Databricks notebook

1. Databricks → **Workspace** → **+ New** → **Notebook**
2. Name: `00b_connect_storage_no_mount`
3. Language: Python
4. Attach to your cluster

### Step 4 — Cell 1: Load secrets from Key Vault

```python
SCOPE = "kv-ev-scope"

storage_account  = dbutils.secrets.get(scope=SCOPE, key="adls-account-name")
sp_client_id     = dbutils.secrets.get(scope=SCOPE, key="sp-client-id")
sp_client_secret = dbutils.secrets.get(scope=SCOPE, key="sp-client-secret")
sp_tenant_id     = dbutils.secrets.get(scope=SCOPE, key="sp-tenant-id")

print(f"Storage account : {storage_account}")
print(f"SP client ID    : {sp_client_id[:8]}...[REDACTED]")
print("Secrets loaded — OK")
```

**Expected output:**
```
Storage account : evdatalakedev
SP client ID    : xxxxxxxx...[REDACTED]
Secrets loaded — OK
```

### Step 5 — Cell 2: Set Spark OAuth config

```python
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", sp_client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", sp_client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{sp_tenant_id}/oauth2/token")

print("Spark OAuth config set — OK")
```

**Expected output:**
```
Spark OAuth config set — OK
```

### Step 6 — Cell 3: Define path helper and verify

```python
def abfss(container: str, path: str = "") -> str:
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base

# Verify all 4 containers
for container in ["bronze", "silver", "gold", "source"]:
    try:
        items = dbutils.fs.ls(abfss(container))
        print(f"  {container:<8} OK — {len(items)} items")
    except Exception as e:
        print(f"  {container:<8} ERROR — {e}")
```

**Expected output:**
```
  bronze   OK — 0 items
  silver   OK — 0 items
  gold     OK — 0 items
  source   OK — 0 items
```

### Errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `Secret does not exist` | Secret missing from Key Vault | Add it via Portal or CLI in Step 1 |
| `403 Forbidden` on ls | SP missing RBAC role | Complete Step 2 — assign `Storage Blob Data Contributor` |
| `AuthenticationFailed` | Wrong client secret value | Key Vault → `sp-client-secret` → re-check value |
| `Secret scope does not exist` | Scope not created | Day 1 Part 6.5 — create the scope first |
| `PERMISSION_DENIED: KeyVault 403` | `AzureDatabricks` app missing `Key Vault Secrets User` role | Key Vault → IAM → add that role for `AzureDatabricks` → wait 2 min |

---

## Method 2 — SAS Token

**Used in this project for:** reading from the shared external blob `dataenggdailystorage`
**File:** `02_read_source_blob.ipynb`

### What it is
A Shared Access Signature (SAS) token is a signed URL string that grants time-limited, permission-scoped access to a storage container. The storage owner generates it and shares it. You do not need an Azure account — just the token string.

### Limitations
- Uses `wasbs://` protocol — NOT `abfss://`
- Token has an expiry date — must be renewed when it expires
- Can only be scoped to read, list, write — not fine-grained per-folder

### What you need before starting
- [ ] SAS token string (provided by storage owner — looks like `se=2027-07-30&sp=rl&...`)
- [ ] Storage account name and container name
- [ ] Secret scope `kv-ev-scope` created in Databricks

### Step 1 — Generate a SAS token (if you own the storage)

**Via Portal:**
1. Portal → **Storage accounts** → your storage account
2. Left menu → **Shared access signature** (under Security + networking)
3. Fill in:
   - **Allowed services:** Blob
   - **Allowed resource types:** Container + Object
   - **Allowed permissions:** Read + List (add Write if needed)
   - **Expiry:** set to a future date
   - **Allowed protocols:** HTTPS only
4. Click **Generate SAS and connection string**
5. Copy the **SAS token** value (starts with `sv=` or `se=`)

**Via CLI:**
```cmd
az storage container generate-sas --account-name evdatalakedev --name source --permissions rl --expiry 2027-12-31 --auth-mode login --as-user -o tsv
```

> **If someone else owns the storage** (like `dataenggdailystorage`), they generate the token and share it with you. Skip Step 1 and go straight to Step 2.

### Step 2 — Store token in Key Vault

**Via Portal:**
1. Key Vault → `kv-ev-intelligence-dev` → **Secrets** → **+ Generate/Import**
2. Add these 3 secrets:

| Secret Name | Value |
|---|---|
| `source-storage-account` | `dataenggdailystorage` |
| `source-container` | `source` |
| `source-sas-token` | paste full SAS token — no quotes, no leading `?` |

**Via CLI:**
```cmd
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "source-storage-account" --value "dataenggdailystorage"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "source-container"       --value "source"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "source-sas-token"       --value "<paste-sas-token-here>"
```

### Step 3 — Create a Databricks notebook

1. Databricks → **Workspace** → **+ New** → **Notebook**
2. Name: `02_read_source_blob`
3. Language: Python, attach to cluster

### Step 4 — Cell 1: Load secrets and configure Spark

```python
SCOPE = "kv-ev-scope"

storage_account = dbutils.secrets.get(scope=SCOPE, key="source-storage-account")
container       = dbutils.secrets.get(scope=SCOPE, key="source-container")
sas_token       = dbutils.secrets.get(scope=SCOPE, key="source-sas-token")

# Configure Spark to use SAS for this container
# Important: use blob.core.windows.net (not dfs) — SAS uses wasbs:// not abfss://
spark.conf.set(
    f"fs.azure.sas.{container}.{storage_account}.blob.core.windows.net",
    sas_token
)

print(f"Storage account : {storage_account}")
print(f"Container       : {container}")
print(f"SAS token       : [REDACTED]")
print("Spark SAS config set — OK")
```

**Expected output:**
```
Storage account : dataenggdailystorage
Container       : source
SAS token       : [REDACTED]
Spark SAS config set — OK
```

### Step 5 — Cell 2: List and read files

```python
# Build wasbs:// path — SAS tokens require wasbs://, not abfss://
base_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net"

# List top-level folders
print("Top-level folders:")
for item in dbutils.fs.ls(base_path):
    print(f"  {item.name}")

# Read CSV files
df = spark.read.option("header", "true").csv(f"{base_path}/realtime/charging_sessions/*/*/*/*/*.csv")
print(f"\nTotal rows: {df.count():,}")
display(df.limit(10))
```

**Expected output:**
```
Top-level folders:
  realtime/

Total rows: <number>
```

### Errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `403 Forbidden` | Token wrong, expired, or missing `List` permission | Confirm `sp=rl` in token. Re-check Key Vault secret value — no extra spaces |
| `InvalidAuthenticationInfo` | Using `abfss://` instead of `wasbs://` | SAS tokens require `wasbs://` — replace in all paths |
| `Secret not found: source-sas-token` | Secret not added to Key Vault | Complete Step 2 |
| `UNABLE_TO_INFER_SCHEMA` | Reading a folder that has only subfolders at top | Use glob `/*/*/*/*/*.csv` to reach actual CSV files |
| Output shows `[REDACTED]` for token | Databricks masked the secret | Expected — token is still working |

---

## Method 3 — Storage Account Access Key

> **Warning:** This gives full root-level access to every container in the storage account. Anyone with this key can read, write, and delete everything. Use for local dev testing only — never commit to git, never use in production or shared environments.

### What it is
A static 512-bit base64 key directly associated with the storage account. Every storage account has two keys (key1, key2) for rotation. Simpler to set up than OAuth but far less secure.

### What you need before starting
- [ ] Storage account `evdatalakedev` created
- [ ] Access to Portal or CLI to retrieve the key
- [ ] Secret scope `kv-ev-scope` created in Databricks

### Step 1 — Get the access key

**Via Portal:**
1. Portal → **Storage accounts** → `evdatalakedev`
2. Left menu → **Access keys** (under Security + networking)
3. Click **Show** next to `key1`
4. Copy the full **Key** value (long base64 string ending in `==`)

**Via CLI:**
```cmd
az storage account keys list --account-name evdatalakedev --resource-group rg-ev-intelligence-dev --query "[0].value" -o tsv
```

### Step 2 — Store key in Key Vault

**Via Portal:**
1. Key Vault → `kv-ev-intelligence-dev` → **Secrets** → **+ Generate/Import**
2. Name: `adls-account-key`, Value: paste the key → **Create**

**Via CLI:**
```cmd
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "adls-account-key" --value "<paste-key-here>"
```

### Step 3 — Create a Databricks notebook and configure Spark

```python
SCOPE = "kv-ev-scope"

storage_account = dbutils.secrets.get(scope=SCOPE, key="adls-account-name")
account_key     = dbutils.secrets.get(scope=SCOPE, key="adls-account-key")

# Set access key for this storage account
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    account_key
)

print(f"Storage account : {storage_account}")
print("Access key config set — OK")
```

### Step 4 — Read and write using abfss:// paths

```python
# Works with abfss:// like Method 1 — only the auth mechanism differs
storage_account = dbutils.secrets.get(scope="kv-ev-scope", key="adls-account-name")

def abfss(container, path=""):
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base

# Read
df = spark.read.parquet(abfss("bronze", "api_payments/"))

# Write
df.write.mode("append").parquet(abfss("bronze", "api_payments/"))
```

### Errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `AccountIsDisabled` | Storage account suspended | Check Portal — storage account status |
| `AuthenticationFailed` | Wrong key value | Re-copy key from Portal → Access keys → Show → copy exactly |
| `Secret not found: adls-account-key` | Secret not added to Key Vault | Complete Step 2 |

---

## Method 4 — Mount with Service Principal OAuth (Legacy)

> **Note:** `dbutils.fs.mount()` is deprecated. Blocked on Standard, Shared, and Serverless clusters. Requires Dedicated cluster mode. Use Method 1 for all new notebooks.

### What it is
Mounts a storage container to a `/mnt/` path. Once mounted, all notebooks on the cluster can access storage via `/mnt/bronze/` style paths without any spark.conf setup. Mount is lost when the cluster restarts — must re-run the mount notebook each time.

### What you need before starting
- [ ] Same SP secrets as Method 1 (`sp-client-id`, `sp-client-secret`, `sp-tenant-id`, `adls-account-name`)
- [ ] Secret scope `kv-ev-scope` created in Databricks
- [ ] Cluster Access mode = **Dedicated** (not Standard, Shared, or Serverless)

### Step 1 — Confirm cluster access mode is Dedicated

1. Databricks → **Compute** → click your cluster → **Edit**
2. **Access mode** must be `Dedicated (Single user)`
3. If it shows `Standard` or `Shared` — change it, confirm, restart cluster
4. If it shows `Serverless` in the top-right compute picker — click it and switch to your cluster

### Step 2 — Add secrets to Key Vault (same as Method 1 Step 1)

Same 4 secrets: `adls-account-name`, `sp-client-id`, `sp-client-secret`, `sp-tenant-id`

### Step 3 — Assign SP the storage role (same as Method 1 Step 2)

`Storage Blob Data Contributor` on `evdatalakedev`

### Step 4 — Create notebook `00_mount_storage` and run

```python
SCOPE = "kv-ev-scope"

storage_account  = dbutils.secrets.get(scope=SCOPE, key="adls-account-name")
sp_client_id     = dbutils.secrets.get(scope=SCOPE, key="sp-client-id")
sp_client_secret = dbutils.secrets.get(scope=SCOPE, key="sp-client-secret")
sp_tenant_id     = dbutils.secrets.get(scope=SCOPE, key="sp-tenant-id")

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type":
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": sp_client_id,
    "fs.azure.account.oauth2.client.secret": sp_client_secret,
    "fs.azure.account.oauth2.client.endpoint":
        f"https://login.microsoftonline.com/{sp_tenant_id}/oauth2/token",
}

for container in ["bronze", "silver", "gold", "source"]:
    mount_point = f"/mnt/{container}"
    if not any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source=f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs,
        )
        print(f"Mounted  : {mount_point}")
    else:
        print(f"Already mounted : {mount_point}")

display(dbutils.fs.mounts())
```

**Expected output:**
```
Mounted  : /mnt/bronze
Mounted  : /mnt/silver
Mounted  : /mnt/gold
Mounted  : /mnt/source
```

### Step 5 — Use mounted paths in notebooks

```python
# Read — no spark.conf needed, mount handles auth
# Replace the path below with your actual data path — no data exists yet on Day 1
# Example: df = spark.read.parquet("/mnt/bronze/charging_sessions/")
df = spark.read.parquet("/mnt/bronze/<your-folder>/")

# Write — replace path with where you want to land the data
df.write.mode("overwrite").parquet("/mnt/bronze/<your-folder>/")

# List files in bronze to see what is there
display(dbutils.fs.ls("/mnt/bronze"))
```

### Step 6 — Unmount (cleanup)

```python
for container in ["bronze", "silver", "gold", "source"]:
    mount_point = f"/mnt/{container}"
    if any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
        dbutils.fs.unmount(mount_point)
        print(f"Unmounted: {mount_point}")
```

### Errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `Method dbutils.mount() is not whitelisted` | Cluster is Standard or Shared mode | Edit cluster → Access mode → Dedicated → restart |
| `Method dbutils.mounts() is not whitelisted` | Same — wrong cluster mode | Same fix as above |
| `Serverless compute` shown in top-right | Wrong compute attached to notebook | Click compute picker → switch to `dev-cluster` |
| `403 Forbidden` on mount | SP missing RBAC role | Storage account → IAM → add `Storage Blob Data Contributor` for SP |
| Mount succeeds but read fails | Mount is stale after cluster restart | Re-run this notebook — mounts reset on every restart |

---

## Method 5 — Managed Identity (MSI)

> **For production workspaces.** No credentials, no rotation, no Key Vault secrets for storage auth. Azure assigns an identity to the cluster and grants it access automatically.

### What it is
A Managed Identity is an Azure-managed identity attached directly to a Databricks cluster (or workspace). Azure handles credential issuance and rotation completely — you never see or store a client secret. The cluster just presents its identity to Azure and is granted access based on RBAC roles.

### What you need before starting
- [ ] Databricks workspace with **Premium** tier (Standard does not support cluster policies or MSI assignment)
- [ ] Permission to create a User-Assigned Managed Identity in Azure
- [ ] Permission to assign RBAC roles on the storage account

### Step 1 — Create a User-Assigned Managed Identity

**Via Portal:**
1. Portal → search **Managed Identities** → **+ Create**
2. Fill in:
   - **Resource group:** `rg-ev-intelligence-dev`
   - **Region:** `Central India`
   - **Name:** `msi-databricks-ev-dev`
3. Click **Review + Create** → **Create**
4. Once created, open it and copy:
   - **Client ID** (looks like a GUID)
   - **Resource ID** (full path under Properties)

**Via CLI:**
```cmd
az identity create --name msi-databricks-ev-dev --resource-group rg-ev-intelligence-dev --location centralindia
az identity show --name msi-databricks-ev-dev --resource-group rg-ev-intelligence-dev --query "{clientId:clientId, principalId:principalId, id:id}"
```

### Step 2 — Assign the MSI the storage role

**Via Portal:**
1. Portal → **Storage accounts** → `evdatalakedev` → **Access Control (IAM)**
2. Click **+ Add** → **Add role assignment**
3. Role: `Storage Blob Data Contributor` → **Next**
4. Assign access to: **Managed identity**
5. **+ Select members** → select your subscription → Managed identity type: **User-assigned managed identity** → select `msi-databricks-ev-dev` → **Select**
6. **Review + assign**

**Via CLI:**
```cmd
az role assignment create --assignee-object-id <principalId-from-step-1> --assignee-principal-type ServicePrincipal --role "Storage Blob Data Contributor" --scope /subscriptions/<sub-id>/resourceGroups/rg-ev-intelligence-dev/providers/Microsoft.Storage/storageAccounts/evdatalakedev
```

### Step 3 — Attach the MSI to the Databricks cluster

1. Databricks → **Compute** → your cluster → **Edit**
2. Scroll to **Advanced options** → **Azure attributes**
3. Under **Instance Profile / Managed Identity** — paste the **Resource ID** of your MSI
4. Click **Confirm and restart**

> This option may be absent on Standard-tier workspaces. If you don't see it, your workspace tier does not support MSI attachment.

### Step 4 — Configure Spark in the notebook

```python
storage_account = "evdatalakedev"

spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
    "ManagedIdentity"
)

# No client ID, no secret, no tenant needed — Azure resolves it automatically
print("Managed Identity auth configured — OK")
```

### Step 5 — Read and write normally

```python
def abfss(container, path=""):
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base

# Verify
for container in ["bronze", "silver", "gold", "source"]:
    try:
        items = dbutils.fs.ls(abfss(container))
        print(f"  {container:<8} OK — {len(items)} items")
    except Exception as e:
        print(f"  {container:<8} ERROR — {e}")

# Replace the path below with your actual data path — no data exists yet on Day 1
# Example: df = spark.read.parquet(abfss("bronze", "charging_sessions/"))
df = spark.read.parquet(abfss("bronze", "<your-folder>/"))

# Write — replace path with where you want to land the data
df.write.mode("overwrite").parquet(abfss("bronze", "<your-folder>/"))
```

### Errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `ManagedIdentity` option not visible on cluster | Standard-tier workspace | Upgrade workspace to Premium |
| `403 Forbidden` | MSI not assigned storage role | Step 2 — assign `Storage Blob Data Contributor` to the MSI's `principalId` |
| `AuthenticationFailed` | MSI not attached to cluster | Step 3 — paste Resource ID in cluster Advanced options |
| Config accepted but still `403` | Wrong MSI attached (client ID vs resource ID) | Step 3 — use the full Resource ID, not the Client ID |

---

## Method 6 — Unity Catalog Storage Credential

> **For enterprise workspaces with Unity Catalog enabled.** Auth is configured once by an admin. Notebooks need zero auth setup — just use `abfss://` paths directly.

### What it is
Unity Catalog stores a **Storage Credential** (wrapping a SP or MSI) and an **External Location** (a container path + which credential to use for it). Once set up by an admin, every notebook on the workspace can read/write that storage without any `spark.conf` setup. Auth is invisible.

### What you need before starting
- [ ] Databricks workspace with **Unity Catalog metastore** attached (Premium tier, set up by an admin)
- [ ] You must be a **Metastore Admin** or **Storage Credential Creator** to complete Steps 1–3
- [ ] A Service Principal or Managed Identity to use as the credential

### Step 1 — Create a Storage Credential in Unity Catalog

1. Databricks → left menu → **Catalog** → **External Data** → **Credentials** → **+ Create credential**
2. Fill in:
   - **Credential name:** `evdatalakedev-credential`
   - **Authentication type:** `Azure service principal`
   - **Directory (tenant) ID:** your tenant ID
   - **Application (client) ID:** your SP client ID
   - **Client secret:** your SP client secret
3. Click **Create**

**Via Databricks CLI:**
```bash
databricks storage-credentials create --json '{
  "name": "evdatalakedev-credential",
  "azure_service_principal": {
    "directory_id": "<tenant-id>",
    "application_id": "<client-id>",
    "client_secret": "<client-secret>"
  }
}'
```

### Step 2 — Create an External Location

1. Databricks → **Catalog** → **External Data** → **External Locations** → **+ Create location**
2. Fill in:
   - **External location name:** `evdatalakedev-location`
   - **URL:** `abfss://bronze@evdatalakedev.dfs.core.windows.net/` (or the root `abfss://evdatalakedev.dfs.core.windows.net/`)
   - **Storage credential:** select `evdatalakedev-credential` from Step 1
3. Click **Create**

### Step 3 — Grant access to users

1. Databricks → **Catalog** → **External Data** → **External Locations** → click your location
2. Click **Permissions** → **Grant**
3. Select user or group → permission: `READ FILES` (and `WRITE FILES` if needed)
4. Click **Grant**

### Step 4 — Use in notebooks (zero auth setup needed)

```python
# No spark.conf needed — Unity Catalog handles auth automatically
# Just use abfss:// paths directly

# Replace the path below with your actual data path — no data exists yet on Day 1
# Example: df = spark.read.parquet("abfss://bronze@evdatalakedev.dfs.core.windows.net/charging_sessions/")

# Read
df = spark.read.parquet("abfss://bronze@evdatalakedev.dfs.core.windows.net/<your-folder>/")

# Write
df.write.mode("overwrite").parquet("abfss://bronze@evdatalakedev.dfs.core.windows.net/<your-folder>/")

# List
display(dbutils.fs.ls("abfss://bronze@evdatalakedev.dfs.core.windows.net/"))
```

### Errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `PERMISSION_DENIED` on read/write | User not granted READ/WRITE FILES on the External Location | Step 3 — grant the permission |
| `This location is not allowed` | External Location not defined for this path | Step 2 — create an External Location that covers this path |
| `Unity Catalog not enabled` | Workspace does not have a metastore attached | Contact workspace admin to attach Unity Catalog metastore |
| Catalog menu not visible | Workspace is Standard tier or not UC-enabled | Premium tier required |

---

## Full Comparison — All 6 Methods

| | Method 1 | Method 2 | Method 3 | Method 4 | Method 5 | Method 6 |
|---|---|---|---|---|---|---|
| **Name** | SP OAuth Direct | SAS Token | Access Key | Mount + SP OAuth | Managed Identity | Unity Catalog |
| **Used in this project** | ✅ Yes | ✅ Yes (source blob) | ❌ Avoid | ⚠️ Legacy option | ❌ Extra setup | ❌ Extra setup |
| **Cluster mode** | Any | Any | Any | Dedicated only | Any | Any |
| **Credentials needed** | SP ID + Secret | SAS string | Account key | SP ID + Secret | None | None (admin sets up once) |
| **Credential expires** | Secret (180 days) | Yes — set expiry | Never | Secret (180 days) | Never | Depends on credential type |
| **Path style** | `abfss://` | `wasbs://` | `abfss://` | `/mnt/` | `abfss://` | `abfss://` |
| **Re-run each session?** | Yes — 3 cells or `%run` | Yes — 1 cell | Yes — 1 cell | Yes — mount notebook | Yes — 1 cell | No — fully transparent |
| **Security level** | High | Medium | Low | High | Highest | Highest |
| **Setup complexity** | Low | Low | Lowest | Medium | High | Highest |
| **Best for** | This project | External shared storage | Dev testing only | Legacy notebooks | Production | Enterprise |

---

## What This Project Uses

| Notebook | Method | Storage |
|---|---|---|
| `00b_connect_storage_no_mount` | Method 1 — SP OAuth Direct | `evdatalakedev` (your account) |
| `02_read_source_blob` | Method 2 — SAS Token | `dataenggdailystorage` (external) |
| `00_mount_storage` (legacy) | Method 4 — Mount + SP OAuth | `evdatalakedev` (your account) |

For Day 1 onwards: **use Method 1 for your storage, Method 2 for the shared source blob.**
