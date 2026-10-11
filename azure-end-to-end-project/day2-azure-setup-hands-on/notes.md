# Day 1 — Kickoff, Architecture Scope, and Azure Setup

**Duration:** 2 hours | **Goal:** Provision every Azure resource the project needs and wire up security so no credentials are ever hardcoded.

> **Region for all resources: Central India (centralindia)** — cheapest India region with full service availability.

---

## What You Will Have at the End of Day 1

- Azure Resource Group containing all project resources
- ADLS Gen2 storage account with Bronze / Silver / Gold / Source containers
- Azure Key Vault holding all secrets (API credentials, SP credentials, storage name)
- Service Principal with correct RBAC roles
- Azure Databricks workspace + cluster
- Storage connected in Databricks using Service Principal OAuth direct access — no mount, no access key
- API auth verified — VoltGrid API login returns a live token
- Source blob readable from the instructor's shared storage via SAS token

---

## Glossary — What Is Each Azure Service?

| Term | Plain English |
|---|---|
| **Azure Subscription** | Your billing account. All resources are billed here. Think of it as your Azure "wallet". |
| **Resource Group** | A folder inside your subscription. Delete the group → everything inside is deleted too. |
| **ADLS Gen2** | Azure Data Lake Storage Gen2. Cloud file system for large files (Parquet, CSV, JSON). Central store for Bronze, Silver, Gold layers. |
| **Blob Storage** | Simpler cloud object store. ADLS Gen2 is Blob Storage + hierarchical namespace. `source` container is the blob landing zone. |
| **Container** | Top-level folder inside a Storage Account. This project uses: `bronze`, `silver`, `gold`, `source`. |
| **Azure Key Vault** | Secure vault for secrets (passwords, API keys). Only authorized identities can read from it. No hardcoded credentials anywhere in code. |
| **Secret** | A key-value pair in Key Vault. Value is encrypted. When read via `dbutils.secrets.get()`, Databricks masks it as `[REDACTED]`. |
| **Service Principal (SP)** | A non-human robot identity for applications. Databricks and ADF use it to log in to Azure. Has its own Client ID + Secret. |
| **RBAC** | Role-Based Access Control. Decides "who can do what on which resource". |
| **Azure Databricks** | Managed Apache Spark platform. Run Python/Spark notebooks here to ingest, clean, and transform data. |
| **Cluster** | A set of VMs inside Databricks that run your Spark code. Pay only when running — terminate after every session. |
| **Secret Scope** | Links a Databricks workspace to an Azure Key Vault. Notebooks call `dbutils.secrets.get(scope, key)` to read secrets without ever seeing the value. |
| **OAuth / SP Auth** | Databricks presents Client ID + Secret to Azure Entra ID → gets an OAuth token → accesses storage. Secret never travels directly to storage. |
| **Delta Lake** | Open-source format on top of Parquet + a transaction log. Enables ACID transactions, time travel, MERGE. Used for Silver and Gold layers. |
| **Medallion Architecture** | Three-layer data pattern: Bronze (raw, never changed) → Silver (cleaned) → Gold (aggregated for reports). |
| **ADF** | Azure Data Factory. No-code pipeline orchestration. Calls the VoltGrid API, paginates results, lands data in Bronze. |

---

## Part 1 — Azure Subscription Check

### 1.1 Find your Subscription ID

**Via Portal:**
1. Go to [portal.azure.com](https://portal.azure.com)
2. Search **Subscriptions** → click your subscription
3. Copy the **Subscription ID** (looks like `a1b2c3d4-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)
4. Confirm **Status = Active**

**Via CLI:**
```bash
az account show
az account show --query id -o tsv   # just the ID
```

### 1.2 Set a Budget Alert

Protect yourself from surprise charges:
1. Portal → **Cost Management + Billing** → **Budgets** → **+ Add**
2. Name: `ev-project-budget` | Reset period: Monthly | Amount: ₹1,500
3. Add two alerts: 50% → email, 90% → email

### 1.3 Install Azure CLI

**Windows (recommended — MSI installer):**
1. Download from: [aka.ms/installazurecliwindows](https://aka.ms/installazurecliwindows)
2. Run the `.msi` file → follow prompts → close and reopen terminal

**Windows (PowerShell):**
```powershell
Invoke-WebRequest -Uri https://aka.ms/installazurecliwindows -OutFile AzureCLI.msi
Start-Process msiexec.exe -ArgumentList '/I AzureCLI.msi /quiet' -Wait
Remove-Item AzureCLI.msi
```

**Mac:**
```bash
brew install azure-cli
```

**Login (all platforms):**
```bash
az --version
az login
az account show
```

### 1.4 Register Resource Providers

> If you skip this, you will hit: `MissingSubscriptionRegistration: The subscription is not registered to use namespace 'Microsoft.KeyVault'`

**Via Portal:**
1. Portal → **Subscriptions** → your subscription → **Resource providers** (left menu)
2. Search and Register each:

| Provider | Filter term |
|---|---|
| `Microsoft.KeyVault` | KeyVault |
| `Microsoft.Storage` | Storage |
| `Microsoft.Databricks` | Databricks |
| `Microsoft.EventHub` | EventHub |
| `Microsoft.DataFactory` | DataFactory |
| `Microsoft.ManagedIdentity` | ManagedIdentity |

**Via CLI:**
```cmd
az provider register --namespace Microsoft.KeyVault
az provider register --namespace Microsoft.Storage
az provider register --namespace Microsoft.Databricks
az provider register --namespace Microsoft.EventHub
az provider register --namespace Microsoft.DataFactory
az provider register --namespace Microsoft.ManagedIdentity
```

**Verify (wait ~1 minute first):**
```cmd
az provider show --namespace Microsoft.KeyVault --query registrationState -o tsv
az provider show --namespace Microsoft.Storage --query registrationState -o tsv
az provider show --namespace Microsoft.Databricks --query registrationState -o tsv
az provider show --namespace Microsoft.EventHub --query registrationState -o tsv
az provider show --namespace Microsoft.DataFactory --query registrationState -o tsv
az provider show --namespace Microsoft.ManagedIdentity --query registrationState -o tsv
```
All 6 must output `Registered` before proceeding.

---

## Part 2 — Create Resource Group

> **Cost: ₹0** — Resource Groups are free.

**Via Portal:**
1. Portal → **Resource groups** → **+ Create**
2. Subscription: yours | Name: `rg-ev-intelligence-dev` | Region: `Central India`
3. **Review + Create** → **Create**

**Via CLI:**
```cmd
az group create --name rg-ev-intelligence-dev --location centralindia
```

---

## Part 3 — Create ADLS Gen2 Storage Account

> **Cost: ~₹20-30/month** for ~10-15 GB of Bronze + Silver + Gold data.
> Choose: Performance = **Standard**, Redundancy = **LRS**

### 3.1 Create Storage Account

**Via Portal:**
1. Portal → **Storage accounts** → **+ Create**
2. Fill in:
   - Resource group: `rg-ev-intelligence-dev`
   - Storage account name: `evdatalakedev` *(globally unique, lowercase, no hyphens)*
   - Region: `Central India`
   - Performance: `Standard`
   - Redundancy: `LRS (Locally-redundant storage)`
3. **Advanced** tab → **Enable hierarchical namespace: ON** ← makes it ADLS Gen2
4. Access tier: `Cool`
5. **Review + Create** → **Create**

### 3.2 Create 4 Containers

Storage account → **Containers** → **+ Container** — create these one by one:

| Container | Purpose | Access Tier |
|---|---|---|
| `bronze` | Raw ingested data — never modified | Hot |
| `silver` | Cleaned and validated data | Hot |
| `gold` | Aggregated, analytics-ready data | Cool |
| `source` | Blob uploads: CSV, PDF, XML, JSON | Hot |

Each: Public access level = **Private**

### 3.3 Lifecycle Policy (Optional — saves 50% on Bronze after 30 days)

Storage account → **Lifecycle management** → **+ Add rule**:
- Name: `move-to-cool` | Base blobs last modified > 30 days → Move to Cool
- Base blobs last modified > 90 days → Move to Archive

### 3.4 Via CLI

```cmd
az storage account create --name evdatalakedev --resource-group rg-ev-intelligence-dev --location centralindia --sku Standard_LRS --kind StorageV2 --enable-hierarchical-namespace true --access-tier Cool
az storage container create --name bronze --account-name evdatalakedev --auth-mode login
az storage container create --name silver --account-name evdatalakedev --auth-mode login
az storage container create --name gold --account-name evdatalakedev --auth-mode login
az storage container create --name source --account-name evdatalakedev --auth-mode login
```

---

## Part 4 — Create Azure Key Vault

> **Cost: ~₹5 total for the entire project** — essentially free.
> Choose: Pricing tier = **Standard**

### 4.1 Create Key Vault

**Via Portal:**
1. Portal → **Key vaults** → **+ Create**
2. Resource group: `rg-ev-intelligence-dev` | Name: `kv-ev-intelligence-dev` | Region: `Central India` | Pricing tier: `Standard`
3. **Access configuration** tab → Permission model: **Azure role-based access control (RBAC)**
4. **Review + Create** → **Create**

### 4.2 Assign Yourself Key Vault Secrets Officer Role

> Without this you get: `Forbidden: Caller is not authorized to perform action — ForbiddenByRbac`

**Via Portal:**
1. Key Vault → **Access Control (IAM)** → **+ Add** → **Add role assignment**
2. Role: search `Key Vault Secrets Officer` → select → **Next**
3. Members: `User, group, or service principal` → **+ Select members** → search your email → **Select**
4. **Review + assign** → wait 1–2 minutes before adding secrets

**Role reference:**

| Identity | Role | Why |
|---|---|---|
| Your account | `Key Vault Secrets Officer` | Read + write secrets from CLI/Portal |
| `AzureDatabricks` enterprise app | `Key Vault Secrets User` | Secret scope reads Key Vault in notebooks |
| Service Principal | `Key Vault Secrets User` | Databricks reads secrets at runtime |
| ADF Managed Identity | `Key Vault Secrets User` | ADF reads secrets at runtime |

### 4.3 Add Initial Secrets

Key Vault → **Secrets** → **+ Generate/Import**:

| Secret Name | Value | Notes |
|---|---|---|
| `voltgrid-api-base-url` | `https://ev-project-navy-mu.vercel.app` | VoltGrid API host |
| `voltgrid-username` | `voltgrid_demo` | API login username |
| `voltgrid-password` | `EVcharge@AU2025` | API login password |
| `adls-account-name` | `evdatalakedev` | Storage account name |
| `sp-client-id` | *(fill after Part 5)* | Service Principal App ID |
| `sp-client-secret` | *(fill after Part 5)* | Service Principal password |
| `sp-tenant-id` | *(fill after Part 5)* | Azure Entra ID tenant |

**Via CLI:**
```cmd
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-api-base-url" --value "https://ev-project-navy-mu.vercel.app"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-username" --value "voltgrid_demo"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-password" --value "EVcharge@AU2025"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "adls-account-name" --value "evdatalakedev"
```

---

## Part 5 — Create Service Principal

> **Cost: ₹0** — Service Principals in Azure Entra ID are free.

A Service Principal is a non-human identity for applications. After creating it you get 3 values that go into Key Vault:

| Value | Key Vault Secret Name | What it looks like |
|---|---|---|
| Application (Client) ID | `sp-client-id` | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| Client Secret (password) | `sp-client-secret` | Long random string — shown only once |
| Tenant (Directory) ID | `sp-tenant-id` | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |

### 5.1 Via Portal

**Step 1 — Register the App:**
1. Portal → **App registrations** → **+ New registration**
2. Name: `sp-ev-intelligence-dev` | Supported account types: Single tenant | Redirect URI: blank
3. Click **Register**
4. On the Overview page — copy **Application (client) ID** and **Directory (tenant) ID**

**Step 2 — Create Client Secret:**
1. Left menu → **Certificates & secrets** → **+ New client secret**
2. Description: `ev-project-dev` | Expires: `180 days`
3. Click **Add** → **copy the Value immediately** (shown only once)

**Step 3 — Store in Key Vault:**
```cmd
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-client-id" --value "<Application ID>"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-client-secret" --value "<Secret Value>"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-tenant-id" --value "<Tenant ID>"
```

### 5.2 Via CLI (all-in-one)

Replace `<YOUR_SUBSCRIPTION_ID>` with your actual Subscription ID:
```cmd
az ad sp create-for-rbac --name sp-ev-intelligence-dev --role Contributor --scopes /subscriptions/<YOUR_SUBSCRIPTION_ID>/resourceGroups/rg-ev-intelligence-dev
```

Output:
```json
{
  "appId":       "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "displayName": "sp-ev-intelligence-dev",
  "password":    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "tenant":      "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

> **Save the `password` immediately — shown only once.**

Store in Key Vault:
```cmd
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-client-id" --value "<appId>"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-client-secret" --value "<password>"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-tenant-id" --value "<tenant>"
```

### 5.3 Assign Storage Blob Data Contributor Role to SP

| Role | What it allows |
|---|---|
| `Storage Blob Data Reader` | Read only |
| `Storage Blob Data Contributor` | Read + write + delete files ← use this |
| `Storage Blob Data Owner` | Full control + ACLs — avoid for service accounts |

**Via Portal:**
1. Portal → **Storage accounts** → `evdatalakedev` → **Access Control (IAM)**
2. **+ Add** → **Add role assignment**
3. Role: `Storage Blob Data Contributor` → **Next**
4. Members → **+ Select members** → search `sp-ev-intelligence-dev` → **Select**
5. **Review + assign**

**Via CLI (step by step):**
```cmd
az ad sp list --display-name sp-ev-intelligence-dev --query "[0].appId" -o tsv
```
Copy the appId, then:
```cmd
az ad sp show --id <appId> --query id -o tsv
```
Copy the Object ID, then:
```cmd
az storage account show --name evdatalakedev --resource-group rg-ev-intelligence-dev --query id -o tsv
```
Copy the storage resource ID, then:
```cmd
az role assignment create --assignee-object-id <SP-Object-ID> --assignee-principal-type ServicePrincipal --role "Storage Blob Data Contributor" --scope <storage-resource-id>
```

---

## Part 6 — Create Azure Databricks Workspace

> **Cost: ~₹40-45 per 2-hour session** — biggest cost in the project.
> Always terminate the cluster after every session. Auto-termination is 15 minutes.

### 6.1 Create Workspace

**Via Portal:**
1. Portal → **Azure Databricks** → **+ Create**
2. Resource group: `rg-ev-intelligence-dev` | Name: `dbw-ev-intelligence-dev` | Region: `Central India`
3. Pricing tier: **Trial (Premium - 14 Days Free DBUs)** ← free DBUs for new workspaces
4. **Review + Create** → **Create** (~3 minutes)

### 6.2 Create Cluster — Minimum Cost Settings

Databricks → **Compute** → **+ Create compute**

| Setting | Value | Why |
|---|---|---|
| Cluster name | `dev-cluster` | — |
| Policy | Unrestricted | — |
| **Cluster mode** | **Single Node** | No worker nodes = half the VM cost |
| **Access mode** | **Dedicated (Single user)** | Required for `dbutils.fs.mount()` |
| **Databricks runtime** | `15.4 LTS (Spark 3.5, Scala 2.12)` | Stable LTS — no extra cost |
| **Use Photon Acceleration** | **OFF** | Photon adds extra DBU charges |
| **Node type** | `Standard_DS3_v2` | 4 vCPU, 14 GB RAM — minimum viable |
| **Auto termination** | **15 minutes** | CRITICAL — kills cluster when idle |

> **Access mode note:**
> Standard/Shared mode blocks `dbutils.fs.mount()` with `Method not whitelisted`.
> Always use **Dedicated** mode for this project.

### 6.3 Grant AzureDatabricks App Access to Key Vault

> Without this, notebooks fail with: `PERMISSION_DENIED: Invalid permissions on KeyVault 403`

**Via Portal:**
1. Key Vault → **Access Control (IAM)** → **+ Add** → **Add role assignment**
2. Role: `Key Vault Secrets User` → **Next**
3. Members → **+ Select members** → search **`AzureDatabricks`** → select → **Review + assign**
4. Wait 2 minutes before creating the secret scope

**Via CLI:**
```cmd
az ad sp list --display-name "AzureDatabricks" --query "[0].id" -o tsv
```
Copy the ID, then:
```cmd
az keyvault show --name kv-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query id -o tsv
```
Copy the Key Vault resource ID, then:
```cmd
az role assignment create --assignee-object-id <AzureDatabricks-id> --assignee-principal-type ServicePrincipal --role "Key Vault Secrets User" --scope <keyvault-resource-id>
```

### 6.4 Create Key Vault-Backed Secret Scope

You need two values from Key Vault:
1. Portal → **Key vaults** → `kv-ev-intelligence-dev` → **Properties**
2. Copy **Vault URI** (e.g. `https://kv-ev-intelligence-dev.vault.azure.net/`)
3. Copy **Resource ID** (long path starting with `/subscriptions/...`)

Create the scope:
1. Open your Databricks workspace URL + append `#secrets/createScope`:
   `https://adb-XXXXXXX.azuredatabricks.net#secrets/createScope`
2. Fill in:
   - **Scope Name:** `kv-ev-scope`
   - **Manage Principal:** `All Users`
   - **DNS Name:** Vault URI from above
   - **Resource ID:** Resource ID from above
3. Click **Create**

---

## Part 7 — Connect Storage and Verify Everything

### 7.1 Connect ADLS Gen2 — SP OAuth Direct Access

Create notebook `00b_connect_storage_no_mount` in Databricks → Workspace → + New → Notebook.

**Cell 1 — Load secrets:**
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

**Cell 2 — Configure Spark OAuth:**
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

**Cell 3 — Path helper + verify all 4 containers:**
```python
def abfss(container: str, path: str = "") -> str:
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base

for container in ["bronze", "silver", "gold", "source"]:
    try:
        items = dbutils.fs.ls(abfss(container))
        print(f"  {container:<8} OK — {len(items)} items")
    except Exception as e:
        print(f"  {container:<8} ERROR — {e}")
```

**Expected output (Day 1 — containers are empty):**
```
  bronze   OK — 0 items
  silver   OK — 0 items
  gold     OK — 0 items
  source   OK — 0 items
```

> After every cluster restart: re-run Cells 1, 2, 3 to re-establish the session config.

### 7.2 Verify VoltGrid API Auth

Create notebook `01_verify_api_auth`:

```python
import requests

SCOPE = "kv-ev-scope"

api_base_url = dbutils.secrets.get(scope=SCOPE, key="voltgrid-api-base-url")
username     = dbutils.secrets.get(scope=SCOPE, key="voltgrid-username")
password     = dbutils.secrets.get(scope=SCOPE, key="voltgrid-password")

# Step 1: get token
resp = requests.post(
    f"{api_base_url}/api/auth/login/",
    json={"username": username, "password": password},
    timeout=10,
)
resp.raise_for_status()
token = resp.json()["token"]

print(f"Token acquired: {token[:8]}...[REDACTED]")

# Step 2: test one endpoint
headers = {"Authorization": f"Token {token}"}
r = requests.get(f"{api_base_url}/api/db/payments/?page=1&page_size=5", headers=headers, timeout=10)
r.raise_for_status()
data = r.json()

pg = data.get("pagination", {})
print(f"Payments total: {pg.get('total', 0):,}, pages: {pg.get('total_pages', 0):,}")
print("API auth working correctly.")
```

**Expected output:**
```
Token acquired: a1b2c3d4...[REDACTED]
Payments total: 125,430, pages: 25,086
API auth working correctly.
```

**Token auth flow:**
```
Key Vault (username + password)
    → POST /api/auth/login/  → token (in memory only)
    → Authorization: Token <value>
    → all 18 API endpoints
    → token discarded when notebook ends
```

### 7.3 Read Source Blob via SAS Token

The project uses a shared external blob `dataenggdailystorage` containing pre-loaded source CSVs. Access is via SAS token — read + list only.

**Step 1 — Add 3 secrets to Key Vault:**

| Secret Name | Value |
|---|---|
| `source-storage-account` | `dataenggdailystorage` |
| `source-container` | `source` |
| `source-sas-token` | Provided during session (paste exactly — no quotes) |

```cmd
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "source-storage-account" --value "dataenggdailystorage"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "source-container" --value "source"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "source-sas-token" --value "<paste token here>"
```

**Step 2 — Create notebook `02_read_source_blob`:**

```python
SCOPE = "kv-ev-scope"

STORAGE_ACCOUNT = dbutils.secrets.get(scope=SCOPE, key="source-storage-account")
CONTAINER       = dbutils.secrets.get(scope=SCOPE, key="source-container")
SAS_TOKEN       = dbutils.secrets.get(scope=SCOPE, key="source-sas-token")

# SAS tokens use wasbs://, NOT abfss://
spark.conf.set(
    f"fs.azure.sas.{CONTAINER}.{STORAGE_ACCOUNT}.blob.core.windows.net",
    SAS_TOKEN
)
print(f"Storage account : {STORAGE_ACCOUNT}")
print(f"Container       : {CONTAINER}")
print("Spark SAS config set — OK")

# List top-level folders
base_path = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/"
for item in dbutils.fs.ls(base_path):
    print(f"  {item.name}")

# Read all CSVs using glob (files are 4 folder levels deep: year/month/day/hour)
glob_path = f"{base_path}realtime/charging_sessions/*/*/*/*/*.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(glob_path)
print(f"Total rows: {df.count():,}")
display(df.limit(10))
```

**Protocol rule:**
| Situation | Protocol | Why |
|---|---|---|
| Your own ADLS Gen2 (Bronze/Silver/Gold) | `abfss://` | SP OAuth — requires hierarchical namespace |
| External blob with SAS token | `wasbs://` | SAS tokens work with classic blob protocol |

---

## Part 8 — All 6 Ways to Connect Databricks to ADLS Gen2

| Method | When to use | Cluster mode | Credentials |
|---|---|---|---|
| **SP OAuth Direct** ← this project | Connecting to your own ADLS | Any | SP Client ID + Secret |
| **SAS Token** | Accessing someone else's storage | Any | SAS token string |
| **Storage Access Key** | Dev testing only — avoid | Any | Account key (full access) |
| **Mount + SP OAuth** | Legacy notebooks only | Dedicated only | SP Client ID + Secret |
| **Managed Identity** | Production / no-credential setup | Any | None — Azure handles it |
| **Unity Catalog** | Enterprise / shared workspace | Any | Admin sets up once |

---

## Part 9 — Architecture Diagram

```
[Source Systems]
    |-- VoltGrid API (REST / 18 endpoints / CDC via updated_at)
    |-- Blob Storage (CSV / PDF / XML / JSON uploads)
    |-- Event Hub (IoT Streaming JSON)
                |
         [ADF] ← pagination + watermark
         [Databricks Auto Loader] ← blob ingestion
         [Databricks Streaming] ← Event Hub
                |
         [ADLS Gen2 — evdatalakedev]
         abfss://bronze@evdatalakedev...  ← raw, append-only
         abfss://silver@evdatalakedev...  ← cleaned, MERGE upsert (Delta)
         abfss://gold@evdatalakedev...    ← aggregated, star schema (Delta)
                |
         [Azure Databricks]
         Delta Lake tables
                |
         [Power BI / Synapse Analytics]

Auth:
  Databricks → Entra ID (SP OAuth) → ADLS Gen2
  Databricks → Key Vault (secret scope) → secrets at runtime
  Databricks → VoltGrid API (username/password → token) → 18 endpoints
```

---

## Day 1 Cost Summary

| Resource | Cost |
|---|---|
| Resource Group | ₹0 |
| ADLS Gen2 + 4 containers | ~₹2/day |
| Key Vault | ~₹0.30/day |
| Service Principal | ₹0 |
| Databricks workspace + cluster | ~₹40-45 per 2-hour session |
| **Day 1 total** | **~₹45-47** |

---

## End of Session — STOP THE CLUSTER

**Do this every time before closing your laptop:**
1. Databricks → **Compute** → `dev-cluster` → **Terminate**
2. Wait for status: **Terminated**

---

## Day 1 Checklist

- [ ] Budget alert set at ₹1,500/month
- [ ] All 6 resource providers registered — all show `Registered`
- [ ] Resource Group `rg-ev-intelligence-dev` created in Central India
- [ ] Storage Account `evdatalakedev` created (Standard LRS, hierarchical namespace ON)
- [ ] Containers: `bronze`, `silver`, `gold`, `source` created — all Private
- [ ] Key Vault `kv-ev-intelligence-dev` created (Standard tier, RBAC model)
- [ ] Your account has `Key Vault Secrets Officer` role on Key Vault
- [ ] Secrets added: `voltgrid-api-base-url`, `voltgrid-username`, `voltgrid-password`, `adls-account-name`
- [ ] Service Principal `sp-ev-intelligence-dev` created
- [ ] SP secrets stored in Key Vault: `sp-client-id`, `sp-client-secret`, `sp-tenant-id`
- [ ] SP has `Storage Blob Data Contributor` role on `evdatalakedev`
- [ ] Databricks workspace `dbw-ev-intelligence-dev` created (Trial tier)
- [ ] Cluster `dev-cluster` — Single Node, DS3_v2, Photon OFF, 15-min auto-terminate
- [ ] `AzureDatabricks` app has `Key Vault Secrets User` role on Key Vault
- [ ] Secret scope `kv-ev-scope` created in Databricks
- [ ] Notebook `00b_connect_storage_no_mount` — all 4 containers returned `OK`
- [ ] Notebook `01_verify_api_auth` — token acquired, payments endpoint returned data
- [ ] Secrets added: `source-storage-account`, `source-container`, `source-sas-token`
- [ ] Notebook `02_read_source_blob` — CSV files readable via glob pattern
- [ ] **Cluster terminated**

---

## Common Errors on Day 1

| Error | Fix |
|---|---|
| `az login` fails | Try `az login --use-device-code` |
| `MissingSubscriptionRegistration` | `az provider register --namespace <e.g. Microsoft.KeyVault>` → wait 1-2 min → retry |
| `Forbidden: ForbiddenByRbac` on `az keyvault secret set` | Assign `Key Vault Secrets Officer` role to your account → wait 1-2 min → retry |
| `Conflict: ObjectIsDeletedButRecoverable` | Recover soft-deleted secret: `az keyvault secret recover --vault-name kv-ev-intelligence-dev --name <name>` |
| Storage account name taken | Add initials: `evdatalakedevhs` |
| Key Vault name taken | Add random suffix: `kv-ev-dev-01` |
| `403 Forbidden` on `dbutils.fs.ls(abfss(...))` | SP missing `Storage Blob Data Contributor` role on `evdatalakedev` |
| `PERMISSION_DENIED: Invalid permissions on KeyVault 403` | `AzureDatabricks` app needs `Key Vault Secrets User` role → Key Vault → IAM → assign → wait 2 min → retry |
| `Method dbutils.mount() is not whitelisted` | Cluster is Standard mode — edit cluster → Access mode → Dedicated → restart |
| Cluster won't start | Check region quota — try `Standard_D3_v2` if `Standard_DS3_v2` is unavailable |
| `Secret does not exist` | Secret name typo — check Key Vault → Secrets for exact name |
| `InvalidAuthenticationInfo` on SAS | Using `abfss://` instead of `wasbs://` for SAS token storage |
| API login returns 401 | Username or password in Key Vault doesn't match the Django database |

---

## Quick Reference

```
Resource Group  : rg-ev-intelligence-dev    (Central India)
Storage Account : evdatalakedev             (Standard LRS, HNS ON)
Key Vault       : kv-ev-intelligence-dev    (Standard, RBAC model)
Service Principal: sp-ev-intelligence-dev
Databricks WS   : dbw-ev-intelligence-dev
Secret Scope    : kv-ev-scope
Your ADLS path  : abfss://<container>@evdatalakedev.dfs.core.windows.net/<path>
Source blob     : wasbs://source@dataenggdailystorage.blob.core.windows.net/<path>
```
