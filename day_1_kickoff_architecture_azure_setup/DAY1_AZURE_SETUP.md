# Day 1 — Kickoff, Architecture Scope, and Azure Setup
**Session:** 2 hours | **Goal:** Provision every Azure resource the project needs and wire up security so no credentials are ever hardcoded.

> **Region for all resources: Central India (centralindia)** — cheapest India region with full service availability.
> **Free credit tip:** New Azure accounts get ₹13,370 (~$200 USD) free for 30 days. If you are on a new account, this entire project costs ₹0.

---

## Glossary — What Is Each Azure Service?

Read this once before you start. You will create all of these resources today.

| Term | Plain English Definition |
|---|---|
| **Azure Subscription** | Your billing account. All resources you create are billed to this subscription. Think of it as your Azure "wallet". |
| **Resource Group** | A folder inside your subscription that holds related resources together. When you delete the Resource Group, everything inside it is deleted too. Useful for cleanup at end of project. |
| **ADLS Gen2** | Azure Data Lake Storage Gen2. A cloud file system for storing large amounts of data as files (Parquet, CSV, JSON, PDF). This is your data lake — the central store for Bronze, Silver, and Gold data layers. |
| **Blob Storage** | Azure Blob Storage. A simpler cloud object store (like S3). ADLS Gen2 is actually built on top of Blob Storage, but with a file-system hierarchy. In this project, the `source` container is used as a blob landing zone for raw CSV, PDF, XML, and JSON uploads. |
| **Container** | A top-level folder inside a Storage Account. Like a bucket in S3. In this project: `bronze`, `silver`, `gold`, `source` are your 4 containers. Each holds files and subfolders. |
| **Azure Key Vault** | A secure vault for storing secrets (passwords, API keys, connection strings). Only authorized identities can read from it. No code should ever have a hardcoded password — it should read from Key Vault at runtime instead. |
| **Secret** | A key-value pair stored in Key Vault. Example: key = `voltgrid-password`, value = `EVcharge@AU2025`. The value is encrypted and access-controlled. |
| **Service Principal (SP)** | A non-human identity (like a robot user) that your applications (Databricks, ADF) use to log in to Azure. Has its own client ID + client secret. You assign it specific permissions via RBAC. |
| **RBAC** | Role-Based Access Control. A system for deciding "who can do what on which resource". Example: your Service Principal gets the `Storage Blob Data Contributor` role on the storage account, which means it can read and write files but cannot delete the storage account. |
| **Azure Databricks** | A managed Apache Spark platform. You run Python/Spark notebooks here to ingest, clean, and transform data. It connects to ADLS Gen2 for reading/writing data files. |
| **Cluster** | The compute engine inside Databricks. A cluster is a set of virtual machines that run your Spark code. You pay only when the cluster is running — so always terminate it when done. |
| **Secret Scope** | A Databricks feature that links a Databricks workspace to an Azure Key Vault. Once linked, notebooks can call `dbutils.secrets.get(scope, key)` to read any Key Vault secret without ever seeing its value. |
| **OAuth / Service Principal Auth** | The recommended way to connect Databricks to ADLS Gen2. Databricks presents its Client ID + Client Secret to Azure Entra ID, which returns an OAuth access token. That token is used to access storage. The secret never travels directly to the storage account. |
| **Access Key Auth** | The simpler but less secure way to connect to ADLS Gen2. A static 512-bit key directly associated with the storage account. Anyone with this key has full root-level access to all containers. |
| **Delta Lake** | An open-source storage format built on Parquet files + a transaction log. Enables ACID transactions, time travel, and MERGE (upsert) operations on data lake files. Used for Silver and Gold layers in this project. |
| **Medallion Architecture** | A data organization pattern with three layers: Bronze (raw data, never changed), Silver (cleaned and validated), Gold (aggregated, ready for reports). Data flows one-way: Bronze → Silver → Gold. |
| **ADF** | Azure Data Factory. A no-code/low-code pipeline orchestration tool. Used to call the VoltGrid API, paginate through results, and land data in Bronze. Free tier covers this entire project. |

---

## What You Will Have at the End of Day 1
- Azure Resource Group containing all project resources
- ADLS Gen2 storage account with Bronze / Silver / Gold / Source containers
- Azure Databricks workspace linked to storage via OAuth (secure)
- Azure Key Vault holding all secrets (API credentials, SP credentials, storage name)
- Service Principal with correct RBAC roles
- Storage mounted in Databricks using Service Principal OAuth — no access key

---

## Part 1 — Azure Subscription Check (10 min)

> **Cost: ₹0** — subscription verification and CLI setup are free.

### 1.1 Verify your subscription and find your Subscription ID

**What is a Subscription ID?**
Every Azure account has a Subscription — a billing container. The Subscription ID is a unique identifier for your billing account, formatted like `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`. You will need it when creating the Service Principal in Part 5.

**Via Portal:**
1. Go to [https://portal.azure.com](https://portal.azure.com)
2. In the top search bar, search **Subscriptions** and click it
3. You will see a list of subscriptions. Click the name of your subscription
4. On the **Overview** page you will see:
   - **Subscription ID** — copy and save this (looks like `a1b2c3d4-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)
   - **Status** — confirm it says **Active**
   - **Display name** — this is your subscription's friendly name

**Via CLI:**
```bash
az account show
# Look for the "id" field in the output — that is your Subscription ID
# Example output:
# {
#   "id": "a1b2c3d4-xxxx-xxxx-xxxx-xxxxxxxxxxxx",   ← this is your Subscription ID
#   "name": "My Azure Subscription",
#   "state": "Enabled"
# }

# Or get just the ID directly:
az account show --query id -o tsv
```

### 1.2 Set a Budget Alert First (do this before anything else)
Protect yourself from surprise charges:
1. Portal → search **Cost Management + Billing**
2. Left menu → **Budgets** → **+ Add**
3. Fill in:
   - **Name:** `ev-project-budget`
   - **Reset period:** Monthly
   - **Amount:** ₹1,500
4. **Alerts** tab → add two alerts:
   - 50% threshold → email you
   - 90% threshold → email you
5. Save

### 1.3 Install tools on your laptop
Open a terminal and run:
```bash
# Azure CLI
winget install Microsoft.AzureCLI          # Windows
brew install azure-cli                     # Mac

# Verify
az --version

# Login
az login
# A browser window opens — sign in with your Azure account
az account show   # should show your subscription
```

---

## Part 2 — Create Resource Group (5 min)

> **Cost: ₹0** — Resource Groups are free containers. No charges for the group itself.

**What is a Resource Group?**
A Resource Group is a logical container for all project resources. Think of it as a project folder. Keep everything in one group so you can delete cleanly at the end — one delete = everything gone.

### 2.1 Via Azure Portal
1. Portal → search **Resource groups** → click **+ Create**
2. Fill in:
   - **Subscription:** your subscription
   - **Resource group name:** `rg-ev-intelligence-dev`
   - **Region:** `Central India`
3. Click **Review + Create** → **Create**

### 2.2 Via CLI (faster)
```bash
az group create \
  --name rg-ev-intelligence-dev \
  --location centralindia
```

---

## Part 3 — Create ADLS Gen2 Storage Account (15 min)

> **Cost: ~₹1.68/GB/month (Hot tier) | ~₹0.84/GB/month (Cool tier)**
> Estimated for this project: **~₹20-30/month** for ~10-15 GB of Bronze + Silver + Gold data.
>
> **Minimum cost config to select:**
> - Performance: **Standard** (NOT Premium — Premium is 5x more expensive)
> - Redundancy: **LRS** (Locally Redundant) — NOT GRS or ZRS (2-3x more expensive)

**What is ADLS Gen2?**
ADLS Gen2 = Azure Data Lake Storage Gen2. It is your central file store — every raw file, every cleaned Parquet file, every Delta table lives here. It is organized into Containers (top-level buckets), and inside those, folders and files.

**What is Blob Storage vs ADLS Gen2?**
Azure Blob Storage is a flat object store — it has no real folder hierarchy. ADLS Gen2 is Blob Storage + hierarchical namespace (real folders). This makes it efficient for large-scale analytics with Spark. In this project, ADLS Gen2 is used for Bronze/Silver/Gold, and the `source` container acts as a blob landing zone for uploaded files (CSV, PDF, XML, JSON).

**What is a Container?**
A Container is a top-level folder inside the storage account. It groups related files. In this project you have 4 containers: `bronze` (raw ingested data), `silver` (cleaned data), `gold` (aggregated data), `source` (raw file uploads).

### 3.1 Create Storage Account
1. Portal → search **Storage accounts** → **+ Create**
2. Fill in:
   - **Resource group:** `rg-ev-intelligence-dev`
   - **Storage account name:** `evdatalakedev` *(must be globally unique, lowercase, no hyphens)*
   - **Region:** `Central India`
   - **Performance:** `Standard` ← cost choice
   - **Redundancy:** `LRS (Locally-redundant storage)` ← cost choice
3. Click **Advanced** tab:
   - **Enable hierarchical namespace:** `ON` ← this makes it ADLS Gen2, required
   - **Access tier:** `Cool` ← saves cost; we override to Hot per-container as needed
4. Click **Review + Create** → **Create**

### 3.2 Create Containers (Medallion Zones)
Once storage is created:
1. Go to your storage account → left menu **Containers** → **+ Container**
2. Create these 4 containers one by one:

| Container Name | Purpose | Access Tier |
|---|---|---|
| `bronze` | Raw ingested data — never modified | Hot (active ingestion) |
| `silver` | Cleaned and validated data | Hot (active queries) |
| `gold` | Aggregated, analytics-ready data | Cool (read occasionally) |
| `source` | Blob uploads: CSV, PDF, XML, JSON files | Hot (uploads landing zone) |

For each:
- **Name:** as above
- **Public access level:** Private (no anonymous access)
- Click **Create**

### 3.3 Set Lifecycle Policy to Move Old Data to Cool Automatically
This saves 50% on Bronze storage after 30 days:
1. Storage account → left menu **Lifecycle management** → **+ Add rule**
2. Rule name: `move-to-cool`
3. Base blobs: last modified **> 30 days** → Move to **Cool**
4. Base blobs: last modified **> 90 days** → Move to **Archive**
5. Save

### 3.4 Via CLI
```bash
# Create storage account
az storage account create \
  --name evdatalakedev \
  --resource-group rg-ev-intelligence-dev \
  --location centralindia \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --access-tier Cool

# Create containers
for container in bronze silver gold source; do
  az storage container create \
    --name $container \
    --account-name evdatalakedev \
    --auth-mode login
done
```

---

## Part 4 — Create Azure Key Vault (10 min)

> **Cost: ~₹5 total for the entire 18-day project** — essentially free.
> ~100 secret reads/day × 18 days = 1,800 operations. Charged per 10,000 operations = negligible.
>
> **Minimum cost config to select:**
> - Pricing tier: **Standard** (NOT Premium — Premium adds HSM hardware, not needed for dev)

**What is Azure Key Vault?**
Key Vault is a secure, access-controlled vault for storing secrets. A secret is any sensitive value — a password, an API key, a connection string. Only identities you explicitly authorize can read secrets from it. Notebooks never contain raw passwords; they call Key Vault at runtime to get the value. This means: if a secret leaks, you rotate it in Key Vault and every notebook gets the new value automatically — no code changes needed.

**What is a Secret?**
A key-value pair stored in Key Vault. The key is a name you choose (e.g. `voltgrid-password`). The value is the sensitive string (e.g. `EVcharge@AU2025`). The value is encrypted at rest and in transit. When you read it in a Databricks notebook via `dbutils.secrets.get()`, the value is masked in logs — it is never printed in plaintext.

**How auth works in this project:**
The VoltGrid API uses Django REST Framework token auth — there is no direct database connection from Azure. Key Vault stores the **username + password** of the API user. At runtime, Databricks calls `POST /api/auth/login/` with those credentials and receives a token. Every subsequent API call uses `Authorization: Token <token>` in the header. The token is held in memory only — it is never written to disk or stored anywhere.

### 4.1 Create Key Vault
1. Portal → search **Key vaults** → **+ Create**
2. Fill in:
   - **Resource group:** `rg-ev-intelligence-dev`
   - **Key vault name:** `kv-ev-intelligence-dev` *(globally unique)*
   - **Region:** `Central India`
   - **Pricing tier:** `Standard` ← cost choice
3. **Access configuration** tab:
   - Permission model: **Vault access policy** *(easier for beginners — switch to RBAC on Day 2)*
4. Click **Review + Create** → **Create**

### 4.2 Add Your First Secrets
Go to Key Vault → left menu **Secrets** → **+ Generate/Import**

Add these secrets now (you will add more on Day 2):

| Secret Name | Value | What it is |
|---|---|---|
| `voltgrid-api-base-url` | `https://ev-project-navy-mu.vercel.app` | VoltGrid API host |
| `voltgrid-username` | `voltgrid_demo` | API login username |
| `voltgrid-password` | `EVcharge@AU2025` | API login password |
| `adls-account-name` | `evdatalakedev` | Storage account name (not sensitive, but centralised) |
| `sp-client-id` | *(fill after Part 5)* | Service Principal App ID |
| `sp-client-secret` | *(fill after Part 5)* | Service Principal password |
| `sp-tenant-id` | *(fill after Part 5)* | Azure Entra ID tenant |

> **Why username/password in Key Vault and not a hardcoded token?**
> DRF tokens persist in the database. If the token ever rotates or the user is recreated, a hardcoded token breaks every pipeline. Storing username + password means Databricks can always call `/api/auth/login/` to get a fresh valid token at the start of each run — no manual rotation needed.

### 4.3 Via CLI
```bash
KV="kv-ev-intelligence-dev"

az keyvault create \
  --name $KV \
  --resource-group rg-ev-intelligence-dev \
  --location centralindia \
  --sku standard

az keyvault secret set --vault-name $KV --name "voltgrid-api-base-url" --value "https://ev-project-navy-mu.vercel.app"
az keyvault secret set --vault-name $KV --name "voltgrid-username"     --value "voltgrid_demo"
az keyvault secret set --vault-name $KV --name "voltgrid-password"     --value "EVcharge@AU2025"
az keyvault secret set --vault-name $KV --name "adls-account-name"     --value "evdatalakedev"
```

---

## Part 5 — Create Service Principal (15 min)

> **Cost: ₹0** — Service Principals in Azure Entra ID are completely free to create and use.

**What is a Service Principal?**
A Service Principal is a non-human identity — like a robot user account for your application. Instead of Databricks logging in as *you* (a human), it logs in as the Service Principal. This is safer because: (a) the SP only has the permissions you explicitly gave it, (b) if the SP credential is compromised, you rotate it without affecting any human accounts, (c) human accounts can be disabled or have passwords changed, which would break pipelines.

**What is RBAC?**
RBAC = Role-Based Access Control. After creating the SP, you assign it roles on specific resources. A role says "what actions are allowed". A scope says "on which resource". Example: SP + role `Storage Blob Data Contributor` + scope `evdatalakedev` = "the SP can read and write blobs in that storage account, but cannot delete the account itself."

**What values will you get from this step?**
After creating the SP you will have 3 values that go into Key Vault:

| Value Name | Key Vault Secret Name | What it looks like | What it is |
|---|---|---|---|
| Application (Client) ID | `sp-client-id` | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` | The SP's unique ID — its "username" |
| Client Secret (password) | `sp-client-secret` | A long random string | The SP's password — shown only once |
| Tenant (Directory) ID | `sp-tenant-id` | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` | Your Azure Entra ID directory ID |

---

### Option A — Via Azure Portal (UI)

#### Step 1 — Register the App (creates the SP identity)

1. Go to [https://portal.azure.com](https://portal.azure.com)
2. In the top search bar, search **App registrations** and click it
3. Click **+ New registration**
4. Fill in:
   - **Name:** `sp-ev-intelligence-dev`
   - **Supported account types:** `Accounts in this organizational directory only (Single tenant)`
   - **Redirect URI:** leave blank
5. Click **Register**

You are now on the app's **Overview** page. **Copy and save these two values now:**
- **Application (client) ID** — this is your `sp-client-id`
- **Directory (tenant) ID** — this is your `sp-tenant-id`

> Both look like: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
> They are visible on the Overview page any time — you can always come back here.

#### Step 2 — Create a Client Secret (the SP's password)

1. On the same app page, click **Certificates & secrets** in the left menu
2. Click **+ New client secret**
3. Fill in:
   - **Description:** `ev-project-dev`
   - **Expires:** `180 days` (6 months — set a calendar reminder to rotate)
4. Click **Add**
5. You will see the secret in the **Value** column — **copy it immediately**

> **Critical:** The secret value is shown only once. If you navigate away without copying it, you must delete and create a new one. It will never be shown again.

This copied value is your `sp-client-secret`.

#### Step 3 — Find your Tenant ID (if you missed it above)

1. Search **Azure Active Directory** or **Microsoft Entra ID** in the portal search bar
2. On the **Overview** page, you will see **Tenant ID** — that is your `sp-tenant-id`

#### Step 4 — Store all 3 values in Key Vault

1. Go to Key Vault → `kv-ev-intelligence-dev` → left menu **Secrets**
2. Click **+ Generate/Import** for each secret:

| Secret Name | Value to paste |
|---|---|
| `sp-client-id` | Application (client) ID from Step 1 |
| `sp-client-secret` | Secret Value from Step 2 |
| `sp-tenant-id` | Directory (tenant) ID from Step 1 |

---

### Option B — Via CLI (faster, all-in-one)

> **Before running:** replace `<YOUR_SUBSCRIPTION_ID>` with the ID you copied in Part 1.1

```bash
# Create the Service Principal and assign Contributor role to the Resource Group
az ad sp create-for-rbac \
  --name sp-ev-intelligence-dev \
  --role Contributor \
  --scopes /subscriptions/<YOUR_SUBSCRIPTION_ID>/resourceGroups/rg-ev-intelligence-dev
```

This outputs:
```json
{
  "appId":       "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "displayName": "sp-ev-intelligence-dev",
  "password":    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "tenant":      "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

> **Save this output immediately — the `password` is shown only this one time.**
> If you lose it, you must go to App Registrations → Certificates & secrets → delete and create a new secret.

Store all 3 values in Key Vault immediately:
```bash
KV="kv-ev-intelligence-dev"
az keyvault secret set --vault-name $KV --name "sp-client-id"     --value "<appId from output>"
az keyvault secret set --vault-name $KV --name "sp-client-secret" --value "<password from output>"
az keyvault secret set --vault-name $KV --name "sp-tenant-id"     --value "<tenant from output>"
```

---

### 5.3 Assign Storage Blob Data Contributor Role to the SP

This step gives the SP permission to read and write files in the ADLS Gen2 storage account. Without this, the Databricks mount will fail with a 403 error.

**What role does what:**

| Role | What it allows | What it blocks |
|---|---|---|
| `Storage Blob Data Reader` | Read files only | Cannot write or delete |
| `Storage Blob Data Contributor` | Read + write + delete files | Cannot delete the storage account itself |
| `Storage Blob Data Owner` | Full control including ACLs | Dangerous — avoid for service accounts |

We use `Storage Blob Data Contributor` — enough for Databricks to read and write all layers.

#### Via Portal:

1. Go to [https://portal.azure.com](https://portal.azure.com)
2. Search **Storage accounts** → click `evdatalakedev`
3. In the left menu, click **Access Control (IAM)**
4. Click **+ Add** → **Add role assignment**
5. On the **Role** tab: search for `Storage Blob Data Contributor` → select it → click **Next**
6. On the **Members** tab:
   - **Assign access to:** `User, group, or service principal`
   - Click **+ Select members**
   - In the search box, type `sp-ev-intelligence-dev` → click it → click **Select**
7. Click **Review + assign** → **Review + assign** again to confirm

To verify it worked:
1. On the same `evdatalakedev` → **Access Control (IAM)** page
2. Click **Role assignments** tab
3. You should see `sp-ev-intelligence-dev` listed under `Storage Blob Data Contributor`

#### Via CLI:
```bash
# Get the storage account's full resource ID
STORAGE_ID=$(az storage account show \
  --name evdatalakedev \
  --resource-group rg-ev-intelligence-dev \
  --query id -o tsv)

# Get the SP's object ID (different from appId — Azure uses object ID for role assignments)
SP_OID=$(az ad sp show --id <appId from earlier> --query id -o tsv)

# Assign the role
az role assignment create \
  --assignee-object-id $SP_OID \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ID

# Verify
az role assignment list --scope $STORAGE_ID --query "[].{Role:roleDefinitionName, Principal:principalName}" -o table
```

---

## Part 6 — Create Azure Databricks Workspace (15 min)

> **Cost: ~₹40-45 per 2-hour session** — this is the biggest cost in the project.
> **Total across 18 sessions: ~₹810**
>
> **Minimum cost config to select:**
> - Pricing tier: **Trial** (14-day free DBU credits for new workspaces) — use this first
> - After trial ends: **Standard** (~₹3.0/DBU-hour) NOT Premium (~₹4.5/DBU-hour)
> - Premium is only needed for Unity Catalog — skip for this project
>
> **Most important cost rule: always terminate your cluster after each session.**
> A forgotten running cluster overnight = ₹40/hr × 8 hrs = ₹320 wasted.

### 6.1 Create Workspace
1. Portal → search **Azure Databricks** → **+ Create**
2. Fill in:
   - **Resource group:** `rg-ev-intelligence-dev`
   - **Workspace name:** `dbw-ev-intelligence-dev`
   - **Region:** `Central India`
   - **Pricing tier:** `Trial (Premium - 14 Days Free DBUs)` ← use this to get free DBUs
3. Click **Review + Create** → **Create** (takes ~3 minutes)

### 6.2 Launch Workspace
1. Once deployed, click **Launch Workspace**
2. This opens the Databricks UI at `https://adb-xxxxxxxxx.azuredatabricks.net`

### 6.3 Create a Cluster — Minimum Cost Settings

> Every setting below is chosen to minimize cost. Do not change these for dev.

1. Databricks left menu → **Compute** → **+ Create compute**
2. Fill in every field exactly as shown:

| Setting | Value to Select | Why |
|---|---|---|
| Cluster name | `dev-cluster` | — |
| Policy | Unrestricted | — |
| **Cluster mode** | **Single Node** | No worker nodes = half the VM cost |
| Access mode | Single user | — |
| **Databricks runtime** | `15.4 LTS (Spark 3.5, Scala 2.12)` | Stable, no extra cost |
| **Use Photon Acceleration** | **OFF** | Photon adds extra DBU charges |
| **Node type** | `Standard_DS3_v2` | 4 vCPU, 14 GB RAM — minimum viable for Spark |
| **Auto termination** | **15 minutes** | MOST IMPORTANT — kills cluster when idle |

3. Click **Create compute** (takes ~5 minutes to start)

> **Cost breakdown per session:**
> - VM: Standard_DS3_v2 ≈ ₹18/hr × 2 hr = ₹36
> - DBU: 0.75 DBU/hr × ₹3.0 × 2 hr = ₹4.5
> - **Total per 2-hour session: ~₹40-45**

### 6.4 Add Key Vault-backed Secret Scope in Databricks

**What is a Secret Scope?**
A Secret Scope is a named link between your Databricks workspace and an Azure Key Vault. Once created, any notebook can call `dbutils.secrets.get(scope="kv-ev-scope", key="some-secret")` to read a Key Vault secret. The secret value is never shown in notebook output — Databricks masks it as `[REDACTED]` automatically.

**Two values you need from Key Vault before doing this step:**

**Value 1 — Vault URI**
1. Go to [https://portal.azure.com](https://portal.azure.com)
2. Search **Key vaults** → click `kv-ev-intelligence-dev`
3. In the left menu, click **Properties** (under Settings)
4. Copy the **Vault URI** — it looks like:
   `https://kv-ev-intelligence-dev.vault.azure.net/`

**Value 2 — Resource ID**
1. On the same **Properties** page (you are already there)
2. Copy the **Resource ID** — it looks like:
   `/subscriptions/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/resourceGroups/rg-ev-intelligence-dev/providers/Microsoft.KeyVault/vaults/kv-ev-intelligence-dev`

**Now create the Secret Scope:**

> The Secret Scope creation page is not in the normal Databricks left menu — it is only accessible via a special URL.

1. Copy your Databricks workspace URL from the browser — it looks like:
   `https://adb-1234567890123456.7.azuredatabricks.net`
2. Add `#secrets/createScope` at the end and open it:
   `https://adb-1234567890123456.7.azuredatabricks.net#secrets/createScope`
3. You will see a form. Fill in:
   - **Scope Name:** `kv-ev-scope`
   - **Manage Principal:** `All Users`
   - **DNS Name:** paste the **Vault URI** you copied above
   - **Resource ID:** paste the **Resource ID** you copied above
4. Click **Create**

You should see a success message. Now any notebook on this workspace can read Key Vault secrets using `dbutils.secrets.get(scope="kv-ev-scope", key="<secret-name>")`.

**Verify the scope was created (CLI):**
```bash
# Install Databricks CLI if you don't have it
pip install databricks-cli

# Or just verify from inside a Databricks notebook cell:
# display(dbutils.secrets.listScopes())
# You should see "kv-ev-scope" in the output
```

---

## Part 7 — Mount ADLS Gen2 in Databricks (10 min)

> **Cost: ₹0** — mounting is free. You pay only for the cluster time (already running from Part 6).

**Two approaches to connect Databricks to ADLS Gen2 — choose one:**

---

### Approach A — Service Principal OAuth (Recommended — use this)

**What it is:** Databricks presents the Service Principal's Client ID + Client Secret to Azure Entra ID. Azure validates the identity, checks that the SP has the correct RBAC role on the storage account, and issues a short-lived OAuth token. That token is used to access storage. The actual storage account key is never used or exposed.

**Why this is more secure:**
- The Service Principal can be given minimal permissions (only what it needs — e.g. read-only Bronze, read-write Silver)
- If the SP's secret is compromised, you rotate the `sp-client-secret` in Key Vault. The storage account itself is unaffected
- Access can be revoked instantly by removing the SP's RBAC role — no need to rotate the storage key
- Azure Entra ID logs every login by the SP, so you have a full audit trail of who accessed storage and when
- Follows the principle of least privilege

### 7.1 Create Notebook `00_mount_storage`
1. Databricks → **Workspace** → **+ New** → **Notebook**
2. Name: `00_mount_storage`
3. Language: Python
4. Attach to `dev-cluster`

### 7.2 Mount using Service Principal OAuth
```python
# All secrets come from Key Vault via the secret scope — no hardcoded values
SCOPE = "kv-ev-scope"

client_id     = dbutils.secrets.get(scope=SCOPE, key="sp-client-id")
client_secret = dbutils.secrets.get(scope=SCOPE, key="sp-client-secret")
tenant_id     = dbutils.secrets.get(scope=SCOPE, key="sp-tenant-id")
account_name  = dbutils.secrets.get(scope=SCOPE, key="adls-account-name")

# OAuth config — Databricks exchanges client_id + client_secret for a short-lived token
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type":
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint":
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

# Mount each container
for container in ["bronze", "silver", "gold", "source"]:
    mount_point = f"/mnt/{container}"
    if not any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source=f"abfss://{container}@{account_name}.dfs.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs,
        )
        print(f"Mounted  : {container}")
    else:
        print(f"Already mounted: {container}")

# Verify
display(dbutils.fs.ls("/mnt/bronze"))
```

Run the notebook — if you see no errors, all 4 containers are mounted.

---

### Approach B — Storage Account Access Key (Alternative — less secure, simpler for beginners)

> **Security Warning — read before using this approach.**
>
> The storage account access key is a **root-level, full-access key**. Anyone who has this key can read, write, or delete **everything** in the storage account across **all containers** — Bronze, Silver, Gold, Source. There is no way to scope it to specific containers or limit what they can do.
>
> **Specific risks vs Approach A:**
> - If the key leaks (in a git commit, a log file, a screenshot), an attacker has complete control over all your data
> - Rotating the key (the only way to revoke access) breaks every notebook and pipeline that uses it — you must update all references simultaneously
> - Azure does not log *who* used the key — it only logs that the key was used. No audit trail of which service or person accessed which file
> - The key never expires — it is valid indefinitely until manually rotated
> - It violates the principle of least privilege — a notebook that only reads Bronze data should not hold a key that can delete Silver data
>
> **When is it acceptable?**
> For a short-lived local dev test where you know the key will not be committed to git and the storage account holds no sensitive data. Never in any shared or production environment.

```python
# Approach B — Access Key (less secure)
SCOPE = "kv-ev-scope"

account_name = dbutils.secrets.get(scope=SCOPE, key="adls-account-name")
account_key  = dbutils.secrets.get(scope=SCOPE, key="adls-account-key")

# Note: the key is still read from Key Vault (not hardcoded) — that part is correct.
# The weakness is the key itself, not where it is stored.
spark.conf.set(
    f"fs.azure.account.key.{account_name}.dfs.core.windows.net",
    account_key,
)

# Mount containers
for container in ["bronze", "silver", "gold", "source"]:
    mount_point = f"/mnt/{container}"
    if not any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source=f"abfss://{container}@{account_name}.dfs.core.windows.net/",
            mount_point=mount_point,
            extra_configs={
                f"fs.azure.account.key.{account_name}.dfs.core.windows.net": account_key
            },
        )
        print(f"Mounted: {container}")
    else:
        print(f"Already mounted: {container}")
```

**If you use Approach B, you need one extra secret in Key Vault — the storage access key.**

**What is a storage access key?**
It is a long base64-encoded string (looks like `AbCdEf1234...==`) that gives full root-level access to your entire storage account. Every storage account has two of them (key1 and key2) so you can rotate one without downtime.

**Step 1 — Get the access key from the Portal:**
1. Go to [https://portal.azure.com](https://portal.azure.com)
2. Search **Storage accounts** → click `evdatalakedev`
3. In the left menu, click **Access keys** (under Security + networking)
4. Click **Show** next to **key1**
5. Copy the full **Key** value (a long string ending in `==`)

**Step 2 — Get the access key via CLI:**
```bash
az storage account keys list \
  --account-name evdatalakedev \
  --resource-group rg-ev-intelligence-dev \
  --query "[0].value" -o tsv
# Outputs the key1 value directly
```

**Step 3 — Store it in Key Vault:**

Via Portal:
1. Go to Key Vault → `kv-ev-intelligence-dev` → left menu **Secrets**
2. Click **+ Generate/Import**
3. Fill in:
   - **Name:** `adls-account-key`
   - **Value:** paste the key you copied in Step 1
4. Click **Create**

Via CLI:
```bash
KEY=$(az storage account keys list \
  --account-name evdatalakedev \
  --resource-group rg-ev-intelligence-dev \
  --query "[0].value" -o tsv)

az keyvault secret set \
  --vault-name kv-ev-intelligence-dev \
  --name "adls-account-key" \
  --value "$KEY"
```

> **Recommendation: Use Approach A (OAuth). Approach B is documented here so you understand what the access key is, where it comes from, and why it is avoided in production.**

---

### 7.3 Verify API Auth — Runtime Token Pattern
Create a new notebook `01_verify_api_auth` and run this to confirm the full auth flow works end to end:

```python
import requests

SCOPE = "kv-ev-scope"

# Pull credentials from Key Vault — no plaintext values here
api_base_url = dbutils.secrets.get(scope=SCOPE, key="voltgrid-api-base-url")
username     = dbutils.secrets.get(scope=SCOPE, key="voltgrid-username")
password     = dbutils.secrets.get(scope=SCOPE, key="voltgrid-password")

# Step 1: POST /api/auth/login/ → get a token at runtime
resp = requests.post(
    f"{api_base_url}/api/auth/login/",
    json={"username": username, "password": password},
    timeout=10,
)
resp.raise_for_status()
token = resp.json()["token"]

print(f"Token acquired: {token[:8]}...{token[-4:]}")   # partial — never print the full token

# Step 2: Use the token for all subsequent API calls
headers = {"Authorization": f"Token {token}"}

# Test: fetch first page of payments
r = requests.get(
    f"{api_base_url}/api/db/payments/?page=1&page_size=5",
    headers=headers,
    timeout=10,
)
r.raise_for_status()
data = r.json()
print(f"Payments total: {data['total']}, pages: {data['total_pages']}")
print("API auth working correctly.")
```

> **How this scales across all 18 API endpoints in ADF / Databricks pipelines:**
> 1. Pipeline or notebook calls `POST /api/auth/login/` once → gets a token
> 2. Token is stored in a pipeline variable (in-memory only — never written to disk)
> 3. All 18 API endpoints are called with `Authorization: Token <token>`
> 4. Token is discarded automatically when the pipeline/notebook run ends
> 5. Next run calls login again for a fresh token — no stale credential risk

---

## Part 8 — Architecture Diagram (Reference)

```
[Source Systems]
    |
    |-- VoltGrid API (REST / CDC via updated_at) ──────────┐
    |-- Blob Storage (CSV / PDF / XML / JSON uploads) ─────┤
    |-- Event Hub (IoT Streaming JSON) ────────────────────┤
                                                           |
                                              [ADF] ← free tier (pagination + watermark)
                                              [Databricks Auto Loader] ← blob ingestion
                                              [Databricks Streaming]   ← Event Hub
                                                           |
                                              [ADLS Gen2]           ← ~₹20/month
                                              /mnt/bronze   ← raw, append-only
                                              /mnt/silver   ← cleaned, MERGE upsert (Delta)
                                              /mnt/gold     ← aggregated, star schema (Delta)
                                                           |
                                              [Azure Databricks]    ← ~₹45/session
                                              Delta Lake tables
                                                           |
                                              [Power BI / Synapse Analytics]

Auth flows:
  Databricks → Azure Entra ID (SP OAuth) → ADLS Gen2       [Approach A — recommended]
  Databricks → Key Vault (secret scope)  → secrets at runtime
  Databricks → VoltGrid API (username/password → token)    → 18 endpoints
```

---

## Day 1 Cost Summary

| Resource Created Today | Cost |
|---|---|
| Resource Group | ₹0 |
| ADLS Gen2 storage + 4 containers | ~₹2/day (for ~10 GB) |
| Key Vault | ~₹0.30/day |
| Service Principal | ₹0 |
| Databricks workspace + cluster | ~₹40-45 per 2-hr session |
| **Day 1 total** | **~₹45-47** |

---

## End of Session — STOP THE CLUSTER

**Do this every single time before closing your laptop:**

1. Databricks → left menu **Compute**
2. Click your cluster `dev-cluster`
3. Click **Terminate**
4. Wait for status to show **Terminated**

If you forget, the cluster auto-terminates after 15 minutes — but do not rely on it.

---

## Day 1 Checklist

- [ ] Budget alert set at ₹1,500/month
- [ ] Resource Group `rg-ev-intelligence-dev` created in Central India
- [ ] Storage Account `evdatalakedev` created (Standard LRS, hierarchical namespace ON)
- [ ] Containers: `bronze`, `silver`, `gold`, `source` created
- [ ] Lifecycle rule set (move to Cool after 30 days, Archive after 90)
- [ ] Key Vault `kv-ev-intelligence-dev` created (Standard tier)
- [ ] Secrets added: `voltgrid-api-base-url`, `voltgrid-username`, `voltgrid-password`, `adls-account-name`
- [ ] Service Principal `sp-ev-intelligence-dev` created
- [ ] SP credentials stored in Key Vault (`sp-client-id`, `sp-client-secret`, `sp-tenant-id`)
- [ ] SP has **Storage Blob Data Contributor** role on `evdatalakedev`
- [ ] Databricks workspace `dbw-ev-intelligence-dev` created (Trial tier)
- [ ] Cluster `dev-cluster` created — Single Node, DS3_v2, Photon OFF, auto-terminate 15 min
- [ ] Key Vault secret scope `kv-ev-scope` created in Databricks
- [ ] Storage mounted using **Approach A (SP OAuth)** at `/mnt/bronze`, `/mnt/silver`, `/mnt/gold`, `/mnt/source`
- [ ] API auth verified — login endpoint returns token, payments endpoint returns data
- [ ] **Cluster terminated at end of session**

---

## Common Errors on Day 1

| Error | Fix |
|---|---|
| `az login` fails | Try `az login --use-device-code` |
| Storage account name taken | Add your initials: `evdatalakedevhs` |
| Key Vault name taken | Add random suffix: `kv-ev-dev-01` |
| Mount fails with 403 | SP does not have Storage Blob Data Contributor — re-check IAM |
| Mount fails with "invalid client secret" | Check `sp-client-secret` in Key Vault is the exact value output when you created the SP |
| Secret scope creation fails | Your Databricks workspace identity needs Key Vault access — add it in Key Vault Access Policies |
| Cluster won't start | Check region quota — if `Standard_DS3_v2` is unavailable, try `Standard_D3_v2` |
| API login returns 401 | Username or password in Key Vault does not match what is in the Django database |
