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

#### Windows — Azure CLI

`winget` requires **Windows 10 1709+** with **App Installer** from the Microsoft Store. If `winget` is not recognized, use one of these alternatives instead:

**Option 1 — MSI Installer (simplest, recommended)**
1. Download the installer: [https://aka.ms/installazurecliwindows](https://aka.ms/installazurecliwindows)
2. Run the downloaded `.msi` file and follow the prompts
3. Close and reopen your terminal after installation

**Option 2 — PowerShell (no browser needed)**
```powershell
# Run PowerShell as Administrator
Invoke-WebRequest -Uri https://aka.ms/installazurecliwindows -OutFile AzureCLI.msi
Start-Process msiexec.exe -ArgumentList '/I AzureCLI.msi /quiet' -Wait
Remove-Item AzureCLI.msi
```

**Option 3 — Fix winget first (then install normally)**
1. Open Microsoft Store → search **App Installer** → click **Update**
2. Close and reopen PowerShell
3. Run: `winget install Microsoft.AzureCLI`

#### Mac
```bash
brew install azure-cli
```

#### Verify and login (all platforms)
```bash
# Confirm install worked
az --version

# Login — opens a browser window to sign in with your Azure account
az login
az account show   # should show your subscription
```

### 1.4 Register Resource Providers (do this before creating any resources)

> **Why this matters:** Azure subscriptions do not have all resource providers enabled by default. If you skip this step you will hit errors like:
> `MissingSubscriptionRegistration: The subscription is not registered to use namespace 'Microsoft.KeyVault'`
> Register all providers now — it takes 1–2 minutes and only needs to be done once per subscription.

#### Option A — Via Azure Portal (UI)

1. Go to [https://portal.azure.com](https://portal.azure.com)
2. In the top search bar, search **Subscriptions** and click it
3. Click your subscription name (`DataEngineeringDaily`)
4. In the left menu, scroll down and click **Resource providers** (under Settings)
5. You will see a long list of providers with their registration state
6. For each provider below, type its name in the **Filter by name** box, click it, then click **Register** at the top:

| Provider to register | Filter search term |
|---|---|
| `Microsoft.KeyVault` | KeyVault |
| `Microsoft.Storage` | Storage |
| `Microsoft.Databricks` | Databricks |
| `Microsoft.EventHub` | EventHub |
| `Microsoft.DataFactory` | DataFactory |
| `Microsoft.ManagedIdentity` | ManagedIdentity |

7. After clicking Register for each, refresh the page — status changes from `NotRegistered` → `Registering` → `Registered`
8. Wait until all 6 show **Registered** before moving to Part 2

> **Tip:** You can register all 6 one after another without waiting — they all register in parallel. Then wait once at the end for all to finish.

#### Option B — Via CLI (CMD / PowerShell)

**Register all 6:**
```cmd
az provider register --namespace Microsoft.KeyVault
az provider register --namespace Microsoft.Storage
az provider register --namespace Microsoft.Databricks
az provider register --namespace Microsoft.EventHub
az provider register --namespace Microsoft.DataFactory
az provider register --namespace Microsoft.ManagedIdentity
```

**Wait ~1 minute, then verify all show `Registered`:**
```cmd
az provider show --namespace Microsoft.KeyVault --query registrationState -o tsv
az provider show --namespace Microsoft.Storage --query registrationState -o tsv
az provider show --namespace Microsoft.Databricks --query registrationState -o tsv
az provider show --namespace Microsoft.EventHub --query registrationState -o tsv
az provider show --namespace Microsoft.DataFactory --query registrationState -o tsv
az provider show --namespace Microsoft.ManagedIdentity --query registrationState -o tsv
```

All 6 should output `Registered`. If any still shows `Registering`, wait 30 more seconds and re-run that check. Do not proceed until all say `Registered`.

> **Note:** Registration is permanent — you never need to repeat this for the same subscription.

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

> **CMD / PowerShell users:** The `\` line continuation below is bash syntax and will break in CMD/PowerShell. Use the single-line version to copy-paste directly.

**Single line (CMD / PowerShell — copy-paste this):**
```cmd
az group create --name rg-ev-intelligence-dev --location centralindia
```

**Multi-line (bash / Git Bash only):**
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

> **CMD / PowerShell users:** Use the single-line versions below. The `\` and `for` loop syntax is bash only.

**Single line — create storage account (CMD / PowerShell):**
```cmd
az storage account create --name evdatalakedev --resource-group rg-ev-intelligence-dev --location centralindia --sku Standard_LRS --kind StorageV2 --enable-hierarchical-namespace true --access-tier Cool
```

**Single line — create each container (CMD / PowerShell — run 4 times):**
```cmd
az storage container create --name bronze --account-name evdatalakedev --auth-mode login
az storage container create --name silver --account-name evdatalakedev --auth-mode login
az storage container create --name gold --account-name evdatalakedev --auth-mode login
az storage container create --name source --account-name evdatalakedev --auth-mode login
```

**Multi-line (bash / Git Bash only):**
```bash
az storage account create \
  --name evdatalakedev \
  --resource-group rg-ev-intelligence-dev \
  --location centralindia \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --access-tier Cool

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
   - Permission model: **Azure role-based access control (RBAC)** ← use this
4. Click **Review + Create** → **Create**

### 4.2 Assign Yourself `Key Vault Secrets Officer` Role (required before adding secrets)

> **Why this is needed:** When Key Vault uses the RBAC permission model, even the account that created the vault cannot read or write secrets until it is explicitly assigned a role. Without this step you will get:
> `Forbidden: Caller is not authorized to perform action — ForbiddenByRbac`

**Via Portal:**
1. Portal → **Key vaults** → `kv-ev-intelligence-dev`
2. Left menu → **Access Control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. **Role** tab: search `Key Vault Secrets Officer` → select → click **Next**
5. **Members** tab:
   - **Assign access to:** `User, group, or service principal`
   - Click **+ Select members** → search your Azure login email → select → **Select**
6. Click **Review + assign** → **Review + assign**
7. Wait **1–2 minutes** for the role to propagate before running any `az keyvault secret set` commands

**Via CLI:**
```cmd
az ad signed-in-user show --query id -o tsv
```
Copy the output (your object ID), then:
```cmd
az keyvault show --name kv-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query id -o tsv
```
Copy the output (Key Vault resource ID), then:
```cmd
az role assignment create --assignee-object-id <your-object-id> --assignee-principal-type User --role "Key Vault Secrets Officer" --scope <keyvault-resource-id>
```
Wait 1–2 minutes, then proceed to adding secrets.

**Role reference — who gets what:**

| Identity | Role | Why |
|---|---|---|
| Your account | `Key Vault Secrets Officer` | You need to read + write secrets from CLI/Portal |
| Databricks workspace identity | `Key Vault Secrets User` | Secret scope reads Key Vault secrets — without this you get `PERMISSION_DENIED: Invalid permissions on KeyVault 403` in notebooks |
| Service Principal | `Key Vault Secrets User` | Databricks reads secrets at runtime (read-only) |
| Managed Identity (ADF) | `Key Vault Secrets User` | ADF reads secrets at runtime (read-only) |

### 4.3 Add Your First Secrets
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

### 4.4 Via CLI

> **CMD / PowerShell users:** Use the single-line versions below. The `\` and `$KV` variable syntax is bash only — in CMD use the full vault name directly.

**Single line (CMD / PowerShell — copy-paste each line):**
```cmd
az keyvault create --name kv-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --location centralindia --sku standard
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-api-base-url" --value "https://ev-project-navy-mu.vercel.app"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-username" --value "voltgrid_demo"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-password" --value "EVcharge@AU2025"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "adls-account-name" --value "evdatalakedev"
```

**Multi-line (bash / Git Bash only):**
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

**Single line (CMD / PowerShell — copy-paste this):**
```cmd
az ad sp create-for-rbac --name sp-ev-intelligence-dev --role Contributor --scopes /subscriptions/<YOUR_SUBSCRIPTION_ID>/resourceGroups/rg-ev-intelligence-dev
```

**Multi-line (bash / Git Bash only):**
```bash
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

**Single line (CMD / PowerShell):**
```cmd
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-client-id" --value "<appId from output>"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-client-secret" --value "<password from output>"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-tenant-id" --value "<tenant from output>"
```

**Multi-line (bash / Git Bash only):**
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

> **CMD / PowerShell users:** Variables like `$()` and `$VAR` are bash syntax. Use the step-by-step single-line version below.

**Step-by-step (CMD / PowerShell — run each line separately):**

**Step 1 — get your SP's appId** (the Application/Client ID from Part 5):
```cmd
az ad sp list --display-name sp-ev-intelligence-dev --query "[0].appId" -o tsv
```
Copy the output — this is your `APP_ID` (looks like `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)

**Step 2 — get the SP's internal Object ID** (different from appId — Azure uses this for role assignments):
```cmd
az ad sp show --id <APP_ID from Step 1> --query id -o tsv
```
Copy the output — this is your `SP_OID`

**Step 3 — get the Storage Account resource ID:**
```cmd
az storage account show --name evdatalakedev --resource-group rg-ev-intelligence-dev --query id -o tsv
```
Copy the output — this is your `STORAGE_ID` (looks like `/subscriptions/81dd57e1-.../providers/Microsoft.Storage/storageAccounts/evdatalakedev`)

**Step 4 — assign the role:**
```cmd
az role assignment create --assignee-object-id <SP_OID from Step 2> --assignee-principal-type ServicePrincipal --role "Storage Blob Data Contributor" --scope <STORAGE_ID from Step 3>
```

**Step 5 — verify:**
```cmd
az role assignment list --scope <STORAGE_ID from Step 3> --query "[].{Role:roleDefinitionName, Principal:principalName}" -o table
```

**Multi-line (bash / Git Bash only):**
```bash
STORAGE_ID=$(az storage account show \
  --name evdatalakedev \
  --resource-group rg-ev-intelligence-dev \
  --query id -o tsv)

SP_OID=$(az ad sp show --id <appId from earlier> --query id -o tsv)

az role assignment create \
  --assignee-object-id $SP_OID \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ID

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
| **Access mode** | **Dedicated (formerly: Single user)** | Required — `dbutils.fs.mount()` is blocked in Standard/Shared mode |
| **Databricks runtime** | `15.4 LTS (Spark 3.5, Scala 2.12)` | Stable, no extra cost |
| **Use Photon Acceleration** | **OFF** | Photon adds extra DBU charges |
| **Node type** | `Standard_DS3_v2` | 4 vCPU, 14 GB RAM — minimum viable for Spark |
| **Auto termination** | **15 minutes** | MOST IMPORTANT — kills cluster when idle |

> **Access mode — why Dedicated is required:**
>
> | Access mode | What it means | `mount()` works? |
> |---|---|---|
> | Standard (formerly: Shared) | Multiple users share the cluster — Databricks restricts `mount()` to protect other users | ❌ No — gives `Method not whitelisted` error |
> | **Dedicated (formerly: Single user)** | Only your account runs on this cluster — full permissions | ✅ Yes |
>
> If you accidentally created the cluster with Standard mode and get `Method public dbutils.mount() is not whitelisted` — terminate the cluster → Edit → change Access mode to **Dedicated** → Confirm → restart.

3. Click **Create compute** (takes ~5 minutes to start)

> **Cost breakdown per session:**
> - VM: Standard_DS3_v2 ≈ ₹18/hr × 2 hr = ₹36
> - DBU: 0.75 DBU/hr × ₹3.0 × 2 hr = ₹4.5
> - **Total per 2-hour session: ~₹40-45**

### 6.4 Grant Databricks Workspace Access to Key Vault (required before creating secret scope)

> **If you skip this step**, your notebooks will fail with:
> `PERMISSION_DENIED: Invalid permissions on the specified KeyVault — Status code 403`
> even though your own account has access. The Databricks workspace uses its **own managed identity** to read Key Vault — separate from your user account.

> **Important:** Azure Databricks workspace does not expose a managed identity in the Portal UI. Instead, Databricks accesses Key Vault through a global **AzureDatabricks** enterprise application. You assign the role to that application.

**Via Portal:**
1. Portal → **Key vaults** → `kv-ev-intelligence-dev` → left menu **Access Control (IAM)**
2. Click **+ Add** → **Add role assignment**
3. Role: `Key Vault Secrets User` → **Next**
4. Members: **+ Select members** → search **`AzureDatabricks`** → select it → **Review + assign**
5. Wait **2 minutes** before proceeding to create the secret scope

**Via CLI:**

**Step 1 — get the AzureDatabricks SP object ID:**
```cmd
az ad sp list --display-name "AzureDatabricks" --query "[0].id" -o tsv
```
Copy the output, then:

**Step 2 — get the Key Vault resource ID:**
```cmd
az keyvault show --name kv-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query id -o tsv
```
Copy the output, then:

**Step 3 — assign the role:**
```cmd
az role assignment create --assignee-object-id <output from Step 1> --assignee-principal-type ServicePrincipal --role "Key Vault Secrets User" --scope <output from Step 2>
```
Wait 2 minutes, then re-run the notebook.

---

### 6.5 Add Key Vault-backed Secret Scope in Databricks

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

## Part 7.4 — Read Source Blob Data via SAS Token (External Storage)

> **Cost: ₹0** — reading via SAS token is free on your end. The storage owner pays for egress.

**What is this?**
The project uses a shared external Azure Blob Storage (`dataenggdailystorage`) that contains pre-loaded source data — IoT session CSVs, maintenance files, etc. Access is granted via a **SAS (Shared Access Signature) token** — a time-limited read-only URL. You do not need an Azure account or credentials for this storage. You just use the SAS token provided.

**What credentials you need for this:**

| Field | Value |
|---|---|
| Storage Account | `dataenggdailystorage` |
| Container | `source` |
| SAS Token | Provided during the session — see note below |

> **SAS Token will be shared during the session.**
> The token looks like: `se=2027-07-30&sp=rl&spr=https&sv=2026-04-06&sr=c&sig=<signature>`
> Paste it exactly as given — do not add quotes or modify it.
> It has **read + list** permissions only — you cannot write or delete anything in this storage.

**Folder structure of the source container:**
```
source/
  realtime/
    charging_sessions/    2026/06/01/06/  sessions_20260601_0600.csv
    ...
```

### 7.4.1 Create notebook `02_read_source_blob`

1. Databricks → **Workspace** → **+ New** → **Notebook**
2. Name: `02_read_source_blob`
3. Language: Python → Attach to `dev-cluster`

### 7.4.2 Full test script

```python
# ── Source Blob Read via SAS Token ────────────────────────────────────────────
# Storage account : dataenggdailystorage  (external — not your account)
# Container       : source
# Auth            : SAS token (read + list only — provided during session)
# Use wasbs:// protocol — NOT abfss:// (abfss requires OAuth, wasbs works with SAS)

STORAGE_ACCOUNT = "dataenggdailystorage"
CONTAINER       = "source"

# Paste the SAS token provided during the session here (without quotes around it)
# It looks like: se=2027-07-30&sp=rl&spr=https&sv=2026-04-06&sr=c&sig=xxxxx
SAS_TOKEN = "<SAS_TOKEN_PROVIDED_DURING_SESSION>"

# ── Step 1: Configure Spark to use SAS token for this storage account ─────────
spark.conf.set(
    f"fs.azure.sas.{CONTAINER}.{STORAGE_ACCOUNT}.blob.core.windows.net",
    SAS_TOKEN
)
print("Step 1: Spark SAS config set — OK")

# ── Step 2: List top-level folders in the source container ────────────────────
print("\nStep 2: Listing top-level folders in source/")
try:
    items = dbutils.fs.ls(f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/")
    for item in items:
        print(f"  {item.name:<50} {item.size:>10} bytes")
    print(f"  Total: {len(items)} items found")
except Exception as e:
    print(f"  ERROR: {e}")
    print("  → Check SAS token is correct and has 'List' permission (sp=rl)")

# ── Step 3: List charging_sessions folder ─────────────────────────────────────
print("\nStep 3: Listing realtime/charging_sessions/")
try:
    sessions_path = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/realtime/charging_sessions/"
    for item in dbutils.fs.ls(sessions_path):
        print(f"  {item.name}")
except Exception as e:
    print(f"  ERROR: {e}")
    print("  → Folder path may differ — check Step 2 output for correct folder names")

# ── Step 4: Read a specific CSV file ──────────────────────────────────────────
print("\nStep 4: Reading a specific CSV file")
csv_path = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/realtime/charging_sessions/2026/06/01/06/sessions_20260601_0600.csv"

try:
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(csv_path))

    row_count = df.count()
    print(f"  File path  : {csv_path}")
    print(f"  Row count  : {row_count:,}")
    print(f"  Columns    : {df.columns}")
    df.printSchema()
    display(df.limit(10))
except Exception as e:
    print(f"  ERROR: {e}")
    print("  → File path may differ — adjust the date/hour in the path based on Step 3 output")

# ── Step 5: Read entire charging_sessions folder (all CSVs at once) ───────────
print("\nStep 5: Reading all CSVs under charging_sessions/ folder")
try:
    folder_path = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/realtime/charging_sessions/"
    df_all = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(folder_path))

    print(f"  Total rows across all files : {df_all.count():,}")
    display(df_all.limit(10))
    print("\nSAS token read test PASSED — source blob is accessible.")
except Exception as e:
    print(f"  ERROR: {e}")

# ── Step 6: Quick data quality check ──────────────────────────────────────────
print("\nStep 6: Basic data quality check")
try:
    import pyspark.sql.functions as F
    print(f"  Total rows     : {df_all.count():,}")
    print(f"  Null check per column:")
    for col in df_all.columns:
        null_count = df_all.filter(F.col(col).isNull()).count()
        print(f"    {col:<35} nulls: {null_count:,}")
except Exception as e:
    print(f"  Skipped: {e}")
```

### 7.4.3 Expected output if SAS is working

```
Step 1: Spark SAS config set — OK

Step 2: Listing top-level folders in source/
  realtime/                                            0 bytes
  Total: 1 items found

Step 3: Listing realtime/charging_sessions/
  2026/

Step 4: Reading a specific CSV file
  File path  : wasbs://source@dataenggdailystorage...
  Row count  : <number>
  Columns    : [session_id, plug_in_ts, charge_end_ts, ...]

Step 5: Reading all CSVs under charging_sessions/ folder
  Total rows across all files : <number>

SAS token read test PASSED — source blob is accessible.
```

### 7.4.4 Common errors with SAS token

| Error | Cause | Fix |
|---|---|---|
| `403 Forbidden` on ls or read | SAS token is wrong, expired, or missing List permission | Confirm token has `sp=rl` (read + list). Paste token exactly — no extra spaces or quotes |
| `No such file or directory` on Step 4 | CSV path does not match actual folder structure | Run Step 2 and Step 3 first to confirm exact folder/file names, then adjust path |
| `java.io.IOException: No value for...` | SAS config key is wrong | Confirm CONTAINER and STORAGE_ACCOUNT variables match exactly — case-sensitive |
| `InvalidAuthenticationInfo` | Using `abfss://` instead of `wasbs://` | SAS tokens require `wasbs://` protocol. Replace `abfss` with `wasbs` in all paths |
| Output shows `[REDACTED]` for SAS token | You printed the token — Databricks masked it | This is correct and expected — the token is still being used, just not shown |
| Step 5 reads 0 rows | CSV files have no header or different delimiter | Add `.option("header", "false")` or `.option("delimiter", ";")` and check file format |

> **The above section is now replaced — see the updated Part 7.4 below.**

---

## Part 7.4 — Read Source Blob Data via SAS Token (External Storage) [Updated]

> **Cost: ₹0** — reading via SAS token is free on your end.

**What is this?**
The project uses a shared external Azure Blob Storage (`dataenggdailystorage`) containing pre-loaded source data — IoT session CSVs, maintenance files, etc. Access is granted via a SAS token (read + list only). All 3 credentials are stored in **your Key Vault** — nothing is hardcoded in notebooks.

**Folder structure of the source container:**
```
source/
  realtime/
    charging_sessions/    2026/06/01/06/  sessions_20260601_0600.csv
```

### 7.4.1 Add 3 secrets to your Key Vault

> **SAS token will be shared during the session.** Add it to Key Vault as soon as you receive it — the notebook reads it automatically.

**Via Portal:**
1. Portal → **Key vaults** → `kv-ev-intelligence-dev` → **Secrets** → **+ Generate/Import**
2. Add these 3 secrets one by one:

| Secret Name | Value | Notes |
|---|---|---|
| `source-storage-account` | `dataenggdailystorage` | External storage account name |
| `source-container` | `source` | Container name |
| `source-sas-token` | `se=2027-07-30&sp=rl&...` | Provided during session — paste exactly, no quotes |

**Via CLI:**
```cmd
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "source-storage-account" --value "dataenggdailystorage"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "source-container" --value "source"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "source-sas-token" --value "<paste SAS token here>"
```

> **SAS token format:** `se=2027-07-30&sp=rl&spr=https&sv=2026-04-06&sr=c&sig=xxxxx`
> Paste exactly as given. No leading `?`, no quotes. It has **read + list** (`sp=rl`) only.

### 7.4.2 Import and run the notebook

Import `02_read_source_blob.ipynb` from the `notebooks/` folder — see the notebooks README for import steps.

The notebook reads all 3 credentials from Key Vault automatically in Cell 1:

```python
SCOPE           = "kv-ev-scope"
STORAGE_ACCOUNT = dbutils.secrets.get(scope=SCOPE, key="source-storage-account")
CONTAINER       = dbutils.secrets.get(scope=SCOPE, key="source-container")
SAS_TOKEN       = dbutils.secrets.get(scope=SCOPE, key="source-sas-token")

spark.conf.set(
    f"fs.azure.sas.{CONTAINER}.{STORAGE_ACCOUNT}.blob.core.windows.net",
    SAS_TOKEN
)
```

**What each cell does:**

| Cell | What it does |
|---|---|
| Cell 1 | Loads all 3 secrets from Key Vault, sets Spark SAS config |
| Cell 2 | Lists top-level folders in the source container |
| Cell 3 | Drills into `realtime/charging_sessions/` and shows folder structure |
| Cell 4 | Reads one specific CSV file — prints schema + sample rows |
| Cell 5 | Reads ALL CSVs using glob `/*/*/*/*/*.csv` — handles nested year/month/day/hour structure |
| Cell 6 | Null check per column |
| Cell 7 | Final summary |

> **Why glob in Cell 5 and not just the folder path?**
> Structure is `charging_sessions/2026/06/01/06/file.csv` — 4 levels of subfolders before the CSV.
> Reading `charging_sessions/` directly gives `UNABLE_TO_INFER_SCHEMA` because Spark only sees subfolders at the top, not files.
> The pattern `/*/*/*/*/*.csv` recurses through all 4 levels and finds the actual CSV files.

### 7.4.3 Expected output

```
Cell 1 — Storage account : dataenggdailystorage
          Container       : source
          SAS token       : [REDACTED]
          Spark SAS config set — OK

Cell 2 — [DIR] realtime/    0 bytes

Cell 4 — Row count    : <number>
          Columns      : [session_id, plug_in_ts, charge_end_ts, ...]

Cell 5 — Total rows across all files : <number>

Cell 7 — SAS token read test PASSED — source blob is accessible.
```

### 7.4.4 Common errors

| Error | Cause | Fix |
|---|---|---|
| `Secret not found: source-sas-token` | Secret not added to Key Vault yet | Add all 3 secrets via Portal or CLI above, wait 1 min, re-run Cell 1 |
| `403 Forbidden` on ls or read | SAS token wrong or missing List permission | Confirm `sp=rl` in token. Re-check the Key Vault secret value — no extra spaces |
| `UNABLE_TO_INFER_SCHEMA` | Reading a folder that has only subfolders at top level | Cell 5 already uses `/*/*/*/*/*.csv` glob — if still failing, run Cell 3 to check depth and adjust number of `/*` |
| `No such file` on Cell 4 | Date/hour in path doesn't match actual file | Run Cell 3 to see exact folder names, then adjust Cell 4 path |
| `InvalidAuthenticationInfo` | Using `abfss://` instead of `wasbs://` | SAS tokens need `wasbs://` — notebook already uses this correctly |
| `[REDACTED]` for SAS token in output | Databricks masked the secret value | Correct and expected — token is still working, just hidden |

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
- [ ] All 6 resource providers registered and showing `Registered` (KeyVault, Storage, Databricks, EventHub, DataFactory, ManagedIdentity)
- [ ] Resource Group `rg-ev-intelligence-dev` created in Central India
- [ ] Storage Account `evdatalakedev` created (Standard LRS, hierarchical namespace ON)
- [ ] Containers: `bronze`, `silver`, `gold`, `source` created
- [ ] Lifecycle rule set (move to Cool after 30 days, Archive after 90)
- [ ] Key Vault `kv-ev-intelligence-dev` created (Standard tier, RBAC permission model)
- [ ] Your account assigned `Key Vault Secrets Officer` role on the Key Vault
- [ ] Secrets added: `voltgrid-api-base-url`, `voltgrid-username`, `voltgrid-password`, `adls-account-name`
- [ ] Service Principal `sp-ev-intelligence-dev` created
- [ ] SP credentials stored in Key Vault (`sp-client-id`, `sp-client-secret`, `sp-tenant-id`)
- [ ] SP has **Storage Blob Data Contributor** role on `evdatalakedev`
- [ ] Databricks workspace `dbw-ev-intelligence-dev` created (Trial tier)
- [ ] Cluster `dev-cluster` created — Single Node, DS3_v2, Photon OFF, auto-terminate 15 min
- [ ] Databricks workspace managed identity assigned `Key Vault Secrets User` role on Key Vault
- [ ] Key Vault secret scope `kv-ev-scope` created in Databricks
- [ ] Storage mounted using **Approach A (SP OAuth)** at `/mnt/bronze`, `/mnt/silver`, `/mnt/gold`, `/mnt/source`
- [ ] API auth verified — login endpoint returns token, payments endpoint returns data
- [ ] SAS token received for `dataenggdailystorage` / `source` container
- [ ] 3 secrets added to Key Vault: `source-storage-account`, `source-container`, `source-sas-token`
- [ ] Notebook `02_read_source_blob.ipynb` ran — Cell 1 loads secrets from Key Vault, Cell 4 reads CSV, Cell 5 reads all CSVs via glob pattern
- [ ] **Cluster terminated at end of session**

---

## Common Errors on Day 1

| Error | Fix |
|---|---|
| `az login` fails | Try `az login --use-device-code` |
| `MissingSubscriptionRegistration` on any resource | Run `az provider register --namespace <e.g. Microsoft.KeyVault>` then wait 1–2 min and retry |
| `Forbidden: ForbiddenByRbac` on `az keyvault secret set` | Your account needs `Key Vault Secrets Officer` role — assign it via IAM on the Key Vault, wait 1–2 min, then retry |
| `Conflict: ObjectIsDeletedButRecoverable` on `az keyvault secret set` | Secret was previously deleted but is still in soft-delete state. Recover it first: `az keyvault secret recover --vault-name kv-ev-intelligence-dev --name "<secret-name>"` then retry the set command. Or purge it: `az keyvault secret purge --vault-name kv-ev-intelligence-dev --name "<secret-name>"` then set fresh |
| Storage account name taken | Add your initials: `evdatalakedevhs` |
| Key Vault name taken | Add random suffix: `kv-ev-dev-01` |
| Mount fails with 403 | SP does not have Storage Blob Data Contributor — re-check IAM |
| Mount fails with "invalid client secret" | Check `sp-client-secret` in Key Vault is the exact value output when you created the SP |
| Secret scope creation fails or `PERMISSION_DENIED: Invalid permissions on KeyVault 403` in notebook | The `AzureDatabricks` enterprise app needs `Key Vault Secrets User` role — Key Vault → IAM → Add role assignment → role: `Key Vault Secrets User` → member: search `AzureDatabricks` → assign. Wait 2 min, retry. Note: there is no Identity page on the Databricks workspace resource — use the `AzureDatabricks` global SP instead |
| Cluster won't start | Check region quota — if `Standard_DS3_v2` is unavailable, try `Standard_D3_v2` |
| `Method dbutils.mount() is not whitelisted` | Cluster Access mode is **Standard (Shared)** — terminate cluster → Edit → change Access mode to **Dedicated (formerly: Single user)** → Confirm → restart |
| `Method dbutils.mounts() is not whitelisted` | Same fix as above — Access mode must be Dedicated, not Standard/Shared |
| Notebook shows Serverless in top-right compute selector | Click the compute selector → switch from Serverless to `dev-cluster` — Serverless does not support `mount()` |
| API login returns 401 | Username or password in Key Vault does not match what is in the Django database |
