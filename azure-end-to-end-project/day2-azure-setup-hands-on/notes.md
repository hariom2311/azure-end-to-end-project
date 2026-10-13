# Day 2 — Azure Setup + ADF Bronze Ingestion (End-to-End)

**Duration:** 3 hours | **Goal:** Provision the full Azure foundation and run the first real data movement — VoltGrid API → ADF pipeline → Bronze layer in ADLS Gen2.

> **Modern standard only:** All ADLS Gen2 access uses **Azure RBAC + Managed Identity / Service Principal OAuth**. No storage access keys. No SAS tokens for your own storage.

---

## What You Will Have at the End of Day 2

| # | Item |
|---|---|
| 1 | Resource Group `rg-ev-intelligence-dev` |
| 2 | ADLS Gen2 with `bronze/` and `silver/` containers (plus `gold/`, `source/`) |
| 3 | Key Vault with RBAC permission model + secrets for API credentials |
| 4 | Your user account has `Key Vault Secrets Officer` role — can read/write secrets |
| 5 | ADF instance with System-assigned Managed Identity |
| 6 | ADF MI has `Key Vault Secrets User` on Key Vault |
| 7 | ADF MI has `Storage Blob Data Contributor` on ADLS (`bronze/` write) |
| 8 | Pipeline `pl_bronze_api_payments` runs and lands JSON in `bronze/api/payments/raw/` |

---

## Architecture: What We Are Building Today

```
[VoltGrid API]
 POST /api/auth/login/  →  token (in ADF pipeline memory)
 GET  /api/db/payments/ →  100 payment records (JSON)
        │
  [Azure Data Factory]
   act_get_username  ─┐
   act_get_password  ─┤→ act_api_login → act_set_token → act_copy_payments
                      │         (reads from Key Vault via MI)               │
                   [Key Vault]                               [ADLS Gen2 — bronze]
                kv-ev-intelligence-dev              bronze/api/payments/raw/payments.json
```

**Access control chain (modern standard):**
```
ADF Managed Identity
  → Key Vault Secrets User role      → reads voltgrid-username, voltgrid-password
  → Storage Blob Data Contributor    → writes to bronze/ container
```

No access keys. No SAS tokens. No credentials in pipeline code.

---

## Part 1 — Register Resource Providers

> Skip this → hit `MissingSubscriptionRegistration` errors later.

**Via Portal:**
Portal → **Subscriptions** → your subscription → **Resource providers** → search and Register each:

| Provider | Status needed |
|---|---|
| `Microsoft.KeyVault` | Registered |
| `Microsoft.Storage` | Registered |
| `Microsoft.DataFactory` | Registered |
| `Microsoft.Databricks` | Registered |

**Via CLI:**
```bash
az provider register --namespace Microsoft.KeyVault
az provider register --namespace Microsoft.Storage
az provider register --namespace Microsoft.DataFactory
az provider register --namespace Microsoft.Databricks
```

Verify (wait ~1 min):
```bash
az provider show --namespace Microsoft.KeyVault --query registrationState -o tsv
az provider show --namespace Microsoft.Storage --query registrationState -o tsv
az provider show --namespace Microsoft.DataFactory --query registrationState -o tsv
```

---

## Part 2 — Create Resource Group

> Cost: ₹0 — Resource Groups are free.

**Via Portal:**
1. Portal → **Resource groups** → **+ Create**
2. Subscription: yours | Name: `rg-ev-intelligence-dev` | Region: `Central India`
3. **Review + Create** → **Create**

**Via CLI:**
```bash
az group create --name rg-ev-intelligence-dev --location centralindia
```

---

## Part 3 — Create ADLS Gen2 Storage Account

> Cost: ~₹20–30/month for ~10–15 GB across all layers.

### 3.1 Create the Storage Account

**Via Portal:**
1. Portal → **Storage accounts** → **+ Create**
2. Fill in:
   - Resource group: `rg-ev-intelligence-dev`
   - Storage account name: `evdatalakedev` *(globally unique, lowercase, no hyphens)*
   - Region: `Central India`
   - Performance: `Standard`
   - Redundancy: `LRS`
3. **Advanced** tab → **Enable hierarchical namespace: ON** ← this makes it ADLS Gen2 (required for `abfss://` protocol and directory-level access control)
4. **Review + Create** → **Create**

> **Why hierarchical namespace?** Flat blob storage has no real directories — just key prefixes. Hierarchical namespace gives true directories with atomic renames, ACL inheritance, and `abfss://` protocol support for Spark. Required for Medallion Architecture.

**Via CLI:**
```bash
az storage account create \
  --name evdatalakedev \
  --resource-group rg-ev-intelligence-dev \
  --location centralindia \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --access-tier Cool
```

### 3.2 Disable Storage Account Key Access (Modern Standard)

> This enforces RBAC-only access. No one can use the account key to bypass IAM.

**Via Portal:**
Storage account → **Configuration** → **Allow storage account key access: Disabled** → **Save**

**Via CLI:**
```bash
az storage account update \
  --name evdatalakedev \
  --resource-group rg-ev-intelligence-dev \
  --allow-shared-key-access false
```

### 3.3 Create 4 Containers

Storage account → **Containers** → **+ Container** — create these one by one:

| Container | Purpose | Who writes |
|---|---|---|
| `bronze` | Raw ingested data — append-only, never modified | ADF (API), Databricks (streaming) |
| `silver` | Cleaned, typed, deduplicated Delta tables | Databricks |
| `gold` | Aggregated, analytics-ready Delta tables | Databricks |
| `source` | Uploaded CSVs, PDFs, XMLs from external teams | Manual upload / ADF |

Each container: Public access level = **Private**

**Via CLI:**
```bash
az storage container create --name bronze --account-name evdatalakedev --auth-mode login
az storage container create --name silver --account-name evdatalakedev --auth-mode login
az storage container create --name gold   --account-name evdatalakedev --auth-mode login
az storage container create --name source --account-name evdatalakedev --auth-mode login
```

> `--auth-mode login` uses your Azure AD credentials to create the container — does not require the account key, which we have disabled.

### 3.4 Verify Containers

```bash
az storage container list --account-name evdatalakedev --auth-mode login --query "[].name" -o tsv
```

Expected:
```
bronze
gold
silver
source
```

---

## Part 4 — Create Azure Key Vault

> Cost: ~₹5 total for the entire project.

### 4.1 Create Key Vault with RBAC Permission Model

**Via Portal:**
1. Portal → **Key vaults** → **+ Create**
2. Fill in:
   - Resource group: `rg-ev-intelligence-dev`
   - Name: `kv-ev-intelligence-dev`
   - Region: `Central India`
   - Pricing tier: `Standard`
3. **Access configuration** tab → Permission model: **Azure role-based access control (RBAC)** ← critical, do not choose Vault access policy
4. **Review + Create** → **Create**

> **Why RBAC model?** Vault access policy is the legacy model — it has a separate permission system outside of Azure IAM. RBAC model is the modern standard: Key Vault permissions are managed in the same IAM panel as all other Azure resources. Unified audit logs, conditional access, and PIM all work only with RBAC model.

**Via CLI:**
```bash
az keyvault create \
  --name kv-ev-intelligence-dev \
  --resource-group rg-ev-intelligence-dev \
  --location centralindia \
  --sku standard \
  --enable-rbac-authorization true
```

### 4.2 Assign Yourself Key Vault Secrets Officer

> Without this: `Forbidden: Caller is not authorized to perform action — ForbiddenByRbac` when you try to add secrets.

**Via Portal:**
1. Key Vault → **Access Control (IAM)** → **+ Add** → **Add role assignment**
2. Role tab → search `Key Vault Secrets Officer` → select → **Next**
3. Members tab → `User, group, or service principal` → **+ Select members** → search your email → **Select**
4. **Review + assign**

Wait 1–2 minutes before adding secrets.

**Via CLI:**
```bash
# Get your user object ID
az ad signed-in-user show --query id -o tsv

# Get Key Vault resource ID
az keyvault show --name kv-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query id -o tsv

# Assign role
az role assignment create \
  --assignee-object-id <YOUR-USER-OBJECT-ID> \
  --assignee-principal-type User \
  --role "Key Vault Secrets Officer" \
  --scope <KEYVAULT-RESOURCE-ID>
```

### 4.3 Key Vault RBAC Roles — Who Gets What

| Identity | Role | Purpose |
|---|---|---|
| **Your user account** | `Key Vault Secrets Officer` | Add / read / update secrets from Portal and CLI |
| **ADF Managed Identity** | `Key Vault Secrets User` | Read secrets at pipeline runtime (read-only) |
| **AzureDatabricks** enterprise app | `Key Vault Secrets User` | Databricks notebooks read secrets via secret scope |
| **Service Principal** (if used later) | `Key Vault Secrets User` | Databricks runtime reads SP credentials |

`Secrets Officer` = read + write + delete secrets.
`Secrets User` = read secret values only. **Principle of least privilege — ADF and Databricks only need to read, not manage.**

### 4.4 Add API Credentials as Secrets

Key Vault → **Secrets** → **+ Generate/Import**:

| Secret Name | Value |
|---|---|
| `voltgrid-api-base-url` | `https://ev-project-navy-mu.vercel.app` |
| `voltgrid-username` | `voltgrid_demo` |
| `voltgrid-password` | `EVcharge@AU2025` |
| `adls-account-name` | `evdatalakedev` |

**Via CLI (run after role assignment propagates):**
```bash
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-api-base-url" --value "https://ev-project-navy-mu.vercel.app"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-username" --value "voltgrid_demo"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-password" --value "EVcharge@AU2025"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "adls-account-name" --value "evdatalakedev"
```

Verify secrets are visible:
```bash
az keyvault secret list --vault-name kv-ev-intelligence-dev --query "[].name" -o tsv
```

---

## Part 5 — Create ADF Instance

> Cost: ~₹0 for the ADF instance itself. You pay per pipeline run activity (very cheap — a few paise per run).

### 5.1 Create ADF

**Via Portal:**
1. Portal → search **Data factories** → **+ Create**
2. Fill in:
   - Resource group: `rg-ev-intelligence-dev`
   - Name: `adf-ev-intelligence-dev`
   - Region: `Central India`
   - Version: `V2`
3. **Review + Create** → **Create** (~1 minute)
4. Click **Launch studio** → ADF authoring opens at `https://adf.azure.com`

**Via CLI:**
```bash
az datafactory create \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
  --location centralindia
```

> ADF automatically creates a System-assigned Managed Identity when the instance is provisioned. You cannot create it separately — it is tied to the ADF instance lifecycle.

### 5.2 Find the ADF Managed Identity Object ID

You need this to assign RBAC roles.

**Via Portal:**
ADF instance → **Properties** → copy **Managed Identity Object ID**

**Via CLI:**
```bash
az datafactory show \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
  --query "identity.principalId" -o tsv
```

---

## Part 6 — Access Control: Grant ADF the Right Roles

This is the critical step. ADF's Managed Identity needs exactly two roles:

| Resource | Role | Why |
|---|---|---|
| `kv-ev-intelligence-dev` | `Key Vault Secrets User` | Pipeline reads `voltgrid-username` and `voltgrid-password` at runtime |
| `evdatalakedev` | `Storage Blob Data Contributor` | Pipeline writes JSON files to `bronze/` container |

**No other roles. No account key. No SAS token.**

### 6.1 Grant Key Vault Secrets User to ADF MI

**Via Portal:**
1. Portal → **Key vaults** → `kv-ev-intelligence-dev` → **Access Control (IAM)**
2. **+ Add** → **Add role assignment**
3. Role: `Key Vault Secrets User` → **Next**
4. Members: select **Managed identity** → **+ Select members**
5. Subscription: yours | Managed identity: **Data factory (V2)** → select `adf-ev-intelligence-dev` → **Select**
6. **Review + assign**

**Via CLI:**
```bash
KV_ID=$(az keyvault show --name kv-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query id -o tsv)
ADF_MI=$(az datafactory show --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --query "identity.principalId" -o tsv)

az role assignment create \
  --assignee-object-id $ADF_MI \
  --assignee-principal-type ServicePrincipal \
  --role "Key Vault Secrets User" \
  --scope $KV_ID
```

### 6.2 Grant Storage Blob Data Contributor to ADF MI

**Via Portal:**
1. Portal → **Storage accounts** → `evdatalakedev` → **Access Control (IAM)**
2. **+ Add** → **Add role assignment**
3. Role: `Storage Blob Data Contributor` → **Next**
4. Members: Managed identity → `adf-ev-intelligence-dev` → **Select**
5. **Review + assign**

**Via CLI:**
```bash
STORAGE_ID=$(az storage account show --name evdatalakedev --resource-group rg-ev-intelligence-dev --query id -o tsv)
ADF_MI=$(az datafactory show --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --query "identity.principalId" -o tsv)

az role assignment create \
  --assignee-object-id $ADF_MI \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ID
```

> Wait **2 minutes** after both role assignments before testing linked services or running the pipeline. IAM propagation takes ~1–2 minutes.

### 6.3 Access Control Summary

```
Identity                   Resource                    Role
─────────────────────────────────────────────────────────────────────
Your user account          kv-ev-intelligence-dev      Key Vault Secrets Officer
ADF MI (adf-ev-...-dev)    kv-ev-intelligence-dev      Key Vault Secrets User
ADF MI (adf-ev-...-dev)    evdatalakedev (storage)     Storage Blob Data Contributor
```

---

## Part 7 — Build the ADF Pipeline

### 7.1 Create Linked Services

In ADF Studio → **Manage** (toolbox icon on left) → **Linked services** → **+ New**

**ls_keyvault — connects ADF to Key Vault:**
1. Search `Key Vault` → **Azure Key Vault** → **Continue**
2. Fill:
   - Name: `ls_keyvault`
   - Azure Key Vault: `kv-ev-intelligence-dev`
   - Authentication method: `System Assigned Managed Identity`
3. **Test connection** → must show green `Connection successful` → **Create**

**ls_voltgrid_api — connects ADF to VoltGrid REST API:**
1. **+ New** → search `REST` → **REST** → **Continue**
2. Fill:
   - Name: `ls_voltgrid_api`
   - Base URL: `https://ev-project-navy-mu.vercel.app`
   - Authentication type: `Anonymous`
3. **Test connection** → green → **Create**

> **Why Anonymous on the REST linked service?** ADF's built-in REST auth types (Basic, OAuth2) don't support this pattern: POST to get a token, then use it in a header. We handle token acquisition ourselves inside the pipeline (Web Activities), so the linked service stays anonymous.

**ls_adls_bronze — connects ADF to ADLS Gen2:**
1. **+ New** → search `Azure Data Lake Storage Gen2` → **Continue**
2. Fill:
   - Name: `ls_adls_bronze`
   - Authentication method: `System Assigned Managed Identity`
   - Storage account: `evdatalakedev`
3. **Test connection** → green → **Create**

> The connection test works because ADF MI has `Storage Blob Data Contributor` on `evdatalakedev`. If it fails with 403, wait 2 more minutes for IAM propagation.

### 7.2 Create Datasets

**Author** (pencil icon) → **Datasets** → **+ New dataset**

**Source dataset — `ds_voltgrid_payments_src`:**
1. **+ New dataset** → search `REST` → **REST** → **Continue**
2. Name: `ds_voltgrid_payments_src` | Linked service: `ls_voltgrid_api` | Relative URL: `/api/db/payments/` → **OK**
3. **Parameters** tab → **+ New** for each:
   - `p_page` — type: `Int` — default: `1`
   - `p_page_size` — type: `Int` — default: `100`
4. **Connection** tab → Relative URL field → click **Add dynamic content** (blue link):
   ```
   /api/db/payments/?page=@{dataset().p_page}&page_size=@{dataset().p_page_size}
   ```
5. **Publish all**

**Sink dataset — `ds_bronze_payments_sink`:**
1. **+ New dataset** → search `Azure Data Lake Storage Gen2` → select `JSON` format → **Continue**
2. Name: `ds_bronze_payments_sink` | Linked service: `ls_adls_bronze`
3. File path:
   - Container: `bronze`
   - Directory: `api/payments/raw`
   - File: `payments.json`
4. **Publish all**

> **Why this path?** Following the bronze folder convention: `bronze/<source-type>/<entity>/raw/<file>`. For API sources that is `api/payments/raw/`. Blob CSV sources will land at `blob/sessions/raw/`, streaming at `streaming/telemetry/raw/`.

### 7.3 Build Pipeline `pl_bronze_api_payments`

**Author** → **Pipelines** → **+** → **New pipeline**
- Name: `pl_bronze_api_payments`
- **Parameters** tab (bottom panel):
  - `p_page` — type: `int` — default: `1`
  - `p_page_size` — type: `int` — default: `100`
- **Variables** tab:
  - `v_token` — type: `String`

**Activity 1 — act_get_username** (runs in parallel with act_get_password):
- Drag **Web Activity** onto canvas → rename: `act_get_username`
- **Settings** tab:
  - URL: `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-username/?api-version=7.0`
  - Method: `GET`
  - Authentication: `System Assigned Managed Identity`
  - Resource: `https://vault.azure.net`

**Activity 2 — act_get_password** (runs in parallel with act_get_username):
- Drag another **Web Activity** → rename: `act_get_password`
- **Settings** tab:
  - URL: `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-password/?api-version=7.0`
  - Method: `GET`
  - Authentication: `System Assigned Managed Identity`
  - Resource: `https://vault.azure.net`
- (No dependency arrow from act_get_username — both run at the same time)

**Activity 3 — act_api_login:**
- Drag **Web Activity** → rename: `act_api_login`
- Draw **success arrow** from `act_get_username` → `act_api_login`
- Draw **success arrow** from `act_get_password` → `act_api_login`
- (Both must succeed before login attempt)
- **Settings** tab:
  - URL: `https://ev-project-navy-mu.vercel.app/api/auth/login/`
  - Method: `POST`
  - Headers: add `Content-Type` = `application/json`
  - Body → **Add dynamic content**:
    ```
    @concat('{"username":"', activity('act_get_username').output.value, '","password":"', activity('act_get_password').output.value, '"}')
    ```

> `activity('act_get_username').output.value` — the Key Vault REST API returns `{"value": "voltgrid_demo", ...}`. The `.output.value` accesses the secret string.

**Activity 4 — act_set_token:**
- Drag **Set Variable** → rename: `act_set_token`
- Connect `act_api_login` → `act_set_token`
- **Settings** tab:
  - Variable: `v_token`
  - Value → **Add dynamic content**: `@activity('act_api_login').output.token`

> The login endpoint returns `{"token": "abc123..."}`. `.output.token` extracts the token string into the pipeline variable `v_token`.

**Activity 5 — act_copy_payments:**
- Drag **Copy data** → rename: `act_copy_payments`
- Connect `act_set_token` → `act_copy_payments`
- **Source** tab:
  - Dataset: `ds_voltgrid_payments_src`
  - Dataset parameters:
    - `p_page` → **Add dynamic content**: `@pipeline().parameters.p_page`
    - `p_page_size` → **Add dynamic content**: `@pipeline().parameters.p_page_size`
  - Additional headers → **+ New**: Name `Authorization` | Value → **Add dynamic content**: `@concat('Token ', variables('v_token'))`
  - Request method: `GET`
- **Sink** tab:
  - Dataset: `ds_bronze_payments_sink`
  - File pattern: `setOfObjects`

6. **Publish all**

---

## Part 8 — Trigger and Verify

### 8.1 Run the Pipeline

1. **Author** → `pl_bronze_api_payments` → **Add trigger** → **Trigger now**
2. Parameters: `p_page = 1`, `p_page_size = 100` → **OK**
3. **Monitor** (chart icon) → **Pipeline runs** → watch the pipeline appear

### 8.2 Watch Activity Runs

Click the pipeline run name → **Activity runs**:

| Activity | Expected status | Duration |
|---|---|---|
| act_get_username | Succeeded | ~1s |
| act_get_password | Succeeded | ~1s |
| act_api_login | Succeeded | ~2s |
| act_set_token | Succeeded | <1s |
| act_copy_payments | Succeeded | ~5–15s |

Click `act_copy_payments` → **Output** tab:
```json
{
  "dataRead": 14523,
  "dataWritten": 14523,
  "rowsRead": 100,
  "rowsCopied": 100
}
```

### 8.3 Verify File in Bronze

In a Databricks notebook (or Cloud Shell with CLI), verify the file landed in the correct Bronze path:

**In Databricks (after SP OAuth or MI setup):**
```python
# List the bronze payments directory
display(dbutils.fs.ls("abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/"))

# Read the file
df = spark.read.option("multiLine", "true").json(
    "abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/payments.json"
)
print(f"Record count: {df.count()}")
display(df.limit(5))
```

Expected:
```
Record count: 100
payment_id | session_id | amount_aud | status | paid_at | ...
PAY-000001 | SES-12345  | 15.50      | PAID   | 2026-...
```

**Via Azure CLI (Storage Explorer alternative):**
```bash
az storage blob list \
  --account-name evdatalakedev \
  --container-name bronze \
  --prefix "api/payments/raw/" \
  --auth-mode login \
  --query "[].name" -o tsv
```

Expected:
```
api/payments/raw/payments.json
```

---

## Part 9 — Pipeline JSON (Import Shortcut)

Instead of building each activity in the UI, import the pipeline JSON directly.

**Step 1 — Create linked services and datasets first** (Steps 7.1 and 7.2 above) — JSON import expects them to exist.

**Step 2 — Import pipeline:**
1. **Author** → **Pipelines** → **+** → **New pipeline** → name it `pl_bronze_api_payments`
2. Click **{ }** Code button (top right of canvas)
3. Select all → paste the JSON below → **OK**
4. **Publish all**

```json
{
    "name": "pl_bronze_api_payments",
    "properties": {
        "activities": [
            {
                "name": "act_get_username",
                "type": "WebActivity",
                "dependsOn": [],
                "typeProperties": {
                    "method": "GET",
                    "url": "https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-username/?api-version=7.0",
                    "authentication": {
                        "type": "MSI",
                        "resource": "https://vault.azure.net"
                    }
                }
            },
            {
                "name": "act_get_password",
                "type": "WebActivity",
                "dependsOn": [],
                "typeProperties": {
                    "method": "GET",
                    "url": "https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-password/?api-version=7.0",
                    "authentication": {
                        "type": "MSI",
                        "resource": "https://vault.azure.net"
                    }
                }
            },
            {
                "name": "act_api_login",
                "type": "WebActivity",
                "dependsOn": [
                    {
                        "activity": "act_get_username",
                        "dependencyConditions": ["Succeeded"]
                    },
                    {
                        "activity": "act_get_password",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "method": "POST",
                    "headers": { "Content-Type": "application/json" },
                    "url": "https://ev-project-navy-mu.vercel.app/api/auth/login/",
                    "body": "@concat('{\"username\":\"', activity('act_get_username').output.value, '\",\"password\":\"', activity('act_get_password').output.value, '\"}')"
                }
            },
            {
                "name": "act_set_token",
                "type": "SetVariable",
                "dependsOn": [
                    {
                        "activity": "act_api_login",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "variableName": "v_token",
                    "value": {
                        "value": "@activity('act_api_login').output.token",
                        "type": "Expression"
                    }
                }
            },
            {
                "name": "act_copy_payments",
                "type": "Copy",
                "dependsOn": [
                    {
                        "activity": "act_set_token",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "source": {
                        "type": "RestSource",
                        "requestMethod": "GET",
                        "additionalHeaders": {
                            "Authorization": "@concat('Token ', variables('v_token'))"
                        },
                        "paginationRules": { "supportRFC5988": "true" }
                    },
                    "sink": {
                        "type": "JsonSink",
                        "storeSettings": { "type": "AzureBlobFSWriteSettings" },
                        "formatSettings": {
                            "type": "JsonWriteSettings",
                            "filePattern": "setOfObjects"
                        }
                    },
                    "enableStaging": false
                },
                "inputs": [
                    {
                        "referenceName": "ds_voltgrid_payments_src",
                        "type": "DatasetReference",
                        "parameters": {
                            "p_page": { "value": "@pipeline().parameters.p_page", "type": "Expression" },
                            "p_page_size": { "value": "@pipeline().parameters.p_page_size", "type": "Expression" }
                        }
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "ds_bronze_payments_sink",
                        "type": "DatasetReference"
                    }
                ]
            }
        ],
        "parameters": {
            "p_page": { "type": "string", "defaultValue": "1" },
            "p_page_size": { "type": "string", "defaultValue": "100" }
        },
        "variables": {
            "v_token": { "type": "String" }
        }
    }
}
```

---

## Part 10 — ADLS Folder Structure Convention

Bronze is append-only. Data lands in a deterministic path so downstream consumers know exactly where to look.

```
bronze/
├── api/
│   ├── payments/
│   │   └── raw/
│   │       └── payments.json             ← today's target (Day 2, page 1 only)
│   ├── sessions/
│   │   └── raw/
│   ├── customers/
│   │   └── raw/
│   └── stations/
│       └── raw/
├── blob/
│   ├── sessions/
│   │   └── raw/
│   └── invoices/
│       └── raw/
└── streaming/
    └── telemetry/
        └── raw/

silver/
├── sessions/                             ← Delta tables, MERGE upsert
├── payments/
├── customers/
└── chargers/

gold/
├── fact_charging_session/               ← Star schema
├── dim_customer/
└── mart_revenue_by_geo_month/
```

> Day 3 adds partitioning inside the bronze paths: `api/payments/raw/ingestion_date=2026-10-13/page_001.json`

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `act_get_username` fails with 403 | ADF MI missing `Key Vault Secrets User` on Key Vault | Key Vault → IAM → assign role → wait 2 min → rerun |
| `act_api_login` returns 401 | Wrong credential value in Key Vault secrets | Portal → Key Vault → Secrets → verify `voltgrid-username` and `voltgrid-password` values |
| `act_api_login` fails — `token` not in output | Body expression is malformed — concat issue | Monitor → `act_api_login` → Input tab → verify body JSON is valid |
| `act_copy_payments` 403 on sink | ADF MI missing `Storage Blob Data Contributor` on storage | Storage → IAM → assign role → wait 2 min → rerun |
| `act_copy_payments` 403 on source | Token not captured — `v_token` is empty | Check `act_set_token` → verify `@activity('act_api_login').output.token` resolves |
| Linked service test connection 403 | Role not propagated | Wait 2 more minutes → test again |
| `AuthorizationFailure` on container create via CLI | `--auth-mode login` requires Storage Blob Data Owner/Contributor on your account | Assign yourself `Storage Blob Data Contributor` on the storage account, or use `--account-key` temporarily just for container creation |
| `SharedKeyDisabled` error | Storage account key access disabled (correct) but code is using account key | Switch to `--auth-mode login` in all CLI commands |
| `MissingSubscriptionRegistration` | Resource provider not registered | `az provider register --namespace Microsoft.DataFactory` → wait 1 min |

---

## Day 2 Checklist

**Infrastructure:**
- [ ] Resource providers registered: KeyVault, Storage, DataFactory
- [ ] Resource Group `rg-ev-intelligence-dev` created in Central India
- [ ] Storage Account `evdatalakedev` created — Standard LRS, hierarchical namespace ON
- [ ] Storage account key access **disabled** (modern standard)
- [ ] Containers created: `bronze`, `silver`, `gold`, `source` — all Private
- [ ] Key Vault `kv-ev-intelligence-dev` created — Standard tier, RBAC permission model
- [ ] Your user has `Key Vault Secrets Officer` on Key Vault
- [ ] Secrets added: `voltgrid-api-base-url`, `voltgrid-username`, `voltgrid-password`, `adls-account-name`
- [ ] ADF instance `adf-ev-intelligence-dev` created in Central India

**Access Control:**
- [ ] ADF MI has `Key Vault Secrets User` on `kv-ev-intelligence-dev`
- [ ] ADF MI has `Storage Blob Data Contributor` on `evdatalakedev`

**ADF Pipeline:**
- [ ] Linked service `ls_keyvault` — test connection green
- [ ] Linked service `ls_voltgrid_api` — test connection green
- [ ] Linked service `ls_adls_bronze` — test connection green
- [ ] Dataset `ds_voltgrid_payments_src` — parameters p_page and p_page_size configured
- [ ] Dataset `ds_bronze_payments_sink` — path: `bronze/api/payments/raw/payments.json`
- [ ] Pipeline `pl_bronze_api_payments` — 5 activities built, published
- [ ] Pipeline triggered manually — all 5 activities green
- [ ] `act_copy_payments` output shows rowsCopied = 100
- [ ] File `bronze/api/payments/raw/payments.json` exists in ADLS
- [ ] (Optional) Databricks notebook confirms record count = 100

---

## Quick Reference

```
Resource Group   : rg-ev-intelligence-dev      (Central India)
Storage Account  : evdatalakedev               (Standard LRS, HNS ON, key access OFF)
Key Vault        : kv-ev-intelligence-dev      (Standard, RBAC model)
ADF Instance     : adf-ev-intelligence-dev     (V2, System-assigned MI)
Bronze API path  : bronze/api/payments/raw/payments.json
abfss path       : abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/

Access control (modern standard — no account keys, no SAS):
  Your user          → Key Vault Secrets Officer  → kv-ev-intelligence-dev
  ADF MI             → Key Vault Secrets User     → kv-ev-intelligence-dev
  ADF MI             → Storage Blob Data Contributor → evdatalakedev
```
