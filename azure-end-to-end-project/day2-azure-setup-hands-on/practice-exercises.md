# Day 2 — Practice Exercises: Azure Setup + ADF Bronze Ingestion

> Complete in order. Each exercise builds on the previous one.
> All resources: region **Central India** | Resource Group: **rg-ev-intelligence-dev**
> Modern standard: **no storage account keys, no SAS tokens for your own storage.**

---

## Exercise 1 — Create Resource Group and ADLS Gen2

**Objective:** Provision the project's resource container and data lake storage with all 4 containers.

1. Register the required resource providers:
   ```bash
   az provider register --namespace Microsoft.KeyVault
   az provider register --namespace Microsoft.Storage
   az provider register --namespace Microsoft.DataFactory
   ```
2. Create the Resource Group:
   ```bash
   az group create --name rg-ev-intelligence-dev --location centralindia
   ```
3. Create the ADLS Gen2 storage account with hierarchical namespace ON:
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
4. Disable storage account key access (modern standard — enforce RBAC-only):
   ```bash
   az storage account update \
     --name evdatalakedev \
     --resource-group rg-ev-intelligence-dev \
     --allow-shared-key-access false
   ```
5. Create 4 containers using your Azure AD login (no account key):
   ```bash
   az storage container create --name bronze --account-name evdatalakedev --auth-mode login
   az storage container create --name silver --account-name evdatalakedev --auth-mode login
   az storage container create --name gold   --account-name evdatalakedev --auth-mode login
   az storage container create --name source --account-name evdatalakedev --auth-mode login
   ```

**Verify:**
- Portal → `evdatalakedev` → **Configuration** → Shared key access = **Disabled**
- Portal → `evdatalakedev` → **Containers** → all 4 visible with Access level = Private
- Portal → `evdatalakedev` → **Overview** → Hierarchical namespace = **Enabled**

---

## Exercise 2 — Create Key Vault with RBAC Model

**Objective:** Create a secure secrets store using the modern RBAC permission model and add API credentials.

1. Create the Key Vault with RBAC authorization:
   ```bash
   az keyvault create \
     --name kv-ev-intelligence-dev \
     --resource-group rg-ev-intelligence-dev \
     --location centralindia \
     --sku standard \
     --enable-rbac-authorization true
   ```
2. Get your user Object ID:
   ```bash
   az ad signed-in-user show --query id -o tsv
   ```
3. Get the Key Vault resource ID:
   ```bash
   az keyvault show --name kv-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query id -o tsv
   ```
4. Assign yourself `Key Vault Secrets Officer` (replace placeholders):
   ```bash
   az role assignment create \
     --assignee-object-id <YOUR-USER-OBJECT-ID> \
     --assignee-principal-type User \
     --role "Key Vault Secrets Officer" \
     --scope <KEYVAULT-RESOURCE-ID>
   ```
5. Wait 1–2 minutes, then add the API secrets:
   ```bash
   az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-api-base-url" --value "https://ev-project-navy-mu.vercel.app"
   az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-username" --value "voltgrid_demo"
   az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-password" --value "EVcharge@AU2025"
   az keyvault secret set --vault-name kv-ev-intelligence-dev --name "adls-account-name" --value "evdatalakedev"
   ```

**Verify:**
```bash
az keyvault secret show --vault-name kv-ev-intelligence-dev --name voltgrid-username --query value -o tsv
```
Expected output: `voltgrid_demo`

```bash
az keyvault secret list --vault-name kv-ev-intelligence-dev --query "[].name" -o tsv
```
Expected: all 4 secret names listed.

---

## Exercise 3 — Create ADF and Assign Access Control

**Objective:** Create ADF and give its Managed Identity exactly the two roles it needs — no more.

1. Create the ADF instance:
   ```bash
   az datafactory create \
     --resource-group rg-ev-intelligence-dev \
     --factory-name adf-ev-intelligence-dev \
     --location centralindia
   ```
2. Get the ADF Managed Identity Object ID:
   ```bash
   az datafactory show \
     --resource-group rg-ev-intelligence-dev \
     --factory-name adf-ev-intelligence-dev \
     --query "identity.principalId" -o tsv
   ```
3. Assign `Key Vault Secrets User` to ADF MI on Key Vault:
   ```bash
   KV_ID=$(az keyvault show --name kv-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query id -o tsv)
   ADF_MI=$(az datafactory show --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --query "identity.principalId" -o tsv)

   az role assignment create \
     --assignee-object-id $ADF_MI \
     --assignee-principal-type ServicePrincipal \
     --role "Key Vault Secrets User" \
     --scope $KV_ID
   ```
4. Assign `Storage Blob Data Contributor` to ADF MI on storage:
   ```bash
   STORAGE_ID=$(az storage account show --name evdatalakedev --resource-group rg-ev-intelligence-dev --query id -o tsv)

   az role assignment create \
     --assignee-object-id $ADF_MI \
     --assignee-principal-type ServicePrincipal \
     --role "Storage Blob Data Contributor" \
     --scope $STORAGE_ID
   ```
5. Wait 2 minutes for IAM propagation.

**Verify:**
- Portal → `kv-ev-intelligence-dev` → **Access Control (IAM)** → **Role assignments** → `adf-ev-intelligence-dev` shows under `Key Vault Secrets User`
- Portal → `evdatalakedev` → **Access Control (IAM)** → **Role assignments** → `adf-ev-intelligence-dev` shows under `Storage Blob Data Contributor`

---

## Exercise 4 — Create Linked Services in ADF Studio

**Objective:** Configure ADF's three connection definitions and verify each with Test Connection.

1. Go to ADF Studio → **Manage** → **Linked services** → **+ New**

2. Create `ls_keyvault`:
   - Type: **Azure Key Vault**
   - Name: `ls_keyvault`
   - Key Vault: `kv-ev-intelligence-dev`
   - Auth: `System Assigned Managed Identity`
   - Click **Test connection** — must be green

3. Create `ls_voltgrid_api`:
   - Type: **REST**
   - Name: `ls_voltgrid_api`
   - Base URL: `https://ev-project-navy-mu.vercel.app`
   - Auth type: `Anonymous`
   - Click **Test connection** — must be green

4. Create `ls_adls_bronze`:
   - Type: **Azure Data Lake Storage Gen2**
   - Name: `ls_adls_bronze`
   - Auth method: `System Assigned Managed Identity`
   - Storage account: `evdatalakedev`
   - Click **Test connection** — must be green

5. **Publish all**

**Verify:** All 3 linked services show green status in **Manage → Linked services**.

> If `ls_adls_bronze` test fails with 403, the role assignment hasn't propagated. Wait 2 more minutes and retry.

---

## Exercise 5 — Create Datasets

**Objective:** Define where data comes from (source) and where it goes (sink).

**Source dataset:**
1. **Author** → **Datasets** → **+ New dataset** → search `REST` → select **REST** → **Continue**
2. Name: `ds_voltgrid_payments_src` | Linked service: `ls_voltgrid_api`
3. **Parameters** tab → add:
   - `p_page` — Int — default: `1`
   - `p_page_size` — Int — default: `100`
4. **Connection** tab → Relative URL → **Add dynamic content**:
   ```
   /api/db/payments/?page=@{dataset().p_page}&page_size=@{dataset().p_page_size}
   ```
5. **Publish all**

**Sink dataset:**
1. **+ New dataset** → search `Azure Data Lake Storage Gen2` → select `JSON` format → **Continue**
2. Name: `ds_bronze_payments_sink` | Linked service: `ls_adls_bronze`
3. File path: Container = `bronze` | Directory = `api/payments/raw` | File = `payments.json`
4. **Publish all**

**Verify:** Both datasets appear in **Author → Datasets** with no red validation errors.

---

## Exercise 6 — Build and Run Pipeline `pl_bronze_api_payments`

**Objective:** Build the 5-activity pipeline that authenticates to the VoltGrid API and copies payment records to Bronze.

1. **Author** → **Pipelines** → **+** → **New pipeline** → name: `pl_bronze_api_payments`
2. **Parameters** tab: add `p_page` (int, default 1), `p_page_size` (int, default 100)
3. **Variables** tab: add `v_token` (String)
4. Build 5 activities as per notes Part 7.3 — or paste the JSON from Part 9 via the **{ }** Code button
5. **Publish all**
6. **Add trigger** → **Trigger now** → `p_page = 1`, `p_page_size = 100` → **OK**
7. **Monitor** panel → watch all 5 activities turn green (~20–30 seconds total)
8. Click the run → **Activity runs** → click `act_copy_payments` → **Output** tab

**Expected output:**
```json
{
  "rowsRead": 100,
  "rowsCopied": 100
}
```

**Verify the file in storage:**
```bash
az storage blob list \
  --account-name evdatalakedev \
  --container-name bronze \
  --prefix "api/payments/raw/" \
  --auth-mode login \
  --query "[].name" -o tsv
```

Expected: `api/payments/raw/payments.json`

---

## Exercise 7 — Inspect the Bronze File

**Objective:** Read the raw JSON that ADF wrote and understand the Bronze-layer structure.

Open a Databricks notebook and run (assumes SP OAuth is configured — see Day 2 notes Part 7 or set up SP credentials in Key Vault first):

```python
SCOPE = "kv-ev-scope"
storage_account = dbutils.secrets.get(scope=SCOPE, key="adls-account-name")
sp_client_id    = dbutils.secrets.get(scope=SCOPE, key="sp-client-id")
sp_client_secret= dbutils.secrets.get(scope=SCOPE, key="sp-client-secret")
sp_tenant_id    = dbutils.secrets.get(scope=SCOPE, key="sp-tenant-id")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", sp_client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", sp_client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{sp_tenant_id}/oauth2/token")

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/api/payments/raw/"

# List files
display(dbutils.fs.ls(bronze_path))

# Read the JSON
df = spark.read.option("multiLine", "true").json(bronze_path + "payments.json")
print(f"Record count: {df.count()}")
print(f"Schema:")
df.printSchema()
display(df.limit(5))
```

Answer these questions from the output:
1. How many records were written? Does it match the `rowsCopied = 100` in ADF Monitor?
2. What columns does the raw JSON have? Are there any null columns?
3. Is there a `_ingestion_ts` or `_source_file` column? (Answer: No — Bronze from ADF lands raw. Audit columns are added by Databricks in Day 3.)
4. What is the data type ADF inferred for `amount_aud`? Is it correct?

**Verify:** Record count = 100, file exists at `api/payments/raw/payments.json`.

---

## Exercise 8 — Access Control Audit

**Objective:** Verify that the access control setup is correct and follows the principle of least privilege.

Answer each question from the Portal (IAM blade) or CLI — do not guess:

1. **Key Vault IAM:** Who has `Key Vault Secrets Officer`? Who has `Key Vault Secrets User`?
   ```bash
   az role assignment list --scope <KV-RESOURCE-ID> --query "[].{principal:principalName, role:roleDefinitionName}" -o table
   ```

2. **Storage IAM:** Who has `Storage Blob Data Contributor`?
   ```bash
   az role assignment list --scope <STORAGE-RESOURCE-ID> --query "[].{principal:principalName, role:roleDefinitionName}" -o table
   ```

3. Does anyone have `Owner` or `Contributor` on the storage account? (Answer should be: your account from subscription, but ADF MI should only have `Storage Blob Data Contributor` — not Owner.)

4. Is the storage account key access disabled?
   ```bash
   az storage account show --name evdatalakedev --resource-group rg-ev-intelligence-dev --query "allowSharedKeyAccess" -o tsv
   ```
   Expected: `false`

5. If you try to run `az storage blob list --account-name evdatalakedev --container-name bronze --account-key <key>`, what error do you get and why?

**Verify:** The access control table matches the expected state from notes Part 6.3.

---
