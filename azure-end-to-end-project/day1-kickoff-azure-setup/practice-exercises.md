# Day 1 — Practice Exercises: Kickoff, Architecture, and Azure Setup

> Complete in order. Each exercise builds on the previous one.
> All resources go in region: **Central India** | Resource Group: **rg-ev-intelligence-dev**

---

## Exercise 1 — Subscription Check and CLI Setup

**Objective:** Verify your Azure account is active and CLI tools are working.

1. Log in to [portal.azure.com](https://portal.azure.com) — confirm your subscription shows **Active**
2. Note your **Subscription ID** from the Subscriptions page
3. Install Azure CLI (MSI from aka.ms/installazurecliwindows on Windows, `brew install azure-cli` on Mac)
4. Run:
   ```cmd
   az --version
   az login
   az account show
   ```
5. Confirm the output shows your subscription name and status `Enabled`
6. Set a Budget Alert at ₹1,500/month with alerts at 50% and 90%

**Verify:** `az account show --query id -o tsv` returns your Subscription ID without error.

---

## Exercise 2 — Register Resource Providers

**Objective:** Register all 6 required providers so resources can be created without errors.

1. Register via CLI:
   ```cmd
   az provider register --namespace Microsoft.KeyVault
   az provider register --namespace Microsoft.Storage
   az provider register --namespace Microsoft.Databricks
   az provider register --namespace Microsoft.EventHub
   az provider register --namespace Microsoft.DataFactory
   az provider register --namespace Microsoft.ManagedIdentity
   ```
2. Wait 1 minute, then verify all 6 show `Registered`:
   ```cmd
   az provider show --namespace Microsoft.KeyVault --query registrationState -o tsv
   az provider show --namespace Microsoft.Storage --query registrationState -o tsv
   az provider show --namespace Microsoft.Databricks --query registrationState -o tsv
   az provider show --namespace Microsoft.DataFactory --query registrationState -o tsv
   az provider show --namespace Microsoft.EventHub --query registrationState -o tsv
   az provider show --namespace Microsoft.ManagedIdentity --query registrationState -o tsv
   ```

**Verify:** All 6 return `Registered`. Do not proceed until all 6 are registered.

---

## Exercise 3 — Create Resource Group and Storage Account

**Objective:** Create the project's resource container and ADLS Gen2 storage with all 4 containers.

1. Create the resource group:
   ```cmd
   az group create --name rg-ev-intelligence-dev --location centralindia
   ```
2. Create the ADLS Gen2 storage account (note: hierarchical namespace must be ON):
   ```cmd
   az storage account create --name evdatalakedev --resource-group rg-ev-intelligence-dev --location centralindia --sku Standard_LRS --kind StorageV2 --enable-hierarchical-namespace true --access-tier Cool
   ```
3. Create all 4 containers:
   ```cmd
   az storage container create --name bronze --account-name evdatalakedev --auth-mode login
   az storage container create --name silver --account-name evdatalakedev --auth-mode login
   az storage container create --name gold --account-name evdatalakedev --auth-mode login
   az storage container create --name source --account-name evdatalakedev --auth-mode login
   ```
4. In the Portal, go to `evdatalakedev` → **Containers** — confirm all 4 are visible

**Verify:** All 4 containers show as Private. Storage account shows **Hierarchical namespace: Enabled** in the Overview.

---

## Exercise 4 — Create Key Vault and Add Secrets

**Objective:** Create a secure secret store and add the first set of API credentials.

1. Create the Key Vault:
   ```cmd
   az keyvault create --name kv-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --location centralindia --sku standard
   ```
2. Via Portal, assign yourself `Key Vault Secrets Officer` role:
   - Key Vault → **Access Control (IAM)** → **+ Add** → **Add role assignment**
   - Role: `Key Vault Secrets Officer` → Members: your account → **Review + assign**
   - Wait 1–2 minutes
3. Add the first 4 secrets:
   ```cmd
   az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-api-base-url" --value "https://ev-project-navy-mu.vercel.app"
   az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-username" --value "voltgrid_demo"
   az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-password" --value "EVcharge@AU2025"
   az keyvault secret set --vault-name kv-ev-intelligence-dev --name "adls-account-name" --value "evdatalakedev"
   ```
4. Portal → Key Vault → **Secrets** — confirm all 4 are listed with **Enabled** status

**Verify:** `az keyvault secret show --vault-name kv-ev-intelligence-dev --name voltgrid-username --query value -o tsv` returns `voltgrid_demo`.

---

## Exercise 5 — Create Service Principal and Assign Storage Role

**Objective:** Create a non-human identity for Databricks and ADF to use, and grant it storage access.

1. Create the SP (replace `<YOUR_SUBSCRIPTION_ID>`):
   ```cmd
   az ad sp create-for-rbac --name sp-ev-intelligence-dev --role Contributor --scopes /subscriptions/<YOUR_SUBSCRIPTION_ID>/resourceGroups/rg-ev-intelligence-dev
   ```
2. **Save the output immediately** — copy `appId`, `password`, `tenant`
3. Store all 3 in Key Vault:
   ```cmd
   az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-client-id" --value "<appId>"
   az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-client-secret" --value "<password>"
   az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-tenant-id" --value "<tenant>"
   ```
4. Get the SP's Object ID:
   ```cmd
   az ad sp show --id <appId> --query id -o tsv
   ```
5. Get the storage resource ID:
   ```cmd
   az storage account show --name evdatalakedev --resource-group rg-ev-intelligence-dev --query id -o tsv
   ```
6. Assign `Storage Blob Data Contributor` role:
   ```cmd
   az role assignment create --assignee-object-id <SP-Object-ID> --assignee-principal-type ServicePrincipal --role "Storage Blob Data Contributor" --scope <storage-resource-id>
   ```

**Verify:** Portal → `evdatalakedev` → **Access Control (IAM)** → **Role assignments** → confirm `sp-ev-intelligence-dev` appears under `Storage Blob Data Contributor`.

---

## Exercise 6 — Create Databricks Workspace and Cluster

**Objective:** Launch a Databricks workspace and create a minimum-cost cluster.

1. Via Portal: Create Azure Databricks workspace
   - Resource group: `rg-ev-intelligence-dev`
   - Name: `dbw-ev-intelligence-dev`
   - Region: `Central India`
   - Pricing tier: **Trial (Premium - 14 Days Free DBUs)**
2. Click **Launch Workspace** once deployed
3. In Databricks: **Compute** → **+ Create compute**
   - Cluster mode: **Single Node**
   - Access mode: **Dedicated (Single user)**
   - Runtime: `15.4 LTS (Spark 3.5, Scala 2.12)`
   - Photon Acceleration: **OFF**
   - Node type: `Standard_DS3_v2`
   - Auto termination: **15 minutes**
4. Click **Create compute** — wait ~5 minutes for it to start

**Verify:** Cluster shows green dot with status **Running**.

---

## Exercise 7 — Create Secret Scope and Connect Storage in Databricks

**Objective:** Link Databricks to Key Vault and verify all 4 storage containers are accessible.

**Part A — Assign AzureDatabricks app access to Key Vault:**
1. Key Vault → **Access Control (IAM)** → **+ Add** → **Add role assignment**
2. Role: `Key Vault Secrets User` → Members: search **AzureDatabricks** → assign
3. Wait 2 minutes

**Part B — Create secret scope:**
1. Open your Databricks URL + `#secrets/createScope`
2. Scope Name: `kv-ev-scope` | Manage Principal: `All Users`
3. DNS Name: Vault URI from Key Vault → Properties
4. Resource ID: Resource ID from Key Vault → Properties
5. Click **Create**

**Part C — Create and run notebook `00b_connect_storage_no_mount`:**
```python
# Cell 1 — load secrets
SCOPE = "kv-ev-scope"
storage_account  = dbutils.secrets.get(scope=SCOPE, key="adls-account-name")
sp_client_id     = dbutils.secrets.get(scope=SCOPE, key="sp-client-id")
sp_client_secret = dbutils.secrets.get(scope=SCOPE, key="sp-client-secret")
sp_tenant_id     = dbutils.secrets.get(scope=SCOPE, key="sp-tenant-id")
print("Secrets loaded — OK")

# Cell 2 — configure Spark OAuth
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", sp_client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", sp_client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{sp_tenant_id}/oauth2/token")
print("Spark OAuth config set — OK")

# Cell 3 — verify all 4 containers
def abfss(container, path=""):
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base

for container in ["bronze", "silver", "gold", "source"]:
    try:
        items = dbutils.fs.ls(abfss(container))
        print(f"  {container:<8} OK — {len(items)} items")
    except Exception as e:
        print(f"  {container:<8} ERROR — {e}")
```

**Verify:** All 4 containers show `OK — 0 items` (empty is expected on Day 1).

---

## Exercise 8 — Verify API Auth and Read Source Blob

**Objective:** Confirm the VoltGrid API is reachable and source CSV files are readable.

**Part A — API auth (notebook `01_verify_api_auth`):**
```python
import requests
SCOPE = "kv-ev-scope"
api_base_url = dbutils.secrets.get(scope=SCOPE, key="voltgrid-api-base-url")
username     = dbutils.secrets.get(scope=SCOPE, key="voltgrid-username")
password     = dbutils.secrets.get(scope=SCOPE, key="voltgrid-password")

resp = requests.post(f"{api_base_url}/api/auth/login/",
                     json={"username": username, "password": password}, timeout=10)
resp.raise_for_status()
token = resp.json()["token"]
print(f"Token: {token[:8]}...[REDACTED]")

headers = {"Authorization": f"Token {token}"}
r = requests.get(f"{api_base_url}/api/db/payments/?page=1&page_size=5", headers=headers, timeout=10)
pg = r.json().get("pagination", {})
print(f"Total payments: {pg.get('total', 0):,}")
```

**Part B — Source blob (notebook `02_read_source_blob`):**
1. Add secrets to Key Vault: `source-storage-account`, `source-container`, `source-sas-token`
2. Run the notebook that reads from `wasbs://` using the SAS token
3. Confirm you can see the `realtime/` folder and read CSV files

**Verify:**
- API: token acquired, payments total > 0
- Source blob: CSV rows read successfully via glob pattern

---
