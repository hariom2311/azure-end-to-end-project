# 01 — ADF Linked Services
**Day 2 | Step 1 of 4**

Create the 4 linked services ADF needs to talk to external systems.
A linked service = a saved connection definition. Think of it as a named connector — pipelines reference it by name, never re-entering credentials.

---

## What You Will Create

| Linked Service Name | Type | Connects To |
|---|---|---|
| `ls_keyvault` | Azure Key Vault | `kv-ev-intelligence-dev` |
| `ls_voltgrid_api` | REST | VoltGrid API base URL |
| `ls_source_blob` | Azure Blob Storage | `dataenggdailystorage` (SAS token) |
| `ls_adls_bronze` | Azure Data Lake Storage Gen2 | `evdatalakedev` (Managed Identity) |

**Create them in this order** — `ls_keyvault` must exist before the others because they reference secrets from it.

---

## Linked Service 1 — Azure Key Vault (`ls_keyvault`)

### Why Key Vault first?
ADF can pull secret values from Key Vault at runtime. All other linked services reference secrets by name — no credentials stored in ADF itself.

---

### UI Steps

1. Open ADF Studio: `https://adf.azure.com` → select `adf-ev-intelligence-dev`
2. Left sidebar → **Manage** (toolbox icon)
3. Under **Connections** → **Linked services** → **+ New**
4. Search `Key Vault` → select **Azure Key Vault** → **Continue**
5. Fill in:
   - **Name:** `ls_keyvault`
   - **Azure subscription:** select yours
   - **Azure Key Vault name:** `kv-ev-intelligence-dev`
   - **Authentication method:** System Assigned Managed Identity
6. Click **Test connection** — must show **Connection successful**
7. Click **Create**

> **If test fails:** Go to Key Vault → Access policies → confirm ADF Managed Identity has **Secret Get** and **Secret List** permissions. See Day 1 Part 5.

---

### CLI Steps

```bash
# Replace with your values
SUBSCRIPTION="your-subscription-id"
RG="rg-ev-intelligence-dev"
ADF="adf-ev-intelligence-dev"
KV="kv-ev-intelligence-dev"

az datafactory linked-service create \
  --resource-group $RG \
  --factory-name $ADF \
  --linked-service-name "ls_keyvault" \
  --properties '{
    "type": "AzureKeyVault",
    "typeProperties": {
      "baseUrl": "https://'"$KV"'.vault.azure.net/"
    }
  }'
```

**Verify:**
```bash
az datafactory linked-service show \
  --resource-group $RG \
  --factory-name $ADF \
  --linked-service-name "ls_keyvault" \
  --query "properties.type"
# Expected output: "AzureKeyVault"
```

---

## Linked Service 2 — VoltGrid REST API (`ls_voltgrid_api`)

### What it is
ADF's REST linked service lets Copy Activity call HTTP endpoints directly. The base URL is stored here — individual pipeline datasets append the specific endpoint path.

---

### UI Steps

1. Manage → Linked services → **+ New**
2. Search `REST` → select **REST** → **Continue**
3. Fill in:
   - **Name:** `ls_voltgrid_api`
   - **Base URL:** click **Azure Key Vault** radio button
     - Linked service: `ls_keyvault`
     - Secret name: `voltgrid-api-base-url`
   - **Authentication type:** Anonymous

   > Authentication is Anonymous here because we handle the token ourselves inside the pipeline (Web Activity → POST /api/auth/login/ → store token → attach as header). ADF's built-in auth does not support this token-rotation pattern.

4. Click **Test connection** → **Connection successful**
5. Click **Create**

---

### CLI Steps

```bash
# Get base URL from Key Vault to use in the definition
BASE_URL=$(az keyvault secret show \
  --vault-name $KV \
  --name "voltgrid-api-base-url" \
  --query "value" -o tsv)

az datafactory linked-service create \
  --resource-group $RG \
  --factory-name $ADF \
  --linked-service-name "ls_voltgrid_api" \
  --properties '{
    "type": "RestService",
    "typeProperties": {
      "url": "'"$BASE_URL"'",
      "enableServerCertificateValidation": true,
      "authenticationType": "Anonymous"
    }
  }'
```

---

## Linked Service 3 — Source Blob Storage (`ls_source_blob`)

### What it is
Connects to `dataenggdailystorage` — the instructor's storage account that holds the raw CSV source files. Uses a SAS token stored in Key Vault.

---

### UI Steps

1. Manage → Linked services → **+ New**
2. Search `Azure Blob Storage` → **Continue**
3. Fill in:
   - **Name:** `ls_source_blob`
   - **Authentication method:** SAS URI
   - **SAS URI:** leave blank for now — we will use Key Vault reference
   - Actually: select **Authentication method → SAS URI** then click the Key Vault icon next to the SAS URI field:
     - Linked service: `ls_keyvault`
     - Secret name: `source-sas-token`

   > If the UI does not show a Key Vault option for SAS URI, use the approach below: store the full SAS URI (including account URL) as a secret.

   **Alternative — full SAS URI secret:**

   First, store the full SAS URI in Key Vault:
   ```bash
   SAS_TOKEN=$(az keyvault secret show --vault-name $KV --name "source-sas-token" --query "value" -o tsv)
   FULL_SAS_URI="https://dataenggdailystorage.blob.core.windows.net/?$SAS_TOKEN"

   az keyvault secret set \
     --vault-name $KV \
     --name "source-blob-sas-uri" \
     --value "$FULL_SAS_URI"
   ```

   Then in ADF UI:
   - Authentication method: **SAS URI**
   - SAS URI → Key Vault → secret name: `source-blob-sas-uri`

4. Click **Test connection** → **Connection successful**
5. Click **Create**

---

### CLI Steps

```bash
SAS_TOKEN=$(az keyvault secret show \
  --vault-name $KV \
  --name "source-sas-token" \
  --query "value" -o tsv)

az datafactory linked-service create \
  --resource-group $RG \
  --factory-name $ADF \
  --linked-service-name "ls_source_blob" \
  --properties '{
    "type": "AzureBlobStorage",
    "typeProperties": {
      "sasUri": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "ls_keyvault",
          "type": "LinkedServiceReference"
        },
        "secretName": "source-blob-sas-uri"
      }
    }
  }'
```

---

## Linked Service 4 — ADLS Gen2 Bronze (`ls_adls_bronze`)

### What it is
Connects to your `evdatalakedev` storage account using the ADF Managed Identity. This is where all Bronze Delta data gets written.

**Why Managed Identity?** ADF's system-assigned Managed Identity was given `Storage Blob Data Contributor` on `evdatalakedev` in Day 1. No secret needed — Azure handles the token exchange automatically.

---

### UI Steps

1. Manage → Linked services → **+ New**
2. Search `Azure Data Lake Storage Gen2` → **Continue**
3. Fill in:
   - **Name:** `ls_adls_bronze`
   - **Authentication method:** System Assigned Managed Identity
   - **Azure subscription:** select yours
   - **Storage account name:** `evdatalakedev`
4. Click **Test connection** → **Connection successful**
5. Click **Create**

---

### CLI Steps

```bash
STORAGE_ACCOUNT=$(az keyvault secret show \
  --vault-name $KV \
  --name "adls-account-name" \
  --query "value" -o tsv)

az datafactory linked-service create \
  --resource-group $RG \
  --factory-name $ADF \
  --linked-service-name "ls_adls_bronze" \
  --properties '{
    "type": "AzureBlobFS",
    "typeProperties": {
      "url": "https://'"$STORAGE_ACCOUNT"'.dfs.core.windows.net/",
      "accountKey": null
    },
    "connectVia": {
      "referenceName": "AutoResolveIntegrationRuntime",
      "type": "IntegrationRuntimeReference"
    }
  }'
```

> Note: When `accountKey` is null and no credential block is set, ADF defaults to Managed Identity auth for ADLS Gen2.

---

## Verify All 4 Linked Services

### UI
Manage → Linked services → you should see all 4 listed. Click each → **Test connection** → all show green.

### CLI
```bash
az datafactory linked-service list \
  --resource-group $RG \
  --factory-name $ADF \
  --query "[].name" \
  --output table
```

**Expected output:**
```
Result
--------------------
ls_keyvault
ls_voltgrid_api
ls_source_blob
ls_adls_bronze
```

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `Access denied` on Key Vault test | ADF Managed Identity missing Key Vault access policy | Key Vault → Access policies → add ADF MI with Secret Get + List |
| `Connection failed` on REST test | `voltgrid-api-base-url` secret value has trailing slash or wrong URL | Check the secret value — should be `https://hostname` with no trailing slash |
| `AuthorizationPermissionMismatch` on ADLS test | ADF MI missing Storage Blob Data Contributor role | Storage account → IAM → add role assignment for ADF MI |
| SAS token test fails | SAS token expired or missing `r` + `l` permissions | Generate a new SAS token with `sp=rl` and update the secret in Key Vault |

---

## Next Step

→ `02_ADF_PIPELINE_API_PAYMENTS.md` — build the payments ingestion pipeline
