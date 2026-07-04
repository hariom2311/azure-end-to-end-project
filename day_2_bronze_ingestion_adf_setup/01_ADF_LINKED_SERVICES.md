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

> **If test fails:** ADF Managed Identity is missing the `Key Vault Secrets User` role on `kv-ev-intelligence-dev`. Go back to Day 2 Part 3 and assign it, wait 2 minutes, then re-test.

---

### CLI Steps

> **CMD / PowerShell users:** The `\` line continuation below is bash syntax and will break in CMD/PowerShell. Use the single-line version to copy-paste directly.

**Single line (CMD / PowerShell — copy-paste this):**
```cmd
az datafactory linked-service create --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --linked-service-name "ls_keyvault" --properties "{\"type\": \"AzureKeyVault\", \"typeProperties\": {\"baseUrl\": \"https://kv-ev-intelligence-dev.vault.azure.net/\"}}"
```

**Multi-line (bash / Git Bash only):**
```bash
az datafactory linked-service create \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
  --linked-service-name "ls_keyvault" \
  --properties '{
    "type": "AzureKeyVault",
    "typeProperties": {
      "baseUrl": "https://kv-ev-intelligence-dev.vault.azure.net/"
    }
  }'
```

**Verify (CMD / PowerShell):**
```cmd
az datafactory linked-service show --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --linked-service-name "ls_keyvault" --query "properties.type" -o tsv
```

Expected output: `AzureKeyVault`

---

## Linked Service 2 — VoltGrid REST API (`ls_voltgrid_api`)

### What it is
ADF's REST linked service lets Copy Activity call HTTP endpoints directly. The base URL is stored here — individual pipeline datasets append the specific endpoint path and query parameters.

**Why Anonymous auth?**
Authentication is Anonymous at the linked service level because we handle the VoltGrid token ourselves inside the pipeline: Web Activity calls `POST /api/auth/login/` → stores the token in a pipeline variable → Copy Activity attaches it as an `Authorization: Token` header. ADF's built-in auth does not support this token-rotation pattern.

**Why not use Key Vault for the Base URL here?**
This is a known ADF limitation: when Authentication type is set to **Anonymous**, the Base URL field does not show a Key Vault reference option — it only accepts plain text. Key Vault references for Base URL are only available with certain auth types (Basic, Service Principal).

This is fine for the VoltGrid API URL because the base URL (`https://ev-project-navy-mu.vercel.app`) is not a secret — it is a public endpoint. Only the username, password, and runtime token are sensitive. Those never touch the linked service — they travel only through pipeline variables set by the Web Activity login call.

---

### UI Steps

1. Manage → Linked services → **+ New**
2. Search `REST` → select **REST** → **Continue**
3. Fill in:
   - **Name:** `ls_voltgrid_api`
   - **Base URL:** paste directly → `https://ev-project-navy-mu.vercel.app`
   - **Authentication type:** Anonymous
   - **Server certificate validation:** Enable
4. Click **Test connection** → **Connection successful**
5. Click **Create**

---

### CLI Steps

First, read the base URL from Key Vault so you can embed it in the linked service definition.

**Step 1 — Read the base URL (CMD / PowerShell):**
```cmd
az keyvault secret show --vault-name kv-ev-intelligence-dev --name "voltgrid-api-base-url" --query "value" -o tsv
```
Copy the output (e.g. `https://ev-project-navy-mu.vercel.app`) — use it in Step 2.

**Step 2 — Create the linked service (CMD / PowerShell — replace the URL with your output from Step 1):**
```cmd
az datafactory linked-service create --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --linked-service-name "ls_voltgrid_api" --properties "{\"type\": \"RestService\", \"typeProperties\": {\"url\": \"https://ev-project-navy-mu.vercel.app\", \"enableServerCertificateValidation\": true, \"authenticationType\": \"Anonymous\"}}"
```

**Multi-line (bash / Git Bash only):**
```bash
BASE_URL=$(az keyvault secret show \
  --vault-name kv-ev-intelligence-dev \
  --name "voltgrid-api-base-url" \
  --query "value" -o tsv)

az datafactory linked-service create \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
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
Connects to `dataenggdailystorage` — the instructor's storage account that holds the raw CSV source files. ADF needs a **SAS URI** to authenticate — not just the SAS token.

---

### SAS Token vs SAS URI — What is the difference?

You received a **SAS token** during the session. It looks like this:
```
se=2027-07-30&sp=rl&spr=https&sv=2026-04-06&sr=c&sig=EeNl1mQL9JN...
```

That is just the query string — a list of parameters that prove access. It has no host address in it.

ADF's **SAS URI** field expects the **full URL** — the storage account host address plus a `?` plus the SAS token:

```
https://dataenggdailystorage.blob.core.windows.net/?se=2027-07-30&sp=rl&spr=https&sv=2026-04-06&sr=c&sig=EeNl1mQL9JN...
```

Breaking it down:

| Part | Value | What it is |
|---|---|---|
| `https://dataenggdailystorage.blob.core.windows.net/` | fixed | The storage account's public endpoint. `dataenggdailystorage` is the account name. `.blob.core.windows.net` is the Azure Blob Storage domain. |
| `?` | fixed | Separates the URL from the query parameters |
| `se=2027-07-30` | from SAS token | Expiry date of this token |
| `sp=rl` | from SAS token | Permissions: `r` = read, `l` = list |
| `sig=...` | from SAS token | Cryptographic signature — proves the token is genuine |

So the rule is simple:
```
SAS URI = https://dataenggdailystorage.blob.core.windows.net/  +  ?  +  <your SAS token>
```

You build this once, store it in Key Vault as `source-blob-sas-uri`, and ADF reads it at runtime.

---

### Step 0 — Build and store the full SAS URI in Key Vault (do this before UI or CLI steps)

> **CMD / PowerShell users:** The `\` line continuation below is bash syntax and will break in CMD/PowerShell. Use the single-line version to copy-paste directly.

**CMD / PowerShell — Step 1, get the SAS token you stored in Day 1:**
```cmd
az keyvault secret show --vault-name kv-ev-intelligence-dev --name "source-sas-token" --query "value" -o tsv
```
Copy the output. It starts with `se=...` — no leading `?`, no quotes.

**CMD / PowerShell — Step 2, store the full SAS URI (replace `<paste-sas-token-here>` with the value from Step 1):**
```cmd
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "source-blob-sas-uri" --value "https://dataenggdailystorage.blob.core.windows.net/?<paste-sas-token-here>"
```

Example of what the final value looks like:
```
https://dataenggdailystorage.blob.core.windows.net/?se=2027-07-30&sp=rl&spr=https&sv=2026-04-06&sr=c&sig=EeNl1mQL9JN...
```

**Multi-line (bash / Git Bash only — both steps combined):**
```bash
SAS_TOKEN=$(az keyvault secret show \
  --vault-name kv-ev-intelligence-dev \
  --name "source-sas-token" \
  --query "value" -o tsv)

az keyvault secret set \
  --vault-name kv-ev-intelligence-dev \
  --name "source-blob-sas-uri" \
  --value "https://dataenggdailystorage.blob.core.windows.net/?$SAS_TOKEN"
```

**Verify the secret was stored correctly (CMD / PowerShell):**
```cmd
az keyvault secret show --vault-name kv-ev-intelligence-dev --name "source-blob-sas-uri" --query "value" -o tsv
```
The output should start with `https://dataenggdailystorage.blob.core.windows.net/?se=...`

---

### UI Steps

1. Manage → Linked services → **+ New**
2. Search `Azure Blob Storage` → **Continue**
3. Fill in:
   - **Name:** `ls_source_blob`
   - **Authentication method:** SAS URI
   - **SAS URI** field → click the **Azure Key Vault** icon next to the field (small KV icon on the right side of the text box):
     - **AKV linked service:** `ls_keyvault`
     - **Secret name:** `source-blob-sas-uri`
     - **Secret version:** leave blank (always uses latest)
   - Click **OK**
4. Click **Test connection** — must show **Connection successful**
5. Click **Create**

> **If Test connection fails with 403:** The SAS URI was not built correctly. Common mistakes:
> - Missing `?` between the host URL and the token (`...windows.net/se=...` instead of `...windows.net/?se=...`)
> - Extra space or newline at the start or end of the token when pasting
> - SAS token has expired (`se=` date is in the past)
> Re-run Step 0 carefully and update the secret.

---

### CLI Steps

**Single line (CMD / PowerShell):**
```cmd
az datafactory linked-service create --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --linked-service-name "ls_source_blob" --properties "{\"type\": \"AzureBlobStorage\", \"typeProperties\": {\"sasUri\": {\"type\": \"AzureKeyVaultSecret\", \"store\": {\"referenceName\": \"ls_keyvault\", \"type\": \"LinkedServiceReference\"}, \"secretName\": \"source-blob-sas-uri\"}}}"
```

**Multi-line (bash / Git Bash only):**
```bash
az datafactory linked-service create \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
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
Connects to your `evdatalakedev` storage account using the ADF Managed Identity. This is where all Bronze Delta data gets written by Copy Activities.

**Why Managed Identity?**
ADF's system-assigned Managed Identity was granted `Storage Blob Data Contributor` on `evdatalakedev` in Day 2 Part 2. No secret is needed — Azure handles the token exchange automatically at runtime. This is the most secure approach: no credential to rotate, no expiry to track.

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

**Single line (CMD / PowerShell):**
```cmd
az datafactory linked-service create --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --linked-service-name "ls_adls_bronze" --properties "{\"type\": \"AzureBlobFS\", \"typeProperties\": {\"url\": \"https://evdatalakedev.dfs.core.windows.net/\"}}"
```

**Multi-line (bash / Git Bash only):**
```bash
az datafactory linked-service create \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
  --linked-service-name "ls_adls_bronze" \
  --properties '{
    "type": "AzureBlobFS",
    "typeProperties": {
      "url": "https://evdatalakedev.dfs.core.windows.net/"
    }
  }'
```

> When no `credential` or `accountKey` block is present, ADF uses its System Assigned Managed Identity automatically for `AzureBlobFS` type.

---

## Verify All 4 Linked Services

### UI
Manage → Linked services → you should see all 4 listed. Click each → **Test connection** → all show green.

### CLI

**Single line (CMD / PowerShell):**
```cmd
az datafactory linked-service list --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --query "[].name" -o table
```

**Multi-line (bash / Git Bash only):**
```bash
az datafactory linked-service list \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
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
| `Access denied` on Key Vault test | ADF Managed Identity missing `Key Vault Secrets User` role | Day 2 Part 3 — assign the role, wait 2 min, re-test |
| `Connection failed` on REST linked service test | `voltgrid-api-base-url` secret has trailing slash or wrong value | Check the secret — should be `https://hostname` with no trailing slash |
| `AuthorizationPermissionMismatch` on ADLS test | ADF MI missing `Storage Blob Data Contributor` on `evdatalakedev` | Day 2 Part 2 — assign the role, wait 2 min, re-test |
| SAS URI test fails | SAS token expired or missing `r` + `l` permissions | Regenerate SAS token with `sp=rl`, update `source-blob-sas-uri` secret in Key Vault |
| `ls_source_blob` test shows `403 Forbidden` | Full SAS URI not built correctly (missing `?` between host and token) | Re-run Step 0 — ensure value is `https://dataenggdailystorage.blob.core.windows.net/?se=...` |

---

## Next Step

→ `02_ADF_PIPELINE_API_PAYMENTS.md` — build the payments ingestion pipeline
