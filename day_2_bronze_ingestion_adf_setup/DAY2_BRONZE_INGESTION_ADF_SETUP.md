# Day 2 — Bronze Layer Ingestion: ADF Setup + API and Blob Sources
**Session:** ~2.5 hours | **Goal:** Provision Azure Data Factory, connect it to VoltGrid API and source blob storage, and load one API endpoint and one blob source into the Bronze Delta layer.

> **Region for all resources: Central India (centralindia)** — cheapest India region with full service availability.
> **Prerequisite:** All Day 1 resources must exist before starting Day 2.

---

## Glossary — What Is Each New Azure Service?

Read this once before you start. These are the new services added on Day 2.

| Term | Plain English Definition |
|---|---|
| **Azure Data Factory (ADF)** | A fully managed, no-code/low-code pipeline orchestration tool. You define what data to move (source), where to put it (sink), and when (trigger). ADF handles the scheduling, retry, and monitoring. In this project it calls the VoltGrid API, paginates through thousands of pages, and writes results to ADLS Gen2. |
| **ADF Instance / Factory** | The top-level ADF resource inside your Resource Group. Think of it as the "account" that holds all your pipelines, datasets, linked services, and triggers. |
| **ADF Studio** | The web UI for building ADF pipelines. Accessed by clicking "Launch Studio" from the ADF resource in the Portal. No install needed — everything is browser-based. |
| **Linked Service** | A saved connection definition inside ADF. It holds the connection information (URL, auth method, credentials reference) for one external system. Pipelines never store credentials — they reference a linked service by name. |
| **Dataset** | A pointer to a specific table, file path, or API endpoint that a linked service can reach. A dataset says "within the VoltGrid API linked service, I want the `/api/db/payments/` endpoint". |
| **Pipeline** | A workflow of activities (Copy, Web call, ForEach loop, etc.) that runs in sequence or parallel. In this project: Web Activity (login) → Set Variable (store token) → Until loop (paginate pages) → Copy Activity (write page to Delta). |
| **Copy Activity** | An ADF activity that reads from a source dataset and writes to a sink dataset. It is the workhorse of ADF — it does the actual data movement. |
| **Web Activity** | An ADF activity that makes an HTTP request and captures the response. Used here to call `POST /api/auth/login/` and extract the token from the JSON response. |
| **Set Variable Activity** | Stores a value into a pipeline variable so later activities can use it. Used here to store the token after login and to store the watermark value. |
| **Until Activity** | A loop that keeps running its inner activities until a condition becomes True. Used here to paginate: loop while `current_page <= total_pages`. |
| **Trigger** | A schedule or event that automatically starts a pipeline. ADF has: Schedule Trigger (cron-style), Tumbling Window Trigger (fixed intervals with backfill), Event Trigger (fires when a file arrives). |
| **Managed Identity (MI)** | An automatically provisioned identity for an Azure service. ADF gets one for free — no credentials to manage. You assign it RBAC roles just like a Service Principal. Used for ADF to write to ADLS Gen2 without a password. |
| **Delta Lake / Delta Table** | An open-source storage layer on top of Parquet that adds ACID transactions, time travel, and schema enforcement. Bronze tables use Delta so Silver can do MERGE (upsert) later. |
| **Watermark** | The `max(updated_at)` value from the last run stored in a `pipeline_audit` table. Next run reads this value and adds `?updated_after=<watermark>` to the API URL — this is how incremental load works. |
| **Full Load vs Incremental Load** | Full load: download all records. Incremental load: download only records updated since the last run. First run is always full; subsequent runs are incremental using the watermark. |

---

## What You Will Have at the End of Day 2

- Azure Data Factory instance `adf-ev-intelligence-dev` provisioned and connected to Key Vault
- ADF Managed Identity has write access to ADLS Gen2
- 4 ADF linked services: Key Vault, VoltGrid API (REST), Source Blob (SAS), ADLS Gen2 (Managed Identity)
- ADF pipeline `pl_bronze_api_payments`: full load on first run, incremental on re-runs
- ADF pipeline `pl_bronze_blob_sessions`: reads current hour's CSV, writes to Bronze Delta partitioned by hour
- Bronze Delta tables in ADLS: `bronze/api/payments/` and `bronze/blob/iot_sessions/`
- Databricks internal Delta tables: `bronze.payments` and `bronze.charging_sessions`
- Pipeline audit row written after every API run: `bronze/api/pipeline_audit/`

---

## Architecture for Today

```
VoltGrid API
  POST /api/auth/login/  →  token (pipeline variable, memory only)
  GET  /api/db/payments/ →  paginated JSON  →  ADF Copy Activity (Until loop)
                                                    ↓
                                         Bronze Delta Table
                               abfss://bronze@evdatalakedev.../api/payments/
                                                    ↓
                                    External Delta Table: bronze.payments
                                    Internal Delta Table: bronze.payments_internal

Source Blob (dataenggdailystorage)
  wasbs://source@.../realtime/charging_sessions/YYYY/MM/DD/HH/*.csv
                                                    ↓
                                         ADF Copy Activity
                                                    ↓
                                         Bronze Delta Table
                               abfss://bronze@evdatalakedev.../blob/iot_sessions/
                                                    ↓
                                    External Delta Table: bronze.charging_sessions
                                    Internal Delta Table: bronze.charging_sessions_internal
```

---

## Load Strategy

| Source | First Run | Subsequent Runs |
|---|---|---|
| API (payments) | Full load — no date filter, all pages | Incremental — `?updated_after={max(updated_at) from last run}` |
| Blob (charging_sessions) | All files for today's date | Files for current UTC hour only |

**Watermark for API:** After each run, `max(updated_at)` from the loaded records is written to `bronze/api/pipeline_audit/` as a Delta row. Next run reads this table, takes the max value, and appends it as `?updated_after=<value>` to the API request URL.

**Partitioning for Blob:** Source files sit at `charging_sessions/YYYY/MM/DD/HH/`. ADF reads the current hour's folder. Delta table is partitioned by `ingestion_date` and `ingestion_hour` columns.

---

## Reading Order for Day 2

1. This file — provision ADF and grant permissions (Parts 1–3)
2. `01_ADF_LINKED_SERVICES.md` — create the 4 linked services
3. `02_ADF_PIPELINE_API_PAYMENTS.md` — build the payments pipeline
4. `03_ADF_PIPELINE_BLOB_SESSIONS.md` — build the blob pipeline
5. `04_DATABRICKS_BRONZE_TABLES.md` — create internal Delta tables in Databricks

---

## Part 1 — Create Azure Data Factory Instance (15 min)

> **Cost: ~₹0–8 for this project**
> ADF pricing has two components:
> - **Pipeline Orchestration Activity runs:** 1 unit = 1 activity execution. First 1,000 runs/month are **free**.
> - **Data Integration Unit (DIU) hours:** Charged only when Copy Activity actually moves data. Small test runs cost fractions of a rupee.
> - **For the 18-day course total:** Estimated ~₹20–40 across all pipeline test runs.
>
> **Minimum cost config to select:**
> - No integration runtime to change — the default **AutoResolveIntegrationRuntime** is free
> - Do not create a Self-hosted Integration Runtime (for on-premises) — not needed here

**What is Azure Data Factory?**
ADF is a cloud ETL/ELT orchestration service. You define pipelines (workflows), and ADF runs them on a schedule or on demand. It handles authentication, retry logic, monitoring, and alerting. In this project ADF is responsible for the Bronze ingestion layer — pulling data from external sources and landing it in ADLS Gen2 as Delta tables.

**What is an ADF Instance (Factory)?**
One ADF instance = one factory. It is the container for all your pipelines, linked services, datasets, and triggers. You get one factory per environment (dev/prod). We create one factory for the whole 18-day course.

### 1.1 Via Azure Portal

1. Go to [https://portal.azure.com](https://portal.azure.com)
2. In the top search bar, search **Data factories** and click it
3. Click **+ Create**
4. Fill in the **Basics** tab:
   - **Subscription:** your subscription
   - **Resource group:** `rg-ev-intelligence-dev`
   - **Name:** `adf-ev-intelligence-dev` *(must be globally unique)*
   - **Region:** `Central India`
   - **Version:** `V2` ← always V2, V1 is retired
5. Click **Git configuration** tab:
   - Select **Configure Git later** ← keep it simple for training; Git integration is optional
6. Click **Review + Create** → **Create**
7. Wait ~1 minute for deployment to complete
8. Click **Go to resource**

> **If the name is taken:** Add your initials — `adf-ev-intelligence-dev-hs`. The name must be globally unique across all Azure customers.

### 1.2 Launch ADF Studio

Once the resource is deployed:
1. On the ADF resource page, click **Launch studio**
2. This opens `https://adf.azure.com` — the ADF authoring UI
3. Bookmark this URL — you will return here for the rest of Day 2

> **ADF Studio is a separate web app.** The Azure Portal shows you the resource metadata (name, region, pricing). ADF Studio is where you build pipelines. Both show the same factory — just different views.

### 1.3 Via CLI

> **CMD / PowerShell users:** The `\` line continuation below is bash syntax and will break in CMD/PowerShell. Use the single-line version to copy-paste directly.

**Single line (CMD / PowerShell — copy-paste this):**
```cmd
az datafactory create --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --location centralindia
```

**Multi-line (bash / Git Bash only):**
```bash
az datafactory create \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
  --location centralindia
```

**Verify it was created:**
```cmd
az datafactory show --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --query "{Name:name, Location:location, State:provisioningState}" -o table
```

Expected output:
```
Name                         Location       State
---------------------------  -------------  ---------
adf-ev-intelligence-dev      centralindia   Succeeded
```

---

## Part 2 — Grant ADF Managed Identity Access to ADLS Gen2 (10 min)

> **Cost: ₹0** — RBAC role assignments are free.

**What is a Managed Identity?**
When you create an ADF instance, Azure automatically creates a Managed Identity for it — like a service account that Azure manages completely. It has a unique Object ID. You never see or manage a password for it. You assign it RBAC roles, and ADF can then access resources (like ADLS Gen2) using that identity at runtime.

**Why this step is required:**
ADF's Copy Activity will write Delta files to your `evdatalakedev` storage account. Without this role assignment, every Copy Activity will fail with `403 Forbidden` even if your pipelines are configured correctly.

**What role to assign:**

| Role | What it allows | What it blocks |
|---|---|---|
| `Storage Blob Data Reader` | Read only | Cannot write — Copy sink will fail |
| `Storage Blob Data Contributor` | Read + write + delete files | Cannot delete the storage account |
| `Storage Blob Data Owner` | Full control including ACLs | Over-permissioned — avoid |

Use `Storage Blob Data Contributor` — enough for ADF to write Bronze files.

### 2.1 Via Azure Portal

**Step 1 — Find the ADF Managed Identity Object ID:**
1. Portal → search **Data factories** → click `adf-ev-intelligence-dev`
2. In the left menu, click **Properties** (under Settings)
3. Copy the **Managed Identity Object ID** — looks like `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`

**Step 2 — Assign the role on ADLS Gen2:**
1. Portal → search **Storage accounts** → click `evdatalakedev`
2. Left menu → **Access Control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. **Role** tab: search `Storage Blob Data Contributor` → select → **Next**
5. **Members** tab:
   - **Assign access to:** `Managed identity`
   - Click **+ Select members**
   - **Managed identity:** select `Data factory (V2)` from the dropdown
   - Select `adf-ev-intelligence-dev`
   - Click **Select**
6. Click **Review + assign** → **Review + assign** again to confirm

**Verify:**
1. On the same `evdatalakedev` → **Access Control (IAM)** page
2. Click **Role assignments** tab
3. You should see `adf-ev-intelligence-dev` under `Storage Blob Data Contributor`

### 2.2 Via CLI

> **CMD / PowerShell users:** Run each step separately and copy the output before moving to the next.

**Step 1 — Get the ADF Managed Identity Object ID:**
```cmd
az datafactory show --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --query identity.principalId -o tsv
```
Copy the output — this is your `MI_OID`.

**Step 2 — Get the Storage Account resource ID:**
```cmd
az storage account show --name evdatalakedev --resource-group rg-ev-intelligence-dev --query id -o tsv
```
Copy the output — this is your `STORAGE_ID`.

**Step 3 — Assign the role:**
```cmd
az role assignment create --assignee-object-id <MI_OID from Step 1> --assignee-principal-type ServicePrincipal --role "Storage Blob Data Contributor" --scope <STORAGE_ID from Step 2>
```

**Step 4 — Verify:**
```cmd
az role assignment list --scope <STORAGE_ID from Step 2> --query "[].{Role:roleDefinitionName, Principal:principalName}" -o table
```

**Multi-line (bash / Git Bash only):**
```bash
MI_OID=$(az datafactory show \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
  --query identity.principalId -o tsv)

STORAGE_ID=$(az storage account show \
  --name evdatalakedev \
  --resource-group rg-ev-intelligence-dev \
  --query id -o tsv)

az role assignment create \
  --assignee-object-id $MI_OID \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ID

az role assignment list --scope $STORAGE_ID --query "[].{Role:roleDefinitionName, Principal:principalName}" -o table
```

---

## Part 3 — Grant ADF Managed Identity Access to Key Vault (10 min)

> **Cost: ₹0** — RBAC role assignments are free.

**Why this is needed:**
ADF will use Key Vault-backed secrets in its linked services (e.g. the VoltGrid API URL, SAS token). ADF reads these secrets at pipeline runtime using its Managed Identity. Without the `Key Vault Secrets User` role, ADF cannot retrieve any secret and every pipeline that references Key Vault will fail.

**Role to assign:** `Key Vault Secrets User` — read-only access to secrets. ADF never needs to create or modify secrets.

### 3.1 Via Azure Portal

1. Portal → **Key vaults** → `kv-ev-intelligence-dev` → left menu **Access Control (IAM)**
2. Click **+ Add** → **Add role assignment**
3. **Role** tab: search `Key Vault Secrets User` → select → **Next**
4. **Members** tab:
   - **Assign access to:** `Managed identity`
   - Click **+ Select members**
   - **Managed identity:** select `Data factory (V2)` → select `adf-ev-intelligence-dev`
   - Click **Select**
5. Click **Review + assign** → **Review + assign**
6. Wait **1–2 minutes** before testing any ADF pipeline that references Key Vault

### 3.2 Via CLI

**Step 1 — Get the ADF Managed Identity Object ID** (same as Part 2 Step 1 — skip if you already have it):
```cmd
az datafactory show --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --query identity.principalId -o tsv
```

**Step 2 — Get the Key Vault resource ID:**
```cmd
az keyvault show --name kv-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query id -o tsv
```

**Step 3 — Assign the role:**
```cmd
az role assignment create --assignee-object-id <MI_OID from Step 1> --assignee-principal-type ServicePrincipal --role "Key Vault Secrets User" --scope <KV_ID from Step 2>
```

**Multi-line (bash / Git Bash only):**
```bash
MI_OID=$(az datafactory show \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
  --query identity.principalId -o tsv)

KV_ID=$(az keyvault show \
  --name kv-ev-intelligence-dev \
  --resource-group rg-ev-intelligence-dev \
  --query id -o tsv)

az role assignment create \
  --assignee-object-id $MI_OID \
  --assignee-principal-type ServicePrincipal \
  --role "Key Vault Secrets User" \
  --scope $KV_ID
```

---

## Part 4 — Add Day 2 Secrets to Key Vault (5 min)

> **Cost: ~₹0** — a few extra secret reads per day is negligible.

ADF linked services will read these secrets at runtime. Add them to Key Vault before building the linked services.

### 4.1 Secrets required for Day 2

| Secret Name | Value | What it is |
|---|---|---|
| `source-storage-account` | `dataenggdailystorage` | Source blob storage account name |
| `source-container` | `source` | Source container name |
| `source-sas-token` | `se=2027-...&sig=...` | SAS token for read access to source blob — provided during session |

> **Note:** `voltgrid-api-base-url`, `voltgrid-username`, `voltgrid-password`, `adls-account-name`, `sp-client-id`, `sp-client-secret`, `sp-tenant-id` were all added in Day 1. If any are missing, add them now.

### 4.2 Via Portal

1. Portal → **Key vaults** → `kv-ev-intelligence-dev` → **Secrets** → **+ Generate/Import**
2. For each row in the table above:
   - **Name:** exact secret name from the table
   - **Secret value:** the value from the table
   - Click **Create**

### 4.3 Via CLI

**Single line (CMD / PowerShell — run each line separately):**
```cmd
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "source-storage-account" --value "dataenggdailystorage"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "source-container" --value "source"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "source-sas-token" --value "<SAS token provided during session>"
```

**Multi-line (bash / Git Bash only):**
```bash
KV="kv-ev-intelligence-dev"
az keyvault secret set --vault-name $KV --name "source-storage-account" --value "dataenggdailystorage"
az keyvault secret set --vault-name $KV --name "source-container"        --value "source"
az keyvault secret set --vault-name $KV --name "source-sas-token"        --value "<SAS token provided during session>"
```

---

## Part 5 — Configure Linked Services and Pipelines

Now that ADF is provisioned and all permissions are in place, follow these 4 files in order:

| File | What it covers |
|---|---|
| `01_ADF_LINKED_SERVICES.md` | Create 4 linked services: Key Vault, VoltGrid API, Source Blob, ADLS Gen2 |
| `02_ADF_PIPELINE_API_PAYMENTS.md` | Build the payments API pipeline with full + incremental load logic |
| `03_ADF_PIPELINE_BLOB_SESSIONS.md` | Build the charging_sessions blob pipeline with hourly partitioning |
| `04_DATABRICKS_BRONZE_TABLES.md` | Create internal Delta tables in Databricks mirroring the ADLS data |

---

## Part 6 — Databricks Notebooks

After running the ADF pipelines, use these notebooks to inspect and register the Bronze data:

| Notebook | What it does |
|---|---|
| `notebooks/03_bronze_api_payments.ipynb` | Read payments Delta from ADLS, register as external + internal table, write audit log |
| `notebooks/04_bronze_blob_sessions.ipynb` | Read sessions Delta from ADLS, register as external + internal table |

See `notebooks/README.md` for import instructions.

---

## Day 2 Cost Summary

| Resource | Cost |
|---|---|
| ADF instance (provisioning) | ₹0 |
| ADF pipeline runs (~5 test runs, ~20 activities total) | ~₹2–5 |
| Copy Activity DIU hours (small JSON + CSV) | ~₹3–5 |
| Databricks cluster (2 hours) | ~₹40–45 |
| ADLS Gen2 writes (Bronze Delta files) | ~₹1 |
| Key Vault secret reads (~50 reads) | ~₹0 |
| **Day 2 total** | **~₹46–56** |

> **Free tier note:** ADF gives you 1,000 free orchestration activity runs per month. For 5 pipeline test runs × ~6 activities each = 30 runs total — well within the free tier.

---

## End of Session — STOP THE CLUSTER

**Do this every single time before closing your laptop:**

1. Databricks → left menu **Compute**
2. Click your cluster `dev-cluster`
3. Click **Terminate**
4. Wait for status to show **Terminated**

ADF itself has no "stop" — it only runs when triggered. Pipelines are not running between sessions.

---

## Day 2 Checklist

### ADF Provisioning
- [ ] ADF instance `adf-ev-intelligence-dev` created in `rg-ev-intelligence-dev` (Central India)
- [ ] ADF Studio opens at `https://adf.azure.com`
- [ ] ADF Managed Identity Object ID noted
- [ ] `Storage Blob Data Contributor` role assigned to ADF Managed Identity on `evdatalakedev`
- [ ] `Key Vault Secrets User` role assigned to ADF Managed Identity on `kv-ev-intelligence-dev`
- [ ] Day 2 secrets added to Key Vault: `source-storage-account`, `source-container`, `source-sas-token`

### ADF Linked Services
- [ ] Linked service `ls_keyvault` created and tested — connects to `kv-ev-intelligence-dev`
- [ ] Linked service `ls_voltgrid_api` created and tested — connects to VoltGrid API
- [ ] Linked service `ls_source_blob` created and tested — connects to `dataenggdailystorage` via SAS
- [ ] Linked service `ls_adls_bronze` created and tested — connects to `evdatalakedev` via Managed Identity

### Payments API Pipeline
- [ ] Dataset `ds_voltgrid_payments_src` created (parameterised URL)
- [ ] Pipeline `pl_bronze_api_payments` created with: Web Activity (login) → Set Variable (token + watermark) → Until loop → Copy Activity → Notebook (audit)
- [ ] Full load run completed — rows visible in `abfss://bronze@evdatalakedev.../api/payments/`
- [ ] Incremental run completed — fewer rows than full load, audit table updated

### Blob Sessions Pipeline
- [ ] Dataset `ds_charging_sessions_src` created (parameterised path with date/hour)
- [ ] Pipeline `pl_bronze_blob_sessions` created
- [ ] Hourly trigger `tr_blob_sessions_hourly` created and started
- [ ] Manual run completed — rows visible in `abfss://bronze@evdatalakedev.../blob/iot_sessions/`

### Databricks Tables
- [ ] Notebook `03_bronze_api_payments.ipynb` ran — external table `bronze.payments` queryable
- [ ] Notebook `04_bronze_blob_sessions.ipynb` ran — external table `bronze.charging_sessions` queryable
- [ ] Internal tables `bronze.payments_internal` and `bronze.charging_sessions_internal` created (for comparison)
- [ ] Row counts match between ADLS files and registered Delta tables
- [ ] **Cluster terminated at end of session**

---

## Common Errors on Day 2

| Error | Cause | Fix |
|---|---|---|
| `403 Forbidden` when ADF runs Copy Activity to ADLS | ADF Managed Identity missing `Storage Blob Data Contributor` role on `evdatalakedev` | Part 2 — assign the role, wait 2 min, retry |
| `Access denied` when ADF reads a Key Vault secret | ADF Managed Identity missing `Key Vault Secrets User` role on Key Vault | Part 3 — assign the role, wait 2 min, retry |
| `Secret not found` in ADF linked service test | Secret name typo, or secret not created yet | Part 4 — add the secret, exact name match, retry linked service Test Connection |
| ADF pipeline runs but writes 0 rows | VoltGrid API returned 0 records (token expired or wrong endpoint) | Re-check linked service URL, re-run pipeline |
| `DELTA_ILLEGAL_OPERATION` on Delta write | Attempting to overwrite a Delta table with a different schema | Add `mergeSchema` option or drop and recreate the Delta path |
| `abfss` path error from Databricks notebook | SP OAuth config not set in this session | Re-run Cell 1 and Cell 2 of the notebook (sets SP OAuth for this session) |
| `Secret scope not found: kv-ev-scope` | Secret scope was not created in Day 1, or cluster was restarted | Day 1 Part 6.5 — recreate the secret scope. The scope itself persists; only the Spark config needs re-running per session. |
| ADF Studio shows blank after opening | Browser cache issue | Hard refresh: Ctrl + Shift + R. Or open in Incognito. |
| `ResourceNotFound` on `az datafactory` CLI commands | ADF not registered as a resource provider | `az provider register --namespace Microsoft.DataFactory` then wait 1–2 min |
