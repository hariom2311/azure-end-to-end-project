# Day 15 — CI/CD for Azure Data Factory using Azure DevOps

**Duration:** 2 hours | **Goal:** Build a full CI/CD pipeline that validates and deploys ADF ARM templates through staging and production using Azure DevOps pipelines, environments, variable groups, and approval gates.

> **Project context:** ADF pipelines built across Days 4–12 — `pl_bronze_master_foreach`, `pl_silver_dynamic_foreach`, `pl_gold_dynamic_foreach`, `pl_master_e2e` — are now deployed via CI/CD instead of being published manually through the ADF Studio UI.

---

## What We Built

```
day15-adf-cicd/
├── azure-pipelines/
│   ├── ci-pipeline.yml   ← ARM validate (staging + production) → publish artifact
│   └── cd-pipeline.yml   ← deploy staging → smoke test → (approval) → deploy production
└── notes.md
```

---

## How ADF CI/CD Works

ADF does NOT version-control itself. You connect it to Git and use the Publish flow to generate ARM templates.

```
Developer edits pipelines in ADF Studio
        │
        │ (Save)    → commits to feature branch in Git
        │ (Publish) → ADF generates ARM templates
        │              commits them to adf_publish branch
        ▼
Azure DevOps detects push to adf_publish
        ├── CI: validates ARM template against dev parameters
        └── CD: deploys to dev → production (with approval)
```

---

## The `adf_publish` Branch

When you click **Publish** in ADF Studio, ADF auto-generates:
- `ARMTemplateForFactory.json` — all pipelines, datasets, linked services, triggers
- `ARMTemplateParametersForFactory.json` — environment-specific values (connection strings, URLs)

This branch is a **deployment artifact** — never edit it manually. The collaboration branch (`main`) is where developers save their work. `adf_publish` is only for CI/CD to read from.

---

## ARM Template Parameters

ADF separates pipeline logic from environment config via ARM parameters:

```
ARMTemplateForFactory.json          → same for all environments (the logic)
ARMTemplateParametersForFactory.json → different per environment (the config)
```

You maintain separate override files:
- `adf/parameters/staging-parameters.json`
- `adf/parameters/production-parameters.json`

Sensitive values (connection strings, keys) must reference **Azure Key Vault**, not be embedded in the parameter file.

---

## Azure DevOps vs GitHub Actions for ADF

| | Azure DevOps | GitHub Actions |
|---|---|---|
| Native Azure integration | Built-in service connections | Requires `azure/login` action |
| ADF deployment task | `AzureResourceManagerTemplateDeployment@3` | `azure/arm-deploy@v1` |
| Approval gates | Environment approvals in Pipelines | GitHub Environment reviewers |
| Environment config | Variable Groups (linkable to Key Vault) | Repository / environment secrets |
| Preferred for enterprise | Yes — tight Azure AD integration | Possible but more setup |

Azure DevOps is the standard choice for ADF CI/CD in enterprise Azure environments.

---

## Step-by-Step Setup

### Step 1 — Connect ADF to Git

1. ADF Studio → **Manage** → **Git configuration** → Configure
2. Fill in:
   - Repository type: **Azure DevOps Git**
   - Organisation: `https://dev.azure.com/hariomsuryawanshi682580706`
   - Project: `azureendtoendproject`
   - Repository: `azureendtoendproject`
   - Collaboration branch: `main`
   - Publish branch: `adf_publish`
   - Root folder: `/` (root — ADF files are at the repo root: `pipeline/`, `dataset/`, `linkedService/`, etc.)
3. Click Apply — ADF Studio now shows a Git icon on every save

After clicking **Publish**, ADF auto-generates and commits to `adf_publish`:
```
azureendtoendproject/          ← repo root
├── ARMTemplateForFactory.json
├── ARMTemplateParametersForFactory.json
├── pipeline/
├── dataset/
├── linkedService/
├── trigger/
└── factory/
```

---

### Step 2 — Create a Service Principal for DevOps

**Create the SP and assign Contributor on the resource group:**
```bash
az ad sp create-for-rbac \
  --name "sp-ev-devops-cicd" \
  --role Contributor \
  --scopes /subscriptions/81dd57e1-876a-4fcc-8778-e06f68c13228/resourceGroups/rg-ev-intelligence-dev
```

The command prints JSON once — the `password` field is never shown again:
```json
{
  "appId":       "41e64467-afbc-46ed-9402-836114706eab",
  "displayName": "sp-ev-devops-cicd",
  "password":    "<only visible at creation — store it immediately>",
  "tenant":      "c8fe40ce-7c95-4958-8992-21dfb0ea6c3c"
}
```

**Grant Data Factory Contributor on the factory:**
```bash
az role assignment create \
  --assignee 41e64467-afbc-46ed-9402-836114706eab \
  --role "Data Factory Contributor" \
  --scope /subscriptions/81dd57e1-876a-4fcc-8778-e06f68c13228/resourceGroups/rg-ev-intelligence-dev/providers/Microsoft.DataFactory/factories/adf-ev-intelligence-dev2
```

**Role assignments completed:**
- `Contributor` on `rg-ev-intelligence-dev` ✅
- `Data Factory Contributor` on `adf-ev-intelligence-dev2` ✅

> **Why two roles?** `Contributor` lets the SP deploy ARM templates to the resource group. `Data Factory Contributor` is the minimum permission required to start/stop ADF triggers and read pipeline definitions — without it the pre/post deploy script in Step 9 fails.

**Store the credentials in Key Vault so they are retrievable without being committed to Git:**
```bash
az keyvault secret set --vault-name kv-ev-intelligence-dev02 --name "sp-devops-app-id"    --value "41e64467-afbc-46ed-9402-836114706eab" --output none
az keyvault secret set --vault-name kv-ev-intelligence-dev02 --name "sp-devops-tenant-id" --value "c8fe40ce-7c95-4958-8992-21dfb0ea6c3c" --output none
az keyvault secret set --vault-name kv-ev-intelligence-dev02 --name "sp-devops-secret"    --value "<password from above>" --output none
```

**Retrieve the credentials when needed:**
```bash
az keyvault secret show --vault-name kv-ev-intelligence-dev02 --name "sp-devops-app-id"    --query value -o tsv
az keyvault secret show --vault-name kv-ev-intelligence-dev02 --name "sp-devops-tenant-id" --query value -o tsv
az keyvault secret show --vault-name kv-ev-intelligence-dev02 --name "sp-devops-secret"    --query value -o tsv
```

> If the password is lost, reset it with: `az ad sp credential reset --id 41e64467-afbc-46ed-9402-836114706eab`

---

### Step 3 — Create Azure DevOps Service Connection

Four values from Step 2 are needed for the Service Connection form:

| Field in DevOps form | Value |
|---|---|
| Subscription ID | `81dd57e1-876a-4fcc-8778-e06f68c13228` |
| Subscription name | `DataEngineeringDaily` |
| Service Principal (App) ID | `41e64467-afbc-46ed-9402-836114706eab` |
| Service Principal Key | `<secret from kv-ev-intelligence-dev02>` |
| Tenant ID | `c8fe40ce-7c95-4958-8992-21dfb0ea6c3c` |
| Service connection name | `sc-ev-azure` |

Steps:
1. Azure DevOps → Project Settings → **Service connections** → New service connection
2. Select **Azure Resource Manager** → Next
3. Select **Service principal (manual)** → Next
4. Fill in the table above → Verify connection (green tick = success)
5. Name it `sc-ev-azure` → check **Grant access permission to all pipelines** → Save

---

### Step 4 — Create Variable Groups

Azure DevOps → Pipelines → Library → Variable Groups

**`vg-adf-dev`:**

| Variable | Value |
|---|---|
| `ADF_RESOURCE_GROUP` | `rg-ev-intelligence-dev` |
| `ADF_FACTORY_NAME` | `adf-ev-intelligence-dev2` |
| `ADLS_ACCOUNT_NAME` | `evdatalakedev2` |
| `KEY_VAULT_URL` | `https://kv-ev-intelligence-dev02.vault.azure.net/` |
| `ENV_NAME` | `dev` |

> In a multi-environment setup (staging + production), create separate variable groups per environment. For this project all environments share `rg-ev-intelligence-dev` and `adf-ev-intelligence-dev2`.

---

### Step 5 — Create ADF Parameter Files

This file lives in the Git repo at:
```
azureendtoendproject/parameters/dev-parameters.json
```

The parameter names are generated by ADF when you click Publish. They are based on the actual linked services in the factory. For this project the linked services are:

| Linked Service | Type | Auth |
|---|---|---|
| `ls_adls_bronze` | Azure Data Lake Storage Gen2 | Managed Identity — no key needed |
| `ls_keyvault` | Azure Key Vault | Managed Identity |
| `ls_blob_source` | Azure Blob Storage | SAS URI (stored encrypted in ADF) |
| `ls_voltgrid_api` | REST | Anonymous |
| `ls_databricks` | Azure Databricks | Managed Identity |
| `ls_databricks_cluster` | Azure Databricks | PAT via Key Vault secret `databricks-pat-token` |

Because `ls_adls_bronze` and `ls_databricks` use **Managed Identity**, ADF does not parameterise their credentials — only the URLs are exposed as ARM parameters.

`azureendtoendproject/parameters/dev-parameters.json`:
```json
{
  "$schema": "https://schema.management.azure.com/schemas/2015-01-01/deploymentParameters.json#",
  "contentVersion": "1.0.0.0",
  "parameters": {
    "factoryName": {
      "value": "adf-ev-intelligence-dev2"
    },
    "ls_adls_bronze_properties_typeProperties_url": {
      "value": "https://evdatalakedev2.dfs.core.windows.net/"
    },
    "ls_keyvault_properties_typeProperties_baseUrl": {
      "value": "https://kv-ev-intelligence-dev02.vault.azure.net/"
    },
    "ls_voltgrid_api_properties_typeProperties_url": {
      "value": "https://ev-project-navy-mu.vercel.app"
    },
    "ls_databricks_properties_typeProperties_domain": {
      "value": "https://adb-7405607119523299.19.azuredatabricks.net"
    },
    "ls_databricks_properties_typeProperties_workspaceResourceId": {
      "value": "/subscriptions/81dd57e1-876a-4fcc-8778-e06f68c13228/resourceGroups/rg-ev-intelligence-dev/providers/Microsoft.Databricks/workspaces/dbw-ev-intelligence-dev-2"
    },
    "ls_databricks_cluster_properties_typeProperties_domain": {
      "value": "https://adb-7405605939978737.17.azuredatabricks.net"
    },
    "ls_databricks_cluster_properties_typeProperties_existingClusterId": {
      "value": "0629-122335-009orlij"
    },
    "ls_blob_source_properties_typeProperties_sasUri": {
      "value": "https://dataenggdailystorage.blob.core.windows.net/"
    }
  }
}
```

> **Important:** The exact parameter key names (e.g. `ls_adls_bronze_properties_typeProperties_url`) are generated by ADF when you click Publish. After Step 1, open `ARMTemplateParametersForFactory.json` from the `adf_publish` branch and copy the exact keys from there — the names above are based on the actual linked services in this project but may differ slightly from what ADF generates.

> `ls_blob_source` uses an encrypted SAS credential stored in ADF itself — the `sasUri` above is the storage account base URL only. The actual SAS token is managed by ADF and does not appear in the parameter file.

---

### Step 6 — Create Environments with Approval Gates

Azure DevOps → Pipelines → Environments:

**`adf-staging`** — no approvals (deploys automatically)

**`adf-production`**:
- Approvals and checks → Approvals → add yourself as approver
- Optional: Branch control → Allowed branches: `refs/heads/adf_publish`
- Optional: Business hours check (Mon–Fri 9am–5pm only)

---

### Step 7 — Register the CI Pipeline

1. Azure DevOps → Pipelines → New Pipeline
2. Code location: **Azure Repos Git** (or GitHub)
3. Existing YAML file: `/azure-end-to-end-project/day15-adf-cicd/azure-pipelines/ci-pipeline.yml`
4. Name it: `ADF-CI` → Save (do not run yet)

---

### Step 8 — Register the CD Pipeline

1. Azure DevOps → Pipelines → New Pipeline → same repo
2. Existing YAML: `/azure-end-to-end-project/day15-adf-cicd/azure-pipelines/cd-pipeline.yml`
3. Name it: `ADF-CD` → Save

---

### Step 9 — Download the Pre/Post Deploy Script

Before deploying an ARM template, all ADF triggers must be stopped. After deploy, they restart. Without this the deploy fails if a trigger fires mid-deploy.

Download the official Microsoft script and place it at `adf/scripts/PrePostDeploymentScript.ps1`:
```
https://raw.githubusercontent.com/Azure/Azure-DataFactory/main/SamplesV2/ContinuousIntegrationAndDelivery/PrePostDeploymentScript.ps1
```

This script is referenced in `cd-pipeline.yml` for both staging and production stages.

---

### Step 10 — Trigger the First Deployment

1. Edit any ADF pipeline in Studio (e.g. add a description)
2. Save → commits to `main` in Git
3. Click **Publish** in ADF Studio
4. ADF auto-commits ARM templates to `adf_publish` branch
5. Azure DevOps `ADF-CI` pipeline triggers automatically
6. If CI passes, `ADF-CD` deploys to staging automatically
7. CD pauses before production — you receive an email/Teams approval notification
8. Azure DevOps → Pipelines → the pending run → Review → Approve
9. Production deploys and release is git-tagged

---

## What the Pipeline Files Do

### `ci-pipeline.yml`

Triggered on: push to `adf_publish` + PRs to `main`

1. Checkout `adf_publish` branch
2. ARM Validate (dev params) — `deploymentMode: Validation` — dry run, no changes
3. Publish artifact `adf-arm` — ARM template + `dev-parameters.json` + scripts

### `cd-pipeline.yml`

Triggered by: successful completion of `ADF-CI` pipeline (pipeline resource trigger)

**Stage 1 — Deploy Dev** (auto, no approval):
1. Download `adf-arm` artifact from CI
2. Stop all active ADF triggers (`PrePostDeploymentScript.ps1 -preDeployment $true`)
3. ARM deploy to `rg-ev-intelligence-dev` / `adf-ev-intelligence-dev2` with `dev-parameters.json`
4. Restart triggers (`-preDeployment $false`)
5. Smoke test — verify `pl_master_e2e` exists in `adf-ev-intelligence-dev2`

**Stage 2 — Deploy Production** (requires manual approval via `adf-production` environment):
1. Download artifact
2. Stop triggers on factory
3. ARM deploy with `dev-parameters.json`
4. Restart triggers
5. Smoke test — verify `pl_master_e2e`
6. Git tag the release: `adf-release-YYYYMMDD-HHmmss-<sha8>`

---

## Full CI/CD Flow Diagram

```
Developer edits ADF pipeline in Studio
        │
        ▼
  Save → commits to main in Git
        │
        ▼
  Publish → ADF generates ARM → commits to adf_publish
        │
        ▼
┌──────────────────────────────────────────┐
│  ADF-CI Pipeline                         │
│  Job A: ARM Validate (dev-parameters)    │
│  Job B: Publish adf-arm artifact         │  ← after A passes
└────────────────────┬─────────────────────┘
                     │ passes
                     ▼
┌──────────────────────────────────────────┐
│  ADF-CD Pipeline                         │
│                                          │
│  Stage 1: Deploy Dev (auto)              │
│    RG     : rg-ev-intelligence-dev       │
│    Factory: adf-ev-intelligence-dev2     │
│    Stop triggers                         │
│    ARM deploy (dev-parameters.json)      │
│    Restart triggers                      │
│    Smoke test pl_master_e2e              │
│                 │                        │
│  Stage 2: Deploy Production              │
│    ⏸ Manual approval (adf-production)   │
│    ✅ Approved                           │
│    Stop triggers                         │
│    ARM deploy (dev-parameters.json)      │
│    Restart triggers                      │
│    Smoke test pl_master_e2e              │
│    Git tag adf-release-...               │
└──────────────────────────────────────────┘
```

---

## Key Commands Reference

```bash
# Create service principal
az ad sp create-for-rbac \
  --name "sp-ev-devops-cicd" \
  --role Contributor \
  --scopes /subscriptions/81dd57e1-876a-4fcc-8778-e06f68c13228/resourceGroups/rg-ev-intelligence-dev

# Validate ARM template locally (dry run)
az deployment group validate \
  --resource-group rg-ev-intelligence-dev \
  --template-file ARMTemplateForFactory.json \
  --parameters @dev-parameters.json

# Deploy ARM template manually
az deployment group create \
  --resource-group rg-ev-intelligence-dev \
  --template-file ARMTemplateForFactory.json \
  --parameters @dev-parameters.json

# List Azure deployment history
az deployment group list \
  --resource-group rg-ev-intelligence-dev \
  --query "[].{name:name, time:properties.timestamp, state:properties.provisioningState}" \
  --output table
```

```powershell
# Stop all ADF triggers (before deploy)
$triggers = Get-AzDataFactoryV2Trigger -ResourceGroupName $rg -DataFactoryName $adf
$triggers | ForEach-Object { Stop-AzDataFactoryV2Trigger -ResourceGroupName $rg -DataFactoryName $adf -Name $_.Name -Force }

# Restart all ADF triggers (after deploy)
$triggers | ForEach-Object { Start-AzDataFactoryV2Trigger -ResourceGroupName $rg -DataFactoryName $adf -Name $_.Name }

# Manually trigger a pipeline run and wait
$run = Invoke-AzDataFactoryV2Pipeline -ResourceGroupName $rg -DataFactoryName $adf -PipelineName "pl_master_e2e"
do {
    Start-Sleep -Seconds 30
    $status = Get-AzDataFactoryV2PipelineRun -ResourceGroupName $rg -DataFactoryName $adf -PipelineRunId $run.RunId
} while ($status.Status -in @("InProgress", "Queued"))
Write-Host "Final status: $($status.Status)"
```
