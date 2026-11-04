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
        ├── CI: validates ARM template for staging + production
        └── CD: deploys to staging → production (with approval)
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
   - Organisation: `https://dev.azure.com/<your-org>`
   - Project: `ev-intelligence`
   - Repository: `azure-ev-end-to-end-project`
   - Collaboration branch: `main`
   - Publish branch: `adf_publish`
   - Root folder: `/azure-end-to-end-project/adf`
3. Click Apply — ADF Studio now shows a Git icon on every save

---

### Step 2 — Create a Service Principal for DevOps

```bash
az ad sp create-for-rbac \
  --name "sp-ev-devops-cicd" \
  --role Contributor \
  --scopes \
    /subscriptions/<sub-id>/resourceGroups/rg-ev-intelligence-staging \
    /subscriptions/<sub-id>/resourceGroups/rg-ev-intelligence-prod
# → outputs appId, password, tenant — save all three
```

Grant Data Factory Contributor on both factories:
```bash
az role assignment create \
  --assignee <appId> \
  --role "Data Factory Contributor" \
  --scope /subscriptions/<sub-id>/resourceGroups/rg-ev-intelligence-staging/providers/Microsoft.DataFactory/factories/adf-ev-staging

az role assignment create \
  --assignee <appId> \
  --role "Data Factory Contributor" \
  --scope /subscriptions/<sub-id>/resourceGroups/rg-ev-intelligence-prod/providers/Microsoft.DataFactory/factories/adf-ev-prod
```

---

### Step 3 — Create Azure DevOps Service Connection

1. Azure DevOps → Project Settings → **Service connections** → New
2. Select **Azure Resource Manager** → Service principal (manual)
3. Fill in: Subscription ID, Subscription name, SP ID (`appId`), SP Key (`password`), Tenant ID
4. Name it: `sc-ev-azure` → Grant access to all pipelines → Save

---

### Step 4 — Create Variable Groups

Azure DevOps → Pipelines → Library → Variable Groups

**`vg-adf-staging`:**

| Variable | Value |
|---|---|
| `ADF_RESOURCE_GROUP` | `rg-ev-intelligence-staging` |
| `ADF_FACTORY_NAME` | `adf-ev-staging` |
| `ADLS_ACCOUNT_NAME` | `evdatalakedev2` |
| `KEY_VAULT_URL` | `https://kv-ev-intelligence-dev.vault.azure.net/` |
| `ENV_NAME` | `staging` |

**`vg-adf-production`:**

| Variable | Value |
|---|---|
| `ADF_RESOURCE_GROUP` | `rg-ev-intelligence-prod` |
| `ADF_FACTORY_NAME` | `adf-ev-prod` |
| `ADLS_ACCOUNT_NAME` | `evdatalakeprod` |
| `KEY_VAULT_URL` | `https://kv-ev-intelligence-prod.vault.azure.net/` |
| `ENV_NAME` | `production` |

---

### Step 5 — Create ADF Parameter Files

`adf/parameters/staging-parameters.json`:
```json
{
  "$schema": "https://schema.management.azure.com/schemas/2015-01-01/deploymentParameters.json#",
  "contentVersion": "1.0.0.0",
  "parameters": {
    "factoryName": { "value": "adf-ev-staging" },
    "ls_adls_ev_accountKey": { "value": "" },
    "ls_adls_ev_url": { "value": "https://evdatalakedev2.dfs.core.windows.net" },
    "ls_kv_ev_baseUrl": { "value": "https://kv-ev-intelligence-dev.vault.azure.net/" }
  }
}
```

`adf/parameters/production-parameters.json`:
```json
{
  "$schema": "https://schema.management.azure.com/schemas/2015-01-01/deploymentParameters.json#",
  "contentVersion": "1.0.0.0",
  "parameters": {
    "factoryName": { "value": "adf-ev-prod" },
    "ls_adls_ev_accountKey": { "value": "" },
    "ls_adls_ev_url": { "value": "https://evdatalakeprod.dfs.core.windows.net" },
    "ls_kv_ev_baseUrl": { "value": "https://kv-ev-intelligence-prod.vault.azure.net/" }
  }
}
```

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
2. ARM Validate (staging params) — `deploymentMode: Validation` — dry run, no changes
3. ARM Validate (production params) — same check with prod overrides
4. Publish artifact `adf-arm` — ARM template + parameter files + scripts

### `cd-pipeline.yml`

Triggered by: successful completion of `ADF-CI` pipeline (pipeline resource trigger)

**Stage 1 — Deploy Staging** (auto, no approval):
1. Download `adf-arm` artifact from CI
2. Stop all active ADF triggers (`PrePostDeploymentScript.ps1 -preDeployment $true`)
3. ARM deploy with `staging-parameters.json`
4. Restart triggers (`-preDeployment $false`)
5. Smoke test — verify `pl_master_e2e` exists in staging factory

**Stage 2 — Deploy Production** (requires manual approval via `adf-production` environment):
1. Download artifact
2. Stop triggers on production factory
3. ARM deploy with `production-parameters.json`
4. Restart triggers
5. Smoke test — verify `pl_master_e2e` in production
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
│  Job A: ARM Validate (staging params)    │  ← parallel
│  Job B: ARM Validate (production params) │  ← parallel
│  Job C: Publish adf-arm artifact         │  ← after A + B pass
└────────────────────┬─────────────────────┘
                     │ all pass
                     ▼
┌──────────────────────────────────────────┐
│  ADF-CD Pipeline                         │
│                                          │
│  Stage 1: Deploy Staging                 │
│    Stop triggers                         │
│    ARM deploy (staging-parameters.json)  │
│    Restart triggers                      │
│    Smoke test pl_master_e2e              │
│                 │                        │
│  Stage 2: Deploy Production              │
│    ⏸ Manual approval (adf-production)   │
│    ✅ Approved                           │
│    Stop triggers                         │
│    ARM deploy (production-parameters.json│
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
  --scopes /subscriptions/<sub-id>/resourceGroups/rg-ev-intelligence-staging

# Validate ARM template locally (dry run)
az deployment group validate \
  --resource-group rg-ev-intelligence-staging \
  --template-file ARMTemplateForFactory.json \
  --parameters @staging-parameters.json

# Deploy ARM template manually
az deployment group create \
  --resource-group rg-ev-intelligence-staging \
  --template-file ARMTemplateForFactory.json \
  --parameters @staging-parameters.json

# List Azure deployment history
az deployment group list \
  --resource-group rg-ev-intelligence-prod \
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
