# Day 15 — Interview Solutions: ADF CI/CD with Azure DevOps

---

## ARM Templates and the adf_publish Branch

**A1**
The `adf_publish` branch is automatically created and maintained by ADF Studio when you click **Publish**. ADF generates the ARM templates and commits them directly to this branch — no human writes to it.

You must never merge `adf_publish` into `main` because:
- `adf_publish` contains auto-generated JSON that reflects the current state of the ADF workspace, not hand-authored code
- Merging it would pollute `main` with machine-generated files that conflict on every publish
- It is a deployment artifact, not source code — the two branches have completely different purposes

`main` = developer collaboration branch (what engineers edit)
`adf_publish` = deployment branch (what Azure DevOps reads to deploy)

---

**A2**
Two files are generated in `adf_publish`:

1. **`ARMTemplateForFactory.json`** — the full factory definition. Contains all pipelines, datasets, linked services, triggers, data flows, and integration runtimes as Azure Resource Manager JSON. This file is the same for all environments — it contains the logic, not the config.

2. **`ARMTemplateParametersForFactory.json`** — the default parameter values. Contains environment-specific values (factory name, connection strings, URLs). You override this per environment using separate parameter files (`staging-parameters.json`, `production-parameters.json`) at deploy time.

---

**A3**
Because ADF Studio's Git integration has two separate actions:

- **Save** — commits the pipeline JSON to the collaboration branch (`main`) in Git. This is just saving work in progress — it does NOT generate ARM templates.
- **Publish** — validates all changes, generates the ARM templates, and commits them to `adf_publish`. Only published changes get deployed.

If you Save but never Publish, the `adf_publish` branch still contains the old ARM template. The CI/CD pipeline deploys from `adf_publish`, not from `main`. The developer's change is in Git but not in `adf_publish`, so it is invisible to the pipeline.

---

**A4**
`deploymentMode: Validation` tells Azure Resource Manager to validate the template without creating or modifying any resources. It is a dry run.

**What it catches:**
- Invalid JSON syntax in the ARM template
- Missing required parameters (e.g. a factory name parameter not provided in the override file)
- Invalid resource references (e.g. a linked service referenced by a dataset that does not exist in the template)
- Invalid resource types or API versions

**What it does NOT catch:**
- Connectivity issues — a linked service with a wrong connection string passes validation but fails at runtime
- Permission issues — the deploying SP may lack rights on a Key Vault; validation does not check this
- Logic errors — a pipeline that will loop infinitely or a wrong column mapping passes ARM validation

---

## Triggers, Deployments, and Rollback

**A5**
ADF triggers (scheduled, tumbling window, event-based) are resources within the factory. When you deploy an ARM template, Azure Resource Manager updates all resources defined in the template — including triggers. If a trigger is in the `Started` state, ARM cannot modify it and the deployment fails with an error like:

```
The resource Microsoft.DataFactory/factories/.../triggers/... cannot be updated because it is currently running.
```

The Microsoft pre/post deploy script (`PrePostDeploymentScript.ps1`) solves this by:
1. Reading all triggers from the factory before deploy
2. Stopping every trigger that is in the `Started` state
3. Storing their names so they can be restarted after deploy
4. Deploying the ARM template (triggers are now stopped, so ARM can update them)
5. Restarting the same triggers that were running before

Without this script, deploying during business hours (when scheduled triggers are active) will consistently fail.

---

**A6**
ARM deployments are incremental by default — they update resources that exist and add new ones, but they are not transactional. A partial failure leaves some resources updated and some at the old version.

Recovery steps:

1. **Find the last successful deployment** in Azure deployment history:
```bash
az deployment group list \
  --resource-group rg-ev-intelligence-prod \
  --query "[?properties.provisioningState=='Succeeded'].{name:name, time:properties.timestamp}" \
  --output table
```

2. **Redeploy the last good ARM template.** In this project, every successful production deploy creates a git tag (`adf-release-YYYYMMDD-...`). Check out that commit and redeploy:
```bash
git checkout adf-release-20261102-120000-abc12345
az deployment group create \
  --resource-group rg-ev-intelligence-prod \
  --template-file ARMTemplateForFactory.json \
  --parameters @production-parameters.json
```

3. **Stop and restart triggers** after the re-deploy so they resume on schedule.

4. **Investigate the failure** — check Azure Activity Log and the failed deployment's error details before attempting a new forward deploy.

---

**A7**
Blue/Green deployment uses two parallel ADF factories instead of updating one factory in place:

- **Blue** (`adf-ev-prod-blue`) — currently live; triggers are running; users / downstream systems point to this
- **Green** (`adf-ev-prod-green`) — receives the new ARM template deployment

Steps:
1. Deploy new ARM to green factory
2. Run smoke tests against green (verify all pipelines exist, trigger a test run)
3. If green passes: stop triggers on blue, start triggers on green → green is now live
4. If green fails: blue is still running untouched → zero impact on users

**Problem it solves that in-place ARM deploy cannot:**
- In-place deploy modifies the live factory directly — if it fails halfway, the factory is in a broken state while it is still supposed to be serving workloads
- Blue/Green keeps the live factory completely untouched until green is verified — rollback is instant (restart blue triggers, stop green triggers)

**Drawback:** Double the ADF compute cost while both factories exist, and any pipelines that write to shared storage (ADLS) need idempotent writes to avoid data duplication if both factories run simultaneously during the cutover.

---

## Azure DevOps Concepts

**A8**
A Service Connection is a named credential store in Azure DevOps that holds authentication details for an external system (in this case, an Azure subscription via a service principal). Pipeline YAML references it by name:

```yaml
azureSubscription: sc-ev-azure
```

Azure DevOps injects the actual credentials (app ID, secret, tenant ID) at runtime without exposing them in the YAML file.

Why not put credentials in YAML directly?
- YAML files are committed to Git — anyone with repo access would see the credentials
- Rotating credentials would require editing the YAML in every pipeline that uses them
- Service connections support managed identity and workload identity federation — no secret needed at all
- Service connections have audit logs and can be scoped to specific pipelines

---

**A9**
**Variable Group** — a named collection of key-value pairs managed in Azure DevOps Library. Shared across multiple pipelines. Can be linked directly to an Azure Key Vault so values are fetched from KV at runtime. Used for environment-specific config that multiple pipelines need (e.g. resource group names, factory names).

**Pipeline variable** — a string value defined inline in the pipeline YAML or set dynamically at runtime with `##vso[task.setvariable]`. Scoped to a single pipeline run. Used for values that are computed during the run (e.g. a release tag, a run ID).

For ADF CI/CD:
- Variable Groups (`vg-adf-staging`, `vg-adf-production`) store `ADF_FACTORY_NAME`, `ADF_RESOURCE_GROUP` — these are environment config, shared across CI and CD pipelines
- Pipeline variables store `deploymentName: adf-cd-staging-$(Build.BuildId)` — computed per run

---

**A10**
All three requirements are configured on the `adf-production` **Environment** in Azure DevOps → Pipelines → Environments → `adf-production` → Approvals and checks:

1. **Branch control** — Add check → Branch control → Allowed branches: `refs/heads/adf_publish`. Any attempt to deploy from another branch is blocked before the approval gate is even reached.

2. **Two approvers** — Add check → Approvals → add two approvers → set "Required approvers": 2 (or set both as separate required approvers). The deployment waits until both have approved.

3. **Business hours** — Add check → Business hours → Days: Monday–Friday, Time: 09:00–17:00, Timezone: your local timezone. The deployment is queued outside these hours and only proceeds during the allowed window.

All three checks must pass before the deployment job starts.

---

## Environment Parameterisation

**A11**
Three environments means three sets of parameters but one set of pipeline logic.

Approach:
1. Create three ARM parameter files: `dev-parameters.json`, `staging-parameters.json`, `production-parameters.json` — each with the correct factory name, ADLS URL, Key Vault URL for that environment
2. Create three Variable Groups: `vg-adf-dev`, `vg-adf-staging`, `vg-adf-production`
3. Add a third stage to `cd-pipeline.yml`:

```yaml
stages:
  - stage: DeployDev
    variables: [ group: vg-adf-dev ]
    # deploys automatically, no approval
  - stage: DeployStaging
    dependsOn: DeployDev
    variables: [ group: vg-adf-staging ]
    # deploys automatically after dev
  - stage: DeployProduction
    dependsOn: DeployStaging
    environment: adf-production   # approval gate here
    variables: [ group: vg-adf-production ]
```

The ARM template (`ARMTemplateForFactory.json`) is identical for all three stages — only the parameter file changes. ADF pipeline logic is never duplicated.

---

**A12**
The correct solution is to store the connection string in Azure Key Vault and have ADF reference it via a Key Vault linked service — so the connection string never appears in any file, parameter, or pipeline variable.

**Changes needed:**

**In ADF Studio:**
1. Create a linked service for Key Vault: `ls_kv_ev` → points to the Key Vault URL
2. Edit the linked service that uses the connection string (e.g. `ls_adls_ev`) → change auth method to **Azure Key Vault** → select `ls_kv_ev` → secret name: `adls-ev-connection-string`
3. Save + Publish → ARM template now contains a Key Vault reference object, not the literal connection string

**In Azure Key Vault:**
- Add secret: `adls-ev-connection-string` = the actual connection string value
- Grant the ADF managed identity `Key Vault Secrets User` role on the KV

**In the ARM parameter files:**
- Remove `ls_adls_ev_accountKey` parameter entirely — it is no longer in the template
- The Key Vault URL parameter (`ls_kv_ev_baseUrl`) stays — this is non-sensitive

**In Azure DevOps:**
- Grant the deploying service principal `Key Vault Secrets User` on the KV so it can resolve references during deployment validation
- No secrets are stored in Variable Groups or pipeline YAML for the connection string
