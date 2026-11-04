# Day 15 — Practice Exercises: ADF CI/CD with Azure DevOps

> Complete these exercises in order. Each builds on the previous one.
> All exercises use the ADF instance and Azure DevOps project set up in the notes.

---

## Exercise 1 — Add a New Linked Service and Deploy Through CI/CD

**Goal:** Make a real change in ADF Studio, push it through the full CI/CD pipeline, and verify it lands in staging.

Steps:
1. Open ADF Studio → Manage → Linked services → New → Azure Blob Storage
2. Name it `ls_blob_ev_reports`, connect to your storage account using Key Vault secret reference
3. Click Save — this commits the change to `main` in Git
4. Click **Publish** — ADF generates updated ARM templates and commits to `adf_publish`
5. Go to Azure DevOps → Pipelines — watch `ADF-CI` trigger automatically
6. Verify CI passes (both ARM Validate jobs green)
7. Watch `ADF-CD` start and deploy to staging automatically
8. Open ADF Studio (staging factory) → Manage → Linked services → confirm `ls_blob_ev_reports` is there

**Verify:** The linked service in staging ADF matches what you created in Studio.

---

## Exercise 2 — Add a Branch Control Check to Production

**Goal:** Ensure the production environment can only be deployed from the `adf_publish` branch — not triggered manually from any other branch.

Steps:
1. Azure DevOps → Pipelines → Environments → `adf-production`
2. Approvals and checks → + Add check → **Branch control**
3. Allowed branches: `refs/heads/adf_publish`
4. Protect branch: enabled → Save
5. Test it: manually trigger `ADF-CD` from the Azure DevOps UI, targeting a different branch
6. Observe: the production stage should be blocked with a branch control violation

**Verify:** The production stage fails the branch control check. The staging stage still succeeds.

---

## Exercise 3 — Add a Smoke Test After Staging Deploy

**Goal:** After the staging ARM deploy, call the ADF API to programmatically verify that `pl_master_e2e` exists in the staging factory.

Add this task to `cd-pipeline.yml` after the `Restart ADF triggers (post-deploy)` step in Stage 1:

```yaml
- task: AzurePowerShell@5
  displayName: Smoke test — verify pl_master_e2e exists in staging
  inputs:
    azureSubscription: sc-ev-azure
    ScriptType: InlineScript
    Inline: |
      $pipeline = Get-AzDataFactoryV2Pipeline `
        -ResourceGroupName "$(ADF_RESOURCE_GROUP)" `
        -DataFactoryName "$(ADF_FACTORY_NAME)" `
        -Name "pl_master_e2e" `
        -ErrorAction SilentlyContinue
      if (-not $pipeline) {
        Write-Error "pl_master_e2e not found in $(ADF_FACTORY_NAME) — deploy may have failed"
        exit 1
      }
      Write-Host "Smoke test passed: pl_master_e2e exists in $(ADF_FACTORY_NAME)"
    azurePowerShellVersion: LatestVersion
```

Commit and push — watch the CD pipeline run this new step.

**Verify:** The smoke test step appears in the pipeline run log and prints the success message.

---

## Exercise 4 — Trigger a Pipeline Run and Wait for It

**Goal:** After staging deploy, automatically trigger `pl_master_e2e` on staging and wait for it to complete before the CD pipeline moves on.

Add this task after the smoke test step:

```yaml
- task: AzurePowerShell@5
  displayName: Trigger pl_master_e2e on staging and wait
  inputs:
    azureSubscription: sc-ev-azure
    ScriptType: InlineScript
    Inline: |
      $run = Invoke-AzDataFactoryV2Pipeline `
        -ResourceGroupName "$(ADF_RESOURCE_GROUP)" `
        -DataFactoryName "$(ADF_FACTORY_NAME)" `
        -PipelineName "pl_master_e2e"
      Write-Host "Run ID: $($run.RunId)"
      do {
        Start-Sleep -Seconds 30
        $status = Get-AzDataFactoryV2PipelineRun `
          -ResourceGroupName "$(ADF_RESOURCE_GROUP)" `
          -DataFactoryName "$(ADF_FACTORY_NAME)" `
          -PipelineRunId $run.RunId
        Write-Host "Status: $($status.Status)"
      } while ($status.Status -in @("InProgress", "Queued"))
      if ($status.Status -ne "Succeeded") {
        Write-Error "pl_master_e2e FAILED on staging: $($status.Message)"
        exit 1
      }
      Write-Host "pl_master_e2e succeeded on staging"
    azurePowerShellVersion: LatestVersion
```

**Verify:** The pipeline run completes with `Succeeded` in the log. If it fails, the CD pipeline stops before production is approved.

---

## Exercise 5 — Set Up Key Vault Integration for Linked Service Secrets

**Goal:** Move the ADLS connection string out of the ARM parameter file and into Azure Key Vault so no secret ever touches the pipeline YAML or parameter files.

Steps:
1. Azure Key Vault → Secrets → Generate/Import → Name: `adls-ev-connection-string`, Value: your ADLS connection string
2. ADF Studio → Manage → Linked services → open `ls_adls_ev`
3. Change authentication to **Azure Key Vault** → select your Key Vault linked service → secret name: `adls-ev-connection-string`
4. Save + Publish → ARM template now stores a Key Vault reference, not the actual value
5. In Azure DevOps, ensure the deploying service principal has the `Key Vault Secrets User` role:
```bash
az role assignment create \
  --assignee <sp-appId> \
  --role "Key Vault Secrets User" \
  --scope /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/<kv-name>
```
6. Trigger a new publish → watch CI/CD run → verify staging ADF can connect to ADLS via the KV reference

**Verify:** ADF Monitor → staging factory → run `pl_master_e2e` → Bronze activities succeed → no authentication errors.
