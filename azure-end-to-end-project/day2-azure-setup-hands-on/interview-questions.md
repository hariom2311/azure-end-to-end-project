# Day 2 — Interview Questions: Azure Setup + ADF Bronze Ingestion

> 25 questions covering Resource Group, ADLS Gen2, Key Vault RBAC, ADF Managed Identity, access control, linked services, datasets, pipeline activities, and Bronze layer design.
> Attempt first, then check `interview-solutions.md`.

---

## Azure Resources and ADLS Gen2

**Q1 (Warm-up)**
What is the difference between Azure Blob Storage and ADLS Gen2? What single setting turns a Blob Storage account into ADLS Gen2, and why does this project require it?

---

**Q2 (Conceptual)**
This project disables storage account key access (`allowSharedKeyAccess = false`). What does this enforce? What happens if a developer tries to use `az storage blob list --account-key <key>` after this setting is applied?

---

**Q3 (Scenario)**
A data engineer creates an ADLS Gen2 storage account but forgets to enable hierarchical namespace. They try to connect Databricks with `abfss://bronze@evdatalakedev.dfs.core.windows.net/`. What specific error will they see, and how do they fix it?

---

**Q4 (Conceptual)**
The bronze container is created with `--auth-mode login` in the CLI. What does this mean? Why not use `--account-key` or `--sas-token` for the container create command when account key access is disabled?

---

**Q5 (Tricky)**
A new team member asks: "Why do we have 4 containers — bronze, silver, gold, source? Can't we just use folders inside one container?" Explain why separate containers are the correct design choice.

---

## Key Vault — RBAC Model

**Q6 (Warm-up)**
The Key Vault in this project uses the **RBAC permission model**. What is the alternative (legacy) model, and what is the key advantage of RBAC over the legacy model?

---

**Q7 (Conceptual)**
A data engineer creates a Key Vault with RBAC model and immediately tries `az keyvault secret set`. They get `Forbidden: ForbiddenByRbac`. What did they miss and what is the exact fix?

---

**Q8 (Scenario)**
The ADF pipeline needs to read the `voltgrid-username` secret. The ADF Managed Identity is assigned `Key Vault Secrets Officer` instead of `Key Vault Secrets User`. Does the pipeline still work? What is the problem with using Secrets Officer for ADF?

---

**Q9 (Conceptual)**
What is the difference between `Key Vault Secrets Officer` and `Key Vault Secrets User`? Map each role to the correct identity in this project and explain why.

---

**Q10 (Tricky)**
A data engineer stores the VoltGrid API password directly in the ADF pipeline's Web Activity body as a hardcoded string instead of reading it from Key Vault. Name 3 specific risks this creates.

---

## ADF Managed Identity and Access Control

**Q11 (Warm-up)**
What is a System-assigned Managed Identity in ADF? How is it different from a Service Principal, and why does this project use it for ADF instead of creating a separate SP?

---

**Q12 (Conceptual)**
ADF needs to write JSON files to the `bronze/` container. The ADF MI is assigned `Storage Blob Data Owner` instead of `Storage Blob Data Contributor`. What extra capability does Owner add that is not needed? What principle does this violate?

---

**Q13 (Scenario)**
The ADF linked service `ls_adls_bronze` test connection returns green. But when the pipeline runs, `act_copy_payments` fails with `403 AuthorizationPermissionMismatch` on the sink. The role assignments look correct in the Portal. What is the most likely cause?

---

**Q14 (Conceptual)**
In ADF, how does the `ls_keyvault` linked service authenticate to Key Vault? Write the exact chain: what identity is used, how it gets a token, and what REST call it makes to read a secret.

---

**Q15 (Tricky)**
The ADF Web Activity `act_get_username` uses Authentication = `System Assigned Managed Identity` and Resource = `https://vault.azure.net`. What would happen if you set Resource to `https://storage.azure.com` by mistake? Would the request succeed or fail?

---

## ADF Pipeline Design

**Q16 (Warm-up)**
In the pipeline `pl_bronze_api_payments`, why do `act_get_username` and `act_get_password` run in parallel instead of sequentially? What is the dependency condition that both must satisfy before `act_api_login` runs?

---

**Q17 (Conceptual)**
The `act_api_login` Web Activity uses this body expression:
```
@concat('{"username":"', activity('act_get_username').output.value, '","password":"', activity('act_get_password').output.value, '"}')
```
What does `.output.value` refer to here? What JSON does the Key Vault REST API return that makes `.value` the correct field?

---

**Q18 (Scenario)**
The `act_api_login` activity succeeds and returns a token. But `act_copy_payments` fails with `401 Unauthorized`. The pipeline variable `v_token` was set using `@activity('act_api_login').output.token`. What output key should actually be used — how do you verify this in ADF Monitor?

---

**Q19 (Conceptual)**
The `ls_voltgrid_api` linked service uses `Authentication type = Anonymous`. Does this mean the API connection is unauthenticated? If not, explain what Anonymous means here and where the actual authentication happens in the pipeline.

---

**Q20 (Tricky)**
In the sink dataset `ds_bronze_payments_sink`, the file pattern is set to `setOfObjects`. What does this mean for the output JSON format? What would the file look like if `arrayOfObjects` was chosen instead?

---

## Bronze Layer Design

**Q21 (Warm-up)**
The payment data lands at `bronze/api/payments/raw/payments.json`. Why is the path structured as `bronze/<source-type>/<entity>/raw/<file>` and not just `bronze/payments.json`?

---

**Q22 (Conceptual)**
The Bronze file written by ADF today does not contain audit columns like `_ingestion_ts`, `_source_file`, or `_pipeline_run_id`. Does this mean the Bronze design is incomplete? When and by what process are these columns added?

---

**Q23 (Scenario)**
A data engineer runs the pipeline twice. The second run overwrites `payments.json` with the same 100 records. Is this a problem for the Bronze layer? What would Day 3 do differently to prevent data loss on re-runs?

---

**Q24 (Conceptual)**
Why is the `p_page` and `p_page_size` parameterized at both the pipeline and dataset level? Could we just hardcode `page=1&page_size=100` in the linked service Base URL instead?

---

**Q25 (System design)**
You are asked to extend this pipeline to also ingest from the `/api/db/sessions/` endpoint and land the data at `bronze/api/sessions/raw/sessions.json`. Describe all the ADF changes needed:
- Which existing components can be reused?
- What new components must be created?
- How would you avoid duplicating the Key Vault Web Activities and token logic?

---
