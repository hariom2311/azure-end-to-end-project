# Day 2 — Interview Solutions: Azure Setup + ADF Bronze Ingestion

---

## Azure Resources and ADLS Gen2

**A1**
Azure Blob Storage is a flat object store — "folders" are simulated with `/` in blob names, no real directory hierarchy. ADLS Gen2 is Blob Storage with **hierarchical namespace (HNS) enabled** — this creates true directories with atomic renames, per-directory ACLs, and better Spark performance.

The single setting that converts a Blob Storage account to ADLS Gen2: **Enable hierarchical namespace = ON** (set at creation time, cannot be changed after). This project requires ADLS Gen2 because:
- `abfss://` protocol (required for SP OAuth Spark access) only works with HNS-enabled accounts
- Directory-level operations in Medallion Architecture (OPTIMIZE, VACUUM, MERGE) are faster with true hierarchical directories
- Per-directory RBAC and POSIX ACLs are available for fine-grained access control

---

**A2**
Setting `allowSharedKeyAccess = false` on the storage account enforces that **all access must go through Azure Active Directory (RBAC)**. The storage account keys become non-functional — they still exist but requests authenticated with them are rejected with `SharedKeyDisabled`.

If a developer runs:
```bash
az storage blob list --account-name evdatalakedev --account-key <key>
```
They will get: `AuthorizationFailure: This request is not authorized to perform this operation using this permission.`

The correct command uses `--auth-mode login` (Azure AD identity) or `--auth-mode key` is blocked. This is the modern standard — it removes the risk of account key leakage, which would give anyone full read/write/delete access to all containers with no audit trail.

---

**A3**
Without hierarchical namespace, the account is plain Blob Storage. When Databricks tries `abfss://bronze@evdatalakedev.dfs.core.windows.net/`, it connects via the ABFS (Azure Blob File System) connector which requires ADLS Gen2. The error will be something like:
```
StatusCode=403, ErrorCode=FilesystemNotFound — The specified filesystem does not exist.
```
Or the connector may fail to resolve the DFS endpoint entirely.

**Fix:** HNS cannot be enabled retroactively on an existing account. The engineer must:
1. Create a new storage account with HNS enabled
2. Re-create the containers
3. Delete the old account

This is why enabling HNS at creation time is critical — it is not fixable after the fact.

---

**A4**
`--auth-mode login` tells the Azure CLI to authenticate the container-create request using your **Azure AD user token** (obtained via `az login`). It does not use the storage account key at all.

When account key access is disabled, `--auth-mode key` and `--account-key` are rejected at the storage service level. `--auth-mode login` is the only viable option because it uses the same IAM (RBAC) path. The user running the CLI command must have `Storage Blob Data Contributor` (or higher) on the storage account for this to succeed.

---

**A5**
Separate containers are the correct design for several reasons:

| Concern | One container (folder-based) | Separate containers |
|---|---|---|
| Access control | RBAC applies at storage account level only | RBAC can be scoped to individual containers — give an analyst read on `gold/` only |
| Lifecycle policies | Hard to apply different retention per layer | Different lifecycle rules per container — bronze to Cool after 30 days, gold stays Hot |
| Blast radius | A misconfigured pipeline that deletes from the wrong path could touch all layers | A pipeline with access only to `bronze` container cannot touch `silver` |
| Soft delete | Applies per container | Can enable separately per container |

In this project: ADF only has `Storage Blob Data Contributor` on the whole account — in production you would scope it to just the `bronze` container so ADF cannot accidentally overwrite Silver or Gold.

---

## Key Vault — RBAC Model

**A6**
The legacy model is **Vault access policy** — it has its own permission system separate from Azure IAM. Each Key Vault had an independent access policy list managed inside the Key Vault resource itself.

The RBAC model's key advantages over Vault access policies:
- **Unified IAM:** Key Vault permissions appear in the same IAM blade as all other Azure resources — one place to audit who has access to what
- **Standard Azure RBAC roles:** Same `az role assignment` commands, same Conditional Access, same PIM (Privileged Identity Management)
- **Inheritance:** Roles can be assigned at subscription or resource group level and inherit down to Key Vaults
- **Audit logs:** All access appears in Azure Activity Log alongside other resource actions
- **Policy cannot be set per secret:** Vault access policy grants access to all secrets in the vault; RBAC can scope a role to a specific secret via its resource ID

---

**A7**
With the RBAC permission model, **creating a resource does not grant you data-plane access**. The control-plane action (creating the Key Vault) and the data-plane actions (reading/writing secrets) are governed separately. The engineer must explicitly assign themselves `Key Vault Secrets Officer` via IAM.

Exact fix:
1. Portal → `kv-ev-intelligence-dev` → **Access Control (IAM)** → **+ Add** → **Add role assignment**
2. Role: `Key Vault Secrets Officer`
3. Members: your user account
4. **Review + assign**
5. Wait 1–2 minutes for propagation
6. Retry `az keyvault secret set`

---

**A8**
The pipeline still works — `Key Vault Secrets Officer` includes all the permissions of `Secrets User` plus write/delete/manage. So ADF can read secrets.

The problem is **over-privileged access** — principle of least privilege violation. With `Secrets Officer`, the ADF Managed Identity can:
- Create new secrets
- Update existing secrets
- Delete secrets
- Recover soft-deleted secrets

A pipeline with write access to Key Vault could potentially be exploited (through a compromised ADF pipeline definition) to overwrite credentials — e.g., set `voltgrid-password` to an attacker-controlled value, disrupting the entire platform. ADF only needs to **read** secrets — `Secrets User` is sufficient and safe.

---

**A9**

| Role | Can read secret values | Can create/update/delete secrets | Assigned to |
|---|---|---|---|
| `Key Vault Secrets Officer` | Yes | Yes | Your user account (needs to manage secrets) |
| `Key Vault Secrets User` | Yes | No | ADF MI, AzureDatabricks app, Service Principal |

Principle: grant the **minimum privilege needed** for the task. ADF, Databricks, and the SP only need to read secrets at runtime — they should never be able to create or overwrite them.

---

**A10**
Three specific risks of hardcoding credentials in the ADF pipeline body:

1. **Git exposure:** ADF pipelines can be connected to a Git repository (Azure DevOps or GitHub). If the pipeline JSON is committed, the password appears in plaintext in every commit, pull request, and git history. It cannot be truly removed without force-pushing (destructive) and rotating the credential.

2. **ARM template export:** When you export the resource group as an ARM template (for disaster recovery or deployment), the pipeline JSON is included. Hardcoded secrets appear in the exported template, which engineers may store in SharePoint, email, or other non-secure locations.

3. **ADF Monitor logs:** ADF Monitor captures activity inputs and outputs. A hardcoded password in the POST body appears in the `act_api_login` Input tab in Monitor, which any user with ADF Monitor Reader access can view.

---

## ADF Managed Identity and Access Control

**A11**
A System-assigned Managed Identity is automatically created by Azure when you create an ADF instance. Its lifecycle is tied to the ADF resource — deleting ADF deletes the MI.

Differences from a Service Principal:

| | System-assigned MI | Service Principal |
|---|---|---|
| Created by | Azure automatically | You create it manually |
| Credentials | None — Azure handles token issuance | Client ID + Secret (or certificate) |
| Rotation | Never needed — Azure rotates internally | You must rotate the client secret (expires) |
| Lifecycle | Tied to the ADF resource | Independent — survives ADF deletion |
| Use case | One resource authenticating as itself | Cross-resource, cross-service identity |

ADF uses its MI for pipeline execution because it eliminates credential management — no secret to store, rotate, or accidentally expose.

---

**A12**
`Storage Blob Data Owner` adds the ability to manage **POSIX ACLs** (Access Control Lists) on individual files and directories in ADLS Gen2. ADF only needs to read from source and write to Bronze — `Storage Blob Data Contributor` (read + write + delete files) is sufficient.

Violated principle: **Principle of Least Privilege** — grant only the minimum permissions required for the task. Using Owner means:
- If the ADF Managed Identity is ever compromised (e.g., a rogue pipeline definition is deployed), an attacker could change ACLs on every file in the storage account — potentially locking out all other services or granting themselves access to Silver and Gold data.

---

**A13**
Most likely cause: **IAM propagation delay**. Azure role assignments typically take 1–2 minutes to propagate. If the pipeline was triggered immediately after assigning the role, the storage service had not yet received the updated permissions. The linked service test connection at that moment happened to be tested from a different data plane node that already received the update, while the actual Copy Activity ran on a different path.

Fix: wait 2 full minutes after assigning any RBAC role before testing or running. If the issue persists, verify the correct scope was assigned — the role must be on the storage account resource, not on the resource group or subscription (though those would work too via inheritance).

---

**A14**
The `ls_keyvault` linked service authenticates using ADF's **System-assigned Managed Identity**. The chain:

1. ADF pipeline activity runs and needs a secret value
2. ADF requests an OAuth token from **Azure Entra ID** for the scope `https://vault.azure.net` — presenting its Managed Identity credentials (no password; Azure handles this internally)
3. Entra ID returns a short-lived OAuth bearer token
4. ADF makes the Key Vault REST API call:
   ```
   GET https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-username/?api-version=7.0
   Authorization: Bearer <oauth-token>
   ```
5. Key Vault checks if the identity (ADF MI) has `Key Vault Secrets User` role → yes → returns `{"value": "voltgrid_demo", ...}`
6. ADF extracts `.value` from the response

The password never travels over the network directly — only the OAuth token does, and it is short-lived (typically 1 hour).

---

**A15**
The request would **fail with 401 Unauthorized**. Setting `Resource = https://storage.azure.com` tells Azure Entra ID to issue an OAuth token for the **Storage** scope, not the **Key Vault** scope. When this token is sent to `vault.azure.net`, the Key Vault service rejects it — it expects a token issued for the `https://vault.azure.net` audience.

Each Azure service validates that the token's audience (`aud` claim) matches its own endpoint. A token for Storage cannot be used to access Key Vault, and vice versa. This is a core OAuth security property — tokens are audience-bound.

---

## ADF Pipeline Design

**A16**
`act_get_username` and `act_get_password` run in **parallel** because they are independent — neither depends on the other's output. Running them simultaneously saves ~1 second.

The dependency condition: `act_api_login` has TWO incoming dependency arrows, both set to `Succeeded`. ADF evaluates both conditions before starting `act_api_login`:
- `act_get_username` must be `Succeeded`
- `act_get_password` must be `Succeeded`

If either fails, `act_api_login` does not start — the pipeline moves to the failed state. This is an implicit AND gate: both must succeed.

---

**A17**
The Key Vault REST API response for a secret is:
```json
{
  "value": "voltgrid_demo",
  "id": "https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-username/abc123",
  "attributes": { "enabled": true, "created": ..., "updated": ... },
  "contentType": null
}
```

`activity('act_get_username').output` is the full parsed JSON object above. `.output.value` accesses the `value` field — which contains the actual secret string `"voltgrid_demo"`.

The expression `@concat(...)` builds a valid JSON string body:
```json
{"username":"voltgrid_demo","password":"EVcharge@AU2025"}
```

---

**A18**
To verify what the activity actually returned, go to **Monitor** → **Pipeline runs** → click the run → **Activity runs** → click `act_api_login` → **Output** tab.

The output tab shows the raw JSON response from the login endpoint:
```json
{
  "token": "abc123def456..."
}
```

The correct key is `token`. If `act_set_token` used `@activity('act_api_login').output.auth_token` (wrong key), `v_token` would be null/empty, and `act_copy_payments` would send `Authorization: Token ` (empty) — causing 401.

The Output tab is the definitive way to verify what any Web Activity actually returned and which key to reference.

---

**A19**
`Authentication type = Anonymous` on the linked service means ADF does not apply any built-in authentication scheme when making REST calls through this linked service. It does NOT mean the API calls are unauthenticated end-to-end.

The actual authentication happens inside the pipeline logic:
1. `act_get_username` and `act_get_password` read credentials from Key Vault
2. `act_api_login` POSTs credentials to `/api/auth/login/` → receives a token
3. `act_set_token` stores the token in `v_token`
4. `act_copy_payments` adds `Authorization: Token <v_token>` as an **additional header** on the GET request

The linked service handles the transport (HTTPS to the base URL). The pipeline handles the application-level authentication manually. This pattern is necessary when the API uses a custom auth flow (fetch token via POST, then use in header) that ADF's built-in auth types don't natively support.

---

**A20**
`setOfObjects` writes the JSON as a single JSON object or array at the root level — ADF wraps all records in one valid JSON document. Each record from the API response appears as an object within the array.

Example output with `setOfObjects`:
```json
[
  {"payment_id": "PAY-001", "amount_aud": 15.50, ...},
  {"payment_id": "PAY-002", "amount_aud": 22.00, ...}
]
```

`arrayOfObjects` is essentially the same for a REST source that returns a JSON array — both produce valid JSON arrays. The difference becomes relevant when the source returns a stream of objects (NDJSON/newline-delimited JSON): `setOfObjects` would wrap the whole stream in `[...]`, while `arrayOfObjects` expects the source to already return a proper array.

For reading this file back in Spark, `spark.read.option("multiLine", "true").json(path)` handles `setOfObjects` format correctly.

---

## Bronze Layer Design

**A21**
The path `bronze/api/payments/raw/payments.json` follows the convention `bronze/<source-type>/<entity>/raw/<file>` for three reasons:

1. **Source type disambiguation:** When the project has 28 source systems (API, blob uploads, streaming), you need to know at a glance where the data came from. `api/` means REST API, `blob/` means CSV upload, `streaming/` means IoT Event Hub.

2. **Multi-entity expansion:** Day 3 adds sessions, customers, and stations. Without entity-level folders, all API data would land in the same directory, making it impossible to run targeted reads or apply different retention policies per entity.

3. **Downstream predictability:** Databricks ingestion notebooks use a deterministic path pattern — they know exactly where to look for payments data without parameter configuration.

Flat path (`bronze/payments.json`) breaks as soon as a second entity is added and gives no indication of the source system.

---

**A22**
No — the Bronze design is not incomplete. ADF's job is to land the raw API response exactly as received. Adding audit columns (`_ingestion_ts`, `_source_file`, `_pipeline_run_id`) is the Databricks notebook's job when it reads from Bronze and writes to Silver.

This separation is intentional:
- ADF is a no-code orchestration tool — adding columns requires a transformation engine
- Bronze = raw landing zone. If you add columns in Bronze, you've already transformed the data
- Audit columns added at the Bronze→Silver transition reflect when Databricks processed the data, which is meaningful for debugging Silver processing issues

Day 3 adds a Databricks notebook that reads `payments.json`, adds the 4 audit columns, and writes to `silver/payments/` as a Delta table with MERGE upsert.

---

**A23**
Yes, this is a problem. Running the pipeline twice with the same sink path overwrites `payments.json` — if Day 2 fetches page 1 in the morning and again in the afternoon, the first file is replaced. If the API data for page 1 changed between runs, you lose the morning snapshot. Bronze should be an audit log — every run's raw data should be preserved.

Day 3 fixes this with **partitioned sink paths**:
```
bronze/api/payments/raw/ingestion_date=2026-10-13/page_001.json
bronze/api/payments/raw/ingestion_date=2026-10-13/page_002.json
```

Using `@formatDateTime(utcNow(), 'yyyy-MM-dd')` as the partition value ensures each day's run lands in its own directory. Re-running on the same day would overwrite that day's partition only — not historical data.

---

**A24**
Parameterizing at both levels is the correct design because the dataset and pipeline have different scopes:

- **Dataset level (`p_page`, `p_page_size`):** Makes the dataset reusable for any caller — if another pipeline wanted to call the payments endpoint with different pagination, it can pass different values without modifying the dataset definition.

- **Pipeline level (`p_page`, `p_page_size`):** Allows the pipeline to be triggered with different parameters from ADF Monitor, a trigger, or a parent pipeline (ForEach in Day 3).

If you hardcoded `page=1&page_size=100` in the linked service Base URL, you would need a different linked service for every pagination variant, and the URL-level change would affect every dataset that uses that linked service. Parameterization keeps concerns separated: linked service = connection, dataset = shape + path, pipeline = logic.

---

**A25**
To add `/api/db/sessions/` with sink at `bronze/api/sessions/raw/sessions.json`:

**Reusable existing components:**
- Linked services: `ls_keyvault`, `ls_voltgrid_api`, `ls_adls_bronze` — all reuse unchanged
- The Key Vault Web Activities (`act_get_username`, `act_get_password`) and login logic are the same

**New components needed:**
1. **New source dataset:** `ds_voltgrid_sessions_src` — same linked service `ls_voltgrid_api`, same parameters `p_page` and `p_page_size`, but relative URL: `/api/db/sessions/?page=@{dataset().p_page}&page_size=@{dataset().p_page_size}`
2. **New sink dataset:** `ds_bronze_sessions_sink` — same linked service `ls_adls_bronze`, file path: `bronze` / `api/sessions/raw` / `sessions.json`
3. **New Copy Activity** in the pipeline: `act_copy_sessions` — same source config as `act_copy_payments` (same auth header `@concat('Token ', variables('v_token'))`), but using the new datasets

**How to avoid duplicating Key Vault Web Activities:**
Option 1 (simple — Day 2 scope): Add `act_copy_sessions` in parallel with `act_copy_payments` after `act_set_token` — both share the same `v_token` variable. No duplication needed.

Option 2 (Day 3 pattern): Extract the authentication steps into a **separate pipeline** (`pl_get_api_token`) that outputs the token, then call it from a parent pipeline using **Execute Pipeline Activity** before a ForEach that loops over all 6 endpoints.

---
