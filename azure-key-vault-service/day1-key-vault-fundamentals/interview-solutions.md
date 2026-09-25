# Day 1 — Interview Solutions: Azure Key Vault Fundamentals

> Complete answers for all 30 questions in `interview-questions.md`.

---

## Concept 1: What is Azure Key Vault — Fundamentals & Creation

**Q1 — What is Azure Key Vault and why not use .env files?**

**Azure Key Vault** is a cloud-managed service for storing and controlling access to secrets (passwords, tokens, connection strings), cryptographic keys, and TLS/SSL certificates. It is the Azure equivalent of HashiCorp Vault, but fully managed — no server to provision, patch, or maintain.

**The problem it solves:** Secrets must be available to applications but must not be visible to unauthorized people or systems. The naive solution — putting secrets in code or config files — fails in practice for several reasons.

**Why `.env` + `.gitignore` is not "secure enough":**

| Risk | What can go wrong |
|---|---|
| Accidental Git commit | `.gitignore` is a convention, not enforcement. One `git add -A` or a misconfigured IDE commits the file. Once in Git history, it is permanently exposed — even after deletion. |
| CI/CD pipeline exposure | Build logs, environment variable dumps, or crash reports can expose values even if the file is never committed |
| Local machine compromise | If a developer's laptop is stolen, all `.env` files on it are exposed |
| No audit trail | You have no record of who read the secret, when, or from where |
| No access control | Everyone with access to the repo or CI/CD system gets the secret |
| No rotation workflow | Rotating a secret means updating every `.env` file on every machine manually |

**What Key Vault provides instead:**
- Centralised storage with encryption at rest (AES-256) and in transit (TLS)
- Access control: only authenticated identities with explicit RBAC permission can read a value
- Full audit log: every read, write, and delete is logged to Azure Monitor
- Versioning: rotate a secret without touching any application code
- Expiry: secrets can automatically become unreadable after a date

---

**Q2 — Three types of objects in Key Vault**

| Type | What it is | Data engineering example |
|---|---|---|
| **Secrets** | Any string value you want to keep confidential | `db-password = "MyStr0ng$ecretPass!"`, `adls-sas-token = "sv=2026-..."`, `event-hub-connection-string = "Endpoint=sb://..."` |
| **Keys** | Cryptographic keys used for encryption/decryption or signing operations. Key Vault performs crypto operations on your behalf — the key never leaves the vault | An RSA-2048 or AES-256 key used to encrypt PII fields (names, email addresses) before writing them to the Bronze layer in ADLS Gen2 |
| **Certificates** | Full TLS/SSL certificate lifecycle management — create, import, auto-renew, monitor expiry | The HTTPS certificate for an internal API that serves fraud scores to the risk team; Key Vault auto-renews it before expiry via integration with DigiCert or Let's Encrypt |

**Key distinction:** Secrets are opaque strings — Key Vault stores them but does not interpret them. Keys are mathematical objects — Key Vault performs `encrypt()`, `decrypt()`, `sign()`, `verify()` operations using the key, without ever returning the raw key bytes to the caller.

---

**Q3 — Standard vs. Premium tier**

| Feature | Standard | Premium |
|---|---|---|
| Secrets storage | Yes | Yes |
| Software-protected keys (stored in Azure's managed infrastructure) | Yes | Yes |
| Hardware Security Module (HSM) backed keys (FIPS 140-2 Level 2) | No | Yes |
| Approximate cost | ~$0.03 per 10,000 operations | ~$1.00 per HSM-backed key per month + operations |

**HSM (Hardware Security Module):** A physical device that generates and stores cryptographic keys in tamper-resistant hardware. The key material never exists in software memory — even Azure engineers cannot extract it. Required by regulations like PCI-DSS Level 1, HIPAA for covered entities, and FedRAMP High.

**For a data engineering payment data lake:** Standard tier. The secrets you store (SAS tokens, passwords, connection strings) are plain strings — they are not cryptographic keys that need HSM protection. The encryption of data at rest (storage encryption, database encryption) uses Azure-managed keys which are already HSM-backed by default. Upgrade to Premium only if a compliance auditor specifically requires customer-managed keys in a dedicated HSM — an unusual requirement for most data engineering teams.

---

**Q4 — Risks of `.env` file approach vs. Key Vault**

The junior engineer is partially right — `.gitignore` reduces the risk of accidental commits. But "reduced risk" is not the same as "eliminated risk."

**Specific risks that remain:**

1. **Human error:** A developer runs `git add .` in the project root. The `.gitignore` was not checked into the repo yet (first commit). The `.env` file is now in the repository's history permanently. Even after removing it, the file can be retrieved from any clone made before the removal.

2. **Forked repositories:** If the repo is forked (even privately) before the `.env` was removed, the fork retains the secret.

3. **CI/CD exposure:** Many CI/CD systems print environment variables during debug mode. Pipeline configurations that expand environment variables in YAML echo secrets to build logs.

4. **Developer machine as attack surface:** `.env` files sitting on 10 developers' laptops means 10 attack surfaces. Key Vault means one.

5. **No audit trail:** If the database is breached and you investigate, you have no way to know who read `db-password`, when, or from which machine. Key Vault logs every read.

6. **Secret sprawl:** Six months later, you have 12 `.env` files across different projects, all with slightly different versions of the same password. When you rotate, you must update all 12. Key Vault has one canonical value; all consumers always get the current version.

**What Key Vault eliminates:** centralized canonical storage, access control per identity, full audit, versioned rotation, expiry enforcement, and zero-secrets-on-disk for applications running in Azure.

---

**Q5 — What is the Vault URI?**

The **Vault URI** is the HTTPS endpoint your application uses to communicate with Key Vault. It is globally unique because Key Vault names are globally unique.

**Format:**
```
https://{vault-name}.vault.azure.net/
```

**Example:**
```
https://kv-payments-prod.vault.azure.net/
```

**Why it matters:**
- Your `SecretClient` in Python is initialised with this URL: `SecretClient(vault_url="https://kv-payments-prod.vault.azure.net/", credential=...)`
- The SDK constructs the full REST API URL: `GET https://kv-payments-prod.vault.azure.net/secrets/adls-sas-token?api-version=7.4`
- It must be stored in an environment variable or app configuration — not hardcoded — so the same code can point to different vaults in dev vs. prod

**Finding it:** Azure Portal → Key Vault → Overview page → "Vault URI" field.

---

**Q6 — One vault vs. one per environment**

**Option A — One vault, multiple environments (not recommended):**
```
kv-datalake-shared
├── dev-adls-sas-token
├── staging-adls-sas-token
└── prod-adls-sas-token
```
- Pro: Less to manage
- Con: A developer with Secrets Officer on the shared vault can read production secrets. One misconfigured permission grants cross-environment access. Violates the principle of least privilege.

**Option B — One vault per environment (strongly recommended):**
```
kv-datalake-dev-001        (Australia East)
kv-datalake-staging-001    (Australia East)
kv-datalake-prod-001       (Australia East)
```
- Pro: Hard isolation — dev RBAC roles cannot touch production secrets; separate audit logs; prod vault can have stricter network access rules (VNet only); different rotation schedules
- Con: Three vaults to manage; application config must point to the correct vault per environment

**The rule in regulated industries:** Always one vault per environment minimum. Many enterprises go further: one vault per application per environment. The Azure cost of a Key Vault is negligible (operations-based pricing) compared to the compliance and security value of isolation.

---

**Q7 — Soft delete and purge protection**

**Soft delete:** When you delete a Key Vault (or a secret within it), it is not permanently erased immediately. It enters a "deleted" state and is retained for the configured period (7–90 days, default 90 days). During this window, you can recover it.

```bash
# Recover a deleted vault
az keyvault recover --name kv-datalake-dev-001

# List deleted vaults (soft-deleted, awaiting purge)
az keyvault list-deleted
```

**Purge protection:** When enabled, the vault (and its secrets) cannot be permanently deleted (purged) during the soft-delete retention window — not even by the Owner or an Administrator. Even Microsoft support cannot purge it. The vault MUST wait out the full retention period before it is permanently gone.

**If purge protection is enabled and you delete the vault:**
- The vault enters a soft-deleted state
- No one can purge it until the retention period expires (e.g., 90 days)
- The vault name is reserved during this period — you cannot create a new vault with the same name
- After 90 days, it is permanently purged automatically

**Why this matters:** If a disgruntled employee or an attacker deletes your production vault, purge protection gives you a 90-day recovery window. Without it, a single `az keyvault delete` followed by `az keyvault purge` permanently destroys all secrets.

**Production recommendation:** Always enable both soft delete (90 days) and purge protection. They are not enabled by default on older vaults — check existing vaults.

---

**Q8 — Key Vault Secrets User vs. Key Vault Secrets Officer**

| Role | Can read secret values (`get`) | Can create/update secrets (`set`) | Can delete secrets | Can manage access (IAM) |
|---|---|---|---|---|
| Key Vault Secrets User | Yes | No | No | No |
| Key Vault Secrets Officer | Yes | Yes | Yes | No |
| Key Vault Administrator | Yes | Yes | Yes | Yes (on vault) |

**For an application's Managed Identity:** Assign **Key Vault Secrets User**. The application only needs to read secrets at runtime — it should never create, update, or delete them. Giving it Secrets Officer would allow a compromised application to overwrite production secrets.

**For a human operator or an automation script that rotates secrets:** Assign **Key Vault Secrets Officer**. It needs to create new versions and disable old ones.

**Principle of least privilege:** An application only ever gets the minimum permission it needs to do its job. A pipeline that reads secrets should never be able to delete them.

---

**Q9 — Blast radius of a compromised developer account**

**Assuming the developer has Key Vault Secrets Officer on the production vault:**

**What the attacker can do:**
- Read all secret values (SAS tokens, database passwords, API keys, Event Hub connection strings)
- Update secrets to values they control (point the pipeline to their own database)
- Disable or delete secrets (denial of service — pipelines fail to start)
- Create new secrets (inject malicious values)

**What limits the blast radius:**

1. **RBAC scope:** If the role was assigned at the vault level (not subscription level), the attacker can only affect that one vault. They cannot create new Azure resources or access storage accounts directly (they need the SAS tokens, which they now have — but they still can't access without the token).

2. **Conditional Access:** If your Azure AD tenant has Conditional Access policies (MFA required, trusted device only), the attacker's session may be blocked if they're on an untrusted device.

3. **Anomaly detection:** Azure Defender for Key Vault (Microsoft Defender for Cloud) monitors for unusual access patterns — bulk secret reads at unusual hours, reads from unknown IPs — and can alert or block automatically.

4. **Audit logs:** Azure Monitor diagnostic logs on the vault record every operation with timestamp, caller identity, and IP address. You can determine exactly what was read and when.

5. **Secret expiry:** If SAS tokens and database passwords are short-lived (30-day expiry), the attacker's window to use them is limited.

**Response steps:** (1) Revoke the developer's Azure AD session immediately (Azure AD → User → Revoke sessions). (2) Remove their Key Vault RBAC role. (3) Rotate all secrets in the vault. (4) Review audit logs to understand what was accessed.

---

**Q10 — What happens when a Key Vault is deleted? Recovery requirements**

**What happens:**
- With soft delete (default): vault enters "deleted" state, retained for 7–90 days, recoverable
- Without soft delete (old vaults pre-2019): vault and all secrets are permanently gone immediately
- With purge protection: even after deletion, the vault cannot be permanently erased during the retention window

**Minimum configuration for recovery:**
1. **Soft delete enabled** (now on by default for all new vaults) — the vault exists in a deleted state and can be recovered
2. **Purge protection enabled** (not on by default) — prevents permanent deletion during retention period

**Recovery command:**
```bash
az keyvault recover --name kv-datalake-dev-001 --location australiaeast
```

**What cannot be recovered:**
- If soft delete was disabled (old vault) and the vault was deleted, all secrets are gone permanently
- If soft delete is enabled but the retention period (e.g., 7 days) has passed and no purge protection, the vault is purged automatically

**Additional protection:** For production vaults, apply an Azure Resource Lock of type "Delete" to the vault resource. This prevents any delete operation at all until the lock is explicitly removed — even by an Owner. The lock + purge protection is a defence-in-depth approach.

---

## Concept 2: Secrets Management — Create, Version, Retrieve, Access Control

**Q11 — Secret versioning**

Every time you update a secret's value (call `set_secret` with the same name in the SDK, or create a "New Version" in the portal), Key Vault:
1. Creates a new version with a unique 32-character GUID version identifier
2. Marks the new version as the **current** version
3. Retains all previous versions intact, with their original values

**Visual representation:**
```
Secret name: adls-sas-token

Version GUIDs:
├── a3f2...91c4  (version 1 — created 2026-01-01, value: "sv=2026-old...")
├── b7d1...42e8  (version 2 — created 2026-06-01, value: "sv=2026-v2...")
└── c9a0...88f3  (version 3 — CURRENT — created 2026-09-01, value: "sv=2026-v3...")
```

**Key behaviour:**
- `get_secret("adls-sas-token")` → always returns version 3 (current)
- `get_secret("adls-sas-token", version="a3f2...91c4")` → returns version 1
- Previous versions are not deleted — they accumulate until you explicitly delete them
- This means you can always roll back: update the secret to the previous value (creating a new version 4) or retrieve the old version directly

**Production implication:** You can rotate a secret (create version 2) and the application running `get_secret` with no version pinning automatically starts using the new value the next time it calls the SDK — no deployment needed.

---

**Q12 — Disabled vs. expired**

| | Disabled | Expired |
|---|---|---|
| How it happens | Manual toggle (portal, CLI, SDK) | Automatic when expiry date passes |
| Can the value be read? | No | No |
| Is the secret deleted? | No | No |
| Can it be re-enabled? | Yes — toggle Enabled back to Yes | Yes — update the expiry date to the future, or create a new version |
| Typical use case | Emergency credential revocation (suspected breach), credential rotation preparation | Enforcing mandatory rotation (SAS tokens, API keys with known expiry) |

**Re-enabling after disable:** The secret's value is preserved exactly. You are only toggling the availability flag — the underlying encrypted value is untouched.

**Re-enabling after expiry:** The original version remains inaccessible by expiry date. To restore service, either: (a) create a new version of the secret (recommended — this is a rotation), or (b) update the expiry date on the existing version to extend it (use with care — it may mean a credential is still valid longer than intended).

---

**Q13 — Vault Access Policies vs. Azure RBAC**

| | Vault Access Policies (Legacy) | Azure RBAC (Recommended) |
|---|---|---|
| Where configured | Inside the vault's own settings ("Access policies" blade) | Azure IAM (same as all other Azure resources) |
| Scope | Per vault only — you grant access to the entire vault | Per vault, per secret, per tag, per resource group |
| Maximum entries | 1,024 policies per vault | Unlimited (managed via Azure AD groups) |
| Integration with Azure AD groups | Limited | Full — assign a role to a group, all members inherit |
| Audit | Key Vault diagnostic logs (separate system) | Azure Activity Log + Key Vault diagnostic logs (unified) |
| Conditional Access support | No | Yes |
| Microsoft recommendation | Deprecated for new vaults | Use this for all new vaults |

**Why RBAC is better:**

1. **Consistency:** One permission model across all Azure resources — same IAM blade, same `az role assignment create` command, same Azure Policy tooling
2. **Granularity:** You can grant a service principal access to only one specific secret using a scope like `/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.KeyVault/vaults/{vault}/secrets/{secretName}` — impossible with Access Policies
3. **Scale:** 1,024 access policies is a real limit for large organisations. RBAC via groups scales infinitely.

---

**Q14 — RBAC for Databricks cluster vs. data analyst**

**For the Databricks cluster (a Managed Identity):**
```
Role:   Key Vault Secrets User
Scope:  kv-datalake-dev-001 (vault level)
Member: Databricks managed identity (the cluster's system-assigned Managed Identity)
```
- Secrets User: can read secret values, cannot modify or delete them
- Vault-level scope: cluster can read any secret in the vault (fine if you structure naming well)
- Managed Identity: no credentials stored anywhere — the cluster proves its identity via Azure AD tokens automatically

**For the data analyst (a human user):**
```
Role:   Key Vault Secrets User
Scope:  /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.KeyVault/vaults/{vault}/secrets/adls-sas-token
Member: analyst@company.com
```
- Same role (Secrets User) — they only need to read, not write
- Secret-level scope: they can only read this ONE specific secret — not `db-password`, not `event-hub-connection-string`
- If many analysts need the same access, add them to an Azure AD group and assign the role to the group

**Key distinction:** Managed Identities are the preferred way to give Azure services (Databricks, Functions, App Service) access to Key Vault — no credentials to store or rotate. Human users should be scoped as narrowly as possible.

---

**Q15 — RBAC scope levels for Key Vault**

RBAC assignments can be scoped at four levels, from broadest to narrowest:

```
Subscription
└── Resource Group
    └── Key Vault (vault level)
        └── Secret (individual secret level)
```

**Example — grant access to only one secret:**
```bash
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee "analytics-service-principal-object-id" \
  --scope "/subscriptions/{sub-id}/resourceGroups/rg-datalake-dev/providers/Microsoft.KeyVault/vaults/kv-datalake-dev-001/secrets/adls-sas-token"
```

**Yes, you can grant access to exactly one secret.** The service principal assigned this role can only call `get_secret("adls-sas-token")` — any other secret name returns a 403 Forbidden.

**In practice:** Secret-level scoping is used when different teams have very different data access needs and share a single vault. Most teams use vault-level scoping combined with disciplined secret naming (so each team only calls `get_secret` for secrets they know they need) — the extra granularity of secret-level RBAC adds operational overhead.

---

**Q16 — Secret expiry at day 31**

**What happens when the pipeline calls `get_secret("adls-sas-token")` on day 31:**
- Key Vault evaluates the secret's metadata before returning the value
- The expiry date has passed
- Key Vault returns an error response: the secret is disabled/expired
- The `azure-keyvault-secrets` SDK raises: `azure.core.exceptions.ResourceNotFoundError` (or `HttpResponseError` with status 403/404)
- Without error handling, the pipeline crashes with an unhandled exception

**What the pipeline should do instead of crashing:**

```python
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

try:
    secret = client.get_secret("adls-sas-token")
    sas_token = secret.value
except (ResourceNotFoundError, HttpResponseError) as e:
    # Alert the operations team — a secret has expired and needs rotation
    raise RuntimeError(
        f"Cannot retrieve 'adls-sas-token' from Key Vault — it may be expired or disabled. "
        f"Original error: {e}. "
        f"Action required: rotate the SAS token and update the secret in Key Vault."
    ) from e
```

**Better design:** Monitor secret expiry proactively. Use Azure Monitor alerts or a scheduled Python script that checks `secret.properties.expires_on` and sends a Slack/Teams alert 7 days before expiry — so the pipeline never encounters an expired secret at runtime.

---

**Q17 — Rolling back to a previous secret version**

**At the Key Vault level (no-code approach):**
1. In the portal, go to the secret → version history
2. Click the previous version → note its version GUID
3. Create a **new version** of the secret with the old value (copy and paste it)
4. The new version becomes current — all applications reading "latest" automatically get the rolled-back value

**Why not just "set current version" to an older one?** Key Vault does not have a "promote version" operation. The current version is always the most recently created. To roll back, you create a new version whose value is the same as the old one.

**At the consuming application level (version pinning — for emergency only):**
```python
# Emergency: pin to a specific version while you investigate
secret = client.get_secret("adls-sas-token", version="a3f2...91c4")
```

**Warning:** Version pinning in application code defeats the purpose of Key Vault versioning. It should only be used as an emergency measure while you fix the vault-side issue. Remove the version pin and return to reading latest as soon as the correct value is the current version.

---

**Q18 — Secret activation date**

The **activation date** (also called "not before" date) makes a secret temporarily unavailable before a specified time. The value is stored in the vault but cannot be read until that date.

**Realistic scenario — pre-creating production credentials before a planned cutover:**

Your team is deploying a new payment processing service on Monday at 09:00 AEST. The new database has credentials that will be created on Friday, but the database will not be switched live until Monday. You want to:
1. Store the new `db-password` in Key Vault on Friday
2. Ensure no existing pipeline accidentally starts using the new password before Monday
3. At 09:00 Monday, the deployment script switches the application to the new database — the new Key Vault secret automatically becomes readable at that moment

```
Secret: db-password-v2
Activation date: 2026-09-28 09:00:00 AEST
Expiry date:     2026-12-28 09:00:00 AEST
```

Before 09:00 Monday: `get_secret("db-password-v2")` returns 403 — not yet active.
At and after 09:00 Monday: `get_secret("db-password-v2")` returns the value — automatically available.

---

**Q19 — Audit trail for Key Vault access**

**Azure Monitor Diagnostic Logs for Key Vault** provides the complete audit trail.

**How to enable and access:**
1. Go to Key Vault → **Diagnostic settings** (under Monitoring)
2. Click **"+ Add diagnostic setting"**
3. Select log categories:
   - **AuditEvent** — every read, write, delete, and list operation with caller identity, operation, result, and timestamp
4. Send to: **Log Analytics Workspace** (for querying), **Storage Account** (for long-term retention), or **Event Hub** (for SIEM integration)

**Query in Log Analytics to see `db-password` access:**
```kusto
AzureDiagnostics
| where ResourceType == "VAULTS"
| where OperationName == "SecretGet"
| where id_s contains "db-password"
| where TimeGenerated > ago(30d)
| project TimeGenerated, CallerIPAddress, identity_claim_oid_g, resultType_s
| order by TimeGenerated desc
```

This shows: who accessed it (Azure AD object ID), from which IP, at what time, and whether the access succeeded or was denied.

**Retention:** Log Analytics retains data for 30 days by default; extend to 90 or 365 days via workspace settings. For regulatory compliance, export to a Storage Account with immutable storage (legal hold) for guaranteed 1-year retention.

---

**Q20 — Maximum Access Policies and how RBAC solves it**

**The limit:** A single Azure Key Vault supports a maximum of **1,024 Access Policy entries**.

**Why this is a real problem for large organisations:**
- A company with 500 service principals, 200 developers, and 300 CI/CD agents = 1,000 entries before even accounting for multiple environments
- Once the limit is hit, new applications or users cannot be granted access without revoking existing entries
- There is no graceful warning — the 1,025th assignment simply fails

**How RBAC solves it:**
RBAC uses Azure AD groups, not individual entries per identity. You create groups like `sg-keyvault-readers-prod` and `sg-keyvault-officers-dev`, assign the RBAC role to the group once, and then add identities to the group via Azure AD. One role assignment serves thousands of members.

```
Without RBAC (Access Policies):
Policy 1: service-principal-A → get/list
Policy 2: service-principal-B → get/list
Policy 3: service-principal-C → get/list
... (up to 1,024)

With RBAC + Azure AD groups:
Role assignment 1: Group "sg-kv-readers" → Key Vault Secrets User
  Group members: service-principal-A, B, C, D, ... (unlimited)
```

**The group approach also simplifies offboarding:** Remove a user from the Azure AD group → they lose vault access immediately, without touching any Key Vault configuration.

---

## Concept 3: Accessing Secrets from Python — SDK, DefaultAzureCredential, Safe Patterns

**Q21 — Required Python packages and SecretClient**

**Install command:**
```bash
pip install azure-keyvault-secrets azure-identity python-dotenv
```

| Package | Version (as of 2026) | Purpose |
|---|---|---|
| `azure-keyvault-secrets` | 4.x | Provides `SecretClient` — the class for all secret CRUD operations |
| `azure-identity` | 1.x | Provides `DefaultAzureCredential` and other credential classes for Azure AD auth |
| `python-dotenv` | 1.x | Loads `.env` files into `os.environ` (local development only) |

**The class you use:**
```python
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

client = SecretClient(
    vault_url="https://kv-datalake-dev-001.vault.azure.net/",
    credential=DefaultAzureCredential()
)
```

**Main methods:**
- `client.get_secret(name)` → returns `KeyVaultSecret` object with `.value` and `.properties`
- `client.set_secret(name, value)` → creates or updates a secret, returns `KeyVaultSecret`
- `client.list_properties_of_secrets()` → returns an iterable of `SecretProperties` (names, expiry, enabled — no values)
- `client.begin_delete_secret(name)` → starts a delete (returns a poller; call `.result()` to wait)

---

**Q22 — DefaultAzureCredential — what it is and credential chain**

`DefaultAzureCredential` is a composite credential that tries multiple authentication methods in order and uses the first one that succeeds. It is the single class that works identically across local development and every Azure hosting environment.

**The credential chain (in order):**

| # | Credential type | When it works |
|---|---|---|
| 1 | `EnvironmentCredential` | `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` (or `AZURE_CLIENT_CERTIFICATE_PATH`), `AZURE_TENANT_ID` are all set in environment |
| 2 | `WorkloadIdentityCredential` | Running in Kubernetes with Azure Workload Identity configured |
| 3 | `ManagedIdentityCredential` | Running on Azure VM, App Service, Azure Functions, Azure Container Instances, or Azure Databricks with Managed Identity enabled |
| 4 | `SharedTokenCacheCredential` | Valid token from Visual Studio (Windows only) |
| 5 | `VisualStudioCodeCredential` | VS Code's Azure Account extension is logged in |
| 6 | `AzureCliCredential` | `az login` has been run and a valid session exists |
| 7 | `AzurePowerShellCredential` | `Connect-AzAccount` in PowerShell has an active session |
| 8 | `AzureDeveloperCliCredential` | `azd auth login` (Azure Developer CLI) has an active session |

**For a local Python script after `az login`:** Step 6 — `AzureCliCredential` — succeeds. The SDK reads the access token from the Azure CLI's token cache (`~/.azure/`).

**For a Databricks cluster with Managed Identity:** Step 3 — `ManagedIdentityCredential` — succeeds. The SDK calls the Azure Instance Metadata Service endpoint (`169.254.169.254`) which returns a token for the cluster's identity automatically.

---

**Q23 — Why DefaultAzureCredential over ClientSecretCredential in production?**

`ClientSecretCredential` requires you to provide a service principal's client ID, tenant ID, and client secret:

```python
from azure.identity import ClientSecretCredential

# This requires you to store a secret to access secrets — circular problem
credential = ClientSecretCredential(
    tenant_id="00000000-...",
    client_id="11111111-...",
    client_secret="the-service-principal-password"  # WHERE do you store this?
)
```

**The circular problem:** To use `ClientSecretCredential`, you need a client secret. That client secret must be stored somewhere — in an environment variable, a config file, or another vault. You have only moved the problem one level up, not eliminated it.

**Why DefaultAzureCredential with Managed Identity is better:**
- A Managed Identity is an Azure AD identity assigned to an Azure resource (Databricks cluster, App Service, Azure Function)
- The identity is automatically provisioned and managed by Azure — no password, no certificate, no key to store anywhere
- Azure rotates the underlying credentials automatically
- `ManagedIdentityCredential` works by calling a local endpoint available only inside the Azure resource — not accessible from outside
- There are zero credentials to store, rotate, or accidentally expose

**The rule:** If your code runs on Azure, use Managed Identity + `DefaultAzureCredential`. If your code runs outside Azure (on-premises, GitHub Actions), use a Service Principal with a certificate (not a password) — certificates are more secure than client secrets.

---

**Q24 — Dangerous logging of secret values**

**What's wrong with `logging.info(f"Calling API with token: {sas_token}")`:**

1. **Log files are persistent:** Even if the terminal session ends, the log file on disk retains the secret indefinitely (until log rotation)
2. **Centralised logging:** Most production systems ship application logs to a centralised platform (Splunk, Datadog, Azure Monitor, ELK). The secret now exists in a system accessed by dozens of people — including ops engineers who don't need the secret
3. **Log retention:** Centralised logs are often retained for 30, 90, or 365 days — your secret is exposed for that entire period even after rotation
4. **Search and indexing:** Centralised log platforms index every field — the secret value becomes searchable by anyone with log access
5. **Screenshot risk:** If someone screenshots a log view for a bug report, the secret is captured in the screenshot

**The fix:**

```python
import logging

# BAD — logs the value
logging.info(f"Calling API with token: {sas_token}")

# GOOD — logs only the fact, not the value
logging.info(f"Calling API with token (length: {len(sas_token)} chars, starts with '{sas_token[:4]}...')")

# BETTER — don't log anything about the secret value at all
logging.info("Calling API with SAS token from Key Vault")
```

**General rule:** Never include a secret value in any string that goes to a logger, a metrics system, a user-facing error message, or a function's positional arguments (they appear in tracebacks). The secret should exist only in memory, used directly as a parameter to the function that needs it.

---

**Q25 — list_properties_of_secrets vs. get_secret**

**`list_properties_of_secrets()`:**
- Returns an iterable of `SecretProperties` objects
- Contains: `name`, `version`, `enabled`, `expires_on`, `created_on`, `updated_on`, `tags`, `content_type`
- Does **NOT** contain: secret value
- Makes one API call per page (Key Vault paginates results automatically)
- Useful for: monitoring expiry, discovering what secrets exist, building a dashboard of vault contents

**`get_secret(name)`:**
- Makes a single API call: `GET /secrets/{name}` (or `GET /secrets/{name}/{version}`)
- Returns a `KeyVaultSecret` object with `.value` and `.properties`
- The `.value` field contains the actual secret string
- Every call to `get_secret` is individually logged in Key Vault's audit trail

**Why `list_properties_of_secrets()` does not return values:**

By design and for security reasons:
- A bulk secret read would allow a single misconfigured permission to expose every secret at once
- Auditing a bulk value dump is impossible — you cannot know which specific secrets an attacker was looking for
- The principle of explicit intent: every secret value access must be an explicit `get_secret(name)` call with a specific name, making the audit trail meaningful

**Pattern in production:**
```python
# Monitor expiry without accessing values
for props in client.list_properties_of_secrets():
    if props.expires_on and props.expires_on < datetime.now(timezone.utc) + timedelta(days=7):
        alert(f"Secret '{props.name}' expires in less than 7 days!")
```

---

**Q26 — Secret rotation in a long-running Streaming job**

**The problem with caching at startup:**
```python
# Dangerous pattern for long-running jobs
DB_PASSWORD = read_secret(client, "db-password")  # Read once at startup

# ... 6 hours later, DBA rotates the password in Key Vault
# DB_PASSWORD still holds the old value
# Database connections start failing with authentication errors
# Job crashes; requires manual restart and redeploy
```

**Redesigned pattern — periodic refresh:**
```python
import time
from datetime import datetime, timezone, timedelta

SECRET_CACHE = {}
SECRET_TTL_SECONDS = 300  # Refresh every 5 minutes

def get_secret_cached(client, name: str) -> str:
    """Get a secret with a 5-minute in-memory cache."""
    cached = SECRET_CACHE.get(name)
    if cached and cached["expires_at"] > datetime.now(timezone.utc):
        return cached["value"]
    # Cache miss or expired — fetch from Key Vault
    value = client.get_secret(name).value
    SECRET_CACHE[name] = {
        "value": value,
        "expires_at": datetime.now(timezone.utc) + timedelta(seconds=SECRET_TTL_SECONDS)
    }
    return value

# In the streaming loop:
for batch in streaming_batches:
    db_password = get_secret_cached(client, "db-password")
    write_to_database(batch, db_password)
```

**The right approach for Databricks Structured Streaming:**
- Use Databricks Secrets (backed by Key Vault via Secret Scope) — Databricks refreshes secrets automatically when used through `dbutils.secrets.get()`
- Or: Catch database authentication errors in the stream, and trigger a secret re-read only on auth failure (not every batch)
- Or: Use a connection pool that automatically reconnects and re-authenticates — the pool reads the secret at reconnect time

**The rule:** Never cache a secret for longer than your maximum acceptable credential exposure window. For production pipelines, 5 minutes is a common TTL.

---

**Q27 — Diagnosing the 403 Forbidden error from Key Vault**

**Full error:** `azure.core.exceptions.HttpResponseError: (Forbidden) The user, group or application 'objectId=...' does not have secrets get permission on key vault 'kv-datalake-dev-001'`

**Possible causes and how to diagnose each:**

| Cause | How to check |
|---|---|
| No RBAC role assigned to the identity | Portal → Key Vault → Access control (IAM) → Role assignments. Filter by the object ID in the error. Does it appear? |
| Wrong role assigned (e.g., Key Vault Reader instead of Secrets User) | Same IAM tab — check which role is assigned. Key Vault Reader can see vault metadata but NOT secret values. |
| Role assigned at wrong scope | If the role was assigned at the secret level, the identity can only read THAT secret — any other name returns 403. |
| RBAC permission model not selected | Portal → Key Vault → Settings → Access policies. If the vault uses "Access Policies" model, RBAC assignments are ignored entirely. Check the permission model. |
| Azure AD propagation delay | RBAC assignments can take up to 5 minutes to propagate globally in Azure AD. If you just assigned the role, wait and retry. |
| Wrong identity in code | The `DefaultAzureCredential` may be authenticating as a different identity than you think. Print the authenticated identity: `credential.get_token("https://vault.azure.net/.default")` → decode the JWT to see the subject claim. |
| Managed Identity not enabled on the resource | For a Managed Identity to work, it must be explicitly enabled on the Azure resource (VM, App Service, Databricks cluster). Check the resource's Identity settings. |
| Vault network rules blocking access | If the vault has network restrictions (VNet integration, IP allowlist), the caller's IP may be blocked. Check Portal → Key Vault → Networking. |

**Diagnostic command:**
```bash
# Check who the CLI is currently authenticated as
az account show

# Check role assignments for a specific object ID
az role assignment list \
  --scope "/subscriptions/{sub-id}/resourceGroups/rg-datalake-dev/providers/Microsoft.KeyVault/vaults/kv-datalake-dev-001" \
  --query "[].{principal:principalName, role:roleDefinitionName}" \
  --output table
```

---

**Q28 — ResourceNotFoundError handling**

**The exception:** When a secret does not exist, is disabled, or has expired, the SDK raises:
- `azure.core.exceptions.ResourceNotFoundError` — secret name does not exist in the vault
- `azure.core.exceptions.HttpResponseError` with status 403 — secret exists but is disabled/expired and you lack permission to see its metadata

**Correct handling:**

```python
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

def safe_get_secret(client: SecretClient, name: str) -> str | None:
    """
    Read a secret value. Returns None if the secret does not exist,
    is disabled, or has expired. Raises for unexpected errors.
    """
    try:
        secret = client.get_secret(name)
        return secret.value
    except ResourceNotFoundError:
        # Secret name not found in vault, or secret is disabled/expired
        print(f"WARNING: Secret '{name}' is not available (not found, disabled, or expired).")
        return None
    except HttpResponseError as e:
        if e.status_code == 403:
            # Permission denied — this is a configuration problem, not a missing secret
            raise PermissionError(
                f"Access denied to secret '{name}'. Check RBAC role assignments."
            ) from e
        raise  # Re-raise unexpected HTTP errors
```

**Why distinguish 404 from 403?**
- A 404/ResourceNotFoundError means the secret is gone or disabled — often acceptable for optional config
- A 403 means your code doesn't have permission — this is a configuration error that must be fixed, not silently ignored

---

**Q29 — Automated weekly SAS token rotation pipeline**

**Design overview:**

```
Azure Function (Timer trigger — runs every Monday 00:00 UTC)
│
├── 1. For each of 5 ADLS containers:
│       a. Generate a new SAS token (using storage SDK + account key from another secret)
│       b. Call client.set_secret(name=f"adls-{container}-sas-token", value=new_token)
│       c. Key Vault creates a new version — existing pipelines reading "latest" get new token
│
├── 2. Verify each new secret is readable:
│       a. Call client.get_secret(name) with no version pinning
│       b. Confirm the returned version matches the newly created version GUID
│       c. Confirm the value starts with "sv=" (basic sanity check)
│
├── 3. If any verification fails:
│       a. Alert operations team via Teams webhook
│       b. Do NOT delete the old version (it remains as fallback)
│
└── 4. Log: "Rotated 5 SAS tokens. All verified readable."
```

**Python sketch of the rotation function:**

```python
def rotate_sas_token(client: SecretClient, storage_client, container_name: str) -> bool:
    secret_name = f"adls-{container_name}-sas-token"

    # Step 1: Generate new SAS token (expiry: 8 days — 1 day buffer after next rotation)
    new_token = generate_sas_token(storage_client, container_name, expiry_days=8)

    # Step 2: Store in Key Vault (creates a new version)
    result = client.set_secret(name=secret_name, value=new_token)
    new_version = result.properties.version

    # Step 3: Verify by reading back
    verification = client.get_secret(secret_name)
    assert verification.properties.version == new_version, "Version mismatch after rotation"
    assert verification.value.startswith("sv="), "Unexpected token format"

    return True
```

**Key decisions:**
- SAS token expiry: 8 days (rotation happens weekly, so there is always 1 day of buffer)
- Old versions: retained automatically by Key Vault versioning — rollback is possible
- The Function's Managed Identity has Key Vault Secrets Officer role (needs to write secrets)
- The storage account key (used to generate SAS) is itself stored as a secret in Key Vault — the Function reads it from there, never hardcodes it

---

**Q30 — Fintech EV payment platform: complete secrets management architecture**

**Three-vault structure (one per environment):**

```
kv-ev-payments-dev-001      (Australia East, Standard, RBAC)
kv-ev-payments-staging-001  (Australia East, Standard, RBAC)
kv-ev-payments-prod-001     (Australia East, Standard, RBAC, purge protection enabled)
```

**Secret inventory (production vault):**

| Secret name | What it stores | Expiry | Rotation |
|---|---|---|---|
| `adls-bronze-sas-token` | SAS token for bronze container | 8 days | Weekly — Azure Function |
| `adls-silver-sas-token` | SAS token for silver container | 8 days | Weekly — Azure Function |
| `adls-gold-sas-token` | SAS token for gold container | 8 days | Weekly — Azure Function |
| `adls-ml-sas-token` | SAS token for ml-data container | 8 days | Weekly — Azure Function |
| `adls-checkpoints-sas-token` | SAS token for checkpoints container | 8 days | Weekly — Azure Function |
| `evh-payments-connection-string` | Event Hub for payments ingest | 90 days | Quarterly — rotation script |
| `evh-settlements-connection-string` | Event Hub for settlements | 90 days | Quarterly — rotation script |
| `evh-chargebacks-connection-string` | Event Hub for chargebacks | 90 days | Quarterly — rotation script |
| `postgres-username` | PostgreSQL service account username | Never | On staff change |
| `postgres-password` | PostgreSQL service account password | 90 days | Quarterly — rotation script |
| `payment-gateway-api-key-primary` | Primary gateway API key | 90 days | Quarterly |
| `payment-gateway-api-key-secondary` | Secondary gateway API key | 90 days | Quarterly |

**RBAC assignments (production vault):**

| Identity | Role | Scope | Rationale |
|---|---|---|---|
| Databricks cluster Managed Identity | Key Vault Secrets User | Vault level | Reads all secrets at runtime; cannot modify |
| Azure Function (rotation) Managed Identity | Key Vault Secrets Officer | Vault level | Writes new secret versions during rotation |
| Payment App Service Managed Identity | Key Vault Secrets User | Secret level: `evh-*`, `payment-gateway-*` | Only reads what it needs |
| Admin team Azure AD group | Key Vault Administrator | Vault level | Break-glass access for incidents |
| Data science team Azure AD group | Key Vault Secrets User | Secret level: `adls-gold-sas-token`, `adls-ml-sas-token` | Only the containers they need |
| All individual human users | No direct assignment | — | Access only through group membership |

**No human reads production secret values directly:**
- The admin group has Administrator role — but enabling it requires PIM (Privileged Identity Management) just-in-time activation with MFA and a business justification
- Every PIM activation is logged in Azure AD audit logs
- For emergency access: rotation script reads the value, no human does directly

**Audit configuration:**
```
Key Vault Diagnostic Setting (prod vault):
  Log category: AuditEvent
  Destination 1: Log Analytics Workspace (kv-audit-prod-law) — 90 days active, 365 days total
  Destination 2: Storage Account (stkeyvaditprod001, immutable legal hold) — 7 years
  
Alert rule: Any secret read by a human identity (non-Managed-Identity) 
  → PagerDuty alert to security team within 5 minutes
```

**Rotation mechanism:**
- Azure Function triggered weekly (SAS tokens) and quarterly (connection strings, passwords) via Timer trigger
- Function uses Managed Identity — no credentials stored in Function app configuration
- After each rotation: (1) verify new secret is readable, (2) post rotation summary to Teams channel, (3) update rotation timestamp tag on the secret
- Failed rotation: alert sent, old secret remains valid, runbook initiated

**Network security (production):**
```
Key Vault Networking:
  Public access: Disabled
  Allow from: Virtual Network — vnet-datalake-prod (contains Databricks, Function, App Service)
  Allow from: Trusted Microsoft services (enables diagnostic log export)
  Firewall: Add specific IP for emergency admin access via PIM
```

**Summary table:**

| Requirement | Solution |
|---|---|
| 3 environments isolated | 3 separate vaults; RBAC roles do not cross vault boundaries |
| 10M payments/day via Databricks | Managed Identity on cluster; DefaultAzureCredential; 5-min secret cache TTL |
| 1-year audit retention | Log Analytics (90 days active) + immutable Storage Account (7 years) |
| 90-day automated rotation | Azure Function with Timer trigger; Managed Identity; set_secret creates new version |
| No human reads prod secrets | PIM just-in-time for admin access; Managed Identities for all services |
| Purge protection | Enabled on prod vault; 90-day soft-delete retention |
| Network isolation | VNet integration; public access disabled in production |
