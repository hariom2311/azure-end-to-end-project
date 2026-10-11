# Day 1 — Interview Solutions: Kickoff, Architecture, and Azure Setup

---

## Azure Resources and Setup

**A1**
A Resource Group is a logical container inside an Azure subscription. It groups related resources — storage, Key Vault, Databricks, ADF — under one name. Reasons to keep one project in one Resource Group:
- **Lifecycle management:** delete the group → all resources inside are deleted in one operation. No hunting for individual resources to clean up.
- **Cost visibility:** Azure Cost Management can filter costs by Resource Group — instantly see what this project is spending.
- **Access control:** you can assign RBAC roles at the group level — one assignment covers all resources inside.
- **Region scope:** a Resource Group has a default region but resources inside can be in any region.

---

**A2**

| | Azure Blob Storage | ADLS Gen2 |
|---|---|---|
| Folder model | Flat — no real folder hierarchy | Hierarchical namespace (HNS) — real directories |
| Protocol | `wasbs://` | `abfss://` |
| Spark performance | Slower for analytics — simulate folders with `/` in names | Faster — directory operations are atomic, metadata efficient |
| Required for | Simple object storage | Large-scale analytics, Databricks, Spark |

This project uses **ADLS Gen2** (`evdatalakedev`) for Bronze/Silver/Gold because Spark reads and writes perform better with a true hierarchical file system. `abfss://` is the correct protocol for ADLS Gen2. The instructor's shared storage (`dataenggdailystorage`) is plain Blob Storage — accessed via `wasbs://` + SAS token.

---

**A3**
A resource provider is an Azure service namespace that must be registered to your subscription before you can create that type of resource. Example: if `Microsoft.KeyVault` is not registered and you try to create a Key Vault, you get:
```
MissingSubscriptionRegistration: The subscription is not registered to use namespace 'Microsoft.KeyVault'
```
The fix is `az provider register --namespace Microsoft.KeyVault`, then wait ~1 minute for `registrationState` to become `Registered`. This registration is permanent — you only do it once per subscription.

---

**A4**
**Cost difference:**
- LRS (Locally Redundant Storage): 3 copies in one data centre — cheapest
- ZRS (Zone Redundant Storage): 3 copies across 3 availability zones in the same region — ~2× the cost of LRS

**Fix:** You cannot change redundancy on an existing storage account from the Portal for all tiers. Options:
1. Create a new storage account with LRS, migrate data (azcopy), then delete the old account
2. Some regions allow changing via CLI: `az storage account update --name evdatalakedev --sku Standard_LRS`

---

**A5**
Without hierarchical namespace, the storage account is plain Blob Storage. `abfss://` requires ADLS Gen2 with HNS enabled. When Databricks tries to use `abfss://container@account.dfs.core.windows.net/`, the connection will fail with something like `No such file or directory` or `ADLS Gen2 is not enabled on this account`. The fix: the engineer must either (a) delete and recreate the storage account with HNS ON, or (b) switch to `wasbs://` with an access key — but `wasbs://` is the less secure, legacy approach. HNS cannot be enabled on an existing account retroactively.

---

## Key Vault and Secrets

**A6**
Azure Key Vault is a cloud-managed vault that stores secrets (passwords, API keys, connection strings) in encrypted form. Only identities with an explicit RBAC role (`Key Vault Secrets User` or `Secrets Officer`) can read from it.

Why not hardcode credentials in notebooks:
- If the notebook is committed to git, the credential leaks immediately
- GitHub's push protection will block the push if it detects a token/secret pattern
- If a credential is compromised, rotating it in Key Vault fixes every notebook instantly — no code change needed
- Databricks masks any value read via `dbutils.secrets.get()` as `[REDACTED]` in all logs and outputs — impossible to accidentally print in plaintext

---

**A7**
When Key Vault uses the RBAC permission model (not Vault access policies), even the account that created the vault has no permissions by default. Creating a resource does not automatically grant you data-plane access. The engineer missed assigning the `Key Vault Secrets Officer` role to their own account via IAM before trying to set secrets. Fix: Portal → Key Vault → **Access Control (IAM)** → **+ Add role assignment** → role: `Key Vault Secrets Officer` → assign to your account → wait 1–2 minutes.

---

**A8**
Azure Key Vault has a **soft-delete** feature enabled by default. When you delete a secret, it enters a deleted-but-recoverable state for a retention period (default 90 days). During this period, you cannot create a new secret with the same name — you get the `ObjectIsDeletedButRecoverable` error.

**Fix — recover the deleted secret:**
```cmd
az keyvault secret recover --vault-name kv-ev-intelligence-dev --name "<secret-name>"
```
Then update it to the new value.

**Or permanently purge it first (if you don't need the old version):**
```cmd
az keyvault secret purge --vault-name kv-ev-intelligence-dev --name "<secret-name>"
```
Then create fresh with `az keyvault secret set`.

---

**A9**
The secret has been read correctly. `[REDACTED]` is Databricks' security feature — when a notebook calls `dbutils.secrets.get()`, the returned string is marked internally as a secret. Any attempt to print it — via `print()`, `display()`, logging — is intercepted by Databricks and replaced with `[REDACTED]`. The variable still holds the real value. Code that uses it (like `spark.conf.set(...)` or `requests.get(headers=...)`) works correctly with the actual secret value. This is by design and expected.

---

## Service Principal and RBAC

**A10**
A Service Principal is a non-human Azure Active Directory (Entra ID) identity — essentially a robot account for applications. Databricks and ADF use it instead of a human account because:
- A human account's password can change (reset, expiry, employee leaves) — breaks all pipelines
- A human account's permissions may change — breaks pipelines unexpectedly
- The SP is dedicated to the application — its permissions are exactly what the application needs, nothing more
- SP credentials can be rotated without affecting any human accounts
- If the SP credential is compromised, you rotate just that credential — no impact on humans

---

**A11**
The `password` (client secret) is shown only once — at the moment of creation. Azure does not store it in retrievable form. If navigated away without copying: **there is no way to retrieve it**. The engineer must go to Portal → **App registrations** → `sp-ev-intelligence-dev` → **Certificates & secrets** → delete the old secret → **+ New client secret** → copy the new value → store it in Key Vault immediately. The `appId` (Client ID) and `tenant` (Tenant ID) are always visible in the app's Overview page and are not affected.

---

**A12**
**Extra capability of Owner vs Contributor:**
`Storage Blob Data Owner` also grants the ability to manage **POSIX ACLs (Access Control Lists)** on individual files and folders in ADLS Gen2. `Storage Blob Data Contributor` can read, write, and delete files — which is all Databricks needs.

**Risk:** If the SP's credential is ever compromised, an attacker with Owner access could modify file permissions on every file in the storage account, potentially locking out legitimate users or granting themselves access to sensitive data in other containers. `Contributor` limits blast radius to just the files themselves.

---

**A13**
The most likely cause: the SP was created but the `Storage Blob Data Contributor` role was never assigned to the storage account. The SP has a valid credential (Client ID + Secret) and can authenticate to Azure Entra ID successfully — but once authenticated, it has no authorization on the storage resource, hence 403. Fix: Portal → `evdatalakedev` → **Access Control (IAM)** → **Add role assignment** → `Storage Blob Data Contributor` → assign to the SP → wait 1–2 minutes → retry.

---

**A14**
- **Application (Client) ID** (`appId`): the SP's identity in Azure Entra ID — used in `spark.conf.set(...)` as the OAuth client identifier when Databricks presents credentials to Entra ID.
- **Object ID** (`id`): the SP's unique identifier within the Azure resource management system — used when creating role assignments because RBAC works on objects, not applications.

For `az role assignment create --assignee-object-id`, you must use the **Object ID** (from `az ad sp show --id <appId> --query id -o tsv`), not the Application (Client) ID. Using the wrong ID silently assigns the role to nothing or fails with an error.

---

## Databricks Cluster and Secret Scope

**A15**
A Secret Scope is a Databricks object that creates a bridge between a Databricks workspace and an Azure Key Vault. Once created, any notebook in the workspace can call `dbutils.secrets.get(scope="kv-ev-scope", key="some-secret")` to read a Key Vault secret.

How it works:
1. You create the scope by providing the Key Vault's Vault URI and Resource ID
2. Databricks registers this as a named scope (`kv-ev-scope`)
3. When a notebook calls `dbutils.secrets.get(scope, key)`, Databricks uses the workspace's managed identity (backed by the `AzureDatabricks` enterprise application) to call the Key Vault API and retrieve the secret
4. The secret value is returned to the notebook but masked in all output as `[REDACTED]`

---

**A16**
**Standard (Shared) mode** is a multi-tenant cluster mode where Databricks restricts certain filesystem operations to protect other users running on the same cluster. `dbutils.fs.mount()` is one of the blocked operations — calling it returns `Method public dbutils.mount() is not whitelisted`.

**Fix:** Edit the cluster → change Access mode from **Standard** to **Dedicated (Single user)** → confirm → restart the cluster. On Dedicated mode, only one user's workloads run on the cluster, so all `dbutils` methods are available. Alternatively, switch to Method 1 (SP OAuth direct access) which does not use `mount()` at all and works on any cluster mode.

---

**A17**
The most likely cause: the notebook's Python variables were cleared. Spark config is a session-level setting — it persists as long as the cluster is running. But if the engineer started a new notebook cell without running the earlier cells that set the config and loaded the variables, the Python variable `storage_account` does not exist in the new cell's scope. The fix: run the setup cells (Cell 1 and Cell 2) in order from the top before running any other cells. Using `%run "./00b_connect_storage_no_mount"` as the first cell in any notebook auto-runs the setup.

---

**A18**
**Dedicated mode** means only one user's code runs on the cluster — your account. This is required for:
- `dbutils.fs.mount()` — which this project uses in the legacy notebook
- Full filesystem visibility — other users' mounted paths are not visible
- Unrestricted `dbutils` access

**Shared mode** is Databricks' default for collaborative clusters. It isolates users at the session level to prevent one user from reading another user's data. As part of this isolation, it blocks `dbutils.fs.mount()` — because a mount in Shared mode would expose the mounted path to ALL users on the cluster, violating the isolation guarantee.

---

**A19**
The engineer's account has `Key Vault Secrets Officer` — that grants the human user access. But when a Databricks notebook reads a secret via the secret scope, it is the **Databricks workspace's service identity** (`AzureDatabricks` enterprise application) that makes the actual Key Vault API call, not the human user. The `AzureDatabricks` global service principal needs `Key Vault Secrets User` role on the Key Vault.

Fix: Portal → Key Vault → **Access Control (IAM)** → **+ Add role assignment** → role: `Key Vault Secrets User` → Members: search **`AzureDatabricks`** (the built-in global Databricks service principal) → assign → wait 2 minutes → retry.

---

## ADLS Gen2 Connection Methods

**A20**
`spark.conf.set(key, value)` sets a configuration value on the active Spark session. For SP OAuth, it tells the Azure ABFS (Azure Blob File System) connector which OAuth credentials to use when accessing a specific storage account. The configuration is **session-scoped** — it is cleared when the cluster restarts or the Spark session ends. This means: after every cluster restart, you must re-run the cells that call `spark.conf.set()`. This is why the project uses `%run "./00b_connect_storage_no_mount"` as the first cell in every other notebook — to re-establish the config automatically.

---

**A21**
```python
def abfss(container: str, path: str = "") -> str:
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base
```

`abfss` stands for **Azure Blob File System Secure** — it is the protocol for ADLS Gen2 over HTTPS. It requires that the storage account has **hierarchical namespace enabled** (making it a true Gen2 data lake, not plain blob storage). The `wasbs://` protocol is for classic Azure Blob Storage (non-hierarchical) and is used with SAS tokens — like when accessing the shared source blob `dataenggdailystorage` which is a standard blob account without HNS.

---

**A22**
SAS tokens only work with `wasbs://` (Windows Azure Storage Blob Service protocol), not `abfss://`. The `abfss://` protocol is for ADLS Gen2 with OAuth authentication. Using `abfss://` with a SAS token causes `InvalidAuthenticationInfo` because the connector expects an OAuth flow, not a SAS token.

Fix: replace `abfss://` with `wasbs://` in all paths for the shared source storage:
```python
# Wrong
f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/..."

# Correct
f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/..."
```

---

**A23**

| | Method 1 — SP OAuth Direct | Method 4 — Mount + SP OAuth |
|---|---|---|
| Uses `spark.conf.set` | Yes | Yes (inside the mount config) |
| Uses `dbutils.fs.mount()` | No | Yes |
| Works on Shared/Standard mode | Yes | No — blocked |
| Works on Serverless | Yes | No — blocked |
| Path style | `abfss://container@account.dfs.core.windows.net/` | `/mnt/container/` |
| Re-run on restart | 3 cells (~30 seconds) | Run mount notebook |
| Deprecated | No — current approach | Yes — being phased out |

Mounting is blocked on Standard, Shared, and Serverless clusters because `dbutils.fs.mount()` creates a cluster-wide mount that all users on a shared cluster can see — violating user isolation. Method 1 is the recommended approach: zero dependency on cluster mode, no mount lifecycle to manage, compatible with Unity Catalog.

---

## API Auth and Architecture

**A24**
Complete auth flow:

1. **Key Vault → Credentials:** Notebook reads `voltgrid-username` and `voltgrid-password` from Key Vault via `dbutils.secrets.get()`. Both values stay in memory — never printed, never written to disk.

2. **Login request:** `requests.post(f"{api_base_url}/api/auth/login/", json={"username": username, "password": password}, timeout=10)` — sends credentials as a JSON POST body over HTTPS.

3. **Token received:** API responds with `{"token": "abcdef..."}`. The token is extracted and stored as a Python variable — in memory only, never persisted.

4. **All subsequent calls:** Every API call adds `Authorization: Token <value>` as a header. The password is never sent again.

5. **End of session:** When the notebook finishes or the cluster restarts, the token variable is cleared automatically.

**Why the token is more secure than sending the password every time:**
- If the token is intercepted or leaked, it can be invalidated server-side without changing the password
- The password (the "master credential") travels only once — reducing the attack surface
- Tokens can have shorter lifetimes than passwords, reducing exposure window

---

**A25**
Full design for a retail analytics platform:

**Resources to create on Day 1:**

| Resource | Name | Configuration |
|---|---|---|
| Resource Group | `rg-retail-analytics-dev` | Central India |
| ADLS Gen2 | `retaildatalakedev` | Standard LRS, HNS ON, Cool tier |
| Containers | `bronze`, `silver`, `gold`, `source` | Private access |
| Key Vault | `kv-retail-dev` | Standard tier, RBAC model |
| Service Principal | `sp-retail-dev` | Created via `az ad sp create-for-rbac` |
| Databricks Workspace | `dbw-retail-dev` | Trial tier, Central India |
| Databricks Cluster | `dev-cluster` | Single Node, Dedicated, DS3_v2, Photon OFF, 15-min auto-terminate |

**Naming convention:** `<resource-type>-<project>-<environment>` — e.g. `kv-retail-dev`, `sp-retail-dev`, `rg-retail-analytics-dev`.

**Secrets strategy:**

| Secret | Key Vault Name | Notes |
|---|---|---|
| SQL connection string | `sql-connection-string` | Full JDBC connection string — never in code |
| External API key | `vendor-api-key` | Stored as plain string — notebooks call `dbutils.secrets.get()` |
| Storage account name | `adls-account-name` | Even non-sensitive values go in Key Vault for centralisation |

**Databricks → Storage connection:** SP OAuth direct access (`Method 1`) — SP Client ID, Secret, and Tenant ID stored in Key Vault, read via secret scope, passed to `spark.conf.set()`.

**Identity roles:**

| Identity | Key Vault role | Storage role |
|---|---|---|
| Your user account | `Key Vault Secrets Officer` | `Storage Blob Data Contributor` |
| `AzureDatabricks` global app | `Key Vault Secrets User` | None (reads via SP OAuth) |
| `sp-retail-dev` (SP) | `Key Vault Secrets User` | `Storage Blob Data Contributor` |
| ADF Managed Identity | `Key Vault Secrets User` | `Storage Blob Data Contributor` |

---
