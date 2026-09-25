# Day 1 — Azure Key Vault: Fundamentals, Secrets, and Accessing Secrets from Python

## Overview

Every data pipeline eventually touches a secret — a database password, a SAS token, an API key. The beginner mistake is to paste those values directly into code or a config file and commit them to Git. The professional solution is **Azure Key Vault**: a managed, audited, access-controlled safe for every credential your pipeline needs.

Day 1 covers everything from creating your first Key Vault to reading secrets inside a Python script — without ever printing a secret value to the console.

**The 3 concepts:**
1. What is Azure Key Vault — why it exists, what it stores, and how to create one in the portal
2. Secrets Management — creating, versioning, and retrieving secrets; access control with RBAC
3. Accessing Secrets from Python — the `azure-keyvault-secrets` SDK, `DefaultAzureCredential`, and safe coding patterns

---

## Concept 1: What is Azure Key Vault?

### The core analogy

Think of Azure Key Vault as a **bank safe**, not a sticky note on your monitor.

A sticky note (environment variable in a `.env` file, hardcoded string, config file committed to Git) is convenient but insecure — anyone who sees the screen, clones the repo, or reads the CI logs gets the secret.

A bank safe (Key Vault) requires:
- **Identity verification** before the door opens (Azure AD authentication)
- **Permission to access** each individual box inside (RBAC or Access Policies)
- A **full audit log** of every person who opened it and what they took out (Azure Monitor / Diagnostic Logs)

And if someone's access card is compromised, you revoke *their card* — you don't change the lock on the entire building.

### What does Key Vault store?

Azure Key Vault stores three types of objects:

| Type | What it is | Examples |
|---|---|---|
| **Secrets** | Plain text values — any string you want to keep private | Database passwords, API keys, SAS tokens, connection strings |
| **Keys** | Cryptographic keys for encryption/decryption operations | RSA keys, AES keys used to encrypt data at rest |
| **Certificates** | TLS/SSL certificates — full lifecycle management (create, renew, import) | HTTPS certificates for web apps and APIs |

**In this course:** We focus on **Secrets** — the most common use case for data engineers. Keys and Certificates are used by security and DevOps teams.

### Standard vs. Premium tier

| Feature | Standard | Premium |
|---|---|---|
| Secrets storage | Yes | Yes |
| Software-protected keys | Yes | Yes |
| Hardware-protected keys (HSM) | No | Yes (FIPS 140-2 Level 2) |
| Price (approx.) | ~$0.03 per 10,000 operations | ~$1.00 per key per month + operations |
| Use case | All data engineering workloads | Regulatory compliance (PCI-DSS, HIPAA) requiring hardware key protection |

**For this course and most data engineering work:** Standard tier is sufficient. Premium is only needed when regulations require that cryptographic keys never leave a Hardware Security Module (HSM).

### How Key Vault fits into the data pipeline

```
Your pipeline (Python / Databricks / ADF)
         │
         │  "Give me the value of secret: adls-sas-token"
         ▼
  Azure Key Vault  ←── Azure AD checks: does this identity have permission?
         │
         │  Returns secret value over encrypted HTTPS
         ▼
  Your pipeline uses the secret value in memory
  (never written to disk, never logged, never committed to Git)
```

---

### Step-by-step: Create an Azure Key Vault in the portal

**Step 1 — Open the Azure Portal**
- Go to `https://portal.azure.com` and sign in

**Step 2 — Search for Key Vault**
- In the top search bar, type **"Key vaults"**
- Click **"Key vaults"** under Services

**Step 3 — Click Create**
- Click **"+ Create"** (top left)

**Step 4 — Fill in the Basics tab**

| Field | Value to enter |
|---|---|
| Subscription | Your "Free Trial" subscription |
| Resource group | `rg-datalake-dev` |
| Key vault name | `kv-datalake-dev-001` (must be globally unique, 3–24 chars, alphanumeric and hyphens only) |
| Region | **(Asia Pacific) Australia East** |
| Pricing tier | **Standard** |
| Days to retain deleted vaults | 90 (default — this is the soft-delete retention period) |
| Purge protection | **Enabled** (recommended — prevents permanent deletion during retention period) |

> **Naming note:** Key Vault names must be globally unique across all of Azure. If `kv-datalake-dev-001` is taken, try `kv-datalake-dev-002` or add your initials: `kv-datalake-hs-001`.

**Step 5 — Access configuration tab**

| Field | Value to enter |
|---|---|
| Permission model | **Azure role-based access control (RBAC)** ← select this |
| Resource access | Leave defaults unchecked |

> **Why RBAC?** RBAC is the modern, recommended model. The older "Vault access policy" model is being deprecated for new vaults. RBAC integrates with the same IAM system used by all other Azure resources — one consistent model for your entire platform.

**Step 6 — Networking tab**
- Public access: **Allow public access from all networks** (acceptable for dev; restrict to VNet in prod)
- Click **"Next: Tags"**

**Step 7 — Tags tab**

| Name | Value |
|---|---|
| Environment | dev |
| Project | datalake-course |
| Owner | your-name |

**Step 8 — Review + create**
- Confirm: Resource group = `rg-datalake-dev`, Region = Australia East, Pricing tier = Standard
- Click **"Create"**
- Deployment takes about 15–30 seconds

**Step 9 — Confirm it was created**
- Click **"Go to resource"**
- You should see the Key Vault overview page with:
  - **Vault URI:** `https://kv-datalake-dev-001.vault.azure.net/` (save this — you need it for Python)
  - **Subscription**, **Resource group**, **Location** all correct

> **Checkpoint:** You should see the Vault URI on the overview page. Copy it and save it — your Python code will use this URL to connect to the vault.

---

### Step-by-step: Assign yourself the Key Vault Secrets Officer role

Before you can create or read secrets, you must assign yourself permission. With RBAC, this is done through IAM — just like storage accounts.

**Step 1 — Go to your Key Vault (`kv-datalake-dev-001`)**

**Step 2 — Click "Access control (IAM)"** in the left menu

**Step 3 — Click "Add role assignment"**

**Step 4 — Select role**
- Search for **"Key Vault Secrets Officer"**
- Select it → Click **"Next"**

| Role | What it allows |
|---|---|
| Key Vault Secrets Officer | Create, read, update, delete secrets (but NOT keys or certificates) |
| Key Vault Secrets User | Read secrets only — what your application's Managed Identity gets |
| Key Vault Administrator | Full control over secrets, keys, certificates, and vault settings |
| Key Vault Reader | Read vault metadata (names, properties) but NOT secret values |

**Step 5 — Select member**
- Click **"+ Select members"**
- Search for your own email address → select it → click **"Select"**
- Click **"Review + assign"** → **"Review + assign"** again

**Step 6 — Confirm**
- Go to the **"Role assignments"** tab
- You should see your email listed under **"Key Vault Secrets Officer"**

> **Checkpoint:** Your email appears under Key Vault Secrets Officer. Without this step, the portal will deny you when you try to create a secret.

---

## Concept 2: Secrets Management

### What is a secret in Key Vault?

A **secret** is any string value you want to store securely. Key Vault adds:
- **Versioning:** Every time you update a secret, the old value is preserved as a previous version. You can always roll back.
- **Lifecycle controls:** Set an activation date (not valid before this date) and an expiry date (automatically disabled after this date).
- **Enabled/disabled state:** Disable a secret without deleting it — useful for rotating credentials.
- **Metadata tagging:** Add tags to secrets for organisation (e.g., `team: data-engineering`).

### Secret versioning

```
Secret name: adls-sas-token

Version history:
├── v3 (current)  → value: "sv=2023-11-03&ss=b&srt=co&sp=rwdlacupx&..."  [created 2026-09-01]
├── v2            → value: "sv=2023-08-03&ss=b&srt=co&sp=r&..."           [created 2026-06-01]
└── v1            → value: "sv=2022-11-03&ss=b&srt=co&sp=r&..."           [created 2026-01-01]
```

When your Python code calls `get_secret("adls-sas-token")` it always gets the **current (latest enabled) version** — so rotating the secret means updating it in Key Vault, not redeploying your application.

### Access Policies vs. RBAC — which to use?

This is a common interview question. Key Vault supports two permission models:

| | Vault Access Policies (legacy) | Azure RBAC (recommended) |
|---|---|---|
| Where configured | Inside the Key Vault → Access policies | Azure IAM (same as storage, VMs, etc.) |
| Granularity | Per-vault only — can't scope to individual secrets | Per-vault, per-secret, or by tag |
| Consistency | Separate from all other Azure RBAC | One unified permission system for all Azure resources |
| Audit | Key Vault diagnostic logs | Azure Activity Log + Key Vault diagnostic logs |
| Microsoft recommendation | Migrating away | **Use this for all new vaults** |
| Max policies per vault | 1,024 | Unlimited (Azure AD groups handle scale) |

**Rule: Always use RBAC for new Key Vaults.** If you see a vault using Access Policies, it was created before RBAC was available or by someone following outdated documentation.

---

### Step-by-step: Store the `adls-sas-token` secret

**Step 1 — Go to your Key Vault (`kv-datalake-dev-001`)**

**Step 2 — Click "Secrets"** in the left menu (under Objects)

**Step 3 — Click "+ Generate/Import"**

**Step 4 — Fill in the secret details**

| Field | Value |
|---|---|
| Upload options | **Manual** |
| Name | `adls-sas-token` |
| Secret value | Paste the SAS token string from your ADLS Gen2 account (or use: `sv=2026-demo&sig=placeholder`) |
| Content type | `application/x-www-form-urlencoded` (optional, for documentation) |
| Set activation date? | Leave unchecked (active immediately) |
| Set expiration date? | Check this → set to 30 days from today |
| Enabled | **Yes** |

> **Naming convention for secrets:** Use lowercase with hyphens: `adls-sas-token`, `db-password`, `event-hub-connection-string`. No underscores, no dots. Keep names descriptive but not so long they are hard to type.

**Step 5 — Click "Create"**
- The secret appears in the list with status **"Enabled"**

> **Checkpoint:** You should see `adls-sas-token` in the secrets list. The "Current version" column shows a 32-character GUID — that is the version identifier for this value.

---

### Step-by-step: Store the `db-password` secret

**Step 1 — Click "+ Generate/Import"** again

**Step 2 — Fill in the details**

| Field | Value |
|---|---|
| Name | `db-password` |
| Secret value | `MyStr0ng$ecretPass!` (a placeholder for this exercise) |
| Set expiration date? | Check this → set to 90 days from today |
| Enabled | **Yes** |

**Step 3 — Click "Create"**

---

### Step-by-step: Retrieve a secret via the portal

**Step 1 — Click on `adls-sas-token`** in the secrets list

**Step 2 — Click on the current version** (the GUID shown under "Current version")

**Step 3 — Click "Show Secret Value"**
- The value appears in the field
- Notice the portal shows: Created date, Updated date, Expiry date, Enabled status

**Step 4 — Observe version history**
- Click the back arrow to the secret overview
- You will see one version listed. After you update the secret, more versions appear here.
- Click **"New Version"** to simulate a rotation without losing the old value

> **Checkpoint:** You can see the secret value in the portal. Note that every time you click "Show Secret Value", this access is logged in Key Vault's diagnostic logs — full auditability.

---

### Step-by-step: Retrieve a secret via Azure CLI

Install the Azure CLI from `https://learn.microsoft.com/en-us/cli/azure/install-azure-cli` if you haven't already.

**Step 1 — Log in**
```bash
az login
```
A browser window opens. Sign in with your Azure account.

**Step 2 — Set your subscription (if you have multiple)**
```bash
az account set --subscription "Free Trial"
```

**Step 3 — Retrieve the secret value**
```bash
az keyvault secret show \
  --vault-name kv-datalake-dev-001 \
  --name adls-sas-token \
  --query value \
  --output tsv
```

The `--query value` extracts only the secret value from the JSON response. `--output tsv` prints it as plain text (no quotes). This is the pattern used in shell scripts and CI/CD pipelines.

**Step 4 — List all secrets (names only — not values)**
```bash
az keyvault secret list \
  --vault-name kv-datalake-dev-001 \
  --output table
```

> **Checkpoint:** You should see the secret value printed in your terminal. In a CI/CD pipeline (GitHub Actions, Azure DevOps), you would store this in a masked pipeline variable — never echo it to the build log.

---

### Step-by-step: Observe the disabled state (expiry simulation)

**Step 1 — Go to `db-password`** in the portal

**Step 2 — Click on the current version**

**Step 3 — Click "Edit"** (pencil icon at the top)

**Step 4 — Change settings**
- **Set activation date:** Check it → set to tomorrow's date (secret not active until tomorrow)
- Click **"Apply"**

**Step 5 — Try to show the secret value**
- Click **"Show Secret Value"**
- The portal shows the value — but note the "Enabled: No" or "Not yet active" indicator
- When your Python code tries to read this secret, it will receive an error: `SecretDisabledException`

**Step 6 — Re-enable it**
- Return to the Edit screen → Uncheck "Set activation date" → Apply

> **Key insight:** Disabling a secret (or letting it expire) does not delete it. The previous versions and current version are still in the vault — they just cannot be read until re-enabled. This is how you gracefully rotate credentials without service interruption.

---

## Concept 3: Accessing Secrets from Python

### Required packages

```bash
pip install azure-keyvault-secrets azure-identity python-dotenv
```

| Package | Purpose |
|---|---|
| `azure-keyvault-secrets` | Client library for reading/writing secrets in Key Vault |
| `azure-identity` | Provides `DefaultAzureCredential` — handles all Azure authentication methods |
| `python-dotenv` | Loads `.env` file into environment variables (for local development) |

### What is DefaultAzureCredential?

`DefaultAzureCredential` is an authentication chain that tries multiple credential sources in order:

```
DefaultAzureCredential tries (in order):
1. EnvironmentCredential      → reads AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID
2. WorkloadIdentityCredential → for Kubernetes / AKS workloads
3. ManagedIdentityCredential  → for Azure VMs, App Service, Azure Functions, Databricks
4. SharedTokenCacheCredential → uses tokens cached by Visual Studio / VS Code
5. VisualStudioCodeCredential → reads from VS Code's logged-in Azure account
6. AzureCliCredential         → uses the token from `az login` ← this is what we use locally
7. AzurePowerShellCredential  → uses the token from Connect-AzAccount (PowerShell)
```

**Why this matters:** The same code works locally (using `az login` token) and in production (using Managed Identity) — without any code changes. You never hardcode credentials.

---

### Project structure for this course

```
azure-ev-end-to-end-project/
├── .env                          ← local only, in .gitignore
│     AZURE_KEY_VAULT_NAME=kv-datalake-dev-001
│
└── azure-key-vault-service/
    └── day1-key-vault-fundamentals/
        └── keyvault_demo.py
```

Your `.env` file (never committed to Git):
```
AZURE_KEY_VAULT_NAME=kv-datalake-dev-001
```

---

### Python code: Read a secret

```python
# keyvault_demo.py
import os
from dotenv import load_dotenv
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

# Load environment variables from .env file (local development only)
load_dotenv()

# Read vault name from environment — never hardcode it
vault_name = os.environ["AZURE_KEY_VAULT_NAME"]
vault_url = f"https://{vault_name}.vault.azure.net/"

# DefaultAzureCredential works locally (az login) and in production (Managed Identity)
credential = DefaultAzureCredential()
client = SecretClient(vault_url=vault_url, credential=credential)

# Read a secret
retrieved_secret = client.get_secret("adls-sas-token")

# Use the value in memory — NEVER log it
sas_token = retrieved_secret.value
print(f"Secret name: {retrieved_secret.name}")
print(f"Secret version: {retrieved_secret.properties.version}")
print(f"Expires on: {retrieved_secret.properties.expires_on}")
# DO NOT print: print(f"Secret value: {sas_token}")  ← never do this

# Pass the value to your pipeline
print("SAS token loaded successfully. Using it to connect to ADLS Gen2...")
```

---

### Python code: List all secret names

```python
# List all secrets in the vault (names only — not values)
print("\nAll secrets in the vault:")
secrets = client.list_properties_of_secrets()
for secret_props in secrets:
    print(f"  - {secret_props.name} | enabled: {secret_props.enabled} | expires: {secret_props.expires_on}")
```

Output:
```
All secrets in the vault:
  - adls-sas-token | enabled: True | expires: 2026-10-25 00:00:00+00:00
  - db-password    | enabled: True | expires: 2026-12-24 00:00:00+00:00
```

> **Note:** `list_properties_of_secrets()` returns metadata only — names, versions, enabled state, expiry. Secret values are never returned in bulk. You must call `get_secret(name)` individually for each value you need. This is by design — auditing a bulk value dump would be impossible.

---

### Python code: Set a secret from Python

```python
# Create or update a secret from Python
new_secret = client.set_secret(
    name="event-hub-connection-string",
    value="Endpoint=sb://evh-payments-dev.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=...",
)
print(f"Secret set: {new_secret.name} (version: {new_secret.properties.version})")
```

> **When to use this:** In a secrets rotation pipeline — a Python script that generates new SAS tokens daily, writes them to Key Vault, and the application always reads the latest version.

---

### Python code: Full demo script

```python
# keyvault_demo.py — complete working script
import os
from dotenv import load_dotenv
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

def get_secret_client() -> SecretClient:
    """Create and return an authenticated SecretClient."""
    load_dotenv()
    vault_name = os.environ.get("AZURE_KEY_VAULT_NAME")
    if not vault_name:
        raise ValueError("AZURE_KEY_VAULT_NAME is not set in environment or .env file")
    vault_url = f"https://{vault_name}.vault.azure.net/"
    credential = DefaultAzureCredential()
    return SecretClient(vault_url=vault_url, credential=credential)


def read_secret(client: SecretClient, secret_name: str) -> str:
    """Read a secret value. Never log the returned value."""
    secret = client.get_secret(secret_name)
    return secret.value


def list_secret_names(client: SecretClient) -> list[str]:
    """Return a list of all enabled secret names in the vault."""
    return [
        s.name
        for s in client.list_properties_of_secrets()
        if s.enabled
    ]


def set_secret(client: SecretClient, name: str, value: str) -> str:
    """Create or update a secret. Returns the new version ID."""
    result = client.set_secret(name=name, value=value)
    return result.properties.version


if __name__ == "__main__":
    client = get_secret_client()

    # List all secrets
    names = list_secret_names(client)
    print(f"Secrets in vault: {names}")

    # Read a specific secret
    sas_token = read_secret(client, "adls-sas-token")
    print(f"adls-sas-token loaded (length: {len(sas_token)} chars)")
    # Do NOT print the value itself

    # Set a new secret
    version = set_secret(client, "event-hub-connection-string", "Endpoint=sb://demo...")
    print(f"event-hub-connection-string created, version: {version}")
```

---

### Safe coding rules for secrets

| Rule | Why |
|---|---|
| Never `print(secret.value)` or `log(secret_value)` | Log files are often shipped to centralised logging — secrets appear in plaintext in Splunk, Datadog, etc. |
| Never store secret values in variables named generically (`data`, `result`) when you might later print that variable | Makes accidental logging easy |
| Never pass secrets as function arguments in positional form | They appear in stack traces on exceptions |
| Always load vault name from environment (`os.environ`) | If you hardcode the vault URL, rotating vaults requires a code change |
| Use `try/except` around `get_secret` | Expired or disabled secrets raise `ResourceNotFoundError` — your pipeline should fail gracefully with a clear error, not a raw exception |
| In production, use Managed Identity — never store client credentials | Managed Identity needs no secret to authenticate — it eliminates the "secret to access secrets" problem |

```python
# Correct pattern: graceful error handling
from azure.core.exceptions import ResourceNotFoundError

try:
    sas_token = read_secret(client, "adls-sas-token")
except ResourceNotFoundError:
    raise RuntimeError(
        "Secret 'adls-sas-token' not found or is disabled in Key Vault. "
        "Check the secret name and ensure it is enabled and not expired."
    )
```

---

## Summary

| Concept | Key takeaway |
|---|---|
| Azure Key Vault | A managed safe for secrets, keys, and certificates — with access control, versioning, and full audit logs |
| Secrets vs. Keys vs. Certificates | Secrets = any string (passwords, tokens); Keys = cryptographic keys; Certificates = TLS certs |
| Standard vs. Premium | Standard for all data engineering; Premium only when hardware-backed keys are a compliance requirement |
| RBAC vs. Access Policies | Always use RBAC for new vaults — it is the modern, unified permission model |
| Secret versioning | Updating a secret creates a new version; old versions are retained and accessible; applications always read the latest |
| Expiry and disabled state | Expired or disabled secrets cannot be read by code but are not deleted — re-enable without data loss |
| DefaultAzureCredential | One credential object that works with `az login` locally and Managed Identity in production — no code changes needed |
| Never log secret values | Log files are persistent and often centralised — a logged secret is a leaked secret |
| Load vault name from `.env` | Keep vault URL out of source code; environment-specific config belongs in the environment |
