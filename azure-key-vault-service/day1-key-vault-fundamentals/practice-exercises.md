# Day 1 — Practice Exercises: Azure Key Vault Fundamentals

> All exercises are done in the **Azure Portal** (`https://portal.azure.com`) and your local terminal.  
> Complete them in order — each exercise builds on the previous one.  
> Estimated total time: 75–100 minutes.

---

## Exercise 1 — Create Your Key Vault

**Concept:** Azure Key Vault — creation, configuration, and access control

**Tasks:**

**1a. Create the Key Vault**
- Go to `https://portal.azure.com` → search for **"Key vaults"** → click **"+ Create"**
- Configure with the following settings:

| Field | Value |
|---|---|
| Resource group | `rg-datalake-dev` (create it first if it doesn't exist) |
| Key vault name | `kv-datalake-dev-001` (globally unique — add initials if taken) |
| Region | Australia East |
| Pricing tier | Standard |
| Soft-delete retention | 90 days |
| Purge protection | Enabled |
| Permission model | **Azure role-based access control (RBAC)** |

- Click **"Review + create"** → **"Create"**
- Once deployed, click **"Go to resource"**
- Copy the **Vault URI** from the overview page (format: `https://kv-datalake-dev-001.vault.azure.net/`) — you will use this in every exercise

**1b. Assign yourself the Key Vault Secrets Officer role**
- Inside the vault, click **"Access control (IAM)"**
- Click **"+ Add role assignment"**
- Role: **Key Vault Secrets Officer**
- Member: your own email address
- Assign → confirm your email appears in the Role assignments tab

**1c. Understand the role hierarchy**
Research and fill in this table (use the portal's role assignment wizard to browse roles):

| Role | Read secret values? | Create/update secrets? | Manage access (IAM)? |
|---|---|---|---|
| Key Vault Reader | ? | ? | ? |
| Key Vault Secrets User | ? | ? | ? |
| Key Vault Secrets Officer | ? | ? | ? |
| Key Vault Administrator | ? | ? | ? |

**1d. Tag the Key Vault**
- Go to the vault → **Tags** (in the left menu)
- Add:

| Name | Value |
|---|---|
| Environment | dev |
| Project | datalake-course |
| Owner | your-name |

- Save the tags
- Discuss: why is the `Environment` tag particularly important when you have Key Vaults for dev, staging, and prod?

**1e. Explore the vault overview**
- Note the "Vault URI" — this is the endpoint your Python code will call
- Click **"Secrets"** in the left menu — it should be empty
- Click **"Keys"** — empty
- Click **"Certificates"** — empty
- Confirm the vault has no secrets yet. You will create them in Exercise 2.

---

## Exercise 2 — Store 3 Secrets and Observe Versioning

**Concept:** Secrets Management — creating, versioning, and lifecycle

**Tasks:**

**2a. Create the `adls-sas-token` secret**
- Go to your vault → **Secrets** → **"+ Generate/Import"**
- Configure:

| Field | Value |
|---|---|
| Upload options | Manual |
| Name | `adls-sas-token` |
| Secret value | `sv=2026-demo&ss=b&srt=co&sp=rwdlacupx&se=2026-10-25&spr=https&sig=PLACEHOLDER` |
| Set expiration date | Check → set to 30 days from today |
| Enabled | Yes |

- Click **"Create"**
- Confirm it appears in the list with status **"Enabled"**

**2b. Create the `db-password` secret**
- Click **"+ Generate/Import"**
- Name: `db-password`
- Value: `MyStr0ng$ecretPass!`
- Set expiration date: 90 days from today
- Enabled: Yes
- Create

**2c. Create the `event-hub-connection-string` secret**
- Click **"+ Generate/Import"**
- Name: `event-hub-connection-string`
- Value: `Endpoint=sb://evh-payments-dev.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=PLACEHOLDER`
- No expiration (leave unchecked)
- Enabled: Yes
- Create

**2d. Verify all 3 secrets exist**
- The Secrets list should now show:
  - `adls-sas-token` — Enabled, with expiry date
  - `db-password` — Enabled, with expiry date
  - `event-hub-connection-string` — Enabled, no expiry

**2e. Simulate a secret rotation (versioning)**
- Click on `adls-sas-token`
- Click **"New Version"** (or `"+ Generate/Import"` with the same name)
- Enter a new (updated) secret value: `sv=2026-demo-v2&ss=b&srt=co&sp=rwdlacupx&se=2026-11-25&spr=https&sig=PLACEHOLDER_V2`
- Set a new expiration date 60 days from today
- Click **"Create"**
- Now click on `adls-sas-token` again — you should see **2 versions listed**
- Click on each version and confirm they have different values
- Answer: if a Python application calls `get_secret("adls-sas-token")` right now, which version does it get? What does that mean for rotating secrets in production?

**2f. Disable a secret**
- Go to `db-password` → click the current version → click **"Edit"** (pencil icon)
- Change **Enabled** to **No**
- Click **"Apply"**
- Return to the Secrets list — observe the status changed to **"Disabled"**
- Answer: what happens when Python code tries to retrieve `db-password` now?
- Re-enable it: click on the secret → current version → Edit → Enabled: Yes → Apply

---

## Exercise 3 — Retrieve Secrets via Azure CLI

**Concept:** Secrets retrieval, CLI automation, and safe output handling

**Tasks:**

**3a. Install and authenticate the Azure CLI**
- Download from: `https://learn.microsoft.com/en-us/cli/azure/install-azure-cli`
- Open your terminal and run:
```bash
az login
```
- A browser window opens — sign in with your Azure account
- Confirm your terminal shows your subscription name and ID

**3b. Show a single secret value**
```bash
az keyvault secret show \
  --vault-name kv-datalake-dev-001 \
  --name adls-sas-token \
  --query value \
  --output tsv
```
- Confirm you see the secret value printed
- Now run it WITHOUT `--query value --output tsv`:
```bash
az keyvault secret show \
  --vault-name kv-datalake-dev-001 \
  --name adls-sas-token
```
- Observe the full JSON response: what other fields does it return besides the value?

**3c. List all secret names**
```bash
az keyvault secret list \
  --vault-name kv-datalake-dev-001 \
  --output table
```
- Confirm you see all 3 secret names (but NOT their values)
- Answer: why does `az keyvault secret list` not return secret values?

**3d. Retrieve a specific version of a secret**
- In the portal, go to `adls-sas-token` and copy the version GUID for version 1 (the older one)
- Run:
```bash
az keyvault secret show \
  --vault-name kv-datalake-dev-001 \
  --name adls-sas-token \
  --version <paste-version-guid-here> \
  --query value \
  --output tsv
```
- Confirm you get the OLD value (version 1), not the current one
- Answer: in what scenario would retrieving a specific version (not the latest) be useful?

**3e. Show secret properties without value**
```bash
az keyvault secret show \
  --vault-name kv-datalake-dev-001 \
  --name db-password \
  --query "{name:name, enabled:attributes.enabled, expires:attributes.expires}" \
  --output json
```
- This retrieves metadata without the secret value — useful for monitoring expiry in CI/CD pipelines

**3f. Safe usage in a shell script**
- Create a file called `use_secret.sh`:
```bash
#!/bin/bash
# GOOD: store in a variable, use it, never echo
SAS_TOKEN=$(az keyvault secret show --vault-name kv-datalake-dev-001 --name adls-sas-token --query value --output tsv)
echo "Token loaded, length: ${#SAS_TOKEN} characters"
# Use $SAS_TOKEN in azcopy, curl, python script — do NOT echo the value itself
```
- Answer: why is `echo "Token: $SAS_TOKEN"` dangerous even in a local terminal session?

---

## Exercise 4 — Read Secrets from Python with DefaultAzureCredential

**Concept:** Python SDK, DefaultAzureCredential, safe secret handling

**Prerequisites:** Python 3.10+, Azure CLI authenticated (`az login` completed)

**Tasks:**

**4a. Set up the project**
- Navigate to `C:/Users/hariom/Downloads/azure-ev-end-to-end-project/`
- Create or edit the `.env` file in the project root:
```
AZURE_KEY_VAULT_NAME=kv-datalake-dev-001
```
- Confirm `.env` is in `.gitignore` (it should already be there)

**4b. Install required packages**
```bash
pip install azure-keyvault-secrets azure-identity python-dotenv
```

**4c. Write and run the basic read script**
- Create `azure-key-vault-service/day1-key-vault-fundamentals/keyvault_demo.py`:

```python
import os
from dotenv import load_dotenv
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

# Load .env file
load_dotenv()

vault_name = os.environ["AZURE_KEY_VAULT_NAME"]
vault_url = f"https://{vault_name}.vault.azure.net/"

credential = DefaultAzureCredential()
client = SecretClient(vault_url=vault_url, credential=credential)

# Read a secret
secret = client.get_secret("adls-sas-token")
print(f"Name: {secret.name}")
print(f"Version: {secret.properties.version}")
print(f"Expires: {secret.properties.expires_on}")
print(f"Value length: {len(secret.value)} chars")
# Never print: print(secret.value)
```

- Run it: `python keyvault_demo.py`
- Confirm you see the name, version, and expiry — but NOT the value itself

**4d. List all secret names**
Add this block to your script (after the existing code):

```python
print("\nAll enabled secrets in vault:")
for props in client.list_properties_of_secrets():
    status = "enabled" if props.enabled else "DISABLED"
    print(f"  - {props.name} [{status}] expires: {props.expires_on}")
```

- Run the script again
- Confirm you see all 3 secrets listed with their status

**4e. Handle a missing or disabled secret gracefully**
Add error handling:

```python
from azure.core.exceptions import ResourceNotFoundError

def safe_get_secret(client, name: str) -> str | None:
    try:
        return client.get_secret(name).value
    except ResourceNotFoundError:
        print(f"WARNING: Secret '{name}' not found or is disabled.")
        return None

# Test with a non-existent secret
result = safe_get_secret(client, "this-secret-does-not-exist")
print(f"Missing secret result: {result}")  # Should print None
```

- Run and confirm you see the WARNING message instead of an unhandled exception

**4f. Set a secret from Python**
Add this to your script:

```python
new_version = client.set_secret(
    name="python-created-secret",
    value="set-from-python-demo"
)
print(f"\nCreated secret: {new_version.name} (version: {new_version.properties.version})")
```

- Run the script
- Go to the portal → Secrets — confirm `python-created-secret` now exists

**4g. Reflection questions**
Answer these after completing the exercises:
1. What credential type does `DefaultAzureCredential` use when you run this script locally after `az login`?
2. If this same script runs on an Azure VM with a Managed Identity, which step in the credential chain succeeds?
3. Why is it important that the vault URL is in `.env` and not hardcoded in the Python file?

---

## Exercise 5 — Set Expiry and Observe the Disabled State

**Concept:** Secret lifecycle — expiry, disabled state, rotation

**Tasks:**

**5a. Set a secret to expire in 2 minutes (simulation)**
- In the portal, go to `python-created-secret` → current version → **Edit**
- Set expiration date to 2 minutes from now (you may need to pick the exact date/time)
- Set activation date to 1 minute ago (so it is currently active)
- Click **"Apply"**
- While it is still active (within 2 minutes), run your Python script and confirm it can read the secret

**5b. Wait for expiry and observe the result**
- After the expiry time passes, run the Python script again
- The `get_secret("python-created-secret")` call should raise an exception (or return a disabled secret)
- Observe the error message in the terminal

**5c. Disable the secret manually (simulating a breach response)**
- In the portal, go to `event-hub-connection-string` → current version → Edit
- Change **Enabled** to **No** → Apply
- In your Python script, call `safe_get_secret(client, "event-hub-connection-string")`
- Confirm the script prints the WARNING message and returns `None` instead of raising an unhandled exception

**5d. Rotate the secret (restore + new value)**
- In the portal, create a new version of `event-hub-connection-string`:
  - Go to the secret → **"New Version"**
  - Enter a new value: `Endpoint=sb://evh-payments-dev.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=NEW_KEY_PLACEHOLDER`
  - Enabled: Yes
  - No expiration
  - Create
- Re-run the Python script
- Confirm `safe_get_secret(client, "event-hub-connection-string")` now succeeds and returns the new version's value

**5e. Answer the design questions**
1. A data pipeline reads `db-password` from Key Vault every time it starts. You rotate `db-password` (create a new version). Do you need to restart the pipeline? Explain why or why not.
2. A pipeline caches the secret value in a global variable at startup. You rotate the secret. When does the pipeline pick up the new value?
3. What is the best pattern for a long-running pipeline (e.g., a Spark Structured Streaming job) to handle secret rotation without restarting?

---

## Bonus Challenge — Fintech Key Vault Architecture Design

You are the lead data engineer at a fintech startup building an EV payment processing platform. The platform needs:

1. **ADLS SAS tokens** — 5 different containers, each with a separate SAS token rotated weekly
2. **Database credentials** — PostgreSQL username + password for Silver layer writes
3. **Event Hub connection strings** — 3 event hubs (payments, settlements, chargebacks)
4. **Third-party API keys** — 2 external payment gateway API keys
5. **Encryption keys** — AES-256 keys for encrypting PII fields before storing in Bronze layer

**Your tasks:**

**A. Name all secrets and keys following a consistent convention**
Design a naming scheme and list all secret/key names. Example: `evh-payments-connection-string`, `adls-bronze-sas-token`. Name all 11+ items.

**B. Design the vault structure**
Should you use one vault or multiple? Consider:
- The analytics team needs the ADLS SAS tokens but NOT the database credentials or API keys
- The data science team needs the ADLS SAS tokens only
- The payment processing service needs the Event Hub connection strings and API keys only
- The data pipeline (Databricks) needs the ADLS SAS tokens, database credentials, and Event Hub strings

**C. Assign RBAC roles**
For each vault (or per-secret), specify which service principal or user gets which role:
- Databricks Managed Identity
- Analytics team user group (5 people)
- Payment service (Azure App Service with Managed Identity)
- Data science team user group (3 people)
- Your own admin account

**D. Design the rotation schedule**
Which secrets need automated rotation, and how often? Who or what triggers the rotation?

**E. Implement in the portal (optional)**
Create the vault structure you designed. Create placeholder secrets for each item. Assign roles. Take screenshots of your role assignments.

---

## Summary Checklist

Before moving to Day 2, confirm you have done all of the following:

- [ ] Created Key Vault `kv-datalake-dev-001` in `rg-datalake-dev`, Australia East, Standard tier, RBAC model
- [ ] Assigned yourself Key Vault Secrets Officer role
- [ ] Created 3 secrets: `adls-sas-token`, `db-password`, `event-hub-connection-string`
- [ ] Created a new version of `adls-sas-token` and observed the version history
- [ ] Disabled and re-enabled `db-password` via the portal
- [ ] Retrieved a secret value using `az keyvault secret show --query value --output tsv`
- [ ] Listed all secret names using `az keyvault secret list --output table`
- [ ] Run `keyvault_demo.py` locally with `DefaultAzureCredential` and confirmed it reads secrets
- [ ] Added error handling with `ResourceNotFoundError` and confirmed it handles missing secrets gracefully
- [ ] Created a secret from Python using `client.set_secret()`
- [ ] Observed the disabled state and confirmed Python returns `None` rather than crashing
- [ ] Can explain the difference between RBAC and Access Policies for Key Vault
- [ ] Can explain what `DefaultAzureCredential` does and why it is the right tool for both local and production use
