# Day 2 — Storage Design, Security, and Secret Management
**Session:** 2 hours | **Goal:** Build a zero-credential architecture — tiered storage zones, Managed Identity, Key Vault for all secrets, and RBAC locking everything down.

> **Prerequisite:** Day 1 is complete. You have: Resource Group, Storage Account with 4 containers, Key Vault, Service Principal, Databricks workspace with cluster running, and storage accessible via SP OAuth (`00b_connect_storage_no_mount` verified).

> **Before starting — restart your cluster if needed:**
> Databricks left menu → **Compute** → click `dev-cluster` → if it shows **Terminated**, click **Start** and wait ~5 minutes.

---

## Glossary — New Terms for Day 2

| Term | Plain English Definition |
|---|---|
| **Medallion Architecture** | A 3-layer data organization pattern. Bronze = raw data exactly as received. Silver = cleaned and validated. Gold = aggregated for reporting. Data only flows forward — Bronze → Silver → Gold. You never go back and modify Bronze. |
| **Partition** | Splitting data into folders by a column value (e.g. `load_date=2026-06-29/`). Spark reads only the partitions it needs — so filtering by date reads only that day's folder instead of the entire table. Makes queries much faster. |
| **Parquet** | A columnar file format used for analytics. Much smaller than CSV (typically 10x compression) and much faster to query because Spark can read only the columns it needs. Bronze API data is stored as Parquet. |
| **Delta Lake** | An open-source layer on top of Parquet files that adds: ACID transactions (no corrupt partial writes), a transaction log (`_delta_log/`), MERGE (upsert) support, and time travel (query data as it was yesterday). Silver and Gold use Delta. |
| **Event Hub** | Azure's managed message broker — like a cloud Kafka. IoT devices and stream generators publish JSON events to it. Databricks reads from it in real-time using Structured Streaming. |
| **Namespace** | The top-level container for Event Hubs — like a server that holds multiple topics. One namespace can hold many Event Hub topics. |
| **Throughput Unit (TU)** | A unit of Event Hub capacity. 1 TU = 1 MB/s ingress + 2 MB/s egress. For dev with simulated IoT data, 1 TU is more than enough. |
| **Managed Identity** | An Azure identity for a service (like ADF) that Azure manages automatically — no client secret to store or rotate. ADF's Managed Identity is created automatically when you create ADF and can be assigned roles just like a Service Principal. |
| **Access Policy (Key Vault)** | The older Key Vault permission model. You list identities and tick which operations they can do (Get, List, Set, Delete). Simpler for beginners. |
| **RBAC model (Key Vault)** | The newer Key Vault permission model. Uses the same IAM roles system as every other Azure resource. Recommended for production. You already set this up in Day 1. |
| **Connection String** | A single string containing everything needed to connect to a service — host, port, credentials — all in one. Example: `Endpoint=sb://evh-ev.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...`. Stored in Key Vault, never hardcoded. |
| **Checkpoint** | A folder that Databricks Auto Loader / Structured Streaming writes to, to track which files or events it has already processed. If the job restarts it picks up from the checkpoint — no double-ingestion. |
| **Hot / Cool / Archive tier** | Storage pricing tiers. Hot = fast access, highest cost (~₹1.68/GB/month). Cool = 50% cheaper, slightly slower. Archive = 90% cheaper but takes hours to read. Older Bronze data moves down tiers automatically via Lifecycle rules. |
| **Unity Catalog** | Databricks' central governance layer. One place to manage all databases, tables, files, and access control across all your Databricks workspaces. Think of it as a metadata registry that sits on top of your ADLS Gen2 storage. |
| **Access Connector** | An Azure resource that acts as a bridge between Unity Catalog and your storage account. Unity Catalog cannot talk to ADLS Gen2 directly — it goes through the Access Connector's Managed Identity, which has IAM roles on the storage account. |
| **Storage Credential** | A Unity Catalog object that wraps the Access Connector's identity. You create it once, then reference it when creating External Locations. It never holds actual keys or tokens — it points to a Managed Identity. |
| **External Location** | A Unity Catalog object that maps a name (e.g. `bronze_location`) to a storage path (`abfss://bronze@evdatalakedev.dfs.core.windows.net/`). Once registered, Unity Catalog can enforce access control on who can read/write that path. |
| **Volume** | A Unity Catalog object that makes an External Location path browsable in the Catalog UI — like a folder shortcut. Free to create. After creating a Volume, you can see and browse your ADLS files directly in the Catalog tree inside Databricks. |
| **File Events** | An optional EventGrid integration that notifies Databricks instantly when new files land in storage, instead of Databricks polling the folder. Makes Auto Loader faster. Requires 3 extra IAM roles on the Access Connector. Non-blocking — can skip for dev. |

---

## What You Will Have at the End of Day 2
- Full Bronze / Silver / Gold folder structure created inside each container (40+ folders)
- ALL project secrets stored in Key Vault (API credentials, SP, Event Hub, storage)
- Event Hub namespace + two topics ready for Day 5 streaming
- RBAC properly assigned: SP and Managed Identity can read Key Vault secrets and write to Storage
- Zero-credential notebook pattern working — every secret read from Key Vault at runtime
- Lifecycle management policy auto-moving old Bronze data to cheaper storage tiers
- Unity Catalog fully configured: Access Connector → Storage Credential → External Locations (bronze/silver/gold) → Volumes browsable in Catalog UI

---

## Day 2 Cost Summary

| Resource | Cost |
|---|---|
| Folder creation (mkdirs) | ₹0 |
| Key Vault secret operations | ~₹0.30 total |
| Event Hub namespace (Basic, 1 TU) | ~₹11/session |
| Managed Identity (`mi-ev-intelligence-dev`) | ₹0 |
| Access Connector (`ac-ev-intelligence-dev`) | ₹0 |
| Unity Catalog (Storage Credential, External Locations, Volumes) | ₹0 |
| Cluster runtime (2 hrs) | ~₹40-45 |
| **Day 2 total** | **~₹52-56** |

> **Remember: terminate your cluster immediately after each task. A forgotten running cluster = ₹18/hr wasted.**

---

## Part 1 — Verify Day 1 Setup is Complete (5 min)

Before doing anything new, confirm yesterday's work is intact. Run this in a Databricks notebook cell:

### 1.1 Create verification notebook

1. Databricks → **Workspace** → **+ New** → **Notebook**
2. Name: `day2_00_verify_day1`
3. Language: Python → Attach to `dev-cluster`

```python
SCOPE = "kv-ev-scope"

# ── 1. Verify Key Vault secret scope exists ───────────────────────────────────
print("=== Checking secret scope ===")
scopes = [s.name for s in dbutils.secrets.listScopes()]
if "kv-ev-scope" in scopes:
    print("  kv-ev-scope         OK")
else:
    print("  kv-ev-scope         MISSING — re-create secret scope from Day 1 Part 6.5")

# ── 2. Verify secrets exist ───────────────────────────────────────────────────
print("\n=== Checking secrets ===")
required_secrets = [
    "voltgrid-api-base-url", "voltgrid-username", "voltgrid-password",
    "adls-account-name", "sp-client-id", "sp-client-secret", "sp-tenant-id"
]
for key in required_secrets:
    try:
        dbutils.secrets.get(scope=SCOPE, key=key)
        print(f"  {key:<30} OK (value masked)")
    except Exception:
        print(f"  {key:<30} MISSING — add to Key Vault")

# ── 3. Verify storage OAuth config works ─────────────────────────────────────
print("\n=== Checking storage connection ===")
try:
    storage_account  = dbutils.secrets.get(scope=SCOPE, key="adls-account-name")
    sp_client_id     = dbutils.secrets.get(scope=SCOPE, key="sp-client-id")
    sp_client_secret = dbutils.secrets.get(scope=SCOPE, key="sp-client-secret")
    sp_tenant_id     = dbutils.secrets.get(scope=SCOPE, key="sp-tenant-id")

    spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", sp_client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", sp_client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
                   f"https://login.microsoftonline.com/{sp_tenant_id}/oauth2/token")

    def abfss(container, path=""):
        base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
        return f"{base}/{path}" if path else base

    for container in ["bronze", "silver", "gold", "source"]:
        items = dbutils.fs.ls(abfss(container))
        print(f"  {container:<8} OK — {len(items)} items")
except Exception as e:
    print(f"  Storage ERROR — {e}")

print("\nIf all show OK, proceed to Part 2.")
```

> **If any secret is MISSING:** Portal → Key Vault → `kv-ev-intelligence-dev` → Secrets → add the missing secret, then re-run this cell.
> **If storage shows ERROR:** Check SP has `Storage Blob Data Contributor` role on `evdatalakedev` (Day 1 Part 5.3).

---

## Part 2 — Create the Folder Structure Inside Containers (20 min)

> **Cost: ₹0** — creating folders in ADLS Gen2 is free.

On Day 1 you created 4 containers (`bronze`, `silver`, `gold`, `source`). Containers are like top-level buckets. Now you create folders inside them — this is the physical layout of your data lake.

**Why partition by date?**
When Databricks reads `bronze/api/payments/` it scans every file ever written unless you partition. With `load_date=2026-06-29/` partitioning, a query filtering on today's date reads only today's files. On a 632k-row table ingested daily for a year, this is the difference between scanning 365 files vs 1.

### 2.1 What the folder structure looks like

```
bronze/
  api/
    payments/             load_date=2026-06-29/  part-00000.parquet
    sessions/             load_date=2026-06-29/  part-00000.parquet
    customers/
    fleet/
    chargers/
    vehicles/
    complaints/
    maintenance_events/
    energy_prices/
    tariffs/
    charge_cards/
    employees/
    partners/
    cities/
    stations/
    states/
    weather/
    pipeline_audit/
  blob/
    iot_sessions/         year=2026/month=06/day=29/  iot_stream_001.csv
    maintenance/          year=2026/month=06/day=29/  maintenance_001.csv
    invoices/             year=2026/month=06/          invoice_001.pdf
    station_configs/      year=2026/month=06/          config_001.xml
    energy_reports/       year=2026/month=06/day=29/   report_001.json
  streaming/
    event_hub/            year=2026/month=06/day=29/hour=14/  events_001.json
  _checkpoints/           (Auto Loader tracking — do not modify manually)

silver/
  payments/               _delta_log/  part-00000.parquet  ...
  sessions/
  customers/
  fleet/
  vehicles/
  chargers/
  complaints/
  maintenance_events/
  energy_prices/
  pipeline_audit/

gold/
  fact_charging_sessions/
  fact_payments/
  dim_customer/
  dim_vehicle/
  dim_station/
  agg_daily_revenue/
  agg_station_utilization/
```

### 2.2 Create via Databricks Notebook (recommended)

Folders in ADLS Gen2 do not actually exist until a file is written into them. `dbutils.fs.mkdirs()` creates an empty placeholder so you can verify structure before ingestion starts.

1. Databricks → **Workspace** → **+ New** → **Notebook**
2. Name: `01_create_folder_structure`
3. Language: Python → Attach to `dev-cluster`

```python
# Run Cell 1 of this notebook first (or %run 00b_connect_storage_no_mount) to set up abfss()

folders = [
    # Bronze — API (one folder per endpoint)
    abfss("bronze", "api/payments"),
    abfss("bronze", "api/sessions"),
    abfss("bronze", "api/customers"),
    abfss("bronze", "api/fleet"),
    abfss("bronze", "api/chargers"),
    abfss("bronze", "api/vehicles"),
    abfss("bronze", "api/complaints"),
    abfss("bronze", "api/maintenance_events"),
    abfss("bronze", "api/energy_prices"),
    abfss("bronze", "api/tariffs"),
    abfss("bronze", "api/charge_cards"),
    abfss("bronze", "api/employees"),
    abfss("bronze", "api/partners"),
    abfss("bronze", "api/cities"),
    abfss("bronze", "api/stations"),
    abfss("bronze", "api/states"),
    abfss("bronze", "api/weather"),
    abfss("bronze", "api/pipeline_audit"),
    # Bronze — Blob file uploads
    abfss("bronze", "blob/iot_sessions"),
    abfss("bronze", "blob/maintenance"),
    abfss("bronze", "blob/invoices"),
    abfss("bronze", "blob/station_configs"),
    abfss("bronze", "blob/energy_reports"),
    # Bronze — Streaming checkpoint root
    abfss("bronze", "streaming/event_hub"),
    abfss("bronze", "_checkpoints"),
    # Silver — one Delta table per entity
    abfss("silver", "payments"),
    abfss("silver", "sessions"),
    abfss("silver", "customers"),
    abfss("silver", "fleet"),
    abfss("silver", "vehicles"),
    abfss("silver", "chargers"),
    abfss("silver", "complaints"),
    abfss("silver", "maintenance_events"),
    abfss("silver", "energy_prices"),
    abfss("silver", "pipeline_audit"),
    # Gold — aggregated tables
    abfss("gold", "fact_charging_sessions"),
    abfss("gold", "fact_payments"),
    abfss("gold", "dim_customer"),
    abfss("gold", "dim_vehicle"),
    abfss("gold", "dim_station"),
    abfss("gold", "agg_daily_revenue"),
    abfss("gold", "agg_station_utilization"),
]

created = 0
for folder in folders:
    try:
        dbutils.fs.mkdirs(folder)
        print(f"  Created : {folder}")
        created += 1
    except Exception as e:
        print(f"  ERROR   : {folder} — {e}")

print(f"\nDone — {created}/{len(folders)} folders created.")
```

> **If mkdirs fails with 403 Forbidden:** SP is missing `Storage Blob Data Contributor` role on `evdatalakedev` — check IAM in the Portal.

### 2.3 Verify the structure via Portal

1. Portal → `evdatalakedev` → left menu **Containers** → click `bronze`
2. You should see: `api/`, `blob/`, `streaming/`, `_checkpoints/` at the top level
3. Click into `api/` — you should see all 18 endpoint folders listed
4. If the portal shows empty containers, wait 1 minute and refresh — ADLS Gen2 has slight propagation delay

### 2.4 Verify via Databricks

```python
# abfss() is available after running the storage auth cell above
print("=== Bronze/api folders ===")
for item in dbutils.fs.ls(abfss("bronze", "api")):
    print(f"  {item.name}")

print("\n=== Silver folders ===")
for item in dbutils.fs.ls(abfss("silver")):
    print(f"  {item.name}")

print("\n=== Gold folders ===")
for item in dbutils.fs.ls(abfss("gold")):
    print(f"  {item.name}")
```

---

## Part 3 — Add ALL Project Secrets to Key Vault (20 min)

> **Cost: ~₹5 total for 18 days** — negligible.

Every credential this project needs must live in Key Vault. If it is not in Key Vault, it should not be in your code.

**How to add a secret via Portal (step-by-step):**
1. Go to [https://portal.azure.com](https://portal.azure.com)
2. Search **Key vaults** → click `kv-ev-intelligence-dev`
3. Left menu → **Secrets** → **+ Generate/Import**
4. **Upload options:** Manual
5. Fill in **Name** and **Value** exactly as shown below → click **Create**
6. Repeat for each secret

> **Important — secret names are case-sensitive.** `voltgrid-username` and `VoltGrid-Username` are two different secrets. Use lowercase-with-hyphens exactly as shown.

### 3.1 VoltGrid API Credentials

| Secret Name | Value | Where to get it |
|---|---|---|
| `voltgrid-api-base-url` | `https://ev-project-navy-mu.vercel.app` | Fixed value — type it in directly |
| `voltgrid-username` | `voltgrid_demo` | Fixed value — type it in directly |
| `voltgrid-password` | `EVcharge@AU2025` | Fixed value — type it in directly |

> **These may already exist from Day 1.** If you see them in Key Vault → Secrets, skip adding them again — Azure will create a new version if you add the same name, but the old one still works.

### 3.2 Service Principal (carried from Day 1)

Confirm these already exist in Key Vault. If not, add them now:

| Secret Name | Where to get the value |
|---|---|
| `sp-client-id` | Portal → **App registrations** → `sp-ev-intelligence-dev` → **Overview** → **Application (client) ID** |
| `sp-client-secret` | Copied when SP was created in Day 1. If lost: App registrations → `sp-ev-intelligence-dev` → **Certificates & secrets** → delete old secret → **+ New client secret** → copy value **immediately** before navigating away |
| `sp-tenant-id` | Portal → **App registrations** → `sp-ev-intelligence-dev` → **Overview** → **Directory (tenant) ID** |

> **If you lost the SP client secret (common!):**
> 1. Portal → **App registrations** → search `sp-ev-intelligence-dev` → click it
> 2. Left menu → **Certificates & secrets**
> 3. Under **Client secrets** — find the old secret → click **Delete** → confirm
> 4. Click **+ New client secret** → Description: `ev-project-dev` → Expires: `180 days` → **Add**
> 5. **Copy the Value immediately** — it disappears when you navigate away
> 6. Go to Key Vault → update the `sp-client-secret` value with the new one
> 7. Re-run the storage auth cells in your notebook (or `%run 00b_connect_storage_no_mount`) — they read the updated secret from Key Vault automatically

### 3.3 Storage Account Name

| Secret Name | Value | Why store a non-sensitive value? |
|---|---|---|
| `adls-account-name` | `evdatalakedev` | Centralised config — if you rename the storage account later, update Key Vault once and all notebooks pick it up automatically |

### 3.4 Event Hub (add after completing Part 4)

| Secret Name | Value | Where to get it |
|---|---|---|
| `eventhub-namespace` | `evh-ev-intelligence-dev` | The namespace name you create in Part 4 |
| `eventhub-name` | `iot-telemetry` | The topic name from Part 4 |
| `eventhub-connection-string` | Long `Endpoint=sb://...` string | Event Hub namespace → Shared access policies → copy (Part 4.3) |

### 3.5 Add all via CLI (faster for bulk entry)

> **CMD / PowerShell users:** Use the single-line version below. The `$KV` variable is bash syntax only.

**Single line (CMD / PowerShell — copy-paste each line):**
```cmd
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-api-base-url" --value "https://ev-project-navy-mu.vercel.app"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-username" --value "voltgrid_demo"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "voltgrid-password" --value "EVcharge@AU2025"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "adls-account-name" --value "evdatalakedev"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-client-id" --value "<your-app-id>"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-client-secret" --value "<your-sp-password>"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "sp-tenant-id" --value "<your-tenant-id>"
```

**Multi-line (bash / Git Bash only):**
```bash
KV="kv-ev-intelligence-dev"

az keyvault secret set --vault-name $KV --name "voltgrid-api-base-url" --value "https://ev-project-navy-mu.vercel.app"
az keyvault secret set --vault-name $KV --name "voltgrid-username"     --value "voltgrid_demo"
az keyvault secret set --vault-name $KV --name "voltgrid-password"     --value "EVcharge@AU2025"
az keyvault secret set --vault-name $KV --name "adls-account-name"     --value "evdatalakedev"
az keyvault secret set --vault-name $KV --name "sp-client-id"          --value "<your-app-id>"
az keyvault secret set --vault-name $KV --name "sp-client-secret"      --value "<your-sp-password>"
az keyvault secret set --vault-name $KV --name "sp-tenant-id"          --value "<your-tenant-id>"
```

> **If CLI gives `Forbidden: ForbiddenByRbac`:**
> Your account needs `Key Vault Secrets Officer` or `Key Vault Administrator` role on the Key Vault.
> Portal → Key Vault → Access Control (IAM) → Add role assignment → role: `Key Vault Secrets Officer` → assign to your login email → wait 2 minutes → retry.

> **If CLI gives `Conflict: ObjectIsDeletedButRecoverable`:**
> A secret with that name was previously deleted but is still in soft-delete (recoverable for 90 days by default).
> Recover it first: `az keyvault secret recover --vault-name kv-ev-intelligence-dev --name "<secret-name>"`
> Then retry the `secret set` command.
> Or purge it permanently: `az keyvault secret purge --vault-name kv-ev-intelligence-dev --name "<secret-name>"` then set fresh.

### 3.6 Verify all secrets exist

**Via Portal:**
1. Key Vault → left menu **Secrets**
2. You see a list of all secret names (values are hidden by default)
3. Click any secret → click the current version → **Show Secret Value** to confirm the value is correct

**Via CLI:**
```cmd
az keyvault secret list --vault-name kv-ev-intelligence-dev --query "[].name" -o table
```

**Via Databricks notebook (best way — tests the full path your code will use):**
```python
SCOPE = "kv-ev-scope"
required_secrets = [
    "voltgrid-api-base-url", "voltgrid-username", "voltgrid-password",
    "adls-account-name", "sp-client-id", "sp-client-secret", "sp-tenant-id"
]

print("Checking secrets via Databricks secret scope:")
for key in required_secrets:
    try:
        val = dbutils.secrets.get(scope=SCOPE, key=key)
        print(f"  {key:<30} OK")
    except Exception as e:
        print(f"  {key:<30} ERROR: {e}")
```

> **If `dbutils.secrets.get()` throws even though the secret exists in Key Vault:**
> The Databricks secret scope may have lost its link to Key Vault (this can happen if Key Vault permission model was changed).
> Portal → Key Vault → Access Control (IAM) → confirm `AzureDatabricks` enterprise app has `Key Vault Secrets User` role.
> If missing, add it: IAM → Add role assignment → role: `Key Vault Secrets User` → search `AzureDatabricks` → assign → wait 2 minutes.

---

## Part 4 — Create Azure Event Hub (15 min)

> **Cost: ~₹11 per 2-hour session** (Basic tier, 1 TU)
> Total across 18 sessions: ~₹198

**What is Event Hub?**
Event Hub is Azure's managed message broker. IoT devices publish JSON events to it at high volume. Databricks reads from it using Structured Streaming. Think of it as a high-throughput queue — publishers write events in, consumers read them out, and Event Hub holds messages for up to 1 day (Basic tier) before they expire.

**What is a Namespace vs an Event Hub topic?**
The Namespace is the server — it has a globally unique DNS name (`evh-ev-intelligence-dev.servicebus.windows.net`). An Event Hub topic is a named channel inside that namespace. Today you create 2 topics: `iot-telemetry` and `maintenance-alerts`.

### 4.1 Create Event Hub Namespace

**Via Portal:**
1. Go to [https://portal.azure.com](https://portal.azure.com)
2. Search **Event Hubs** in the top bar → click it
3. Click **+ Create**
4. Fill in every field:
   - **Subscription:** your subscription
   - **Resource group:** `rg-ev-intelligence-dev`
   - **Namespace name:** `evh-ev-intelligence-dev` *(must be globally unique — add your initials if taken)*
   - **Location:** `Central India`
   - **Pricing tier:** `Basic` ← cheapest, sufficient for dev
   - **Throughput units:** `1` ← minimum
5. Click **Review + Create** → **Create**
6. Wait ~1-2 minutes for deployment → click **Go to resource**

**Via CLI:**

**Single line (CMD / PowerShell):**
```cmd
az eventhubs namespace create --name evh-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --location centralindia --sku Basic --capacity 1
```

**Multi-line (bash / Git Bash only):**
```bash
az eventhubs namespace create \
  --name evh-ev-intelligence-dev \
  --resource-group rg-ev-intelligence-dev \
  --location centralindia \
  --sku Basic \
  --capacity 1
```

> **If namespace name is taken:**
> Add your initials: `evh-ev-intelligence-dev-hs`. Then use this name everywhere below instead.

> **If CLI gives `MissingSubscriptionRegistration` for Microsoft.EventHub:**
> Run: `az provider register --namespace Microsoft.EventHub`
> Wait 1-2 minutes: `az provider show --namespace Microsoft.EventHub --query registrationState -o tsv`
> Wait until it shows `Registered`, then retry.

### 4.2 Create Event Hub Topics

**Via Portal:**
1. On the namespace page → left menu **Event Hubs** → **+ Event Hub**
2. Fill in for the first topic:
   - **Name:** `iot-telemetry`
   - **Partition count:** `4` *(Basic tier max)*
   - **Message Retention:** `1` day *(Basic tier limit)*
3. Click **Create** → wait for it to appear in the list
4. Click **+ Event Hub** again → create the second topic:
   - **Name:** `maintenance-alerts`
   - Same partition and retention settings

**Via CLI:**

**Single line (CMD / PowerShell):**
```cmd
az eventhubs eventhub create --name iot-telemetry --namespace-name evh-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --partition-count 4 --message-retention 1
az eventhubs eventhub create --name maintenance-alerts --namespace-name evh-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --partition-count 4 --message-retention 1
```

**Multi-line (bash / Git Bash only):**
```bash
az eventhubs eventhub create \
  --name iot-telemetry \
  --namespace-name evh-ev-intelligence-dev \
  --resource-group rg-ev-intelligence-dev \
  --partition-count 4 \
  --message-retention 1

az eventhubs eventhub create \
  --name maintenance-alerts \
  --namespace-name evh-ev-intelligence-dev \
  --resource-group rg-ev-intelligence-dev \
  --partition-count 4 \
  --message-retention 1
```

> **If `--message-retention 1` fails or gives a validation error:**
> Basic tier only supports 1 day retention. If you accidentally selected Standard tier during namespace creation, either delete and recreate as Basic, or use `--message-retention 1` — Standard supports up to 7 days but also accepts 1.

### 4.3 Get Connection String and Store in Key Vault

**What is the connection string?**
A single string containing the namespace endpoint URL + authentication key. Looks like:
`Endpoint=sb://evh-ev-intelligence-dev.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=xxxxxxxxxxx=`

**Via Portal — get the connection string:**
1. Go to the namespace `evh-ev-intelligence-dev`
2. Left menu → **Shared access policies** (under Settings)
3. Click **RootManageSharedAccessKey**
4. A panel opens on the right — click the **copy icon** next to **Connection string–primary key**
5. Paste it somewhere temporary (Notepad) — you will use it in the next step

**Via Portal — store in Key Vault:**
1. Key Vault → `kv-ev-intelligence-dev` → **Secrets** → **+ Generate/Import**
2. Name: `eventhub-connection-string` → Value: paste the connection string → **Create**

Also add the namespace and topic names separately:
1. Name: `eventhub-namespace` → Value: `evh-ev-intelligence-dev` → **Create**
2. Name: `eventhub-name` → Value: `iot-telemetry` → **Create**

**Via CLI — get and store in Key Vault:**

**Step-by-step (CMD / PowerShell — run each separately):**
```cmd
az eventhubs namespace authorization-rule keys list --name RootManageSharedAccessKey --namespace-name evh-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query primaryConnectionString -o tsv
```
Copy the output (the full connection string), then:
```cmd
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "eventhub-connection-string" --value "<paste connection string here>"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "eventhub-namespace" --value "evh-ev-intelligence-dev"
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "eventhub-name" --value "iot-telemetry"
```

**Multi-line (bash / Git Bash only):**
```bash
CONN_STR=$(az eventhubs namespace authorization-rule keys list \
  --name RootManageSharedAccessKey \
  --namespace-name evh-ev-intelligence-dev \
  --resource-group rg-ev-intelligence-dev \
  --query primaryConnectionString -o tsv)

KV="kv-ev-intelligence-dev"
az keyvault secret set --vault-name $KV --name "eventhub-connection-string" --value "$CONN_STR"
az keyvault secret set --vault-name $KV --name "eventhub-namespace"         --value "evh-ev-intelligence-dev"
az keyvault secret set --vault-name $KV --name "eventhub-name"              --value "iot-telemetry"

echo "Event Hub secrets stored in Key Vault."
```

### 4.4 Verify Event Hub is reachable from Databricks

```python
SCOPE = "kv-ev-scope"
conn_str = dbutils.secrets.get(scope=SCOPE, key="eventhub-connection-string")
namespace = dbutils.secrets.get(scope=SCOPE, key="eventhub-namespace")
eh_name   = dbutils.secrets.get(scope=SCOPE, key="eventhub-name")

print(f"Namespace : {namespace}")
print(f"Topic     : {eh_name}")
print(f"Conn str  : {conn_str[:40]}...[REDACTED]")
print("Event Hub secrets loaded successfully.")
```

---

## Part 5 — RBAC: Lock Down Who Can Access What (15 min)

**What is RBAC and why does it matter?**
RBAC = Role-Based Access Control. It determines which identity can do what on which resource. Without RBAC assignments, the Service Principal and Managed Identity have no permissions — they cannot read secrets or write data even if they exist.

**All RBAC assignments needed for this project:**

| Identity | Resource | Role | Status |
|---|---|---|---|
| `sp-ev-intelligence-dev` | `evdatalakedev` storage | Storage Blob Data Contributor | Done in Day 1 |
| `AzureDatabricks` enterprise app | `kv-ev-intelligence-dev` Key Vault | Key Vault Secrets User | Done in Day 1 |
| `sp-ev-intelligence-dev` | `kv-ev-intelligence-dev` Key Vault | Key Vault Secrets User | **Do today** |
| `mi-ev-intelligence-dev` | `evdatalakedev` storage | Storage Blob Data Contributor | **Do today** |
| `mi-ev-intelligence-dev` | `kv-ev-intelligence-dev` Key Vault | Key Vault Secrets User | **Do today** |
| Your user account | `kv-ev-intelligence-dev` Key Vault | Key Vault Administrator | **Do today** |

### 5.1 Confirm Key Vault is on RBAC Permission Model

Day 1 set this up, but confirm it is still correct:

1. Portal → **Key vaults** → `kv-ev-intelligence-dev`
2. Left menu → **Access configuration** (under Settings)
3. Confirm **Permission model** shows **Azure role-based access control**
4. If it shows **Vault access policy** — click it, select **Azure role-based access control** → **Save**

> **After switching from Vault access policy to RBAC:** All existing Access Policies are ignored. You must re-grant all access via IAM roles. Add your own account as `Key Vault Administrator` first (Part 5.3) or you will lose the ability to manage secrets.

### 5.2 Assign SP the Key Vault Secrets User Role

This lets the Service Principal read secrets at runtime from notebooks and ADF pipelines.

**Via Portal:**
1. Key Vault → left menu **Access Control (IAM)**
2. Click **+ Add** → **Add role assignment**
3. **Role** tab: search `Key Vault Secrets User` → select → click **Next**
4. **Members** tab:
   - **Assign access to:** `User, group, or service principal`
   - Click **+ Select members** → search `sp-ev-intelligence-dev` → select → **Select**
5. Click **Review + assign** → **Review + assign**
6. Wait **2 minutes** before testing — RBAC propagates across Azure's systems

**Via CLI:**

**Step-by-step (CMD / PowerShell — run each line separately, copy the output before moving to the next):**

```cmd
az keyvault show --name kv-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query id -o tsv
```
Copy the output → this is your `KV_ID` (looks like `/subscriptions/.../providers/Microsoft.KeyVault/vaults/kv-ev-intelligence-dev`)

```cmd
az keyvault secret show --vault-name kv-ev-intelligence-dev --name sp-client-id --query value -o tsv
```
Copy the output → this is your `SP_CLIENT_ID` (a GUID)

```cmd
az ad sp show --id <SP_CLIENT_ID from above> --query id -o tsv
```
Copy the output → this is your `SP_OID` (the SP's internal Object ID — different from Client ID)

```cmd
az role assignment create --assignee-object-id <SP_OID> --assignee-principal-type ServicePrincipal --role "Key Vault Secrets User" --scope <KV_ID>
```

**Multi-line (bash / Git Bash only):**
```bash
KV_ID=$(az keyvault show \
  --name kv-ev-intelligence-dev \
  --resource-group rg-ev-intelligence-dev \
  --query id -o tsv)

SP_CLIENT_ID=$(az keyvault secret show \
  --vault-name kv-ev-intelligence-dev \
  --name sp-client-id --query value -o tsv)

SP_OID=$(az ad sp show --id $SP_CLIENT_ID --query id -o tsv)

az role assignment create \
  --assignee-object-id $SP_OID \
  --assignee-principal-type ServicePrincipal \
  --role "Key Vault Secrets User" \
  --scope $KV_ID
```

> **If `az ad sp show` gives `Resource not found`:**
> The SP Client ID stored in Key Vault may be wrong. Get the correct one:
> `az ad sp list --display-name sp-ev-intelligence-dev --query "[0].appId" -o tsv`
> Then update the `sp-client-id` secret in Key Vault with the correct value.

> **If role assignment gives `RoleAssignmentAlreadyExists`:**
> The role is already assigned — no action needed. Verify with:
> `az role assignment list --scope <KV_ID> --query "[].{Role:roleDefinitionName, Principal:principalName}" -o table`

### 5.3 Give Your Own Account Key Vault Administrator

This ensures your human account can always manage secrets even if other role assignments change.

**Via Portal:**
1. Key Vault → **Access Control (IAM)** → **+ Add** → **Add role assignment**
2. Role: `Key Vault Administrator` → **Next**
3. Members: **User, group, or service principal** → **+ Select members** → search your Azure login email → select → **Review + assign**
4. Wait 1-2 minutes

**Via CLI:**

**Step-by-step (CMD / PowerShell):**
```cmd
az ad signed-in-user show --query id -o tsv
```
Copy output (your user Object ID), then:
```cmd
az role assignment create --assignee-object-id <MY_OID> --assignee-principal-type User --role "Key Vault Administrator" --scope <KV_ID from Part 5.2>
```

**Multi-line (bash / Git Bash only):**
```bash
MY_OID=$(az ad signed-in-user show --query id -o tsv)

az role assignment create \
  --assignee-object-id $MY_OID \
  --assignee-principal-type User \
  --role "Key Vault Administrator" \
  --scope $KV_ID
```

### 5.4 Verify All IAM Assignments

**Via Portal:**
1. Key Vault → **Access Control (IAM)** → **Role assignments** tab
2. You should see all the assigned identities listed with their roles

**Via CLI:**
```cmd
az role assignment list --scope <KV_ID> --query "[].{Role:roleDefinitionName, Principal:principalName}" -o table
```

Expected output:
```
Role                          Principal
----------------------------  ----------------------------------
Key Vault Administrator       <your-email>
Key Vault Secrets User        sp-ev-intelligence-dev
Key Vault Secrets User        AzureDatabricks
```

---

## Part 6 — Create Managed Identity for ADF (10 min)

> **Cost: ₹0** — Managed Identities are completely free.

**What is a Managed Identity vs a Service Principal?**

| | Service Principal | Managed Identity |
|---|---|---|
| Created by | You manually | Azure automatically |
| Credentials | Client secret you store and rotate | Azure rotates internally — you never see it |
| Where to use | Any application (local scripts, GitHub Actions) | Azure services only (ADF, App Service, Functions) |
| Security | Good — but you own secret rotation | Better — no secret to leak or rotate |

Use Managed Identity for ADF. Use Service Principal for Databricks (already configured in Day 1).

### 6.1 Create the Managed Identity

**Via Portal:**
1. Go to [https://portal.azure.com](https://portal.azure.com)
2. Search **Managed Identities** → click it
3. Click **+ Create**
4. Fill in:
   - **Subscription:** your subscription
   - **Resource group:** `rg-ev-intelligence-dev`
   - **Region:** `Central India`
   - **Name:** `mi-ev-intelligence-dev`
5. Click **Review + Create** → **Create**
6. Wait ~30 seconds → click **Go to resource**
7. On the Overview page, copy the **Principal ID** (a GUID) — you need it in the next step

**Via CLI:**

**Single line (CMD / PowerShell):**
```cmd
az identity create --name mi-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --location centralindia
```

After creation, get the Principal ID:
```cmd
az identity show --name mi-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query principalId -o tsv
```
Copy the output — this is your `MI_PRINCIPAL` (used for RBAC assignments below).

**Multi-line (bash / Git Bash only):**
```bash
az identity create \
  --name mi-ev-intelligence-dev \
  --resource-group rg-ev-intelligence-dev \
  --location centralindia
```

### 6.2 Assign Roles to the Managed Identity

**What roles does ADF's Managed Identity need?**
- `Storage Blob Data Contributor` on the storage account — so ADF can read raw files and write pipeline outputs
- `Key Vault Secrets User` on Key Vault — so ADF can read connection strings and credentials at runtime

**Via Portal — Storage access:**
1. Portal → search **Storage accounts** → click `evdatalakedev`
2. Left menu → **Access Control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. **Role** tab: search `Storage Blob Data Contributor` → select → **Next**
5. **Members** tab: **+ Select members** → search `mi-ev-intelligence-dev` → select → **Review + assign**
6. Wait 2 minutes

**Via Portal — Key Vault access:**
1. Portal → `kv-ev-intelligence-dev` → left menu **Access Control (IAM)**
2. **+ Add** → **Add role assignment**
3. Role: `Key Vault Secrets User` → **Next**
4. Members: search `mi-ev-intelligence-dev` → select → **Review + assign**
5. Wait 2 minutes

**Via CLI (both roles — step-by-step for CMD / PowerShell):**

```cmd
az identity show --name mi-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query principalId -o tsv
```
Copy output → `MI_PRINCIPAL`

```cmd
az storage account show --name evdatalakedev --resource-group rg-ev-intelligence-dev --query id -o tsv
```
Copy output → `STORAGE_ID`

```cmd
az keyvault show --name kv-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query id -o tsv
```
Copy output → `KV_ID`

```cmd
az role assignment create --assignee-object-id <MI_PRINCIPAL> --assignee-principal-type ServicePrincipal --role "Storage Blob Data Contributor" --scope <STORAGE_ID>
az role assignment create --assignee-object-id <MI_PRINCIPAL> --assignee-principal-type ServicePrincipal --role "Key Vault Secrets User" --scope <KV_ID>
```

**Multi-line (bash / Git Bash only):**
```bash
MI_PRINCIPAL=$(az identity show \
  --name mi-ev-intelligence-dev \
  --resource-group rg-ev-intelligence-dev \
  --query principalId -o tsv)

STORAGE_ID=$(az storage account show \
  --name evdatalakedev \
  --resource-group rg-ev-intelligence-dev \
  --query id -o tsv)

KV_ID=$(az keyvault show \
  --name kv-ev-intelligence-dev \
  --resource-group rg-ev-intelligence-dev \
  --query id -o tsv)

az role assignment create \
  --assignee-object-id $MI_PRINCIPAL \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ID

az role assignment create \
  --assignee-object-id $MI_PRINCIPAL \
  --assignee-principal-type ServicePrincipal \
  --role "Key Vault Secrets User" \
  --scope $KV_ID

echo "Managed Identity roles assigned."
```

> **If Managed Identity not found in IAM search (Portal):**
> Wait 2-3 minutes after creation — Azure Entra ID propagation delay. Then search again using the exact name `mi-ev-intelligence-dev`.

> **If `az role assignment create` gives `principalNotFound`:**
> The Managed Identity's principal hasn't propagated yet. Wait 2 minutes and retry.

### 6.3 Verify Managed Identity assignments

**Via Portal:**
1. Storage account `evdatalakedev` → **Access Control (IAM)** → **Role assignments** tab
2. You should see both `sp-ev-intelligence-dev` and `mi-ev-intelligence-dev` listed under `Storage Blob Data Contributor`

**Via CLI:**
```cmd
az role assignment list --scope <STORAGE_ID> --query "[].{Role:roleDefinitionName, Principal:principalName}" -o table
```

---

## Part 7 — Zero-Credential Notebook Pattern (10 min)

This is the standard header every notebook in this project uses. No notebook ever contains a raw password, key, or token.

**Why this matters:**
- If a notebook is accidentally shared or exported, no credentials are exposed
- `dbutils.secrets.get()` masks values as `[REDACTED]` in all Databricks output — values are never printed
- Rotating a credential means updating it in Key Vault once — every notebook gets the new value automatically on the next run

### 7.1 Create notebook `02_secrets_pattern`

1. Databricks → **Workspace** → **+ New** → **Notebook**
2. Name: `02_secrets_pattern`
3. Language: Python → Attach to `dev-cluster`

**Cell 1 — Standard secret header (paste at the top of every notebook):**
```python
# ── Standard notebook header — paste this at the top of every notebook ─────────
SCOPE = "kv-ev-scope"

def secret(key):
    return dbutils.secrets.get(scope=SCOPE, key=key)

# ── Storage authentication via Service Principal OAuth ─────────────────────────
storage_account  = secret("adls-account-name")
sp_client_id     = secret("sp-client-id")
sp_client_secret = secret("sp-client-secret")
sp_tenant_id     = secret("sp-tenant-id")

spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
    sp_client_id)
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
    sp_client_secret)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{sp_tenant_id}/oauth2/token")

print(f"Storage account : {storage_account}")
print("Storage auth configured.")
```

**Cell 2 — VoltGrid API token (call this once per notebook session):**
```python
import requests

api_base_url = secret("voltgrid-api-base-url")
username     = secret("voltgrid-username")
password     = secret("voltgrid-password")

resp = requests.post(
    f"{api_base_url}/api/auth/login/",
    json={"username": username, "password": password},
    timeout=10,
)
resp.raise_for_status()
API_TOKEN   = resp.json()["token"]
API_HEADERS = {"Authorization": f"Token {API_TOKEN}"}

print(f"API base URL : {api_base_url}")
print(f"API token    : {API_TOKEN[:8]}...[REDACTED]")
print("All secrets loaded. Ready.")
```

### 7.2 Test storage read/write

```python
# abfss() is available after running the storage auth cell above
test_path = abfss("bronze", "api/_test.txt")

dbutils.fs.put(test_path, "connection ok", overwrite=True)
content = dbutils.fs.head(test_path)
dbutils.fs.rm(test_path)
print(f"Read back: {content}")
print("Storage read/write: OK")
```

> **If `dbutils.fs.put()` fails with 403:** SP client secret in Key Vault may be expired. Go to App registrations → `sp-ev-intelligence-dev` → create a new client secret → update Key Vault `sp-client-secret` → re-run the storage auth cell.

### 7.3 List all secrets available (names only — values never shown)

```python
print("Available scopes:")
for s in dbutils.secrets.listScopes():
    print(f"  {s.name}")

print("\nSecrets in kv-ev-scope:")
for s in dbutils.secrets.list("kv-ev-scope"):
    print(f"  {s.key}")
```

Expected output should include all 10 secrets:
```
voltgrid-api-base-url
voltgrid-username
voltgrid-password
adls-account-name
sp-client-id
sp-client-secret
sp-tenant-id
eventhub-connection-string
eventhub-namespace
eventhub-name
```

---

## Part 8 — Lifecycle Management Policy (5 min)

> **Cost saving: ~50% on Bronze storage costs after day 30**

Older Bronze data does not need fast access. A lifecycle policy moves files to cheaper tiers automatically — set it once and forget it.

| Age of data | Tier | Cost | Speed |
|---|---|---|---|
| 0–30 days | Hot | ~₹1.68/GB/month | Instant |
| 30–90 days | Cool | ~₹0.84/GB/month (50% cheaper) | Slightly slower |
| 90+ days | Archive | ~₹0.17/GB/month (90% cheaper) | Hours to rehydrate |

### 8.1 Via Portal

1. Portal → `evdatalakedev` storage
2. Left menu → **Lifecycle management** (under Data management)
3. If you already created `move-to-cool` in Day 1 — verify it is there. If yes, skip to 8.3.
4. Click **+ Add a rule**
5. Fill in:
   - **Rule name:** `tier-old-data`
   - **Rule scope:** `Apply to all blobs in the storage account`
   - Click **Next**
6. **Base blobs** tab:
   - Condition 1: **Last modified** more than **30** days ago → **Move to cool storage**
   - Click **+ Add condition**
   - Condition 2: **Last modified** more than **90** days ago → **Move to archive storage**
7. Click **Add**

### 8.2 Via CLI

> **CMD / PowerShell users:** The inline JSON with `\` continuation is bash only. Use the single-line version below.

**Single line (CMD / PowerShell):**
```cmd
az storage account management-policy create --account-name evdatalakedev --resource-group rg-ev-intelligence-dev --policy "{\"rules\":[{\"name\":\"tier-old-data\",\"enabled\":true,\"type\":\"Lifecycle\",\"definition\":{\"filters\":{\"blobTypes\":[\"blockBlob\"]},\"actions\":{\"baseBlob\":{\"tierToCool\":{\"daysAfterModificationGreaterThan\":30},\"tierToArchive\":{\"daysAfterModificationGreaterThan\":90}}}}}]}"
```

**Multi-line (bash / Git Bash only):**
```bash
az storage account management-policy create \
  --account-name evdatalakedev \
  --resource-group rg-ev-intelligence-dev \
  --policy '{
    "rules": [{
      "name": "tier-old-data",
      "enabled": true,
      "type": "Lifecycle",
      "definition": {
        "filters": {"blobTypes": ["blockBlob"]},
        "actions": {
          "baseBlob": {
            "tierToCool":    {"daysAfterModificationGreaterThan": 30},
            "tierToArchive": {"daysAfterModificationGreaterThan": 90}
          }
        }
      }
    }]
  }'
```

> **If CLI gives `PolicyAlreadyExists`:**
> A policy already exists — this is from Day 1 (the `move-to-cool` rule). You can either leave the existing rule or update it via Portal. No need to recreate.

### 8.3 Verify lifecycle policy

1. Portal → `evdatalakedev` → **Lifecycle management**
2. You should see your rule listed with **Enabled** status
3. Click the rule name to confirm the conditions are correct (30 days → Cool, 90 days → Archive)

---

## Part 9 — End-to-End Verification (10 min)

Run this final verification notebook to confirm everything Day 2 set up is working together.

### 9.1 Create notebook `day2_99_verify`

```python
import requests
print("=" * 60)
print("DAY 2 END-TO-END VERIFICATION")
print("=" * 60)

SCOPE = "kv-ev-scope"
errors = []

# 1. Secret scope accessible
try:
    scopes = [s.name for s in dbutils.secrets.listScopes()]
    assert "kv-ev-scope" in scopes
    print("1. Secret scope kv-ev-scope        : OK")
except Exception as e:
    print(f"1. Secret scope kv-ev-scope        : FAIL — {e}")
    errors.append("secret scope")

# 2. All secrets readable
required = [
    "voltgrid-api-base-url", "voltgrid-username", "voltgrid-password",
    "adls-account-name", "sp-client-id", "sp-client-secret", "sp-tenant-id",
    "eventhub-connection-string", "eventhub-namespace", "eventhub-name"
]
missing_secrets = []
for key in required:
    try:
        dbutils.secrets.get(scope=SCOPE, key=key)
    except:
        missing_secrets.append(key)
if missing_secrets:
    print(f"2. Secrets check                   : FAIL — missing: {missing_secrets}")
    errors.append("missing secrets")
else:
    print(f"2. All {len(required)} secrets readable              : OK")

# 3. Storage OAuth config + connection check
try:
    storage_account  = dbutils.secrets.get(scope=SCOPE, key="adls-account-name")
    sp_client_id     = dbutils.secrets.get(scope=SCOPE, key="sp-client-id")
    sp_client_secret = dbutils.secrets.get(scope=SCOPE, key="sp-client-secret")
    sp_tenant_id     = dbutils.secrets.get(scope=SCOPE, key="sp-tenant-id")
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", sp_client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", sp_client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
                   f"https://login.microsoftonline.com/{sp_tenant_id}/oauth2/token")
    def abfss(container, path=""):
        base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
        return f"{base}/{path}" if path else base
    for c in ["bronze","silver","gold","source"]:
        dbutils.fs.ls(abfss(c))
    print("3. Storage OAuth connection (4)    : OK")
except Exception as e:
    print(f"3. Storage OAuth connection        : FAIL — {e}")
    errors.append("storage connection")

# 4. Folder structure exists
try:
    bronze_folders = [item.name for item in dbutils.fs.ls(abfss("bronze", "api"))]
    assert len(bronze_folders) >= 10
    print(f"4. Bronze/api folders ({len(bronze_folders)} found)     : OK")
except Exception as e:
    print(f"4. Bronze/api folders              : FAIL — {e}")
    errors.append("folder structure")

# 5. Storage write test
try:
    dbutils.fs.put(abfss("bronze", "_day2_test.txt"), "ok", overwrite=True)
    dbutils.fs.rm(abfss("bronze", "_day2_test.txt"))
    print("5. Storage write/delete            : OK")
except Exception as e:
    print(f"5. Storage write/delete            : FAIL — {e}")
    errors.append("storage write")

# 6. API auth test
try:
    api_base = dbutils.secrets.get(scope=SCOPE, key="voltgrid-api-base-url")
    username = dbutils.secrets.get(scope=SCOPE, key="voltgrid-username")
    password = dbutils.secrets.get(scope=SCOPE, key="voltgrid-password")
    resp = requests.post(f"{api_base}/api/auth/login/",
        json={"username": username, "password": password}, timeout=10)
    resp.raise_for_status()
    token = resp.json()["token"]
    print(f"6. API auth (token: {token[:8]}...)      : OK")
except Exception as e:
    print(f"6. API auth                        : FAIL — {e}")
    errors.append("API auth")

print("\n" + "=" * 60)
if errors:
    print(f"RESULT: {len(errors)} issue(s) found: {errors}")
    print("Fix the issues above before starting Day 3.")
else:
    print("RESULT: ALL CHECKS PASSED — Day 2 complete!")
print("=" * 60)
```

> **Expected output if everything worked:**
> ```
> 1. Secret scope kv-ev-scope        : OK
> 2. All 10 secrets readable         : OK
> 3. Storage OAuth connection (4)    : OK
> 4. Bronze/api folders (18 found)   : OK
> 5. Storage write/delete            : OK
> 6. API auth (token: abcd1234...)   : OK
> RESULT: ALL CHECKS PASSED — Day 2 complete!
> ```

---

## Part 10 — Unity Catalog: Browse Storage in Databricks UI (30 min)

> **Cost: ₹0** — Unity Catalog metadata, Storage Credentials, External Locations, and Volumes are all free. You only pay when a cluster actually reads data.

**Why do this?**
Without Unity Catalog wiring, your ADLS Gen2 containers are invisible inside Databricks — you can only access them by hardcoding `abfss://` paths in notebooks. After completing this part, you can:
- Click through `bronze`, `silver`, `gold` folders directly in the Databricks Catalog browser
- See files inside containers without writing any code
- Apply fine-grained table and column access control later (Day 7+)

**How the pieces connect:**

```
ADLS Gen2 Storage
  (evdatalakedev)
       │
       │  IAM role: Storage Blob Data Contributor
       ▼
Access Connector (ac-ev-intelligence-dev)
  — Azure resource with its own Managed Identity
       │
       │  registered as
       ▼
Storage Credential (in Unity Catalog)
  — UC object that wraps the Access Connector identity
       │
       │  one per container
       ▼
External Locations  (bronze_location / silver_location / gold_location)
  — UC object: name → abfss:// path
       │
       │  one per schema/container
       ▼
Volumes  (bronze_volume / silver_volume / gold_volume)
  — UC object: makes the path browsable in Catalog UI
```

---

### Part 10.1 — Create the Databricks Access Connector (Azure side)

**What is the Access Connector?**
The Access Connector is an Azure resource specifically designed to bridge Unity Catalog and Azure storage. It has its own Managed Identity — Unity Catalog uses this identity to authenticate with your storage account. You never see a key or token; Azure handles the authentication internally.

**Why not just use the existing Service Principal?**
Unity Catalog's Storage Credentials only support Managed Identity (`Azure Managed Identity` credential type) — not Service Principal client secrets or SAS tokens. The Access Connector provides that Managed Identity.

**Via Portal:**
1. Go to [https://portal.azure.com](https://portal.azure.com)
2. In the search bar at the top, type **Access Connector for Azure Databricks** → click it
3. Click **+ Create**
4. Fill in every field:
   - **Subscription:** your subscription
   - **Resource group:** `rg-ev-intelligence-dev`
   - **Name:** `ac-ev-intelligence-dev`
   - **Region:** `Central India`
   - **Managed identity:** `System assigned` ← this auto-creates a Managed Identity for the connector
5. Click **Review + Create** → **Create**
6. Wait ~30 seconds → click **Go to resource**
7. On the Overview page, look for **Identity** section or go to left menu → **Identity** — copy the **Object (principal) ID** — you need this for the IAM role assignment below

> **Propagation delay:** After creating the Access Connector, wait 1-2 minutes before assigning IAM roles — the Managed Identity needs time to appear in Azure Entra ID.

**Via CLI:**
```cmd
az databricks access-connector create --name ac-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --location centralindia --identity-type SystemAssigned
```

Get the principal ID:
```cmd
az databricks access-connector show --name ac-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query "identity.principalId" -o tsv
```
Copy this value — you need it for Part 10.2.

> **If CLI gives `'databricks' is not in the 'az' command group`:**
> Install the extension: `az extension add --name databricks`
> Then retry.

> **If CLI gives `MissingSubscriptionRegistration` for Microsoft.Databricks:**
> `az provider register --namespace Microsoft.Databricks`
> Wait 2 minutes → retry.

---

### Part 10.2 — Assign IAM Roles to the Access Connector

The Access Connector's Managed Identity needs permissions on your storage account before Unity Catalog can use it. Without this, External Location tests will fail with `403 Forbidden`.

**Roles required:**

| Role | On | Why needed |
|---|---|---|
| `Storage Blob Data Contributor` | Storage account `evdatalakedev` | Read and write files in ADLS Gen2 containers |
| `Storage Account Contributor` | Storage account `evdatalakedev` | Required for File Events (EventGrid) — optional but recommended |
| `EventGrid EventSubscription Contributor` | Resource group `rg-ev-intelligence-dev` | Create EventGrid subscriptions for file event notifications |
| `Storage Queue Data Contributor` | Storage account `evdatalakedev` | Write file event notifications to a storage queue |

> **Note on File Events:** The last 3 roles are only needed if you want File Events (instant notifications when files arrive, instead of directory polling). If you skip them, External Location creation will show a yellow warning — "File Events failed" — but Read/Write/List/Delete all work fine. For dev purposes, just the first role (`Storage Blob Data Contributor`) is enough to proceed.

**Via Portal — assign Storage Blob Data Contributor (required):**
1. Portal → **Storage accounts** → `evdatalakedev`
2. Left menu → **Access Control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. **Role** tab: search `Storage Blob Data Contributor` → select → click **Next**
5. **Members** tab:
   - **Assign access to:** select `Managed identity`
   - Click **+ Select members**
   - In the **Managed identity** dropdown: select `Access Connector for Azure Databricks`
   - You should see `ac-ev-intelligence-dev` in the list → select it → **Select**
6. Click **Review + assign** → **Review + assign**
7. Wait **2 minutes** before testing — RBAC propagates

> **Cannot find `ac-ev-intelligence-dev` in the member search?**
> The Access Connector was just created — wait 2-3 minutes and try again. Azure Entra ID propagation takes a moment.

**Via CLI — all 4 roles (CMD / PowerShell, run each line separately):**

First get the Access Connector's principal ID:
```cmd
az databricks access-connector show --name ac-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query "identity.principalId" -o tsv
```
Copy the output → `AC_PRINCIPAL_ID`

```cmd
az role assignment create --assignee-object-id <AC_PRINCIPAL_ID> --assignee-principal-type ServicePrincipal --role "Storage Blob Data Contributor" --scope /subscriptions/81dd57e1-876a-4fcc-8778-e06f68c13228/resourceGroups/rg-ev-intelligence-dev/providers/Microsoft.Storage/storageAccounts/evdatalakedev
```

For File Events (optional — skip if you just want basic read/write):
```cmd
az role assignment create --assignee-object-id <AC_PRINCIPAL_ID> --assignee-principal-type ServicePrincipal --role "Storage Account Contributor" --scope /subscriptions/81dd57e1-876a-4fcc-8778-e06f68c13228/resourceGroups/rg-ev-intelligence-dev/providers/Microsoft.Storage/storageAccounts/evdatalakedev

az role assignment create --assignee-object-id <AC_PRINCIPAL_ID> --assignee-principal-type ServicePrincipal --role "EventGrid EventSubscription Contributor" --scope /subscriptions/81dd57e1-876a-4fcc-8778-e06f68c13228/resourceGroups/rg-ev-intelligence-dev

az role assignment create --assignee-object-id <AC_PRINCIPAL_ID> --assignee-principal-type ServicePrincipal --role "Storage Queue Data Contributor" --scope /subscriptions/81dd57e1-876a-4fcc-8778-e06f68c13228/resourceGroups/rg-ev-intelligence-dev/providers/Microsoft.Storage/storageAccounts/evdatalakedev
```

**Verify IAM assignments via Portal:**
1. Storage account `evdatalakedev` → **Access Control (IAM)** → **Role assignments** tab
2. Filter by `Storage Blob Data Contributor` — you should see `ac-ev-intelligence-dev` listed
3. The **Type** column will show `Managed identity`

---

### Part 10.3 — Create Storage Credential in Unity Catalog

**What is a Storage Credential?**
It is a Unity Catalog object that stores a reference to your Access Connector. Think of it as telling Unity Catalog: "when you need to access storage, use the identity of `ac-ev-intelligence-dev`." The credential itself stores no keys — it just points to the Access Connector's Managed Identity.

**Why not SAS token here?** Unity Catalog Storage Credentials only support `Azure Managed Identity` and `AWS IAM Role`. SAS tokens are not a supported credential type. SAS-based access (for the shared external source storage) stays in notebooks only — it does not get a Storage Credential.

**Via Databricks UI:**
1. Databricks → left menu → **Catalog** (grid icon)
2. At the top of the Catalog pane, click the **gear icon** (⚙) or go to **Catalog → External Data → Storage Credentials**
3. Click **+ Create credential** or **+ Add a storage credential**
4. Fill in:
   - **Credential type:** `Storage Credential` (should be default)
   - **Credential Type dropdown:** `Azure Managed Identity` ← select this
     > You will see other options: AWS IAM Role, Cloudflare API Token, DBFS Root. Do NOT select these — they are for other cloud providers or legacy Databricks storage.
   - **Credential name:** `ac-ev-intelligence-dev`
   - **Access Connector ID:** paste the full resource ID:
     ```
     /subscriptions/81dd57e1-876a-4fcc-8778-e06f68c13228/resourceGroups/rg-ev-intelligence-dev/providers/Microsoft.Databricks/accessConnectors/ac-ev-intelligence-dev
     ```
     > **How to get this ID via CLI:**
     > ```cmd
     > az databricks access-connector show --name ac-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query id -o tsv
     > ```
     > **How to get this ID via Portal:**
     > Access Connector resource → Overview → click **JSON View** (top right) → copy the `id` field value
5. Click **Create**
6. The credential appears in the list with a green status

> **If you see `Access Connector not found` error:**
> The resource ID is wrong. Double-check the subscription ID, resource group name, and connector name — all must match exactly (case-sensitive).

> **If creation fails with `Permission denied`:**
> Your Databricks account needs the `Account admin` role in Databricks account console. Sign in at `accounts.azuredatabricks.net` → User management → confirm your user is Account admin.

---

### Part 10.4 — Create External Locations (one per container)

**What is an External Location?**
It maps a friendly name to an `abfss://` path. Once registered, Unity Catalog knows "bronze_location = `abfss://bronze@evdatalakedev.dfs.core.windows.net/`" and can apply governance rules on that path. You create one per container.

**Why `abfss://` and not `wasbs://`?**
`abfss://` uses the ADLS Gen2 hierarchical namespace endpoint and OAuth (via the Access Connector's Managed Identity). `wasbs://` uses the Blob Storage endpoint and only works with SAS tokens or account keys — Unity Catalog does not use those.

**Create bronze External Location via UI:**
1. Databricks → **Catalog** → **External Data** → **External Locations**
2. Click **+ Create location** → **Create location manually**
3. Fill in:
   - **External location name:** `bronze`
   - **URL:** `abfss://bronze@evdatalakedev.dfs.core.windows.net/`
   - **Storage credential:** select `ac-ev-intelligence-dev` (the one you just created)
4. Click **Create**
5. A test runs automatically — you will see a table with results:

   | Test | Expected result |
   |---|---|
   | Read | ✅ Success |
   | List | ✅ Success |
   | Write | ✅ Success |
   | Delete | ✅ Success |
   | Path Exists | ✅ Success |
   | Hierarchical Namespace Enabled | ✅ Success |
   | File Events Resource Provision | ⚠️ May show Failed (403) |
   | File Events Resource Teardown | ⚠️ May show Failed (403) |

6. **If File Events shows Failed:**
   The warning says: "Your storage credential can read and write to this location, but file events permissions could not be verified. File events are optional but recommended."
   - This means Read/Write/List/Delete **all passed** — the location works correctly
   - File Events needs the 3 extra IAM roles from Part 10.2 (EventGrid + Queue)
   - **For dev, click "Force create the location"** — works perfectly, just uses directory listing instead of event-based file detection
   - You can fix File Events later by assigning the 3 extra roles and re-testing

7. **If all checks including File Events passed:** Click **Create** normally.

8. Repeat for silver and gold containers:

   | Location name | URL |
   |---|---|
   | `silver` | `abfss://silver@evdatalakedev.dfs.core.windows.net/` |
   | `gold` | `abfss://gold@evdatalakedev.dfs.core.windows.net/` |

> **Tip: Do NOT create an External Location for the source blob storage (`dataenggdailystorage`).**
> That storage uses SAS token auth, which is not supported by Unity Catalog Storage Credentials. Access to that storage stays in notebooks using `wasbs://` + `spark.conf.set()` — which is exactly what `02_read_source_blob.ipynb` does.

**Via CLI (Databricks CLI — optional, Portal UI is easier):**
```cmd
databricks external-locations create --name bronze --url "abfss://bronze@evdatalakedev.dfs.core.windows.net/" --credential-name ac-ev-intelligence-dev
databricks external-locations create --name silver --url "abfss://silver@evdatalakedev.dfs.core.windows.net/" --credential-name ac-ev-intelligence-dev
databricks external-locations create --name gold   --url "abfss://gold@evdatalakedev.dfs.core.windows.net/"   --credential-name ac-ev-intelligence-dev
```

**Verify all 3 External Locations:**
1. Databricks → **Catalog** → **External Data** → **External Locations**
2. You should see `bronze`, `silver`, `gold` all listed with status Active
3. Click any location → click **Test connection** to re-run the test anytime

---

### Part 10.5 — Create Volumes (make storage browsable in Catalog UI)

**What is a Volume?**
A Volume is a Unity Catalog object that creates a browsable "folder shortcut" inside the Catalog tree. It points to a path in an External Location. After creating a volume, you can expand `Catalog → your_catalog → schema → volume_name` in the UI and browse files — just like a file explorer.

**Cost: ₹0** — Volumes are pure metadata. No data is copied or moved. The actual files stay in ADLS Gen2 exactly where they are.

**Before creating a Volume, you need a Catalog and Schema.**

**Step 1 — Verify Unity Catalog is enabled:**
1. Databricks → **Catalog** browser (left menu grid icon)
2. You should see `dbw_ev_intelligence_dev` (your catalog) in the list
3. If you only see `hive_metastore` — Unity Catalog is not enabled. Contact your Databricks account admin.

**Step 2 — Create schemas inside your catalog:**

You need one schema per container (schema = database in Unity Catalog terminology).

Via Databricks UI:
1. **Catalog** → expand `dbw_ev_intelligence_dev`
2. Click the **+** icon or right-click → **Create schema**
3. Schema name: `bronze` → **Create**
4. Repeat for `silver` and `gold`

Via SQL in a notebook:
```sql
-- Run this in a SQL notebook or use %sql in a Python notebook
CREATE SCHEMA IF NOT EXISTS dbw_ev_intelligence_dev.bronze;
CREATE SCHEMA IF NOT EXISTS dbw_ev_intelligence_dev.silver;
CREATE SCHEMA IF NOT EXISTS dbw_ev_intelligence_dev.gold;
```

**Step 3 — Create Volumes (one per schema):**

Via Databricks UI:
1. **Catalog** → expand `dbw_ev_intelligence_dev` → expand `bronze` schema
2. Click the **+** icon → **Create volume**
3. Fill in:
   - **Volume name:** `bronze_volume`
   - **Volume type:** `External` ← select this (not Managed)
     > **Managed vs External volume:**
     > - Managed: Databricks controls the storage path — files are stored inside the catalog's default location. Used for temporary data.
     > - External: You specify the path in your own ADLS Gen2 — your existing data is exposed as-is. Use this.
   - **External location:** select `bronze` (the one from Part 10.4)
   - **Path:** leave empty or type `/` — this means the root of the `bronze` container
4. Click **Create**
5. The volume appears under `bronze` schema → click it → you can now browse files

Repeat for silver and gold:
| Volume name | Schema | External location | Path |
|---|---|---|---|
| `bronze_volume` | `bronze` | `bronze` | `/` |
| `silver_volume` | `silver` | `silver` | `/` |
| `gold_volume` | `gold` | `gold` | `/` |

Via SQL (faster, run all at once in a notebook):
```sql
CREATE EXTERNAL VOLUME IF NOT EXISTS dbw_ev_intelligence_dev.bronze.bronze_volume
  LOCATION 'abfss://bronze@evdatalakedev.dfs.core.windows.net/';

CREATE EXTERNAL VOLUME IF NOT EXISTS dbw_ev_intelligence_dev.silver.silver_volume
  LOCATION 'abfss://silver@evdatalakedev.dfs.core.windows.net/';

CREATE EXTERNAL VOLUME IF NOT EXISTS dbw_ev_intelligence_dev.gold.gold_volume
  LOCATION 'abfss://gold@evdatalakedev.dfs.core.windows.net/';
```

**Verify volumes are browsable:**
1. **Catalog** tree → expand `dbw_ev_intelligence_dev` → expand `bronze` → expand `bronze_volume`
2. You should see the folders you created in Part 2: `api/`, `blob/`, `streaming/`, `_checkpoints/`
3. Click `api/` → you should see the 18 endpoint folders
4. If folders appear empty — this is correct for now. Data gets written here starting Day 3.

> **If Volume creation fails with `PERMISSION_DENIED`:**
> Your Databricks user does not have `CREATE VOLUME` privilege on the schema. Grant it:
> ```sql
> GRANT CREATE VOLUME ON SCHEMA dbw_ev_intelligence_dev.bronze TO `your-email@domain.com`;
> ```
> Or via UI: Catalog → right-click schema → Permissions → add your user with CREATE VOLUME privilege.

> **If volume shows in catalog but clicking Browse shows empty:**
> This is normal if Part 2 (folder creation) hasn't run yet. Run the `01_create_folder_structure` notebook from Part 2 first.

> **If volume shows "External location not found":**
> The External Location name in the Volume definition doesn't match. Verify via Catalog → External Data → External Locations that the location name is exactly `bronze` (lowercase).

---

### Part 10.6 — Verify Unity Catalog setup via Notebook

Create a new notebook to confirm the full Unity Catalog setup is working:

1. Databricks → **Workspace** → **+ New** → **Notebook**
2. Name: `03_verify_unity_catalog`
3. Language: Python → Attach to `dev-cluster`

```python
# Cell 1 — Verify External Locations are registered
print("=== External Locations ===")
locations = spark.sql("SHOW EXTERNAL LOCATIONS").collect()
for loc in locations:
    print(f"  {loc['name']:<20} → {loc['url']}")

if not locations:
    print("  No external locations found — complete Part 10.4 first")
```

```python
# Cell 2 — Verify schemas exist
print("=== Schemas in dbw_ev_intelligence_dev ===")
schemas = spark.sql("SHOW SCHEMAS IN dbw_ev_intelligence_dev").collect()
for s in schemas:
    print(f"  {s['databaseName']}")
```

```python
# Cell 3 — Verify volumes exist and list their contents
for container in ["bronze", "silver", "gold"]:
    print(f"\n=== Volume: {container}_volume ===")
    try:
        items = dbutils.fs.ls(f"/Volumes/dbw_ev_intelligence_dev/{container}/{container}_volume/")
        for item in items:
            print(f"  {item.name}")
        print(f"  → {len(items)} items found")
    except Exception as e:
        print(f"  ERROR: {e}")
        print(f"  → Create the volume first via Part 10.5")
```

```python
# Cell 4 — Write and read a test file through the Volume path
print("=== Volume write/read test ===")
test_path = "/Volumes/dbw_ev_intelligence_dev/bronze/bronze_volume/_uc_test.txt"

try:
    dbutils.fs.put(test_path, "unity catalog volume write test ok", overwrite=True)
    content = dbutils.fs.head(test_path)
    dbutils.fs.rm(test_path)
    print(f"  Write → Read → Delete via Volume path : OK")
    print(f"  Content read back: {content}")
except Exception as e:
    print(f"  ERROR: {e}")
    print("  → Check External Location and Volume are correctly configured")
```

```python
# Cell 5 — Summary
print("\n" + "=" * 55)
print("UNITY CATALOG VERIFICATION SUMMARY")
print("=" * 55)
print("  If all cells above showed OK:")
print("  ✓ Storage Credential created (ac-ev-intelligence-dev)")
print("  ✓ External Locations registered (bronze / silver / gold)")
print("  ✓ Volumes created and browsable in Catalog UI")
print("  ✓ Files accessible via /Volumes/ path")
print("\n  You can now browse ADLS folders in the Catalog UI:")
print("  Catalog → dbw_ev_intelligence_dev → bronze → bronze_volume")
print("=" * 55)
```

---

### Part 10.7 — Permission Summary (what was assigned and why)

This table summarises every permission needed for Unity Catalog, and why each one is required:

| Permission | Assigned to | On | Why |
|---|---|---|---|
| `Storage Blob Data Contributor` | Access Connector Managed Identity | Storage account `evdatalakedev` | Allows the Access Connector to read and write blobs/files in ADLS Gen2 containers. Without this, all External Location tests fail with 403. |
| `Storage Account Contributor` | Access Connector Managed Identity | Storage account `evdatalakedev` | Allows the Access Connector to register EventGrid subscriptions for File Events (instant file-arrival notifications). Optional — skip for dev. |
| `EventGrid EventSubscription Contributor` | Access Connector Managed Identity | Resource group `rg-ev-intelligence-dev` | Allows creating EventGrid event subscriptions at the resource group level. Required for File Events. Optional — skip for dev. |
| `Storage Queue Data Contributor` | Access Connector Managed Identity | Storage account `evdatalakedev` | File Events write notifications to an Azure Storage Queue. The Access Connector needs write permission on that queue. Optional — skip for dev. |
| `CREATE EXTERNAL LOCATION` | Your Databricks user | Unity Catalog metastore | Required to create External Location objects in Unity Catalog. Account admin role grants this automatically. |
| `CREATE SCHEMA` | Your Databricks user | Catalog `dbw_ev_intelligence_dev` | Required to create schemas (bronze, silver, gold) inside the catalog. |
| `CREATE VOLUME` | Your Databricks user | Schema `bronze/silver/gold` | Required to create Volume objects under each schema. |

**UI path to verify IAM roles (Azure side):**
- Portal → `evdatalakedev` storage → **Access Control (IAM)** → **Role assignments** tab
- Filter by: Role = `Storage Blob Data Contributor`
- You should see `ac-ev-intelligence-dev` in the list with Type = `Managed identity`

**UI path to verify Unity Catalog permissions (Databricks side):**
- Catalog → right-click `dbw_ev_intelligence_dev` → **Permissions**
- Or: Catalog → External Data → Storage Credentials → click `ac-ev-intelligence-dev` → **Permissions**

---

## Day 2 Checklist

- [ ] Day 1 verification notebook ran — storage OAuth connection and secrets confirmed OK
- [ ] Bronze/Silver/Gold folder structure created — all 40+ folders via Databricks notebook
- [ ] Verified in Databricks: `dbutils.fs.ls(abfss("bronze", "api"))` shows 18 folders
- [ ] Verified in Portal: browse `bronze/api/` and confirm all 18 endpoint folders exist
- [ ] Key Vault secrets confirmed: `voltgrid-api-base-url`, `voltgrid-username`, `voltgrid-password`
- [ ] Key Vault secrets confirmed: `sp-client-id`, `sp-client-secret`, `sp-tenant-id`
- [ ] Key Vault secrets confirmed: `adls-account-name`
- [ ] Event Hub namespace `evh-ev-intelligence-dev` created (Basic, 1 TU, Central India)
- [ ] Event Hub topic `iot-telemetry` created (4 partitions, 1-day retention)
- [ ] Event Hub topic `maintenance-alerts` created
- [ ] Event Hub connection string stored in Key Vault as `eventhub-connection-string`
- [ ] Key Vault is on RBAC permission model (confirmed in Access configuration)
- [ ] SP `sp-ev-intelligence-dev` has `Key Vault Secrets User` role on Key Vault (IAM verified)
- [ ] Your account has `Key Vault Administrator` role on Key Vault (IAM verified)
- [ ] Managed Identity `mi-ev-intelligence-dev` created in Central India
- [ ] Managed Identity has `Storage Blob Data Contributor` on `evdatalakedev` (IAM verified)
- [ ] Managed Identity has `Key Vault Secrets User` on Key Vault (IAM verified)
- [ ] Notebook `02_secrets_pattern` runs without errors — all secrets load, storage write/delete passes
- [ ] Lifecycle policy visible in storage → Lifecycle management (either `move-to-cool` or `tier-old-data`)
- [ ] End-to-end verification notebook `day2_99_verify` shows ALL CHECKS PASSED
- [ ] **Unity Catalog — Access Connector**
  - [ ] `ac-ev-intelligence-dev` Access Connector created in `rg-ev-intelligence-dev`, Central India
  - [ ] System-assigned Managed Identity enabled on the connector
  - [ ] `Storage Blob Data Contributor` role assigned to connector's Managed Identity on `evdatalakedev` (verified in IAM)
  - [ ] (Optional) `Storage Account Contributor`, `EventGrid EventSubscription Contributor`, `Storage Queue Data Contributor` assigned for File Events
- [ ] **Unity Catalog — Storage Credential**
  - [ ] Storage Credential `ac-ev-intelligence-dev` created in Databricks → Catalog → External Data → Storage Credentials
  - [ ] Credential type = Azure Managed Identity pointing to the Access Connector resource ID
- [ ] **Unity Catalog — External Locations**
  - [ ] External Location `bronze` created → URL: `abfss://bronze@evdatalakedev.dfs.core.windows.net/`
  - [ ] External Location `silver` created → URL: `abfss://silver@evdatalakedev.dfs.core.windows.net/`
  - [ ] External Location `gold` created → URL: `abfss://gold@evdatalakedev.dfs.core.windows.net/`
  - [ ] All 3 locations tested: Read/List/Write/Delete = Success (File Events warning is OK to ignore for dev)
- [ ] **Unity Catalog — Schemas and Volumes**
  - [ ] Schemas created: `dbw_ev_intelligence_dev.bronze`, `.silver`, `.gold`
  - [ ] Volumes created: `bronze_volume`, `silver_volume`, `gold_volume` (External type)
  - [ ] Volumes are browsable in Catalog UI — can see `api/`, `blob/`, `streaming/` folders under `bronze_volume`
  - [ ] Notebook `03_verify_unity_catalog` runs — all cells show OK
- [ ] **Cluster terminated at end of session**

---

## End of Session — STOP THE CLUSTER

**Do this every single time before closing your laptop:**

1. Databricks → left menu **Compute**
2. Click `dev-cluster`
3. Click **Terminate**
4. Wait for status → **Terminated**

> Auto-termination is set to 15 minutes but do not rely on it — terminate manually every time.

---

## Common Errors on Day 2

| Error | Root Cause | Fix |
|---|---|---|
| `mkdirs` fails with 403 | SP missing `Storage Blob Data Contributor` role | Storage account → IAM → confirm role is assigned to `sp-ev-intelligence-dev` |
| Key Vault secret not found via `dbutils.secrets.get()` | Secret name is case-sensitive | Check exact spelling — `sp-client-id` not `SP-Client-ID` |
| `dbutils.secrets.get()` throws even though secret exists in Portal | `AzureDatabricks` enterprise app lost `Key Vault Secrets User` role | Key Vault → IAM → verify `AzureDatabricks` has the role, re-assign if missing, wait 2 min |
| Event Hub namespace name taken | Namespace names are globally unique | Add your initials: `evh-ev-intelligence-dev-hs` |
| `az eventhubs namespace create` gives `MissingSubscriptionRegistration` | EventHub provider not registered | `az provider register --namespace Microsoft.EventHub` → wait 2 min → retry |
| RBAC assignment fails — `AuthorizationFailed` | Your account needs Owner or User Access Administrator on the resource | Go to Portal → resource → IAM → check your current role. You need Owner or User Access Administrator to assign roles |
| After switching Key Vault to RBAC, you get `ForbiddenByRbac` on your own account | Access Policies no longer work after switching to RBAC | Add yourself as `Key Vault Administrator` via IAM immediately after switching |
| Managed Identity not found in IAM member search | Propagation delay — MI just created | Wait 2-3 minutes then search again in the IAM dialog |
| `az role assignment create` gives `principalNotFound` | MI or SP not yet propagated to Azure AD | Wait 2-3 minutes and retry the assignment |
| `RoleAssignmentAlreadyExists` from CLI | Role already assigned (maybe from Day 1) | Not an error — ignore it. Verify with `az role assignment list` |
| `ObjectIsDeletedButRecoverable` on `az keyvault secret set` | Secret deleted but still in soft-delete | Recover: `az keyvault secret recover --vault-name kv-ev-intelligence-dev --name "<name>"` then retry |
| Event Hub connection string shows `[REDACTED]` in notebook output | Working correctly | That is correct — Databricks masks all secret values as `[REDACTED]` in output automatically |
| API returns 401 in Cell 2 of `02_secrets_pattern` | Token expired (tokens expire after ~24 hours) | Re-run Cell 2 to get a fresh token — this is expected behavior |
| Storage write test fails with `InvalidAuthenticationInfo` | SP client secret in Key Vault is wrong or expired | App registrations → `sp-ev-intelligence-dev` → create new client secret → update Key Vault `sp-client-secret` → re-run storage auth cells |
| Unity Catalog: Storage Credential dropdown shows no SAS token option | By design — Unity Catalog only supports Managed Identity or AWS IAM | Use `Azure Managed Identity` with the Access Connector. SAS-based access stays in notebooks using `wasbs://` |
| External Location test: Read/List/Write/Delete ✅ but File Events ❌ 403 | Access Connector missing `Storage Account Contributor`, `EventGrid EventSubscription Contributor`, `Storage Queue Data Contributor` roles | Click "Force create the location" to proceed immediately — File Events are optional. Add the 3 extra roles later (Part 10.2) to fix the warning |
| External Location test: all checks fail with 403 | Access Connector Managed Identity missing `Storage Blob Data Contributor` on storage account | Portal → `evdatalakedev` → IAM → confirm `ac-ev-intelligence-dev` has `Storage Blob Data Contributor`. If missing, assign it and wait 2 minutes |
| `Access Connector not found` when creating Storage Credential | Resource ID is wrong or typo in subscription/RG/connector name | Get correct ID via CLI: `az databricks access-connector show --name ac-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query id -o tsv` |
| Cannot find `ac-ev-intelligence-dev` in IAM member search | Access Connector Managed Identity not yet propagated to Entra ID | Wait 2-3 minutes after creating the Access Connector, then search again in the IAM member picker |
| Volume creation fails with `PERMISSION_DENIED` | Databricks user lacks `CREATE VOLUME` on the schema | `GRANT CREATE VOLUME ON SCHEMA dbw_ev_intelligence_dev.bronze TO 'your-email@domain.com'` — or grant via Catalog UI permissions |
| Volume shows but Browse shows empty | Part 2 folder creation hasn't run yet | Run `01_create_folder_structure` notebook (Part 2) — folders need to exist before they appear in Browse |
| Volume path `/Volumes/...` gives `Path does not exist` | Volume name or schema name has a typo | Check exact path: `/Volumes/<catalog>/<schema>/<volume_name>/` — all lowercase, matching exactly what you created |
| `'databricks' is not in the 'az' command group` | Azure CLI Databricks extension not installed | `az extension add --name databricks` |

---

## Security Rules for This Project

1. **Never** put a password, key, or token directly in a notebook cell — always `dbutils.secrets.get()`
2. **Never** commit `.env` files or any file with credentials to git
3. **Always** use the SP or Managed Identity for service-to-service auth — never a human account's credentials
4. **Always** store a new secret in Key Vault before writing the code that uses it
5. **Rotate** the SP client secret every 90 days — set a calendar reminder now
6. **Never** print a full secret value — always print only the first 8 characters: `token[:8]...[REDACTED]`
