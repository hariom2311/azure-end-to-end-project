# 02 — ADF Pipeline: API Payments → Bronze
**Day 2 | Step 2 of 3**

Build the `pl_bronze_api_payments` pipeline — authenticates to VoltGrid API using credentials from Key Vault, then copies one page of raw payment records as JSON into ADLS Gen2 Bronze layer.

> **Scope note:** This pipeline covers the VoltGrid REST API only. The source blob storage (`dataenggdailystorage`) is accessed from Databricks notebooks — not ADF. Blob ingestion is covered in Day 3.

---

## Pipeline Overview

| Property | Value |
|---|---|
| Pipeline name | `pl_bronze_api_payments` |
| Source | VoltGrid REST API — `GET /api/db/payments/` |
| Sink | ADLS Gen2 Bronze — `bronze/api/payments/raw/payments.json` |
| Auth pattern | Key Vault → login → runtime token → Copy Activity header |
| Activities | 5 (get_username → get_password → api_login → set_token → copy_payments) |
| Parameters | `p_page` (default 1), `p_page_size` (default 100) |
| Day 2 goal | Prove connectivity: Key Vault → API → ADLS. One page only. |
| Pagination | Not in Day 2. Full pagination + incremental load → Day 3. |

---

## Part A — Create Datasets

Datasets are pointers to the source and sink locations. Create both datasets before building the pipeline — the pipeline JSON references them by name.

---

### Dataset 1 — Source: VoltGrid Payments API (`ds_voltgrid_payments_src`)

**What it is:** Points to `GET /api/db/payments/` on the VoltGrid API. The page and page size are parameters — the pipeline passes them at runtime.

**Why parameters instead of hardcoded values?**
You can trigger the pipeline with different page numbers (e.g. page 2 for a quick test) without changing the dataset definition.

#### UI Steps

1. ADF Studio → **Author** (pencil icon) → **Datasets** → **+** → **New dataset**
2. Search `REST` → select **REST** → **Continue**
3. Fill in:
   - **Name:** `ds_voltgrid_payments_src`
   - **Linked service:** `ls_voltgrid_api`
   - **Relative URL:** `/api/db/payments/`
4. Click **OK**
5. In the dataset editor → **Parameters** tab → add:
   - `p_page` — type: `int` — default: `1`
   - `p_page_size` — type: `int` — default: `100`
6. In the **Connection** tab → **Relative URL** field → click **Add dynamic content**:
   ```
   /api/db/payments/?page=@{dataset().p_page}&page_size=@{dataset().p_page_size}
   ```
7. Click **Publish all**

#### JSON (paste via `{ }` Code button)

File: `adf_pipeline_json/ds_voltgrid_payments_src.json`

```json
{
  "name": "ds_voltgrid_payments_src",
  "properties": {
    "linkedServiceName": {
      "referenceName": "ls_voltgrid_api",
      "type": "LinkedServiceReference"
    },
    "parameters": {
      "p_page":      { "type": "int", "defaultValue": 1 },
      "p_page_size": { "type": "int", "defaultValue": 100 }
    },
    "type": "RestResource",
    "typeProperties": {
      "relativeUrl": {
        "value": "@concat('/api/db/payments/?page=', string(dataset().p_page), '&page_size=', string(dataset().p_page_size))",
        "type": "Expression"
      }
    }
  }
}
```

> In ADF Studio: Author → Datasets → `+` → Code button (`{ }`) → select all → paste → OK → Publish all

---

### Dataset 2 — Sink: ADLS Gen2 Bronze (`ds_bronze_payments_sink`)

**What it is:** Points to a fixed file path in the `bronze` container. Day 2 always writes to the same file — no date partitioning yet. That is added in Day 3.

#### UI Steps

1. ADF Studio → **Author** → **Datasets** → **+** → **New dataset**
2. Search `Azure Data Lake Storage Gen2` → **Continue**
3. Select **JSON** as format → **Continue**
4. Fill in:
   - **Name:** `ds_bronze_payments_sink`
   - **Linked service:** `ls_adls_bronze`
   - **File path:** `bronze` / `api/payments/raw` / `payments.json`
5. Click **OK** → **Publish all**

#### JSON (paste via `{ }` Code button)

File: `adf_pipeline_json/ds_bronze_payments_sink.json`

```json
{
  "name": "ds_bronze_payments_sink",
  "properties": {
    "linkedServiceName": {
      "referenceName": "ls_adls_bronze",
      "type": "LinkedServiceReference"
    },
    "type": "Json",
    "typeProperties": {
      "location": {
        "type": "AzureBlobFSLocation",
        "fileSystem": "bronze",
        "folderPath": "api/payments/raw",
        "fileName": "payments.json"
      }
    }
  }
}
```

---

## Part B — Build the Pipeline

**Pipeline name:** `pl_bronze_api_payments`
**Activities (in order):**

| # | Activity | Type | What it does |
|---|---|---|---|
| 1 | `act_get_username` | Web Activity | Reads `voltgrid-username` from Key Vault via ADF Managed Identity |
| 2 | `act_get_password` | Web Activity | Reads `voltgrid-password` from Key Vault via ADF Managed Identity |
| 3 | `act_api_login` | Web Activity | POST to `/api/auth/login/` — gets a bearer token |
| 4 | `act_set_token` | Set Variable | Stores the token in `v_token` for the Copy Activity |
| 5 | `act_copy_payments` | Copy Activity | Calls `GET /api/db/payments/?page={p_page}&page_size={p_page_size}` with token → writes raw JSON to Bronze |

---

### Step 1 — Create the Pipeline

1. ADF Studio → **Author** → **Pipelines** → **+** → **New pipeline**
2. **Name:** `pl_bronze_api_payments`
3. In the **Parameters** tab (bottom panel) → add:
   - `p_page` — type: `int` — default: `1`
   - `p_page_size` — type: `int` — default: `100`
4. In the **Variables** tab → add:
   - `v_token` — type: `String`

---

### Step 2 — Activity 1: `act_get_username`

**Type:** Web Activity

1. Drag **Web Activity** from the Activities pane onto the canvas
2. **Name:** `act_get_username`
3. **Settings** tab:
   - **URL:** `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-username/?api-version=7.0`
   - **Method:** GET
   - **Authentication:** System Assigned Managed Identity
   - **Resource:** `https://vault.azure.net`

---

### Step 3 — Activity 2: `act_get_password`

**Type:** Web Activity

1. Drag another **Web Activity** onto the canvas
2. **Name:** `act_get_password`
3. Draw a success arrow from `act_get_username` → `act_get_password`
4. **Settings:**
   - **URL:** `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-password/?api-version=7.0`
   - **Method:** GET
   - **Authentication:** System Assigned Managed Identity
   - **Resource:** `https://vault.azure.net`

---

### Step 4 — Activity 3: `act_api_login`

**Type:** Web Activity

1. Drag another **Web Activity** → name it `act_api_login`
2. Connect success arrow from `act_get_password` → `act_api_login`
3. **Settings:**
   - **URL:** `https://ev-project-navy-mu.vercel.app/api/auth/login/`
   - **Method:** POST
   - **Headers:** `Content-Type` = `application/json`
   - **Body:** click **Add dynamic content**:
     ```
     @concat('{"username":"', activity('act_get_username').output.value, '","password":"', activity('act_get_password').output.value, '"}')
     ```

**What this returns:**
```json
{ "token": "abc123xyz..." }
```

---

### Step 5 — Activity 4: `act_set_token`

**Type:** Set Variable

1. Drag **Set Variable** → name it `act_set_token`
2. Connect success arrow from `act_api_login` → `act_set_token`
3. **Settings:**
   - **Variable:** `v_token`
   - **Value:** click **Add dynamic content**:
     ```
     @activity('act_api_login').output.token
     ```

---

### Step 6 — Activity 5: `act_copy_payments`

**Type:** Copy Activity — this is the main data movement step.

1. Drag **Copy data** → name it `act_copy_payments`
2. Connect success arrow from `act_set_token` → `act_copy_payments`

#### Source tab:
- **Source dataset:** `ds_voltgrid_payments_src`
- **Dataset parameters:**
  - `p_page` → `@pipeline().parameters.p_page`
  - `p_page_size` → `@pipeline().parameters.p_page_size`
- **Additional headers:**
  - **Name:** `Authorization`
  - **Value:** click **Add dynamic content** → `@concat('Token ', variables('v_token'))`
- **Request method:** GET

#### Sink tab:
- **Sink dataset:** `ds_bronze_payments_sink`
- **File pattern:** setOfObjects

#### Settings tab:
- Leave defaults — no staging needed

---

### Full Pipeline JSON (paste via `{ }` Code button)

File: `adf_pipeline_json/pl_bronze_api_payments.json`

> ADF Studio: Author → Pipelines → `pl_bronze_api_payments` → `{ }` Code button → select all → paste → OK → Publish all

```json
{
  "name": "pl_bronze_api_payments",
  "properties": {
    "description": "Day 2 — simple payments pipeline. Reads one page of VoltGrid API and stores raw JSON in Bronze. Authentication via Key Vault MSI. Pagination and incremental load added in Day 3.",
    "parameters": {
      "p_page":      { "type": "int", "defaultValue": 1 },
      "p_page_size": { "type": "int", "defaultValue": 100 }
    },
    "variables": {
      "v_token": { "type": "String" }
    },
    "activities": [
      {
        "name": "act_get_username",
        "type": "WebActivity",
        "dependsOn": [],
        "typeProperties": {
          "url": "https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-username/?api-version=7.0",
          "method": "GET",
          "authentication": {
            "type": "MSI",
            "resource": "https://vault.azure.net"
          }
        }
      },
      {
        "name": "act_get_password",
        "type": "WebActivity",
        "dependsOn": [
          { "activity": "act_get_username", "dependencyConditions": ["Succeeded"] }
        ],
        "typeProperties": {
          "url": "https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-password/?api-version=7.0",
          "method": "GET",
          "authentication": {
            "type": "MSI",
            "resource": "https://vault.azure.net"
          }
        }
      },
      {
        "name": "act_api_login",
        "type": "WebActivity",
        "dependsOn": [
          { "activity": "act_get_password", "dependencyConditions": ["Succeeded"] }
        ],
        "typeProperties": {
          "url": "https://ev-project-navy-mu.vercel.app/api/auth/login/",
          "method": "POST",
          "headers": { "Content-Type": "application/json" },
          "body": {
            "value": "@concat('{\"username\":\"', activity('act_get_username').output.value, '\",\"password\":\"', activity('act_get_password').output.value, '\"}')",
            "type": "Expression"
          }
        }
      },
      {
        "name": "act_set_token",
        "type": "SetVariable",
        "dependsOn": [
          { "activity": "act_api_login", "dependencyConditions": ["Succeeded"] }
        ],
        "typeProperties": {
          "variableName": "v_token",
          "value": {
            "value": "@activity('act_api_login').output.token",
            "type": "Expression"
          }
        }
      },
      {
        "name": "act_copy_payments",
        "type": "Copy",
        "dependsOn": [
          { "activity": "act_set_token", "dependencyConditions": ["Succeeded"] }
        ],
        "typeProperties": {
          "source": {
            "type": "RestSource",
            "additionalHeaders": {
              "Authorization": {
                "value": "@concat('Token ', variables('v_token'))",
                "type": "Expression"
              }
            },
            "requestMethod": "GET"
          },
          "sink": {
            "type": "JsonSink",
            "storeSettings": { "type": "AzureBlobFSWriteSettings" },
            "formatSettings": {
              "type": "JsonWriteSettings",
              "filePattern": "setOfObjects"
            }
          },
          "enableStaging": false
        },
        "inputs": [
          {
            "referenceName": "ds_voltgrid_payments_src",
            "type": "DatasetReference",
            "parameters": {
              "p_page":      { "value": "@pipeline().parameters.p_page",      "type": "Expression" },
              "p_page_size": { "value": "@pipeline().parameters.p_page_size", "type": "Expression" }
            }
          }
        ],
        "outputs": [
          {
            "referenceName": "ds_bronze_payments_sink",
            "type": "DatasetReference"
          }
        ]
      }
    ]
  }
}
```

---

## Part C — Trigger the Pipeline

### Manual trigger (Day 2 test)

1. ADF Studio → **Author** → `pl_bronze_api_payments`
2. Click **Add trigger** → **Trigger now**
3. Enter parameters:
   - `p_page`: `1`
   - `p_page_size`: `100`
4. Click **OK**
5. Left sidebar → **Monitor** → watch the pipeline run

**Expected result:** All 5 activities show green checkmarks. Run time ~10–20 seconds.

---

## Verify the Output

### In ADF Monitor

1. **Monitor** → **Pipeline runs** → click the run
2. Confirm all 5 activities succeeded
3. Click `act_copy_payments` → **Output** → rows read and written should match

### In Databricks

Run in a Databricks notebook after the pipeline completes:

```python
# List the output
display(dbutils.fs.ls("abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/"))

# Read the raw JSON
df = spark.read.option("multiLine", "true").json(
    "abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/payments.json"
)
display(df.limit(3))
print(f"Records in this page: {df.count()}")
```

Expected output — a DataFrame with payment fields: `payment_id`, `session_id`, `customer_id`, `amount_aud`, `status`, etc.

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `act_get_username` fails with 403 | ADF Managed Identity missing `Key Vault Secrets User` role | Portal → Key Vault → IAM → assign role to ADF MI, wait 2 min |
| `act_api_login` fails with 401 | Wrong credentials in Key Vault | Check `voltgrid-username` and `voltgrid-password` values |
| `act_copy_payments` fails with 401 | Token not stored — `act_api_login` output key is wrong | Monitor → `act_api_login` output → confirm `.output.token` key exists |
| `act_copy_payments` fails with 403 | ADF MI missing `Storage Blob Data Contributor` on `evdatalakedev` | Portal → Storage → IAM → assign role, wait 2 min |
| Output file is empty | API returned 0 records | Test with `p_page_size=10` first; verify linked service URL |
| Dataset `ds_voltgrid_payments_src` not found | Dataset was not published before pasting pipeline | Create and publish both datasets before pasting the pipeline JSON |

---

## What Day 3 Adds

Day 2 proves the connection works. Day 3 (`day_3_*/adf_pipeline_json/`) upgrades to:

| Feature | Day 2 | Day 3 |
|---|---|---|
| Pages fetched | 1 (manual parameter) | All pages (Until loop) |
| Load type | Full only | Full + incremental (`updated_after` watermark) |
| Watermark source | None — manual input | Auto-read from `pipeline_audit` Delta table |
| Audit trail | None | Row written to `pipeline_audit` after every run |
| Sink path | Fixed `payments.json` | Partitioned `ingestion_date=YYYY-MM-DD/page_N.json` |
| Pipeline | `pl_bronze_api_payments` | `pl_bronze_api_payments_v3` |

---

## Next Step

→ `05_UNITY_CATALOG_EXTERNAL_LOCATIONS.md` — set up Access Connector, Storage Credential, External Locations, and Volumes
