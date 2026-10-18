# Day 4 — ADF Incremental Pipeline: Pagination + Watermark

**Duration:** 3 hours | **Goal:** Wire ADF to the Day 3 control table and Databricks notebooks, build `pl_bronze_incremental_load` with full pagination via Until loop, and schedule it with a Tumbling Window trigger.

> **Prerequisites:** Day 3 complete — `ev_catalog.ctl.pipeline_control` exists, payments row inserted with `load_type = 'full'` and `last_watermark = '1900-01-01T00:00:00Z'`, both notebooks created and tested.

---

## BEFORE YOU START — Student Customization Guide

**You cannot copy-paste the JSON files as-is.** Every value below is specific to the instructor's Azure environment. Replace each one with your own value before publishing.

### ADF Artifacts

| Where | What to Change | Instructor's Value | How to Find Yours |
|---|---|---|---|
| All pipelines | ADF factory name (in resource IDs) | `adf-ev-intelligence-dev` | Portal → Azure Data Factory |
| `act_get_username` & `act_get_password` URLs | Key Vault name | `kv-ev-intelligence-dev02` | Portal → Key Vault |
| `act_api_login` URL | API base URL | `https://ev-project-navy-mu.vercel.app` | Shared by instructor |
| `act_fetch_page1_meta` URL | Same API base URL | `https://ev-project-navy-mu.vercel.app` | Same |
| `act_read_ctl` notebook path | Databricks notebook path | `/Users/hariomsuryawanshi68258@gmail.com/nb_read_control_table` | Databricks → right-click notebook → Copy path |
| `act_update_watermark` notebook path | Databricks notebook path | `/Users/hariomsuryawanshi68258@gmail.com/nb_update_watermark` | Same |
| `ds_voltgrid_payments_src` linked service | Linked service name | `ls_voltgrid_api` | Must match what you created in Day 2 |
| `ds_bronze_payments_sink` linked service | Linked service name | `ls_adls_bronze` | Must match what you created in Day 2 |
| `ds_bronze_payments_sink` fileSystem | Container name | `bronze` | Your ADLS Gen2 container name |
| `act_read_ctl` linked service | Linked service name | `ls_databricks` | What you named it in Part 1 |
| Control table catalog.schema | Unity Catalog path | `ev_catalog.ctl.pipeline_control` | Your Databricks catalog name |
| ADLS account name (CLI commands, sink) | Storage account name | `evdatalakedev2` | Portal → Storage Accounts |

### Linked Services

| Linked Service | What to Change | How |
|---|---|---|
| `ls_voltgrid_api` | Must have NO extra parameters (no leftover test params from Day 2) | ADF Studio → Manage → Linked services → `ls_voltgrid_api` → delete any unused parameters → Save → Publish |
| `ls_adls_bronze` | Account name in connection | Match your ADLS Gen2 account |
| `ls_databricks` | Workspace URL, cluster config | Your Databricks workspace |
| `ls_keyvault` | Key Vault URL | Your Key Vault name |

### Key Vault Secrets

| Secret Name | What It Stores | Note |
|---|---|---|
| `voltgrid-username` | VoltGrid API username | Must match exactly — secret names are case-sensitive |
| `voltgrid-password` | VoltGrid API password | Same |

### Warning: `ls_voltgrid_api` Leftover Parameters

> If `ls_voltgrid_api` has any parameter with no default value (e.g. a test parameter named `abc` from Day 2 experiments), the pipeline will fail with `BadRequest` and the error message will be `null` — very hard to debug. Before you start Day 4:
> 1. ADF Studio → Manage → Linked services → `ls_voltgrid_api`
> 2. Check the Parameters tab — delete any parameter that is not needed
> 3. Save → Publish All

---

## What Changes vs What Stays from Day 2

```
ADF Artifact                  Action        Reason
────────────────────────────  ──────────    ──────────────────────────────────────────────
ls_voltgrid_api               KEEP          REST connection to the VoltGrid API — unchanged
ls_adls_bronze                KEEP          ADLS Gen2 sink connection — unchanged
ls_keyvault                   KEEP          Key Vault connection — unchanged
ls_databricks                 CREATE NEW    New — didn't exist in Day 2
ds_voltgrid_payments_src      UPDATE        Add p_endpoint, p_updated_after, p_token params;
                                            update Relative URL to include updated_after
ds_bronze_payments_sink       UPDATE        Make folder path + file name dynamic
                                            (currently hardcoded in Day 2 — needs variables)
pl_bronze_api_payments        LEAVE ALONE   Day 2 pipeline stays as-is (working reference)
pl_bronze_incremental_load    CREATE NEW    New production pipeline — control table driven
trg_payments_incremental      CREATE NEW    Tumbling Window trigger — didn't exist in Day 2
```

> Rule of thumb: linked services are infrastructure-level (rarely change). Datasets evolve as the pipeline matures. The new pipeline is a new artifact — not a modification of Day 2 — because it is a fundamentally different design.

---

## What We Are Building

```
Day 3 output:                      Day 4 adds:
──────────────────────────         ──────────────────────────────────────────────────
Control table in Delta             ls_databricks (new linked service)
nb_read_control_table              ds_voltgrid_payments_src (updated — 3 new params)
nb_update_watermark                ds_bronze_payments_sink (updated — dynamic paths)
                                   pl_bronze_incremental_load (new pipeline)
                                     ├── Read control table (Databricks notebook)
                                     ├── Get auth token (Web Activities)
                                     ├── Fetch page 1 + capture total_pages
                                     ├── Copy page 1 data to Bronze
                                     ├── Until loop (pages 2 to N)
                                     └── Update watermark (Databricks notebook)
                                   trg_payments_incremental (new Tumbling Window trigger)
```

---

## Architecture: Full Pipeline Activity Graph

See [pipeline-flow.md](pipeline-flow.md) for the full annotated diagram.

```
[START — 3 activities run in parallel]
  act_get_username  (Web → KV)
  act_get_password  (Web → KV)
  act_read_ctl      (Databricks Notebook)

[After act_get_username + act_get_password]
  act_api_login     (Web POST → login)
  act_set_token     (SetVar)

[After act_read_ctl — all 7 run in parallel]
  act_set_endpoint, act_set_sink_path, act_set_file_prefix,
  act_set_page_size, act_set_watermark, act_set_ingestion_date, act_set_run_ts

[JOIN POINT 1 — after act_set_token + act_set_endpoint + act_set_page_size + act_set_watermark]
  act_fetch_page1_meta  (Web GET — reads total_pages)
  act_set_total_pages   (SetVar)

[JOIN POINT 2 — after act_set_total_pages + all 7 SetVar activities]
  Copy data1            (Copy — page 1 → Bronze)
  act_set_current_page_2 (SetVar → v_current_page = 2)

[Until loop — pages 2..N, exit when v_current_page > v_total_pages]
  act_copy_page         (Copy — page N → Bronze)   ← MUST have additionalHeaders
  act_increment_page    (SetVar — v_next_page = N + 1)
  act_set_current_page  (SetVar — v_current_page = v_next_page)

[After Until loop]
  act_update_watermark  (Databricks — writes v_run_ts → control table)
```

---

## Part 1 — Access Setup (Three Layers)

### 1.1 Azure RBAC — Contributor on Databricks Workspace

**Via Portal:**
1. Portal → **Azure Databricks** → `dbw-ev-intelligence-dev` → **Access Control (IAM)**
2. **+ Add** → **Add role assignment**
3. Role: `Contributor` → **Next**
4. Members: **Managed identity** → select `adf-ev-intelligence-dev`
5. **Review + assign**

**Via CLI:**
```bash
ADF_MI=$(az datafactory show \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
  --query "identity.principalId" -o tsv)

DBW_ID=$(az databricks workspace show \
  --resource-group rg-ev-intelligence-dev \
  --name dbw-ev-intelligence-dev \
  --query id -o tsv)

az role assignment create \
  --assignee-object-id $ADF_MI \
  --assignee-principal-type ServicePrincipal \
  --role "Contributor" \
  --scope $DBW_ID
```

### 1.2 Databricks Workspace — Add ADF MI as Service Principal

Azure RBAC (`Contributor`) lets ADF submit job requests at the ARM level. It does NOT grant access inside the Databricks workspace. The ADF MI must also be added as a **Service Principal** inside the workspace.

**Steps:**
1. Databricks workspace → top-right avatar → **Manage** → **Identity and Access** → **Service Principals** tab
2. **+ Add Service Principal**
3. Enter the **Application ID** (Client ID) of the ADF MI — find it at:
   Portal → Azure Data Factory → `adf-ev-intelligence-dev` → **Properties** → **Managed Identity Application ID**
4. Role: **User**
5. Click **Add**

> **Application ID vs Object ID:** These are two different GUIDs for the same service principal. The Databricks Service Principals tab requires the **Application ID**. The Object ID (used for Azure RBAC role assignments) will not resolve correctly here.

> Missing this step causes `User not authorized` when the pipeline tries to run a notebook, even if Azure RBAC is set correctly.

### 1.3 Unity Catalog — Grant ADF MI Access to Control Table

After adding the ADF MI as a workspace user, it still cannot read or write the Delta control table unless Unity Catalog privileges are granted. Run these three SQL commands in a Databricks notebook or SQL editor:

```sql
-- Allow the ADF MI to see the catalog
GRANT USAGE ON CATALOG ev_catalog TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Allow the ADF MI to see the schema
GRANT USAGE ON SCHEMA ev_catalog.ctl TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Allow the ADF MI to read and write the control table
GRANT SELECT, MODIFY ON TABLE ev_catalog.ctl.pipeline_control TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
```

> **What is `9245da2f-7129-4e0c-92c4-134fe896e294`?** This is the **Application ID** (also called Client ID) of the ADF Managed Identity service principal. It is different from the Object ID used in step 1.2. You can find it in: Portal → Azure Data Factory → `adf-ev-intelligence-dev` → **Properties** → **Managed Identity Application ID** (or search for the ADF name in Azure AD → Enterprise Applications).

> **Students:** Replace this GUID with your own ADF MI Application ID. The format is the same — a UUID.

> **Why three separate grants?** Unity Catalog uses a layered permission model. `USAGE` on the catalog allows the principal to see it. `USAGE` on the schema allows seeing the schema. `SELECT, MODIFY` on the table allows reading rows (`nb_read_control_table`) and updating the watermark (`nb_update_watermark`). All three are required — granting only table-level access without catalog/schema USAGE will fail with a permissions error.

### 1.4 Create Linked Service `ls_databricks`

**ADF Studio → Manage → Linked services → + New** → search `Azure Databricks` → select **Azure Databricks** → Continue.

---

#### Step 1 — Name and Workspace

| Field | Value | Note |
|---|---|---|
| Name | `ls_databricks` | |
| Azure subscription | select yours | dropdown populates automatically |
| Databricks workspace | `dbw-ev-intelligence-dev` | dropdown — workspace must exist and be in the same subscription |

---

#### Step 2 — Authentication

| Field | Value | Note |
|---|---|---|
| Authentication type | **System Assigned Managed Identity** | Uses ADF's own identity — no tokens to rotate, no passwords to store |

> **Why Managed Identity and not Access Token?**
> An access token expires (typically 90 days). When it expires, the linked service silently breaks — the next pipeline run fails with `401 Unauthorized`. Managed Identity is credential-free and never expires. It requires Layers 1 and 2 from Parts 1.1 and 1.2 above to be complete first.

> **Why not user-assigned MI?** System-assigned MI is tied to the ADF resource lifecycle — if ADF is deleted, the identity is deleted too. For a project-scoped Databricks connection this is fine and simpler to manage.

---

#### Step 3 — Select Cluster Type

Click **Select cluster** dropdown → choose **New job cluster**.

> **Do NOT select "Existing interactive cluster"** — reasons:
> - An interactive cluster must be running at all times, billing even when idle
> - If someone restarts or resizes it, the cluster ID changes and the linked service breaks
> - Interactive clusters are shared — a runaway job from another user can affect your pipeline
>
> **New job cluster** means: ADF provisions a brand-new Databricks cluster each time a notebook activity runs, the notebook runs, then the cluster is automatically terminated. You pay only for the duration of the job. For a daily batch pipeline this is the correct choice.

---

#### Step 4 — Cluster Configuration

After selecting **New job cluster**, two more fields appear:

| Field | Value | Note |
|---|---|---|
| Job cluster version | `15.4 LTS` | Pick from dropdown — choose the LTS (Long Term Support) version. LTS means Databricks guarantees no breaking runtime changes for the life of the release. Avoid non-LTS versions for production. |
| Cluster node type | `Standard_DS3_v2` | Pick from dropdown — 4 vCPU, 14 GB RAM. Sufficient for reading control table rows and writing Bronze JSON pages. You do not need a GPU or memory-optimised SKU for Bronze ingestion. |

> **Why `Standard_DS3_v2`?**
> Bronze ingestion is I/O-bound (read from API, write JSON to ADLS), not compute-bound. A single `Standard_DS3_v2` node handles this with headroom. You can scale up to `Standard_DS4_v2` (8 vCPU, 28 GB) if you see memory pressure on large sources, but start here.

> **Single node vs multi-node:** ADF creates a single-node job cluster by default (driver = worker). For notebook activities that run SQL queries against the control table and write small JSON files, single-node is sufficient. Multi-node clusters are needed when Spark shuffle operations distribute data across workers — not applicable here.

> **What you will NOT see in the dropdown if Layer 1 is missing:** If the ADF MI does not have `Contributor` on the Databricks workspace, the cluster version and node type dropdowns will be **empty**. ADF calls the Databricks API to populate these lists — without ARM-level access it cannot make that call. Fix: complete Part 1.1 before creating this linked service.

---

#### Step 5 — Test and Save

1. Click **Test connection** — must show **Connection successful** before saving
2. Click **Create**
3. Click **Publish All** (top bar)

**If test connection fails:**

| Error shown | Cause | Fix |
|---|---|---|
| `AuthorizationFailed` | Layer 1 missing — ADF MI not a Contributor on the workspace | Complete Part 1.1 |
| `User not authorized` | Layer 2 missing — ADF MI not added as Service Principal inside workspace | Complete Part 1.2 |
| Cluster version dropdown empty | Layer 1 missing — ADF cannot call Databricks API to list runtimes | Complete Part 1.1 first, then reopen the linked service |
| `MSI not enabled` | ADF instance does not have System Assigned Managed Identity enabled | Portal → ADF → Identity → System assigned → Status = On → Save |

---

## Part 2 — Update Datasets

### 2.1 Update Source Dataset `ds_voltgrid_payments_src`

**ADF Studio → Author → Datasets → `ds_voltgrid_payments_src`:**

1. **Parameters** tab → add these (keep existing `p_page` and `p_page_size`):
   - `p_endpoint` — String — default: `/api/db/payments/`
   - `p_updated_after` — String — default: `1900-01-01T00:00:00Z`
   - `p_token` — String — default: `` (empty)

2. **Connection** tab → Relative URL → **Edit dynamic content**:
   ```
   @concat(
     dataset().p_endpoint,
     '?page=',
     string(dataset().p_page),
     '&page_size=',
     string(dataset().p_page_size),
     '&updated_after=',
     encodeUriComponent(dataset().p_updated_after)
   )
   ```

**Full parameters after update:**

| Parameter | Type | Default |
|---|---|---|
| `p_endpoint` | String | `/api/db/payments/` |
| `p_page` | String | `1` |
| `p_page_size` | String | `1000` |
| `p_updated_after` | String | `1900-01-01T00:00:00Z` |
| `p_token` | String | `` (empty) |

> The `p_token` parameter exists in the dataset but **auth is NOT done at dataset level** — see Part 3 "Authorization" for why. The actual authorization header is added in each Copy Activity's source block.

### 2.2 Update Sink Dataset `ds_bronze_payments_sink`

**ADF Studio → Author → Datasets → `ds_bronze_payments_sink`:**

1. **Parameters** tab:
   - `p_folder` — String — default: `api/payments/ingestion_date=2026-10-16`
   - `p_file` — String — default: `payments_page_1.json`

2. **Connection** tab:
   - File system: `bronze`
   - Directory: `@dataset().p_folder`
   - File: `@dataset().p_file`

3. Linked service: `ls_adls_bronze`

4. **Publish all**

---

## Part 3 — Create Pipeline: pl_bronze_incremental_load

### 3.1 Pipeline Parameters

| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `p_source_name` | String | `payments` | Which control table row to read |
| `p_load_type_override` | String | `` (empty string — NOT null) | Force `full` for backfill; empty = use control table value |

> **Important:** `p_load_type_override` must have an empty string `""` as default, NOT left blank (null). The `act_set_watermark` expression calls `empty()` on it — `empty(null)` throws an error. `empty("")` returns true safely.

### 3.2 Pipeline Variables

| Variable | Type | Purpose |
|---|---|---|
| `v_token` | String | VoltGrid API auth token |
| `v_endpoint` | String | API endpoint from control table |
| `v_sink_base_path` | String | Bronze base path from control table |
| `v_file_prefix` | String | File name prefix from control table |
| `v_page_size` | String | Page size from control table |
| `v_last_watermark` | String | `updated_after` value for this run |
| `v_ingestion_date` | String | `YYYY-MM-DD` for sink partition folder |
| `v_current_page` | Integer | Loop counter (starts at 2) |
| `v_next_page` | Integer | Temp variable for increment (see below) |
| `v_total_pages` | Integer | Max pages from API page 1 response |
| `v_run_ts` | String | UTC timestamp captured at pipeline start — becomes new watermark |

> **Why `v_next_page` exists:** ADF does NOT allow a SetVariable activity to read and write the same variable in one expression. `v_current_page = @add(variables('v_current_page'), 1)` throws `self referencing variable` error. Solution: write `v_next_page = current + 1` first, then `v_current_page = v_next_page` in a separate activity.

### 3.3 Authorization in Copy Activities — Critical

**The challenge:** ADF Copy Activity needs to pass `Authorization: Token <token>` to the REST API. There are three places where you might try to put this — only one works.

| Approach | Does it work? | Why |
|---|---|---|
| `additionalHeaders` in **dataset** `typeProperties` | No | ADF REST dataset additionalHeaders only accepts static strings — expressions are not evaluated |
| `Auth headers` in **linked service** | No | Requires configuring auth directly in the linked service, which doesn't support dynamic token expressions from variables |
| `additionalHeaders` in **Copy Activity source block** | **YES** | Copy Activity source evaluates ADF expressions in additionalHeaders — this is the working solution |

**Working source block — use this exact format for BOTH `Copy data1` AND `act_copy_page`:**

```json
"source": {
    "type": "RestSource",
    "httpRequestTimeout": "00:01:40",
    "requestInterval": "00.00:00:00.010",
    "requestMethod": "GET",
    "additionalHeaders": {
        "Authorization": {
            "value": "@concat('Token ', variables('v_token'))",
            "type": "Expression"
        }
    }
}
```

> In ADF Studio UI: Copy Activity → Source tab → scroll to **Additional headers** → add `Authorization` → toggle to **Dynamic content** → `@concat('Token ', variables('v_token'))`

> **Common mistake:** Only adding the header to `Copy data1` (page 1) and forgetting to add it to `act_copy_page` (inside the Until loop). Page 1 will succeed, page 2+ will get 401 Unauthorized.

---

### 3.4 Activities — Build Order and Dependency Chain

#### `act_read_ctl` — Databricks Notebook

- Type: `DatabricksNotebook`
- Linked service: `ls_databricks`
- Notebook path: `/Users/your-email@gmail.com/nb_read_control_table`
- Base parameters: `source_name` = `@pipeline().parameters.p_source_name`
- Depends on: nothing (pipeline start)

> Output: `activity('act_read_ctl').output.runOutput` contains `endpoint`, `sink_base_path`, `file_prefix`, `page_size`, `load_type`, `last_watermark`

---

#### `act_set_endpoint` — Set Variable
- Variable: `v_endpoint`
- Value: `@activity('act_read_ctl').output.runOutput.endpoint`
- Depends on: `act_read_ctl` (Succeeded)

#### `act_set_sink_path` — Set Variable
- Variable: `v_sink_base_path`
- Value: `@activity('act_read_ctl').output.runOutput.sink_base_path`
- Depends on: `act_read_ctl` (Succeeded)

#### `act_set_file_prefix` — Set Variable
- Variable: `v_file_prefix`
- Value: `@activity('act_read_ctl').output.runOutput.file_prefix`
- Depends on: `act_read_ctl` (Succeeded)

#### `act_set_page_size` — Set Variable
- Variable: `v_page_size`
- Value: `@string(activity('act_read_ctl').output.runOutput.page_size)`
- Depends on: `act_read_ctl` (Succeeded)

#### `act_set_watermark` — Set Variable
- Variable: `v_last_watermark`
- Value:
  ```
  @if(
    and(
      not(empty(pipeline().parameters.p_load_type_override)),
      equals(pipeline().parameters.p_load_type_override, 'full')
    ),
    '1900-01-01T00:00:00Z',
    activity('act_read_ctl').output.runOutput.last_watermark
  )
  ```
- Depends on: `act_read_ctl` (Succeeded)

#### `act_set_ingestion_date` — Set Variable
- Variable: `v_ingestion_date`
- Value: `@formatDateTime(utcnow(), 'yyyy-MM-dd')`
- Depends on: `act_read_ctl` (Succeeded)

#### `act_set_run_ts` — Set Variable
- Variable: `v_run_ts`
- Value: `@utcnow()`
- Depends on: `act_read_ctl` (Succeeded)

> Captured at pipeline **start** — not end. Records updated during the run are still caught in the next incremental fetch (safe overlap vs gap).

---

#### `act_get_username` — Web Activity (Key Vault)
- Method: `GET`
- URL: `https://kv-ev-intelligence-dev02.vault.azure.net/secrets/voltgrid-username/?api-version=7.0`
- Authentication: MSI, resource: `https://vault.azure.net`
- Depends on: nothing (parallel with `act_read_ctl`)

#### `act_get_password` — Web Activity (Key Vault)
- Method: `GET`
- URL: `https://kv-ev-intelligence-dev02.vault.azure.net/secrets/voltgrid-password/?api-version=7.0`
- Authentication: MSI, resource: `https://vault.azure.net`
- Depends on: nothing (parallel with `act_get_username` and `act_read_ctl`)

#### `act_api_login` — Web Activity (POST login)
- Method: `POST`
- URL: `https://ev-project-navy-mu.vercel.app/api/auth/login/`
- Headers: `Content-Type: application/json`
- Body: `@concat('{"username":"', activity('act_get_username').output.value, '","password":"', activity('act_get_password').output.value, '"}')`
- Depends on: `act_get_username` (Succeeded), `act_get_password` (Succeeded)

#### `act_set_token` — Set Variable
- Variable: `v_token`
- Value: `@activity('act_api_login').output.token`
- Depends on: `act_api_login` (Succeeded)

---

#### `act_fetch_page1_meta` — Web Activity (GET page 1)

This is **JOIN POINT 1** — waits for auth branch AND config branch.

- Method: `GET`
- Headers: `authorization` = `@concat('Token ', variables('v_token'))`
- URL:
  ```
  @concat(
    'https://ev-project-navy-mu.vercel.app',
    variables('v_endpoint'),
    '?page=1&page_size=',
    variables('v_page_size'),
    '&updated_after=',
    encodeUriComponent(variables('v_last_watermark'))
  )
  ```
- Depends on: `act_set_token`, `act_set_endpoint`, `act_set_page_size`, `act_set_watermark` (all Succeeded)

> Uses Web Activity (not Copy) because only Web Activity exposes the full response body. ADF Copy Activity does not surface `pagination.total_pages` — it only reports rows/bytes copied.

#### `act_set_total_pages` — Set Variable
- Variable: `v_total_pages`
- Value: `@int(activity('act_fetch_page1_meta').output.pagination.total_pages)`
- Depends on: `act_fetch_page1_meta` (Succeeded)

---

#### `Copy data1` — Copy Activity (page 1)

This is **JOIN POINT 2** — waits for ALL variables to be set.

**Depends on (9 total):**
`act_set_total_pages`, `act_set_token`, `act_set_endpoint`, `act_set_sink_path`, `act_set_file_prefix`, `act_set_page_size`, `act_set_watermark`, `act_set_ingestion_date`, `act_set_run_ts`

**Source — `ds_voltgrid_payments_src`:**

| Parameter | Value |
|---|---|
| `p_page` | `"1"` (string literal) |
| `p_page_size` | `@variables('v_page_size')` |
| `p_endpoint` | `@variables('v_endpoint')` |
| `p_updated_after` | `@variables('v_last_watermark')` |
| `p_token` | `@variables('v_token')` |

**Source block additionalHeaders (required for auth):**
```json
"additionalHeaders": {
    "Authorization": {
        "value": "@concat('Token ', variables('v_token'))",
        "type": "Expression"
    }
}
```

**Sink — `ds_bronze_payments_sink`:**

| Parameter | Value |
|---|---|
| `p_folder` | `@concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'))` |
| `p_file` | `@concat(variables('v_file_prefix'), '_page_1.json')` |

Result: `bronze/api/payments/ingestion_date=2026-10-16/payments_page_1.json`

---

#### `act_set_current_page_2` — Set Variable

Between `Copy data1` and Until loop. Initialises the loop counter.

- Variable: `v_current_page`
- Value: `2`
- Depends on: `Copy data1` (Succeeded)

---

#### Until Loop

**Depends on:** `act_set_current_page_2` (Succeeded)

**Exit condition:**
```
@greaterOrEquals(variables('v_current_page'), add(variables('v_total_pages'), 1))
```

**Inside the Until body** (click Until → pencil icon to edit the inner canvas):

**`act_copy_page`** — Copy Activity
- Depends on: nothing inside loop (first to run each iteration)

Source — `ds_voltgrid_payments_src`:

| Parameter | Value |
|---|---|
| `p_page` | `@string(variables('v_current_page'))` |
| `p_page_size` | `@variables('v_page_size')` |
| `p_endpoint` | `@variables('v_endpoint')` |
| `p_updated_after` | `@variables('v_last_watermark')` |
| `p_token` | `@variables('v_token')` |

Source block additionalHeaders (**same as `Copy data1` — required**):
```json
"additionalHeaders": {
    "Authorization": {
        "value": "@concat('Token ', variables('v_token'))",
        "type": "Expression"
    }
}
```

Sink — `ds_bronze_payments_sink`:

| Parameter | Value |
|---|---|
| `p_folder` | `@concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'))` |
| `p_file` | `@concat(variables('v_file_prefix'), '_page_', string(variables('v_current_page')), '.json')` |

**`act_increment_page`** — Set Variable
- Variable: `v_next_page`
- Value: `@add(variables('v_current_page'), 1)`
- Depends on: `act_copy_page` (Succeeded)

**`act_set_current_page`** — Set Variable
- Variable: `v_current_page`
- Value: `@variables('v_next_page')`
- Depends on: `act_increment_page` (Succeeded)

> All three must be inside the Until body. If any are on the main canvas, `v_current_page` never advances and the loop fetches page 2 forever until it times out.

---

#### `act_update_watermark` — Databricks Notebook

- Linked service: `ls_databricks`
- Notebook path: `/Users/your-email@gmail.com/nb_update_watermark`
- Base parameters:
  - `source_name` = `@pipeline().parameters.p_source_name`
  - `new_watermark` = `@variables('v_run_ts')`
- Depends on: Until loop (Succeeded)

---

### 3.5 Complete Activity Dependency Summary

```
Pipeline start (parallel):
  act_read_ctl         ── no dependency
  act_get_username     ── no dependency
  act_get_password     ── no dependency

After act_read_ctl (all 7 run in parallel):
  act_set_endpoint, act_set_sink_path, act_set_file_prefix,
  act_set_page_size, act_set_watermark, act_set_ingestion_date, act_set_run_ts

After act_get_username + act_get_password:
  act_api_login        ── depends on: act_get_username, act_get_password
  act_set_token        ── depends on: act_api_login

JOIN POINT 1:
  act_fetch_page1_meta ── depends on: act_set_token, act_set_endpoint,
                                       act_set_page_size, act_set_watermark
  act_set_total_pages  ── depends on: act_fetch_page1_meta

JOIN POINT 2:
  Copy data1           ── depends on: act_set_total_pages, act_set_token,
                                       act_set_endpoint, act_set_sink_path,
                                       act_set_file_prefix, act_set_page_size,
                                       act_set_watermark, act_set_ingestion_date,
                                       act_set_run_ts

  act_set_current_page_2 ── depends on: Copy data1

  Until loop             ── depends on: act_set_current_page_2
    act_copy_page          ── no dependency (first in loop body)
    act_increment_page     ── depends on: act_copy_page
    act_set_current_page   ── depends on: act_increment_page

  act_update_watermark   ── depends on: Until (Succeeded)
```

---

## Part 4 — Sink Path Convention

For a payments run on 2026-10-16 with 321 total pages:

```
bronze/
└── api/
    └── payments/
        └── ingestion_date=2026-10-16/
            ├── payments_page_1.json    ← Copy data1 (page 1)
            ├── payments_page_2.json    ← Until loop iteration 1
            │   ...
            └── payments_page_321.json  ← Until loop final iteration
```

| Expression | Example value |
|---|---|
| `variables('v_sink_base_path')` | `api/payments` |
| `variables('v_ingestion_date')` | `2026-10-16` |
| `variables('v_file_prefix')` | `payments` |
| `variables('v_current_page')` | `42` |
| Full folder | `api/payments/ingestion_date=2026-10-16` |
| Full file (loop page 42) | `payments_page_42.json` |

---

## Part 5 — Trigger Setup

**ADF Studio → Manage → Triggers → + New:**

| Field | Value |
|---|---|
| Type | `Tumbling window` |
| Name | `trg_payments_incremental` |
| Start time | `2026-10-14T00:00:00Z` |
| Recurrence | `1 Day` |
| Max concurrency | `1` |
| Retry | `2` times, `30` minutes interval |

Pipeline parameters:
- `p_source_name`: `payments`
- `p_load_type_override`: `` (empty)

**Publish all** → **Activate trigger**

---

## Part 6 — Adding a New Source (Zero Pipeline Changes)

```sql
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('sessions', '/api/db/sessions/', 'api/sessions', 'sessions', 1000, 'full', '1900-01-01T00:00:00Z', true);
```

Trigger with `p_source_name = 'sessions'` — no ADF pipeline changes required.

---

## Verify: Day 4 Checklist

**Access setup:**
- [ ] ADF MI has `Contributor` on `dbw-ev-intelligence-dev` (Azure RBAC)
- [ ] ADF MI added as **User** inside Databricks workspace (workspace-level)
- [ ] Unity Catalog grants run: `USAGE` on catalog, `USAGE` on schema, `SELECT, MODIFY` on table
- [ ] `ls_databricks` test connection green
- [ ] `ls_voltgrid_api` has NO extra parameters with null values (delete leftover test params)

**Datasets:**
- [ ] `ds_voltgrid_payments_src` has 5 parameters: `p_endpoint`, `p_page`, `p_page_size`, `p_updated_after`, `p_token`
- [ ] `ds_bronze_payments_sink` has 2 parameters: `p_folder`, `p_file`

**Pipeline:**
- [ ] `p_load_type_override` parameter has `defaultValue: ""` (empty string, not null)
- [ ] All 7 Set Variable activities depend on `act_read_ctl`
- [ ] `act_api_login` depends on both `act_get_username` AND `act_get_password`
- [ ] `act_fetch_page1_meta` depends on `act_set_token` + `act_set_endpoint` + `act_set_page_size` + `act_set_watermark`
- [ ] `Copy data1` has 9 dependencies listed in section 3.4
- [ ] `Copy data1` source has `additionalHeaders: Authorization: Token @concat(...)`
- [ ] `Copy data1` sink `p_file` = `@concat(variables('v_file_prefix'), '_page_1.json')`
- [ ] `act_set_current_page_2` sets `v_current_page = 2`, depends on `Copy data1`
- [ ] Until loop depends on `act_set_current_page_2`
- [ ] `act_copy_page`, `act_increment_page`, `act_set_current_page` are all **inside** the Until body
- [ ] `act_copy_page` source has same `additionalHeaders: Authorization` as `Copy data1`
- [ ] `act_copy_page` `p_page` parameter = `@string(variables('v_current_page'))` (String-typed param needs string cast)
- [ ] `act_increment_page` writes `v_next_page = @add(variables('v_current_page'), 1)`
- [ ] `act_set_current_page` writes `v_current_page = @variables('v_next_page')`
- [ ] `act_update_watermark` depends on Until loop success

**Post-run verification:**
```bash
az storage blob list \
  --account-name evdatalakedev2 \
  --container-name bronze \
  --prefix "api/payments/ingestion_date=" \
  --auth-mode login \
  --query "[].{name:name, size:properties.contentLength}" -o table
```
- [ ] Files `payments_page_1.json` through `payments_page_NNN.json` all present
- [ ] Control table: `load_type = 'incremental'`, `last_watermark` updated to pipeline start UTC

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `BadRequest` with `null` message | `ls_voltgrid_api` has a parameter with no value (e.g. leftover `abc` param from experiments) | ADF Studio → Manage → Linked services → `ls_voltgrid_api` → delete unused parameters → Save → Publish. Check Monitor → activity detail (glasses icon) for the real message |
| `401 Unauthorized` on `Copy data1` | Missing `additionalHeaders` in Copy Activity source OR `act_set_token` not in `Copy data1`'s dependsOn | Add `additionalHeaders` block to source (see Part 3.3). Add `act_set_token` to dependsOn |
| `401 Unauthorized` on `act_copy_page` (page 2+) | `additionalHeaders` was added to `Copy data1` but not to `act_copy_page` inside the Until loop | Add identical `additionalHeaders` block to `act_copy_page` source — both Copy activities must have it |
| `User not authorized` on notebook run | ADF MI not added as workspace user in Databricks | Admin Console → Users → Add the ADF MI as User |
| `self referencing variable` error | SetVariable reads and writes the same variable (`v_current_page = current + 1`) | Use two activities: write to `v_next_page` first, then copy to `v_current_page` |
| `empty(null)` throws in `act_set_watermark` | `p_load_type_override` has no defaultValue (null when not passed) | Set `defaultValue: ""` on the parameter |
| `total_pages` = 0 | `updated_after` filter is too recent | Trigger with `p_load_type_override = full` |
| Until loop runs forever | `act_copy_page` / `act_increment_page` / `act_set_current_page` placed on main canvas | Click Until → pencil icon → confirm all three are on the inner canvas |
| `act_update_watermark` widget not found | Parameter names don't match notebook widget names | Notebook expects exactly `source_name` and `new_watermark` |
| Pipeline-level error shows `message: null` | ADF always shows null at pipeline level | Click Monitor → activity row → glasses icon → read the activity-level error detail |

---

## Quick Reference

```
Pipeline      : pl_bronze_incremental_load
Trigger       : trg_payments_incremental (Tumbling Window, daily)

Sink pattern  : bronze/<sink_base_path>/ingestion_date=YYYY-MM-DD/<file_prefix>_page_N.json
Example       : bronze/api/payments/ingestion_date=2026-10-16/payments_page_1.json

Control table : ev_catalog.ctl.pipeline_control

Access chain (ADF):
  adf-ev-intelligence-dev MI  →  Key Vault Secrets User        →  kv-ev-intelligence-dev02
  adf-ev-intelligence-dev MI  →  Storage Blob Data Contributor →  evdatalakedev2
  adf-ev-intelligence-dev MI  →  Contributor (ARM)             →  dbw-ev-intelligence-dev
  adf-ev-intelligence-dev MI  →  User (workspace)              →  dbw-ev-intelligence-dev

Notebook paths (must match exactly — case-sensitive):
  /Users/your-email@gmail.com/nb_read_control_table
  /Users/your-email@gmail.com/nb_update_watermark

Auth flow:
  KV → username + password → POST /api/auth/login/ → token
  token stored in v_token
  Both Copy activities pass it via: additionalHeaders.Authorization = "Token <v_token>"
  Web Activity (act_fetch_page1_meta) passes it via: headers.authorization

API pagination:
  page_size     = 1000 (from control table)
  total_pages   = response.pagination.total_pages (from act_fetch_page1_meta)
  updated_after = v_last_watermark (epoch on full load, previous watermark on incremental)

Watermark update: v_run_ts (utcnow() at pipeline start) written after all pages succeed
```
