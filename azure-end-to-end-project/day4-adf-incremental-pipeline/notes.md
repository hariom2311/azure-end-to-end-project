# Day 4 — ADF Incremental Pipeline: Pagination + Watermark

**Duration:** 3 hours | **Goal:** Wire ADF to the Day 3 control table and Databricks notebooks, build `pl_bronze_incremental_load` with full pagination via Until loop, and schedule it with a Tumbling Window trigger.

> **Prerequisites:** Day 3 complete — `ev_catalog.ctl.pipeline_control` exists, payments row inserted with `load_type = 'full'` and `last_watermark = '1900-01-01T00:00:00Z'`, both notebooks created and tested.

---

## What Changes vs What Stays from Day 2

```
ADF Artifact                  Action        Reason
────────────────────────────  ──────────    ──────────────────────────────────────────────
ls_rest_voltgrid              KEEP          Still the REST connection to the API — unchanged
ls_adls                       KEEP          Still the ADLS sink connection — unchanged
ls_keyvault                   KEEP          Still the Key Vault connection — unchanged
ls_databricks                 CREATE NEW    New — didn't exist in Day 2
ds_voltgrid_payments_src      UPDATE        Add p_endpoint + p_updated_after params;
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
nb_read_control_table              ds_voltgrid_payments_src (updated — 2 new params)
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

```
[START — no dependencies]
  act_get_username  (Web → KV)     ─────────────────────────────────────┐
  act_get_password  (Web → KV)     ────────────────────────────────┐    │
  act_read_ctl      (Databricks)   ──┐                             │    │
                                     │                             ▼    ▼
[depends on act_read_ctl]            │                        act_api_login (Web POST)
  act_set_endpoint    (SetVar)  ◄────┤                             │
  act_set_sink_path   (SetVar)  ◄────┤                        act_set_token (SetVar)
  act_set_file_prefix (SetVar)  ◄────┤                             │
  act_set_page_size   (SetVar)  ◄────┤                        act_fetch_page1_meta (Web GET)
  act_set_watermark   (SetVar)  ◄────┤                             │
  act_set_ingestion_date (SetVar) ◄──┤                        act_set_total_pages (SetVar)
  act_set_run_ts      (SetVar)  ◄────┘                             │
                                                                    │
[Copy data1 — depends on ALL set variables + act_set_total_pages]  │
  Copy data1 (act_copy_page1) ◄──────────────────────────────────────
     depends on: act_set_total_pages, act_set_endpoint, act_set_sink_path,
                 act_set_file_prefix, act_set_page_size, act_set_watermark,
                 act_set_ingestion_date, act_set_run_ts

[Until loop — depends on Copy data1]
  Until (act_set_current_page_2 = 2, loop pages 2..N)
    ├── act_copy_page   (Copy — no dependency inside loop, first to run)
    └── act_increment_page (SetVar — depends on act_copy_page)

[act_update_watermark — depends on Until success]
  act_update_watermark (Databricks)
```

---

## Part 1 — Access Setup (Three Layers)

This is the part most commonly missed. ADF's MI needs three separate permissions — one Azure RBAC role and one Databricks workspace-level user grant.

### 1.1 Azure RBAC — Contributor on Databricks Workspace (ARM level)

Required so ADF can call the Databricks Jobs API to submit and monitor notebook runs.

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

### 1.2 Databricks Workspace User — Add ADF MI as User (workspace level)

This is the step that causes the permission error when running notebooks via ADF. Azure RBAC (`Contributor`) gives ADF access to the ARM plane — it can create job runs. But the Databricks workspace itself has its own permission system. The ADF MI must be added as a user **inside** the Databricks workspace, or it cannot access notebooks, catalogs, or clusters.

**Steps:**
1. In Databricks workspace → top-right avatar → **Manage** → **Identity and Access** → **Users** tab  
   (or go to `https://accounts.azuredatabricks.net` → **User Management**)
2. **+ Add User**
3. Enter the **Object ID** (Principal ID) of the ADF MI — this is the value of `$ADF_MI` from the CLI command above
   - Alternatively, search for the ADF factory name in the identities list
4. Assign workspace role: **User** (sufficient — ADF only needs to run notebooks, not admin the workspace)
5. Click **Add**

**Or via Databricks workspace → Admin Settings → Users:**
1. **Settings** (gear icon) → **Admin Console** → **Users** tab
2. **+ Add User** → enter the service principal's Application ID or Object ID
3. Role: `User`

> **Why this is needed:** Azure RBAC controls the ARM/resource plane (can ADF submit a job request?). Databricks workspace access controls the data plane (can the principal access notebooks, Unity Catalog, clusters?). Both are required. Missing the workspace user is the most common cause of `com.databricks.common.client.DatabricksServiceException: User not authorized`.

### 1.3 Create Linked Service `ls_databricks`

**ADF Studio → Manage → Linked services → + New → Azure Databricks (Compute category):**

| Field | Value |
|---|---|
| Name | `ls_databricks` |
| Azure subscription | yours |
| Databricks workspace | `dbw-ev-intelligence-dev` |
| Select cluster | **New job cluster** |
| Job cluster version | `15.4 LTS (includes Apache Spark 3.5.0, Scala 2.12)` |
| Node type | `Standard_DS3_v2` |
| Authentication type | `System Assigned Managed Identity` |

**Test connection** → green → **Create**

> **New job cluster:** ADF spins up a fresh cluster per pipeline run and terminates it when done. No idle cluster to forget to stop — cost only during execution.

---

## Part 2 — Update Datasets

### 2.1 Update Source Dataset `ds_voltgrid_payments_src`

**ADF Studio → Author → Datasets → `ds_voltgrid_payments_src`:**

1. **Parameters** tab → add:
   - `p_endpoint` — String — default: `/api/db/payments/`
   - `p_updated_after` — String — default: `1900-01-01T00:00:00Z`
   (Keep existing `p_page` and `p_page_size`)

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
| `p_page` | Int | `1` |
| `p_page_size` | Int | `1000` |
| `p_updated_after` | String | `1900-01-01T00:00:00Z` |

### 2.2 Update Sink Dataset `ds_bronze_payments_sink`

The Day 2 sink had folder and file name hardcoded. Day 4 needs both to be dynamic.

**ADF Studio → Author → Datasets → `ds_bronze_payments_sink`:**

1. **Parameters** tab → add:
   - `p_folder` — String — default: `api/payments/ingestion_date=2026-10-14`
   - `p_file` — String — default: `payments_page_001.json`

2. **Connection** tab:
   - Container: `bronze`
   - File path (Directory): `@dataset().p_folder`
   - File path (File): `@dataset().p_file`

3. **Publish all**

---

## Part 3 — Create Pipeline: pl_bronze_incremental_load

### 3.1 Pipeline Parameters

| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `p_source_name` | String | `payments` | Which control table row to read |
| `p_load_type_override` | String | `` (empty) | Force `full` for backfill; empty = use control table value |

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
| `v_next_page` | Integer | Temp variable for increment — ADF cannot self-reference a variable, so `v_current_page + 1` is written here first then copied back |
| `v_total_pages` | Integer | Max pages from API page 1 response |
| `v_run_ts` | String | ISO 8601 UTC captured at pipeline start — written back as new watermark |

### 3.3 Activities — Build Order and Dependency Chain

Each activity below lists exactly what it depends on and what depends on it.

---

#### `act_read_ctl` — Databricks Notebook

**Depends on:** nothing (pipeline start)  
**Downstream of:** nothing  
**Required by:** all 7 Set Variable activities below

- Type: `DatabricksNotebook`
- Linked service: `ls_databricks`
- Notebook path: `/Users/hariomsuryawanshi68258@gmail.com/nb_read_control_table`
  *(use your actual Databricks notebook path — check in Databricks Workspace)*
- Base parameters:
  - `source_name` = `@pipeline().parameters.p_source_name`

> Exits with JSON string: `{"endpoint": "...", "sink_base_path": "...", "file_prefix": "...", "page_size": 1000, "load_type": "...", "last_watermark": "..."}`

---

#### `act_set_endpoint` — Set Variable

**Depends on:** `act_read_ctl` (Succeeded)  
**Required by:** `Copy data1`

- Variable: `v_endpoint`
- Value: `@json(activity('act_read_ctl').output.runOutput).endpoint`

---

#### `act_set_sink_path` — Set Variable

**Depends on:** `act_read_ctl` (Succeeded)  
**Required by:** `Copy data1`

- Variable: `v_sink_base_path`
- Value: `@json(activity('act_read_ctl').output.runOutput).sink_base_path`

---

#### `act_set_file_prefix` — Set Variable

**Depends on:** `act_read_ctl` (Succeeded)  
**Required by:** `Copy data1`

- Variable: `v_file_prefix`
- Value: `@json(activity('act_read_ctl').output.runOutput).file_prefix`

---

#### `act_set_page_size` — Set Variable

**Depends on:** `act_read_ctl` (Succeeded)  
**Required by:** `Copy data1`

- Variable: `v_page_size`
- Value: `@string(json(activity('act_read_ctl').output.runOutput).page_size)`

---

#### `act_set_watermark` — Set Variable

**Depends on:** `act_read_ctl` (Succeeded)  
**Required by:** `Copy data1`

- Variable: `v_last_watermark`
- Value:
  ```
  @if(
    and(
      not(empty(pipeline().parameters.p_load_type_override)),
      equals(pipeline().parameters.p_load_type_override, 'full')
    ),
    '1900-01-01T00:00:00Z',
    json(activity('act_read_ctl').output.runOutput).last_watermark
  )
  ```

---

#### `act_set_ingestion_date` — Set Variable

**Depends on:** `act_read_ctl` (Succeeded)  
**Required by:** `Copy data1`

- Variable: `v_ingestion_date`
- Value: `@formatDateTime(utcnow(), 'yyyy-MM-dd')`

---

#### `act_set_run_ts` — Set Variable

**Depends on:** `act_read_ctl` (Succeeded)  
**Required by:** `Copy data1`, eventually `act_update_watermark` (via `v_run_ts`)

- Variable: `v_run_ts`
- Value: `@utcnow()`

> Captured at pipeline **start** — not end. This ensures records updated during the run window are captured in the next incremental fetch (safe overlap vs. gap).

---

#### `act_get_username` — Web Activity (Key Vault)

**Depends on:** nothing (pipeline start, runs in parallel with `act_read_ctl` branch)  
**Required by:** `act_api_login`

- Method: `GET`
- URL: `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-username/?api-version=7.0`
- Authentication: MSI, resource: `https://vault.azure.net`

---

#### `act_get_password` — Web Activity (Key Vault)

**Depends on:** nothing (pipeline start, runs in parallel with `act_get_username` and `act_read_ctl`)  
**Required by:** `act_api_login`

- Method: `GET`
- URL: `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-password/?api-version=7.0`
- Authentication: MSI, resource: `https://vault.azure.net`

---

#### `act_api_login` — Web Activity (POST login)

**Depends on:** `act_get_username` (Succeeded), `act_get_password` (Succeeded)  
**Required by:** `act_set_token`

- Method: `POST`
- URL: `https://ev-project-navy-mu.vercel.app/api/auth/login/`
- Headers: `Content-Type: application/json`
- Body: `@concat('{"username":"', activity('act_get_username').output.value, '","password":"', activity('act_get_password').output.value, '"}')`

---

#### `act_set_token` — Set Variable

**Depends on:** `act_api_login` (Succeeded)  
**Required by:** `act_fetch_page1_meta`

- Variable: `v_token`
- Value: `@activity('act_api_login').output.token`

---

#### `act_fetch_page1_meta` — Web Activity (GET page 1)

**Depends on:** `act_set_token` (Succeeded)  
**Required by:** `act_set_total_pages`

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

> This uses a Web Activity (not Copy) because only Web Activity exposes the full response body. ADF's Copy Activity does not surface `pagination.total_pages` as an output — it only reports rows/bytes copied.

---

#### `act_set_total_pages` — Set Variable

**Depends on:** `act_fetch_page1_meta` (Succeeded)  
**Required by:** `Copy data1` (act_copy_page1)

- Variable: `v_total_pages`
- Value: `@int(activity('act_fetch_page1_meta').output.pagination.total_pages)`

---

#### `Copy data1` (act_copy_page1) — Copy Activity

**Depends on:** `act_set_total_pages` + all 7 Set Variable activities from `act_read_ctl` branch:
- `act_set_total_pages` (Succeeded)
- `act_set_endpoint` (Succeeded)
- `act_set_sink_path` (Succeeded)
- `act_set_file_prefix` (Succeeded)
- `act_set_page_size` (Succeeded)
- `act_set_watermark` (Succeeded)
- `act_set_ingestion_date` (Succeeded)
- `act_set_run_ts` (Succeeded)

**Required by:** Until loop

This is the join point where both branches (Databricks control table read and API auth + page1 fetch) converge before the loop.

**Source — `ds_voltgrid_payments_src`:**
```
p_page:         "1"
p_page_size:    @variables('v_page_size')
p_endpoint:     @variables('v_endpoint')
p_updated_after: @variables('v_last_watermark')
```

**Sink — `ds_bronze_payments_sink`:**
```
p_folder: @concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'))
p_file:   @concat(variables('v_file_prefix'), '_page_1.json')
```

Result file: `bronze/api/payments/ingestion_date=2026-10-14/payments_page_1.json`

> **Bug in your current JSON:** The `p_file` expression has newlines inside the string which ADF cannot parse, and produces `payments.json` (no page number). Fix to: `@concat(variables('v_file_prefix'), '_page_1.json')`

---

#### Until Loop

**Depends on:** `Copy data1` (Succeeded)  
**Required by:** `act_update_watermark`

**Exit condition:**
```
@greaterOrEquals(variables('v_current_page'), add(variables('v_total_pages'), 1))
```

Before the loop starts, initialise the counter: add a **Set Variable** activity between `Copy data1` and Until:

**`act_set_current_page_2`** (Set Variable):
- **Depends on:** `Copy data1` (Succeeded)
- Variable: `v_current_page`
- Value: `2`

Then connect `act_set_current_page_2` → Until.

**Inside the Until loop** (all three must be on the inner canvas — click Until → Edit pencil):

> **ADF limitation:** A Set Variable activity cannot read and write the same variable in one expression (self-reference error). The counter increment therefore requires two activities — one writes to a temp variable, the next copies it back.

**`act_copy_page`** (Copy Activity):
- **Depends on:** nothing inside the loop (first activity to run each iteration)
- Source — `ds_voltgrid_payments_src`:
  ```
  p_page:          @variables('v_current_page')
  p_page_size:     @variables('v_page_size')
  p_endpoint:      @variables('v_endpoint')
  p_updated_after: @variables('v_last_watermark')
  ```
  Additional header: `Authorization` = `@concat('Token ', variables('v_token'))`
- Sink — `ds_bronze_payments_sink`:
  ```
  p_folder: @concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'))
  p_file:   @concat(variables('v_file_prefix'), '_page_', string(variables('v_current_page')), '.json')
  ```

**`act_increment_page`** (Set Variable):
- **Depends on:** `act_copy_page` (Succeeded)
- Variable: `v_next_page`
- Value: `@add(variables('v_current_page'), 1)`

**`act_set_current_page`** (Set Variable):
- **Depends on:** `act_increment_page` (Succeeded)
- Variable: `v_current_page`
- Value: `@variables('v_next_page')`

> **Critical:** All three activities must be inside the Until body. If any are on the main canvas, `v_current_page` never advances and the loop fetches page 2 forever until timeout.

---

#### `act_update_watermark` — Databricks Notebook

**Depends on:** Until loop (Succeeded)  
**Required by:** nothing (final step on success path)

- Linked service: `ls_databricks`
- Notebook path: `/Users/hariomsuryawanshi68258@gmail.com/nb_update_watermark`
- Base parameters:
  - `source_name` = `@pipeline().parameters.p_source_name`
  - `new_watermark` = `@variables('v_run_ts')`

---

### 3.4 Complete Activity Dependency Summary

```
Pipeline start (parallel):
  act_read_ctl        ── no dependency
  act_get_username    ── no dependency
  act_get_password    ── no dependency

After act_read_ctl (all parallel, all depend only on act_read_ctl):
  act_set_endpoint
  act_set_sink_path
  act_set_file_prefix
  act_set_page_size
  act_set_watermark
  act_set_ingestion_date
  act_set_run_ts

After act_get_username + act_get_password:
  act_api_login       ── depends on: act_get_username, act_get_password

After act_api_login:
  act_set_token       ── depends on: act_api_login

After act_set_token:
  act_fetch_page1_meta ── depends on: act_set_token

After act_fetch_page1_meta:
  act_set_total_pages  ── depends on: act_fetch_page1_meta

JOIN POINT — Copy data1 waits for BOTH branches:
  Copy data1          ── depends on: act_set_total_pages,
                                     act_set_endpoint, act_set_sink_path,
                                     act_set_file_prefix, act_set_page_size,
                                     act_set_watermark, act_set_ingestion_date,
                                     act_set_run_ts

After Copy data1:
  act_set_current_page_2 ── depends on: Copy data1

After act_set_current_page_2:
  Until loop             ── depends on: act_set_current_page_2
    act_copy_page        ── no dependency (first in loop body)
    act_increment_page   ── depends on: act_copy_page  (writes v_next_page = current + 1)
    act_set_current_page ── depends on: act_increment_page  (writes v_current_page = v_next_page)

After Until loop:
  act_update_watermark ── depends on: Until (Succeeded)
```

---

## Part 4 — Sink Path Convention and File Naming

### 4.1 Resolved Paths

For a payments run on 2026-10-14 with 321 total pages:

```
bronze/
└── api/
    └── payments/
        └── ingestion_date=2026-10-14/
            ├── payments_page_1.json    ← page 1  (1,000 records)
            ├── payments_page_2.json    ← page 2  (1,000 records)
            │   ...
            └── payments_page_321.json  ← page 321 (~217 records — last page)
```

### 4.2 ADF Expression Examples

| Expression | Example resolved value |
|---|---|
| `variables('v_sink_base_path')` | `api/payments` |
| `variables('v_ingestion_date')` | `2026-10-14` |
| `variables('v_file_prefix')` | `payments` |
| `variables('v_current_page')` | `42` |
| Full folder | `api/payments/ingestion_date=2026-10-14` |
| Full file (loop) | `payments_page_42.json` |
| Full file (page 1) | `payments_page_1.json` |

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
- [ ] ADF MI added as **User** inside Databricks workspace (workspace-level access)
- [ ] `ls_databricks` test connection green

**Datasets:**
- [ ] `ds_voltgrid_payments_src` has 4 parameters: `p_endpoint`, `p_page`, `p_page_size`, `p_updated_after`
- [ ] `ds_bronze_payments_sink` has 2 parameters: `p_folder`, `p_file`

**Pipeline activities:**
- [ ] `act_read_ctl` — notebook path matches actual path in Databricks Workspace (case-sensitive)
- [ ] All 7 Set Variable activities depend on `act_read_ctl`
- [ ] `act_api_login` depends on both `act_get_username` AND `act_get_password`
- [ ] `act_fetch_page1_meta` depends on `act_set_token`
- [ ] `Copy data1` depends on `act_set_total_pages` + all 7 SetVar activities (8 total dependencies)
- [ ] `Copy data1` sink `p_file` is `@concat(variables('v_file_prefix'), '_page_1.json')`
- [ ] Loop `act_copy_page` sink `p_file` is `@concat(variables('v_file_prefix'), '_page_', string(variables('v_current_page')), '.json')`
- [ ] `act_set_current_page_2` sets `v_current_page = 2`, depends on `Copy data1`
- [ ] Until loop connected from `act_set_current_page_2`
- [ ] `act_copy_page`, `act_increment_page`, `act_set_current_page` are all INSIDE the Until body
- [ ] `act_increment_page` depends on `act_copy_page` — writes `v_next_page = v_current_page + 1`
- [ ] `act_set_current_page` depends on `act_increment_page` — writes `v_current_page = v_next_page`
- [ ] Pipeline variable `v_next_page` (Integer) exists — required because ADF forbids self-referencing variables
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
- [ ] Files `payments_page_001.json` through `payments_page_NNN.json` all exist with correct naming
- [ ] Control table: `load_type = 'incremental'`, `last_watermark` updated to pipeline start UTC

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `User not authorized` or `403` on notebook run | ADF MI not added as user inside Databricks workspace | Admin Console → Users → Add the ADF MI principal as User |
| `act_read_ctl` fails — `Contributor` assigned but still 403 | Only ARM RBAC was set; workspace-level user grant missing | See Part 1.2 — must add MI as workspace user separately |
| `act_read_ctl` fails — notebook not found | Notebook path is case-sensitive and must match exactly | Copy path from Databricks → right-click notebook → Copy URL or check breadcrumb |
| `json()` returns null | Notebook `dbutils.notebook.exit()` was not reached | Check Databricks job run logs linked in the activity output |
| `total_pages` resolves to 0 | `updated_after` filter is too recent (watermark not reset after Day 3 test) | Trigger with `p_load_type_override = full` |
| Page 1 file named `payments.json` | `p_file` expression has newlines or missing `_page_1` suffix | Fix `Copy data1` sink: `@concat(variables('v_file_prefix'), '_page_1.json')` |
| `The expression contains self referencing variable` | `v_current_page = @add(variables('v_current_page'), 1)` — ADF forbids reading and writing the same variable | Use two activities: `act_increment_page` writes `v_next_page = current + 1`, then `act_set_current_page` writes `v_current_page = v_next_page` |
| Until loop runs forever | Any of `act_copy_page`, `act_increment_page`, `act_set_current_page` is on the main canvas instead of inside the Until body | Click Until → Edit (pencil) → confirm all three activities are on the inner canvas |
| Until loop exits after page 1 | `v_current_page` was never set to 2, or condition is wrong | Confirm `act_set_current_page_2` runs before Until and sets value `2` |
| `act_update_watermark` widget not found | Parameter names don't match notebook widget names | Notebook expects exactly `source_name` and `new_watermark` — check spelling |

---

## Quick Reference

```
Pipeline      : pl_bronze_incremental_load
Trigger       : trg_payments_incremental (Tumbling Window, daily)

Sink pattern  : bronze/<sink_base_path>/ingestion_date=YYYY-MM-DD/<file_prefix>_page_N.json
Example       : bronze/api/payments/ingestion_date=2026-10-14/payments_page_1.json

Control table : ev_catalog.ctl.pipeline_control
                abfss://bronze@evdatalakedev2.dfs.core.windows.net/ctl/pipeline_control

Access chain (ADF):
  adf-ev-intelligence-dev MI  →  Key Vault Secrets User        →  kv-ev-intelligence-dev
  adf-ev-intelligence-dev MI  →  Storage Blob Data Contributor →  evdatalakedev2
  adf-ev-intelligence-dev MI  →  Contributor (ARM)             →  dbw-ev-intelligence-dev
  adf-ev-intelligence-dev MI  →  User (workspace)              →  dbw-ev-intelligence-dev

Notebook paths (actual — match exactly):
  /Users/hariomsuryawanshi68258@gmail.com/nb_read_control_table
  /Users/hariomsuryawanshi68258@gmail.com/nb_update_watermark

API pagination:
  page_size     = 1000 (from control table)
  total_pages   = response.pagination.total_pages (from act_fetch_page1_meta)
  updated_after = v_last_watermark (epoch on full load, previous watermark on incremental)

Watermark update: v_run_ts (utcnow() at pipeline start) written after all pages succeed
```
