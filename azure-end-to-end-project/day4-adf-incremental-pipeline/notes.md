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
                                     ├── Until loop (pages 2 to N)
                                     └── Update watermark (Databricks notebook)
                                   trg_payments_incremental (new Tumbling Window trigger)
```

---

## Architecture: Full Pipeline Flow

```
[ADF Pipeline: pl_bronze_incremental_load]

  act_read_ctl  (Databricks Notebook)
    → reads ev_catalog.ctl.pipeline_control for p_source_name
    → exits with JSON: endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark
        ↓
  act_set_endpoint / act_set_sink_path / act_set_file_prefix / act_set_page_size  (Set Variable ×4, parallel)
        ↓
  act_set_watermark  (Set Variable — epoch if p_load_type_override=full, else control table value)
  act_set_ingestion_date  (Set Variable — yyyy-MM-dd)
  act_set_run_ts  (Set Variable — utcnow(), used as new watermark after success)
        ↓
  act_get_username ─┐
  act_get_password  ├──→  act_api_login  →  act_set_token
                    ↓
  act_fetch_page1_meta  (Web Activity — GET page 1, capture total_pages from pagination block)
        ↓
  act_set_total_pages  (Set Variable — v_total_pages)
  act_copy_page1  (Copy Activity — write page 1 data to Bronze sink)
  act_set_current_page_2  (Set Variable — v_current_page = 2)
        ↓
  Until: @greaterOrEquals(v_current_page, add(v_total_pages, 1))
    ├── act_copy_page  (Copy Activity — page N)
    └── act_increment_page  (Set Variable — v_current_page + 1)
        ↓
  act_update_watermark  (Databricks Notebook — writes v_run_ts to control table)
        ↓ (On Failure from any copy)
  act_alert_failure  (Web Activity — alert webhook)
```

---

## Part 1 — ADF Linked Service for Databricks

ADF needs a linked service to call Databricks notebooks. It uses ADF's system-assigned Managed Identity — the same MI already used for Key Vault and ADLS.

### 1.1 Create Linked Service `ls_databricks`

**ADF Studio → Manage → Linked services → + New:**

1. Search `Azure Databricks` → select **Azure Databricks** (under **Compute** category — NOT "Azure Databricks Delta Lake") → **Continue**

2. Fill:

   | Field | Value |
   |---|---|
   | Name | `ls_databricks` |
   | Azure subscription | yours |
   | Databricks workspace | `dbw-ev-intelligence-dev` |
   | Select cluster | **New job cluster** |
   | Job cluster version | `15.4 LTS (includes Apache Spark 3.5.0, Scala 2.12)` |
   | Node type | `Standard_DS3_v2` |
   | Authentication type | `System Assigned Managed Identity` |

3. **Test connection** → green → **Create**

> **New job cluster:** ADF spins up a fresh cluster per pipeline run and terminates it when done. No idle cluster to forget to stop — cost is incurred only during execution (~5 minutes for a full payments load).

### 1.2 Grant ADF MI Access to Databricks Workspace

ADF's Managed Identity needs `Contributor` on the Databricks workspace to trigger notebook jobs.

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

---

## Part 2 — Update Source Dataset

The Day 2 dataset `ds_voltgrid_payments_src` needs two new parameters so it can serve any source endpoint and carry the `updated_after` watermark.

**ADF Studio → Author → Datasets → `ds_voltgrid_payments_src`:**

1. **Parameters** tab → add:
   - `p_endpoint` — String — default: `/api/db/payments/`
   - `p_updated_after` — String — default: `1900-01-01T00:00:00Z`

   (Keep existing `p_page` and `p_page_size` parameters.)

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

**Full dataset parameters after update:**

| Parameter | Type | Default |
|---|---|---|
| `p_endpoint` | String | `/api/db/payments/` |
| `p_page` | Int | `1` |
| `p_page_size` | Int | `1000` |
| `p_updated_after` | String | `1900-01-01T00:00:00Z` |

### 2.2 Update Sink Dataset `ds_bronze_payments_sink`

The Day 2 sink dataset had the folder path and file name hardcoded. In Day 4 both must come from pipeline variables so the pipeline can serve any source and any date partition.

**ADF Studio → Author → Datasets → `ds_bronze_payments_sink`:**

1. **Parameters** tab → add:
   - `p_folder` — String — default: `api/payments/ingestion_date=2026-10-14`
   - `p_file` — String — default: `payments_page_001.json`

2. **Connection** tab:
   - Container: `bronze`
   - File path (Directory): `@dataset().p_folder`
   - File path (File): `@dataset().p_file`

3. **Publish all**

> The pipeline passes these at runtime:
> - `p_folder` = `@concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'))`
> - `p_file` = `@concat(variables('v_file_prefix'), '_page_', padLeft(string(variables('v_current_page')), 3, '0'), '.json')`

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
| `v_total_pages` | Integer | Max pages from API page 1 response |
| `v_run_ts` | String | ISO 8601 UTC captured at pipeline start — written back as new watermark |

### 3.3 Build Step by Step

#### Step 1 — Read Control Table

Drag **Databricks Notebook** activity → rename `act_read_ctl`

**Azure Databricks** tab:
- Linked service: `ls_databricks`
- Notebook path: `/Workspace/ev-project/nb_read_control_table`

**Settings** tab → **Base parameters** → **+ New**:
- Name: `source_name` | Value → **Add dynamic content**: `@pipeline().parameters.p_source_name`

---

#### Step 2 — Set Variables from Control Table Output

Connect 4 **Set Variable** activities in parallel from `act_read_ctl` (all depend on it, none depend on each other):

> The notebook exits with a JSON **string** via `dbutils.notebook.exit(json.dumps(result))`. Use `@json()` in ADF expressions to parse it before accessing fields.

| Activity | Variable | Value expression |
|---|---|---|
| `act_set_endpoint` | `v_endpoint` | `@json(activity('act_read_ctl').output.runOutput).endpoint` |
| `act_set_sink_path` | `v_sink_base_path` | `@json(activity('act_read_ctl').output.runOutput).sink_base_path` |
| `act_set_file_prefix` | `v_file_prefix` | `@json(activity('act_read_ctl').output.runOutput).file_prefix` |
| `act_set_page_size` | `v_page_size` | `@string(json(activity('act_read_ctl').output.runOutput).page_size)` |

---

#### Step 3 — Set Watermark, Date, and Run Timestamp

After all 4 Set Variable activities above, add 3 more **Set Variable** activities:

**act_set_watermark:**
```
Variable: v_last_watermark
Value:
  @if(
    and(
      not(empty(pipeline().parameters.p_load_type_override)),
      equals(pipeline().parameters.p_load_type_override, 'full')
    ),
    '1900-01-01T00:00:00Z',
    json(activity('act_read_ctl').output.runOutput).last_watermark
  )
```
> If `p_load_type_override = 'full'` → use epoch (fetch everything). Otherwise → use control table watermark (incremental).

**act_set_ingestion_date:**
```
Variable: v_ingestion_date
Value: @formatDateTime(utcnow(), 'yyyy-MM-dd')
```

**act_set_run_ts:**
```
Variable: v_run_ts
Value: @utcnow()
```
> Captured at pipeline **start** — not end. Captures at start means any records updated during the pipeline run are included in the next incremental window (safe overlap vs. risk of gap).

---

#### Step 4 — Authenticate to VoltGrid API

Add **act_get_username** and **act_get_password** (parallel Web Activities to Key Vault) then **act_api_login** (POST to VoltGrid) then **act_set_token** — identical to Day 2 pipeline. See Day 2 notes Part 7.3 for exact settings.

Key Vault URLs:
- Username: `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-username/?api-version=7.0`
- Password: `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-password/?api-version=7.0`

Login POST body:
```
@concat('{"username":"', activity('act_get_username').output.value, '","password":"', activity('act_get_password').output.value, '"}')
```

Set token:
```
Variable: v_token
Value: @activity('act_api_login').output.token
```

---

#### Step 5 — Fetch Page 1 and Capture total_pages

ADF's Copy Activity writes data to the sink but does not expose the full API response body (including pagination metadata). Use a **Web Activity** for page 1 to read the response — then a separate Copy Activity to write page 1 data.

**act_fetch_page1_meta** (Web Activity):
- Method: `GET`
- URL → Add dynamic content:
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
- Headers: `Authorization` = `@concat('Token ', variables('v_token'))`

**act_set_total_pages** (Set Variable, after `act_fetch_page1_meta`):
```
Variable: v_total_pages
Value: @int(activity('act_fetch_page1_meta').output.pagination.total_pages)
```

**act_copy_page1** (Copy Activity, parallel with `act_set_total_pages`):
- Source: `ds_voltgrid_payments_src`
  - `p_endpoint`: `@variables('v_endpoint')`
  - `p_page`: `1`
  - `p_page_size`: `@variables('v_page_size')`
  - `p_updated_after`: `@variables('v_last_watermark')`
  - Additional headers: `Authorization` = `@concat('Token ', variables('v_token'))`
- Sink: `ds_bronze_adls_sink` (JSON, parameterised)
  - Container: `bronze`
  - Folder path: `@concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'))`
  - File name: `@concat(variables('v_file_prefix'), '_page_', padLeft('1', 3, '0'), '.json')`

Result: `bronze/api/payments/ingestion_date=2026-10-14/payments_page_001.json`

---

#### Step 6 — Set Current Page to 2

```
Variable: v_current_page
Value: 2
```

Page 1 already written. Until loop starts at page 2.

---

#### Step 7 — Until Loop (Pages 2 to total_pages)

Drag **Until** activity. Connect `act_set_current_page_2` → Until.

**Until expression** → Add dynamic content:
```
@greaterOrEquals(variables('v_current_page'), add(variables('v_total_pages'), 1))
```
> Exits when `v_current_page > v_total_pages`. The `+1` accounts for the post-increment — page N runs, then increments to N+1, then the condition checks N+1 >= N+1 → true → exit.

**Inside the Until loop** (click Until → Edit pencil — both activities must appear on the inner canvas):

**act_copy_page** (Copy Activity):
- Source: `ds_voltgrid_payments_src`
  - `p_endpoint`: `@variables('v_endpoint')`
  - `p_page`: `@variables('v_current_page')`
  - `p_page_size`: `@variables('v_page_size')`
  - `p_updated_after`: `@variables('v_last_watermark')`
  - Additional headers: `Authorization` = `@concat('Token ', variables('v_token'))`
- Sink: `ds_bronze_adls_sink`
  - Folder: `@concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'))`
  - File: `@concat(variables('v_file_prefix'), '_page_', padLeft(string(variables('v_current_page')), 3, '0'), '.json')`

**act_increment_page** (Set Variable, depends on `act_copy_page` success):
```
Variable: v_current_page
Value: @add(variables('v_current_page'), 1)
```

> **Critical:** Both activities must be inside the Until body — drag them onto the inner canvas. If `act_increment_page` is on the main canvas, the page counter never advances and the loop runs forever.

---

#### Step 8 — Update Watermark

On Success from Until loop, add **Databricks Notebook** activity:

**act_update_watermark:**
- Linked service: `ls_databricks`
- Notebook path: `/Workspace/ev-project/nb_update_watermark`
- Base parameters:
  - `source_name`: `@pipeline().parameters.p_source_name`
  - `new_watermark`: `@variables('v_run_ts')`

---

#### Step 9 — Failure Alert

Connect **On Failure** from `act_copy_page1` and `act_copy_page` to a Web Activity `act_alert_failure`. Use a Teams webhook or Logic App URL to notify on any page failure.

---

## Part 4 — Sink Path Convention and File Naming

### 4.1 Resolved Paths

For a payments run on 2026-10-14 with 321 total pages:

```
bronze/
└── api/
    └── payments/
        └── ingestion_date=2026-10-14/
            ├── payments_page_001.json    ← 1,000 records
            ├── payments_page_002.json    ← 1,000 records
            │   ...
            └── payments_page_321.json   ← ~217 records (last page)
```

### 4.2 Why This Naming Convention Is Reliable

| Property | How achieved |
|---|---|
| **Lexicographic sort = correct order** | Zero-padded 3-digit page number — `_page_001`, `_page_002` |
| **Idempotent re-runs** | Same `ingestion_date` + same page → same file name → overwrite is safe |
| **Source isolation** | `file_prefix` from control table — `payments_page_001.json` vs `sessions_page_001.json` |
| **No hardcoded paths in ADF** | All path values from control table — add new source by inserting one row |
| **Hive partition discovery** | `ingestion_date=YYYY-MM-DD` → Spark auto-discovers partitions on read |
| **Long-lasting** | Handles 1–999 pages without naming conflicts |

### 4.3 ADF Expression Examples

| Expression | Example resolved value |
|---|---|
| `variables('v_sink_base_path')` | `api/payments` |
| `variables('v_ingestion_date')` | `2026-10-14` |
| `variables('v_file_prefix')` | `payments` |
| `variables('v_current_page')` | `42` |
| Full folder | `api/payments/ingestion_date=2026-10-14` |
| Full file | `payments_page_042.json` |

---

## Part 5 — Trigger Setup

Use a **Tumbling Window Trigger** — tracks every window, guarantees no day is silently missed (unlike Schedule trigger which fires and forgets).

**ADF Studio → Manage → Triggers → + New:**

| Field | Value |
|---|---|
| Type | `Tumbling window` |
| Name | `trg_payments_incremental` |
| Start time | `2026-10-14T00:00:00Z` |
| Recurrence | `1 Day` |
| Max concurrency | `1` |
| Retry | `2` times, `30` minutes interval |

**Pipeline parameters for this trigger:**
- `p_source_name`: `payments`
- `p_load_type_override`: `` (empty — use control table)

**Publish all** → **Activate trigger**

**For backfill:**
- Set start date to how far back you need (e.g. `2026-01-01T00:00:00Z`)
- ADF queues all windows from that date to today automatically — each window gets its own `ingestion_date` partition

---

## Part 6 — Adding a New Source (Zero Pipeline Changes)

Once the pipeline is built, adding any new API endpoint requires only a SQL INSERT:

```sql
INSERT INTO ev_catalog.ctl.pipeline_control
    (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
VALUES
    ('sessions', '/api/db/sessions/', 'api/sessions', 'sessions', 1000, 'full', '1900-01-01T00:00:00Z', true);
```

Then trigger with `p_source_name = 'sessions'` — no ADF pipeline changes required.

Sink resolves automatically:
```
bronze/api/sessions/ingestion_date=2026-10-14/sessions_page_001.json
```

---

## Verify: Day 4 Checklist

**ADF setup:**
- [ ] Linked service `ls_databricks` created — New job cluster, test connection green
- [ ] ADF MI has `Contributor` on `dbw-ev-intelligence-dev`
- [ ] Dataset `ds_voltgrid_payments_src` updated — 4 parameters including `p_endpoint` and `p_updated_after`
- [ ] Pipeline `pl_bronze_incremental_load` created with all parameters and variables

**Pipeline activities:**
- [ ] `act_read_ctl` returns correct JSON from control table
- [ ] Set Variable activities correctly parse JSON fields with `@json(...)`
- [ ] `act_fetch_page1_meta` returns `pagination.total_pages` from API
- [ ] `act_copy_page1` writes `payments_page_001.json` to Bronze
- [ ] Until loop condition uses `add(v_total_pages, 1)` — not just `v_total_pages`
- [ ] `act_increment_page` is inside the Until body (not on main canvas)
- [ ] Until loop runs pages 2 to N — each page writes `payments_page_NNN.json`
- [ ] `act_update_watermark` updates `last_watermark` and `load_type` in control table

**Post-run verification:**
```bash
az storage blob list \
  --account-name evdatalakedev2 \
  --container-name bronze \
  --prefix "api/payments/ingestion_date=" \
  --auth-mode login \
  --query "[].{name:name, size:properties.contentLength}" -o table
```
- [ ] Files `payments_page_001.json` through `payments_page_NNN.json` all exist
- [ ] No zero-byte files (all pages returned data)
- [ ] Control table: `load_type = 'incremental'`, `last_watermark` updated to today's UTC timestamp
- [ ] Second pipeline run (incremental) fetches fewer pages than full run
- [ ] Trigger `trg_payments_incremental` created and active

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `act_read_ctl` fails — `You do not have permission` | ADF MI missing Contributor on Databricks workspace | Portal → Databricks → IAM → assign Contributor to `adf-ev-intelligence-dev` |
| `act_read_ctl` fails — notebook not found | Notebook path wrong | Check exact path in Databricks Workspace — case-sensitive |
| `json()` returns null in Set Variable | `dbutils.notebook.exit()` not called or notebook failed silently | Check Databricks Jobs logs (triggered by ADF) for the run |
| `total_pages` is 0 or 1 | `updated_after` filter too recent — watermark not reset after Day 3 test | Run with `p_load_type_override = full` to force epoch watermark |
| Until loop runs forever | `act_increment_page` is outside the Until body | Click Until → Edit pencil — confirm `act_increment_page` is on the inner canvas |
| File size 0 bytes for some pages | Copy Activity succeeded but API returned empty `data` array | Check API for that page number manually; may be a gap in the dataset |
| `encodeUriComponent is not recognized` | ADF expression function name case mismatch | Use lowercase exactly: `encodeUriComponent` (not `EncodeUriComponent`) |
| `act_update_watermark` fails — widget not found | Notebook widget name mismatch | Notebook expects `source_name` and `new_watermark` — verify ADF parameter names match exactly |

---

## Quick Reference

```
Pipeline      : pl_bronze_incremental_load
Trigger       : trg_payments_incremental (Tumbling Window, daily)

Sink pattern  : bronze/<sink_base_path>/ingestion_date=YYYY-MM-DD/<file_prefix>_page_NNN.json
Example       : bronze/api/payments/ingestion_date=2026-10-14/payments_page_001.json

Control table : ev_catalog.ctl.pipeline_control
                abfss://bronze@evdatalakedev2.dfs.core.windows.net/ctl/pipeline_control

Access chain (ADF):
  adf-ev-intelligence-dev MI  →  Key Vault Secrets User        →  kv-ev-intelligence-dev
  adf-ev-intelligence-dev MI  →  Storage Blob Data Contributor →  evdatalakedev2
  adf-ev-intelligence-dev MI  →  Contributor                   →  dbw-ev-intelligence-dev

API pagination:
  page_size     = 1000 (from control table)
  total_pages   = response.pagination.total_pages (from act_fetch_page1_meta)
  updated_after = v_last_watermark (epoch on full load, previous watermark on incremental)

Watermark update: v_run_ts (utcnow() captured at pipeline start) written after all pages succeed
```
