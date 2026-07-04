# 02 — ADF Pipeline: API Payments → Bronze Delta
**Day 2 | Step 2 of 4**

Build an ADF pipeline that:
1. Logs in to VoltGrid API → gets a token
2. On first run: loads ALL payments pages (full load)
3. On subsequent runs: loads only pages where `updated_at > last_watermark` (incremental)
4. Writes every page to `bronze/api/payments/` as a Delta table in ADLS Gen2
5. Writes the new high-watermark to `pipeline_audit` after a successful run

---

## Pipeline Overview

```
pl_bronze_api_payments
│
├── Step 1: Web Activity         POST /api/auth/login/   → pipeline variable: token
│
├── Step 2: Lookup Activity      Read last watermark from pipeline_audit table
│                                (if no prior run → use "1900-01-01T00:00:00Z")
│
├── Step 3: Set Variable         watermark = result from Step 2
│
├── Step 4: Until Activity       loop while there are more pages
│   └── Step 4a: Copy Activity   GET /api/db/payments/?updated_after={wm}&page={n}
│                                → append to Bronze Delta table
│
└── Step 5: Notebook Activity    Write new watermark to pipeline_audit
                                 (max updated_at seen in this run)
```

---

## API Behaviour

```
GET /api/db/payments/?updated_after=2026-01-01T00:00:00Z&page=1&page_size=100

Response:
{
  "data": [ { ...payment... }, ... ],
  "pagination": {
    "page": 1,
    "page_size": 100,
    "total": 12500,
    "total_pages": 125
  }
}
```

- `updated_after` — ISO 8601 timestamp. Returns only rows where `updated_at > value`. Omit on first run to get all rows.
- `page_size` max is 100.
- Loop pages 1 → `total_pages` to get everything.

---

## Part A — Create Datasets

### Dataset 1: VoltGrid Payments REST Source (`ds_voltgrid_payments_src`)

**UI Steps:**

1. ADF Studio → **Author** (pencil icon) → **Datasets** → **+ New dataset**
2. Search `REST` → **REST** → **Continue**
3. Fill in:
   - **Name:** `ds_voltgrid_payments_src`
   - **Linked service:** `ls_voltgrid_api`
   - **Relative URL:** `/api/db/payments/`
4. Click **OK**
5. Go to **Parameters** tab → **+ New**:
   - `p_page` | Type: Int | Default: 1
   - `p_page_size` | Type: Int | Default: 100
   - `p_updated_after` | Type: String | Default: (empty)
6. Go to **Connection** tab → **Relative URL** field → click **Add dynamic content**:
   ```
   /api/db/payments/?page=@{dataset().p_page}&page_size=@{dataset().p_page_size}@{if(empty(dataset().p_updated_after),'',concat('&updated_after=',dataset().p_updated_after))}
   ```
   > The `if(empty(...))` means: on first run (no watermark), omit `updated_after` entirely → gets all rows. On subsequent runs, add the filter.

7. Click **Publish all**

---

### Dataset 2: Bronze Payments Delta Sink (`ds_bronze_payments_delta`)

**UI Steps:**

1. **Datasets** → **+ New dataset**
2. Search `Azure Data Lake Storage Gen2` → **Continue**
3. Search `Delta` → **Delta** → **Continue**
4. Fill in:
   - **Name:** `ds_bronze_payments_delta`
   - **Linked service:** `ls_adls_bronze`
   - **File path:** `bronze` / `api/payments`
5. Click **OK**
6. Go to **Connection** tab:
   - **Compression type:** None
   - **Table name:** leave blank (Delta handles it via path)
7. Click **Publish all**

---

## Part B — Create Pipeline `pl_bronze_api_payments`

**UI Steps:**

1. **Author** → **Pipelines** → **+ New pipeline**
2. **Name:** `pl_bronze_api_payments`
3. Go to **Parameters** tab → **+ New**:
   - `p_load_type` | Type: String | Default: `incremental`

---

### Step 1 — Web Activity: Login and get token

1. Drag **Web** activity onto the canvas
2. **Name:** `act_api_login`
3. **Settings** tab:
   - **URL:** click dynamic content:
     ```
     @{linkedService('ls_voltgrid_api').url}/api/auth/login/
     ```
     > Simpler: just hardcode the full login URL using Key Vault reference. In practice use:
     ```
     @{concat(activity('act_get_base_url').output.value, '/api/auth/login/')}
     ```
     Or the cleanest way — store the full login URL as a secret `voltgrid-login-url` in Key Vault and reference it here.

     **Recommended — add a Web Activity before this to fetch the base URL from Key Vault:**

     Name: `act_get_base_url`
     URL: `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-api-base-url/?api-version=7.0`
     Method: GET
     Authentication: Managed Identity
     Resource: `https://vault.azure.net`

     Then the login URL becomes:
     ```
     @{concat(activity('act_get_base_url').output.value, '/api/auth/login/')}
     ```

   - **Method:** POST
   - **Headers:** `Content-Type: application/json`
   - **Body:**
     ```json
     {
       "username": "@{activity('act_get_username').output.value}",
       "password": "@{activity('act_get_password').output.value}"
     }
     ```
     > Add two more Web Activities (`act_get_username`, `act_get_password`) that fetch the secrets from Key Vault the same way as `act_get_base_url`.

4. Output of this activity: `activity('act_api_login').output.token`

---

### Step 2 — Set Variable: Store token

1. Add **Set Variable** activity after `act_api_login`
2. **Name:** `act_set_token`
3. Go to **Variables** tab on the pipeline canvas → **+ New**:
   - `v_token` | Type: String
   - `v_watermark` | Type: String
   - `v_current_page` | Type: Int | Default: 1
   - `v_total_pages` | Type: Int | Default: 1
4. In `act_set_token`:
   - **Variable:** `v_token`
   - **Value:** `@{activity('act_api_login').output.token}`

---

### Step 3 — Lookup Activity: Read last watermark

1. Add **Lookup** activity after `act_set_token`
2. **Name:** `act_read_watermark`
3. **Settings:**
   - **Source dataset:** you will need a Databricks dataset or a script to query `pipeline_audit`
   - **Simpler approach for now:** use a **Web Activity** to call a Databricks job that returns the watermark, OR use a hardcoded **Set Variable** for the first run:

**Watermark Variable approach (simpler — works for Day 2):**

Add a **Set Variable** activity:
- **Name:** `act_set_watermark`
- **Variable:** `v_watermark`
- **Value:** `1900-01-01T00:00:00Z`
  > This means "first run always = full load". In Day 8 (ADF Orchestration) you will wire this to the real pipeline_audit table lookup. For now, changing this value manually simulates incremental.

---

### Step 4 — Until Activity: Paginate all pages

1. Add **Until** activity after `act_set_watermark`
2. **Name:** `act_paginate`
3. **Expression** (stop when current page > total pages):
   ```
   @greater(variables('v_current_page'), variables('v_total_pages'))
   ```
4. Inside the Until, add a **Copy Activity**:

**Copy Activity: `act_copy_payments_page`**

- **Source** tab:
  - Dataset: `ds_voltgrid_payments_src`
  - Dataset parameters:
    - `p_page`: `@{variables('v_current_page')}`
    - `p_page_size`: `100`
    - `p_updated_after`: `@{variables('v_watermark')}`
  - **Additional headers:**
    ```
    Authorization: Token @{variables('v_token')}
    ```
  - **Pagination rules:**
    - `$.pagination.total_pages` → store in: `v_total_pages`

- **Sink** tab:
  - Dataset: `ds_bronze_payments_delta`
  - **Write behavior:** Append
  - **Pre-copy script:** (leave empty — Delta handles dedup in Silver layer)

- **Mapping** tab:
  - Click **Import schemas** → map `data[*]` fields to Delta columns:

    | Source (JSON path) | Destination column | Type |
    |---|---|---|
    | `$.data[*].payment_id` | payment_id | String |
    | `$.data[*].session_id` | session_id | String |
    | `$.data[*].customer_id` | customer_id | String |
    | `$.data[*].gateway` | gateway | String |
    | `$.data[*].amount_aud` | amount_aud | Double |
    | `$.data[*].gst` | gst | Double |
    | `$.data[*].payment_mode` | payment_mode | String |
    | `$.data[*].status` | status | String |
    | `$.data[*].processed_at` | processed_at | String |
    | `$.data[*].created_at` | created_at | String |
    | `$.data[*].updated_at` | updated_at | String |

5. After the Copy Activity inside Until, add **Set Variable** to increment the page:
   - **Name:** `act_increment_page`
   - **Variable:** `v_current_page`
   - **Value:** `@{add(variables('v_current_page'), 1)}`

---

### Step 5 — Notebook Activity: Write new watermark

After the Until activity completes:

1. Add **Notebook** activity
2. **Name:** `act_write_watermark`
3. **Azure Databricks** tab:
   - Linked service: your Databricks linked service (create one if not done — see below)
   - Notebook path: `/Shared/ev-project/03_bronze_api_payments`
4. **Base parameters:**
   - `load_type`: `@{pipeline().parameters.p_load_type}`
   - `pipeline_run_id`: `@{pipeline().RunId}`

---

## Part C — Create Databricks Linked Service (if not done)

**UI Steps:**

1. Manage → Linked services → **+ New**
2. Search `Azure Databricks` → **Continue**
3. Fill in:
   - **Name:** `ls_databricks`
   - **Azure subscription:** yours
   - **Databricks workspace:** `dbw-ev-intelligence-dev`
   - **Select cluster:** Existing interactive cluster → select `dev-cluster`
   - **Authentication:** Managed Identity (ADF MI must have `Contributor` on Databricks workspace)

**CLI:**

```bash
DATABRICKS_URL="https://<your-workspace-id>.azuredatabricks.net"

az datafactory linked-service create \
  --resource-group $RG \
  --factory-name $ADF \
  --linked-service-name "ls_databricks" \
  --properties '{
    "type": "AzureDatabricks",
    "typeProperties": {
      "domain": "'"$DATABRICKS_URL"'",
      "authentication": "MSI",
      "workspaceResourceId": "/subscriptions/'"$SUBSCRIPTION"'/resourceGroups/'"$RG"'/providers/Microsoft.Databricks/workspaces/dbw-ev-intelligence-dev",
      "existingClusterId": "<your-cluster-id>"
    }
  }'
```

---

## Part D — Trigger the Pipeline

### Manual trigger (UI)

1. Open `pl_bronze_api_payments`
2. Click **Add trigger** → **Trigger now**
3. **p_load_type:** `full`
4. Click **OK**
5. Go to **Monitor** tab → watch the pipeline run

### Scheduled trigger (UI)

1. **Add trigger** → **New/Edit**
2. **Name:** `tr_bronze_api_payments_daily`
3. **Type:** Schedule
4. **Recurrence:** every day at `02:00 UTC`
5. **Parameters:** `p_load_type` = `incremental`
6. Click **OK** → **Publish all**

### CLI trigger

```bash
# Manual full load trigger
az datafactory pipeline create-run \
  --resource-group $RG \
  --factory-name $ADF \
  --pipeline-name "pl_bronze_api_payments" \
  --parameters '{"p_load_type": "full"}'

# Check run status
az datafactory pipeline-run query-by-factory \
  --resource-group $RG \
  --factory-name $ADF \
  --last-updated-after "2026-01-01T00:00:00Z" \
  --last-updated-before "2026-12-31T00:00:00Z"
```

---

## Verify in ADLS

After the pipeline runs, check Bronze Delta files exist:

```python
# In Databricks
display(dbutils.fs.ls(abfss("bronze", "api/payments/")))
# Should show Delta files: _delta_log/, part-*.parquet
```

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `401 on login` | Username/password secret wrong | Check `voltgrid-username` and `voltgrid-password` in Key Vault |
| `Until loop never stops` | `v_total_pages` not updating from pagination rule | Check pagination rule key path: `$.pagination.total_pages` |
| `Copy writes 0 rows` | JSON path mapping wrong — data is under `data[]` not `results[]` | In mapping, set collection reference to `$.data[*]` |
| `Delta write fails` | ADF MI missing Storage Blob Data Contributor on bronze container | Storage → IAM → add role for ADF MI |
| `Notebook activity fails` | Cluster not running | Start `dev-cluster` first, or switch to job cluster in linked service |

---

## Next Step

→ `03_ADF_PIPELINE_BLOB_SESSIONS.md` — build the blob charging sessions pipeline
