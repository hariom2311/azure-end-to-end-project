# Day 4 — Practice Exercises: ADF Incremental Pipeline

> Complete in dependency order. Exercise 1 is ADF access setup. Exercises 2–4 build the pipeline. Exercise 5 tests full and incremental runs.

---

## Exercise 1 — ADF Linked Service and Access

**Objective:** Connect ADF to Databricks and verify the linked service works.

1. Grant ADF MI `Contributor` on the Databricks workspace:
   ```bash
   ADF_MI=$(az datafactory show --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --query "identity.principalId" -o tsv)
   DBW_ID=$(az databricks workspace show --resource-group rg-ev-intelligence-dev --name dbw-ev-intelligence-dev --query id -o tsv)

   az role assignment create \
     --assignee-object-id $ADF_MI \
     --assignee-principal-type ServicePrincipal \
     --role "Contributor" \
     --scope $DBW_ID
   ```

2. In ADF Studio → **Manage** → **Linked services** → **+ New** → **Azure Databricks** (Compute category):
   - Name: `ls_databricks`
   - Workspace: `dbw-ev-intelligence-dev`
   - Select cluster: **New job cluster**
   - Node type: `Standard_DS3_v2`
   - Runtime: `15.4 LTS`
   - Auth: `System Assigned Managed Identity`

3. **Test connection** → must be green → **Create**

4. Update dataset `ds_voltgrid_payments_src` — add parameters `p_endpoint` and `p_updated_after`, update Relative URL expression per notes Part 2.

5. **Publish all**

**Verify:** Test connection on `ls_databricks` is green. Dataset preview URL for `p_endpoint=/api/db/payments/`, `p_page=1`, `p_page_size=5`, `p_updated_after=1900-01-01T00:00:00Z` contains `updated_after=1900-01-01T00%3A00%3A00Z`.

---

## Exercise 2 — Build Pipeline: Control Table Read + Variable Setup

**Objective:** Create `pl_bronze_incremental_load` and wire the first 3 steps — read control table, extract variables, set watermark/date/timestamp.

1. Create new pipeline `pl_bronze_incremental_load` with parameters and variables from notes Part 3.1 and 3.2.

2. Add `act_read_ctl` (Databricks Notebook activity):
   - Linked service: `ls_databricks`
   - Notebook path: `/Workspace/ev-project/nb_read_control_table`
   - Base parameter: `source_name` = `@pipeline().parameters.p_source_name`

3. Add 4 Set Variable activities (parallel, all depend on `act_read_ctl`):
   - `act_set_endpoint` → `v_endpoint`
   - `act_set_sink_path` → `v_sink_base_path`
   - `act_set_file_prefix` → `v_file_prefix`
   - `act_set_page_size` → `v_page_size`
   (Use `@json(activity('act_read_ctl').output.runOutput).fieldname` for each)

4. Add `act_set_watermark` with the full `@if(...)` expression from notes Step 3.

5. Add `act_set_ingestion_date` and `act_set_run_ts`.

6. **Publish all**

7. **Trigger Now** with `p_source_name = payments`, `p_load_type_override = full` — let it run up to `act_set_run_ts` only (you can add a temporary failure activity after to stop early for testing).

**Verify:** In Monitor → `act_read_ctl` shows Succeeded. Click the activity → **Output** → confirm `runOutput` contains the expected JSON string. Confirm `v_last_watermark` in `act_set_watermark` output is `1900-01-01T00:00:00Z`.

---

## Exercise 3 — Build Pipeline: Auth + Page 1 Fetch

**Objective:** Add VoltGrid authentication and the page 1 Web Activity that captures total_pages.

1. Add `act_get_username` and `act_get_password` (parallel Web Activities to Key Vault):
   - Auth type: MSI, resource: `https://vault.azure.net`
   - URLs from notes Part 3.3 Step 4

2. Add `act_api_login` → `act_set_token` (same as Day 2 pipeline).

3. Add `act_fetch_page1_meta` (Web Activity):
   - URL: concatenate base URL + `v_endpoint` + query params including `encodeUriComponent(v_last_watermark)`
   - Header: `Authorization` = `@concat('Token ', variables('v_token'))`

4. Add `act_set_total_pages`:
   ```
   @int(activity('act_fetch_page1_meta').output.pagination.total_pages)
   ```

5. Add `act_copy_page1` (Copy Activity) — write page 1 to Bronze with file name `payments_page_001.json`.

6. Add `act_set_current_page_2`:
   ```
   Variable: v_current_page
   Value: 2
   ```

7. **Publish all** → **Trigger Now** (full run)

**Verify:** `act_fetch_page1_meta` output contains `pagination.total_pages` (e.g. 321). `act_copy_page1` succeeded. Check ADLS:
```bash
az storage blob list --account-name evdatalakedev2 --container-name bronze --prefix "api/payments/" --auth-mode login --query "[].name" -o tsv
```
Expected: one file `payments_page_001.json`.

---

## Exercise 4 — Build Pipeline: Until Loop + Watermark Update

**Objective:** Add the pagination loop and watermark update to complete the pipeline.

1. Add **Until** activity → connect from `act_set_current_page_2`

2. Until expression:
   ```
   @greaterOrEquals(variables('v_current_page'), add(variables('v_total_pages'), 1))
   ```

3. Click Until → **Edit** (pencil icon) → add inside the loop:
   - `act_copy_page` (Copy Activity) — same source/sink as page 1 but `p_page = @variables('v_current_page')`
   - `act_increment_page` (Set Variable) — `v_current_page = @add(variables('v_current_page'), 1)`
   - Connect `act_copy_page` → `act_increment_page`

4. After Until (On Success), add `act_update_watermark` (Databricks Notebook):
   - Notebook: `/Workspace/ev-project/nb_update_watermark`
   - Parameters: `source_name` = `@pipeline().parameters.p_source_name`, `new_watermark` = `@variables('v_run_ts')`

5. Add `act_alert_failure` (Web Activity, On Failure from `act_copy_page` and `act_copy_page1`)

6. **Publish all**

**Verify:** Trigger Now with `p_load_type_override = full`. Watch Monitor:
- Until loop shows N iterations (one per page 2 to total_pages)
- `act_update_watermark` succeeds

Check ADLS — all files from `payments_page_001.json` to `payments_page_NNN.json`:
```bash
az storage blob list --account-name evdatalakedev2 --container-name bronze --prefix "api/payments/ingestion_date=" --auth-mode login --query "[].name" -o tsv | wc -l
```

Check control table:
```sql
SELECT source_name, load_type, last_watermark, last_run_ts
FROM ev_catalog.ctl.pipeline_control;
```
Expected: `load_type = 'incremental'`, `last_watermark` is today's UTC timestamp.

---

## Exercise 5 — Test Incremental Run and Trigger

**Objective:** Verify the pipeline is truly incremental, idempotent, and the trigger fires correctly.

1. Check current watermark:
   ```sql
   SELECT source_name, load_type, last_watermark, last_run_ts
   FROM ev_catalog.ctl.pipeline_control;
   ```

2. Run incremental (Trigger Now, leave `p_load_type_override` empty):
   - `total_pages` should be much smaller than the full run (only records changed since the watermark)
   - New files land in `ingestion_date=<today>/` — potentially just 1–2 pages

3. Re-run the same incremental immediately:
   - Same `updated_after` → same data → same file names → overwrites (safe)
   - Row count in ADLS unchanged

4. Force full reload:
   - Trigger Now with `p_load_type_override = full`
   - Verify `total_pages` matches the original full run page count

5. Set up the Tumbling Window trigger per notes Part 5. Activate it.

**Verify:**
- Incremental run produces fewer files than full run
- Re-running same incremental produces identical files (idempotent)
- Full override fetches all pages regardless of watermark
- Trigger appears in Monitor as scheduled for the next window

---
