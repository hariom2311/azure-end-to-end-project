# Day 3 â€” Practice Exercises: Bronze Incremental Load

> Complete in dependency order. Parts 1â€“3 are Databricks setup. Parts 4â€“6 are ADF. Do not start ADF until the control table is verified.

---

## Exercise 1 â€” Access Connector and Unity Catalog Credentials

**Objective:** Create the Azure resource that gives Databricks credential-free access to ADLS Gen2.

1. Create the Access Connector:
   ```bash
   az databricks access-connector create \
     --resource-group rg-ev-intelligence-dev \
     --name ac-ev-databricks-02 \
     --location centralindia \
     --identity-type SystemAssigned
   ```

2. Copy the Resource ID:
   ```bash
   az resource show \
     --resource-group rg-ev-intelligence-dev \
     --resource-type "Microsoft.Databricks/accessConnectors" \
     --name ac-ev-databricks-02 \
     --query id -o tsv
   ```
   Save this â€” you paste it into Databricks in the next step.

3. Assign `Storage Blob Data Contributor` to the Access Connector MI:
   ```bash
   STORAGE_ID=$(az storage account show --name evdatalakedev2 --resource-group rg-ev-intelligence-dev --query id -o tsv)
   AC_MI=$(az resource show --resource-group rg-ev-intelligence-dev --resource-type "Microsoft.Databricks/accessConnectors" --name ac-ev-databricks-02 --query "identity.principalId" -o tsv)

   az role assignment create \
     --assignee-object-id $AC_MI \
     --assignee-principal-type ServicePrincipal \
     --role "Storage Blob Data Contributor" \
     --scope $STORAGE_ID
   ```

4. Wait 2 minutes, then in Databricks Workspace â†’ **Catalog** â†’ **External Data** â†’ **Credentials** â†’ **Create credential**:
   - Radio: `Storage Credential`
   - Type: `Azure Managed Identity`
   - Name: `mi-ev-credential`
   - Access connector ID: paste Resource ID from step 2

5. Create External Location:
   - Name: `ext-loc-ev-bronze`
   - URL: `abfss://bronze@evdatalakedev2.dfs.core.windows.net/`
   - Credential: `mi-ev-credential`
   - Click **Test connection**

**Verify:** Test connection shows âœ… Read, âœ… List, âœ… Write, âœ… Hierarchical Namespace Enabled.

---

## Exercise 2 â€” Create Catalog, Schema, and Control Table

**Objective:** Create the Delta control table that drives the ADF pipeline.

1. In a Databricks notebook (SQL cell), create catalog and schema:
   ```sql
   CREATE CATALOG IF NOT EXISTS ev_catalog;
   USE CATALOG ev_catalog;
   CREATE SCHEMA IF NOT EXISTS ctl
   COMMENT 'Control tables â€” pipeline state, watermarks, config';
   ```

2. Create the control table as an External Delta table:
   ```sql
   CREATE TABLE IF NOT EXISTS ev_catalog.ctl.pipeline_control (
       source_name    STRING    NOT NULL,
       endpoint       STRING    NOT NULL,
       sink_base_path STRING    NOT NULL,
       file_prefix    STRING    NOT NULL,
       page_size      INT       NOT NULL,
       load_type      STRING    NOT NULL DEFAULT 'full',
       last_watermark STRING    NOT NULL DEFAULT '1900-01-01T00:00:00Z',
       last_run_ts    TIMESTAMP,
       is_active      BOOLEAN   NOT NULL DEFAULT true,
       CONSTRAINT pk_source_name PRIMARY KEY (source_name)
   )
   USING DELTA
   LOCATION 'abfss://bronze@evdatalakedev2.dfs.core.windows.net/ctl/pipeline_control'
   COMMENT 'ADF ingestion control table';
   ```

3. Insert the payments row:
   ```sql
   INSERT INTO ev_catalog.ctl.pipeline_control
       (source_name, endpoint, sink_base_path, file_prefix, page_size, load_type, last_watermark, is_active)
   VALUES
       ('payments', '/api/db/payments/', 'api/payments', 'payments', 1000, 'full', '1900-01-01T00:00:00Z', true);
   ```

4. Verify:
   ```sql
   SELECT * FROM ev_catalog.ctl.pipeline_control;
   ```

5. Verify the Delta files exist in ADLS:
   ```bash
   az storage blob list \
     --account-name evdatalakedev2 \
     --container-name bronze \
     --prefix "ctl/pipeline_control/" \
     --auth-mode login \
     --query "[].name" -o tsv
   ```

**Verify:** `SELECT` returns 1 row with `last_watermark = '1900-01-01T00:00:00Z'` and `load_type = 'full'`. CLI shows `_delta_log/` and at least one `.parquet` file under `ctl/pipeline_control/`.

---

## Exercise 3 â€” Create and Test Control Notebooks

**Objective:** Build and manually test the two Databricks notebooks ADF will call.

**Part A â€” nb_read_control_table:**

1. Create a notebook in Databricks Workspace â†’ `/Workspace/ev-project/nb_read_control_table`
2. Add the code from notes Part 3.4
3. Test it manually â€” add a cell:
   ```python
   dbutils.widgets.text("source_name", "payments")
   ```
   Run all cells. Expected output:
   ```json
   {"endpoint": "/api/db/payments/", "sink_base_path": "api/payments", "file_prefix": "payments", "page_size": 1000, "load_type": "full", "last_watermark": "1900-01-01T00:00:00Z"}
   ```

**Part B â€” nb_update_watermark:**

1. Create `/Workspace/ev-project/nb_update_watermark` with code from notes Part 3.5
2. Test manually:
   ```python
   dbutils.widgets.text("source_name", "payments")
   dbutils.widgets.text("new_watermark", "2026-10-14T00:00:00Z")
   ```
   Run all cells, then verify:
   ```sql
   SELECT source_name, last_watermark, load_type FROM ev_catalog.ctl.pipeline_control;
   ```
   Expected: `last_watermark = '2026-10-14T00:00:00Z'`, `load_type = 'incremental'`

3. Reset for the ADF test:
   ```sql
   UPDATE ev_catalog.ctl.pipeline_control
   SET load_type = 'full', last_watermark = '1900-01-01T00:00:00Z', last_run_ts = null
   WHERE source_name = 'payments';
   ```

**Verify:** Both notebooks exit without error. Control table watermark resets to `1900-01-01T00:00:00Z`.

---

## Exercise 4 â€” ADF Linked Service and Dataset Updates

**Objective:** Wire ADF to Databricks and update the source dataset.

1. In ADF Studio â†’ **Manage** â†’ **Linked services** â†’ **+ New** â†’ `Azure Databricks` (Compute category):
   - Name: `ls_databricks`
   - Workspace: `dbw-ev-intelligence-dev`
   - Select cluster: `New job cluster`
   - Auth: `System Assigned Managed Identity`
   - Test connection â†’ green

2. Grant ADF MI `Contributor` on Databricks workspace:
   ```bash
   ADF_MI=$(az datafactory show --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --query "identity.principalId" -o tsv)
   DBW_ID=$(az databricks workspace show --resource-group rg-ev-intelligence-dev --name dbw-ev-intelligence-dev --query id -o tsv)

   az role assignment create \
     --assignee-object-id $ADF_MI \
     --assignee-principal-type ServicePrincipal \
     --role "Contributor" \
     --scope $DBW_ID
   ```

3. Update `ds_voltgrid_payments_src` â€” add 2 new parameters:
   - `p_endpoint` â€” String â€” default: `/api/db/payments/`
   - `p_updated_after` â€” String â€” default: `1900-01-01T00:00:00Z`

4. Update the dataset's Connection â†’ Relative URL:
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

5. **Publish all**

**Verify:** Dataset preview URL for `p_endpoint=/api/db/payments/`, `p_page=1`, `p_page_size=5`, `p_updated_after=1900-01-01T00:00:00Z` resolves to:
```
/api/db/payments/?page=1&page_size=5&updated_after=1900-01-01T00%3A00%3A00Z
```

---

## Exercise 5 â€” Build and Run pl_bronze_incremental_load

**Objective:** Create the full pipeline with control table read, pagination loop, and watermark update.

1. Create pipeline `pl_bronze_incremental_load` with parameters and variables from notes Part 5.1
2. Build all activities following notes Part 5.3 Steps 1â€“9
3. Publish all
4. Test with Trigger Now: `p_source_name = payments`, `p_load_type_override = full`
5. Monitor â†’ watch all activities:
   - `act_read_ctl` â†’ Succeeded, output contains JSON
   - `act_fetch_page1_meta` â†’ Succeeded, `total_pages` in output
   - Until loop â†’ runs N times (one per remaining page)
   - `act_update_watermark` â†’ Succeeded
6. Verify files in ADLS:
   ```bash
   az storage blob list \
     --account-name evdatalakedev2 \
     --container-name bronze \
     --prefix "api/payments/ingestion_date=" \
     --auth-mode login \
     --query "[].{name:name, size:properties.contentLength}" \
     -o table
   ```

**Verify:**
- Files named `payments_page_001.json` through `payments_page_NNN.json`
- File size > 0 for all pages (last page may be smaller)
- Control table: `load_type = 'incremental'`, `last_watermark` is today's UTC timestamp

---

## Exercise 6 â€” Test Incremental Run

**Objective:** Confirm the pipeline is truly incremental and idempotent.

1. Check current watermark:
   ```sql
   SELECT source_name, load_type, last_watermark, last_run_ts
   FROM ev_catalog.ctl.pipeline_control;
   ```

2. Run the pipeline again (Trigger Now) â€” leave `p_load_type_override` empty (incremental):
   - `act_fetch_page1_meta` will return a different (smaller) `total_pages` â€” only records updated after the watermark
   - New files land in a new `ingestion_date=YYYY-MM-DD/` partition

3. Re-run the same pipeline a second time immediately (same `ingestion_date`):
   - Files overwrite the existing ones in the same partition
   - Same `updated_after` â†’ same data â†’ same files â†’ safe overwrite

4. Check ADLS â€” should see 2 dated partitions:
   ```bash
   az storage blob list \
     --account-name evdatalakedev2 \
     --container-name bronze \
     --prefix "api/payments/" \
     --auth-mode login \
     --query "[].name" -o tsv
   ```

5. Force a full reload:
   - Trigger Now with `p_load_type_override = full`
   - Verify watermark is reset to `1900-01-01` before running... wait, the watermark reset is done via `v_last_watermark` logic â€” full override uses epoch watermark without changing control table until success
   - Verify `total_pages` matches the full dataset page count

**Verify:**
- Incremental run produces fewer files than full run (fewer records changed since last run)
- Re-running same incremental run produces identical files (idempotent)
- Full override re-fetches all data regardless of watermark

---
