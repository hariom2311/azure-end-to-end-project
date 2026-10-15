# Day 3 — Practice Exercises: Unity Catalog Setup + Control Table

> Complete in dependency order. All three exercises are Databricks-only. ADF pipeline build is Day 4.

---

## Exercise 1 — Access Connector and Unity Catalog Credentials

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
   Save this — you paste it into Databricks in the next step.

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

4. Wait 2 minutes, then in Databricks Workspace → **Catalog** → **External Data** → **Credentials** → **Create credential**:
   - Radio: `Storage Credential`
   - Type: `Azure Managed Identity`
   - Name: `mi-ev-credential`
   - Access connector ID: paste Resource ID from step 2

5. Create External Location:
   - Name: `ext-loc-ev-bronze`
   - URL: `abfss://bronze@evdatalakedev2.dfs.core.windows.net/`
   - Credential: `mi-ev-credential`
   - Click **Test connection**

**Verify:** Test connection shows ✅ Read, ✅ List, ✅ Write, ✅ Hierarchical Namespace Enabled.

---

## Exercise 2 — Create Catalog, Schema, and Control Table

**Objective:** Create the Delta control table that will drive the ADF pipeline in Day 4.

1. In a Databricks notebook (SQL cell), create catalog and schema:
   ```sql
   CREATE CATALOG IF NOT EXISTS ev_catalog;
   USE CATALOG ev_catalog;
   CREATE SCHEMA IF NOT EXISTS ctl
   COMMENT 'Control tables — pipeline state, watermarks, config';
   ```

2. Create the control table as an External Delta table:
   ```sql
   CREATE TABLE IF NOT EXISTS ev_catalog.ctl.pipeline_control (
       source_name    STRING       NOT NULL,
       endpoint       STRING       NOT NULL,
       sink_base_path STRING       NOT NULL,
       file_prefix    STRING       NOT NULL,
       page_size      INT          NOT NULL,
       load_type      STRING       NOT NULL,
       last_watermark STRING       NOT NULL,
       last_run_ts    TIMESTAMP,
       is_active      BOOLEAN      NOT NULL,
       CONSTRAINT pk_source_name PRIMARY KEY (source_name)
   )
   USING DELTA
   LOCATION 'abfss://bronze@evdatalakedev2.dfs.core.windows.net/ctl/pipeline_control'
   COMMENT 'ADF ingestion control table — drives endpoint, path, watermark per source';
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

## Exercise 3 — Create and Test Control Notebooks

**Objective:** Build and manually test the two Databricks notebooks ADF will call in Day 4.

**Part A — nb_read_control_table:**

1. Create a notebook in Databricks Workspace → `/Workspace/ev-project/nb_read_control_table`
2. Add the code from notes Part 3.4
3. Test it manually — add a cell at the top:
   ```python
   dbutils.widgets.text("source_name", "payments")
   ```
   Run all cells. Expected output:
   ```json
   {"endpoint": "/api/db/payments/", "sink_base_path": "api/payments", "file_prefix": "payments", "page_size": 1000, "load_type": "full", "last_watermark": "1900-01-01T00:00:00Z"}
   ```

**Part B — nb_update_watermark:**

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

3. Reset for Day 4:
   ```sql
   UPDATE ev_catalog.ctl.pipeline_control
   SET load_type = 'full', last_watermark = '1900-01-01T00:00:00Z', last_run_ts = null
   WHERE source_name = 'payments';
   ```

**Verify:** Both notebooks exit without error. Control table is reset to `load_type = 'full'` and `last_watermark = '1900-01-01T00:00:00Z'` before Day 4.

---
