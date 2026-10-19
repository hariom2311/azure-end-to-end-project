# Day 7 — Practice Exercises: Pipeline Idempotency + Blob Storage Ingestion

> Complete in dependency order. Exercises 1–2 set up the idempotency fix. Exercises 3–5 build the blob ingestion pipeline.

---

## Exercise 1 — Create `pl_bronze_incremental_load_v3` (Idempotency Fix)

**Objective:** Clone `pl_bronze_incremental_load_v2` and add RunId isolation to both Copy activities.

**Option A — Import JSON (recommended):**

ADF Studio → Author → Pipelines → **⋮** → **Import from pipeline JSON**
Import: `pipelines/pl_bronze_incremental_load_v3.json`
Publish all.

**Option B — Manual clone:**

1. ADF Studio → Author → Pipelines → `pl_bronze_incremental_load_v2` → **⋮** → **Clone**
2. Rename: `pl_bronze_incremental_load_v3`
3. Click `Copy data1` → **Sink** tab → `p_folder` → **Open dynamic content**

   Change from:
   ```
   @concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'))
   ```
   To:
   ```
   @concat(variables('v_sink_base_path'), '/ingestion_date=', variables('v_ingestion_date'), '/run_id=', pipeline().RunId)
   ```

4. Click **Until** loop → **pencil (Edit)** → click `act_copy_page` → **Sink** tab → `p_folder` → same change as step 3
5. Back arrow → **Publish all**

**Trigger and verify:**

```
Trigger Now → p_source_name: payments, p_load_type_override: full
```

In ADLS verify:
```bash
az storage blob list \
  --account-name evdatalakedev2 \
  --container-name bronze \
  --prefix "api/payments/ingestion_date=" \
  --auth-mode login \
  --query "[].name" -o tsv | head -5
```

Expected path format: `api/payments/ingestion_date=2026-10-19/run_id=<guid>/payments_page_1.json`

**Run a second time immediately:**
Trigger Now again (same parameters) → monitor → confirm a **new `run_id=<different-guid>` subfolder** is created. First run's files untouched.

---

## Exercise 2 — Update Day 6 Master Pipeline to Use v3

**Objective:** Switch `pl_bronze_master_foreach` and `pl_bronze_master_parallel` to call `pl_bronze_incremental_load_v3` instead of `v2`.

1. Open `pl_bronze_master_foreach` → click `act_foreach_source` → **Settings** → inside ForEach, click `act_execute_child` (Execute Pipeline activity)
2. Under **Invoked pipeline** → change from `pl_bronze_incremental_load_v2` to `pl_bronze_incremental_load_v3`
3. Repeat for `pl_bronze_master_parallel`
4. **Publish all**

**Verify:** Trigger Now on `pl_bronze_master_foreach` → in Monitor → drill into a child run → confirm its sink path contains `run_id=`.

---

## Exercise 3 — Create Linked Service for Source Blob Storage

**Objective:** Create `ls_blob_source` in ADF to connect to `dataenggdailystorage/source` via SAS.

**Option A — Import JSON:**

ADF Studio → Manage → Linked services → **+ New** → from pipeline JSON menu or via JSON import.
Import: `linked-services/ls_blob_source.json`

> If ADF does not support linked service JSON import directly: see Option B.

**Option B — Create manually:**

1. ADF Studio → **Manage** → **Linked services** → **+ New**
2. Search: `Azure Blob Storage` → **Continue**
3. Fill in:

   | Field | Value |
   |---|---|
   | Name | `ls_blob_source` |
   | Authentication method | SAS URI |
   | SAS URL | `https://dataenggdailystorage.blob.core.windows.net/source?sp=rl&st=2026-10-19T14:19:08Z&se=2027-10-19T22:34:08Z&spr=https&sv=2026-02-06&sr=c&sig=htxUXkd2knSowSujglmMYRoGWHQkCbXG2fh7bmi91No%3D` |

4. **Test connection** → must be green
5. **Create** → **Publish all**

**Verify:** Test connection green. In ADF data preview (any dataset using this linked service), verify you can browse the `source` container.

---

## Exercise 4 — Create Source Datasets and Sink Dataset

**Objective:** Create four datasets — three sources (XML, Binary, JSON) and one Binary sink.

**Import all four JSON files:**
- `datasets/ds_blob_config_src.json`
- `datasets/ds_blob_invoices_src.json`
- `datasets/ds_blob_reports_src.json`
- `datasets/ds_bronze_blob_sink.json`

ADF Studio → Author → **Datasets** → **⋮** → Import from dataset JSON (repeat for each).

**Or create manually:**

**`ds_blob_config_src`:**
1. New dataset → Azure Blob Storage → XML
2. Linked service: `ls_blob_source`
3. Container: `source`, Folder: `config`
4. Name: `ds_blob_config_src`

**`ds_blob_invoices_src`:**
1. New dataset → Azure Blob Storage → Binary
2. Linked service: `ls_blob_source`
3. Container: `source`
4. Add parameter: `p_date` (String)
5. Folder path (dynamic): `@concat('invoices/', dataset().p_date)`
6. Name: `ds_blob_invoices_src`

**`ds_blob_reports_src`:**
1. New dataset → Azure Blob Storage → JSON
2. Linked service: `ls_blob_source`
3. Container: `source`
4. Add parameter: `p_year_month` (String)
5. Folder path (dynamic): `@concat('reports/', dataset().p_year_month)`
6. Name: `ds_blob_reports_src`

**`ds_bronze_blob_sink`:**
1. New dataset → Azure Data Lake Storage Gen2 → Binary
2. Linked service: `ls_adls_bronze`
3. File system: `bronze`
4. Add parameter: `p_folder` (String)
5. Folder path (dynamic): `@dataset().p_folder`
6. Name: `ds_bronze_blob_sink`

**Publish all.**

**Verify:** For `ds_blob_config_src` → **Preview data** → should list XML files. If preview fails with 403, check SAS token is correctly pasted.

---

## Exercise 5 — Build and Test `pl_bronze_blob_ingestion`

**Objective:** Create the pipeline with three parallel copy activities, schedule it, and verify Bronze output.

**Option A — Import JSON:**

ADF Studio → Pipelines → **⋮** → **Import from pipeline JSON**
Import: `pipelines/pl_bronze_blob_ingestion.json`
Publish all.

**Option B — Build manually:**

1. Create new pipeline: `pl_bronze_blob_ingestion`
2. Add **Copy data** activity: `act_copy_config`
   - Source dataset: `ds_blob_config_src`
   - Source file filter: `*.xml`
   - Sink dataset: `ds_bronze_blob_sink`
   - Sink `p_folder`:
     ```
     @concat('blob/config/ingestion_date=', formatDateTime(utcnow(), 'yyyy-MM-dd'), '/run_id=', pipeline().RunId)
     ```
   - Retry: 2, interval: 60s

3. Add **Copy data** activity: `act_copy_invoices` (no dependency on step 2 — runs in parallel)
   - Source dataset: `ds_blob_invoices_src`
   - Source `p_date`:
     ```
     @formatDateTime(addDays(utcnow(), -1), 'yyyy/MM/dd')
     ```
   - Source file filter: `*.pdf`
   - Sink dataset: `ds_bronze_blob_sink`
   - Sink `p_folder`:
     ```
     @concat('blob/invoices/ingestion_date=', formatDateTime(addDays(utcnow(), -1), 'yyyy-MM-dd'), '/run_id=', pipeline().RunId)
     ```
   - Retry: 2, interval: 60s

4. Add **Copy data** activity: `act_copy_reports` (no dependency — parallel)
   - Source dataset: `ds_blob_reports_src`
   - Source `p_year_month`:
     ```
     @formatDateTime(utcnow(), 'yyyy/MM')
     ```
   - Source file filter: `*.json`
   - Sink dataset: `ds_bronze_blob_sink`
   - Sink `p_folder`:
     ```
     @concat('blob/reports/ingestion_date=', formatDateTime(utcnow(), 'yyyy-MM-dd'), '/run_id=', pipeline().RunId)
     ```
   - Retry: 2, interval: 60s

5. **Publish all**

**Test run:**

```
ADF Studio → pl_bronze_blob_ingestion → Trigger Now (no parameters)
```

Watch Monitor — all three activities should turn green in parallel (~30–60 seconds each).

**Verify ADLS Bronze output:**

```bash
# Config XML files
az storage blob list \
  --account-name evdatalakedev2 \
  --container-name bronze \
  --prefix "blob/config/" \
  --auth-mode login \
  --query "[].name" -o tsv

# Invoice PDFs (yesterday's date)
az storage blob list \
  --account-name evdatalakedev2 \
  --container-name bronze \
  --prefix "blob/invoices/" \
  --auth-mode login \
  --query "[].name" -o tsv

# Report JSON files
az storage blob list \
  --account-name evdatalakedev2 \
  --container-name bronze \
  --prefix "blob/reports/" \
  --auth-mode login \
  --query "[].name" -o tsv
```

Expected structure:
```
bronze/blob/config/ingestion_date=2026-10-19/run_id=<guid>/connector_standards.xml
bronze/blob/config/ingestion_date=2026-10-19/run_id=<guid>/states.xml
... (19 XML files)

bronze/blob/invoices/ingestion_date=2026-10-18/run_id=<guid>/INV-AU-2026-0002266.pdf
... (all PDFs from 2026/10/18 folder)

bronze/blob/reports/ingestion_date=2026-10-19/run_id=<guid>/kpi_report_202610.json
bronze/blob/reports/ingestion_date=2026-10-19/run_id=<guid>/sla_report_202610.json
bronze/blob/reports/ingestion_date=2026-10-19/run_id=<guid>/state_breakdown_202610.json
```

**Test idempotency — run a second time:**

Trigger Now again. A new `run_id=<different-guid>` subfolder is created for each source type. First run's files are untouched. Total file count doubles.

**Schedule the pipeline:**

Import `triggers/tr_blob_ingestion_daily.json`:
ADF Studio → Manage → Triggers → **+ New** → import JSON
Or create manually:
- Type: Schedule
- Start: `2026-10-20T02:00:00Z`
- Frequency: Daily at 02:00 UTC
- Pipeline: `pl_bronze_blob_ingestion`

**Activate trigger** (click the toggle) → **Publish all**

**Verify trigger:** In Monitor → Trigger runs → `tr_blob_ingestion_daily` should appear as scheduled for the next run at 02:00 UTC.

---

## Bonus — Add Key Vault Reference for SAS Token

**Objective:** Move the SAS URI from the linked service to Azure Key Vault so renewal requires only a secret update, not an ADF redeployment.

1. Azure Portal → Key Vault (`kv-ev-intelligence-dev`) → Secrets → **+ Generate/Import**
   - Name: `ls-blob-source-sas-uri`
   - Value: the full SAS URL (including `https://...?sp=rl&...`)

2. Grant ADF MI access to the secret:
   ```bash
   ADF_MI=$(az datafactory show --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --query "identity.principalId" -o tsv)
   KV_ID=$(az keyvault show --name kv-ev-intelligence-dev --query id -o tsv)
   az role assignment create \
     --assignee-object-id $ADF_MI \
     --assignee-principal-type ServicePrincipal \
     --role "Key Vault Secrets User" \
     --scope $KV_ID
   ```

3. ADF Studio → Manage → Linked services → `ls_blob_source` → **Edit**
   - SAS URI → click **Azure Key Vault** tab
   - AKV linked service: `ls_key_vault`
   - Secret name: `ls-blob-source-sas-uri`
   - **Test connection** → green → **Save** → **Publish all**

**Verify:** Test connection on the updated `ls_blob_source` → green. Run `pl_bronze_blob_ingestion` → pipeline succeeds and ADF never sees the raw SAS value — Key Vault injects it at runtime.

When the vendor issues a new SAS token: update the Key Vault secret only. No ADF changes, no Publish All.

---
