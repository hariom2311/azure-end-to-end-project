# Day 8 — Bronze to Silver: API Data (Payments)

## Overview

This day covers the Bronze to Silver transformation for API-ingested payment data.
The Silver layer applies business rules (dedup, timezone, GST split, billing reconciliation),
enforces schema, routes bad records to quarantine, and writes idempotently using Delta MERGE.

---

## Part 1: Architecture

### Medallion recap

```
REST API (VoltGrid)
    |  (Day 3/4 ADF pipeline)
    v
Bronze  — raw JSON, append-only, partitioned by ingestion_date / run_id
    |  (Day 8 Databricks notebook)
    v
Silver  — cleansed, enriched, deduplicated Delta tables, MERGE upsert
    |
    v
Gold    — aggregated facts/dims for reporting
```

### Storage layout

| Layer  | Container | ADLS path                                           | Databricks schema  |
|--------|-----------|-----------------------------------------------------|--------------------|
| Bronze | bronze    | abfss://bronze@evdatalakedev2.dfs.core.windows.net/ | ev_catalog.bronze  |
| Silver | silver    | abfss://silver@evdatalakedev2.dfs.core.windows.net/ | ev_catalog.silver  |

---

## Part 2: Prerequisites — Silver External Location Setup

> **This must be completed before running any Silver notebook.**
> Skipping this causes the error below when the notebook tries to write to ADLS:
>
> `[AZURE_INVALID_CREDENTIALS_CONFIGURATION] Failure to initialize configuration
> for storage account evdatalakedev2.dfs.core.windows.net: Invalid configuration
> value detected for fs.azure.account.key. SQLSTATE: 42KDK`
>
> **Root cause:** Databricks does not have permission to write to the silver container.
> The fix is to register an External Location pointing to the silver container using
> the same Managed Identity credential that already works for bronze.

### Step 1 — Verify the bronze credential works (you already have this)

In Databricks go to: **Catalog > External Data > Storage Credentials**

You should see `mi-ev-credential` listed. This is the Managed Identity credential
that grants access to `evdatalakedev2` storage account. Silver uses the same one.

### Step 2 — Create the Silver External Location

Go to: **Catalog > External Data > External Locations > + Create**

| Field                  | Value                                                 |
|------------------------|-------------------------------------------------------|
| External location name | `ext-loc-ev-silver`                                   |
| URL                    | `abfss://silver@evdatalakedev2.dfs.core.windows.net/` |
| Storage credential     | `mi-ev-credential`                                    |

Click **Create**, then click **Test connection** — it must show green before continuing.

> If the silver container does not exist yet, create it first in Azure Portal:
> Storage account `evdatalakedev2` > Containers > + Container > name: `silver`
> (same settings as bronze — private access, hierarchical namespace enabled)

### Step 3 — Create the Silver Schema

Run this in a Databricks SQL cell or notebook:

```sql
CREATE SCHEMA IF NOT EXISTS ev_catalog.silver
MANAGED LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/';
```

Verify it was created:

```sql
DESCRIBE SCHEMA ev_catalog.silver;
-- Location column should show: abfss://silver@evdatalakedev2.dfs.core.windows.net/
```

### Step 3b — Create the Quarantine Schema

The quarantine path lives inside the silver container, so it uses the same `ext-loc-ev-silver`
external location. Register it as its own schema so bad rows are visible in Databricks Catalog:

```sql
CREATE SCHEMA IF NOT EXISTS ev_catalog.quarantine
MANAGED LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/';
```

After running each Silver notebook at least once (so Delta files are written to the quarantine path),
register the quarantine table so it appears in Databricks Catalog. Run the block below for all entities:

```sql
-- Day 8 — Payments
CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.payments
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/payments/';

-- Day 9 — all remaining API entities
CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.charge_cards
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/charge_cards/';

CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.chargers
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/chargers/';

CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.cities
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/cities/';

CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.complaints
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/complaints/';

CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.customers
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/customers/';

CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.employees
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/employees/';

CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.energy_prices
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/energy_prices/';

CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.maintenance_events
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/maintenance_events/';

CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.partners
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/partners/';

CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.pipeline_audit
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/pipeline_audit/';

CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.sessions
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/sessions/';

CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.states
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/states/';

CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.stations
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/stations/';

CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.tariffs
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/tariffs/';

CREATE TABLE IF NOT EXISTS ev_catalog.quarantine.vehicles
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/vehicles/';
```

> **When to run this:** Run each `CREATE TABLE` statement only after the corresponding Silver notebook
> has executed at least once. If the quarantine path contains no Delta files yet, the statement will
> fail with `Path does not exist` — that is expected and means no bad rows were rejected for that entity.

Verify in Catalog: **ev_catalog > quarantine** — each registered table appears with
`rejection_reason` and `quarantine_ts` columns alongside the original entity fields.

To query bad rows for any entity:
```sql
SELECT rejection_reason, COUNT(*) AS cnt, MAX(quarantine_ts) AS latest
FROM ev_catalog.quarantine.payments     -- swap table name for other entities
GROUP BY rejection_reason
ORDER BY cnt DESC;
```

### Step 4 — Grant permission (Unity Catalog)

If your Databricks workspace uses Unity Catalog (which it does — ev_catalog exists),
run this to allow your user to create external tables:

```sql
GRANT CREATE EXTERNAL TABLE
ON EXTERNAL LOCATION `ext-loc-ev-silver`
TO `hariomsuryawanshi68258@gmail.com`;
```

Replace the email with each student's Databricks login if running in a shared workspace.

### Why this is different from bronze

Bronze data is read via a **Volume** (`/Volumes/ev_catalog/bronze/api/...`) which
bypasses ADLS credentials — Databricks manages volume access internally.
Silver tables write directly to ADLS using `abfss://` paths, so the external location
credential must exist. This is the same pattern used for all external Delta tables.

---

## Part 3: Bronze Source — Real API Response Structure

Each JSON file in Bronze is one API page. The structure is:

```json
{
  "data": [
    {
      "id": 1343318,
      "payment_id": "PAY-AU-TRX-00559391",
      "session_id": "SESS-20250726-00559391",
      "customer_id": "CUST-AU-2025-007275",
      "gateway": "Braintree",
      "amount_aud": "617.56",
      "gst": "61.76",
      "payment_mode": "CreditCard",
      "status": "Disputed",
      "processed_at": "2026-10-09T15:56:00Z",
      "created_at": "2026-10-15T15:56:42.350422Z",
      "updated_at": "2026-10-15T15:56:42.350429Z"
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 1000,
    "total": 320221,
    "total_pages": 321
  }
}
```

**Key points:**
- `amount_aud` and `gst` arrive as **strings** from the API — must cast to double
- `gst` field from API = GST component already computed by source system
- Empty pages are valid: `{"data": [], "pagination": {"total": 0, ...}}` — notebook handles this safely via early exit
- The notebook must **explode the `data` array** first before any transformation

### Bronze path layout

```
/Volumes/ev_catalog/bronze/api/payments/
  ingestion_date=2026-10-16/
    run_id=4a72bb24-3828-481e-8a76-5937efc25f50/   <- ADF pipeline run UUID
      payments_page_1.json     <- { "data": [...1000 records...], "pagination": {...} }
      payments_page_2.json
      ...
      payments_page_321.json
```

> The `run_id=` subfolder name is the ADF pipeline run UUID, not the date.
> The notebook reads `ingestion_date={run_date}/` and uses `recursiveFileLookup=true`
> so it picks up all JSON files inside regardless of the UUID subfolder name.

---

## Part 4: Transformation Rules Applied

### Rule A — Deduplication
- Partition by `payment_id`, order by `updated_at DESC`
- Keep `row_number = 1` only
- Handles: ADF retries re-landing the same page, API returning overlapping records

### Rule D — Billing Reconciliation
- Flag any row where `status IN ('failed', 'Disputed')` AND `amount_aud > 0`
- Sets `billing_anomaly_flag = true`
- Records still load to Silver — not quarantined, flagged for finance review

### Rule G — Timezone Normalisation
- All timestamps arrive as UTC strings (Z suffix)
- Cast to `TIMESTAMP` (UTC), add AEST column: `from_utc_timestamp(col, 'Australia/Sydney')`
- Applied to `processed_at` only (the business-relevant timestamp)
- PySpark handles DST automatically: AEST = UTC+10, AEDT = UTC+11

### Rule J — GST Split
- `gst_amount_aud = ROUND(amount_aud / 11, 4)` — recomputed from amount for consistency
- `excl_gst_aud   = ROUND(amount_aud - gst_amount_aud, 4)`
- API's own `gst` field is dropped (replaced by our computed value)

---

## Part 5: Data Quality and Quarantine

### Quality checks (in order)

| Check            | Condition                           | rejection_reason    |
|------------------|-------------------------------------|---------------------|
| PK not null      | payment_id IS NULL                  | null_payment_id     |
| Valid amount     | amount_aud IS NULL OR <= 0          | invalid_amount      |
| Customer present | customer_id IS NULL                 | null_customer_id    |
| Timestamp present| processed_at IS NULL                | null_processed_at   |
| Status present   | status IS NULL                      | null_status         |

### Quarantine path
```
abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/payments/
```

Bad rows are written as Delta with `rejection_reason` + `quarantine_ts` appended.
The quarantine path is also under the silver container so it uses the same external location.

### Empty data handling
When a page has `"data": []`, the `explode()` step produces zero rows.
The notebook detects this with:
```python
if total_records == 0:
    dbutils.notebook.exit("NO_DATA")
```
This exits cleanly without touching Silver or quarantine.

---

## Part 6: Silver Table — sl_payments

### DDL (auto-created by notebook on first run)

```sql
CREATE TABLE IF NOT EXISTS ev_catalog.silver.sl_payments (
    id                   LONG,
    payment_id           STRING,
    session_id           STRING,
    customer_id          STRING,
    gateway              STRING,
    amount_aud           DOUBLE,
    excl_gst_aud         DOUBLE,
    gst_amount_aud       DOUBLE,
    payment_mode         STRING,
    status               STRING,
    processed_at_utc     TIMESTAMP,
    processed_at_aest    TIMESTAMP,
    created_at_utc       TIMESTAMP,
    updated_at_utc       TIMESTAMP,
    billing_anomaly_flag BOOLEAN,
    sl_ingested_at       TIMESTAMP,
    sl_batch_id          STRING
)
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/payments/';
```

### MERGE pattern (idempotent upsert)

```python
tgt = DeltaTable.forName(spark, "ev_catalog.silver.sl_payments")
tgt.alias("t")
   .merge(df_silver.alias("s"), "t.payment_id = s.payment_id")
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll()
   .execute()
```

Re-running the same `p_run_date` is safe — matched rows are overwritten, no duplicates.

---

## Part 7: Notebook — nb_silver_payments.py

### Widget parameter

| Parameter  | Default | Description                                            |
|------------|---------|--------------------------------------------------------|
| p_run_date | (blank) | YYYY-MM-DD — reads that ingestion_date/run_id partition |
|            |         | blank — reads all partitions (full historical load)    |

### Execution flow

```
Cell 1  — imports, constants (BRONZE_VOL, SILVER_TABLE, SILVER_PATH, QUARANTINE), widget p_run_date
Cell 2  — build read_path (incremental or full), read all pages with recursiveFileLookup
Cell 3  — explode data[] array, select and cast all fields; early exit if 0 records
Cell 4  — quality checks → rejection_reason; write bad rows to quarantine Delta; keep df_good
Cell 5  — Rule A: dedup by payment_id (Window, latest updated_at wins, row_number=1)
Cell 6  — Rule G: UTC timestamps → _utc and _aest columns (Australia/Sydney)
Cell 7  — Rule J: GST split — gst_amount_aud and excl_gst_aud from amount_aud
Cell 8  — Rule D: billing anomaly flag (failed/Disputed + amount > 0)
Cell 9  — add sl_ingested_at, sl_batch_id Silver metadata columns
Cell 10 — CREATE TABLE IF NOT EXISTS ev_catalog.silver.sl_payments USING DELTA
Cell 11 — MERGE upsert on payment_id (whenMatchedUpdateAll / whenNotMatchedInsertAll)
Cell 12 — run stats: merge_result.collect()[0] → inserted / updated / quarantined counts
Cell 13 — post-load SQL breakdown by status (display())
```

---

## Part 8: ADF → Databricks Permission Setup (Unity Catalog)

> **Do this BEFORE creating or running the ADF pipeline.**
> The notebook works when run directly in Databricks (your user has access),
> but fails when triggered from ADF because ADF uses a **different identity**
> (service principal or managed identity) that has not been granted Unity Catalog permissions.
>
> Error you will see in ADF activity output if you skip this step:
> ```
> [UNAUTHORIZED_ACCESS] PERMISSION_DENIED: User does not have READ VOLUME
> on Volume ev_catalog.bronze.api  SQLSTATE: 42501
> ```

### Step 1 — Find the ADF identity

In ADF Studio → **Manage > Linked services > ls_databricks** → note the auth type:
- **Access token** → the token owner is the caller identity
- **Managed Identity** → ADF's system-assigned managed identity is the caller

To confirm the exact identity, check the Databricks job run URL from the ADF error output.
Open it → go to the failed run → **Spark UI / Driver logs** → look for the line:
```
User: <identity>@<tenant>.onmicrosoft.com
```
Or run this in a Databricks notebook triggered by ADF:
```sql
SELECT current_user();
```

### Step 2 — Grant permissions in Databricks

Run the following as a **Databricks admin** (your account), replacing
`9245da2f-7129-4e0c-92c4-134fe896e294` with the actual service principal email or application ID found above:

```sql
-- Navigate catalog and schemas
GRANT USE CATALOG ON CATALOG ev_catalog
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT USE SCHEMA ON SCHEMA ev_catalog.bronze
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Read Bronze volume (fixes the UNAUTHORIZED_ACCESS error)
GRANT READ VOLUME ON VOLUME ev_catalog.bronze.api
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Write to Silver
GRANT USE SCHEMA ON SCHEMA ev_catalog.silver
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT CREATE TABLE ON SCHEMA ev_catalog.silver
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Write to Quarantine
GRANT USE SCHEMA ON SCHEMA ev_catalog.quarantine
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT CREATE TABLE ON SCHEMA ev_catalog.quarantine
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- If Silver tables already exist (created via direct notebook run),
-- grant MODIFY and SELECT explicitly on each table.
-- Without this you get:
--   PERMISSION_DENIED: User does not have MODIFY and SELECT on Table 'ev_catalog.silver.sl_payments'
--   LOCATION_OVERLAP within 'GenerateTemporaryPathCredential' call

-- Day 8
GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_payments
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Day 9 — all remaining Silver tables
GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_charge_cards
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_chargers
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_cities
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_complaints
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_customers
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_employees
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_energy_prices
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_maintenance_events
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_partners
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_pipeline_audit
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_sessions
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_states
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_stations
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_tariffs
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_vehicles
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Quarantine tables — grant after registering each table (see Part 3b)
-- Day 8
GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.payments
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Day 9
GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.charge_cards
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.chargers
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.cities
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.complaints
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.customers
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.employees
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.energy_prices
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.maintenance_events
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.partners
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.pipeline_audit
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.sessions
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.states
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.stations
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.tariffs
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

GRANT MODIFY, SELECT ON TABLE ev_catalog.quarantine.vehicles
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;

-- Tip: run table-level GRANTs only after the tables exist.
-- If a notebook has never run yet, skip that table's GRANT for now and run it after the first notebook execution.
```

### Why notebook works directly but not from ADF

| Context         | Identity used         | Has volume access by default |
|-----------------|-----------------------|------------------------------|
| Direct notebook | Your Databricks login | Yes — you set it up          |
| ADF-triggered   | ADF service principal | No — must be granted explicitly |

Unity Catalog enforces permissions per identity. Volume access granted to your user
is not inherited by the ADF service principal — each identity needs explicit grants.

---

## Part 9: ADF Pipeline — pl_silver_payments

### What it does
`pl_silver_payments` triggers `nb_silver_payments` in Databricks and passes today's
pipeline run date as `p_run_date` automatically — no manual input needed for daily runs.

### Pipeline parameter

| Parameter  | Default | Purpose |
|------------|---------|---------|
| p_run_date | (blank) | Leave blank for scheduled runs — ADF uses `TriggerTime`. Pass `YYYY-MM-DD` for manual backfill. |

### p_run_date expression (inside notebook baseParameters)

```
@if(
  empty(pipeline().parameters.p_run_date),
  formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd'),
  pipeline().parameters.p_run_date
)
```

- **Scheduled trigger:** `p_run_date` is blank → expression evaluates to today's date from `TriggerTime`
- **Manual / debug run:** pass `p_run_date = 2026-10-16` → expression uses that value (backfill)

### How to create in ADF Studio

1. ADF Studio → **Author > Pipelines > + New pipeline**
2. Name: `pl_silver_payments`
3. Add parameter: `p_run_date` (String, default blank)
4. Drag **Databricks Notebook** activity onto canvas
5. Name: `act_silver_payments`
6. **Azure Databricks tab:** Linked service = `ls_databricks`
7. **Settings tab:**
   - Notebook path: `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_payments`
   - Base parameters → `+ New`:
     - Name: `p_run_date`
     - Value (Expression): `@if(empty(pipeline().parameters.p_run_date), formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd'), pipeline().parameters.p_run_date)`
8. **Save** → **Publish**

### How to trigger manually (debug / backfill)

In ADF Studio → `pl_silver_payments` → **Add trigger > Trigger now**:
- `p_run_date` = `2026-10-16` (specific date to reprocess)
- `p_run_date` = `` (blank) → uses today

### Linking to Bronze pipeline

To run Silver automatically after Bronze loads, open the Bronze master pipeline
(e.g. `pl_bronze_master`) and add an **Execute Pipeline** activity after the
ForEach/Copy activities:

| Setting           | Value                                     |
|-------------------|-------------------------------------------|
| Pipeline          | pl_silver_payments                        |
| Wait on completion| Yes                                       |
| p_run_date        | (leave blank — TriggerTime flows through) |

---

## Part 10: Common Errors and Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `LOCATION_OVERLAP` / `PERMISSION_DENIED: User does not have MODIFY and SELECT on Table ev_catalog.silver.sl_payments` | Table already exists from a direct notebook run but ADF service principal has no table-level permission | Run `GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_payments TO \`9245da2f-7129-4e0c-92c4-134fe896e294\`` in Databricks |
| `AZURE_INVALID_CREDENTIALS_CONFIGURATION` / `Invalid configuration value detected for fs.azure.account.key` | External location for silver container not created | Follow Part 2 steps: create ext-loc-ev-silver with mi-ev-credential |
| `Path does not exist` on read | Bronze partition folder missing | Check ADF pipeline ran; leave p_run_date blank for full load |
| `Table not found: ev_catalog.silver` | Schema not created | Run `CREATE SCHEMA IF NOT EXISTS ev_catalog.silver MANAGED LOCATION '...'` |
| `AnalysisException: External location not found` | ext-loc-ev-silver not registered | Go to Catalog > External Locations and create it |
| `UC_COMMAND_NOT_SUPPORTED input_file_name` | Unity Catalog blocks `input_file_name()` | Use `F.col("_metadata.file_path")` instead |
| `MERGE requires Delta table` | Table exists but not as Delta format | Drop and recreate with `USING DELTA` |
| Column `data` is null after read | JSON file is empty `{}` or corrupted | Check Bronze file contents; empty `data:[]` is handled by early exit |
| GST calculation returns null | `amount_aud` cast to double failed (was non-numeric string) | Check raw data; add quality check for non-numeric amount |

---

## Part 11: Checklist Before Running

- [ ] Silver container exists in Azure Storage account `evdatalakedev2`
- [ ] `ext-loc-ev-silver` external location created in Databricks Catalog
- [ ] External location Test connection passes (green tick)
- [ ] `ev_catalog.silver` schema created with MANAGED LOCATION
- [ ] `ev_catalog.quarantine` schema created with MANAGED LOCATION (same ext-loc-ev-silver)
- [ ] GRANT CREATE EXTERNAL TABLE issued to your user on ext-loc-ev-silver
- [ ] Bronze data has correct partition structure (run repartition_bronze.py if needed)
- [ ] Import `nb_silver_payments.py` into Databricks workspace
- [ ] Run with `p_run_date` blank for full historical load
- [ ] Verify `ev_catalog.silver.sl_payments` row count > 0
- [ ] Register `ev_catalog.quarantine.payments` table after first notebook run
- [ ] Check quarantine path for any rejected rows via `SELECT * FROM ev_catalog.quarantine.payments`
