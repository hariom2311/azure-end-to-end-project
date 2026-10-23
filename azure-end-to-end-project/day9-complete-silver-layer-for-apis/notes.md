# Day 9 — Complete Silver Layer for All API Entities

## Overview

Day 8 built the Bronze → Silver pipeline for **payments** only.
Day 9 extends the same pattern to all remaining 15 Bronze API entities, producing a full Silver layer
across all 16 tables. The master ADF pipeline (`pl_bronze_master_foreach`) is updated to chain all
16 Silver pipelines sequentially after the Bronze ForEach completes.

> **Prerequisites:** Day 8 complete — `ev_catalog.silver` schema, `ev_catalog.quarantine` schema,
> `ext-loc-ev-silver` external location, and ADF service principal GRANTs are all already done.
> Day 9 reuses everything from Day 8. No new infrastructure needed.

---

## Part 1: Entity Overview — All 16 Silver Tables

| # | Entity | Silver Table | Primary Key | Business Rules |
|---|--------|-------------|-------------|----------------|
| 1 | payments | sl_payments | payment_id | A, D, G, J |
| 2 | charge_cards | sl_charge_cards | card_id | A, F (card mask), G |
| 3 | chargers | sl_chargers | charger_id | A, C (fault flag), G |
| 4 | cities | sl_cities | city_id | A, G |
| 5 | complaints | sl_complaints | complaint_id | A, C (fault code), G, I (SLA breach) |
| 6 | customers | sl_customers | customer_id | A, F (email mask), G |
| 7 | employees | sl_employees | employee_id | A, G |
| 8 | energy_prices | sl_energy_prices | price_id | A, G, is_current flag |
| 9 | maintenance_events | sl_maintenance_events | event_id | A, C (fault category), G, high_mttr_flag |
| 10 | partners | sl_partners | partner_id | A, G, contract_expired_flag |
| 11 | pipeline_audit | sl_pipeline_audit | run_id | A, G, success_rate_pct, has_error_flag |
| 12 | sessions | sl_sessions | session_id | A, B (late event), G, I (SLA breach), J (GST) |
| 13 | states | sl_states | state_code | A, G |
| 14 | stations | sl_stations | station_id | A, G, state_region |
| 15 | tariffs | sl_tariffs | tariff_id | A, G, H (is_current recompute) |
| 16 | vehicles | sl_vehicles | vehicle_id | A, G, H (SCD2 is_current) |

---

## Part 2: Business Rules Reference

All notebooks apply **Rule A** (dedup) and **Rule G** (timezone) as a baseline. Additional rules are applied per entity.

### Rule A — Deduplication
Every notebook deduplicates by primary key using a `ROW_NUMBER()` window, keeping the record with the latest `updated_at`:
```python
w = Window.partitionBy("<primary_key>").orderBy(F.col("updated_at").desc())
df_dedup = df_good.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn", "_src")
```
Handles: ADF retries that re-land the same pages, API returning overlapping records across runs.

### Rule B — Late Event Flag (sessions)
Flags sessions where `started_at` is more than 24 hours before `created_at` (the ingestion timestamp):
```python
.withColumn("late_event_flag",
    (F.unix_timestamp("created_at_utc") - F.unix_timestamp("started_at_utc")) > 86400)
```

### Rule C — Fault Mapping
- **chargers:** `status IN ('Maintenance', 'Fault', 'Offline')` → `fault_flag = True`
- **complaints:** `category` → `fault_code` (e.g. "Billing Error" → `FAULT_BILLING`)
- **maintenance_events:** `root_cause` → `fault_category` (e.g. "Vandalism" → `FAULT_VANDALISM`)

### Rule D — Billing Reconciliation (payments)
```python
.withColumn("billing_anomaly_flag",
    F.when(F.col("status").isin("failed", "Disputed") & (F.col("amount_aud") > 0), True).otherwise(False))
```

### Rule F — PCI Masking
- **charge_cards:** `card_number` → `card_last4` (last 4 digits only, original dropped)
- **customers:** `email` → `email_masked` (`****@domain.com`, original dropped)

### Rule G — Timezone Normalisation
All timestamp columns are converted from UTC strings to both UTC and AEST:
```python
.withColumn("processed_at_utc",  F.to_timestamp("processed_at"))
.withColumn("processed_at_aest", F.from_utc_timestamp("processed_at_utc", "Australia/Sydney"))
```
PySpark handles DST automatically (AEST = UTC+10, AEDT = UTC+11).

### Rule H — SCD Type 2 / is_current Flag
- **tariffs:** `is_current` recomputed as `effective_to IS NULL OR effective_to >= current_date()`
- **vehicles:** `is_current` preserved from source; dedup prioritises `is_current = True` rows first

### Rule I — SLA Breach
- **complaints:** SLA threshold by priority (P1=4h, P2=24h, P3=72h, P4=168h). `age_hrs` computed from `created_ts` to `resolved_ts` (if resolved) or `current_timestamp` (if open). `sla_breach_flag = age_hrs > threshold`
- **sessions:** `sla_breach_flag = duration_min > 240` (session longer than 4 hours)

### Rule J — GST Split
- **payments:** `gst_amount_aud = ROUND(amount_aud / 11, 4)`, `excl_gst_aud = ROUND(amount_aud - gst_amount_aud, 4)`
- **sessions:** same calculation on `cost_aud`

---

## Part 3: Notebook Structure (All Entities Follow Same Pattern)

Every notebook has 13 cells in the same order. Only the entity-specific fields and rules change.

```
Cell 1  — imports (pyspark.sql functions as F, Window, DeltaTable), constants
           (BRONZE_VOL, SILVER_TABLE, SILVER_PATH, QUARANTINE), widget p_run_date
Cell 2  — build read_path (ingestion_date=<date>/ if run_date, else full BRONZE_VOL)
           spark.read.format("json").option("multiline","true")
                                    .option("recursiveFileLookup","true")
                                    .load(read_path)
           withColumn("_src", F.col("_metadata.file_path"))
Cell 3  — F.explode(F.col("data")), select + cast all fields from record.*
           count total_records; dbutils.notebook.exit("NO_DATA") if 0
Cell 4  — quality checks → rejection_reason column
           df_bad → write.format("delta").mode("append").save(QUARANTINE)
           df_good → drop rejection_reason, continue
Cell 5  — Rule A: Window dedup by primary key, updated_at DESC, row_number=1
           drop _rn, _src
Cell 6  — Rule G: to_timestamp + from_utc_timestamp("Australia/Sydney")
           on all business timestamp columns
Cell 7+ — entity-specific business rules (C, D, F, H, I, J, B as applicable)
Cell 10 — CREATE TABLE IF NOT EXISTS ev_catalog.silver.sl_<entity>
           USING DELTA LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/<entity>/'
Cell 11 — DeltaTable.forName → .merge on primary key
           .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
           capture merge_result
Cell 12 — merge_result.collect()[0] → num_inserted_rows, num_updated_rows
           print run stats block (inserted / updated / quarantined / duplicates)
Cell 13 — post-load SQL .display() breakdown
```

### Why Cell 4 uses append (not MERGE) for quarantine
Bad rows are written with `mode("append")` so every run's rejections are preserved. This gives a full audit history of all rejected records across all pipeline runs. The quarantine table is registered separately with `CREATE TABLE` (see Part 5) and is never overwritten.

---

## Part 4: Entity-Specific Details

### charge_cards
- **PCI Rule F:** `card_number` masked to last 4 digits via `regexp_extract(card_number, r'(\d{4})$', 1)` → stored as `card_last4`. Original `card_number` dropped — never lands in Silver.
- **Quality checks:** null card_id, null customer_id, null card_number, null status

### chargers
- **Rule C:** `fault_flag = status IN ('Maintenance', 'Fault', 'Offline')`
- `max_kw` cast from string to double

### cities
- Simple reference/dimension table. No entity-specific rules beyond A and G.

### complaints
- **Rule C:** `fault_code` mapped from `category` string (e.g. "Billing Error" → `FAULT_BILLING`, "Charger" → `FAULT_CHARGER`, "App" → `FAULT_APP`, etc.)
- **Rule I:** SLA recomputed from scratch — does not trust `sla_breach` from source API.
  - `sla_threshold_hrs` derived from priority (P1=4, P2=24, P3=72, P4=168)
  - `age_hrs` = `(resolved_ts - created_ts) / 3600` if resolved, else `(now - created_ts) / 3600`
  - `sla_breach_flag = age_hrs > sla_threshold_hrs`
- Both `created_ts` (business time) and `created_at` (system time) are timezone-converted

### customers
- **Rule F:** email masked to `****@<domain>` using `regexp_extract(email, r'@(.+)$', 1)`. Original `email` dropped.
- `phone` retained as-is (not masked — not in PCI scope for this dataset)

### employees
- Simple dimension table. No entity-specific rules beyond A and G.

### energy_prices
- `price_per_kwh`, `off_peak_price`, `peak_price` cast from string to double
- `is_current` flag added: `effective_to IS NULL OR effective_to > current_timestamp()`
- Both `effective_from` and `effective_to` timezone-converted

### maintenance_events
- **Rule C:** `fault_category` mapped from `root_cause` using `.ilike()` matching (case-insensitive): Vandalism → `FAULT_VANDALISM`, Hardware → `FAULT_HARDWARE`, Software/Firmware → `FAULT_SOFTWARE`, Power → `FAULT_POWER`, Network → `FAULT_NETWORK`, Wear → `FAULT_WEAR`, Weather → `FAULT_WEATHER`, else → `FAULT_OTHER`
- `mttr_hours` cast from string to double
- `high_mttr_flag = mttr_hours > 48` (flags complex/prolonged repairs)
- `event_ts` (business event time) timezone-converted to `event_ts_utc` and `event_ts_aest`

### partners
- `revenue_share_pct` cast from string to double
- `contract_expired_flag = contract_end IS NOT NULL AND to_date(contract_end) < current_date()`

### pipeline_audit
- `rows_read`, `rows_written`, `rows_rejected` cast to long
- `success_rate_pct = ROUND(rows_written / rows_read * 100, 2)` (null-safe — handles rows_read = 0)
- `has_error_flag = error_message IS NOT NULL AND trim(error_message) != ''`
- `started_at` and `ended_at` timezone-converted

### sessions
- **Rule J:** GST split on `cost_aud` → `gst_amount_aud` and `excl_gst_aud`
- **Rule I:** `sla_breach_flag = duration_min > 240` (sessions longer than 4 hours)
- **Rule B:** `late_event_flag = (created_at_utc - started_at_utc) > 86400 seconds`
- `energy_kwh`, `cost_aud`, `peak_power_kw` cast from string to double
- `duration_min` cast to long

### states
- Very small reference table (~8 rows for Australian states/territories). No entity-specific rules.

### stations
- `latitude`, `longitude`, `max_power_kw` cast from string to double
- `num_connectors` cast to int
- `state_region` derived: NSW/VIC/QLD → `Eastern`, WA → `Western`, others → `Other`

### tariffs
- **Rule H:** `is_current` recomputed: `effective_to IS NULL OR to_date(effective_to) >= current_date()`
  - Source `is_current` field is kept as `is_current_source` during processing then dropped
  - Our recomputed value is authoritative — source may be stale
- `rate_per_kwh` cast from string to double

### vehicles
- **Rule H (SCD2):** Source already carries `is_current`, `effective_from`, `effective_to` — SCD2 managed by source system
- Dedup window orders by `is_current DESC` first, then `updated_at DESC` — ensures the current-version row is kept if duplicates exist
- `battery_capacity_kwh` cast from string to double; `year` cast to int; `range_km` cast to int

---

## Part 5: Quarantine Tables — Registration

After running each Silver notebook for the first time, register the quarantine table in Databricks Catalog so bad rows are queryable via SQL.

> **Run only after the notebook has executed at least once.** If no bad rows were rejected, the quarantine path will not contain Delta files yet and `CREATE TABLE` will fail with `Path does not exist` — that is expected.

Run all at once or one at a time as notebooks complete:

```sql
-- Verify quarantine schema exists first (done in Day 8)
-- CREATE SCHEMA IF NOT EXISTS ev_catalog.quarantine
-- MANAGED LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/';

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

Query any quarantine table to see rejection reasons:
```sql
SELECT rejection_reason, COUNT(*) AS cnt, MAX(quarantine_ts) AS latest
FROM ev_catalog.quarantine.sessions     -- replace with any entity
GROUP BY rejection_reason
ORDER BY cnt DESC;
```

---

## Part 6: Upload Notebooks to Databricks

Each `.py` file in `notebooks/` must be imported into the Databricks workspace before the ADF pipeline can trigger it.

### Import method (one by one)
1. Databricks workspace → left sidebar → **Workspace**
2. Navigate to `/Users/hariomsuryawanshi68258@gmail.com/`
3. Click the **⌄** dropdown next to the folder → **Import**
4. Choose **File** → browse to the `.py` file → **Import**
5. Repeat for each notebook

### Notebooks to import (15 new + payments already done in Day 8)

| File | Databricks path |
|------|----------------|
| nb_silver_charge_cards.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_charge_cards` |
| nb_silver_chargers.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_chargers` |
| nb_silver_cities.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_cities` |
| nb_silver_complaints.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_complaints` |
| nb_silver_customers.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_customers` |
| nb_silver_employees.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_employees` |
| nb_silver_energy_prices.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_energy_prices` |
| nb_silver_maintenance_events.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_maintenance_events` |
| nb_silver_partners.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_partners` |
| nb_silver_pipeline_audit.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_pipeline_audit` |
| nb_silver_sessions.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_sessions` |
| nb_silver_states.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_states` |
| nb_silver_stations.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_stations` |
| nb_silver_tariffs.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_tariffs` |
| nb_silver_vehicles.py | `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_vehicles` |

> The notebook path in the ADF pipeline JSON (`notebookPath`) must exactly match the Databricks workspace path above. If you import to a different folder, update the `notebookPath` field in each `pl_silver_<entity>.json` before importing to ADF.

---

## Part 7: Import ADF Pipelines

### 15 new Silver pipelines

Import each pipeline JSON into ADF Studio:

**ADF Studio → Author → Pipelines → ⋮ (three dots next to Pipelines) → Import from pipeline JSON**

Import in this order (each depends on the previous succeeding — sequential chain in master):

| Pipeline JSON | Calls notebook |
|---------------|---------------|
| pl_silver_charge_cards.json | nb_silver_charge_cards |
| pl_silver_chargers.json | nb_silver_chargers |
| pl_silver_cities.json | nb_silver_cities |
| pl_silver_complaints.json | nb_silver_complaints |
| pl_silver_customers.json | nb_silver_customers |
| pl_silver_employees.json | nb_silver_employees |
| pl_silver_energy_prices.json | nb_silver_energy_prices |
| pl_silver_maintenance_events.json | nb_silver_maintenance_events |
| pl_silver_partners.json | nb_silver_partners |
| pl_silver_pipeline_audit.json | nb_silver_pipeline_audit |
| pl_silver_sessions.json | nb_silver_sessions |
| pl_silver_states.json | nb_silver_states |
| pl_silver_stations.json | nb_silver_stations |
| pl_silver_tariffs.json | nb_silver_tariffs |
| pl_silver_vehicles.json | nb_silver_vehicles |

### Update the master pipeline

The existing `pl_bronze_master_foreach` only called `pl_silver_payments`. The updated version in `pipelines/pl_bronze_master_foreach.json` chains all 16 Silver pipelines sequentially after Bronze ForEach completes.

**Import the updated master pipeline:**

ADF Studio → Author → Pipelines → find `pl_bronze_master_foreach` → **⋮ → Delete** (or overwrite)
→ then **Import from pipeline JSON** → select `pl_bronze_master_foreach.json`

**Or update manually:** open the existing master pipeline in ADF Studio, scroll past `act_silver_payments`, and add 15 more `ExecutePipeline` activities chained sequentially (each `dependsOn` the previous one with `Succeeded`).

After importing all pipelines → **Publish All**.

---

## Part 8: ADF Service Principal Grants

> **Run these AFTER the Silver tables exist** (i.e. after running each notebook at least once).
> If a table has not been created yet, skip that GRANT and run it later.

The ADF service principal (`9245da2f-7129-4e0c-92c4-134fe896e294`) needs `MODIFY, SELECT` on each Silver and quarantine table. The schema-level `CREATE TABLE` grant from Day 8 allows creating new tables, but does NOT cover tables that already exist — those need explicit table-level grants.

```sql
-- Silver tables — Day 9 entities
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

-- Quarantine tables — Day 9 entities (run after quarantine tables are registered in Part 5)
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
```

---

## Part 9: Master Pipeline — pl_bronze_master_foreach

### What it does

```
act_list_sources
    → nb_list_active_sources (returns JSON array of source names)
    ↓ Succeeded
act_foreach_source
    → pl_bronze_incremental_load_v3 for each source (sequential)
    ↓ Succeeded
act_silver_payments       → pl_silver_payments
    ↓ Succeeded
act_silver_charge_cards   → pl_silver_charge_cards
    ↓ Succeeded
act_silver_chargers       → pl_silver_chargers
    ↓ Succeeded
act_silver_cities         → pl_silver_cities
    ↓ Succeeded
act_silver_complaints     → pl_silver_complaints
    ↓ Succeeded
act_silver_customers      → pl_silver_customers
    ↓ Succeeded
act_silver_employees      → pl_silver_employees
    ↓ Succeeded
act_silver_energy_prices  → pl_silver_energy_prices
    ↓ Succeeded
act_silver_maintenance_events → pl_silver_maintenance_events
    ↓ Succeeded
act_silver_partners       → pl_silver_partners
    ↓ Succeeded
act_silver_pipeline_audit → pl_silver_pipeline_audit
    ↓ Succeeded
act_silver_sessions       → pl_silver_sessions
    ↓ Succeeded
act_silver_states         → pl_silver_states
    ↓ Succeeded
act_silver_stations       → pl_silver_stations
    ↓ Succeeded
act_silver_tariffs        → pl_silver_tariffs
    ↓ Succeeded
act_silver_vehicles       → pl_silver_vehicles
```

All Silver activities run sequentially — each `dependsOn` the previous with `Succeeded`.
This ensures Bronze data is fully loaded before any Silver pipeline reads it, and ensures
if one Silver pipeline fails the subsequent ones do not run on stale data.

### Why sequential (not parallel)?
Silver pipelines share the same Databricks cluster. Running them in parallel would overload
the cluster with 16 concurrent notebooks. Sequential execution is safer and simpler to debug
when one entity fails.

### p_run_date flow
The master pipeline does not pass `p_run_date` to Silver pipelines — it leaves it blank.
Each Silver pipeline's internal expression `@if(empty(pipeline().parameters.p_run_date), formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd'), pipeline().parameters.p_run_date)`
resolves to today's date from `TriggerTime` automatically.

For a **manual backfill**, trigger the individual Silver pipeline directly (not the master) and pass `p_run_date = YYYY-MM-DD`.

---

## Part 10: Common Errors and Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `PERMISSION_DENIED: User does not have MODIFY and SELECT on Table ev_catalog.silver.sl_<entity>` | Table already exists from direct notebook run but ADF SP has no table-level permission | Run `GRANT MODIFY, SELECT ON TABLE ev_catalog.silver.sl_<entity> TO \`9245da2f-...\`` |
| `UNAUTHORIZED_ACCESS: PERMISSION_DENIED: User does not have READ VOLUME on Volume ev_catalog.bronze.api` | ADF SP missing volume read (already granted in Day 8 — verify it is still in place) | Re-run the `GRANT READ VOLUME` from Day 8 notes Part 8 |
| `Path does not exist` on Bronze read | Bronze partition for that date not yet loaded | Run Bronze pipeline first, or leave `p_run_date` blank for full load |
| `CREATE TABLE` on quarantine fails with `Path does not exist` | Notebook has not run yet — no Delta files at quarantine path | Run the notebook first; if no bad rows are produced, the quarantine path won't be created (expected) |
| `NO_DATA` exit in ADF activity output | Bronze page files contain `{"data": [], ...}` — empty pages | Not an error — notebook exits cleanly. Check if Bronze data is populated for that date |
| `AnalysisException: Resolved attribute(s) record.xxx missing` | Field name mismatch between notebook column name and actual Bronze JSON field | Check Bronze file schema; update `F.col("record.<field>")` in Cell 3 to match actual field name |
| Merge stats show `rows_duplicate` as negative | `df_silver.count()` called twice — second call after MERGE returns Silver total, not source count | Ensure `df_silver.count()` is called before `.execute()` and cached, or recompute from `rows_inserted + rows_updated` |
| `TypeError: unsupported format string passed to Column.__format__` in Cell 12 | `merge_result` not collected before indexing | Use `merge_result.collect()[0]` then wrap each field with `int()` |

---

## Part 11: Day 9 Checklist

**Prerequisites (verify from Day 8):**
- [ ] `ext-loc-ev-silver` external location exists and Test connection is green
- [ ] `ev_catalog.silver` schema exists
- [ ] `ev_catalog.quarantine` schema exists
- [ ] ADF SP (`9245da2f-...`) has `USE CATALOG`, `USE SCHEMA`, `READ VOLUME`, `CREATE TABLE` grants from Day 8

**Notebook deployment:**
- [ ] All 15 notebooks imported to Databricks workspace at `/Users/hariomsuryawanshi68258@gmail.com/nb_silver_<entity>`
- [ ] Run each notebook manually with `p_run_date` blank (full load) to verify it works before wiring to ADF

**Quarantine registration (after each notebook runs):**
- [ ] `ev_catalog.quarantine.charge_cards` registered
- [ ] `ev_catalog.quarantine.chargers` registered
- [ ] `ev_catalog.quarantine.cities` registered
- [ ] `ev_catalog.quarantine.complaints` registered
- [ ] `ev_catalog.quarantine.customers` registered
- [ ] `ev_catalog.quarantine.employees` registered
- [ ] `ev_catalog.quarantine.energy_prices` registered
- [ ] `ev_catalog.quarantine.maintenance_events` registered
- [ ] `ev_catalog.quarantine.partners` registered
- [ ] `ev_catalog.quarantine.pipeline_audit` registered
- [ ] `ev_catalog.quarantine.sessions` registered
- [ ] `ev_catalog.quarantine.states` registered
- [ ] `ev_catalog.quarantine.stations` registered
- [ ] `ev_catalog.quarantine.tariffs` registered
- [ ] `ev_catalog.quarantine.vehicles` registered

**ADF pipeline deployment:**
- [ ] All 15 `pl_silver_<entity>.json` imported to ADF Studio
- [ ] Updated `pl_bronze_master_foreach.json` imported (replaces existing master)
- [ ] Publish All in ADF Studio
- [ ] Test master pipeline with `p_load_type_override` blank (incremental) — all activities succeed
- [ ] Verify all 16 Silver tables have row counts > 0 in Databricks Catalog

**ADF SP grants (after Silver tables created by first notebook run):**
- [ ] `GRANT MODIFY, SELECT` run for all 15 new Silver tables
- [ ] `GRANT MODIFY, SELECT` run for all 15 new quarantine tables
- [ ] Re-trigger ADF master pipeline — all Silver activities succeed end-to-end
