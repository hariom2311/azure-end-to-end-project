# Day 11 — Gold Layer: Star Schema, Facts, Dims & Marts

## Part 1 — What Is the Gold Layer?

Gold is the **reporting-ready** layer of the Medallion Architecture.

```
Bronze (raw JSON)  →  Silver (cleaned, deduped Delta)  →  Gold (star schema, aggregated)
                                                                    ↓
                                                            Power BI / Synapse / APIs
```

| Layer  | Who writes it | Who reads it          | Format        |
|--------|---------------|-----------------------|---------------|
| Bronze | ADF           | Silver notebooks      | JSON (raw)    |
| Silver | Databricks    | Gold notebooks        | Delta (Unity) |
| Gold   | Databricks    | Power BI, Synapse, APIs | Delta (Unity) |

**Gold container:** `abfss://gold@evdatalakedev2.dfs.core.windows.net/`

---

## Part 2 — Star Schema Design

### Geo Hierarchy (primary join chain)
```
DimCountry → DimState → DimCity → DimStation → DimCharger
```

### Dimensions built in Day 11

| Notebook | Gold Table | SCD Type | Silver Source(s) | Tracked Change |
|---|---|---|---|---|
| nb_gold_dim_station | dim_station | Type 1 | sl_stations + sl_cities + sl_states | Any attribute update |
| nb_gold_dim_customer | dim_customer | Type 2 | sl_customers | loyalty_tier change |
| nb_gold_dim_charger | dim_charger | Type 2 | sl_chargers + sl_stations | firmware_version change |
| nb_gold_dim_tariff | dim_tariff | Type 2 | sl_tariffs | Already versioned in Silver |

### Facts built in Day 11

| Notebook | Gold Table | Grain | Key Metrics |
|---|---|---|---|
| nb_gold_fact_charging_session | fact_charging_session | One row per session | energy_kwh, duration_min, expected vs billed, reconciliation_status |
| nb_gold_fact_payments | fact_payments | One row per payment | amount_aud, gst, gateway, refund_flag |

### Mart built in Day 11

| Notebook | Gold Table | Grain | Used By |
|---|---|---|---|
| nb_gold_mart_revenue_by_geo_month | mart_revenue_by_geo_month | State/City/Station × Month | Revenue Dashboard (Power BI) |

---

## Part 3 — SCD Types Explained

### SCD Type 1 — Overwrite (dim_station)
- Old value is lost; only latest value kept
- Use when history does not matter (station address, status)
- Implementation: standard Delta MERGE — `whenMatchedUpdateAll()`

### SCD Type 2 — Track History (dim_customer, dim_charger, dim_tariff)
- Old row is expired (`effective_to = yesterday, is_current = false`)
- New row is inserted (`effective_from = today, is_current = true`)
- Use when history matters (loyalty tier affects revenue analysis, firmware affects fault rates)

```
Step A: MERGE to expire old rows where tracked attribute changed
Step B: MERGE to insert new current rows + update unchanged current rows
```

**Key columns for SCD2:**
```sql
effective_from  DATE     NOT NULL   -- when this version became active
effective_to    DATE                -- NULL means still active
is_current      BOOLEAN            -- quick filter for latest version
```

---

## Part 4 — Reconciliation Logic (fact_charging_session)

This is the most important business rule in the Gold layer.

```
reconciliation_status = 
  UNPAID   → payment row is NULL (no payment recorded for this session)
  MATCH    → |billed_amount - expected_amount| < $0.01
  MISMATCH → difference ≥ $0.01
```

Where:
- `expected_amount_aud = energy_kwh × rate_per_kwh` (from current tariff)
- `billed_amount_aud` = actual payment amount from sl_payments

**Power BI Finance Dashboard** uses this to show mismatch rate by station/month.

---

## Part 5 — Gold Control Table

Same pattern as Silver — control table drives the ForEach pipeline.

```sql
CREATE TABLE ev_catalog.gold.gold_control (
    notebook_name   STRING  NOT NULL,
    layer           STRING  NOT NULL,   -- 'dim' | 'fact' | 'mart'
    run_order       INT     NOT NULL,   -- dims=1-9, facts=10-19, marts=20+
    is_active       BOOLEAN NOT NULL,
    description     STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
)
```

**run_order discipline:**
- Dims (1–9) always run before Facts (10–19)
- Facts always run before Marts (20+)
- This ensures `dim_station` exists before `fact_charging_session` joins it

---

## Part 6 — Pipeline Architecture

### Full end-to-end flow (Day 11)

```
pl_master_e2e
├── act_list_sources          → nb_list_active_sources (Bronze control table)
├── act_foreach_source        → pl_bronze_incremental_load_v3  (per source)
├── act_silver_dynamic        → pl_silver_dynamic_foreach
│                                 └── pl_silver_runner × 16 entities
└── act_gold_dynamic          → pl_gold_dynamic_foreach        ← NEW
                                    └── pl_gold_runner × N notebooks
```

### pl_gold_dynamic_foreach
```
act_list_gold_notebooks  →  nb_list_gold_notebooks reads gold_control
act_foreach_gold (sequential, isSequential: true)
    └── act_run_gold_runner  →  pl_gold_runner
```

### pl_gold_runner
- Single generic pipeline (same pattern as pl_silver_runner)
- Builds notebook path: `@concat('/Users/hariomsuryawanshi68258@gmail.com/', pipeline().parameters.p_notebook)`
- Passes `p_run_date` and `p_run_type` to each notebook

---

## Part 7 — Storage Layout (Gold Container)

```
abfss://gold@evdatalakedev2.dfs.core.windows.net/
├── dimensions/
│   ├── dim_station/
│   ├── dim_customer/
│   ├── dim_charger/
│   └── dim_tariff/
├── facts/
│   ├── fact_charging_session/
│   └── fact_payments/
└── marts/
    └── mart_revenue_by_geo_month/
```

Unity Catalog tables: `ev_catalog.gold.<table_name>`

---

## Part 8 — Admin Setup Steps (Run Once Before First Pipeline)

### Step 1 — Create External Location for Gold

#### What is an External Location?
Unity Catalog needs to know which ADLS paths it is allowed to use for external Delta tables.
An External Location maps a name (e.g. `ext-loc-ev-gold`) to an ADLS path
(`abfss://gold@evdatalakedev2.dfs.core.windows.net/`).
Without it, any `CREATE TABLE ... LOCATION 'abfss://gold@...'` will fail with:
```
PERMISSION_DENIED: Principal does not have CREATE EXTERNAL TABLE
on External Location 'ext-loc-ev-gold'
```

#### Pre-requisite — Storage Credential
The External Location needs a Storage Credential that has access to the ADLS storage account.
The same credential used for Silver (`ev-storage-credential` or similar) can be reused — it
already has access to `evdatalakedev2` storage account.

To check existing credentials:
```
Databricks UI → Catalog → External Data → Storage Credentials
```

#### UI Steps — Create External Location for Gold

1. Open **Databricks workspace** → click **Catalog** icon (left sidebar)
2. In Catalog Explorer → click **External Data** → **External Locations**
3. Click **Create location** (top right)
4. Fill in:
   - **External location name**: `ext-loc-ev-gold`
   - **URL**: `abfss://gold@evdatalakedev2.dfs.core.windows.net/`
   - **Storage credential**: select the existing credential (same one used for Silver/Bronze)
5. Click **Create**
6. Click **Test connection** — should show "Connection successful"

#### Verify with SQL
```sql
SHOW EXTERNAL LOCATIONS;
-- Should show ext-loc-ev-gold with URL abfss://gold@evdatalakedev2.dfs.core.windows.net/

DESCRIBE EXTERNAL LOCATION `ext-loc-ev-gold`;
```

#### Grant SP access to the External Location
Run in Databricks SQL Editor **as metastore admin**:
```sql
GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `ext-loc-ev-gold`
TO `9245da2f-7129-4e0c-92c4-134fe896e294`;
```
This is already included in `grants_gold_sp.sql` Phase 1 section.

### Step 2 — Run nb_gold_setup_control_table
This notebook:
- Creates `ev_catalog.gold` schema
- Creates `ev_catalog.gold.gold_control` table
- Inserts 7 control rows (4 dims + 2 facts + 1 mart)

### Step 3 — Run Phase 1 GRANTs (grants_gold_sp.sql)
```sql
GRANT USE CATALOG ON CATALOG ev_catalog TO `<sp-id>`;
GRANT USE SCHEMA ON SCHEMA ev_catalog.gold TO `<sp-id>`;
GRANT CREATE TABLE ON SCHEMA ev_catalog.gold TO `<sp-id>`;
GRANT USE SCHEMA ON SCHEMA ev_catalog.silver TO `<sp-id>`;
GRANT SELECT, MODIFY ON TABLE ev_catalog.gold.gold_control TO `<sp-id>`;
```

### Step 4 — Import Notebooks to Databricks
1. Download `.ipynb` files from GitHub (after conversion)
2. Databricks Workspace → Import → select all Gold `.ipynb` files
3. Place them in `/Users/hariomsuryawanshi68258@gmail.com/`

### Step 5 — Import Pipelines to ADF
1. ADF Studio → Manage → ARM template / Git → import `pl_gold_runner.json`
2. Import `pl_gold_dynamic_foreach.json`
3. Import `pl_master_e2e.json` (replaces Day 10's `pl_bronze_master_foreach`)
4. Publish all

### Step 6 — Run Phase 2 GRANTs (after first full pipeline run)
Run the Phase 2 section of `grants_gold_sp.sql` — all Gold tables now exist.

---

## Part 9 — Gold Control Table INSERT

```sql
INSERT INTO ev_catalog.gold.gold_control
    (notebook_name, layer, run_order, is_active, description, created_at, updated_at)
VALUES
    ('nb_gold_dim_station',              'dim',  1,  true, 'SCD1 — station with city/state hierarchy',   current_timestamp(), current_timestamp()),
    ('nb_gold_dim_customer',             'dim',  2,  true, 'SCD2 — customer loyalty tier tracking',      current_timestamp(), current_timestamp()),
    ('nb_gold_dim_charger',              'dim',  3,  true, 'SCD2 — charger firmware version tracking',   current_timestamp(), current_timestamp()),
    ('nb_gold_dim_tariff',               'dim',  4,  true, 'SCD2 — tariff version windows',              current_timestamp(), current_timestamp()),
    ('nb_gold_fact_charging_session',    'fact', 10, true, 'Fact — session grain, reconciliation flag',  current_timestamp(), current_timestamp()),
    ('nb_gold_fact_payments',            'fact', 11, true, 'Fact — payment grain, refund flag',          current_timestamp(), current_timestamp()),
    ('nb_gold_mart_revenue_by_geo_month','mart', 20, true, 'Mart — revenue by state/city/station/month', current_timestamp(), current_timestamp());
```

**To add a new Gold notebook** — no ADF change needed:
```sql
INSERT INTO ev_catalog.gold.gold_control
    (notebook_name, layer, run_order, is_active, description, created_at, updated_at)
VALUES ('nb_gold_fact_maintenance', 'fact', 12, true, 'Fact — maintenance events', current_timestamp(), current_timestamp());
```

---

## Part 10 — Practice Exercises

1. **Add dim_vehicle**: Create `nb_gold_dim_vehicle.py` reading from `sl_vehicles`. Use SCD2 tracking on `is_current` from Silver. Add a row to `gold_control` with `run_order=5`. No ADF change.

2. **Add fact_maintenance**: Create `nb_gold_fact_maintenance.py` reading from `sl_maintenance_events`. Key metrics: `mttr_hours`, `high_mttr_flag`, `fault_category`. Join to `dim_charger` for station context. `run_order=12`.

3. **Extend the mart**: Modify `nb_gold_mart_revenue_by_geo_month.py` to add `avg_energy_kwh_per_session` and `sla_breach_rate` columns.

4. **Reconciliation report**: Write a SQL query on `fact_charging_session` that shows stations with `mismatch_count > 10` this month.

5. **SCD2 validation**: After running `dim_customer`, query:
   ```sql
   SELECT customer_key, COUNT(*) AS versions
   FROM ev_catalog.gold.dim_customer
   GROUP BY customer_key
   HAVING COUNT(*) > 1
   ORDER BY versions DESC
   ```
   These are customers whose loyalty_tier changed.

6. **Disable a Gold notebook**: Set `is_active = false` for `nb_gold_mart_revenue_by_geo_month` in `gold_control`. Re-run `pl_gold_dynamic_foreach`. Verify the mart notebook was skipped.

---

## Part 11 — Interview Q&A

**Q1: Why do Dims run before Facts in the Gold pipeline?**
Facts join to Dim surrogate keys. If `dim_station` doesn't exist yet, `fact_charging_session` would have NULL station keys. `run_order` in `gold_control` enforces this sequence automatically.

**Q2: What is the difference between SCD Type 1 and SCD Type 2?**
- SCD1: overwrite in-place — no history kept. Use when attribute changes don't affect analysis (e.g., station address correction).
- SCD2: expire old row + insert new row — full history kept. Use when the change affects historical analysis (e.g., if a customer moved from Bronze → Gold tier, past revenue should be attributed to the tier they held at that time).

**Q3: Why is `reconciliation_status` a 3-way flag (MATCH/MISMATCH/UNPAID)?**
UNPAID captures sessions where charging happened but no payment record exists — important for fraud and revenue leakage detection. MISMATCH captures billing errors. MATCH is the expected happy path.

**Q4: What happens if a Gold notebook fails mid-run?**
`isSequential: true` in `pl_gold_dynamic_foreach` means the ForEach stops at the first failure. Fix the failing notebook, then re-trigger `pl_gold_dynamic_foreach`. Since all Gold writes are Delta MERGEs, re-running is idempotent — no duplicates.

**Q5: How do you add a new Gold table without touching ADF?**
```sql
INSERT INTO ev_catalog.gold.gold_control
VALUES ('nb_gold_new_table', 'fact', 15, true, 'New fact table', current_timestamp(), current_timestamp());
```
Upload the notebook to Databricks. The next pipeline run picks it up automatically.

**Q6: Why does dim_tariff not need a custom SCD2 implementation?**
`sl_tariffs` in Silver already carries `effective_from`, `effective_to`, and `is_current` (computed in Silver Rule H). We simply promote the Silver rows to Gold with a MERGE on `(tariff_key, effective_from)`.

**Q7: What is a mart and how does it differ from a fact table?**
Fact tables are at business-event grain (one row per session, one row per payment). Marts are pre-aggregated for dashboard performance (one row per station per month). Marts are rebuilt from fact tables, so they can always be dropped and recomputed.

---

## Part 12 — Common Errors

| Error | Cause | Fix |
|---|---|---|
| `Table not found: ev_catalog.gold.dim_station` when running fact | Dims not run first | Check `run_order` in `gold_control`; ensure dims have lower run_order |
| `GRANT ... ON TABLE` fails with "not found" | Table doesn't exist yet | Run Phase 1 grants before pipeline, Phase 2 grants after first run |
| SCD2 creates duplicate current rows | Step A (expire) and Step B (insert) ran concurrently | Keep `isSequential: true` in ForEach; never parallelize SCD2 notebooks |
| `null` in `expected_amount_aud` | `tariff_id` in session has no match in current tariff | Session used an expired tariff — join to `is_current = false` rows too, or use a date-range join |
| Mart shows old data after Silver re-run | Mart was not re-triggered | Re-run `pl_gold_dynamic_foreach` after Silver completes |
| `External Location not found` for Gold path | `ext-loc-ev-gold` not created | Admin must create External Location for the gold container first |

---

## Part 13 — Deployment Checklist

### Pre-pipeline (admin)
- [ ] Gold container exists in ADLS (`abfss://gold@evdatalakedev2.dfs.core.windows.net/`)
- [ ] External Location `ext-loc-ev-gold` created in Unity Catalog
- [ ] `nb_gold_setup_control_table` run — schema + control table created
- [ ] Phase 1 GRANTs applied (`grants_gold_sp.sql` Phase 1 section)
- [ ] All Gold notebooks imported to Databricks workspace
- [ ] `pl_gold_runner`, `pl_gold_dynamic_foreach`, `pl_master_e2e` imported to ADF

### First run
- [ ] Trigger `pl_gold_dynamic_foreach` (or `pl_master_e2e`) with `p_run_type=full`
- [ ] Verify `dim_station`, `dim_customer`, `dim_charger`, `dim_tariff` created
- [ ] Verify `fact_charging_session`, `fact_payments` created
- [ ] Verify `mart_revenue_by_geo_month` created
- [ ] Phase 2 GRANTs applied (`grants_gold_sp.sql` Phase 2 section)

### Ongoing (daily incremental)
- [ ] `pl_master_e2e` triggered by ADF trigger with `p_run_type=incremental`
- [ ] Bronze → Silver → Gold runs end-to-end
- [ ] Monitor `fact_charging_session.reconciliation_status` for MISMATCH rate
