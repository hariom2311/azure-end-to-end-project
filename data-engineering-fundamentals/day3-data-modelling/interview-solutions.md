# Day 3 — Interview Solutions: Data Modelling

> Complete answers for all 40 questions in `interview-questions.md`.

---

## Concept 1: Dimensional Modelling & Star Schema

**Q1 — Fact table vs. dimension table**

A **fact table** stores measurable, numeric business events — one row per event occurrence. Example: `fact_sales` with columns `order_id`, `revenue`, `quantity`, `discount_pct`. The numbers are the "facts" you aggregate.

A **dimension table** stores the descriptive context of those events — who, what, where, when. Example: `dim_customer` with `customer_id`, `customer_name`, `city`, `country`, `segment`. Dimensions are used for filtering and grouping, not for aggregation.

**Rule of thumb:** If a column answers "how much?" or "how many?" → fact. If it answers "which one?" or "what kind?" → dimension.

---

**Q2 — The grain**

The **grain** is a precise statement of what one row in the fact table represents. It must be defined before any column is chosen.

**Why it must come first:** Every column either belongs to the grain (a measure of that event) or is a foreign key to a dimension that describes it. If the grain is ambiguous, columns get mis-assigned and queries return wrong aggregates (fan-out, double-counting).

**Example:** "One row represents one line item within one customer order" is a valid grain. "One row represents one order" is a different grain — you cannot add per-product columns without violating it.

A fact table with the grain "order line item" supports: revenue per product, units sold per category. A fact table with grain "order header" cannot answer those questions without a CASE explosion.

---

**Q3 — Star Schema vs. Snowflake Schema**

| | Star Schema | Snowflake Schema |
|---|---|---|
| Dimension structure | Fully denormalised (all attributes in one dim table) | Normalised (dims have FK to sub-dims: dim_product → dim_category → dim_department) |
| Query joins | 1 join per dimension | 2+ joins per dimension |
| Storage | Slightly larger (repeated values in dims) | Smaller (shared lookup tables) |
| BI tool friendliness | High — simple, readable | Lower — analysts must know sub-dim join paths |

**Choice for BI analytics layer: Star Schema.** At modern columnar storage costs, the storage savings of Snowflake Schema are negligible. The extra joins increase query complexity, confuse BI tools that auto-generate SQL (Tableau, Power BI), and slow ad-hoc analysis. Only use Snowflake Schema if a shared sub-dimension has significant size and is used for write operations — rare in analytics.

---

**Q4 — Wrong grain scenario**

If `fact_sales` has grain = order header (one row per order), then each row has total order revenue but no per-product breakdown. The `product_id` column on the fact table would only store one product per order — meaningless for multi-item orders, or absent entirely.

"Revenue by product category" requires knowing which products were in each order at what quantities — this is line-item level information.

**Fix:** Change the grain to **order line item** — one row per product within each order:
```sql
-- Correct grain: one row = one line item
fact_sales: order_id, product_id, customer_id, date_id, quantity, unit_price, revenue
```

Now `GROUP BY product_category` works correctly because each row has exactly one `product_id`.

---

**Q5 — Ride-sharing fact table design**

**Grain:** One row = one completed trip.

**Fact table columns (measures and FKs):**
```
fact_trips:
  trip_id          (degenerate dimension — kept in fact, no dim table needed)
  driver_key       FK → dim_driver
  rider_key        FK → dim_rider
  pickup_loc_key   FK → dim_location
  dropoff_loc_key  FK → dim_location  (role-playing dimension — same dim, two FKs)
  start_date_key   FK → dim_date
  start_time_key   FK → dim_time      (separate time dim for intra-day analysis)
  fare             NUMERIC            (measure)
  surge_multiplier NUMERIC            (measure)
  trip_duration_min INT               (measure)
  distance_km      NUMERIC            (measure)
  rating_by_rider  SMALLINT           (measure — NULL if not rated)
  rating_by_driver SMALLINT           (measure — NULL if not rated)
```

**Dimension tables:**
- `dim_driver`: driver_key, driver_id, name, license_class, city, join_date
- `dim_rider`: rider_key, rider_id, name, city, account_tier
- `dim_location`: loc_key, lat, lon, suburb, city, region, country (used twice via role-playing FKs)
- `dim_date`: standard calendar dimension
- `dim_time`: hour, minute, time_of_day_bucket (morning/afternoon/evening/night)

**Key design decisions:**
- `dim_location` is a **role-playing dimension** — pickup and dropoff both point to the same table with different FK names
- `trip_duration_min` and `distance_km` are derived measures stored in the fact (cheaper to pre-compute than re-derive on every query)

---

**Q6 — Conformed dimension**

A **conformed dimension** is a dimension table that is shared identically across multiple star schemas in the same data warehouse.

**Example:** `dim_date` — the same calendar dimension is used by `fact_sales`, `fact_marketing_spend`, and `fact_logistics_shipments`. Every star schema joins to the same `dim_date` table.

**Why it matters:** Without conformed dimensions, "sales in Q1" and "marketing spend in Q1" use different date dimension tables that might disagree on what "Q1" means (fiscal vs. calendar), making cross-subject-area metrics impossible to drill across.

**Rule:** If you can `JOIN fact_sales AND fact_marketing ON dim_date.date_key` and get sensible results, `dim_date` is conformed. A non-conformed dimension makes cross-schema analysis require an ETL bridge.

---

**Q7 — Three fact table types (logistics examples)**

**Transaction fact table:**
One row per atomic event. Rows are never updated.
- Example: `fact_shipment_scans` — one row per barcode scan at each depot. Captures every time a package is touched.

**Periodic snapshot fact table:**
One row per entity per time period. Rows are inserted on a schedule regardless of activity.
- Example: `fact_warehouse_inventory_daily` — one row per SKU per day recording ending stock count. Even days with no movement get a row (stock = yesterday's stock).

**Accumulating snapshot fact table:**
One row per lifecycle instance. The same row is **updated** as the pipeline progresses.
- Example: `fact_order_lifecycle` — one row per order, with columns `order_placed_date`, `warehouse_picked_date`, `shipped_date`, `delivered_date`, `returned_date`. Each date is NULL until that milestone occurs, then filled in.

---

**Q8 — Surrogate keys in dimensional models**

A **surrogate key** is a system-generated integer (SERIAL / IDENTITY) with no business meaning, used as the primary key of a dimension table and as the FK in the fact table.

**Why not use the business key directly?**

1. **SCD Type 2 requires multiple rows per entity.** Customer C001 may have 3 rows in `dim_customer` (original, after city change, after segment change). Each needs a unique PK. The business key (`C001`) cannot be the PK because it repeats. The surrogate key (`1001`, `1045`, `1089`) is unique per version.

2. **Source systems change business key formats.** A customer ID might change from numeric to UUID when a system is migrated. The surrogate key insulates all fact table rows from this change — only `dim_customer` is updated; `fact_sales` FKs remain unchanged.

3. **Cross-system integration.** Multiple source systems may use the same business key for different entities (e.g., two systems both have `product_id = 1001` for different products). Surrogate keys deconflict them.

4. **Performance.** Integer surrogate key joins are faster than string business key joins at scale.

---

## Concept 2: Slowly Changing Dimensions (SCD)

**Q9 — SCD Type 2 vs. Type 1**

**SCD Type 1 (overwrite):** When an attribute changes, the old value is permanently replaced. No history. All historical fact rows now show the new attribute value when joined to the dimension.

**SCD Type 2 (new row):** When an attribute changes, a new dimension row is inserted with the new values. The old row is closed (end-dated). Historical fact rows still point to the old surrogate key — they correctly show the attribute value as it was at the time of the event.

**The problem Type 1 cannot solve:** Historical accuracy. If a customer moves from Sydney to Melbourne and you use Type 1, all their historical Sydney purchases now appear as Melbourne purchases. Revenue attribution, regional sales reports, and trend analysis all become wrong retroactively.

---

**Q10 — SCD Type 2 mechanics**

**Columns to add to the dimension table:**
```sql
ALTER TABLE dim_customer ADD COLUMN valid_from DATE NOT NULL DEFAULT '2000-01-01';
ALTER TABLE dim_customer ADD COLUMN valid_to   DATE NOT NULL DEFAULT '9999-12-31';
ALTER TABLE dim_customer ADD COLUMN is_current BOOLEAN NOT NULL DEFAULT TRUE;
```

**Update process when attribute changes:**
```sql
-- 1. Close the current row
UPDATE dim_customer
SET valid_to = :change_date - 1, is_current = FALSE
WHERE customer_id = :cid AND is_current = TRUE;

-- 2. Insert new row
INSERT INTO dim_customer (customer_id, customer_name, city, valid_from, valid_to, is_current)
VALUES (:cid, :name, :new_city, :change_date, '9999-12-31', TRUE);
```

**Fact table join pattern (correct historical attribution):**
```sql
SELECT c.city, SUM(f.revenue)
FROM fact_sales f
JOIN dim_customer c
  ON f.customer_key = c.customer_key  -- surrogate key join; automatically hits correct version
GROUP BY c.city;
```

The fact table stores the `customer_key` surrogate value that was current **at the time of the sale**. Joining on surrogate key automatically selects the correct historical dimension row.

---

**Q11 — SCD Type 1 misattribution scenario**

**SCD type used:** Type 1 (overwrite). When Alice moved from Sydney to Melbourne in April, the `city` column in `dim_customer` was updated directly.

**Consequence:** All fact rows for Alice (C001) still point to her single `dim_customer` row (no new row was created). That row now shows `city = Melbourne`. So every historical report — including the Q1 report run in May — now attributes Alice's Sydney purchases to Melbourne.

**What should have been used:** SCD Type 2. A new dimension row would have been created for Melbourne with `valid_from = April move date`. The Q1 fact rows would still point to the old surrogate key (Sydney row), so the Q1 report always returns Sydney regardless of when it is run.

---

**Q12 — SCD Type 2 and historical category queries**

After the SCD Type 2 update:
- Product rows sold before March 15 have their fact table `product_key` pointing to the old dimension row where `product_category = 'Electronics'`
- Product rows sold on or after March 15 have their `product_key` pointing to the new dimension row where `product_category = 'Computers'`

**Query behaviour:**
```sql
SELECT SUM(revenue) FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
WHERE p.product_category = 'Electronics';
-- Returns: only pre-March 15 revenue for this product (correct — historical attribution)

WHERE p.product_category = 'Computers';
-- Returns: only post-March 15 revenue (correct — new category attribution)
```

This is exactly the desired behaviour. Pre-change sales are reported under the old category; post-change sales under the new one. The 5-year history of 500M fact rows does not need to be touched — only the dimension table gets a new row.

---

**Q13 — Three email changes with SCD Type 2**

After three email changes, `dim_customer` for this customer has **4 rows** (1 original + 3 new rows, one per change):

```
customer_key | customer_id | email             | valid_from | valid_to   | is_current
1001         | C001        | alice@old.com     | 2023-01-01 | 2023-05-14 | FALSE
1045         | C001        | alice@gmail.com   | 2023-05-15 | 2023-09-29 | FALSE
1089         | C001        | alice@company.com | 2023-09-30 | 2023-12-19 | FALSE
1132         | C001        | alice@new.com     | 2023-12-20 | 9999-12-31 | TRUE
```

**Key points:**
- Each period's fact rows point to the surrogate key that was active at the time of the event
- `is_current = TRUE` identifies the one currently active row for lookups that don't need history
- You can query which email address was associated with any purchase by joining on surrogate key or using date range filtering

---

**Q14 — Risk of `valid_to = '9999-12-31'` sentinel**

**Risk:** Future-proof queries that use `BETWEEN` or `<=` on `valid_to` must be carefully written to exclude the sentinel. If a developer writes `WHERE valid_to < '2025-01-01'`, they accidentally exclude all current rows (which have `9999-12-31`). This is a common source of "missing data" bugs.

**Alternative: `is_current` flag only (no `valid_to`):**
```sql
-- Find current dimension row
WHERE is_current = TRUE

-- Find historical row for a point-in-time join
WHERE valid_from <= :event_date
ORDER BY valid_from DESC LIMIT 1
```

**Trade-off of `is_current` only:**
- Faster for "current value" lookups (simple boolean filter vs. date comparison)
- Harder for range-based historical joins (cannot use BETWEEN; must use a self-join or window function)
- Both `is_current` + `valid_from`/`valid_to` together is the most robust: Boolean for current lookups, dates for historical joins

---

**Q15 — dbt SCD Type 2 implementation logic**

**Source:** Daily full extract of `customers` (all current rows, no history).

**Logic (pseudocode):**

```
Step 1 — Load today's snapshot into staging: stg_customers_today

Step 2 — Identify NEW records (in source, not in dim_customer at all):
  SELECT s.*
  FROM stg_customers_today s
  LEFT JOIN dim_customer d ON s.customer_id = d.customer_id
  WHERE d.customer_id IS NULL
  → INSERT these as new rows with valid_from = today, valid_to = '9999-12-31', is_current = TRUE

Step 3 — Identify CHANGED records (exist in dim as current but attributes differ):
  SELECT s.*
  FROM stg_customers_today s
  JOIN dim_customer d
    ON s.customer_id = d.customer_id AND d.is_current = TRUE
  WHERE s.city != d.city OR s.segment != d.segment  -- or use hash_diff
  →  UPDATE old dim row: is_current = FALSE, valid_to = today - 1
  →  INSERT new dim row: is_current = TRUE, valid_from = today

Step 4 — UNCHANGED records: do nothing (avoid unnecessary new surrogate keys)
  Detect by comparing MD5(all attributes) between source and current dim row
```

**In dbt:** The `dbt_utils.surrogate_key` macro + `snapshots` feature handles this natively:
```sql
-- dbt snapshot (snapshots/snp_customers.sql)
{% snapshot snp_customers %}
  {{ config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['city', 'segment', 'email']
  ) }}
  SELECT * FROM {{ source('raw', 'customers') }}
{% endsnapshot %}
```
dbt snapshots automatically manage `dbt_scd_id`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`.

---

## Concept 3: Data Vault Modelling

**Q16 — Three Data Vault building blocks**

| Building block | Stores |
|---|---|
| **Hub** | Unique business keys from source systems — one row per distinct real-world entity |
| **Link** | Relationships between hubs (associations between entities) — one row per unique relationship instance |
| **Satellite** | Descriptive attributes and their history — one row per attribute set version per hub or link |

---

**Q17 — Hash keys vs. surrogate keys in Data Vault**

**Surrogate keys (SERIAL/IDENTITY)** are generated by the database — they are local to one database instance and are assigned sequentially. If you load in parallel across multiple servers or restore a backup and reload, the same business key gets a different surrogate key each time. Cross-environment consistency is impossible.

**Hash keys (MD5/SHA-256 of the business key)** are deterministic — the same business key always produces the same hash, regardless of which server computed it, in which order, and at what time.

This enables:
- **Parallel loading** across multiple servers without coordination (no SEQUENCE bottleneck)
- **Cross-environment consistency** (dev/staging/prod all generate the same hash keys)
- **Idempotent loads** — loading the same row twice produces the same hash key, detected as a duplicate, and skipped

---

**Q18 — Cross-system identity resolution in Data Vault**

**In a dimensional model:** You must resolve the identity conflict (same customer in two systems) before loading `dim_customer`. You need a mapping table and ETL logic that says "System A customer 1001 = System B CUST-A001 = surrogate key 5001". This golden record resolution happens before the warehouse.

**In a Data Vault:** Each system's business key gets its own Hub row:
```
hub_customer:
  hash(1001 || System_A)    → customer_id=1001,    record_source=System_A
  hash(CUST-A001 || System_B) → customer_id=CUST-A001, record_source=System_B
```

Identity resolution is handled by a **Same-As Link (SAL)** or **Bridge table**:
```sql
CREATE TABLE link_customer_same_as (
    lnk_sa_hk         CHAR(32) PRIMARY KEY,
    hub_customer_hk_a  CHAR(32),   -- System A hash key
    hub_customer_hk_b  CHAR(32),   -- System B hash key
    load_date          TIMESTAMPTZ,
    record_source      TEXT
);
```

This defers the "are these the same person?" decision to the Business Vault layer, with a full audit trail of when and why the link was established.

---

**Q19 — `hash_diff` in a Data Vault satellite**

The `hash_diff` is an MD5 or SHA-256 hash of all descriptive attribute columns in the satellite row (concatenated in a fixed order).

**Purpose:** Before inserting a new satellite row during a load, compare the incoming `hash_diff` with the current satellite row's `hash_diff`. If they are equal, the attributes have not changed → skip the insert. If different → close the old row, insert a new one.

**Why it matters for performance:**
- Without `hash_diff`: you must compare every individual column (name, email, city, segment, phone...) in the INSERT SELECT logic. Complex and slow for wide satellites.
- With `hash_diff`: one column comparison (`incoming.hash_diff != current.hash_diff`) replaces all attribute comparisons. The hash is pre-computed at load time, making the detection O(1) regardless of satellite width.

---

**Q20 — Source system changes business key format**

**Impact on the Hub:**
- The existing hub rows have hash keys computed from the old numeric format
- New loads will compute hash keys from the UUID format for the "same" entities
- These will produce **different hash keys** → new hub rows will be inserted — the Data Vault thinks they are new entities

**What to do:**
1. **Do not delete existing hub rows** — they are the audit record of what was loaded before the key change
2. Create a **Same-As Link** mapping old hash keys to new hash keys (same identity, different business key format)
3. Load new data under the UUID business keys going forward
4. The Business Vault's identity resolution layer handles the mapping when building query-facing marts

This is a key advantage of Data Vault over dimensional models: the raw vault is append-only and immutable. Schema/key changes in the source are absorbed without rewriting history.

---

## Concept 4: One Big Table (OBT) & Denormalisation

**Q21 — What is OBT and what problem does it solve**

A **One Big Table (OBT)** pre-joins all dimension attributes into a single wide flat table alongside fact measures. There are no foreign keys; all context columns (customer name, product category, store region, order date) are stored inline with every fact row.

**Problem it solves:** Non-technical BI users and self-service analytics tools (Tableau, Metabase, Looker) cannot write SQL JOIN statements. An OBT presents all data in one table — users just drag-and-drop columns to filter and aggregate with zero query authorship required.

---

**Q22 — OBT vs. star schema trade-offs**

**Trade-off 1 — Write amplification:**
In a star schema, a product category change updates one row in `dim_product`. In an OBT, the same change requires updating every fact row for that product — potentially millions of rows. The OBT has O(N) write cost for a dimension change; the star schema has O(1).

**Trade-off 2 — Historical accuracy vs. query simplicity:**
A star schema with SCD Type 2 correctly attributes historical fact rows to the attribute value that was current at the time. An OBT bakes in the dimension values at build time — if you rebuild the OBT today, it uses today's attributes for all historical rows, losing historical attribution.

---

**Q23 — 50,000 rows must be updated in OBT**

When Alice moves from Sydney to Melbourne, her `customer_city` column needs updating in every OBT row she appears in — all 50,000 order rows.

**Performance implication:** 50,000 row updates in a columnar store (Parquet on S3) are extremely expensive — you must rewrite entire Parquet files because Parquet is immutable. In a Delta Lake OBT table, this triggers a MERGE that rewrites all affected files. At scale (millions of customers × thousands of orders each), a dimension change can trigger a full table rewrite.

**Star schema + SCD Type 2 comparison:** The dimension change adds 1 new row to `dim_customer` and closes 1 old row. The 50,000 fact rows are **not touched at all**. They still point to the old surrogate key (Sydney row), which remains in `dim_customer` with `is_current = FALSE`.

---

**Q24 — Columnar compression reduces OBT storage penalty**

In a **row-oriented database (PostgreSQL):** Every row stores the full string `'SoundMax'` in memory and on disk. 1M rows with `product_brand = 'SoundMax'` store 1M copies of the 8-byte string, taking ~8 MB just for that one value.

In a **columnar format (Parquet):**
- The `product_brand` column is stored as a contiguous byte sequence: `SoundMax, SoundMax, SoundMax, ...`
- Parquet applies **dictionary encoding**: the string `'SoundMax'` is stored once in a dictionary; each row stores a 1-byte integer index into that dictionary
- 1M rows of `'SoundMax'` compress to: 8 bytes (the string) + 1M × 1 byte (index) ≈ 1 MB vs. 8 MB
- If `'Electronics'` appears across multiple columns (category AND subcategory), each is independently dictionary-encoded

The storage penalty of repeating dimension values in an OBT is 5–20× smaller in Parquet than in a row store, making OBT practical in a modern lakehouse.

---

**Q25 — When to choose OBT over star schema**

1. **Self-service BI by non-SQL users:** If the primary consumer is a drag-and-drop BI tool (Tableau, Metabase) operated by non-engineers, OBT eliminates join errors and simplifies the semantic layer.

2. **Read-heavy, rarely updated dimensions:** If dimension attributes almost never change (e.g., product category is fixed at creation), the write amplification risk of OBT is low. Stable dimensions make OBT maintenance tractable.

3. **Single fact table, single use case:** If there is only one subject area (e.g., marketing spend analysis) with one fact table and known, stable dimensions, OBT's duplication overhead is bounded. Multiple fact tables sharing dimensions benefit far more from a star schema.

---

## Concept 5: dbt — Data Build Tool

**Q26 — What dbt does**

dbt handles the **T (Transform)** in ELT. It does not extract data from sources and does not load raw data into the warehouse — that is done by Fivetran, Airbyte, Kafka, or custom ingestion scripts.

dbt takes data that is already in the warehouse and transforms it using SQL `SELECT` statements. It compiles those SELECTs into `CREATE TABLE AS` or `CREATE VIEW AS` DDL, runs them in dependency order (DAG), and tests the output.

**What dbt does:**
- Transform raw → staging → intermediate → marts
- Manage dependencies between models
- Run data quality tests
- Generate documentation and lineage

**What dbt does not do:**
- Move data from source systems to the warehouse
- Run imperative Python transformations (though dbt Python models are an emerging exception)
- Schedule itself (needs Airflow, dbt Cloud Scheduler, or cron)

---

**Q27 — Four dbt materialisation types**

| Materialisation | What dbt runs | Best for |
|---|---|---|
| `view` | `CREATE OR REPLACE VIEW AS SELECT ...` | Staging models — always fresh, zero storage cost, rebuilds instantly |
| `table` | `DROP TABLE; CREATE TABLE AS SELECT ...` | Marts with complex logic — pre-computed for fast BI queries; full rebuild each run |
| `incremental` | `INSERT INTO ... (new rows only)` or `MERGE` | Large fact tables — process only rows newer than the last run; avoids full reprocessing |
| `ephemeral` | Injected as a CTE into downstream models | Intermediate logic not worth persisting to disk; no table created, no storage consumed |

---

**Q28 — Late-arriving data with incremental filter**

**Filter used:** `WHERE order_date >= (SELECT MAX(order_date) FROM {{ this }})`

**Problem:** This filter uses `order_date` (the business date of the order), not `loaded_at` (when the row arrived in the source). A late-arriving order with `order_date = yesterday` but `loaded_at = today` is missed because `MAX(order_date)` in the target is already set to today — yesterday's business date is less than the max, so the filter excludes it.

**Standard fix — use a lookback window:**
```sql
{% if is_incremental() %}
  WHERE loaded_at >= (SELECT MAX(loaded_at) FROM {{ this }}) - INTERVAL '3 days'
{% endif %}
```
This reprocesses the last 3 days of data on every run, catching late arrivals. The `unique_key` parameter then handles deduplication — existing rows are updated, not duplicated.

---

**Q29 — Column rename breaks downstream model**

When `dbt run` executes `fct_revenue`, it runs the SQL which references `dim_customer.customer_city`. If the column was renamed to `city` and the change was applied to `dim_customer`'s SQL file, the compiled SQL for `fct_revenue` will fail at the database level with a "column not found" error.

**How `dbt test` catches this — partially:**
- Column-level `not_null` and `unique` tests on `customer_city` in the YAML will fail with "column not found" at test time
- But `dbt test` runs **after** `dbt run` — the build would already have failed

**The right prevention:** Add a `relationships` test or a `not_null` test on `fct_revenue.customer_city` in `_schema.yml`. A CI pipeline that runs `dbt build` (run + test) on every PR will catch the break before merge. Additionally, using `{{ ref() }}` means dbt knows `fct_revenue` depends on `dim_customer` — a `dbt ls --select dim_customer+` will show all downstream models at risk.

---

**Q30 — `{{ ref() }}` vs. hardcoded table names**

`{{ ref('dim_customer') }}` tells dbt that the current model depends on `dim_customer`. This creates three benefits hardcoding cannot match:

1. **DAG construction:** dbt builds a dependency graph from all `ref()` calls. It runs models in the correct order automatically — `dim_customer` builds before `fct_revenue`.

2. **Environment resolution:** `ref('dim_customer')` resolves to the correct database/schema for the environment (dev: `analytics_dev.dim_customer`, prod: `analytics.dim_customer`). Hardcoded names break when running in dev.

3. **State detection:** `dbt run --select dim_customer+` identifies and rebuilds all models downstream of `dim_customer`. This is only possible because dbt knows the dependency graph via `ref()`.

---

**Q31 — `unique_key` in incremental models**

`unique_key = 'order_id'` tells dbt that `order_id` uniquely identifies a row. On an incremental run, dbt generates either a `DELETE + INSERT` or a `MERGE` (depending on the adapter):

**PostgreSQL / Redshift (DELETE + INSERT):**
```sql
DELETE FROM fct_sales WHERE order_id IN (SELECT order_id FROM fct_sales__dbt_tmp);
INSERT INTO fct_sales SELECT * FROM fct_sales__dbt_tmp;
```

**Snowflake / BigQuery (MERGE):**
```sql
MERGE INTO fct_sales AS target
USING fct_sales__dbt_tmp AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

**If two source rows have the same `order_id`:** The staging temp table (`fct_sales__dbt_tmp`) will contain both rows. The MERGE will process one and the DELETE+INSERT pattern may insert two rows. This is a data quality issue in the source that must be resolved with deduplication in the incremental model's SELECT (e.g., `SELECT DISTINCT ON (order_id) ...` or `ROW_NUMBER()` windowing before the MERGE).

---

**Q32 — Staging models as views**

**The reasoning — agree, with conditions:**
- Staging models are thin wrappers (rename, cast, coalesce) — no expensive computation
- As views, they always reflect the current state of raw tables without a scheduled rebuild
- Zero storage cost and zero maintenance cost
- Downstream models materialised as tables or incrementals consume the view at build time and get the freshest data

**When to break the rule:**
1. **Raw tables are extremely large** (100M+ rows) and staging transformations are expensive (regex, JSON parsing): materialise staging as a table to avoid recomputing on every downstream model build
2. **Multiple downstream models reference the same staging model**: each `dbt run` re-executes the view for each consumer; for expensive views, materialising as a table is cheaper than repeated execution
3. **dbt Cloud concurrent execution**: if 10 downstream models run in parallel and each queries the staging view, the raw table gets 10 concurrent scans — materialising stages the data once

---

## Mixed / Senior-Level Questions

**Q33 — Architecture recommendation for a startup**

**Recommendation: Option A — Replicate to Snowflake + dbt star schema.**

**Reasoning:**
- At 15 tables and 10 GB today, the complexity is low enough that either A or B would work now — but the recommendation is about the 10 TB future state
- Direct analytics on PostgreSQL (Option B OBT) adds analytical load to the production database — a risk at any size, catastrophic at 10 TB
- Data Vault (Option C) is massive over-engineering for a startup: it requires significant implementation time, specialist expertise, and the multi-source integration problem it solves does not yet exist

**What would change the recommendation:**
- If analysts refuse to use Snowflake or there is no budget → B (OBT in PostgreSQL read replica) temporarily
- If they immediately have 10+ data sources and regulatory audit requirements → C (Data Vault)
- If they are already on BigQuery or Databricks → A, but choose the native table format rather than Snowflake

---

**Q34 — The fan-out problem**

The fan-out problem occurs when joining a fact table to a dimension that has a one-to-many relationship with the fact grain, causing rows to be multiplied and aggregates to be double-counted.

**Example:**
- `fact_orders` grain: one row per order
- `dim_order_promotions`: one order can have multiple promotions applied

```sql
SELECT SUM(revenue) FROM fact_orders f
JOIN dim_order_promotions p ON f.order_id = p.order_id;
-- If order 1001 has 3 promotions, it appears 3 times in the result
-- SUM(revenue) is triple-counted for that order
```

**Fix:** The promotion relationship should be in a separate bridge/fact table (many-to-many relationship), not a dimension. Or: aggregate promotions before joining. Or: move to a fact table with grain = one row per order-promotion combination (and adjust all other metrics accordingly).

---

**Q35 — `customer_lifetime_value` does not belong in the fact table**

**Why it is wrong in `fact_orders`:**
- LTV is a measure of a customer entity, not a property of an individual order event
- The fact table grain is one row per order — LTV would repeat identically on every order row for the same customer
- When LTV is recalculated (weekly batch), you would need to update millions of fact rows rather than one dimension row
- LTV is a derived aggregate, not an atomic measurement of the event that the fact row represents

**Where it belongs:** In a customer-level aggregate table or in `dim_customer` as a regularly updated attribute (SCD Type 1 — no need to preserve history if LTV changes are expected). Or in a separate `customer_metrics` mart table: `customer_id, ltv, ltv_as_of_date, churn_probability, ...`

---

**Q36 — Identifying the canonical table from four versions**

**Investigation steps:**
1. Check row counts: `SELECT COUNT(*) FROM orders` — the canonical table is usually the largest
2. Check `MAX(updated_at)` or `MAX(created_at)` across all four: the most recently updated is likely most current
3. Query information_schema or pg_stat_user_tables for last modification time
4. `git log` or dbt lineage: which table does the dbt mart reference via `{{ ref() }}`? That is the canonical source
5. Talk to the team who owns the pipeline

**Governance practices that prevent this:**
- dbt naming conventions: staging (`stg_`), intermediate (`int_`), and mart (`fct_`, `dim_`) prefixes make naming unambiguous
- Data catalog: every table must have an owner, purpose, and freshness documented before it goes to production
- Access control: only the pipeline service account can create tables in the `analytics` schema — analysts cannot create `orders_final` by running ad-hoc SQL in prod
- Deprecation process: old tables are moved to an `archive_` schema and documented before deletion, not left alongside the replacement

---

**Q37 — Adding a `region` column to a 500M-row fact table without reprocessing**

**Strategy: Add to the dimension, not the fact.**

Add `region` to `dim_customer` (derived from `customer_city` via a lookup table):
```sql
-- One-time update to dim_customer
UPDATE dim_customer c
SET region = lr.region
FROM city_region_lookup lr
WHERE c.city = lr.city;
```

For SCD Type 2 dimensions, add `region` to the satellite/dimension and backfill existing rows in `dim_customer` (not in `fact_orders`).

All existing `fact_orders` rows already join to `dim_customer` — they instantly gain access to `region` at query time without any changes to the 500M-row fact table:
```sql
SELECT c.region, SUM(f.revenue)
FROM fact_orders f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.region;
-- Works immediately; fact table untouched
```

**This is the entire point of the star schema:** adding new dimension attributes requires only a dimension update, never a fact table rewrite.

---

**Q38 — Degenerate dimension**

A **degenerate dimension** is a dimension attribute that has no corresponding dimension table — it is stored directly in the fact table.

**Example:** `order_id` on a `fact_order_items` table. The grain is order line item, so `order_id` is a natural grouping key (you want to `GROUP BY order_id` to see totals per order). But `order_id` is just a transaction identifier — it has no descriptive attributes worth putting in a dimension table. Creating a `dim_order` with just `order_id` would be meaningless.

**Why it stays in the fact table:**
- It is a business key with operational meaning (appears on invoices, shipping labels, customer emails) but no dimensional descriptive value
- It is used to GROUP and filter in queries, so it must be accessible
- Other examples: `invoice_number`, `ticket_id`, `transaction_reference`, `session_id`

---

**Q39 — dbt `--select` graph operators**

| Operator | Syntax | What it selects | When to use |
|---|---|---|---|
| Ancestors | `+fct_sales` | `fct_sales` and all models it depends on (upstream) | Rebuild the full dependency chain — when you changed a staging model and want everything downstream to be fresh |
| Descendants | `fct_sales+` | `fct_sales` and all models that depend on it (downstream) | After changing `fct_sales` itself — verify everything built on top of it still works |
| Both directions | `@fct_sales` | All ancestors AND all descendants of `fct_sales` | Full blast — rebuild the whole connected subgraph; use for deep refactors |

**Scenario examples:**
- You changed `stg_orders.sql`: run `dbt run --select stg_orders+` to rebuild stg_orders and everything downstream
- You changed `dim_product.sql`: run `dbt run --select +dim_product` to check that dim_product's sources are up to date, then `dim_product+` to rebuild it and its consumers
- You are debugging `fct_sales` and want to rebuild only it: `dbt run --select fct_sales` (no operator — exact model only)

---

**Q40 — Missing test category: volume / freshness anomaly tests**

**What's missing:** The existing `not_null` and `unique` tests verify structural data quality — individual columns are populated and distinct. They do not detect **business-level anomalies** such as:
- Revenue is 15% lower than the 7-day average
- Row count dropped from 500K/day to 200K/day
- A specific product category disappeared from results entirely

This class of test is called **anomaly detection** or **volumetric/distribution tests**.

**How to add them in dbt:**

```yaml
# Using dbt-expectations or dbt-utils packages
models:
  - name: fct_sales
    tests:
      # Row count within expected range
      - dbt_utils.recency:
          datepart: day
          field: order_date
          interval: 1   # table should have data from the last 1 day
      # Revenue within historical range
      - dbt_expectations.expect_column_mean_to_be_between:
          column_name: revenue
          min_value: 80    # alert if average order revenue drops below $80
          max_value: 300
```

**Or with a custom singular test:**
```sql
-- tests/assert_daily_revenue_not_anomalous.sql
WITH daily_revenue AS (
    SELECT order_date, SUM(revenue) AS daily_rev
    FROM {{ ref('fct_sales') }}
    WHERE order_date >= CURRENT_DATE - 14
    GROUP BY order_date
),
stats AS (
    SELECT AVG(daily_rev) AS avg_rev, STDDEV(daily_rev) AS std_rev
    FROM daily_revenue
    WHERE order_date < CURRENT_DATE
)
SELECT order_date, daily_rev
FROM daily_revenue, stats
WHERE order_date = CURRENT_DATE
  AND daily_rev < avg_rev - 2 * std_rev  -- alert if today's revenue is 2 std devs below average
-- Returns rows (test fails) if revenue is anomalously low
```

---
