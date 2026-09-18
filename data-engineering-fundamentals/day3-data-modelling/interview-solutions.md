# Day 3 — Interview Solutions: Data Modelling

> Complete answers for all 30 questions in `interview-questions.md`.

---

## Concept 1: Dimensional Modelling & Star Schema

**Q1 — Fact table vs. dimension table**

A **fact table** stores measurable, numeric business events — one row per event. Example: `fact_sales` with `order_id`, `quantity`, `unit_price`, `discount_pct`. The numbers are what you aggregate (`SUM`, `AVG`, `COUNT`).

A **dimension table** stores the descriptive context of those events — who, what, where, when. Example: `dim_customer` with `customer_name`, `city`, `country`, `segment`. Dimensions are used for filtering (`WHERE country = 'AU'`) and grouping (`GROUP BY category`), never for aggregation.

**Rule:** "How much / how many?" → fact. "Which one / what kind?" → dimension.

---

**Q2 — The grain**

The **grain** is a precise one-sentence declaration of what one row in the fact table represents. It must be defined before any column is chosen.

**Why first:** Every column either measures the event (goes in the fact) or describes who/what/where (goes in a dimension). An ambiguous grain causes columns to be mis-assigned, leading to double-counted aggregates that are silent and hard to detect.

**Example:** "One row represents one product line item within one customer order." A different grain — "one order" — cannot hold per-product columns without violating the design.

---

**Q3 — Star Schema vs. Snowflake Schema**

**Star Schema:** Dimensions are fully denormalised. `dim_product` contains `product_name`, `category`, `subcategory`, `brand` on one row — one join per dimension.

**Snowflake Schema:** Dimensions are normalised — `dim_product` has a FK to `dim_category`, `dim_category` to `dim_department` — two or more joins per dimension.

**Recommendation: Star Schema for analytics.** At columnar storage costs, Snowflake Schema's storage savings are negligible. The extra joins add query complexity, confuse BI tools that auto-generate SQL, and slow ad-hoc analysis. Only consider Snowflake Schema when a very large sub-dimension is shared across many schemas — rare in practice.

---

**Q4 — Wrong grain scenario**

If `fact_sales` has one row per order (header grain), there is no per-product breakdown. "Revenue per product category" requires line-item level data — which product was in each order at what quantity — which does not exist at the order grain.

**Fix:** Change the grain to **order line item** — one row per product within each order:
```sql
fact_sales: order_id, line_item_id, product_key, customer_key, date_key, quantity, unit_price
```
Now `GROUP BY product_category` works because each row has exactly one `product_key`.

---

**Q5 — Surrogate keys vs. business keys**

A **surrogate key** is a system-generated integer with no business meaning, used as the PK of a dimension and FK in the fact table.

**Two scenarios where business keys fail:**

1. **SCD Type 2:** Customer C001 may have 3 rows in `dim_customer` (one per city change). Business key `C001` repeats — it cannot be the PK. Surrogate keys (`1001`, `1045`, `1089`) uniquely identify each version.

2. **Source system migration:** A company migrates from numeric IDs to UUIDs. The surrogate key in the warehouse never changes — only `dim_customer` is updated. All 500M fact rows keep pointing to the same surrogate keys, requiring no reprocessing.

---

**Q6 — Conformed dimension**

A **conformed dimension** is shared identically across multiple star schemas in the same warehouse.

**Example:** `dim_date` is used by `fact_sales`, `fact_marketing_spend`, and `fact_logistics_shipments`. Every schema joins to the same table with the same `date_key`.

**Why it matters:** Without conformed dimensions, "sales in Q1" and "marketing spend in Q1" might use date tables with different fiscal vs. calendar definitions — making cross-domain queries impossible. With conformed dimensions, you can `JOIN fact_sales AND fact_marketing ON dim_date.date_key` and get meaningful cross-subject results.

---

**Q7 — Three fact table types (logistics examples)**

**Transaction fact:** One row per atomic event; never updated.
- `fact_shipment_scans` — one row every time a package is scanned at a depot.

**Periodic snapshot:** One row per entity per time period; inserted on schedule regardless of activity.
- `fact_warehouse_inventory_daily` — one row per SKU per day with closing stock count. Days with no movement still get a row.

**Accumulating snapshot:** One row per lifecycle instance; the same row is updated as milestones occur.
- `fact_order_lifecycle` — one row per order with columns `order_placed_date`, `picked_date`, `shipped_date`, `delivered_date`. Each date is NULL until the milestone is reached, then filled in.

---

**Q8 — Ride-sharing fact table design**

**Grain:** One completed trip.

```
fact_trips:
  trip_key           INT PK (surrogate)
  trip_id            INT        -- degenerate dimension (transaction ID, no dim table)
  driver_key         INT FK → dim_driver
  rider_key          INT FK → dim_rider
  pickup_loc_key     INT FK → dim_location   -- role 1: pickup
  dropoff_loc_key    INT FK → dim_location   -- role 2: dropoff (same dim, different FK)
  start_date_key     INT FK → dim_date
  start_time_key     INT FK → dim_time
  fare               NUMERIC
  surge_multiplier   NUMERIC
  trip_duration_min  INT
  distance_km        NUMERIC
  rider_rating       SMALLINT   -- NULL if not rated
  driver_rating      SMALLINT   -- NULL if not rated
```

**Role-playing dimension:** `dim_location` is used twice — once for pickup, once for dropoff. One physical table, two logical roles, two FK column names. This avoids duplicating the geography dimension.

---

**Q9 — Degenerate dimension**

A **degenerate dimension** is a dimension key with operational meaning stored directly in the fact table, with no corresponding dimension table.

**Retail example:** `order_id` in `fact_order_items`. The grain is order line item, so `order_id` is a natural grouping key — you want `GROUP BY order_id` to see per-order totals. But `order_id` has no descriptive attributes worth a separate `dim_order` table. Creating `dim_order(order_id)` with a single column adds a join with zero analytical benefit.

Other examples: `invoice_number`, `ticket_id`, `session_id`, `transaction_reference`.

---

**Q10 — Derived columns in fact tables**

**What is wrong:** Storing `revenue = quantity * unit_price` and `net_revenue = revenue * (1 - discount_pct/100)` as columns means:
- If the `revenue` formula changes (e.g., add tax), both columns must be recalculated and all historical rows reprocessed
- Derived columns can get out of sync (pipeline bug updates `revenue` but not `net_revenue`) → silent data corruption
- Business definition changes require a schema migration, not just a view update

**Correct design:** Store only atomic facts (`quantity`, `unit_price`, `discount_pct`). Derive everything in a view or Gold model:
```sql
CREATE VIEW v_sales AS
SELECT *,
    quantity * unit_price AS revenue,
    quantity * unit_price * (1 - discount_pct / 100.0) AS net_revenue
FROM fact_sales;
```
Changing the formula updates one view definition — no historical rows touched.

---

## Concept 2: Slowly Changing Dimensions (SCD)

**Q11 — SCD Type 2 vs. Type 1**

**SCD Type 1 (overwrite):** Old value is permanently replaced. All historical fact rows now reflect the new attribute when joined.

**SCD Type 2 (new row):** A new dimension row is inserted for each change. Old rows are closed (end-dated). Historical fact rows still point to the old surrogate key and correctly show the attribute value as it was at the time of the event.

**The problem Type 1 cannot solve:** Historical accuracy. If a customer moves and you overwrite, all their historical sales are now attributed to the new city. Regional trend analysis, cohort reports, and anything time-sensitive become permanently wrong — with no way to recover.

---

**Q12 — SCD Type 2 mechanics**

**Columns to add:**
```sql
ALTER TABLE dim_customer ADD COLUMN valid_from  DATE    NOT NULL DEFAULT '2000-01-01';
ALTER TABLE dim_customer ADD COLUMN valid_to    DATE    NOT NULL DEFAULT '9999-12-31';
ALTER TABLE dim_customer ADD COLUMN is_current  BOOLEAN NOT NULL DEFAULT TRUE;
```

**Update process when attribute changes:**
```sql
-- 1. Close the current row
UPDATE dim_customer
SET valid_to = :change_date - INTERVAL '1 day',
    is_current = FALSE
WHERE customer_id = :cid AND is_current = TRUE;

-- 2. Insert the new version
INSERT INTO dim_customer (customer_id, customer_name, city, valid_from, valid_to, is_current)
VALUES (:cid, :name, :new_city, :change_date, '9999-12-31', TRUE);
```

**Fact table join:** The fact table stores the surrogate `customer_key` that was current at the time of the sale. Joining on the surrogate key automatically selects the correct historical version — no date filtering needed in the fact query.

---

**Q13 — SCD Type 1 misattribution**

**SCD type used:** Type 1. Alice's `city` column was overwritten when she moved to Melbourne.

**Consequence:** There is now only one dimension row for Alice showing `city = Melbourne`. All historical fact rows for Alice (Q1 sales made while she was in Sydney) point to this single row. The Q1 report run in May returns Melbourne — which is wrong. The Sydney sales are permanently misattributed with no way to recover the correct answer.

**Correct type:** SCD Type 2. A new row (`city = Melbourne`, `valid_from = April move date`) would be inserted. Q1 fact rows still point to the old surrogate key (Sydney row), so the Q1 report always returns Sydney regardless of when it is run.

---

**Q14 — SCD Type 2 and category queries**

After applying SCD Type 2:
- Pre-March 15 fact rows have `product_key` → old dim row where `category = 'Electronics'`
- Post-March 15 fact rows have `product_key` → new dim row where `category = 'Computers'`

`WHERE product_category = 'Electronics'` returns only pre-March 15 revenue. `WHERE product_category = 'Computers'` returns only post-March 15 revenue. Historical attribution is preserved correctly.

**Fact rows modified:** Zero. The 500M fact rows are never touched. Only `dim_product` receives one new row (current) and one closed row.

---

**Q15 — Three email changes**

After three changes, `dim_customer` has **4 rows** for this customer:

```
customer_key | customer_id | email             | valid_from | valid_to   | is_current
1001         | C001        | alice@old.com     | 2023-01-01 | 2023-05-14 | FALSE
1045         | C001        | alice@gmail.com   | 2023-05-15 | 2023-09-29 | FALSE
1089         | C001        | alice@company.com | 2023-09-30 | 2023-12-19 | FALSE
1132         | C001        | alice@new.com     | 2023-12-20 | 9999-12-31 | TRUE
```

**How fact rows stay accurate:** The fact table stores the surrogate `customer_key` that was active at the time of the order — e.g., orders from June 2023 store `customer_key = 1045` (gmail period). Joining on surrogate key returns the correct email for that period automatically, with no fact row updates needed.

---

**Q16 — Risk of `valid_to = '9999-12-31'` sentinel**

**Risk:** A developer writing `WHERE valid_to < '2025-01-01'` accidentally excludes all current rows (sentinel date is beyond 2025), producing silent "missing data" bugs.

**Alternative: `is_current` boolean only:**
- Pro: Clean current-row filter — `WHERE is_current = TRUE`
- Con: Point-in-time historical joins require a self-join or window function instead of a simple `BETWEEN`

**Best practice:** Use both. `is_current` for current-value lookups; `valid_from`/`valid_to` for historical range joins. The slight redundancy is intentional and well worth the query clarity.

---

**Q17 — Daily snapshot → SCD Type 2 pipeline**

```
Input:  stg_customers_today  (all current customer rows from source)
Target: dim_customer         (SCD Type 2 with valid_from, valid_to, is_current)

Step 1 — Detect NEW records (in source, not yet in dim):
  SELECT s.*
  FROM stg_customers_today s
  LEFT JOIN dim_customer d ON s.customer_id = d.customer_id
  WHERE d.customer_id IS NULL
  → INSERT with valid_from = today, valid_to = '9999-12-31', is_current = TRUE

Step 2 — Detect CHANGED records (in dim as current but attributes differ):
  SELECT s.*
  FROM stg_customers_today s
  JOIN dim_customer d ON s.customer_id = d.customer_id AND d.is_current = TRUE
  WHERE MD5(s.city || '|' || s.segment) != MD5(d.city || '|' || d.segment)
  → UPDATE old row: is_current = FALSE, valid_to = today - 1
  → INSERT new row: is_current = TRUE, valid_from = today

Step 3 — UNCHANGED records: skip (no new surrogate key generated)
  Same MD5 hash → do nothing
```

**Key detail:** Using `MD5` (hash_diff) of all tracked attributes detects any change in one comparison. Unchanged rows produce no new surrogate key — preventing "surrogate key explosion" where the dim grows needlessly.

---

**Q18 — Revenue during Sydney period (as-of join)**

```sql
SELECT SUM(f.quantity * f.unit_price * (1 - f.discount_pct / 100.0)) AS sydney_net_revenue
FROM fact_sales f
JOIN dim_customer c
  ON f.customer_key = c.customer_key   -- surrogate key join hits the exact version
WHERE c.customer_id = 'C001'
  AND c.city = 'Sydney';
-- The surrogate key already pins the join to the Sydney-period dim row.
-- No date range needed — surrogate key IS the version selector.
```

Alternatively, if the fact stores `customer_id` (business key) instead of surrogate:
```sql
JOIN dim_customer c
  ON f.customer_id = c.customer_id
  AND f.order_date BETWEEN c.valid_from AND c.valid_to
  AND c.city = 'Sydney'
```

---

**Q19 — SCD Type 1 vs. 2 vs. 3 vs. 4**

| Type | Mechanism | Use when | Fails when |
|---|---|---|---|
| Type 1 — Overwrite | Replace old value | Attribute was wrong (data fix), history not needed | History matters (any analytical use case) |
| Type 2 — New row | Insert new version, close old | History needed for accurate reporting | High-frequency changes (price changes hourly) — dimension explodes |
| Type 3 — New column | Add `previous_value` column | Only two-period comparison needed (current vs. previous) | More than one prior value must be tracked |
| Type 4 — History table | Current values in main dim; all history in separate table | Mostly current lookups, occasional history audit | Queries spanning both current and historical need two-table joins |

---

**Q20 — The no-change problem**

If a daily snapshot delivers a row with identical attributes to the current dim row and you don't detect it:
- A new surrogate key is generated (`customer_key = 1133` for C001 even though nothing changed)
- The old row is closed with `valid_to = yesterday`
- Fact rows loaded today get the new (unnecessary) surrogate key
- Historical fact rows have the old surrogate key
- A query joining on surrogate key misses today's fact rows when filtering by city — silent data split

**Detection:** Compare `MD5(all_tracked_attribute_columns)` between the incoming source row and the current dim row. If hashes match → skip. This is called `hash_diff` in Data Vault terminology and is standard practice in SCD Type 2 pipelines.

```sql
-- Skip if unchanged
WHERE MD5(src.city || '|' || src.segment || '|' || src.email)
   != MD5(dim.city || '|' || dim.segment || '|' || dim.email)
```

---

## Concept 3: Normalisation, Normal Forms & Denormalisation Trade-offs

**Q21 — 1NF, 2NF, 3NF violations**

**1NF — non-atomic value:**
```sql
orders (order_id, products)
-- ('1001', 'Headphones, Shoes, Mat')  -- three values in one column
```

**2NF — partial dependency (on composite key):**
```sql
order_items (order_id, product_id, product_name, quantity)
-- PK: (order_id, product_id)
-- product_name depends only on product_id — partial dependency
```

**3NF — transitive dependency:**
```sql
customers (customer_id, zip_code, city, state)
-- state is determined by city, not directly by customer_id
-- customer_id → zip_code → city → state (transitive chain)
```

---

**Q22 — Why 3NF for OLTP, not analytics**

**3NF for OLTP:** Write-heavy workloads need data integrity. 3NF ensures a product name change updates one row in `products`, not thousands of `order_items` rows. Insertion, update, and deletion anomalies are prevented. The join cost is acceptable because OLTP queries are narrow (fetch one or a few rows by PK).

**Analytics deliberately violates 3NF:** Analytical queries aggregate millions of rows. Each extra JOIN adds latency. A star schema dimension like `dim_product` stores `category` on every product row (2NF violation) to eliminate the join to `dim_category` on every query. At 500M fact rows, that join difference is measured in minutes vs. seconds.

**The specific cost:** A fully normalised 3NF source with 8 tables requires 6+ joins to answer "revenue by product category and customer region." The same query on a star schema requires 2 joins. At analytical scale, this matters enormously.

---

**Q23 — Identify violations and correct schema**

**Table:** `(order_id, product_id, product_name, category, quantity, store_id, store_city)` — PK: `(order_id, product_id)`

**2NF violations (depend on only part of the composite key):**
- `product_name` and `category` depend on `product_id` alone
- `store_id` depends on `order_id` alone (one store per order)
- `store_city` depends on `order_id` alone (transitively through `store_id`)

**3NF violations (transitive dependencies):**
- `store_id → store_city`: `store_city` is determined by `store_id`, not directly by `order_id`

**Corrected 3NF schema:**
```sql
products    (product_id PK, product_name, category)
stores      (store_id PK, store_city)
orders      (order_id PK, store_id FK → stores)
order_items (order_id FK, product_id FK, quantity)  -- PK: (order_id, product_id)
```

---

**Q24 — Transitive dependency**

A **transitive dependency** exists when non-key column A determines non-key column B, and B determines non-key column C — so C is transitively dependent on the PK via B (not directly).

**Example:**
```
customers (customer_id PK, zip_code, city, state)
Dependency chain: customer_id → zip_code → city → state
```
`state` is transitively dependent on `customer_id` through `zip_code` and `city`. This violates **3NF**.

**Why it matters:** If a zip code's city changes, every customer row with that zip must be updated. In 3NF (`zip_codes(zip_code, city, state)`), one row is updated and all customers immediately reflect the change.

---

**Q25 — Is `dim_product` in 3NF?**

```
dim_product (product_id, product_name, category, subcategory, brand, brand_country)
```

**Violation:** `brand → brand_country` — `brand_country` is determined by `brand`, not directly by `product_id`. This is a transitive dependency → **not in 3NF**.

**Would you fix it in a dimensional model? No.** Splitting into `dim_product` + `dim_brand(brand, brand_country)` adds a join on every analytics query for trivial storage savings. In a columnar store (Parquet), `brand_country` repeats per product row but compresses to near-zero via dictionary encoding.

**The answer an interviewer wants:** "I recognise the 3NF violation — `brand_country` is transitively dependent on `product_id` via `brand`. In an OLTP system I would normalise it. In a dimensional model, I leave it denormalised to eliminate a join. I document it so future engineers don't 'fix' it back."

---

**Q26 — 3NF in Silver vs. star schema everywhere**

**Closer to correct: 3NF in Silver with denormalisation in Gold.**

Silver (cleaned, conformed source data) serves multiple consumers: ML engineers who need feature tables, ad-hoc analysts, and BI pipelines. Keeping Silver close to 3NF preserves flexibility — each consumer can build the structure they need.

Gold (marts) should be deliberately denormalised — star schemas or flat tables optimised for specific query patterns. 3NF in Gold forces BI tools to write multi-join SQL that analysts cannot author correctly and tools generate poorly.

**When "star schema everywhere" is fine:** If Silver feeds only one BI tool and no ML consumers, skip the intermediate 3NF layer and go straight to a star schema. The separation of concerns only pays off when multiple teams consume Silver differently.

---

**Q27 — Update, insertion, deletion anomalies**

**Scenario:** Denormalised `orders` table with embedded customer data:
```sql
orders (order_id, customer_id, customer_name, customer_city, product_id, quantity)
```

**Update anomaly:** Customer C001 moves to Melbourne. All 5,000 order rows for C001 must be updated. A partial update (pipeline fails halfway) leaves some rows showing Sydney and some Melbourne — inconsistent data with no way to tell which is correct.

**Insertion anomaly:** A new customer cannot be added to the system until they place their first order — there is no separate customer record. Customer existence is coupled to order existence.

**Deletion anomaly:** Deleting the last order for a customer permanently destroys all information about that customer. There is no separate customer record to preserve.

**Fix for all three:** Extract a `customers` table (3NF). Each anomaly disappears because customer data exists independently of order data.

---

**Q28 — `city → state` in `dim_customer` — fix or leave?**

```
dim_customer (customer_id, city, state, country)
-- city → state: transitive dependency → 3NF violation
```

**Is it a 3NF violation? Yes.** `state` is determined by `city`, not directly by `customer_id`.

**Should you fix it in a dimensional model? No — leave it.** Splitting into `dim_customer` + `dim_city_lookup(city, state, country)` adds a join on every query that filters by state or country, for trivial storage savings. In a columnar format, the repeated `state` values compress to near-zero.

**The one exception:** If `city → state` mapping changes (e.g., a city is redistricted to a new state) and you need to update thousands of customer rows, the denormalised design becomes an update anomaly. At that point, materialising a lookup and MERGE-updating `dim_customer` is the right operational answer — but the dimension structure itself stays flat.

---

**Q29 — Parquet dictionary encoding reduces denormalisation storage cost**

In a **row-oriented database (PostgreSQL):** Every row stores the full string `'Australia'` or `'SoundMax'` as bytes on disk and in memory. 1M rows with `brand_country = 'Australia'` store 1M copies of the 9-byte string.

In a **columnar format (Parquet):**
- The `brand_country` column is stored as a contiguous sequence of identical values
- Parquet applies **dictionary encoding** automatically: the string `'Australia'` is stored once in a per-column dictionary; each row stores a 1-byte integer index into that dictionary
- 1M rows of `'Australia'` compress to: 9 bytes (the string) + 1M × 1 byte (index) ≈ 1 MB vs. ~9 MB in a row store
- With further run-length encoding (RLE), consecutive identical index values compress further — 1M identical values → a single (value=0, count=1M) record

The storage penalty of repeating dimension attributes in a denormalised Parquet table is 5–20× smaller than in a row store. This is why OBT and wide dimension tables are practical in modern lakehouses but impractical in traditional RDBMS.

---

**Q30 — Add `region` to 500M-row fact without reprocessing**

Add `region` to `dim_customer` via a one-time lookup table update:

```sql
-- Step 1: add region column to dim_customer
ALTER TABLE dim_customer ADD COLUMN region TEXT;

-- Step 2: populate via lookup (one-time backfill of the dimension only)
UPDATE dim_customer c
SET region = lr.region
FROM city_region_lookup lr
WHERE c.city = lr.city;
```

All existing `fact_sales` rows join to `dim_customer` at query time. The `region` column is immediately available:

```sql
SELECT c.region, SUM(f.quantity * f.unit_price) AS revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.region;
-- Works immediately; 500M fact rows are never touched
```

**Why the fact table doesn't change:** The star schema's FK-based join model means adding a new dimension attribute only requires updating the dimension. The fact table already has the FK (`customer_key`) that resolves to the dimension at query time — the new `region` column is automatically accessible through that existing join.

This is the core value proposition of the star schema: dimension attribute additions require O(dimension rows) updates, never O(fact rows) updates.
