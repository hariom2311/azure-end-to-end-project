# Day 3 — Interview Solutions: Data Modelling

> Complete answers for all 40 questions in `interview-questions.md`.

---

## Concept 1: Dimensional Modelling & Star Schema

**Q1 — Fact table vs. dimension table**

A **fact table** stores measurable, numeric business events — one row per event. Example: `fact_sales` with `order_id`, `quantity`, `unit_price`, `discount_pct`. The numbers are what you aggregate.

A **dimension table** stores the descriptive context of those events — who, what, where, when. Example: `dim_customer` with `customer_name`, `city`, `country`, `segment`. Dimensions are used for filtering and grouping, never for `SUM` or `AVG`.

**Rule:** If a column answers "how much?" or "how many?" → fact. If it answers "which one?" or "what kind?" → dimension.

---

**Q2 — The grain**

The **grain** is a precise one-sentence declaration of what one row in the fact table represents. It must be defined before any column is chosen.

**Why first:** Every column either measures that event (belongs in fact) or describes who/what/where (belongs in a dimension). An ambiguous grain causes columns to be mis-assigned, leading to double-counted aggregates.

**Example:** "One row represents one product line item within one customer order" is a valid grain. A different grain — "one order" — cannot have per-product columns without violating the design.

---

**Q3 — Star Schema vs. Snowflake Schema**

**Star Schema:** Dimension tables are fully denormalised. `dim_product` contains `product_name`, `category`, `subcategory`, `brand` all on one row. One join per dimension.

**Snowflake Schema:** Dimension tables are normalised — `dim_product` has a FK to `dim_category`, which has a FK to `dim_department`. Two or more joins per dimension.

**Recommendation: Star Schema for analytics.** Storage savings from Snowflake Schema are negligible at columnar storage costs. Extra joins add complexity, confuse BI tools that auto-generate SQL, and slow ad-hoc work. Use Snowflake Schema only when a shared sub-dimension is very large and shared by many schemas — rare in practice.

---

**Q4 — Wrong grain scenario**

If `fact_sales` has one row per order (header grain), there is no `product_id` column — or it stores just one product per order. Either way, "revenue per product category" requires line-item level data that does not exist at the order grain.

**Fix:** Change the grain to **order line item** — one row per product within each order:
```sql
fact_sales: order_id, line_item_id, product_key, customer_key, date_key, quantity, unit_price
```

Now `GROUP BY product_category` is correct because each row has exactly one product.

---

**Q5 — Three fact table types (logistics examples)**

**Transaction fact:** One row per atomic event; never updated.
- `fact_shipment_scans` — one row every time a package barcode is scanned at a depot.

**Periodic snapshot:** One row per entity per time period; inserted on a schedule.
- `fact_warehouse_inventory_daily` — one row per SKU per day recording the closing stock count. Even days with no movement get a row.

**Accumulating snapshot:** One row per lifecycle instance; the same row is updated as milestones occur.
- `fact_order_lifecycle` — one row per order with columns `order_placed_date`, `warehouse_picked_date`, `shipped_date`, `delivered_date`. Each is NULL until the milestone is reached.

---

**Q6 — Surrogate keys vs. business keys**

A **surrogate key** is a system-generated integer with no business meaning, used as the PK of a dimension and FK in the fact table.

**Two scenarios where business keys fail:**

1. **SCD Type 2:** Customer C001 must have 3 rows in `dim_customer` (one per city change). The business key `C001` repeats — it cannot be the PK. The surrogate key (`1001`, `1045`, `1089`) uniquely identifies each version.

2. **Source system migration:** A company migrates from numeric customer IDs to UUIDs. The surrogate key in the warehouse never changes — only `dim_customer` gets the new ID. All 500M fact rows continue pointing to the same surrogate keys with no reprocessing.

---

**Q7 — Conformed dimension**

A **conformed dimension** is shared identically across multiple star schemas in the same warehouse.

**Example:** `dim_date` — the same calendar dimension used by `fact_sales`, `fact_marketing_spend`, and `fact_logistics`. Every schema joins to the same table.

**Why it matters:** Without conformed dimensions, "sales in Q1" and "marketing spend in Q1" might use different date tables with different fiscal vs. calendar definitions, making cross-domain analysis impossible. With conformed dimensions, you can `JOIN fact_sales AND fact_marketing ON dim_date.date_key` and get correct cross-subject results.

---

**Q8 — Ride-sharing fact table design**

**Grain:** One completed trip.

```sql
fact_trips:
  trip_key          INT PK (surrogate)
  trip_id           INT     -- degenerate dimension
  driver_key        INT FK → dim_driver
  rider_key         INT FK → dim_rider
  pickup_loc_key    INT FK → dim_location  -- role 1: pickup
  dropoff_loc_key   INT FK → dim_location  -- role 2: dropoff (same dim, different FK)
  start_date_key    INT FK → dim_date
  start_time_key    INT FK → dim_time
  fare              NUMERIC
  surge_multiplier  NUMERIC
  trip_duration_min INT
  distance_km       NUMERIC
  rider_rating      SMALLINT   -- NULL if not rated
  driver_rating     SMALLINT   -- NULL if not rated
```

**Role-playing dimensions:** `dim_location` is used twice — once for pickup, once for dropoff. Same physical table, two logical roles, two FK column names.

---

## Concept 2: Slowly Changing Dimensions (SCD)

**Q9 — SCD Type 2 vs. Type 1**

**SCD Type 1 (overwrite):** Old value is permanently replaced. All historical fact rows now show the new attribute when joined.

**SCD Type 2 (new row):** A new dimension row is inserted for each change. Old rows are closed (end-dated). Historical fact rows still point to the old surrogate key — they correctly show the old attribute value.

**The problem Type 1 cannot solve:** Historical accuracy. If a customer moves and you overwrite, all their Sydney purchases now appear as Melbourne purchases. Regional reports and trend analysis become permanently wrong.

---

**Q10 — SCD Type 2 mechanics**

**Columns to add:**
```sql
ALTER TABLE dim_customer ADD COLUMN valid_from DATE NOT NULL DEFAULT '2000-01-01';
ALTER TABLE dim_customer ADD COLUMN valid_to   DATE NOT NULL DEFAULT '9999-12-31';
ALTER TABLE dim_customer ADD COLUMN is_current BOOLEAN NOT NULL DEFAULT TRUE;
```

**Update process when attribute changes:**
```sql
-- 1. Close current row
UPDATE dim_customer
SET valid_to = :change_date - INTERVAL '1 day', is_current = FALSE
WHERE customer_id = :cid AND is_current = TRUE;

-- 2. Insert new version
INSERT INTO dim_customer (customer_id, customer_name, city, valid_from, valid_to, is_current)
VALUES (:cid, :name, :new_city, :change_date, '9999-12-31', TRUE);
```

**Fact table join:** The fact table stores the surrogate `customer_key` that was active at the time of the sale. Joining on surrogate key automatically selects the correct historical version — no date range logic needed in the fact query.

---

**Q11 — SCD Type 1 misattribution**

**SCD type used:** Type 1 (overwrite). Alice's `city` column was updated directly when she moved.

**Consequence:** The single `dim_customer` row for Alice now shows `city = Melbourne`. All historical fact rows for Alice point to this same row. The Q1 report — covering sales made while Alice was in Sydney — now attributes those sales to Melbourne.

**Correct type:** SCD Type 2. A new dimension row (`city = Melbourne`) would have been inserted with `valid_from = April move date`. Q1 fact rows still point to the old surrogate key (Sydney row), so the Q1 report always returns Sydney regardless of when it is run.

---

**Q12 — SCD Type 2 and historical category queries**

After SCD Type 2 is applied:
- Pre-March 15 fact rows have `product_key` → old dim row where `category = 'Electronics'`
- Post-March 15 fact rows have `product_key` → new dim row where `category = 'Computers'`

Query `WHERE product_category = 'Electronics'` returns only pre-March revenue. Query `WHERE product_category = 'Computers'` returns only post-March revenue. This is correct — historical attribution is preserved.

**Fact rows touched:** Zero. The 500M fact rows are never modified. Only `dim_product` receives one closed row and one new row.

---

**Q13 — Three email changes**

After three changes, `dim_customer` has **4 rows** for this customer:

```
customer_key | customer_id | email             | valid_from | valid_to   | is_current
1001         | C001        | alice@old.com     | 2023-01-01 | 2023-05-14 | FALSE
1045         | C001        | alice@gmail.com   | 2023-05-15 | 2023-09-29 | FALSE
1089         | C001        | alice@company.com | 2023-09-30 | 2023-12-19 | FALSE
1132         | C001        | alice@new.com     | 2023-12-20 | 9999-12-31 | TRUE
```

The fact table stores the surrogate key that was current at order time. Joining on `customer_key` (surrogate) automatically returns the email address associated with each order period — no date range logic required in the query.

---

**Q14 — Risk of `valid_to = '9999-12-31'` sentinel**

**Risk:** Developers writing `WHERE valid_to < '2025-01-01'` accidentally exclude all current rows (sentinel date is beyond 2025). This produces "missing data" bugs that are hard to trace.

**Alternative: `is_current` boolean only:**
- Pro: Fast, simple current-row filter: `WHERE is_current = TRUE`
- Con: Cannot use `BETWEEN` for historical joins — need a self-join or window function

**Best practice:** Use both. `is_current` for current-value lookups; `valid_from`/`valid_to` for historical range joins. The redundancy is intentional.

---

**Q15 — Daily snapshot → SCD Type 2 pipeline**

**Logic (pseudocode):**

```
Input: stg_customers_today (all current customers from source)
Target: dim_customer (SCD Type 2 with valid_from, valid_to, is_current)

Step 1 — NEW records (in source, not in dim at all):
  SELECT s.* FROM stg_customers_today s
  LEFT JOIN dim_customer d ON s.customer_id = d.customer_id
  WHERE d.customer_id IS NULL
  → INSERT with valid_from = today, valid_to = '9999-12-31', is_current = TRUE

Step 2 — CHANGED records (in dim as current but attributes differ):
  SELECT s.* FROM stg_customers_today s
  JOIN dim_customer d ON s.customer_id = d.customer_id AND d.is_current = TRUE
  WHERE MD5(s.city || s.segment || s.email) != MD5(d.city || d.segment || d.email)
  → UPDATE old row: is_current = FALSE, valid_to = today - 1
  → INSERT new row: is_current = TRUE, valid_from = today

Step 3 — UNCHANGED records:
  WHERE hash matches → skip (no new surrogate key generated)
```

**Key detail:** Use `MD5` (hash_diff) of all tracked attributes to detect changes in one comparison rather than column-by-column. Unchanged rows produce no new surrogate key — important for preventing surrogate key explosion.

---

## Concept 3: Normalisation, Normal Forms & Denormalisation

**Q16 — 1NF, 2NF, 3NF violations**

**1NF violation — non-atomic value:**
```sql
orders (order_id, products)
-- ('1001', 'Headphones, Shoes, Mat')  -- multiple values in one column
```

**2NF violation — partial dependency (composite key):**
```sql
order_items (order_id, product_id, product_name, quantity)
-- PK: (order_id, product_id)
-- product_name depends only on product_id, not on the full composite key
```

**3NF violation — transitive dependency:**
```sql
customers (customer_id, zip_code, city, state)
-- city → state: state is determined by city, not directly by customer_id
```

---

**Q17 — Why 3NF for OLTP, not for analytics**

**3NF for OLTP:** Write-heavy workloads need data integrity. 3NF ensures a product name change updates one row, not thousands. Insertion, update, and deletion anomalies are prevented. The cost — extra joins — is acceptable because OLTP queries are narrow (fetch one row by PK).

**Analytics deliberately violates 3NF:** Analytical queries aggregate millions of rows across many columns. Each extra JOIN adds latency. A star schema dimension like `dim_product` stores `category` directly on each product row (2NF violation) to eliminate the `JOIN dim_category` on every analytics query. The storage cost is trivial in columnar formats with dictionary encoding.

**The cost of 3NF for analytics:** A fully normalised 3NF source with 8 tables requires 6+ joins to answer "revenue by category." The same query on a star schema requires one join. At 500M fact rows, that join difference is measured in minutes vs. seconds.

---

**Q18 — Identify 2NF and 3NF violations**

**Table:** `(order_id, product_id, product_name, category, quantity, store_id, store_city)` — PK: `(order_id, product_id)`

**2NF violations (depend on only part of the composite key):**
- `product_name` depends on `product_id` alone
- `category` depends on `product_id` alone
- `store_id` depends on `order_id` alone (one store per order)
- `store_city` depends on `order_id` alone (transitively via `store_id`)

**3NF violations (transitive dependencies):**
- `store_id → store_city`: `store_city` is determined by `store_id`, not by `order_id` directly

**Corrected schema:**
```sql
products    (product_id PK, product_name, category)
stores      (store_id PK, store_city)
orders      (order_id PK, store_id FK)
order_items (order_id FK, product_id FK, quantity)  -- PK: (order_id, product_id)
```

---

**Q19 — Transitive dependency**

A **transitive dependency** exists when non-key column A determines non-key column B, which determines non-key column C. Column C is transitively dependent on the PK via B.

**Example:**
```
customers (customer_id PK, zip_code, city, state)
customer_id → zip_code → city → state
```

`state` is transitively dependent on `customer_id` through `zip_code` and `city`. This violates **3NF**.

**Why it's a problem:** If a zip code changes its associated city (rare but possible), every customer row with that zip must be updated individually. In 3NF, you update one row in a `zip_codes` table.

---

**Q20 — Is `dim_product` in 3NF?**

```
dim_product (product_id, product_name, category, subcategory, brand, brand_country)
```

**Violation:** `brand → brand_country` — `brand_country` is determined by `brand`, not by `product_id`. This is a transitive dependency → **not in 3NF**.

**Would you fix it?** In a dimensional model: **No, leave it.** Splitting `dim_product` into `dim_product` + `dim_brand` adds a join on every analytics query for a trivial storage saving. The purpose of a dimension table is to be a fast lookup — denormalisation here is intentional.

**The answer an interviewer wants:** "I recognise the 3NF violation — `brand_country` is transitively dependent on `product_id` via `brand`. In an OLTP system I would normalise it. In a dimensional model, I would leave it denormalised to avoid extra joins at query time, and document it in the schema."

---

**Q21 — 3NF in Silver vs. Star Schema everywhere**

**Closer to correct: 3NF in Silver is reasonable, with caveats.**

The Silver layer (cleaned, joined source data) is often kept close to source structure — which is typically 3NF — to preserve the original data relationships and enable diverse downstream uses. Analysts, ML engineers, and BI teams may each need different views of the data.

The Gold layer (marts) should be deliberately denormalised — star schemas or flat OBT tables optimised for specific query patterns. Trying to use 3NF in Gold forces BI tools to write multi-join SQL that most analysts cannot author correctly.

**The answer is context-dependent:** If Silver feeds only one BI tool, star schema throughout is fine. If Silver feeds ML pipelines, ad-hoc analytics, and BI, preserving 3NF-ish structure in Silver while denormalising in Gold is the better separation of concerns.

---

**Q22 — Update, insertion, deletion anomalies**

**Scenario:** A poorly normalised `orders` table with customer data embedded:
```sql
orders (order_id, customer_id, customer_name, customer_city, product_id, quantity)
```

**Update anomaly:** Customer C001 changes their city. If C001 has 5,000 orders, all 5,000 rows must be updated. If the UPDATE runs but fails halfway, some rows show Sydney and some show Melbourne — inconsistent data.

**Insertion anomaly:** You cannot add a new customer to the database until they place their first order — there is no separate customer table. Customer existence is coupled to order existence.

**Deletion anomaly:** If you delete the last order for a customer, you permanently lose all information about that customer (name, city, segment). There is no separate customer record.

**Fix for all three:** Extract a separate `customers` table (3NF). Each anomaly disappears because customer data exists independently of order data.

---

## Concept 4: Advanced Fact Table Patterns

**Q23 — Factless fact table: two scenarios**

**Scenario 1 — Event occurrence (no numeric measure):**
`fact_student_attendance (date_key, student_key, course_key)` — the existence of the row means attendance occurred. Used to count attendance rates, identify absent students, etc.

**Scenario 2 — Coverage/eligibility:**
`fact_product_store_eligibility (date_key, product_key, store_key)` — records which products were stocked in which stores on which dates. A LEFT JOIN to `fact_sales` reveals product-store-date combinations with zero sales — impossible to derive from sales data alone.

---

**Q24 — Computing zero-sales days**

```sql
-- Without eligibility table: impossible
-- GROUP BY date in fact_sales gives days WITH sales only; missing dates are absent

-- With factless eligibility table:
SELECT e.product_key, e.date_key, e.store_key
FROM fact_product_store_eligibility e
LEFT JOIN fact_sales s
  ON e.product_key = s.product_key
  AND e.store_key  = s.store_key
  AND e.date_key   = s.date_key
WHERE s.order_id IS NULL;
-- Rows where IS NULL = eligible but no sale = zero-sale day
```

**Why `fact_sales` alone is insufficient:** Absence of evidence is not evidence of absence. A missing `(product, store, date)` combination in `fact_sales` could mean zero sales or it could mean the product was not stocked there that day. The eligibility table provides the denominator.

---

**Q25 — Promotion FK directly on `fact_orders` causes fan-out**

If `fact_orders` has a single `promotion_id` FK and an order has 3 promotions, you must store the order 3 times (one row per promotion). `SUM(revenue)` over the table now triples the revenue for multi-promotion orders.

**Correct pattern — bridge table:**
```sql
-- fact_orders: no promotion FK
fact_orders (order_id, customer_key, date_key, revenue)

-- Bridge: one row per order-promotion combination
bridge_order_promotions (order_id, promotion_key, weighting_factor)

-- Query: attributed revenue per promotion
SELECT p.promotion_name,
       SUM(f.revenue * b.weighting_factor) AS attributed_revenue
FROM fact_orders f
JOIN bridge_order_promotions b ON f.order_id = b.order_id
JOIN dim_promotion p           ON b.promotion_key = p.promotion_key
GROUP BY p.promotion_name;
-- Revenue is split by weighting_factor — total = original revenue, no double-counting
```

---

**Q26 — Role-playing dimension**

A **role-playing dimension** is a single physical dimension table referenced multiple times in the same fact table under different FK names, each representing a different semantic role.

**`dim_date` with three roles:**
```sql
CREATE TABLE fact_orders (
    order_key          INT PRIMARY KEY,
    order_date_key     INT REFERENCES dim_date(date_key),   -- role: when order was placed
    ship_date_key      INT REFERENCES dim_date(date_key),   -- role: when order shipped
    delivery_date_key  INT REFERENCES dim_date(date_key),   -- role: when order delivered
    revenue            NUMERIC
);

-- Query: average days from order to delivery, by order month
SELECT od.month AS order_month,
       AVG(dd.date - od.date) AS avg_delivery_days
FROM fact_orders f
JOIN dim_date od ON f.order_date_key    = od.date_key  -- "order date" alias
JOIN dim_date dd ON f.delivery_date_key = dd.date_key  -- "delivery date" alias
GROUP BY od.month;
```

---

**Q27 — Junk dimension**

A **junk dimension** consolidates low-cardinality flag and indicator columns from the fact table into a single small dimension table, keeping the fact table narrow.

**Why preferable to inline flags:**
- Fact table width is reduced (one FK `flag_key` instead of 5–10 flag columns)
- Cardinality is managed: 5 binary flags = 32 possible combinations → 32-row dimension
- BI tools display the flags as a named dimension, not as scattered fact columns
- Adding a new flag value requires only inserting one row in `dim_flags`, not altering the fact table

```sql
dim_order_flags (flag_key, is_gift, is_online, is_first_order, payment_method, shipping_priority)
-- 32 rows (all combinations of 5 boolean flags + categorical values)

fact_orders (..., flag_key INT FK → dim_order_flags)  -- one column replaces five
```

---

**Q28 — Role-playing dimension query**

```sql
-- Average delivery time by month of order, joining dim_date in three roles
SELECT
    od.month       AS order_month,
    od.year        AS order_year,
    AVG(dd.date - od.date)::INT AS avg_days_to_deliver
FROM fact_orders f
JOIN dim_date od ON f.order_date_key    = od.date_key  -- order placed date
JOIN dim_date sd ON f.ship_date_key     = sd.date_key  -- shipped date (alias: sd)
JOIN dim_date dd ON f.delivery_date_key = dd.date_key  -- delivered date (alias: dd)
WHERE od.year = 2024
GROUP BY od.month, od.year
ORDER BY od.year, od.month;
```

Each alias (`od`, `sd`, `dd`) independently joins to the same `dim_date` table for a different FK column.

---

**Q29 — Accumulating snapshot vs. transaction fact**

**Transaction fact:** Immutable append-only. One row per discrete event. Can have trillions of rows. Each event is independent — you never update old rows.

**Accumulating snapshot:** One row per business process instance (e.g., one order). The row is updated each time a milestone is reached. Best for tracking progress through a pipeline with a known sequence of stages.

**When accumulating snapshot becomes impractical:**
- **Volume:** If there are billions of active orders simultaneously, the UPDATE overhead (updating the `shipped_date` column when a shipment occurs requires touching and rewriting Parquet files in a lake) becomes prohibitive
- **Velocity:** If milestones occur millions of times per second, the UPDATE rate overwhelms the storage layer
- **Long-lived processes:** If an order might take months (large B2B contracts), the accumulating snapshot row sits with many NULL milestone columns for a long time

**Threshold rule of thumb:** Accumulating snapshots work well for processes with < 10M active instances at a time and O(seconds-to-days) total lifecycle. For high-volume, high-velocity, or long-lived processes, prefer a transaction fact + a separate status dimension.

---

## Concept 5: Data Modelling Anti-patterns

**Q30 — Wrong grain anti-pattern**

The wrong grain occurs when rows in a fact table represent different levels of aggregation — some rows are events, others are summaries.

**Example:**
```sql
fact_sales (order_id, product_id, line_revenue, total_order_revenue)
-- (1001, P101, 120.00, 350.00)
-- (1001, P102, 130.00, 350.00)  ← total_order_revenue repeats
-- (1001, P104, 100.00, 350.00)

SELECT SUM(total_order_revenue) FROM fact_sales WHERE order_id = 1001;
-- Returns 1050 (350 × 3) instead of 350 — triple-counted
```

**Symptom:** `SUM` returns values that are multiples of the correct answer (2×, 3×, N× depending on how many line items per order).

**Fix:** One grain per fact table. `total_order_revenue` belongs in a separate `fact_order_header` table at the order grain, not mixed with line-item rows.

---

**Q31 — Derived columns in fact table**

If `revenue = quantity * unit_price` and `net_revenue = revenue * (1 - discount_pct/100)` are stored as columns, changing the `revenue` formula requires:
1. Updating `revenue` formula in the pipeline
2. Updating `net_revenue` formula (which depends on `revenue`) in the pipeline
3. Reprocessing all historical rows for both columns
4. Ensuring both stay in sync going forward — any pipeline bug breaks one but not the other

**Better design:** Store only atomic facts (`quantity`, `unit_price`, `discount_pct`). Derive everything in a view or Gold model:
```sql
CREATE VIEW v_sales AS
SELECT *,
    quantity * unit_price AS revenue,
    quantity * unit_price * (1 - discount_pct / 100.0) AS net_revenue
FROM fact_sales;
```

Now changing the formula updates one view definition — no historical reprocessing needed.

---

**Q32 — Revenue 40% higher than expected — fan-out diagnosis**

**Most likely cause:** `dim_customer` has duplicate rows for some customers (data quality issue or bad SCD Type 2 implementation). The JOIN multiplies fact rows.

**Diagnosis:**
```sql
-- Step 1: check for duplicate customer_ids in dim_customer
SELECT customer_id, COUNT(*) FROM dim_customer GROUP BY customer_id HAVING COUNT(*) > 1;

-- Step 2: check join row count vs expected
SELECT COUNT(*) FROM fact_sales;                              -- should be N
SELECT COUNT(*) FROM fact_sales JOIN dim_customer ON ...;    -- if > N, fan-out exists

-- Step 3: quantify
SELECT customer_id, COUNT(*) AS duplicates FROM dim_customer GROUP BY customer_id;
-- If avg duplicates = 1.4, that explains a ~40% inflation
```

**Fix:** Add a `WHERE is_current = TRUE` to the join (if SCD Type 2 is in use), or deduplicate the dimension before joining.

---

**Q33 — NULL overloading**

Using NULL for three different business meanings (`no discount`, `unknown`, `not applicable`) conflates genuinely different states into one representation. Downstream queries cannot distinguish them:

```sql
SELECT AVG(discount_pct) FROM fact_sales;
-- AVG ignores NULLs — treats "no discount" (0%), "unknown", and "N/A" identically
-- The average is computed only over rows with a numeric value
-- If 30% of rows are NULL-as-zero, the average is inflated (missing the zeros)
```

**Fix — explicit sentinel and status column:**
```sql
ALTER TABLE fact_sales
  ADD COLUMN discount_pct_status TEXT;
  -- 'applied'   → discount was applied, discount_pct has a value
  -- 'none'      → no discount, discount_pct = 0.00
  -- 'unknown'   → data not collected, discount_pct = NULL
  -- 'na'        → product type not eligible, discount_pct = NULL

-- Now queries can be precise:
SELECT AVG(discount_pct) FROM fact_sales WHERE discount_pct_status = 'applied';
SELECT COUNT(*) FROM fact_sales WHERE discount_pct_status = 'none';
```

---

**Q34 — String date produces wrong query results**

**Format `'DD/MM/YYYY'` vs. filter `> '2024-06-01'`:**

1. **Lexicographic comparison:** `'15/07/2024' > '2024-06-01'` evaluates as a string comparison. `'1'` (first char of `'15/07'`) vs `'2'` (first char of `'2024'`) → `'1' < '2'` → July 15 returns FALSE. Orders from July are excluded; some earlier dates with characters > '2' are incorrectly included.

2. **Mixed format chaos:** If some rows are `'15/01/2024'` and others are `'2024-01-15'`, the comparison is meaningless — neither greater-than nor equals works predictably.

**Fix in staging:**
```sql
SELECT TO_DATE(order_date, 'DD/MM/YYYY') AS order_date FROM raw.orders;
-- or:
SELECT CAST(order_date AS DATE) AS order_date FROM raw.orders;
-- after normalising the format upstream
```

**Why `date_key INT + dim_date` is even better:**
- Integer FK comparisons are faster than DATE comparisons at scale
- `dim_date` adds pre-computed attributes (`month`, `quarter`, `fiscal_year`, `is_holiday`) that are expensive to recompute on every query
- Partitioning by `date_key` (integer) is efficient; BI tools can filter by any date attribute without functions on the fact table

---

**Q35 — Fan-out problem with worked example**

**Setup:**
```
fact_orders grain: one row per order
dim_customer: accidentally loaded twice → 2 rows per customer (keys 1 and 5 both = C001)

SELECT SUM(revenue) FROM fact_orders f
JOIN dim_customer c ON f.customer_id = c.customer_id;
```

**What happens:** Every fact row for C001 now joins to 2 dimension rows → appears twice in the result set → `SUM(revenue)` for C001 is doubled. If this affects 40% of customers with duplicates, total revenue appears ~40% inflated.

**Detection before production:**
```sql
-- Check row count after join vs. fact table
SELECT COUNT(*) FROM fact_orders;                                    -- expected: N
SELECT COUNT(*) FROM fact_orders JOIN dim_customer ON ...;          -- if > N: fan-out

-- Check dimension cardinality
SELECT customer_id, COUNT(*) FROM dim_customer
GROUP BY customer_id HAVING COUNT(*) > 1;  -- should return 0 rows
```

**Prevention via design:**
- Enforce `UNIQUE (customer_id)` on non-SCD dimensions at the DB level
- For SCD Type 2 dimensions: `UNIQUE (customer_id, is_current)` where `is_current = TRUE` (partial unique index in PostgreSQL)
- dbt: add a `unique` test on `customer_id` for `dim_customer` — fails the build if duplicates are loaded

---

## Mixed / Senior-Level Questions

**Q36 — Architecture recommendation for 15-table startup**

**Recommendation: Option A — replicate to Snowflake + star schema with dbt.**

At 10 GB today and 10 TB in 3 years, the analytical layer needs to be separated from the OLTP database before analytical load causes production problems. Direct analytics on PostgreSQL (Option B) adds query load to the operational database — unacceptable at growth scale. A single OBT in PostgreSQL is a dead end: dimension changes require full rewrites, and there is no path to multiple analytical domains.

**Signals that change this:**
- No budget for Snowflake → use a PostgreSQL read replica with a star schema as a temporary measure
- Already on BigQuery/Databricks → use native tools instead of Snowflake
- < 5 tables, < 1 GB, single analyst → OBT in PostgreSQL is fine for 6–12 months

---

**Q37 — Degenerate dimension**

A **degenerate dimension** is a dimension key with operational meaning stored directly in the fact table, with no corresponding dimension table.

**Retail example:** `order_id` in `fact_order_items`. The grain is order line item, so `order_id` is a natural grouping key (you want `GROUP BY order_id` to see per-order totals). But `order_id` is just a transaction reference — it has no descriptive attributes worth a separate `dim_order` table.

**Why it stays in the fact:** It is used for grouping and filtering but has no attributes. Creating a `dim_order (order_id)` with a single column would be a meaningless one-column table that adds a join with no benefit.

Other examples: `invoice_number`, `ticket_id`, `session_id`, `transaction_reference`.

---

**Q38 — Canonical table identification**

**Investigation steps:**
1. `SELECT COUNT(*) FROM each_table` — largest is likely most complete
2. `SELECT MAX(updated_at) FROM each_table` — most recently updated is likely most current
3. Check `pg_stat_user_tables` for `n_live_tup` and last autovacuum/analyze
4. Check which table the BI tool or dbt models reference — that is the operational canonical
5. `git log` or dbt lineage to find which table appears in `{{ ref() }}` calls

**Governance to prevent this:**
- Naming conventions enforced by schema: only the DE pipeline can create tables in `analytics.*`; analysts write to `analyst_sandbox.*`
- dbt: all production tables are defined in code with `{{ ref() }}` — no ad-hoc `CREATE TABLE orders_final` can reach production
- Data catalog: tables require owner, purpose, and deprecation date before creation; old tables are moved to `archive.*`, not left alongside replacements

---

**Q39 — Adding `region` to 500M-row fact without reprocessing**

Add `region` to `dim_customer` via a lookup table update:
```sql
-- Update dim_customer: one-time backfill
UPDATE dim_customer c
SET region = lr.region
FROM city_region_lookup lr
WHERE c.city = lr.city;
```

All existing `fact_orders` rows join to `dim_customer` at query time — they immediately gain access to `region` with zero changes to the fact table:
```sql
SELECT c.region, SUM(f.quantity * f.unit_price) AS revenue
FROM fact_orders f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.region;
-- Works immediately; 500M fact rows untouched
```

**This is the core value of the star schema:** new dimension attributes require only a dimension update, never a fact table rewrite.

---

**Q40 — Missing test category: anomaly/volume detection**

`not_null` and `unique` tests verify structural integrity — individual columns are populated and distinct. They cannot detect **business-level volume anomalies**: revenue dropped 15%, row count halved, a product category disappeared.

**What's missing:** Freshness tests and distribution/volume tests.

```sql
-- Custom singular test: flag if today's revenue is < 50% of the 7-day average
-- tests/assert_revenue_not_anomalous.sql
WITH daily AS (
    SELECT order_date, SUM(quantity * unit_price) AS daily_rev
    FROM {{ ref('fact_sales') }}
    WHERE order_date >= CURRENT_DATE - 14
    GROUP BY order_date
),
stats AS (
    SELECT AVG(daily_rev) AS avg_rev
    FROM daily
    WHERE order_date < CURRENT_DATE
)
SELECT order_date, daily_rev
FROM daily, stats
WHERE order_date = CURRENT_DATE
  AND daily_rev < avg_rev * 0.85  -- returns rows (fails) if revenue drops > 15%
```

**With packages:**
```yaml
# schema.yml — using dbt-expectations
- name: fct_sales
  tests:
    - dbt_utils.recency:
        datepart: day
        field: order_date
        interval: 1   # must have data from the last 1 day
    - dbt_expectations.expect_table_row_count_to_be_between:
        min_value: 400000   # alert if daily row count drops below expected
        max_value: 1000000
```
