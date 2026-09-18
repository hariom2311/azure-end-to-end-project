# Day 3 — Data Modelling

## Overview

Data modelling is the discipline of structuring data so it is easy to query, understand, and trust. A well-modelled dataset answers business questions in one or two joins; a poorly modelled one requires six joins, produces wrong aggregates, and is rebuilt every six months. Day 3 covers five core modelling concepts that appear in almost every data engineering interview — from dimensional modelling and SCD to normalisation, advanced fact table patterns, and the modelling mistakes that sink production pipelines.

**The 5 concepts:**
1. Dimensional Modelling & Star Schema
2. Slowly Changing Dimensions (SCD)
3. Normalisation, Normal Forms & Denormalisation Trade-offs
4. Advanced Fact Table Patterns — Factless Facts, Bridge Tables & Role-Playing Dimensions
5. Data Modelling Anti-patterns & How to Fix Them

---

## Concept 1: Dimensional Modelling & Star Schema

### What is dimensional modelling?

**Dimensional modelling** (Ralph Kimball, 1996) is a design technique for analytical databases that organises data into two types of tables:

- **Fact tables** — store measurable, numeric business events (sales, clicks, payments). Each row is one event.
- **Dimension tables** — store the context of those events (who, what, where, when). Each row describes an entity.

The goal is to make analytical queries simple, fast, and business-readable.

### Star Schema

In a **Star Schema**, one central fact table is surrounded by dimension tables — the shape resembles a star.

```
                  dim_customer
                  (customer_id PK,
                   customer_name,
                   country,
                   segment)
                        |
                        |
dim_product ————— fact_sales ————— dim_date
(product_id PK,  (order_id PK,    (date_id PK,
 product_name,    date_id FK,      date,
 category,        customer_id FK,  month,
 brand)           product_id FK,   quarter,
                  quantity,        year,
                  revenue,         day_of_week)
                  discount)
                        |
                        |
                  dim_store
                  (store_id PK,
                   store_name,
                   city,
                   region)
```

**Fact table characteristics:**
- Narrow primary key (surrogate or composite of FKs)
- Mostly numeric measures: `revenue`, `quantity`, `duration_seconds`
- High row count — billions of rows is normal
- Append-only (new events are inserted, not updated)

**Dimension table characteristics:**
- Wide — many descriptive text columns
- Low-to-medium row count (thousands to millions)
- Queried for filtering and grouping: `WHERE country = 'AU'`, `GROUP BY category`

### Why not just use the normalised OLTP schema?

An OLTP schema for orders might have 8 tables: `orders`, `order_items`, `customers`, `addresses`, `products`, `categories`, `promotions`, `stores`. A simple revenue-by-category query requires 6+ joins and is slow on 100M rows.

In a star schema, the same query is:
```sql
SELECT p.category, SUM(f.revenue)
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category;
```

One join. The denormalised dimension table already has `category` on every product row.

### Grain: the most important decision in fact table design

The **grain** defines what one row in the fact table represents. You must declare it before designing any columns.

| Grain | One row = | Revenue column means |
|---|---|---|
| Order line item | One product in one order | Revenue for that line |
| Daily sales summary | One product per store per day | Total daily revenue |
| Order header | One entire order | Total order value |

**Rule:** Choose the finest grain that business questions require. You can always aggregate up; you cannot disaggregate down.

### Snowflake Schema (normalised dimensions)

A **Snowflake Schema** normalises dimension tables further — `dim_product` has a FK to `dim_category`, which has a FK to `dim_department`. This saves storage but requires extra joins.

**Recommendation:** Use Star Schema for analytics. The storage savings of Snowflake Schema are irrelevant at modern column-store costs; the extra joins add query complexity with no benefit.

### Fact table types

| Type | Description | Example |
|---|---|---|
| Transaction fact | One row per atomic event | Each sale, each click, each login |
| Periodic snapshot | One row per entity per time period | Account balance every month-end |
| Accumulating snapshot | One row per lifecycle instance, updated as it progresses | Order row updated through placed → shipped → delivered |

---

## Concept 2: Slowly Changing Dimensions (SCD)

### The problem: dimensions change over time

A customer moves from Sydney to Melbourne. A product changes its category. A sales rep is reassigned to a new region. If you overwrite the old value in the dimension table, historical fact rows now point to wrong context — revenue that was sold in Sydney is now attributed to Melbourne.

**Slowly Changing Dimensions (SCD)** is a set of strategies for handling changes to dimension attributes while preserving correct historical reporting.

### SCD Type 1 — Overwrite (no history)

Just update the column. The old value is permanently lost.

```sql
UPDATE dim_customer
SET city = 'Melbourne'
WHERE customer_id = 'C001';
```

**Use when:** The old value was wrong (data fix), not changed. Never use when history matters.

**Side effect:** All historical fact rows for C001 now report Melbourne revenue — even sales made while the customer was in Sydney.

### SCD Type 2 — Add a new row (full history)

Insert a new dimension row for every change. The old row is closed by setting an end date and `is_current = FALSE`.

```sql
-- Current state of dim_customer for C001:
-- customer_key | customer_id | city    | valid_from | valid_to   | is_current
--     1001     |    C001     | Sydney  | 2023-01-01 | 2024-06-14 | FALSE
--     1045     |    C001     | Melbourne | 2024-06-15 | 9999-12-31 | TRUE

-- When C001 moves to Melbourne:
-- 1. Close the old row
UPDATE dim_customer
SET valid_to = '2024-06-14', is_current = FALSE
WHERE customer_id = 'C001' AND is_current = TRUE;

-- 2. Insert the new row
INSERT INTO dim_customer (customer_id, city, valid_from, valid_to, is_current)
VALUES ('C001', 'Melbourne', '2024-06-15', '9999-12-31', TRUE);
```

**Fact table join with SCD Type 2:**
```sql
-- Revenue correctly attributed to Sydney for historical sales
SELECT c.city, SUM(f.revenue)
FROM fact_sales f
JOIN dim_customer c
  ON f.customer_id = c.customer_id
  AND f.order_date BETWEEN c.valid_from AND c.valid_to
GROUP BY c.city;
```

**Characteristics:**
- Surrogate key (`customer_key`) is generated for each version — fact table stores the surrogate key of the version that was current at the time of the event
- `valid_from` / `valid_to` or `is_current` flag for identifying the active row
- Enables accurate "as-of" reporting

### SCD Type 3 — Add a column (limited history)

Add a new column for the previous value. Only one version of history is stored.

```sql
ALTER TABLE dim_customer ADD COLUMN previous_city TEXT;

UPDATE dim_customer
SET previous_city = city, city = 'Melbourne'
WHERE customer_id = 'C001';
```

**Limitation:** You can compare current vs. previous, but not current vs. 2 years ago. Rarely used in practice.

### SCD Type 4 — History table

Keep the main dimension with current values only. Maintain a separate history table for all changes.

```sql
-- dim_customer: current values only (fast lookups)
-- dim_customer_history: all historical versions with timestamps
INSERT INTO dim_customer_history
SELECT *, NOW() as changed_at FROM dim_customer WHERE customer_id = 'C001';

UPDATE dim_customer SET city = 'Melbourne' WHERE customer_id = 'C001';
```

**Use when:** Dimension is queried mostly for current values, but occasional history lookups are needed.

### Choosing SCD type

| Scenario | SCD type |
|---|---|
| Data correction (value was just wrong) | Type 1 |
| Slowly changing attribute, history required for reporting | Type 2 |
| Two-period comparison only (current vs previous) | Type 3 |
| Mostly current lookups, occasional history | Type 4 |
| High-frequency changes (price changes hourly) | Periodic snapshot fact instead of SCD |

---

## Concept 3: Normalisation, Normal Forms & Denormalisation Trade-offs

### What is normalisation?

**Normalisation** is the process of organising a relational database to reduce data redundancy and prevent update anomalies. It works by decomposing wide tables into smaller, focused tables connected by foreign keys. The rules that define how far to decompose are called **Normal Forms (NF)**.

Every data engineer must understand normalisation because it defines the structure of OLTP source systems you extract from, and explains *why* you denormalise when building analytics layers.

### The three most important Normal Forms

**First Normal Form (1NF) — No repeating groups, atomic values**

A table is in 1NF if:
- Every column holds a single atomic value (no arrays, comma-separated lists, or JSON blobs in a column)
- Every row is uniquely identifiable (has a primary key)
- No repeating column groups

```sql
-- VIOLATES 1NF: comma-separated list in one column
orders (order_id, customer_id, products)
-- ('1001', 'C001', 'Headphones,Shoes,Mat')

-- 1NF compliant: one row per product
order_items (order_id, line_item_id, product_id)
-- ('1001', 1, 'P101')
-- ('1001', 2, 'P102')
-- ('1001', 3, 'P104')
```

**Second Normal Form (2NF) — No partial dependencies**

A table is in 2NF if it is in 1NF and every non-key column depends on the **entire** primary key (not just part of it). Only relevant when the primary key is composite.

```sql
-- VIOLATES 2NF: product_name depends only on product_id, not on (order_id, product_id)
order_items (order_id, product_id, product_name, quantity)
-- product_name is a partial dependency on product_id alone

-- 2NF compliant: split into two tables
order_items (order_id, product_id, quantity)   -- PK: (order_id, product_id)
products    (product_id, product_name, ...)    -- PK: product_id
```

**Third Normal Form (3NF) — No transitive dependencies**

A table is in 3NF if it is in 2NF and no non-key column depends on another non-key column (i.e., no column is determined by a column that is not the primary key).

```sql
-- VIOLATES 3NF: zip_code → city → state (state depends on city, not on customer_id)
customers (customer_id, zip_code, city, state)
-- 'state' is transitively dependent on 'customer_id' via 'city'

-- 3NF compliant: extract the transitive dependency
customers    (customer_id, zip_code)
zip_codes    (zip_code, city, state)
```

### Why OLTP databases are normalised

| Problem | Cause | Normal form that prevents it |
|---|---|---|
| Insertion anomaly | Cannot insert an order without a product existing | 2NF (partial dependency removed) |
| Update anomaly | Change product name in 1 place, 10,000 order rows show wrong name | 2NF/3NF |
| Deletion anomaly | Deleting last order for a product loses product info | 2NF |

Normalised schemas protect data integrity for write-heavy OLTP workloads. The trade-off is query complexity: a normalised 3NF schema might require 6 joins to answer a business question.

### When and why to denormalise for analytics

The analytics layer deliberately violates 3NF to improve query performance. This is **intentional, controlled denormalisation** — not a mistake.

```
OLTP (3NF) → ETL/ELT → Dimensional model (partial denormalisation: dimensions are 2NF)
                       → OBT (full denormalisation: single flat table, 1NF)
```

| Layer | Normalisation level | Reason |
|---|---|---|
| OLTP source | 3NF | Write performance, data integrity |
| Data warehouse dimensions | ~2NF (denormalised) | Fewer joins, better BI query performance |
| Fact tables | 1NF + FKs | Append-only events; measures are atomic |
| One Big Table (Gold) | 1NF (fully flat) | Self-service BI, zero-join queries |

### Boyce-Codd Normal Form (BCNF) — quick reference

BCNF is a stricter form of 3NF: every determinant must be a candidate key. In practice, BCNF matters for source system design but is rarely enforced in data warehouses. If an interviewer asks about BCNF, acknowledge it exists and explain that analytical systems deliberately relax it for query performance.

### The normalisation vs. denormalisation decision in an interview

> "How normalised should a data warehouse be?"

**Answer framework:**
1. Source systems (OLTP) should be 3NF — protect write integrity
2. Staging / Bronze layer: preserve source structure (whatever NF it came in)
3. Silver / data warehouse dimensions: denormalise to ~2NF (flatten hierarchies like category → subcategory → product into `dim_product`)
4. Gold / marts: fully denormalise into fact + flat dimensions (star schema), or all the way into an OBT for specific use cases

The dimension in a star schema is denormalised: `dim_product` has `product_name`, `category`, `subcategory`, `brand` all on one row — no separate `dim_category` table. This is a deliberate 3NF violation for join reduction.

---

## Concept 4: Advanced Fact Table Patterns — Factless Facts, Bridge Tables & Role-Playing Dimensions

### Factless fact tables

A **factless fact table** records the occurrence of an event without any numeric measure. It captures the intersection of dimensions at a point in time.

**Use case 1 — Event occurrence:**
```sql
-- Did a student attend a class? No numeric measure — the row IS the fact.
CREATE TABLE fact_student_attendance (
    date_key       INT,
    student_key    INT,
    course_key     INT,
    instructor_key INT
    -- No measures — the existence of the row means attendance occurred
);

-- Query: attendance rate per course
SELECT c.course_name,
       COUNT(*) AS attended,
       COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY c.course_key) AS rate_pct
FROM fact_student_attendance f
JOIN dim_course c ON f.course_key = c.course_key
GROUP BY c.course_name;
```

**Use case 2 — Coverage / eligibility table:**
```sql
-- Which products COULD have been sold in each store on each date?
-- Used to identify zero-sale days (products eligible but with no transactions)
CREATE TABLE fact_product_store_eligibility (
    date_key     INT,
    product_key  INT,
    store_key    INT
);

-- Query: find products with zero sales on days they were eligible
SELECT e.product_key, e.date_key
FROM fact_product_store_eligibility e
LEFT JOIN fact_sales s
  ON e.product_key = s.product_key
  AND e.store_key  = s.store_key
  AND e.date_key   = s.date_key
WHERE s.order_id IS NULL;  -- eligible but no sales = zero-sale day
```

This is the standard pattern for **computing zero-sales** — without the eligibility table, a plain `GROUP BY date` on `fact_sales` cannot distinguish "no sales" from "store was closed."

### Bridge tables (many-to-many relationships)

A **bridge table** resolves a many-to-many relationship between a fact table and a dimension where one fact row corresponds to multiple dimension rows.

**Problem without a bridge:**
```
One order can have multiple promotions applied.
If you join fact_orders directly to dim_promotion, each order with 3 promotions
appears 3 times → revenue is triple-counted.
```

**Solution — bridge table:**
```sql
-- Bridge table: many-to-many between orders and promotions
CREATE TABLE bridge_order_promotions (
    order_id      INT,
    promotion_key INT,
    weighting_factor NUMERIC(4,3) DEFAULT 1.0
    -- optional: split revenue attribution across promotions
);

-- Fact table: no direct FK to promotion (avoids fan-out)
CREATE TABLE fact_orders (
    order_key    INT PRIMARY KEY,
    order_id     INT,
    customer_key INT,
    date_key     INT,
    revenue      NUMERIC
    -- promotion_key deliberately ABSENT
);

-- Query: revenue by promotion (no double-counting)
SELECT p.promotion_name,
       SUM(f.revenue * b.weighting_factor) AS attributed_revenue
FROM fact_orders f
JOIN bridge_order_promotions b ON f.order_id = b.order_id
JOIN dim_promotion p           ON b.promotion_key = p.promotion_key
GROUP BY p.promotion_name;
```

**The weighting factor** lets you split revenue attribution across multiple promotions — e.g., if two promotions applied to an order, each gets 0.5 weight.

### Role-playing dimensions

A **role-playing dimension** is a single dimension table used multiple times in the same fact table with different semantic meanings.

**Classic example — `dim_date` used 3 ways in an order fact:**
```sql
CREATE TABLE fact_orders (
    order_key        INT PRIMARY KEY,
    order_date_key   INT REFERENCES dim_date(date_key),  -- when order was placed
    ship_date_key    INT REFERENCES dim_date(date_key),  -- when order shipped
    delivery_date_key INT REFERENCES dim_date(date_key), -- when order delivered
    customer_key     INT,
    revenue          NUMERIC
);
```

`dim_date` plays three roles: order date, ship date, delivery date. One physical table, three logical dimensions.

**Querying a role-playing dimension requires aliasing:**
```sql
SELECT
    od.month        AS order_month,
    sd.month        AS ship_month,
    AVG(dd.date - od.date) AS avg_delivery_days
FROM fact_orders f
JOIN dim_date od ON f.order_date_key   = od.date_key   -- "order date" role
JOIN dim_date sd ON f.ship_date_key    = sd.date_key   -- "ship date" role
JOIN dim_date dd ON f.delivery_date_key = dd.date_key  -- "delivery date" role
WHERE od.year = 2024
GROUP BY od.month, sd.month;
```

**Other role-playing dimension examples:**
- `dim_location` in a ride-sharing fact: pickup location and dropoff location (same geography dimension, two FKs)
- `dim_employee` in an HR fact: employee, manager, recruiter (same people dimension, three FKs)
- `dim_account` in a banking transfer fact: source account and destination account

### Junk dimensions

A **junk dimension** groups miscellaneous low-cardinality flags and indicators from the fact table into a single small dimension table, keeping the fact table narrow.

```sql
-- Without junk dimension: many flag columns on the fact table
fact_orders (order_id, ..., is_gift, is_online, is_first_order, payment_method, shipping_priority)
-- 5 flag columns, each with 2–5 possible values — clutters the fact

-- With junk dimension: pre-compute all combinations
dim_order_flags (
    flag_key         INT PRIMARY KEY,   -- surrogate key
    is_gift          BOOLEAN,
    is_online        BOOLEAN,
    is_first_order   BOOLEAN,
    payment_method   TEXT,              -- cash/card/wallet
    shipping_priority TEXT              -- standard/express/overnight
);
-- fact_orders stores only: flag_key INT (1 FK instead of 5 columns)

-- Typical junk dimension has 2^n rows (all combinations of flags)
-- For 5 binary flags: 2^5 = 32 rows — tiny table
```

---

## Concept 5: Data Modelling Anti-patterns & How to Fix Them

### Anti-pattern 1 — Wrong grain (the most common interview trap)

**The mistake:** Mixing grain levels in one fact table — some rows represent order headers, some represent order line items.

```sql
-- BROKEN: grain is undefined
fact_sales (order_id, product_id, revenue, total_order_revenue)
-- For a single-item order: revenue = total_order_revenue (fine)
-- For a multi-item order: total_order_revenue repeats on every line item row
-- SUM(total_order_revenue) double/triple-counts order-level revenue
```

**The fix:** Define one grain per fact table. If you need both order-level and line-item-level metrics, build two fact tables:
```sql
fact_order_lines  (order_id, line_item_id, product_key, quantity, line_revenue)  -- grain: line item
fact_order_header (order_id, customer_key, date_key, total_revenue, discount)    -- grain: order
```

**Interview tell:** If an interviewer asks "why is SUM(revenue) wrong in this query?", the answer is almost always a grain violation or a fan-out from a bad join.

---

### Anti-pattern 2 — Storing calculated metrics as columns

**The mistake:** Adding pre-computed columns to a fact table that can be derived from existing columns.

```sql
-- BROKEN: revenue, discount_amount, net_revenue are all derivable
fact_sales (order_id, quantity, unit_price, discount_pct,
            revenue,          -- = quantity * unit_price
            discount_amount,  -- = revenue * discount_pct / 100
            net_revenue)      -- = revenue - discount_amount
```

**Why this is bad:**
- If the `revenue` formula changes (e.g., add tax), you must update both the formula column AND the net_revenue column AND reprocess all historical rows
- Derived columns can get out of sync with base columns (data quality issue)
- Business definition changes cause silent inconsistencies

**The fix:** Store atomic facts only; derive everything else in the query or in a view:
```sql
fact_sales (order_id, quantity, unit_price, discount_pct)  -- atomic only

-- View or Gold model derives the rest:
CREATE VIEW v_sales_metrics AS
SELECT *,
    quantity * unit_price AS revenue,
    quantity * unit_price * discount_pct / 100 AS discount_amount,
    quantity * unit_price * (1 - discount_pct / 100) AS net_revenue
FROM fact_sales;
```

**Exception:** Pre-computing expensive aggregates (e.g., `customer_lifetime_value` recalculated by a weekly ML model) is acceptable when the derivation is complex and not expressible as a simple SQL formula from fact columns.

---

### Anti-pattern 3 — Using NULL as a business value

**The mistake:** Using NULL to mean multiple different things in the same column.

```sql
-- What does NULL mean in discount_pct?
-- (a) no discount was applied → 0%
-- (b) discount information is unknown → missing data
-- (c) not applicable for this product type → N/A
-- All three land as NULL; downstream SUM(discount_pct) silently treats all as 0
```

**The fix:**
```sql
-- Use explicit sentinel values and a separate flag column
fact_sales (
    discount_pct        NUMERIC,  -- NULL only for case (b): genuinely unknown
    discount_pct_status TEXT      -- 'applied'/'none'/'unknown'/'not_applicable'
)
-- Or: COALESCE(discount_pct, 0) in the Gold model with documentation
```

**Dimension NULL anti-pattern:**
```sql
-- Never let a fact row have a NULL FK to a dimension
-- fact_sales.store_key = NULL means "no store" — breaks all store-level reports

-- Fix: create a special "Unknown" or "No Store" dimension row with key = -1
INSERT INTO dim_store (store_key, store_name, region)
VALUES (-1, 'Unknown', 'Unknown');

-- Now every fact row has a valid FK; queries GROUP BY store.region don't silently drop rows
```

---

### Anti-pattern 4 — The fan-out problem (join to wrong grain)

**The mistake:** Joining a fact table to a dimension that has more rows per entity than the fact grain expects, causing row multiplication and double-counted aggregates.

```sql
-- fact_orders grain: one row per order
-- dim_customer has one row per customer in theory, but...
-- someone added a contact_phone table and joined it: 3 phones per customer = 3 dim_customer rows

SELECT SUM(f.revenue)
FROM fact_orders f
JOIN dim_customer c ON f.customer_id = c.customer_id;
-- Revenue is triple-counted for customers with 3 phone numbers
```

**Diagnosis:** Add `COUNT(*)` to the query — if it exceeds the expected fact row count, you have fan-out.

**Fix options:**
1. Deduplicate the dimension before joining: `SELECT DISTINCT customer_id, ... FROM dim_customer`
2. Move the multi-value attribute to a separate bridge or satellite table
3. Aggregate the dimension before joining: `JOIN (SELECT customer_id, MAX(phone) FROM dim_customer GROUP BY customer_id) c`

---

### Anti-pattern 5 — Modelling time as a string

**The mistake:** Storing dates as `VARCHAR` because the source system sends dates as strings.

```sql
-- BROKEN: date stored as string
fact_sales (order_id, order_date VARCHAR)  -- '2024-01-15', '15/01/2024', '20240115'

-- Consequences:
SELECT SUM(revenue) FROM fact_sales WHERE order_date > '2024-01-01';
-- Lexicographic comparison: '15/01/2024' > '2024-01-01' → TRUE (wrong)
-- Partitioning/clustering by date_key is impossible
-- Date functions (EXTRACT, DATE_TRUNC) fail or require CAST on every query
```

**Fix:**
```sql
-- Always cast to DATE in the staging layer
SELECT CAST(order_date AS DATE) AS order_date FROM raw.orders;

-- Use a dim_date surrogate key for the fact table for maximum flexibility
fact_sales (order_id, date_key INT REFERENCES dim_date(date_key), ...)
```

---

### Anti-pattern summary table

| Anti-pattern | Root cause | Interview signal | Fix |
|---|---|---|---|
| Wrong grain | Mixing aggregation levels in one table | "Why is SUM wrong?" | One grain per fact table |
| Derived columns in fact | Storing computed values alongside source values | "Why did revenue and net_revenue diverge?" | Store atomic facts; derive in views |
| NULL overloading | NULL used for missing, N/A, and zero simultaneously | "Why does SUM ignore these rows?" | Explicit sentinel values + status column |
| Fan-out | Joining fact to dimension with unexpected cardinality | "Why is row count 3× expected?" | Bridge table or pre-deduplicate |
| String dates | Skipping type casting in staging | "Why does date filter return wrong rows?" | Always cast to DATE in staging |

---

## Summary

| Concept | Core idea | Key interview term |
|---|---|---|
| Dimensional Modelling & Star Schema | Facts + dimensions, one grain per fact table | Grain, surrogate key, conformed dimension |
| Slowly Changing Dimensions (SCD) | Handle attribute changes without losing historical accuracy | SCD Type 2, valid_from / valid_to |
| Normalisation & Normal Forms | 1NF/2NF/3NF remove redundancy; analytics layers deliberately denormalise | Partial dependency, transitive dependency, 3NF |
| Advanced Fact Patterns | Factless facts for events, bridge tables for many-to-many, role-playing dims | Fan-out, bridge table, junk dimension |
| Modelling Anti-patterns | Wrong grain, derived columns, NULL overloading, fan-out, string dates | Grain violation, double-counting, sentinel value |
