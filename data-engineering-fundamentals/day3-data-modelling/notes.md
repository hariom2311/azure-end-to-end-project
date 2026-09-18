# Day 3 — Data Modelling

## Overview

Data modelling is the discipline of structuring data so it is easy to query, understand, and trust. A well-modelled dataset answers business questions in one or two joins; a poorly modelled one requires six joins, produces wrong aggregates, and is rebuilt every six months. Day 3 covers three core modelling concepts that appear in almost every data engineering interview — dimensional modelling, slowly changing dimensions, and the normalisation principles that explain why OLTP schemas look nothing like analytical schemas.

**The 3 concepts:**
1. Dimensional Modelling & Star Schema
2. Slowly Changing Dimensions (SCD)
3. Normalisation, Normal Forms & Denormalisation Trade-offs

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

## Summary

| Concept | Core idea | Key interview term |
|---|---|---|
| Dimensional Modelling & Star Schema | Facts + dimensions, one grain per fact table | Grain, surrogate key, conformed dimension |
| Slowly Changing Dimensions (SCD) | Handle attribute changes without losing historical accuracy | SCD Type 2, valid_from / valid_to |
| Normalisation & Normal Forms | 1NF/2NF/3NF remove redundancy; analytics layers deliberately denormalise | Partial dependency, transitive dependency, 3NF |
