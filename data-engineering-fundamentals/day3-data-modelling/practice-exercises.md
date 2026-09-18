# Day 3 — Practice Exercises: Data Modelling

> Use `data/raw_orders.csv` and `data/customer_changes.csv` for all exercises.  
> Run SQL in PostgreSQL, DuckDB, SQLite, or any ANSI-SQL engine.

---

## Exercise 1 — Build a Star Schema from a Flat File

**Concept:** Dimensional Modelling & Star Schema

**Scenario:**  
`raw_orders.csv` is a wide flat file from an OLTP export. Split it into a proper star schema.

**Tasks:**

**1a.** Write one sentence defining the grain of your fact table.

**1b.** Create the four dimension tables with surrogate keys:
- `dim_customer(customer_key SERIAL, customer_id, customer_name, customer_city, customer_country, customer_segment)`
- `dim_product(product_key SERIAL, product_id, product_name, product_category, product_brand)`
- `dim_store(store_key SERIAL, store_id, store_name, store_region)`
- `dim_date(date_key SERIAL, order_date DATE, day_of_week, month, quarter, year)`

**1c.** Create `fact_sales` with FKs to all four dimensions (surrogate keys) and measures: `quantity`, `unit_price`, `discount_pct`. Do NOT include derived columns like `revenue` or `net_revenue` — explain why.

**1d.** Load the dimensions from `raw_orders.csv` (first load into a staging table), then load `fact_sales` with a join to look up surrogate keys.

**1e.** Answer this question using the star schema (zero raw table access):
```sql
-- Net revenue (quantity * unit_price * (1 - discount_pct/100)) by product_category and customer_segment
-- Which combination generates the highest net revenue?
```

---

## Exercise 2 — Implement SCD Type 2 for Customer Changes

**Concept:** Slowly Changing Dimensions (SCD)

**Scenario:**  
`customer_changes.csv` contains six attribute changes that occurred after the initial load. Apply SCD Type 2 to `dim_customer`.

**Tasks:**

**2a.** Alter `dim_customer` to support SCD Type 2. Add `valid_from DATE`, `valid_to DATE`, `is_current BOOLEAN`. Set `valid_from = '2024-01-01'`, `valid_to = '9999-12-31'`, `is_current = TRUE` for all existing rows.

**2b.** Apply `CHG001` (Alice C001: Sydney → Melbourne, 2024-02-01). Write both statements:
1. Close the existing row
2. Insert the new row

**2c.** Process all six changes. Note:
- `CHG003` is a no-change control row — how do you detect and skip it?
- `CHG004` is Alice's second relocation (Melbourne → Perth). Show `dim_customer` for C001 after both moves — how many rows?

**2d.** Write the as-of query: "What was Alice's city on 2024-03-01?" Use a date range filter on `dim_customer`, not `is_current`.

**2e.** Write the revenue attribution query: "How much revenue did Alice generate while living in Sydney?" Join `fact_sales` to the SCD Type 2 dimension correctly.

---

## Exercise 3 — Normalise and Denormalise

**Concept:** Normalisation, Normal Forms & Denormalisation Trade-offs

**Scenario:**  
`raw_orders.csv` is one wide flat table. Analyse its normal forms and then deliberately denormalise it for an analytics use case.

**Tasks:**

**3a. Identify 1NF violations (if any) in `raw_orders.csv`.**  
Is there any column that stores multiple values in one cell? Is every row uniquely identifiable?

**3b. Identify 2NF violations.**  
The staging table has a composite key of `(order_id, product_id)` (order-line grain). List every column that depends on only part of this key, not the whole key.

**3c. Identify 3NF violations.**  
List all transitive dependencies — columns where column A → column B → column C (B is not a key).  
Example: does `store_id → store_name → store_region`?

**3d. Write the fully normalised 3NF schema** as a set of `CREATE TABLE` statements:
- What tables exist?
- What are the primary and foreign keys?
- Which columns move to which table?

**3e. Now deliberately denormalise for analytics.**  
Write a `CREATE TABLE dim_product_denorm AS SELECT DISTINCT ...` that collapses `product_id`, `product_name`, `product_category`, `product_brand` into one row per product. Which normal form does this violate? Why is this acceptable for an analytics dimension?

**3f. Count the update anomaly.**  
If `SoundMax` brand is renamed to `SoundPro` across all products:
- How many rows must be updated in the 3NF schema?
- How many rows must be updated in `dim_product_denorm`?
- Which is safer and why?

---

## Exercise 4 — Build a Factless Fact and Bridge Table

**Concept:** Advanced Fact Table Patterns

**Scenario:**  
The business has two new requirements that `fact_sales` cannot answer directly:
1. "Which products were available in each store but had zero sales on a given day?"
2. "Each order can have multiple promotions applied. Revenue must not be double-counted."

**Tasks:**

**4a. Build a factless fact table for product-store eligibility.**

First create a small eligibility dataset:
```sql
-- Assume all products were eligible in all stores on all order dates
-- (derive from the distinct combinations in raw_orders)
CREATE TABLE fact_product_store_eligibility AS
SELECT DISTINCT
    d.date_key,
    p.product_key,
    s.store_key
FROM dim_date d
CROSS JOIN dim_product p
CROSS JOIN dim_store s
WHERE d.order_date IN (SELECT DISTINCT order_date FROM dim_date);
```

Now write the query: "Find all product-store-date combinations that had no sales."  
Use `LEFT JOIN` between `fact_product_store_eligibility` and `fact_sales`.

**4b. Build a bridge table for promotions.**

Create and populate:
```sql
CREATE TABLE dim_promotion (
    promotion_key SERIAL PRIMARY KEY,
    promotion_code TEXT,
    promotion_name TEXT,
    discount_type TEXT  -- 'pct', 'fixed', 'bogo'
);

INSERT INTO dim_promotion (promotion_code, promotion_name, discount_type) VALUES
  ('PROMO10', '10% Off Electronics', 'pct'),
  ('FREESHIP', 'Free Shipping', 'fixed'),
  ('BOGO', 'Buy One Get One', 'bogo');

CREATE TABLE bridge_order_promotions (
    order_id      INT,
    promotion_key INT,
    weighting_factor NUMERIC(4,3) DEFAULT 1.0
);

-- Apply promotions to some orders (simulated data)
INSERT INTO bridge_order_promotions VALUES
  (1001, 1, 1.0),    -- order 1001 has one promotion (full weight)
  (1003, 1, 0.5),    -- order 1003 has two promotions (split weight)
  (1003, 2, 0.5),
  (1007, 3, 1.0);
```

Write the query: "Total attributed revenue per promotion" using `bridge_order_promotions`. Verify the result for order 1003 — `SUM(revenue * weighting_factor)` should equal `revenue` for that order, not double it.

**4c. Role-playing dimension.**  
Add two date FK columns to `fact_sales`: `order_date_key` (already exists) and `ship_date_key`.  
For the exercise, simulate ship date = order date + 3 days.  
Write the query: "Average days between order and ship, by store region" using two joins to `dim_date` with different aliases.

---

## Exercise 5 — Diagnose and Fix Anti-patterns

**Concept:** Data Modelling Anti-patterns

**Scenario:**  
A colleague hands you four "fact tables" they built. Each has at least one anti-pattern. Diagnose and fix each one.

**5a. Wrong grain**

```sql
CREATE TABLE broken_fact_a (
    order_id            INT,
    product_id          INT,
    line_revenue        NUMERIC,   -- revenue for this line item
    total_order_revenue NUMERIC    -- total for the whole order (repeated per line)
);
-- Sample data:
-- (1001, P101, 120.00, 350.00)
-- (1001, P102, 130.00, 350.00)   ← total_order_revenue repeats
-- (1001, P104, 100.00, 350.00)
```

1. What happens when you run `SELECT SUM(total_order_revenue) FROM broken_fact_a WHERE order_id = 1001`?
2. Fix the design — which column must be removed, and where does it belong?

**5b. Fan-out**

```sql
-- dim_customer accidentally has 2 rows for C001 (data quality issue: duplicate load)
-- customer_key=1, customer_id=C001, city=Sydney
-- customer_key=5, customer_id=C001, city=Sydney  ← duplicate

SELECT SUM(f.unit_price * f.quantity) AS revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE c.customer_id = 'C001';
```

1. What value does this query return compared to the correct revenue for C001?
2. Write the fix: either a dedup CTE or a DISTINCT in the join.
3. How would you prevent this at load time?

**5c. NULL overloading**

```sql
CREATE TABLE broken_fact_c (
    order_id     INT,
    discount_pct NUMERIC   -- NULL means three different things
);
-- NULL row 1: no discount (0%)
-- NULL row 2: discount info not collected (unknown)
-- NULL row 3: product type not eligible for discounts (N/A)

SELECT AVG(discount_pct) FROM broken_fact_c;  -- what does this return?
```

1. Explain why `AVG(discount_pct)` gives a misleading result.
2. Redesign the table to disambiguate the three NULL meanings.

**5d. String date**

```sql
CREATE TABLE broken_fact_d (
    order_id   INT,
    order_date VARCHAR(20),  -- stored as '15/01/2024'
    revenue    NUMERIC
);

SELECT SUM(revenue) FROM broken_fact_d
WHERE order_date > '2024-01-10';  -- will this work correctly?
```

1. What does the `WHERE` clause actually compare?
2. Write the staging fix using `CAST` or `TO_DATE`.
3. Why is a `date_key INT` + `dim_date` pattern even better than storing `order_date DATE`?

---

## Bonus Challenge — End-to-End Modelling Audit

Using `raw_orders.csv`:

1. Load into a staging table
2. Normalise to 3NF (minimum 4 tables)
3. Build a star schema (fact + 4 dimensions) from the 3NF tables
4. Apply SCD Type 2 for all 6 changes in `customer_changes.csv`
5. Build a factless eligibility table and identify zero-sale product-store-date combinations
6. Introduce one deliberate anti-pattern (wrong grain, fan-out, or string date) and write a query that shows the incorrect result
7. Fix the anti-pattern and show the correct result side by side
8. Write a one-paragraph explanation of why this anti-pattern is hard to detect with standard `not_null` and `unique` tests — and what test would catch it
