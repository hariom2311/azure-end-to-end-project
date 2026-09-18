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

**1c.** Create `fact_sales` with FKs to all four dimensions (surrogate keys) and measures: `quantity`, `unit_price`, `discount_pct`. Do NOT store derived columns like `revenue` — explain why in one sentence.

**1d.** Load the dimensions from `raw_orders.csv` (first load into a staging table), then load `fact_sales` with joins to resolve surrogate keys.

**1e.** Answer this business question using only the star schema:
```sql
-- Net revenue (quantity * unit_price * (1 - discount_pct/100))
-- by product_category and customer_segment.
-- Which combination generates the highest net revenue?
```

**1f.** What is a conformed dimension? Which dimension in your star schema could be shared with a hypothetical `fact_marketing_spend` table without any changes?

---

## Exercise 2 — Implement SCD Type 2 for Customer Changes

**Concept:** Slowly Changing Dimensions (SCD)

**Scenario:**  
`customer_changes.csv` contains six attribute changes that occurred after the initial load. Apply SCD Type 2 to `dim_customer`.

**Tasks:**

**2a.** Alter `dim_customer` to support SCD Type 2. Add `valid_from DATE`, `valid_to DATE`, `is_current BOOLEAN`. Set `valid_from = '2024-01-01'`, `valid_to = '9999-12-31'`, `is_current = TRUE` for all existing rows.

**2b.** Apply `CHG001` (Alice C001: Sydney → Melbourne, 2024-02-01). Write both SQL statements:
1. Close the existing row (`valid_to`, `is_current = FALSE`)
2. Insert the new row

**2c.** Process all six changes from `customer_changes.csv`. Address two edge cases:
- `CHG003` has `old_city = new_city` (no actual change). How do you detect and skip it?
- `CHG004` is Alice's second move (Melbourne → Perth). Show the state of `dim_customer` for C001 after both CHG001 and CHG004 are applied — how many rows, what are the `valid_from`/`valid_to` values?

**2d.** Write the as-of query: "What was Alice's city on 2024-03-01?" Filter `dim_customer` using date ranges, not `is_current`.

**2e.** Write the revenue attribution query: "How much net revenue did Alice generate while living in Sydney?" Join `fact_sales` to the SCD Type 2 dimension correctly so only the Sydney-period orders are counted.

**2f.** Show what would have happened if SCD Type 1 (overwrite) had been used instead — what does the same revenue query return, and why is it wrong?

---

## Exercise 3 — Normalise and Denormalise

**Concept:** Normalisation, Normal Forms & Denormalisation Trade-offs

**Scenario:**  
`raw_orders.csv` is a single wide flat table. Analyse its normal forms and then deliberately denormalise for an analytics use case.

**Tasks:**

**3a. Identify 1NF violations (if any).**  
Is there any column that stores multiple values in one cell? Is every row uniquely identifiable?

**3b. Identify 2NF violations.**  
Assume the staging table uses a composite primary key of `(order_id, product_id)` (order-line grain). List every column that depends only on part of this key, not on the full composite key.

**3c. Identify 3NF violations.**  
List all transitive dependencies in the flat file — where column A → B → C (B is not a key).  
Example: does `store_id → store_name`? Does `store_id → store_region`?

**3d. Write the fully normalised 3NF schema** as a set of `CREATE TABLE` statements:
- What tables are needed?
- What are the primary and foreign keys?
- Which columns move to which table?

**3e. Deliberately denormalise for analytics.**  
Write `CREATE TABLE dim_product AS SELECT DISTINCT product_id, product_name, product_category, product_brand FROM staging_orders`. Which normal form does this violate compared to 3NF? Why is this acceptable in a dimensional model?

**3f. Count the update anomaly.**  
If the brand `SoundMax` is renamed to `SoundPro`:
- How many rows must be updated in the 3NF schema?
- How many rows must be updated in `dim_product`?
- Which approach is safer from a data consistency perspective, and why?

**3g. Write the 3NF join query vs. the star schema query.**  
For the question "total quantity sold per product category," write both versions:
1. Joining the 3NF tables (products → order_items → orders)
2. Using the star schema (fact_sales → dim_product)

Count the number of joins in each version and comment on the difference.

---

## Bonus Challenge — Full Modelling Pipeline

Using both `raw_orders.csv` and `customer_changes.csv`:

1. Load `raw_orders.csv` into a staging table
2. Identify and document all 1NF, 2NF, and 3NF violations
3. Build a 3NF schema (minimum 4 tables) and load data into it
4. Build a star schema (fact + 4 dimensions) from the 3NF tables
5. Apply SCD Type 2 for all 6 customer changes
6. Write a query that proves SCD Type 2 correctly attributes Alice's Sydney-period revenue to Sydney, even after she has since moved to Perth
7. Write a query that shows what SCD Type 1 would have returned for the same question (wrong answer)
8. In one paragraph, explain why the difference between these two answers is the exact definition of the SCD problem — and why it matters to a business that tracks sales performance by region
