# Day 3 — Practice Exercises: Data Modelling

> Use `data/raw_orders.csv` and `data/customer_changes.csv` for all exercises.  
> Run SQL in PostgreSQL, DuckDB, SQLite, or any ANSI-SQL engine.

---

## Exercise 1 — Build a Star Schema from a Flat File

**Concept:** Dimensional Modelling & Star Schema

**Scenario:**  
`raw_orders.csv` is a wide flat file that came from an OLTP export. Your job is to split it into a proper star schema with one fact table and four dimension tables.

**Tasks:**

**1a. Define the grain.**  
Before writing any SQL, write one sentence stating what one row in your fact table represents.

**1b. Create the dimension tables.**  
Write `CREATE TABLE` statements for:
- `dim_customer` (customer_id, customer_name, customer_city, customer_country, customer_segment)
- `dim_product` (product_id, product_name, product_category, product_brand)
- `dim_store` (store_id, store_name, store_region)
- `dim_date` (date_id, order_date, day_of_week, month, quarter, year)

Each dimension should have a surrogate integer primary key (e.g., `customer_key SERIAL`).

**1c. Create the fact table.**  
Write `CREATE TABLE fact_sales (...)` with:
- Foreign keys to all four dimensions (using surrogate keys, not business keys)
- Measures: `quantity`, `unit_price`, `discount_pct`, `revenue` (computed as `quantity * unit_price * (1 - discount_pct/100)`)

**1d. Load the dimensions.**  
Write `INSERT INTO dim_customer SELECT DISTINCT ...` statements to populate each dimension from `raw_orders.csv` (loaded into a staging table).

**1e. Load the fact table.**  
Write the `INSERT INTO fact_sales SELECT ...` with proper joins to look up surrogate keys.

**1f. Answer this business question using the star schema:**
```sql
-- Revenue by product category and customer segment
-- Which category + segment combination generates the most revenue?
```

---

## Exercise 2 — Implement SCD Type 2

**Concept:** Slowly Changing Dimensions

**Scenario:**  
`customer_changes.csv` records six customer attribute changes that happened after the initial load. Your `dim_customer` table (from Exercise 1) has only current values. You need to apply SCD Type 2 to preserve history.

**Tasks:**

**2a. Alter `dim_customer` to support SCD Type 2.**  
Add the columns needed: `valid_from`, `valid_to`, `is_current`. Write the `ALTER TABLE` statements and set sensible defaults for all existing rows.

**2b. Apply the first change: `CHG001` (Alice C001 moves from Sydney → Melbourne on 2024-02-01).**  
Write the two SQL statements required:
1. Close the existing row (set `valid_to` and `is_current = FALSE`)
2. Insert the new row

**2c. Apply all six changes from `customer_changes.csv`.**  
Write a single SQL procedure or sequence of statements that processes all changes.  
- Note: CHG003 is a "no-change" control row — Alice's attributes did not actually change. How do you detect and skip it?
- Note: CHG004 is Alice's second move (Melbourne → Perth). How does your logic handle a customer who already has an SCD Type 2 history?

**2d. Write the "as-of" query.**  
How many orders did Alice (C001) place while living in Sydney?  
Write the JOIN between `fact_sales` and `dim_customer` that correctly filters to the Sydney period only.

**2e. Explain what would have gone wrong** if you had used SCD Type 1 (overwrite) instead.

---

## Exercise 3 — Design a Data Vault Structure

**Concept:** Data Vault Modelling

**Scenario:**  
Your company integrates order data from two source systems:
- **System A:** the e-commerce platform (uses numeric `customer_id` like `1001`, `1002`)
- **System B:** the in-store POS system (uses alphanumeric IDs like `CUST-A001`, `CUST-B002`)

Some customers exist in both systems (identified by email). You need to model this in Data Vault.

**Tasks:**

**3a. Design the Hubs.**  
Write `CREATE TABLE` statements for:
- `hub_customer` — business key: `customer_id` (from each source)
- `hub_order` — business key: `order_id`
- `hub_product` — business key: `product_id`

Include: `_hk` (hash key), business key column, `load_date`, `record_source`.

**3b. Design the Links.**  
Write `CREATE TABLE` for `link_order_customer` and `link_order_product`.

**3c. Design a Satellite.**  
Write `CREATE TABLE sat_customer_profile` with: `hub_customer_hk`, `load_date`, `load_end_date`, `hash_diff`, `customer_name`, `city`, `segment`, `record_source`.

**3d. Write the hash key calculation.**  
In PostgreSQL, write the SQL expression to generate a hash key for a customer:
```sql
-- hub_customer_hk should be MD5(UPPER(TRIM(customer_id || '||' || record_source)))
```
Why is it important to include `record_source` in the hash?

**3e. Answer this question:**  
A customer exists in both System A (customer_id=1001) and System B (CUST-A001) and they are the same person. Where in the Data Vault do you record this link? What pattern handles cross-system identity resolution?

---

## Exercise 4 — Build a One Big Table (OBT) and Benchmark It

**Concept:** OBT & Denormalisation

**Scenario:**  
Your BI team uses Metabase and the analysts cannot write JOINs. They need a single table they can filter and aggregate without any SQL knowledge.

**Tasks:**

**4a. Build the OBT.**  
Write `CREATE TABLE obt_sales AS SELECT ...` that pre-joins `fact_sales` with all four dimension tables, producing a flat table with all the columns a BI analyst needs.

Include at minimum:
- All fact measures: `quantity`, `unit_price`, `discount_pct`, `revenue`, `net_revenue`
- From dim_customer: `customer_name`, `customer_city`, `customer_country`, `customer_segment`
- From dim_product: `product_name`, `product_category`, `product_brand`
- From dim_store: `store_name`, `store_region`
- From dim_date: `order_date`, `order_month`, `order_quarter`, `order_year`

**4b. Compare query complexity.**  
Write the same query against both the star schema and the OBT:
```
"Total net revenue by store region and product category for Q1 2024"
```
Count the number of JOINs required in each version.

**4c. Storage trade-off.**  
Using the data in `raw_orders.csv`, calculate how many times the string `'SoundMax'` is repeated:
- In a normalised `dim_product` table
- In the OBT

In a Parquet-based lake, why is this repetition much cheaper than it would be in a row-oriented database?

**4d. Identify the OBT's failure mode.**  
Alice (C001) moved from Sydney to Melbourne (SCD Type 2 change). You have two version rows in `dim_customer`. When you build the OBT, what decision must you make? What is lost compared to the SCD Type 2 star schema?

---

## Exercise 5 — Write dbt Models for the Star Schema

**Concept:** dbt — Data Build Tool

**Scenario:**  
You have loaded `raw_orders.csv` into a warehouse table called `raw.orders`. You need to write three dbt models: a staging model, a dimension model, and a fact model.

**Tasks:**

**5a. Write the staging model `stg_orders.sql`.**  
The staging model should:
- Rename columns to snake_case
- Cast `order_date` to `DATE`
- Cast `unit_price` and `discount_pct` to the correct numeric types
- Add `COALESCE(discount_pct, 0) AS discount_pct` to handle NULLs
- Filter out any rows where `order_id IS NULL`

**5b. Write the dimension model `dim_product.sql`.**  
Using `{{ ref('stg_orders') }}`, build a `dim_product` model that:
- Selects `DISTINCT product_id, product_name, product_category, product_brand`
- Generates a surrogate key using `ROW_NUMBER() OVER (ORDER BY product_id)`

**5c. Write the fact model `fct_sales.sql`.**  
Using `{{ ref('stg_orders') }}` and `{{ ref('dim_product') }}`:
- Join on `product_id` to bring in the surrogate key
- Compute `revenue = quantity * unit_price`
- Compute `net_revenue = quantity * unit_price * (1 - discount_pct / 100.0)`
- Add `{{ config(materialized='incremental', unique_key='order_id') }}` and an incremental filter on `order_date`

**5d. Write the schema YAML for `fct_sales`.**  
Write the `_schema.yml` that defines:
- `order_id`: `unique` + `not_null` tests
- `net_revenue`: `not_null` + `expression_is_true: ">= 0"` test
- `product_id`: `relationships` test pointing to `dim_product`

**5e. Draw the dbt DAG.**  
Write the dependency chain as a text diagram showing which models feed into which.

---

## Bonus Challenge — Full Modelling Pipeline

Using only `raw_orders.csv` and `customer_changes.csv`:

1. Build the full star schema (Exercise 1)
2. Apply SCD Type 2 for all six customer changes (Exercise 2)
3. Build the OBT using the **current** dimension values
4. Write a query that proves the OBT gives wrong revenue attribution for Alice's historical orders (because it uses current city = Perth, not Sydney at time of purchase)
5. Write a query against the star schema + SCD Type 2 that gives the correct answer
6. Explain in 3 sentences why this is the core trade-off between OBT and SCD Type 2 star schema

This is the exact scenario that comes up in senior data engineering interviews.
