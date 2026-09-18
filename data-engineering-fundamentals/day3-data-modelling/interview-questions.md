# Day 3 — Interview Questions: Data Modelling

> 40 questions across all 5 concepts. Attempt your own answer first, then check `interview-solutions.md`.

---

## Concept 1: Dimensional Modelling & Star Schema

**Q1 (Warm-up)**  
What is the difference between a fact table and a dimension table? Give one example of each from an e-commerce domain.

---

**Q2 (Warm-up)**  
What is the "grain" of a fact table and why must you define it before designing any columns?

---

**Q3 (Conceptual)**  
What is the difference between a Star Schema and a Snowflake Schema? Which would you choose for a BI analytics layer and why?

---

**Q4 (Scenario)**  
A data engineer builds a `fact_sales` table with grain at the order header level (one row = one order). An analyst asks: "What is the average revenue per product category?" The engineer says this query is impossible with the current model. Why? What grain change fixes it?

---

**Q5 (Scenario)**  
You are designing a fact table for a ride-sharing platform. Each trip has: a driver, a rider, a pickup location, a dropoff location, a start time, an end time, a fare, and a rating. Define the grain and list which columns go in the fact table vs. which go in dimension tables.

---

**Q6 (Tricky)**  
What is a "conformed dimension"? Why is it important when an organisation has multiple star schemas (e.g., one for sales, one for marketing, one for logistics)?

---

**Q7 (Tricky)**  
What are the three types of fact tables (Transaction, Periodic Snapshot, Accumulating Snapshot)? Give a concrete example from a logistics domain for each type.

---

**Q8 (Deep dive)**  
What is a surrogate key and why does a dimensional model use surrogate keys instead of business keys as the primary key on dimension tables?

---

## Concept 2: Slowly Changing Dimensions (SCD)

**Q9 (Warm-up)**  
What problem does SCD Type 2 solve that SCD Type 1 cannot?

---

**Q10 (Conceptual)**  
Describe the mechanics of implementing SCD Type 2. What columns must be added to the dimension table? How does a fact table join correctly to a Type 2 dimension?

---

**Q11 (Scenario)**  
A sales report shows that Alice (customer C001) generated $450 in Q1. In April, Alice moves from Sydney to Melbourne. An analyst runs the same Q1 report in May and now sees $450 attributed to Melbourne. Which SCD type was used and what was the consequence? What type should have been used?

---

**Q12 (Scenario)**  
You have a `dim_product` table with SCD Type 2. A product changes its category from "Electronics" to "Computers" on March 15. You have 500M rows of `fact_sales` going back 5 years. After the SCD Type 2 update, how does a query `WHERE product_category = 'Electronics'` behave for pre-March and post-March sales?

---

**Q13 (Tricky)**  
A customer changes their email address three times in one year. How does SCD Type 2 handle this? What does `dim_customer` look like after three changes? How many rows does the customer have?

---

**Q14 (Tricky)**  
What is the risk of using the natural surrogate key pattern `valid_to = '9999-12-31'` to mark the current row? What is an alternative approach and what are its trade-offs?

---

**Q15 (System design)**  
Design a dbt model that implements SCD Type 2 on a `customers` source table. The source is a daily full extract (all current customer rows). Describe the logic in pseudocode: how do you detect new records, changed records, and unchanged records?

---

## Concept 3: Data Vault Modelling

**Q16 (Warm-up)**  
Name the three building blocks of a Data Vault model and describe in one sentence what each stores.

---

**Q17 (Conceptual)**  
Why does Data Vault use a hash key (MD5 or SHA-1 of the business key) rather than a database-generated surrogate key (SERIAL / IDENTITY)?

---

**Q18 (Scenario)**  
Your company acquires another business. The acquired company has a customer database. Some customers exist in both systems. In a dimensional model, how would you handle this? In a Data Vault, what specific construct handles cross-system identity resolution?

---

**Q19 (Conceptual)**  
What is a `hash_diff` column in a Data Vault satellite and why is it important for performance?

---

**Q20 (Tricky)**  
A Data Vault hub stores the business key from the source system. A source system changes its primary key format (from numeric IDs to UUIDs). How does this affect the Data Vault, specifically the hub and its hash key? What do you do with the existing rows?

---

## Concept 4: One Big Table (OBT) & Denormalisation

**Q21 (Warm-up)**  
What is a One Big Table (OBT) and what problem does it solve for non-technical BI users?

---

**Q22 (Conceptual)**  
A star schema has 1 fact table and 4 dimension tables. An analyst query requires 3 joins. An OBT has all columns pre-joined. What are the two main trade-offs of choosing OBT over a star schema for large datasets?

---

**Q23 (Scenario)**  
An OBT stores `customer_city` for every order row. Alice (customer C001) has placed 50,000 orders and recently moved from Sydney to Melbourne. How many rows in the OBT must be updated? What is the performance implication? How would a star schema + SCD Type 2 handle this differently?

---

**Q24 (Conceptual)**  
Why does columnar compression (Parquet/ORC) reduce the storage penalty of OBT denormalisation compared to a row-oriented database like PostgreSQL?

---

**Q25 (Tricky)**  
When would you choose an OBT over a star schema even for a production analytics platform? Give three specific conditions.

---

## Concept 5: dbt — Data Build Tool

**Q26 (Warm-up)**  
What does dbt do? What part of the ELT pipeline does it handle and what does it not do?

---

**Q27 (Conceptual)**  
What is the difference between a dbt `view`, `table`, `incremental`, and `ephemeral` materialisation? When would you use each?

---

**Q28 (Scenario)**  
A dbt `incremental` model for `fct_sales` uses `WHERE order_date >= (SELECT MAX(order_date) FROM {{ this }})` as its incremental filter. A late-arriving order from yesterday arrives in today's source data. Is it captured? What is the standard fix?

---

**Q29 (Scenario)**  
A dbt model `fct_revenue` depends on `dim_customer`, `dim_product`, and `stg_orders`. A developer changes the column name `customer_city` to `city` in `dim_customer`. `fct_revenue` still references `customer_city`. What happens when `dbt run` is executed? How would `dbt test` catch this before it reaches production?

---

**Q30 (Conceptual)**  
What is `{{ ref() }}` in dbt and why is it superior to hardcoding table names like `FROM analytics.dim_customer`?

---

**Q31 (Deep dive)**  
Explain dbt's `unique_key` parameter in an incremental model. What SQL does dbt generate behind the scenes for a `unique_key = 'order_id'`? What happens if two source rows have the same `order_id`?

---

**Q32 (Tricky)**  
A dbt project has 200 models. A senior engineer says "staging models should always be views, not tables." Do you agree? What is the reasoning, and when would you break this rule?

---

## Mixed / Senior-Level Questions

**Q33**  
A startup has one PostgreSQL database with 15 tables. They want to build their first analytics layer. They ask you to choose between: (A) replicate to Snowflake and build a star schema with dbt, (B) build an OBT directly in PostgreSQL, or (C) implement Data Vault. Which do you recommend and why? What signals in their situation would change your recommendation?

---

**Q34**  
What is the "fan-out problem" in dimensional modelling? Give an example where joining a fact table to two different dimension tables at different granularities produces incorrect aggregates.

---

**Q35**  
A `fact_orders` table has grain at the order line item level. A business analyst wants to add a `customer_lifetime_value` column to the fact table. Why is this wrong? Where should it go?

---

**Q36**  
You are inheriting a data warehouse with no documentation. You find four tables named `orders`, `orders_new`, `orders_final`, and `orders_final_v2`. How do you determine which is the canonical model? What governance practices prevent this situation?

---

**Q37**  
A fact table has 500M rows. A new business requirement needs a `region` column that does not currently exist on any table. It must be derived from `customer_city` via a lookup table. How do you add this to the dimensional model without reprocessing all 500M rows?

---

**Q38**  
What is a "degenerate dimension"? Give an example from a retail domain and explain why it is stored in the fact table rather than a separate dimension table.

---

**Q39**  
Explain how dbt's `--select` flag with graph operators (`+model`, `model+`, `@model`) enables targeted rebuilds in a large project. Give a scenario where you would use each.

---

**Q40**  
A company runs `dbt run` nightly and all tests pass. The next day, an analyst reports that revenue numbers are 15% lower than expected for the last 7 days. `dbt test` still passes because all column-level constraints (not_null, unique) are satisfied. What category of test is missing, and how would you add it to the dbt project?

---
