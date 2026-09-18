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
What is the difference between a Star Schema and a Snowflake Schema? Which would you recommend for a BI analytics layer and why?

---

**Q4 (Scenario)**  
A data engineer builds a `fact_sales` table at the order header grain (one row = one order). An analyst asks: "What is the revenue per product category?" Why is this impossible with the current grain? What change fixes it?

---

**Q5 (Tricky)**  
What are the three types of fact tables — Transaction, Periodic Snapshot, and Accumulating Snapshot? Give a concrete example of each from a logistics or supply chain domain.

---

**Q6 (Tricky)**  
What is a surrogate key and why does a star schema use surrogate keys instead of business keys as primary keys on dimension tables? Give two specific scenarios where a business key would fail.

---

**Q7 (Deep dive)**  
What is a conformed dimension and why does it matter when an organisation has multiple star schemas (e.g., one for sales, one for marketing, one for logistics)?

---

**Q8 (System design)**  
Design a fact table for a ride-sharing platform. Each trip has a driver, a rider, a pickup location, a dropoff location, a start time, a fare, and a rating. Define the grain, list which columns belong in the fact vs. dimension tables, and identify any role-playing dimensions.

---

## Concept 2: Slowly Changing Dimensions (SCD)

**Q9 (Warm-up)**  
What problem does SCD Type 2 solve that SCD Type 1 cannot?

---

**Q10 (Conceptual)**  
Describe the full mechanics of SCD Type 2. What columns must be added? How does the fact table join correctly to a Type 2 dimension?

---

**Q11 (Scenario)**  
A Q1 sales report was run in February and showed Alice (C001) generated $450 in Sydney. Alice moved to Melbourne in April. The same Q1 report run in May now shows $450 attributed to Melbourne. Which SCD type was used and what was the consequence? What type should have been used?

---

**Q12 (Scenario)**  
A product changes its category from "Electronics" to "Computers" on March 15. You have 500M rows of `fact_sales` going back 5 years. After applying SCD Type 2, how does a query `WHERE product_category = 'Electronics'` behave for pre-March vs. post-March sales? How many fact rows are touched?

---

**Q13 (Tricky)**  
A customer changes their email address three times in one year. How does `dim_customer` look after all three changes? How many rows does this customer have? How does the fact table remain correct?

---

**Q14 (Tricky)**  
What is the risk of using `valid_to = '9999-12-31'` as a sentinel for the current row? What is an alternative approach and what are the trade-offs?

---

**Q15 (System design)**  
You receive a daily full extract of the `customers` table (all current rows, no history included). Describe the logic — in pseudocode or SQL — of a pipeline that implements SCD Type 2 from this daily snapshot. How do you detect new, changed, and unchanged records?

---

## Concept 3: Normalisation, Normal Forms & Denormalisation Trade-offs

**Q16 (Warm-up)**  
What is the difference between 1NF, 2NF, and 3NF? Give one example violation for each.

---

**Q17 (Conceptual)**  
Why are OLTP databases typically designed in 3NF, but analytical databases deliberately violate it? What is the cost of 3NF for analytical queries?

---

**Q18 (Scenario)**  
A staging table has these columns: `(order_id, product_id, product_name, category, quantity, store_id, store_city)`. The primary key is `(order_id, product_id)`. Identify all 2NF and 3NF violations and write the corrected schema.

---

**Q19 (Conceptual)**  
What is a transitive dependency? Give an example from an e-commerce schema and explain which normal form it violates.

---

**Q20 (Tricky)**  
A dimension table `dim_product` has columns `(product_id, product_name, category, subcategory, brand, brand_country)`. Is this table in 3NF? Identify the violation and explain whether you would fix it or leave it — and why.

---

**Q21 (Scenario)**  
A data engineer argues: "We should keep our Silver layer in 3NF and only denormalise in Gold." Another says: "3NF in the warehouse is over-engineering — just use a star schema everywhere." Which is closer to correct and why?

---

**Q22 (Tricky)**  
What is the update anomaly, insertion anomaly, and deletion anomaly? Give one example of each from a poorly normalised `orders` table that has customer data embedded directly.

---

## Concept 4: Advanced Fact Table Patterns

**Q23 (Warm-up)**  
What is a factless fact table? Give two scenarios where you need one: one for event occurrence, one for coverage/eligibility.

---

**Q24 (Conceptual)**  
How do you calculate zero-sales days for products using a factless fact table? Why can't you derive this from `fact_sales` alone?

---

**Q25 (Scenario)**  
An order can have multiple promotions applied. If you add a `promotion_id` FK directly to `fact_orders`, what goes wrong when you run `SUM(revenue) GROUP BY promotion_name`? What is the correct modelling pattern?

---

**Q26 (Conceptual)**  
What is a role-playing dimension? Give an example using `dim_date` in an order fact table that has three different date foreign keys.

---

**Q27 (Tricky)**  
What is a junk dimension? Why is it preferable to keeping low-cardinality flag columns directly in the fact table?

---

**Q28 (Scenario)**  
`fact_orders` has three FK columns to `dim_date`: `order_date_key`, `ship_date_key`, `delivery_date_key`. Write the SQL to calculate average delivery time (days from order to delivery) by month of order. Show how you alias the dimension table for each role.

---

**Q29 (Deep dive)**  
Compare accumulating snapshot fact tables to transaction fact tables. When does an accumulating snapshot become inappropriate — what volume or velocity threshold makes it impractical?

---

## Concept 5: Data Modelling Anti-patterns

**Q30 (Warm-up)**  
What is the "wrong grain" anti-pattern? Give a concrete example and explain what symptom in query results reveals it.

---

**Q31 (Scenario)**  
A `fact_sales` table has columns `quantity`, `unit_price`, `discount_pct`, `revenue`, and `net_revenue`. A business analyst changes the definition of `revenue` to include tax. Which columns must be recalculated and reloaded? What is the better design?

---

**Q32 (Scenario)**  
A query `SELECT SUM(revenue) FROM fact_sales JOIN dim_customer ON fact_sales.customer_id = dim_customer.customer_id` returns a number that is 40% higher than expected. What is the most likely cause and how do you diagnose it?

---

**Q33 (Tricky)**  
A `discount_pct` column is NULL in 30% of rows. Three different teams interpret NULL differently: "no discount applied," "discount unknown," and "not applicable for this product type." What is wrong with using NULL for all three? Design the fix.

---

**Q34 (Scenario)**  
A fact table stores `order_date` as `VARCHAR(20)` in the format `'DD/MM/YYYY'`. A query filters `WHERE order_date > '2024-06-01'`. What are two specific ways this can produce wrong results?

---

**Q35 (Deep dive)**  
Explain the fan-out problem with a worked example. How do you detect it before it reaches production? What dimension design choices prevent it?

---

## Mixed / Senior-Level Questions

**Q36**  
A startup has one PostgreSQL database with 15 tables and 10 GB of data. They ask you to choose between: (A) build a star schema directly in PostgreSQL, (B) replicate to Snowflake and build a star schema with dbt, (C) build one big flat table. Which do you recommend and what signals would change your recommendation?

---

**Q37**  
What is a degenerate dimension? Give an example from a retail domain and explain why it lives in the fact table rather than a dimension table.

---

**Q38**  
You are inheriting a data warehouse with four tables named `orders`, `orders_v2`, `orders_final`, and `orders_final_v2`. No documentation exists. How do you identify the canonical table, and what governance processes prevent this situation?

---

**Q39**  
A 500M-row fact table needs a new `region` column derived from `customer_city` via a lookup. How do you add it without reprocessing all 500M rows?

---

**Q40**  
A `dbt test` suite runs `not_null` and `unique` on every column of `fct_sales` and all tests pass. An analyst reports revenue is 15% lower than expected for the last 7 days. What category of test is missing and how would you write it?

---
