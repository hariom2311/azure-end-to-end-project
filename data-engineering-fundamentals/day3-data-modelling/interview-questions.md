# Day 3 — Interview Questions: Data Modelling

> 30 questions across 3 concepts. Attempt your own answer first, then check `interview-solutions.md`.

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

**Q5 (Conceptual)**  
What is a surrogate key and why does a star schema use surrogate keys instead of business keys as the primary key on dimension tables? Give two scenarios where a business key would fail.

---

**Q6 (Tricky)**  
What is a conformed dimension? Why does it matter when an organisation has multiple star schemas (sales, marketing, logistics)?

---

**Q7 (Tricky)**  
What are the three types of fact tables — Transaction, Periodic Snapshot, and Accumulating Snapshot? Give one concrete example from a logistics domain for each.

---

**Q8 (System design)**  
Design a fact table for a ride-sharing platform. Each trip has a driver, a rider, a pickup location, a dropoff location, a start time, a fare, and a rating. Define the grain, identify which columns go in the fact vs. dimension tables, and name any role-playing dimensions you would use.

---

**Q9 (Tricky)**  
What is a degenerate dimension? Give a retail example and explain why it lives in the fact table rather than a dedicated dimension table.

---

**Q10 (Deep dive)**  
A `fact_sales` table has `quantity`, `unit_price`, `discount_pct` as atomic measures. A colleague wants to add `revenue` and `net_revenue` as pre-computed columns. What is wrong with this approach and what is the correct design?

---

## Concept 2: Slowly Changing Dimensions (SCD)

**Q11 (Warm-up)**  
What problem does SCD Type 2 solve that SCD Type 1 cannot?

---

**Q12 (Conceptual)**  
Describe the full mechanics of SCD Type 2. What columns must be added to the dimension table? How does a fact table join correctly to retrieve historical attribute values?

---

**Q13 (Scenario)**  
A Q1 sales report was run in February and showed Alice (C001) generated $450 in revenue attributed to Sydney. Alice moved to Melbourne in April. The same Q1 report run in May now shows $450 attributed to Melbourne. Which SCD type was used, what was the consequence, and what type should have been used?

---

**Q14 (Scenario)**  
A product changes its category from "Electronics" to "Computers" on March 15. You have 500M rows of `fact_sales` going back 5 years. After applying SCD Type 2, how does a query `WHERE product_category = 'Electronics'` behave for pre-March vs. post-March sales? How many fact rows are modified?

---

**Q15 (Tricky)**  
A customer changes their email address three times in one year. How does `dim_customer` look after all three changes? How many rows does this customer have? How do historical fact rows remain accurate without being updated?

---

**Q16 (Tricky)**  
What is the risk of using `valid_to = '9999-12-31'` as a sentinel for the current row? What is an alternative and what are its trade-offs?

---

**Q17 (System design)**  
You receive a daily full extract of the `customers` table (all current rows, no history). Describe in pseudocode or SQL the pipeline logic that implements SCD Type 2 from this daily snapshot. How do you detect new, changed, and unchanged records?

---

**Q18 (Scenario)**  
You need to query: "How much revenue did Alice generate while she was in Sydney?" Write the JOIN between `fact_sales` and an SCD Type 2 `dim_customer` that produces the correct result.

---

**Q19 (Deep dive)**  
Compare SCD Type 1, Type 2, Type 3, and Type 4. For each, name one real-world scenario where it is the appropriate choice and one where it would fail.

---

**Q20 (Tricky)**  
What is the "no-change" problem in SCD Type 2 pipelines? If a daily snapshot delivers a customer row with the same attributes as yesterday, what happens if you don't detect and skip it? How do you detect unchanged rows efficiently?

---

## Concept 3: Normalisation, Normal Forms & Denormalisation Trade-offs

**Q21 (Warm-up)**  
What is the difference between 1NF, 2NF, and 3NF? Give one example violation for each.

---

**Q22 (Conceptual)**  
Why are OLTP databases typically designed in 3NF, but analytical databases deliberately violate it? What is the specific cost of 3NF for analytical queries?

---

**Q23 (Scenario)**  
A staging table has columns: `(order_id, product_id, product_name, category, quantity, store_id, store_city)`. The primary key is `(order_id, product_id)`. Identify all 2NF and 3NF violations and write the corrected normalised schema.

---

**Q24 (Conceptual)**  
What is a transitive dependency? Give an example from an e-commerce schema and identify which normal form it violates.

---

**Q25 (Tricky)**  
`dim_product` has columns `(product_id, product_name, category, subcategory, brand, brand_country)`. Is this table in 3NF? Identify the violation and explain whether you would normalise it or leave it — and why.

---

**Q26 (Scenario)**  
A data engineer argues: "Keep the Silver layer in 3NF and only denormalise in Gold." Another says: "3NF in the warehouse is over-engineering." Which is closer to correct and why?

---

**Q27 (Tricky)**  
What are the update anomaly, insertion anomaly, and deletion anomaly? Give one example of each from a denormalised `orders` table that embeds customer data directly.

---

**Q28 (Scenario)**  
A dimension table `dim_customer` has columns `(customer_id, city, state, country)`. There is a functional dependency `city → state`. Is this a 3NF violation? Should you fix it in a dimensional model? Justify your answer.

---

**Q29 (Deep dive)**  
In a Parquet-based data lake, a dimension table is fully denormalised — `dim_product` repeats `brand_country` for every product of that brand. In a row-oriented PostgreSQL table, the same denormalisation wastes significant storage. Why is the storage penalty much smaller in Parquet and what encoding feature makes this possible?

---

**Q30 (Senior-level)**  
A 500M-row `fact_sales` table needs a new `region` column derived from `customer_city` via a lookup table. How do you add this column without reprocessing all 500M rows? Which layer of the dimensional model do you update and why does the fact table not need to change?

---
