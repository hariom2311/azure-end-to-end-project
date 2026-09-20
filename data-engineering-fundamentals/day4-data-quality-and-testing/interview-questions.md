# Day 4 — Interview Questions: Data Quality & Testing

> 40 questions across all 5 concepts. Attempt your own answer first, then check `interview-solutions.md`.

---

## Concept 1: Dimensions of Data Quality

**Q1 (Warm-up)**  
Name the six dimensions of data quality and give one example check for each from a transactions dataset.

---

**Q2 (Warm-up)**  
What is the difference between data validity and data accuracy? Give a concrete example where a value passes validity checks but fails accuracy.

---

**Q3 (Conceptual)**  
How would you measure the completeness of a column? What completeness threshold would you set for a primary key column vs. an optional notes column?

---

**Q4 (Scenario)**  
A `customer_id` column in a Silver table has 5% null values. The source system sends NULL when a purchase is made as a guest (no account). Is this a data quality problem? How should it be handled?

---

**Q5 (Scenario)**  
A pipeline loads 1 million rows per day. On Tuesday it loads 950,000 rows. On Wednesday it loads 1,050,000. On Thursday it loads 200,000. Which day is a quality problem, and which might be a business event? How do you tell the difference automatically?

---

**Q6 (Tricky)**  
What is data consistency and how is it different from validity? Give an example of a consistency violation that would pass all column-level validity checks.

---

**Q7 (Deep dive)**  
A financial dataset shows `total_revenue = $1,234,567` in the transactions table and `total_revenue = $1,234,432` in the settlements table for the same period. Is this a data quality issue, a timing issue, or a business process issue? How do you investigate?

---

## Concept 2: SQL Validation Patterns

**Q8 (Warm-up)**  
What is the "assertion pattern" for data quality checks? Why does a good quality check return zero rows when data is clean?

---

**Q9 (Conceptual)**  
What is the Write-Audit-Publish (WAP) pattern? What problem does it solve compared to writing directly to the production table and checking quality afterwards?

---

**Q10 (Scenario)**  
A quality check finds that 2% of rows have a null `store_id`. You have two options: (A) fail the pipeline and alert on-call, (B) load the 98% of good rows and quarantine the 2%. Which do you choose and what factors determine your decision?

---

**Q11 (Scenario)**  
Write a SQL query that detects duplicate rows in a `transactions` table where the natural key is `(transaction_id, transaction_date)`.

---

**Q12 (Conceptual)**  
What is a golden dataset test? Why is it more valuable for a data pipeline than a unit test on a single transformation function?

---

**Q13 (Tricky)**  
A pipeline deduplicates on `transaction_id` by keeping the latest loaded row (`ORDER BY loaded_at DESC`). A late-arriving correction comes in with the same `transaction_id` but a corrected `amount`. Two hours later, a consumer queries the Silver table. Is the corrected amount reflected? What mechanism ensures this?

---

**Q14 (Scenario)**  
Write the SQL for a row count reconciliation check that alerts if the Silver table is missing more than 1% of the rows that were in staging for a given load date.

---

## Concept 3: Pipeline Testing Strategies

**Q15 (Warm-up)**  
What are the three levels of the data pipeline testing pyramid? Which is fastest to run and which gives the most confidence?

---

**Q16 (Conceptual)**  
What should a unit test for a data transformation function cover? Give three examples of test cases for a function that converts amounts from multiple currencies to AUD.

---

**Q17 (Scenario)**  
A data engineer says: "I don't write unit tests for SQL transformations because I trust the database engine." Do you agree? What should they be testing instead?

---

**Q18 (Conceptual)**  
What is the difference between mocking a database connection in a unit test vs. using a real (test) database in an integration test? When would you use each?

---

**Q19 (Scenario)**  
A pipeline has 50 dbt models. A developer changes the join logic in `stg_orders.sql`. Which models are at risk of breaking? How would you test only the affected models without running the full pipeline?

---

**Q20 (Tricky)**  
What is a "test for absence of data" and why is it harder to write than a test for presence of expected values? Give an example where absence of data is the critical quality signal.

---

**Q21 (Deep dive)**  
You have a Silver table that is loaded incrementally every hour. After each load, you want to verify that: (1) no nulls were introduced in key columns, (2) no duplicate primary keys exist, (3) row count is within 20% of the previous hour's load. Write pseudocode for the post-load validation function that runs after every incremental load.

---

## Concept 4: Anomaly Detection & Statistical Quality Checks

**Q22 (Warm-up)**  
What is a Z-score and how is it used to detect outliers in a numeric column? What Z-score threshold is commonly used and what does it mean statistically?

---

**Q23 (Conceptual)**  
What is the difference between a rule-based quality check and a statistical anomaly check? Give an example of an anomaly that rule-based checks would miss but a statistical check would catch.

---

**Q24 (Scenario)**  
Yesterday's `orders` table loaded 485,000 rows. The 30-day average is 500,000 rows with a standard deviation of 15,000. Is yesterday's load anomalous? Show the calculation.

---

**Q25 (Scenario)**  
A pipeline runs a Z-score check on `order_amount` and flags any transaction with `|z| > 3`. A legitimate bulk corporate order of $500,000 is flagged as an outlier. How do you handle this? What is the risk of auto-rejecting high-Z-score rows?

---

**Q26 (Tricky)**  
What is distribution drift detection and when would you use it over a simple row count check? Give an example where row counts look normal but distribution has shifted in a business-significant way.

---

**Q27 (Deep dive)**  
Design an anomaly detection system for a daily sales pipeline that covers: (1) volume anomalies, (2) value outliers, (3) categorical distribution drift. What thresholds would you set and how would you tune them over time?

---

## Concept 5: Data Contracts & Schema Enforcement

**Q28 (Warm-up)**  
What is a data contract and what problem does it solve in a large organisation with many teams writing and reading shared datasets?

---

**Q29 (Conceptual)**  
What is the difference between a breaking and a non-breaking schema change? Give two examples of each.

---

**Q30 (Scenario)**  
A producer team renames column `customer_city` to `city` in the Silver table. Three downstream consumers break silently — their queries return NULL for the `city` column but do not error. How could a data contract have prevented this? What enforcement mechanism would have caught the break before production?

---

**Q31 (Scenario)**  
A data contract specifies that `amount` must be > 0. A new business requirement introduces refunds, which have negative amounts. How do you evolve the contract? Who must be notified and what is the migration process?

---

**Q32 (Tricky)**  
A data contract has `version: "2.1.0"`. A consumer is pinned to `version: "1.0.0"`. A new pipeline loads data conforming to v2.1.0. Can the v1.0.0 consumer still read the data? What versioning strategy (SemVer vs. date-based vs. append-only) prevents this breaking?

---

**Q33 (Deep dive)**  
What does Great Expectations' `mostly` parameter do? When would you use `mostly=0.99` instead of `mostly=1.0` for a `not_null` expectation, and what risk does a `mostly` value introduce?

---

## Mixed / Senior-Level Questions

**Q34**  
A data engineer says "we test with production data in a staging environment." What are two risks of this approach? What is the standard alternative?

---

**Q35**  
Your Silver table passes all column-level quality checks (`not_null`, `unique`, enum validation) but the Gold revenue report is 12% lower than expected. What category of quality issue is this, and list three specific root causes that column-level checks would not catch.

---

**Q36**  
How would you design a data quality monitoring system that alerts the right team (DE, analytics, or the source system owner) depending on the type of quality failure? Give the routing logic.

---

**Q37**  
A pipeline has been running for 2 years without quality checks. You are asked to add quality checks without disrupting the existing pipeline. Describe your approach: where do you start, how do you baseline current quality, and how do you introduce checks without triggering immediate pipeline failures?

---

**Q38**  
What is the difference between "quality at rest" (validating data in a table) and "quality in motion" (validating data as it streams through a pipeline)? What tools and patterns support each?

---

**Q39**  
A `discount_pct` column is 0 for 95% of rows and a non-zero value for 5%. A new intern writes a quality check `assert discount_pct IS NOT NULL`. All rows pass. Is this check useful? What better check would you write?

---

**Q40**  
Design a data quality dashboard for a senior stakeholder (non-technical). What metrics would you show, at what granularity (table-level, column-level, pipeline-level), and how would you communicate a quality breach that happened 3 days ago and is now fixed?

---
