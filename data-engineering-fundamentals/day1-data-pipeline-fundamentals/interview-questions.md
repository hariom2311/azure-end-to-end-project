# Day 1 — Interview Questions: Data Pipelines & ETL/ELT

> These questions are drawn from real data engineering interviews at product companies, consultancies, and FAANG-adjacent orgs. They are grouped by concept and difficulty. Answers are in `interview-solutions.md`.

---

## Concept 1: What is a Data Pipeline

**Q1 (Warm-up)**  
What is a data pipeline? Describe its three core components and give a real-world example of each.

---

**Q2 (Conceptual)**  
What does it mean for a pipeline to be represented as a DAG? Why must data pipelines be acyclic?

---

**Q3 (Scenario)**  
You are designing a pipeline that pulls data from three sources: a PostgreSQL transactions database, a daily CSV file from the finance team, and a third-party REST API that returns customer profile data. The downstream target is a data warehouse.

Draw the pipeline as a DAG (describe it in text if needed). Identify where fan-in occurs and what happens if the API is unavailable.

---

**Q4 (Tricky)**  
A colleague says: "Our pipeline is just a Python script that runs in a cron job — it's not really a DAG." Is this accurate? What properties would the script need to have before you'd consider it a proper DAG-based pipeline?

---

## Concept 2: ETL vs. ELT

**Q5 (Warm-up)**  
What is the difference between ETL and ELT? Which is more common in modern cloud architectures and why?

---

**Q6 (Conceptual)**  
A company is migrating from an on-premise Oracle data warehouse to Snowflake. Their current ETL pipeline transforms data using an Informatica tool before loading into Oracle. Should they keep the ETL approach or switch to ELT? What factors would influence this decision?

---

**Q7 (Scenario)**  
Your team stores raw customer data including email addresses and credit card last-four-digits in a Bronze/landing zone in your data lake as part of an ELT pipeline. A security audit flags this as a risk. How would you redesign the ingestion layer to handle PII correctly while preserving the ELT paradigm?

---

**Q8 (Tricky)**  
Explain the medallion architecture (Bronze / Silver / Gold). How does it relate to ELT? What are the trade-offs of this approach compared to a single-pass transformation?

---

**Q9 (System design)**  
Design a pipeline for an e-commerce company that receives 10 million order events per day from 5 different regional databases. Orders can be updated for up to 48 hours after creation. The analytics team needs a single `orders` table that reflects the current state of all orders. Which pattern (ETL or ELT) would you choose, and why? What are the challenges?

---

## Concept 3: Idempotency and Incremental vs. Full Loads

**Q10 (Warm-up)**  
What does idempotency mean in the context of a data pipeline? Why is it important?

---

**Q11 (Warm-up)**  
What is the difference between a full load and an incremental load? When would you choose each?

---

**Q12 (Scenario)**  
A pipeline job runs every hour to load new orders into a Silver table using `INSERT INTO orders_silver SELECT * FROM orders_raw WHERE created_at > :last_run_time`. The job failed at 2:45 AM and was automatically retried at 3:00 AM. The next morning, analysts report duplicate orders in the Silver table. What went wrong? How would you fix it?

---

**Q13 (Deep dive)**  
You are building an incremental pipeline using `updated_at` timestamps as a watermark. A source system has the following issue: when a record is updated, the `updated_at` timestamp is set by the application server, not the database. During high load, some updates arrive with timestamps up to 30 minutes late (the application server clock was lagged). How does this affect your watermark strategy? What are your options?

---

**Q14 (Tricky)**  
What is the difference between idempotency and exactly-once delivery? Can you have one without the other? Give an example.

---

**Q15 (System design)**  
You need to build a pipeline that incrementally syncs a 500-million-row `events` table from MySQL to BigQuery. The table has a monotonically increasing `event_id` but no `updated_at` column. Events are never updated, only inserted. Design the full sync approach including: initial load strategy, incremental approach, watermark storage, and failure recovery.

---

## Concept 4: Data Lineage and Dependency Graphs

**Q16 (Warm-up)**  
What is data lineage and why does it matter? Give two concrete scenarios where lineage is essential.

---

**Q17 (Conceptual)**  
What is the difference between table-level lineage and column-level lineage? Which is more useful and in what situations?

---

**Q18 (Scenario)**  
The analytics team reports that the `total_revenue` metric in the monthly report is wrong. The metric is in a Gold table called `revenue_summary`. You have the following lineage:

```
raw.orders → silver.orders → gold.order_metrics → gold.revenue_summary → monthly_report
raw.returns → silver.returns → gold.order_metrics
```

Walk through your debugging process using the lineage graph. What would you check at each node?

---

**Q19 (Scenario)**  
A GDPR deletion request arrives for user `ID=7291`. The user wants all their data deleted within 72 hours. Your lineage graph shows their data exists in: `raw.users`, `silver.users`, `gold.customer_ltv`, `gold.segment_profiles`, and a `churn_prediction` ML feature store. What is your deletion strategy? What challenges might arise with aggregated tables like `gold.customer_ltv`?

---

**Q20 (System design)**  
Your data platform has 200 pipelines and 500 tables. A source team announces a schema change — they are splitting the `full_name` column in `raw.customers` into `first_name` and `last_name`. How would you use lineage to assess the impact of this change and plan the rollout?

---

## Concept 5: Pipeline Failure Modes and Retry Strategies

**Q21 (Warm-up)**  
What are the three delivery guarantees (at-most-once, at-least-once, exactly-once)? Which is the hardest to achieve and why?

---

**Q22 (Warm-up)**  
What is a dead-letter queue? When and why would you use one?

---

**Q23 (Scenario)**  
A pipeline calls a third-party weather API to enrich order data with the weather at the time of purchase. The API has a rate limit of 60 requests per minute and occasionally returns 500 errors. Design a retry strategy that:
- Respects the rate limit
- Handles transient 500 errors
- Does not block the pipeline indefinitely
- Surfaces errors to the team

---

**Q24 (Deep dive)**  
Explain the write-audit-publish pattern. In what scenarios is it preferable to a direct write? What are its trade-offs?

---

**Q25 (Tricky)**  
A pipeline writes data using `INSERT INTO target SELECT * FROM staging`. Due to a bug, it ran twice in quick succession. The target table now has duplicates. What is wrong with this design? Rewrite the write step to be idempotent using SQL. What additional mechanism do you need to guarantee no duplicates even if the pipeline crashes mid-write?

---

**Q26 (System design)**  
You are building a financial data pipeline that processes daily bank transactions. Requirements:
- No transaction can be lost (regulatory requirement)
- No transaction can be counted twice (affects P&L reporting)
- The pipeline must recover automatically from failures
- The team must be alerted within 5 minutes of a failure

Design the end-to-end pipeline including: delivery guarantee target, failure detection mechanism, retry strategy, dead-letter handling, and alerting approach.

---

## Mixed / Senior-Level Questions

**Q27**  
A pipeline has been running for 6 months. The source team tells you they changed a column from `VARCHAR(50)` to `VARCHAR(255)` — a "backwards compatible" change. Do you need to worry about this? What could go wrong?

---

**Q28**  
What is the difference between a pipeline **bug** and a pipeline **data quality issue**? Give an example of each. How would your response differ?

---

**Q29**  
Your pipeline runs in 20 minutes on a typical day, but on the last day of the month it runs in 4 hours because the source table has 30× more rows. How do you design for this? What are your options?

---

**Q30**  
Describe a time (real or hypothetical) when you would intentionally choose a non-idempotent pipeline design. What trade-offs are you accepting?
