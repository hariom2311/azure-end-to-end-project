# Day 2 — Interview Questions: Data Storage, File Formats & the Lakehouse

> 55 questions across all 10 concepts. Read the question, attempt your own answer, then check `interview-solutions.md`.

---

## Concept 1: File Formats — CSV, JSON, Parquet, ORC, Avro

**Q1 (Warm-up)**  
What is the difference between row-oriented and column-oriented file formats? Give one example of each and explain which is better for analytical queries.

---

**Q2 (Warm-up)**  
Why is Parquet the dominant file format in modern data lakes? List at least four reasons.

---

**Q3 (Conceptual)**  
What is predicate pushdown in the context of Parquet files? How does Parquet's internal structure enable it?

---

**Q4 (Scenario)**  
A data engineer stores 10 years of clickstream data (10 TB) as a single large Gzip-compressed CSV file. An analyst needs to run a query that filters by `event_date = '2024-01-15'` and reads only three columns. What are all the problems with this approach, and how would you fix it?

---

**Q5 (Tricky)**  
Parquet uses dictionary encoding internally. What is dictionary encoding and which types of columns benefit most from it? Give a concrete example from an e-commerce dataset.

---

## Concept 2: Change Data Capture (CDC)

**Q6 (Warm-up)**  
What is Change Data Capture (CDC) and what problem does it solve that a timestamp-based incremental load cannot?

---

**Q7 (Conceptual)**  
How does log-based CDC work? What is a Write-Ahead Log (WAL) and why does reading it have near-zero impact on the source database?

---

**Q8 (Scenario)**  
A team uses a daily full-load pipeline to sync a 200M-row `transactions` table from MySQL to Snowflake. The sync takes 6 hours and misses hard DELETEs. Propose a CDC-based replacement architecture. What tool would you use, what is the target latency, and how do you handle the initial load?

---

**Q9 (Tricky)**  
A CDC pipeline captures an `UPDATE` event and delivers it to the Silver table. Due to a network issue the event is delivered twice. The Silver MERGE runs twice on the same event. What must be true of the MERGE statement to ensure no data corruption? Write the MERGE in SQL.

---

## Concept 3: Data Warehouses vs. Data Lakes vs. Lakehouses

**Q10 (Warm-up)**  
What is a data warehouse? What are its two main limitations that led to the invention of data lakes?

---

**Q11 (Warm-up)**  
What is a "data swamp"? What specific data lake properties (or lack thereof) cause it?

---

**Q12 (Conceptual)**  
What is a lakehouse architecture? What does it add on top of a plain data lake that makes it more like a data warehouse?

---

**Q13 (Scenario)**  
A startup is choosing between three architectures for their analytics platform:
- Option A: PostgreSQL (their operational database) directly queried by BI tools
- Option B: Snowflake data warehouse fed by nightly ETL from PostgreSQL
- Option C: Lakehouse (Parquet on S3 + Delta Lake) fed by streaming CDC from PostgreSQL

They have: 10 GB of data today, 20 engineers who write to the operational DB, and 3 analysts. They expect to grow to 10 TB in 3 years. Which would you recommend and why? What would cause you to change your recommendation?

---

**Q14 (Tricky)**  
Explain the compute-storage separation principle of a lakehouse. Why does this matter for cost compared to a traditional data warehouse?

---

## Concept 4: Open Table Formats — Delta Lake, Iceberg, Hudi

**Q15 (Warm-up)**  
What problem does an open table format (Delta Lake, Iceberg, Hudi) solve that plain Parquet files cannot?

---

**Q16 (Conceptual)**  
How does Delta Lake's transaction log work? What is stored in each commit entry?

---

**Q17 (Scenario)**  
A data engineer runs `DELETE FROM orders WHERE customer_id = 'C9999'` on a Delta Lake table for a GDPR deletion request. The table has 500 GB of Parquet files. What actually happens under the hood? Does this immediately free disk space?

---

**Q18 (Scenario)**  
A Gold table is simultaneously written by two Spark jobs — Job A adds January data and Job B adds February data. Both read the current table state, both add new Parquet files, and both update the metadata. What could go wrong without a table format? How does Delta Lake's optimistic concurrency control prevent corruption?

---

**Q19 (Deep dive)**  
Compare Delta Lake and Apache Iceberg on three dimensions: schema evolution, multi-engine support, and hidden partitioning. When would you choose Iceberg over Delta Lake?

---

**Q20 (Tricky)**  
What is time travel in Delta Lake? Give two real-world scenarios where time travel is essential. What happens to old data files after you run `VACUUM`?

---

## Concept 5: Database Indexing & Query Optimisation

**Q21 (Warm-up)**  
What is a database index? How does a B-tree index work and what types of queries does it accelerate?

---

**Q22 (Conceptual)**  
What is a covering index? How does it eliminate heap access and what are its trade-offs compared to a regular index?

---

**Q23 (Scenario)**  
A PostgreSQL table `orders` has 80 million rows. A query `SELECT * FROM orders WHERE status = 'pending'` uses an index on `status` but the query planner ignores it and does a Seq Scan instead. Why might this happen? How would you fix it with a partial index?

---

**Q24 (Tricky)**  
What is the difference between a composite index `(a, b, c)` and three separate indexes on `(a)`, `(b)`, `(c)`? For which queries does order matter in a composite index?

---

**Q25 (System design)**  
You are indexing a 500M-row `events` table. Queries filter by `user_id` (10M distinct values) and `event_date` (daily, 3 years). Nightly bulk loads insert 2M rows. Design a complete indexing strategy: which indexes to create, in what order composite columns should be listed, and how to handle the bulk load performance impact.

---

## Concept 6: Data Catalog & Metadata Management

**Q26 (Warm-up)**  
What is a data catalog? What are the three types of metadata it stores?

---

**Q27 (Conceptual)**  
What is the difference between a Hive Metastore, the AWS Glue Data Catalog, and Databricks Unity Catalog? When would you use each?

---

**Q28 (Scenario)**  
An analyst emails you: "I found three tables named `orders`, `orders_v2`, and `orders_final` in the data lake. Which one is the source of truth?" This is a symptom of a broken data catalog. What governance processes and tooling would prevent this situation?

---

**Q29 (Deep dive)**  
What is the difference between technical metadata, business metadata, and operational metadata? Give two examples of each. Which type is hardest to maintain and why?

---

**Q30 (Tricky)**  
How does dbt contribute to data cataloging? What metadata does it automatically generate and what does it not capture?

---

## Concept 7: Storage Layout Patterns — Hot, Warm, Cold & Lifecycle

**Q31 (Warm-up)**  
What are the three storage tiers (hot, warm, cold) in a data platform context? What query latency and cost trade-offs does each represent?

---

**Q32 (Conceptual)**  
What is file compaction and why is it necessary for tables that receive frequent small writes (e.g., hourly streaming ingestion)?

---

**Q33 (Scenario)**  
A Silver table receives 1,000 new rows every 5 minutes from a streaming job, each written as a separate Parquet file. After 30 days, the table has 8,640 files. Describe the performance impact and design a compaction strategy.

---

**Q34 (Tricky)**  
OPTIMIZE in Delta Lake rewrites files but does not delete the old ones. What command do you run to actually free disk space, and what is the safe minimum retention period? Why does Delta Lake not delete files immediately?

---

## Concept 8: OLTP vs. OLAP Storage Systems

**Q35 (Warm-up)**  
What do OLTP and OLAP stand for and what is the fundamental difference in their query patterns?

---

**Q36 (Conceptual)**  
Why can't you simply run analytical queries (GROUP BY, SUM over millions of rows) directly on your PostgreSQL production database? What are the risks beyond performance?

---

**Q37 (Scenario)**  
A company runs daily sales reports on their MySQL production database. On the last day of the month, the report query locks the `orders` table for 15 minutes, causing the website to time out. Design a solution that eliminates this problem without rewriting the report query.

---

**Q38 (Tricky)**  
What is HTAP (Hybrid Transactional/Analytical Processing)? Name two databases that claim HTAP capabilities. Is HTAP a replacement for having separate OLTP and OLAP systems? When does it make sense?

---

## Concept 9: Data Replication & Consistency Models

**Q39 (Warm-up)**  
What is replication lag and why does it matter for data pipelines that read from a read replica?

---

**Q40 (Conceptual)**  
Explain the CAP theorem. Why can you not have all three properties simultaneously? Give a real-world example of a CP system and an AP system used in data engineering.

---

**Q41 (Scenario)**  
Your pipeline reads from a PostgreSQL read replica with a 30-second average lag. The incremental watermark query is `WHERE updated_at > :last_run`. A record is updated on the primary at 14:00:00. Your pipeline runs at 14:00:10. Is the update captured? What is the fix?

---

**Q42 (Deep dive)**  
What is the difference between ACID and BASE? Where does each model appear in a typical data platform (source database, message queue, data lake)?

---

**Q43 (Tricky)**  
S3 was historically eventually consistent — a LIST call after a PUT could miss the newly written object. Since December 2020 S3 is strongly consistent. Why does this matter for Delta Lake and Iceberg, and how did these table formats work around eventual consistency before the change?

---

## Concept 10: Storage Performance Optimisation

**Q44 (Warm-up)**  
What is Z-ordering in Delta Lake? How does it improve query performance compared to standard partitioning?

---

**Q45 (Conceptual)**  
What is a bloom filter in the context of a data file? What type of query does it accelerate and what are its limitations?

---

**Q46 (Scenario)**  
A table is partitioned by `order_date` (daily). An analyst frequently runs queries like `WHERE customer_id = 'C001'`. The query still scans all partitions even though most partitions have no data for `C001`. What optimisations would you apply?

---

**Q47 (Deep dive)**  
Explain Parquet's internal structure: row groups, column chunks, pages, and footer. How does each level contribute to read performance for a filtered analytical query?

---

**Q48 (Tricky)**  
A Spark job reads a 1 TB Parquet table and runs in 45 minutes. After partitioning by `event_date`, the same query on one day's data (≈3 GB) takes 8 minutes. That is slower per GB than expected. What might be causing this and how would you diagnose it?

---

## Mixed / Senior-Level Questions

**Q49**  
A colleague says: "We should always use Parquet for everything — CSV and JSON are legacy formats." Do you agree? When would you specifically choose CSV or JSON over Parquet?

---

**Q50**  
Explain the "schema-on-read vs. schema-on-write" trade-off in the context of a data lake that is 2 years old and has accumulated hundreds of tables. What technical debt arises from schema-on-read and how do you remediate it?

---

**Q51**  
Your company is migrating from a legacy on-premise Hive data warehouse to a cloud lakehouse. The Hive data is stored as ORC files on HDFS. What is your migration strategy? What format do you convert to and how do you handle the 5 years of historical data?

---

**Q52**  
A data lake stores raw JSON events from a mobile app. Over time, the app has deployed 12 different schema versions — some fields were added, some removed, some renamed. The raw JSON files contain a mixture of all 12 schema versions. How do you handle this in the Silver layer transformation?

---

**Q53**  
You are designing a real-time analytics system for a ride-sharing platform. Trips are created, updated (status changes), and completed within 30–60 minutes. The analytics team needs sub-second query latency on metrics like "active trips right now" and "revenue in the last hour." Design the storage architecture.

---

**Q54**  
A GDPR deletion request arrives for a user. Their data exists in: raw JSON files (Bronze), Parquet Silver tables (Delta Lake), Gold aggregate tables, an ML feature store (Parquet), and a search index (Elasticsearch). Walk through the deletion process for each system, noting which is hardest and why.

---

**Q55**  
Compare the total cost of ownership (storage + compute + operations) between: (A) a traditional data warehouse (Snowflake), (B) a pure data lake (Parquet on S3 with Spark), and (C) a lakehouse (Delta Lake on S3 with Databricks). Under what conditions does each win on cost?
