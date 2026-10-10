# Day 8 — Interview Questions: Delta Lake Deep Dive

> 25 questions covering Delta Lake internals, ACID operations, time travel, OPTIMIZE, VACUUM, schema evolution, cloning, and table properties.
> Attempt first, then check `interview-solutions.md`.

---

## Delta Lake Fundamentals & Transaction Log

**Q1 (Warm-up)**
What is Delta Lake? How is it different from storing plain Parquet files in ADLS?

---

**Q2 (Conceptual)**
What is the `_delta_log/` directory? What does each JSON file inside it record?

---

**Q3 (Conceptual)**
What are the four ACID properties? Give one real example of each in the context of Delta Lake.

---

**Q4 (Tricky)**
A data engineer says: "Delta Lake is just Parquet with a log folder." What is wrong with this statement? What does the transaction log actually enable?

---

**Q5 (Scenario)**
Two Spark jobs run simultaneously: Job A is reading a Delta table; Job B is writing new rows to the same table. What happens to Job A's read? Will it see the data written by Job B mid-read?

---

## DESCRIBE HISTORY, UPDATE, DELETE

**Q6 (Warm-up)**
What does `DESCRIBE HISTORY` return? Name three columns in the output and explain what each means.

---

**Q7 (Conceptual)**
When you run `UPDATE dev_catalog.bronze.sample_people SET amount = 100 WHERE id = 1`, does Delta Lake modify the existing Parquet file in place? Explain what actually happens on disk.

---

**Q8 (Scenario)**
A data engineer runs `DELETE FROM orders WHERE status = 'test'` and then realizes the data is needed for a compliance audit. The table has 5 versions. How do you recover the deleted rows without restoring the entire table?

---

**Q9 (Tricky)**
Every UPDATE and DELETE adds new Parquet files but keeps old ones. Over time, what problem does this create? What command fixes it?

---

## MERGE (Upsert)

**Q10 (Warm-up)**
What is the MERGE statement used for? Explain the WHEN MATCHED and WHEN NOT MATCHED clauses.

---

**Q11 (Conceptual)**
Write a MERGE that:
- Updates `amount` if `id` matches and `amount` differs
- Inserts the row if no match
- Deletes the target row if `id` matches and the source has `status = 'deleted'`

Use table: `target`, source view: `source`, join key: `id`.

---

**Q12 (Scenario)**
A MERGE runs every hour to keep a customer table up to date from a CDC (change data capture) feed. After 6 months, the table has 50 versions and hundreds of small Parquet files. Queries have slowed down. What is the root cause and what two commands resolve it?

---

**Q13 (Tricky)**
Can a MERGE statement produce more than one matched action for the same row? For example, can the same row match both `WHEN MATCHED THEN UPDATE` and `WHEN MATCHED THEN DELETE`? What error does Delta Lake throw?

---

## Time Travel

**Q14 (Warm-up)**
What is Delta Lake time travel? Write the SQL to read `dev_catalog.bronze.orders` as it was at version 3.

---

**Q15 (Conceptual)**
What is the difference between `VERSION AS OF` and `TIMESTAMP AS OF` for time travel? When would you use each?

---

**Q16 (Scenario)**
An analyst accidently runs: `DELETE FROM dev_catalog.bronze.sample_people` (no WHERE clause — deletes all rows). How do you recover all the data? Write the exact SQL.

---

**Q17 (Tricky)**
A data engineer time-travels to version 2 and reads the data. Then they run VACUUM and delete all files older than 7 hours (using `RETAIN 0 HOURS` for testing). Can they still time-travel to version 2 after VACUUM? Explain.

---

## OPTIMIZE and Z-ORDER

**Q18 (Warm-up)**
What does `OPTIMIZE` do? Why does Delta Lake produce many small files over time without it?

---

**Q19 (Conceptual)**
What is Z-ORDER? How does `ZORDER BY (status)` help query performance when you run `SELECT * FROM orders WHERE status = 'pending'`?

---

**Q20 (Scenario)**
A table has 500 small Parquet files. An analyst runs `OPTIMIZE orders ZORDER BY (region, status)`. After OPTIMIZE, the analyst runs `SELECT * FROM orders WHERE region = 'East'`. What performance improvement can they expect, and why?

---

## VACUUM, Schema Evolution, Table Properties, Clone

**Q21 (Warm-up)**
What does `VACUUM` do? What is the default retention period, and why does that number exist?

---

**Q22 (Conceptual)**
What is the difference between `mergeSchema` and schema enforcement in Delta Lake?

---

**Q23 (Scenario)**
A new data source starts sending an additional column `promo_code` that did not exist in the existing Delta table. If you try to append this data without any option, what error appears? How do you fix it?

---

**Q24 (Conceptual)**
What is the difference between `SHALLOW CLONE` and `DEEP CLONE`? Give a use case for each.

---

**Q25 (System design)**
Design a complete Delta Lake operations strategy for a production orders table with the following requirements:
- Receives ~50,000 new rows per day via MERGE (from a CDC feed)
- Must support audit queries going back 90 days using time travel
- Queries filter primarily by `region` and `order_date`
- Must be storage-efficient

Describe: which table properties to set, OPTIMIZE schedule, Z-ORDER columns, VACUUM retention, and how to handle the 90-day time travel requirement with VACUUM.

---
