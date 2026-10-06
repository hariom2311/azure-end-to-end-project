# Day 5 — Interview Questions: Unity Catalog Tables and Volumes

> 25 questions covering internal vs external tables, external volumes, internal volumes, the LOCATION clause, DROP behaviour, and the Medallion landing pattern.
> Attempt first, then check `interview-solutions.md`.

---

## Internal vs External — Concepts

**Q1 (Warm-up)**
What is the key difference between an internal (managed) table and an external table in Unity Catalog? What determines which type is created?

---

**Q2 (Conceptual)**
A data engineer runs:
```sql
DROP TABLE dev_catalog.bronze.payments
```
The table was created with a `LOCATION` clause pointing to ADLS. What happens to:
- The table entry in Unity Catalog?
- The Delta files on ADLS?

---

**Q3 (Scenario)**
A data engineer runs `DROP TABLE dev_catalog.silver.summary`. The table was created WITHOUT a `LOCATION` clause. What happens to the data? What should the engineer have done to prevent data loss?

---

**Q4 (Tricky)**
How can you tell whether an existing table is internal or external without looking at the original `CREATE TABLE` statement?

---

**Q5 (Conceptual)**
A table is created as external at `abfss://bronze@stadlsdev001.dfs.core.windows.net/orders/`. Another team uses ADF to write new Delta files to the same ADLS path. Will the Databricks external table see the new data automatically? Explain why.

---

**Q6 (Scenario)**
A data engineer creates a table like this:
```sql
CREATE TABLE dev_catalog.silver.results (
    id INT,
    score DOUBLE
)
USING DELTA
```
Six months later, a colleague runs `DROP TABLE dev_catalog.silver.results`. Is the data recoverable? Why?

---

**Q7 (Tricky)**
What is stored in the `_delta_log/` folder? Why does a Delta external table need it?

---

## External Tables — Hands-On

**Q8 (Warm-up)**
Write the SQL to create an external Delta table called `transactions` in `dev_catalog.bronze` pointing to the path `abfss://bronze@stadlsdev001.dfs.core.windows.net/transactions/`.

---

**Q9 (Scenario)**
A data engineer registers an external table and queries it. It returns zero rows. The ADLS path exists and has files. What are two possible reasons?

---

**Q10 (Conceptual)**
What is `DESCRIBE EXTENDED` used for in the context of Unity Catalog tables? Which two fields are most important when checking if a table is internal or external?

---

**Q11 (Scenario)**
An external table is dropped. The engineer needs to re-register it. Do they need to re-write the data? What SQL do they need to run?

---

## External Volumes

**Q12 (Warm-up)**
What is a Unity Catalog external volume? How is it different from an external table?

---

**Q13 (Conceptual)**
A volume is created at `abfss://files@stblobdev001.dfs.core.windows.net/`. How is it accessed in a notebook? Write the Python code to list files in it.

---

**Q14 (Scenario)**
A data engineer runs:
```sql
SELECT * FROM dev_catalog.bronze.blob_files
```
`blob_files` is a volume. The query fails. Why? How should the engineer read data from the volume?

---

**Q15 (Tricky)**
Can you write Delta format data to a volume path? If yes, how would you then query it as a SQL table?

---

**Q16 (Scenario)**
A user runs `DROP VOLUME dev_catalog.bronze.blob_files`. The volume pointed to `abfss://files@stblobdev001.dfs.core.windows.net/`. What happens to the files in `stblobdev001`?

---

**Q17 (Conceptual)**
What is the `/Volumes/` path prefix in Databricks? Is it a real filesystem path? Does it work the same on all clusters in the workspace?

---

## Internal Volumes

**Q18 (Warm-up)**
What is the difference between an internal volume and an external volume? How do you create each?

---

**Q19 (Scenario)**
A data engineer creates:
```sql
CREATE VOLUME dev_catalog.silver.temp_files
```
Where are the files stored? What happens when `DROP VOLUME dev_catalog.silver.temp_files` is run?

---

**Q20 (Tricky)**
When would you choose an internal volume over an external volume? Give a concrete example.

---

## Decision Making

**Q21 (Conceptual)**
Fill in the best object type for each use case:

| Use case | Best object |
|---|---|
| Raw CSV files dropped by ADF into Blob Storage | ? |
| Cleaned salary data that analysts query with SQL | ? |
| Temp staging area that only notebooks use | ? |
| Delta table on ADLS shared with Synapse Analytics | ? |

---

**Q22 (Scenario)**
A team stores transaction data in a Delta table inside Unity Catalog. They realise they should have made it external (pointing to ADLS) from the start. The data is in a managed table. How do they migrate without data loss?

---

**Q23 (Conceptual)**
What is the Medallion landing pattern using volumes and external tables? Describe the flow from raw files to a queryable SQL table.

---

**Q24 (Tricky)**
A data engineer creates an external table with a path that is NOT covered by any external location. The `CREATE TABLE` statement succeeds but all queries fail with an access error. Why?

---

**Q25 (System design)**
Design the table and volume strategy for a data pipeline that:
- Receives raw JSON files daily from an API (via ADF)
- Processes them into a clean Delta table for BI reporting
- Archives the raw JSON files for 90 days

State what type of object (internal table, external table, internal volume, external volume) you would use for each layer, and why.

---
