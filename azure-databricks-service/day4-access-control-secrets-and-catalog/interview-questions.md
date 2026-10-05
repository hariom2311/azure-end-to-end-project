# Day 4 — Interview Questions: Access Control, Secrets, Catalog & ADF Integration

> 30 questions covering secret scopes, Key Vault integration, Service Principal auth, external vs internal tables, external volumes, Unity Catalog, and ADF Databricks integration.
> Attempt first, then check `interview-solutions.md`.

---

## Secrets & Key Vault

**Q1 (Warm-up)**
What is a Databricks secret scope? What are the two types and which is recommended for production?

---

**Q2 (Conceptual)**
A data engineer writes:
```python
password = dbutils.secrets.get(scope="kv-scope", key="db-password")
print(password)
```
What does `print(password)` show in the notebook output? Does the variable `password` have the real value?

---

**Q3 (Scenario)**
A company rotates their Service Principal client secret every 90 days. They use an AKV-backed secret scope. After rotation, does the Databricks notebook need to be updated? Why?

---

**Q4 (Tricky)**
What is the URL format to create a secret scope in Databricks? Why can this page not be found through the normal workspace UI navigation?

---

**Q5 (Scenario)**
A data engineer creates a secret scope with `Manage Principal: All Users`. Six months later, the security team says this is a risk. What does `Manage Principal: All Users` mean, and what should it be changed to?

---

**Q6 (Conceptual)**
What is the difference between listing secrets and reading a secret?
```python
dbutils.secrets.list(scope="kv-scope")
dbutils.secrets.get(scope="kv-scope", key="sp-client-id")
```
What does each return?

---

## Service Principal & Storage Access

**Q7 (Warm-up)**
What is a Service Principal in Azure? Why does Databricks use one to access ADLS Gen2 instead of using a user account?

---

**Q8 (Conceptual)**
What is the `abfss://` URI scheme? Write the full URI for the `bronze` container in storage account `stadlsdev001`.

---

**Q9 (Scenario)**
A notebook configures ADLS access with `spark.conf.set(...)` using a Service Principal. It works on the dev cluster. When a job cluster is created for a scheduled run, the notebook fails with `AuthorizationPermissionMismatch`. Why?

---

**Q10 (Tricky)**
A team uses the storage account access key instead of a Service Principal to connect to ADLS. What are two security risks with this approach?

---

## Unity Catalog

**Q11 (Warm-up)**
What is the 3-level namespace in Unity Catalog? Write an example for a table called `payments` in the `bronze` schema of the `dev_catalog` catalog.

---

**Q12 (Conceptual)**
What is the difference between a **Storage Credential** and an **External Location** in Unity Catalog?

---

**Q13 (Scenario)**
An admin creates an external location pointing to `abfss://bronze@stadlsdev001.dfs.core.windows.net/`. A data engineer tries to create an external table at `abfss://silver@stadlsdev001.dfs.core.windows.net/`. It fails. Why?

---

**Q14 (Tricky)**
A workspace has Unity Catalog enabled. A data engineer runs:
```sql
CREATE TABLE my_table (id INT) USING DELTA
```
Without specifying a `LOCATION`. Where are the Delta files stored, and what type of table is created?

---

## External vs Internal Tables

**Q15 (Warm-up)**
What is the key difference between an internal (managed) table and an external table in Databricks?

---

**Q16 (Scenario)**
A data engineer runs `DROP TABLE dev_catalog.bronze.payments`. The table was created with a `LOCATION` clause pointing to ADLS. What happens to the files on ADLS?

---

**Q17 (Scenario)**
A data engineer runs `DROP TABLE dev_catalog.silver.summary`. The table was created WITHOUT a `LOCATION` clause. What happens to the data?

---

**Q18 (Tricky)**
How can you tell whether an existing table is internal or external without looking at the `CREATE TABLE` statement?

---

**Q19 (Conceptual)**
A table is created as external on ADLS. Another team writes new Delta files to the same ADLS path using ADF. After the ADF write, will the Databricks external table see the new data without any changes? Explain.

---

## External Volumes

**Q20 (Warm-up)**
What is a Unity Catalog external volume? How is the volume path accessed in a notebook?

---

**Q21 (Conceptual)**
What is the difference between an external table and an external volume? Give one use case for each.

---

**Q22 (Scenario)**
A data engineer runs:
```sql
SELECT * FROM dev_catalog.bronze.blob_files
```
`blob_files` is an external volume on Blob Storage containing CSV files. The query fails. Why, and how should the engineer read this data?

---

**Q23 (Tricky)**
An external volume is created at `abfss://files@stblobdev001.dfs.core.windows.net/`. A user runs `DROP VOLUME dev_catalog.bronze.blob_files`. What happens to the files in `stblobdev001`?

---

**Q24 (Conceptual)**
Can you write Delta format data to a volume path? Can you then query it as a Delta table?

---

## ADF & Databricks Integration

**Q25 (Warm-up)**
What is the ADF Databricks Notebook Activity? What does it require to connect to a Databricks workspace?

---

**Q26 (Scenario)**
An ADF pipeline has a Copy Activity followed by a Databricks Notebook Activity. The Copy Activity fails. What happens to the Notebook Activity?

---

**Q27 (Conceptual)**
ADF passes `{"env": "prod"}` as a base parameter to a Databricks Notebook Activity. How does the notebook read this value?

---

**Q28 (Tricky)**
A Databricks Notebook Activity in ADF is configured to use `New job cluster`. The same notebook runs in 4 minutes on the dev all-purpose cluster. The ADF job takes 9 minutes. What is likely causing the extra 5 minutes?

---

**Q29 (Scenario)**
A notebook called from ADF runs `dbutils.notebook.exit("PROCESSED: 5000 rows")`. Where can this value be seen in ADF?

---

**Q30 (System design)**
Design the complete access control setup for the VoltGrid project. Include: how Databricks reads secrets, how it accesses `stadlsdev001` for external tables, how it accesses `stblobdev001` for volumes, and how ADF triggers the Silver transform notebook. State what ADF uses to authenticate to Databricks.

---
