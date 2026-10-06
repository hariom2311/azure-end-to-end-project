# Day 6 — Interview Questions: Unity Catalog, Tables, Volumes & ADF

> 30 questions covering Storage Credentials, External Locations, internal vs external tables, internal vs external volumes, Unity Catalog hierarchy, ADF Notebook Activity, Linked Services, and cluster access modes.
> Attempt first, then check `interview-solutions.md`.

---

## Storage Credentials & External Locations

**Q1 (Warm-up)**
What is a Storage Credential in Unity Catalog? What information does it hold, and why is it needed?

---

**Q2 (Conceptual)**
What is the difference between a Storage Credential and an External Location in Unity Catalog? If you have one Service Principal, how many Storage Credentials and how many External Locations would you need to access two different storage accounts?

---

**Q3 (Scenario)**
An admin creates an External Location pointing to `abfss://bronze@stadlsdev001.dfs.core.windows.net/`. A data engineer tries to create an external table at `abfss://silver@stadlsdev001.dfs.core.windows.net/folder/`. The CREATE TABLE succeeds but all SELECT queries fail. Why?

---

**Q4 (Tricky)**
The admin creates an External Location with URL `abfss://bronze@stadlsdev001.dfs.core.windows.net/raw/`. A data engineer creates an external table at `abfss://bronze@stadlsdev001.dfs.core.windows.net/processed/table1/`. Will it work? Why?

---

**Q5 (Scenario)**
A data engineer runs `Test connection` on an External Location and gets `Authorization failed`. The Storage Credential uses the correct Service Principal credentials. What is the most likely cause, and how do you fix it?

---

**Q6 (Conceptual)**
Where in the Azure Databricks UI do you create a Storage Credential? List both options (Account Console and Workspace UI paths, step by step).

---

## Unity Catalog Hierarchy

**Q7 (Warm-up)**
Draw the Unity Catalog object hierarchy from top to bottom. Which level is shared across all workspaces in an Azure Databricks account?

---

**Q8 (Conceptual)**
A company has three Databricks workspaces — dev, staging, and prod. They want Unity Catalog tables to be accessible from all three workspaces. What do they need to ensure?

---

**Q9 (Tricky)**
A data engineer creates a catalog with `Storage = Default`. Where will the files of internal (managed) tables in this catalog be stored?

---

## Internal vs External Tables

**Q10 (Warm-up)**
What SQL clause makes the difference between an internal and an external table? Write both `CREATE TABLE` statements for a table called `sales` in `dev_catalog.bronze`.

---

**Q11 (Scenario)**
A data engineer drops an external table. A business analyst says all the data is lost. Is the analyst correct? Explain what actually happened.

---

**Q12 (Scenario)**
A data engineer drops an internal table to "clean up". A senior engineer says the data is permanently gone. The data engineer says they can just re-run the pipeline to repopulate it. Who is right about the data being gone?

---

**Q13 (Conceptual)**
An external table exists at `abfss://bronze@stadlsdev001.dfs.core.windows.net/orders/`. ADF writes 5000 new rows to the same Delta path using a Delta sink. Will the Databricks external table see the new rows? What if ADF used a Parquet sink instead?

---

**Q14 (Tricky)**
A data engineer runs `DESCRIBE EXTENDED dev_catalog.bronze.orders`. They see `Type = MANAGED` but the table was supposed to be external. What most likely went wrong when the table was created? How do they fix it without losing data?

---

**Q15 (Scenario)**
A production external table accidentally had its Delta files deleted from ADLS (someone ran `dbutils.fs.rm` on the wrong path). The table still exists in the catalog. What happens when someone runs `SELECT * FROM dev_catalog.bronze.orders`? How do you recover?

---

## External Volumes

**Q16 (Warm-up)**
Write the SQL to create an external volume called `raw_files` in `dev_catalog.bronze` pointing to `abfss://files@stblobdev001.dfs.core.windows.net/`. How do you access it in a Python notebook cell?

---

**Q17 (Conceptual)**
A data engineer uploads a CSV to `stblobdev001` Blob Storage using the Azure Portal. They then access `/Volumes/dev_catalog/bronze/raw_files/` in a Databricks notebook. Will they see the uploaded file? Why?

---

**Q18 (Scenario)**
A data engineer runs `SELECT * FROM dev_catalog.bronze.raw_files`. `raw_files` is an external volume containing CSV files. The query fails. What is wrong, and how should they read the data?

---

**Q19 (Tricky)**
Can two External Volumes point to the same storage path? What would happen if both volumes are accessed simultaneously by two different notebooks?

---

**Q20 (Scenario)**
A volume is created at `abfss://files@stblobdev001.dfs.core.windows.net/`. A data engineer accidentally drops it. The operations team says 500 GB of raw files are gone. Is the operations team correct? Explain.

---

## Internal Volumes

**Q21 (Conceptual)**
A data engineer creates two volumes:
```sql
CREATE VOLUME dev_catalog.silver.temp_vol
CREATE EXTERNAL VOLUME dev_catalog.bronze.raw_vol LOCATION 'abfss://...'
```
What is the difference in what happens when each is dropped?

---

**Q22 (Scenario)**
A data engineer needs a scratch space for intermediate files during a Spark job. The files are not needed after the job finishes. Should they use an internal volume or an external volume? Why?

---

## ADF & Databricks Integration

**Q23 (Warm-up)**
What is a Databricks Linked Service in ADF? What two authentication methods can it use?

---

**Q24 (Conceptual)**
ADF passes `{"env": "prod", "batch_date": "2024-01-15"}` as Base Parameters to a Databricks Notebook Activity. Write the notebook code to read both values.

---

**Q25 (Scenario)**
An ADF pipeline runs a Databricks Notebook Activity. The activity runs for 9 minutes — but the notebook itself only takes 4 minutes to run on the all-purpose cluster. What is taking the other 5 minutes? How do you reduce this?

---

**Q26 (Tricky)**
A Databricks notebook calls `dbutils.notebook.exit("DONE: 5000 rows")`. Where exactly in ADF can you see this value? Write the ADF expression to reference it in a downstream activity.

---

**Q27 (Scenario)**
An ADF pipeline has a Copy Activity connected to a Notebook Activity with a green success arrow. The Copy Activity fails halfway. What happens to the Notebook Activity? What happens to the overall pipeline status?

---

**Q28 (Scenario)**
A Databricks notebook triggered by ADF fails with `PermissionDenied: User does not have Can Run permission on notebook`. The notebook is in the engineer's personal folder `Users/engineer@company.com/`. The Linked Service uses a Service Principal. What is the problem and how do you fix it?

---

## Cluster Access Modes

**Q29 (Conceptual)**
What are the three cluster access modes in Azure Databricks? Which one supports all four languages (Python, SQL, Scala, R) AND multiple simultaneous users?

---

**Q30 (System design)**
Design the complete Unity Catalog and ADF setup for this scenario:
- Raw CSV files arrive daily into Azure Blob Storage (`stblobdev001`, container: `landing`)
- A Databricks notebook reads the CSVs, cleans them, and writes Delta tables to ADLS Gen2 (`stadlsdev001`, container: `bronze`)
- Business analysts query the Delta tables from Databricks SQL
- ADF triggers the Databricks notebook daily at 6 AM

List every Unity Catalog object you need to create (storage credentials, external locations, catalog, schemas, tables, volumes), and describe the ADF setup (linked service, pipeline, activity, schedule).

---
