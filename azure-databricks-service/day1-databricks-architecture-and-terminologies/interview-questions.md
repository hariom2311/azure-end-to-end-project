# Day 1 — Interview Questions: Databricks Architecture & Terminologies

> 30 questions on Databricks architecture, Spark execution model, Delta Lake, and all key terminologies.
> Attempt first, then check `interview-solutions.md`.

---

## Architecture & Two-Plane Model

**Q1 (Warm-up)**
What is the two-plane model in Azure Databricks? Name what runs in each plane and explain why this separation matters for data security.

---

**Q2 (Conceptual)**
A colleague says "when I delete my Databricks workspace, all my data is deleted too." Is this accurate? Explain what is and is not deleted when a workspace is removed.

---

**Q3 (Scenario)**
Your company's security team says "no data can leave our Azure subscription." Can you use Databricks and comply with this requirement? What architectural feature enables this?

---

**Q4 (Tricky)**
What is the difference between the Control Plane and the Data Plane in terms of cost? Which one do you pay for as an Azure customer, and which one is included in the Databricks license?

---

## Cluster Architecture

**Q5 (Warm-up)**
What is the role of the Driver Node in a Spark cluster? What happens to the entire pipeline if the Driver Node crashes?

---

**Q6 (Conceptual)**
What is the difference between an All-Purpose Cluster and a Job Cluster? Which one would you use for a nightly scheduled Silver-layer transformation, and why?

---

**Q7 (Scenario)**
A cluster has 4 Worker Nodes. Each Worker has 4 cores and 16 GB RAM. How many Executors does the cluster have, and how much memory can each Executor use? (Assume one Executor per Worker for simplicity.)

---

**Q8 (Tricky)**
Your nightly job runs on an All-Purpose Cluster that is shared with 5 data scientists running interactive notebooks. The job is slow. What are two likely causes and what is the recommended fix?

---

**Q9 (Conceptual)**
What is Databricks Runtime (DBR)? What does "LTS" mean and why should you choose an LTS runtime for production?

---

## Spark Execution Model

**Q10 (Warm-up)**
Explain the difference between a Transformation and an Action in Spark. Give two examples of each. Why does this distinction matter for performance?

---

**Q11 (Conceptual)**
What is lazy evaluation in Spark? What does Spark do between when you call a Transformation and when you call an Action?

---

**Q12 (Scenario)**
A student writes this code:
```python
df = spark.read.json("/mnt/bronze/payments/")
df_clean = df.filter(df.status == 'active')
count = df_clean.count()
df_clean.show(10)
df_clean.write.parquet("/mnt/silver/payments/")
```
How many Spark Jobs are triggered? Why?

---

**Q13 (Tricky)**
What is a Shuffle? Give a concrete example using VoltGrid payments data. Why is shuffle considered the most expensive Spark operation?

---

**Q14 (Conceptual)**
What is the relationship between a Job, a Stage, and a Task? What causes a new Stage to be created?

---

**Q15 (Scenario)**
A DataFrame has 1000 partitions and you call `.count()`. How many Tasks does Spark create for this operation (assume a single stage)?

---

**Q16 (Tricky)**
You run `df.show(5)` three times in a row in a notebook. How many times does Spark read the source file? What do you do to fix this, and what is the trade-off?

---

## Delta Lake

**Q17 (Warm-up)**
What is Delta Lake? Name four features it adds over plain Parquet files.

---

**Q18 (Conceptual)**
What is the `_delta_log` directory? What is stored inside it, and how does it enable ACID transactions?

---

**Q19 (Scenario)**
You accidentally overwrite a Delta table with wrong data at 9:00 AM. You need to restore the data as it was at 8:55 AM. What Delta Lake feature do you use? Write the PySpark code.

---

**Q20 (Tricky)**
What is the difference between OPTIMIZE and VACUUM in Delta Lake? Can you run VACUUM immediately after a table is created without any risk?

---

**Q21 (Conceptual)**
What is the difference between a Managed Table and an External Table in Unity Catalog? Which would you use for the VoltGrid Silver payments table and why?

---

**Q22 (Scenario)**
A colleague writes:
```python
df.write.format("delta").mode("overwrite").save("/mnt/silver/payments/")
```
Then immediately checks: `dbutils.fs.ls("/mnt/silver/payments/")` and sees the old Parquet files are still there alongside the new ones. Why? How does Delta know which files are "current"?

---

## Unity Catalog & Workspace

**Q23 (Warm-up)**
Describe the three-level namespace in Unity Catalog. For the VoltGrid project, give a real example of a fully-qualified table name using all three levels.

---

**Q24 (Conceptual)**
What is the difference between a Unity Catalog Volume and a Unity Catalog Table? When would you use each?

---

**Q25 (Tricky)**
You run `DROP TABLE ev_catalog.silver.payments`. The table is a Managed Table. What happens to the data files in ADLS? If it were an External Table, what would happen instead?

---

## dbutils and Notebooks

**Q26 (Warm-up)**
Name the four main modules of dbutils and what each one does.

---

**Q27 (Scenario)**
You have a notebook `/VoltGrid/silver/process_payments` that reads a `run_date` widget. How do you call this notebook from another notebook, pass `run_date = "2026-10-01"`, and capture its return value?

---

**Q28 (Tricky)**
A developer stores a database password directly in a notebook cell: `password = "EVcharge@AU2025"`. What is wrong with this, and what is the correct Databricks approach?

---

## Workflows & Delta Live Tables

**Q29 (Conceptual)**
What is a Databricks Workflow? What is the difference between using Databricks Workflows vs ADF to orchestrate notebook execution?

---

**Q30 (Tricky)**
What is Delta Live Tables (DLT)? How does it differ from writing regular Spark notebooks? Give one advantage and one limitation of DLT compared to standard PySpark notebooks.

---
