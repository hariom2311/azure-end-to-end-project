# Day 3 — Interview Questions: Notebooks, Jobs & Workflows

> 30 questions on workspace navigation, notebooks, magic commands, dbutils, Databricks Jobs, task dependencies, parameters, and scheduling.
> Attempt first, then check `interview-solutions.md`.

---

## Workspace

**Q1 (Warm-up)**
What is the difference between the `Shared` folder and the `Users` folder in the Databricks Workspace section?

---

**Q2 (Conceptual)**
A data engineer has a notebook in `Users/alice@company.com/transform.py`. A second engineer (Bob) needs to run it in production every night as a job. What is the problem and how should they fix it?

---

**Q3 (Scenario)**
A manager asks: "Can you give the QA team read-only access to our transformation notebooks, but they should not be able to run or edit them?" How do you configure this in Databricks?

---

**Q4 (Tricky)**
What is a Databricks Personal Access Token (PAT)? Where is it used, and what is the security risk of generating one with no expiry date?

---

## Notebooks

**Q5 (Warm-up)**
You create a notebook with default language Python. You want one cell to run SQL. What do you put at the top of that cell?

---

**Q6 (Conceptual)**
What is the difference between `%fs` and `dbutils.fs`? When would you use each?

---

**Q7 (Scenario)**
A data engineer types this in a Python notebook:
```python
%sql
SELECT * FROM payments_clean WHERE status = 'completed'
```
The query fails with `Table or view not found: payments_clean`. What are two possible causes?

---

**Q8 (Conceptual)**
What is `df.createOrReplaceTempView("my_view")`? When does the view disappear?

---

**Q9 (Scenario)**
A notebook has 10 cells. The engineer clicks **Run All**. Cell 5 fails. Which cells have outputs and which do not?

---

**Q10 (Tricky)**
A notebook contains this code:
```python
%run ./helper_notebook
```
What does this do? What is the difference between `%run` and `dbutils.notebook.run()`?

---

**Q11 (Warm-up)**
What does `dbutils.notebook.exit("DONE")` do when:
a) The notebook is run manually by a user
b) The notebook is run by a Databricks Job

---

**Q12 (Conceptual)**
A notebook has this code:
```python
dbutils.widgets.text("env", "dev", "Environment")
env = dbutils.widgets.get("env")
```
What happens when the cells are run in a notebook opened in the browser? What happens when a Job runs the notebook?

---

**Q13 (Scenario)**
A data engineer uses `%sh` magic to install a library:
```bash
%sh pip install great_expectations
```
After the cluster restarts, the library is gone. Why, and what is the correct way to install libraries persistently?

---

## dbutils

**Q14 (Warm-up)**
Write the `dbutils.fs` command to:
a) List files in `abfss://silver@evdatalakedev.dfs.core.windows.net/`
b) Delete the folder `dbfs:/tmp/old_data/` and everything inside it

---

**Q15 (Scenario)**
A notebook needs to read a password from Azure Key Vault. Write the code to read it using `dbutils.secrets`.

---

**Q16 (Tricky)**
`dbutils.notebook.run("./child", timeout_seconds=300, arguments={"env": "prod"})` returns a string. Where does that string come from in the child notebook?

---

## Databricks Jobs

**Q17 (Warm-up)**
What is the difference between running a notebook manually on an all-purpose cluster and running it via a Databricks Job?

---

**Q18 (Conceptual)**
In a Databricks Job, what is the recommended cluster type: an existing all-purpose cluster or a new job cluster? Why?

---

**Q19 (Scenario)**
A job is scheduled at `0 2 * * *`. In plain English, when does it run? Rewrite it to run every Monday at 6:30 AM.

---

**Q20 (Tricky)**
A Databricks Job runs a notebook that calls `spark.read.json(...)` from ADLS Gen2. The notebook works perfectly when run manually on the dev cluster, but fails in the job with `AuthorizationPermissionMismatch`. Why?

---

**Q21 (Scenario)**
A job has `Max retries: 3` and `Retry interval: 5 minutes`. The notebook always fails. How many total attempts run and over what total time?

---

**Q22 (Conceptual)**
What is shown in the **Logs** tab of a job task run? What is shown in the **Spark UI** tab?

---

**Q23 (Tricky)**
A job runs a notebook at midnight. The notebook takes 3 hours. The next scheduled run is at 1 AM. What happens — does the 1 AM run start while the midnight run is still going?

---

## Multi-Task Jobs (Pipelines)

**Q24 (Warm-up)**
In a multi-task Databricks Job, what does "Depends on" mean between two tasks?

---

**Q25 (Scenario)**
A pipeline has 3 tasks: A → B → C. Task B fails. What happens to task C?

---

**Q26 (Conceptual)**
Two tasks in a Databricks Job both depend on Task A but do not depend on each other. Draw the DAG. Do they run in parallel or sequentially?

---

**Q27 (Tricky)**
In a multi-task job, each task uses a **new job cluster**. A data engineer complains: "The pipeline takes 20 minutes but 15 of that is just cluster startup." How do you fix this without switching to a shared all-purpose cluster?

---

## Parameters & Integration

**Q28 (Scenario)**
A job passes `run_date = {{job.start_time.iso_date}}` to a notebook. What is the actual value injected at runtime? How does the notebook read it?

---

**Q29 (Conceptual)**
What is the difference between `dbutils.widgets.text("key", "default", "label")` and `dbutils.widgets.get("key")`? Can `dbutils.widgets.get` be called without `dbutils.widgets.text` first?

---

**Q30 (System design)**
A VoltGrid pipeline runs nightly: Bronze ingest (from API) → Silver transform → Gold aggregation. ADF triggers the Bronze Copy Activity. After the copy, the Silver and Gold notebooks should run. Give two design options for how to trigger the Databricks notebooks from ADF, and state which one you recommend and why.

---
