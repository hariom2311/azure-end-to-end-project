# Day 7 — Interview Questions: ADF + Databricks Integration

> 25 questions covering Linked Service authentication methods, managed identities, PAT tokens, notebook activities, Base Parameters, runOutput, cluster options, pipeline scheduling, and dependency arrows.
> Attempt first, then check `interview-solutions.md`.

---

## ADF Linked Service & Authentication

**Q1 (Warm-up)**
What is an ADF Linked Service? When you connect ADF to Databricks, what two things does the Linked Service store?

---

**Q2 (Conceptual)**
The ADF Databricks Linked Service form shows three authentication options. Name all three and describe in one sentence what makes each different.

---

**Q3 (Scenario)**
A data engineer creates an ADF Linked Service using Access token. Three months later, the pipeline fails with `Authentication failed: token is expired`. What happened, and how do you fix it without downtime?

---

**Q4 (Tricky)**
A data engineer says: "I'll use my own PAT token for the ADF Linked Service — it's convenient." A senior engineer says this is a bad practice. Why?

---

**Q5 (Conceptual)**
What is a system-assigned managed identity in the context of ADF? What happens to this identity if the ADF resource is deleted?

---

**Q6 (Scenario)**
ADF is configured with a system-assigned managed identity Linked Service to Databricks. The Test connection says `Connection successful`. But when the pipeline runs, the Notebook activity fails with `PermissionDenied`. What is the most likely cause?

---

**Q7 (Conceptual)**
What is the difference between a system-assigned and a user-assigned managed identity? Give one scenario where you would prefer user-assigned over system-assigned.

---

**Q8 (Tricky)**
A company has five ADF pipelines, each in a different ADF instance, that all need to access the same Databricks workspace. A user-assigned managed identity is created and assigned to all five ADF instances. What is the advantage of this approach over creating five separate system-assigned managed identities?

---

**Q9 (Conceptual)**
Where in the Azure Portal do you grant a managed identity the `Contributor` role on a Databricks workspace? Walk through the exact steps (UI path).

---

**Q10 (Scenario)**
A data engineer is setting up ADF → Databricks integration using user-assigned managed identity. They create the managed identity, assign it to ADF, but forget to grant it the Contributor role on the Databricks workspace. What error will they see when they click Test connection?

---

## Notebook Activity & Parameters

**Q11 (Warm-up)**
What is a Base Parameter in the ADF Databricks Notebook Activity? How does the notebook read the value of a Base Parameter?

---

**Q12 (Conceptual)**
ADF passes `batch_date = @formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')` as a Base Parameter. Write the complete Python notebook code to read this parameter and print it.

---

**Q13 (Scenario)**
An ADF pipeline passes `env = prod` as a Base Parameter to a Databricks notebook. Inside the notebook, the engineer writes `env = "dev"` on the first line. When ADF triggers the pipeline, which value does `env` hold — `prod` or `dev`? Explain why.

---

**Q14 (Conceptual)**
What does `dbutils.notebook.exit("DONE: 500 rows")` do? Where in ADF can you see the value it returned? Write the ADF expression to use this value in a downstream activity.

---

**Q15 (Tricky)**
A Databricks notebook triggered by ADF has 5 cells. Cell 3 calls `dbutils.notebook.exit("partial")`. Cell 4 and Cell 5 still have code. What happens when Cell 3 runs? Do Cells 4 and 5 execute?

---

**Q16 (Scenario)**
An ADF Notebook Activity is configured with Base Parameter `env = @pipeline().parameters.environment`. The pipeline has no parameter named `environment`. What happens when the pipeline runs?

---

## Cluster Configuration

**Q17 (Warm-up)**
What is the difference between `New job cluster` and `Existing interactive cluster` in the ADF Linked Service? Which is recommended for production, and why?

---

**Q18 (Scenario)**
An ADF pipeline is running in production using `New job cluster`. The pipeline is scheduled to run every 5 minutes. A data architect says this is expensive and slow. What is wrong with this design, and what two options fix it?

---

**Q19 (Scenario)**
A data engineer uses `Existing interactive cluster` in the ADF Linked Service. During a busy period, 10 ADF pipelines run simultaneously and all route to the same cluster. What happens to the cluster's memory and CPU?

---

## Pipeline Design & Scheduling

**Q20 (Conceptual)**
What are the four types of dependency arrows in ADF between activities? Give a real use case for the red (failure) arrow.

---

**Q21 (Scenario)**
An ADF pipeline has three activities in sequence: A → B → C, all connected with green (success) arrows. Activity B fails. What happens to Activity C? What is the final pipeline status?

---

**Q22 (Scenario)**
An ADF pipeline has three activities: A → B connected with a red arrow (failure), and A → C connected with a green arrow (success). Activity A succeeds. Which activities run: B, C, both, or neither?

---

**Q23 (Conceptual)**
What must you do after setting up a Schedule trigger in ADF for it to actually activate and start running? What happens if you forget this step?

---

## Monitoring & Troubleshooting

**Q24 (Scenario)**
An ADF Databricks Notebook Activity shows status `Running` for 25 minutes. The notebook normally takes 3 minutes. What are the two most likely causes, and how do you diagnose each?

---

**Q25 (System design)**
Design the complete ADF integration for this scenario:
- A CSV file arrives daily in `stblobdev001/landing/` container at 7 AM
- ADF should trigger a Databricks notebook that reads the CSV, transforms it, and writes a Delta table to `stadlsdev001/bronze/`
- The notebook should return the number of rows written
- If the notebook fails, a second "error handler" notebook should run

Describe: authentication method, cluster type, Linked Service config, pipeline activities, dependency arrows, Base Parameters, Schedule trigger setup, and how to read the row count from downstream.

---
