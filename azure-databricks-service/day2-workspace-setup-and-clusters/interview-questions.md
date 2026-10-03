# Day 2 — Interview Questions: Workspace Setup & Clusters

> 30 questions on workspace hierarchy, cluster types, configuration, instance pools, policies, secrets, and ADLS access.
> Attempt first, then check `interview-solutions.md`.

---

## Workspace & Account

**Q1 (Warm-up)**
What is the difference between a Databricks Account and a Databricks Workspace? Can one account have multiple workspaces? Can one workspace belong to multiple accounts?

---

**Q2 (Conceptual)**
A company has three environments: dev, staging, prod. They also have two teams: data engineering and data science. How many Databricks workspaces would you recommend and why?

---

**Q3 (Scenario)**
A new workspace is created with Standard pricing tier. The admin tries to enable Unity Catalog and cannot find the option. Why, and what is the fix?

---

**Q4 (Tricky)**
An admin disables DBFS browser in the workspace admin settings. A data engineer says "now I can't access my data." Is the engineer right? What does disabling DBFS browser actually prevent?

---

## Cluster Types

**Q5 (Warm-up)**
Name the three main cluster types in Azure Databricks and describe in one sentence when you use each.

---

**Q6 (Scenario)**
A pipeline runs every night at 2am. It processes 50GB of Bronze data and writes Silver Parquet. Which cluster type should it use and why? What happens to the cluster after the job finishes?

---

**Q7 (Conceptual)**
What is the DBU rate difference between an All-Purpose cluster and a Job cluster for identical VM sizes? Why does Databricks charge more for All-Purpose?

---

**Q8 (Tricky)**
A team runs their nightly job on an All-Purpose cluster that is shared with 3 other teams during the day. The job succeeds but takes 3 hours instead of the expected 45 minutes. What is the most likely cause and how do you fix it?

---

## Cluster Configuration

**Q9 (Warm-up)**
What is the difference between `Single User` and `Shared` access mode in Databricks? Which one supports Scala?

---

**Q10 (Scenario)**
A data scientist wants to run a PyTorch model training notebook on Databricks. Which Databricks runtime should they choose and why?

---

**Q11 (Conceptual)**
What is Photon acceleration? Which types of operations does it speed up, and which does it NOT accelerate?

---

**Q12 (Scenario)**
Your Silver transform job runs on 4 worker nodes (Standard_E8ds_v4) and takes 20 minutes. The team asks you to make it run in 10 minutes. What are two options and what are the trade-offs of each?

---

**Q13 (Tricky)**
A cluster is configured with `Min workers: 2, Max workers: 10` (autoscaling). The job runs for 30 minutes. For the first 5 minutes there is heavy load, then the next 25 minutes are light. How does autoscaling behave and what does the cost profile look like?

---

**Q14 (Conceptual)**
What is the difference between the Driver node and a Worker node in a Spark cluster? If the Driver node is undersized, what symptom do you see?

---

**Q15 (Scenario)**
A cluster is configured with `Terminate after 120 minutes of inactivity`. A notebook runs a cell that takes 3 hours to complete. Does the cluster auto-terminate during that run? Explain.

---

**Q16 (Tricky)**
You use Spot instances for worker nodes on a production job that runs nightly. At 3am the job fails with `SparkException: Lost executor`. What happened and what is the correct fix without abandoning Spot?

---

## Instance Pools

**Q17 (Warm-up)**
What is a Databricks instance pool? What problem does it solve?

---

**Q18 (Scenario)**
A pool has `Min idle instances: 2`. At 2am, a job cluster draws all 2 idle VMs from the pool and needs 3 more. Where do the 3 additional VMs come from and how long does it take?

---

**Q19 (Tricky)**
A pool is configured with `Min idle instances: 5`. Are these 5 idle VMs billed at DBU rates, Azure VM rates, or both? Explain.

---

## Cluster Policies

**Q20 (Warm-up)**
What is a cluster policy in Databricks? Who can create one?

---

**Q21 (Scenario)**
A cluster policy has this setting:
```json
"autotermination_minutes": { "type": "fixed", "value": 60 }
```
A user creates a cluster using this policy and tries to change auto-termination to 120 minutes. What happens?

---

**Q22 (Tricky)**
What is the difference between `"type": "fixed"` and `"type": "forbidden"` in a policy attribute? Give a use case for each.

---

## Secrets & Security

**Q23 (Warm-up)**
What is a Databricks secret scope? What are the two types and which is recommended for production?

---

**Q24 (Scenario)**
A data engineer writes this in a notebook:
```python
password = dbutils.secrets.get(scope="voltgrid-kv", key="voltgrid-password")
print(password)
```
What does the notebook output show and why?

---

**Q25 (Tricky)**
A notebook retrieves a secret and uses it in a Spark config:
```python
spark.conf.set("fs.azure.account.oauth2.client.secret.myaccount.dfs.core.windows.net",
               dbutils.secrets.get(scope="kv", key="sp-secret"))
```
A colleague says: "This is unsafe — the secret is now in the Spark config and anyone can read it with `spark.conf.get()`." Is the colleague right?

---

## ADLS Access & Configuration

**Q26 (Conceptual)**
What is the URI scheme for reading from an ADLS Gen2 container in Databricks? Write the full URI for the `bronze` container in the `evdatalakedev` storage account.

---

**Q27 (Scenario)**
A notebook reads from ADLS Gen2 using Service Principal credentials. It works fine from `voltgrid-dev-shared` cluster but fails from a new cluster with the error `AuthorizationPermissionMismatch`. What is the most likely cause?

---

**Q28 (Tricky)**
What is the difference between configuring ADLS access via Spark config (`spark.conf.set(...)`) and configuring it via Unity Catalog external locations? Which is better for production and why?

---

## Mixed / Senior

**Q29 (System design)**
Design the complete Databricks workspace setup for the VoltGrid project. Include: workspace count, pricing tier, cluster types per environment, access modes, instance pools, secret scope, and ADLS access method. Justify each decision.

---

**Q30 (Tricky)**
A new data engineer joins the team and is given access to the `voltgrid-dev-shared` cluster (Shared access mode). They try to run `%pip install requests` in their notebook and get an error. In a separate notebook, you run the same command on `voltgrid-single-user-test` (Single User mode) and it works. Why does the behaviour differ between access modes, and what are two ways to give the engineer access to `requests` on the shared cluster?

---
