# Day 1 — Interview Questions: Kickoff, Architecture, and Azure Setup

> 25 questions covering Azure resource setup, Key Vault, Service Principals, RBAC, ADLS Gen2, Databricks clusters, secret scopes, API auth, SAS tokens, and storage connection methods.
> Attempt first, then check `interview-solutions.md`.

---

## Azure Resources and Setup

**Q1 (Warm-up)**
What is an Azure Resource Group? Why should all resources for a single project live in one Resource Group?

---

**Q2 (Conceptual)**
What is the difference between Azure Blob Storage and ADLS Gen2? Which one is used in this project and why?

---

**Q3 (Conceptual)**
What is a resource provider in Azure? Give an example of what happens if you skip registering `Microsoft.KeyVault` before trying to create a Key Vault.

---

**Q4 (Scenario)**
A data engineer creates a storage account but accidentally picks **Standard ZRS** instead of **Standard LRS**. What is the cost difference, and how do they fix it?

---

**Q5 (Tricky)**
A data engineer creates a storage account in Azure and enables containers but forgets to turn on **hierarchical namespace**. They try to connect Databricks to it using `abfss://`. What problem will they hit?

---

## Key Vault and Secrets

**Q6 (Warm-up)**
What is Azure Key Vault? Why does this project store all credentials in Key Vault instead of hardcoding them in notebooks?

---

**Q7 (Conceptual)**
The Key Vault is created with the RBAC permission model. The engineer who created it tries to add a secret immediately and gets `Forbidden: ForbiddenByRbac`. What did they miss?

---

**Q8 (Scenario)**
A data engineer runs `az keyvault secret set` but gets:
`Conflict: ObjectIsDeletedButRecoverable — The secret is in a deleted but recoverable state.`
What does this mean and how do they fix it?

---

**Q9 (Tricky)**
A notebook calls `dbutils.secrets.get(scope="kv-ev-scope", key="voltgrid-password")` and prints the value with `print(password)`. The output shows `[REDACTED]`. Has the secret been read correctly or is there an error?

---

## Service Principal and RBAC

**Q10 (Warm-up)**
What is a Service Principal in Azure? Why do Databricks and ADF use a Service Principal instead of a human account to access storage?

---

**Q11 (Conceptual)**
After creating a Service Principal with `az ad sp create-for-rbac`, the engineer navigates away without copying the `password` field. What must they do and why?

---

**Q12 (Conceptual)**
A data engineer gives the Service Principal the `Storage Blob Data Owner` role instead of `Storage Blob Data Contributor`. What extra capability does Owner have that is not needed? What risk does it introduce?

---

**Q13 (Scenario)**
A notebook connects to ADLS Gen2 using SP OAuth and gets:
`403 Forbidden — The client does not have permission to perform this action.`
The SP was created correctly and the secret is in Key Vault. What is the most likely cause?

---

**Q14 (Tricky)**
Explain the difference between a Service Principal's **Application (Client) ID** and its **Object ID**. When you run `az role assignment create`, which one do you use with `--assignee-object-id`?

---

## Databricks Cluster and Secret Scope

**Q15 (Warm-up)**
What is a Databricks Secret Scope? How does it link Databricks to Azure Key Vault?

---

**Q16 (Conceptual)**
A data engineer creates a Databricks cluster with Access mode = Standard. They run `dbutils.fs.mount(...)` and get `Method not whitelisted`. What is the fix?

---

**Q17 (Scenario)**
A notebook ran successfully this morning. After lunch the engineer reopens the notebook on the same cluster (still running) and the cell that calls `dbutils.secrets.get(...)` fails with `KeyError: 'storage_account'`. What is the most likely cause?

---

**Q18 (Conceptual)**
Why does this project use **Dedicated** access mode instead of **Shared** mode for the Databricks cluster? What happens in Shared mode when `dbutils.fs.mount()` is called?

---

**Q19 (Tricky)**
A data engineer creates the Key Vault-backed secret scope but the notebooks keep getting `PERMISSION_DENIED: Invalid permissions on KeyVault — Status code 403`. The engineer's own account has `Key Vault Secrets Officer`. What specifically is still missing?

---

## ADLS Gen2 Connection Methods

**Q20 (Warm-up)**
What does `spark.conf.set()` do when you configure SP OAuth in a Databricks notebook? How long does this configuration last?

---

**Q21 (Conceptual)**
What is the `abfss()` helper function used in this project? Write it from memory and explain why the protocol is `abfss` and not `wasbs`.

---

**Q22 (Scenario)**
A data engineer is reading from the shared source blob `dataenggdailystorage` using `abfss://` and gets `InvalidAuthenticationInfo`. The SAS token is correct. What is wrong?

---

**Q23 (Tricky)**
Compare SP OAuth Direct Access (Method 1) with Mount + SP OAuth (Method 4). When is mounting blocked and why is Method 1 the recommended approach going forward?

---

## API Auth and Architecture

**Q24 (Conceptual)**
The VoltGrid API uses token-based authentication. Describe the complete auth flow from credentials in Key Vault to a successful API call. Why is the token considered more secure than storing the password in the request header directly?

---

**Q25 (System design)**
You are setting up a new Azure project from scratch for a retail analytics platform. Using the same pattern as this EV project, describe:
- What Azure resources you would create on Day 1
- What naming convention you would follow
- How you would handle secrets for a SQL database connection string, an API key, and a storage account
- How Databricks would connect to the storage account
- What role each identity (your user account, Databricks SP, ADF managed identity) would have on Key Vault and Storage

---
