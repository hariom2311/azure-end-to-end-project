# Day 1 — Interview Questions: Azure Key Vault Fundamentals

> 30 questions across all 3 concepts. Attempt your own answer first, then check `interview-solutions.md`.

---

## Concept 1: What is Azure Key Vault — Fundamentals & Creation

**Q1 (Warm-up)**  
What is Azure Key Vault and what problem does it solve? Why shouldn't you store secrets in environment variables or configuration files committed to Git?

---

**Q2 (Warm-up)**  
What are the three types of objects Azure Key Vault can store? Give one real-world example of each type in a data engineering context.

---

**Q3 (Conceptual)**  
What is the difference between Azure Key Vault Standard tier and Premium tier? For a data engineering team building a payment data lake, which tier would you choose and why?

---

**Q4 (Scenario)**  
A junior engineer says: "I just store my database password in a `.env` file and add `.env` to `.gitignore` — that's secure enough." What are the specific risks with this approach that Key Vault eliminates?

---

**Q5 (Conceptual)**  
What is the Vault URI and why is it important? What is the format of the Vault URI for a Key Vault named `kv-payments-prod`?

---

**Q6 (Scenario)**  
Your company has three environments: dev, staging, and production. Each environment has its own ADLS storage account with different SAS tokens. Should you use one Key Vault for all three environments or a separate Key Vault per environment? What are the trade-offs?

---

**Q7 (Tricky)**  
What is **soft delete** and **purge protection** in Azure Key Vault? If purge protection is enabled and you delete a Key Vault, what happens? How do you permanently delete it?

---

**Q8 (Conceptual)**  
What is the difference between the **Key Vault Secrets User** role and the **Key Vault Secrets Officer** role in RBAC? Which role would you assign to an application's Managed Identity that only needs to read secrets at runtime?

---

**Q9 (Scenario)**  
An attacker gains access to one of your developer's Azure AD accounts. The developer had the Key Vault Secrets Officer role on the production vault. What is the blast radius of this breach? What specific Azure features limit or detect this?

---

**Q10 (Tricky)**  
What happens to secrets stored in an Azure Key Vault if the vault is deleted? What is the minimum configuration required to ensure secrets can be recovered after accidental deletion?

---

## Concept 2: Secrets Management — Create, Version, Retrieve, Access Control

**Q11 (Warm-up)**  
What is secret **versioning** in Azure Key Vault? When you update a secret's value, what happens to the previous value?

---

**Q12 (Warm-up)**  
What is the difference between a secret being **disabled** and a secret being **expired** in Key Vault? Can a disabled secret be re-enabled without data loss?

---

**Q13 (Conceptual)**  
What is the difference between **Vault Access Policies** (legacy) and **Azure RBAC** as the permission model for Key Vault? Which does Microsoft recommend for new vaults and why?

---

**Q14 (Scenario)**  
A Databricks cluster needs to read the `adls-sas-token` secret. A data analyst's personal account needs to read the same secret. How do you configure RBAC differently for these two identities, and what role do you assign to each?

---

**Q15 (Conceptual)**  
Explain the RBAC scope levels available for Key Vault secrets. Can you grant a service principal access to only one specific secret inside a vault, without giving it access to all other secrets?

---

**Q16 (Scenario)**  
A SAS token expires after 30 days. You store it in Key Vault with a corresponding secret expiry of 30 days. On day 31, a pipeline tries to read the secret. What happens? What should the pipeline do instead of crashing?

---

**Q17 (Tricky)**  
A pipeline calls `get_secret("adls-sas-token")` and always gets the current version. You need to roll back to the previous version of the secret after discovering the current token has an incorrect permission scope. How do you do this — at the Key Vault level and in the consuming application?

---

**Q18 (Conceptual)**  
What is a **secret activation date** in Key Vault? Give a realistic scenario where you would set a future activation date rather than enabling the secret immediately.

---

**Q19 (Scenario)**  
Your company's security team asks you to prove who accessed the `db-password` secret in Key Vault over the last 30 days, and when. Which Azure feature provides this audit trail and how do you access it?

---

**Q20 (Tricky)**  
What is the maximum number of **Access Policy** entries allowed in a single Azure Key Vault? What problem does this cause for large organisations, and how does RBAC solve it?

---

## Concept 3: Accessing Secrets from Python — SDK, DefaultAzureCredential, Safe Patterns

**Q21 (Warm-up)**  
What Python packages are needed to read a secret from Azure Key Vault? Write the `pip install` command and name the class you use to interact with secrets.

---

**Q22 (Warm-up)**  
What is `DefaultAzureCredential`? List at least four credential sources it tries, in order. Which one does it use when you run a Python script locally after `az login`?

---

**Q23 (Conceptual)**  
Why is `DefaultAzureCredential` preferred over creating a `ClientSecretCredential` (which takes a client ID and client secret explicitly) for production workloads on Azure?

---

**Q24 (Scenario)**  
A Python script reads a secret from Key Vault and then passes it to an HTTP request as a header. The script also logs "Processing request..." to a file. A junior engineer writes: `logging.info(f"Calling API with token: {sas_token}")`. What is wrong with this, and how do you fix it?

---

**Q25 (Conceptual)**  
What is the difference between `client.list_properties_of_secrets()` and `client.get_secret(name)` in the `azure-keyvault-secrets` SDK? Why does `list_properties_of_secrets()` not return secret values?

---

**Q26 (Scenario)**  
A long-running Spark Structured Streaming job reads `db-password` from Key Vault once at startup and caches the value in a Python variable for the life of the job. The DBA rotates the database password mid-run. What problem occurs and how would you redesign the secret-reading pattern to handle rotation without restarting the job?

---

**Q27 (Tricky)**  
You call `client.get_secret("adls-sas-token")` in Python and receive the error: `azure.core.exceptions.HttpResponseError: (Forbidden) The user, group or application 'objectId=...' does not have secrets get permission on key vault 'kv-datalake-dev-001'`. What are all the possible causes of this error and how do you diagnose which one it is?

---

**Q28 (Conceptual)**  
What exception does the `azure-keyvault-secrets` SDK raise when a secret is not found or is disabled? Write a Python `try/except` block that handles this gracefully and returns `None` instead of crashing.

---

**Q29 (Scenario)**  
You need to rotate all ADLS SAS tokens weekly across 5 storage containers. Each SAS token is stored as a separate secret in Key Vault. Design a Python automation script that: (1) generates new SAS tokens, (2) updates each secret in Key Vault using `client.set_secret()`, and (3) verifies the new version is readable before finishing. Describe the design — code is optional.

---

**Q30 (System design — Senior)**  
Design the complete secrets management architecture for a fintech EV payment data platform with the following requirements:
- 3 environments: dev, staging, production
- Production processes 10 million payments per day via Databricks Structured Streaming
- Secrets include: ADLS SAS tokens (5), Event Hub connection strings (3), PostgreSQL credentials (1 username + 1 password), 2 third-party payment gateway API keys
- Regulatory requirement: all production secret access must be auditable for 1 year
- Secrets must be rotated every 90 days automatically
- No human should be able to read production secret values directly — only the application's Managed Identity

For each requirement, specify the Azure Key Vault configuration, RBAC setup, rotation mechanism, and audit approach.
