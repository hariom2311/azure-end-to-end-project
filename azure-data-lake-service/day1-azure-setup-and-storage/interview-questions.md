# Day 1 — Interview Questions: Azure Setup & Storage

> 30 questions across all 3 concepts. Attempt your own answer first, then check `interview-solutions.md`.

---

## Concept 1: Azure Account, Subscriptions & Resource Groups

**Q1 (Warm-up)**  
What is the difference between an Azure account and an Azure subscription? Can one account have multiple subscriptions?

---

**Q2 (Warm-up)**  
What is a Resource Group in Azure? Why should all resources for a single project be placed in the same Resource Group?

---

**Q3 (Conceptual)**  
A company has separate Development, Staging, and Production environments. Describe two ways you could organise these in Azure. What are the trade-offs of each approach?

---

**Q4 (Scenario)**  
Your team creates all Azure resources directly in the root of the subscription without using Resource Groups. At the end of the project, you need to tear down all dev resources. What problem do you face? How would you fix this going forward?

---

**Q5 (Conceptual)**  
What are Azure resource tags? Give three examples of tags a data engineering team would add to a storage account, and explain why each one is useful.

---

**Q6 (Scenario)**  
A data engineer accidentally deletes a Resource Group that contained a production storage account with 2 TB of data. The team had not enabled soft delete. What Azure features, if enabled beforehand, could have prevented data loss?

---

**Q7 (Tricky)**  
What is the difference between the **Owner**, **Contributor**, and **Reader** built-in Azure RBAC roles? If you give a data engineer the Contributor role on a Resource Group, can they delete the Resource Group itself?

---

## Concept 2: Azure Blob Storage

**Q8 (Warm-up)**  
What is an Azure Storage Account? What is a Container? What is a Blob?

---

**Q9 (Warm-up)**  
What are the three access tiers in Azure Blob Storage? When would you use each one?

---

**Q10 (Conceptual)**  
What is LRS, ZRS, and GRS redundancy in Azure Storage? Which would you recommend for a production data lake and why?

---

**Q11 (Scenario)**  
A company stores 500 million raw log files in a Blob Storage container. They want to rename the top-level "folder" from `logs/` to `raw-logs/`. How does Azure perform this rename internally, and what is the performance implication?

---

**Q12 (Conceptual)**  
What is a Shared Access Signature (SAS) token in Azure? When would you use a SAS token instead of assigning an RBAC role?

---

**Q13 (Tricky)**  
What is the difference between a **storage account access key** and an **Azure AD (Entra ID) RBAC role** for accessing Blob Storage? Which is more secure for production use, and why?

---

**Q14 (Scenario)**  
Your company's raw data files are accessed frequently for the first 30 days, rarely between 30–90 days, and almost never after 90 days. The total dataset is 100 TB and growing. How would you manage storage costs using Blob Storage features?

---

## Concept 3: Azure Data Lake Storage Gen2 (ADLS Gen2)

**Q15 (Warm-up)**  
What is Hierarchical Namespace (HNS) in ADLS Gen2? What does enabling it change compared to standard Blob Storage?

---

**Q16 (Warm-up)**  
Name three Azure services or tools that work natively with ADLS Gen2 for big data analytics. Why do they prefer ADLS Gen2 over standard Blob Storage?

---

**Q17 (Conceptual)**  
A data engineer renames a staging directory containing 10 million Parquet files to promote it to the production location. How long does this operation take in ADLS Gen2 vs. standard Blob Storage, and why?

---

**Q18 (Scenario)**  
Your Spark job writes data to ADLS Gen2. A business analyst should be able to read the `gold/` container but must not be able to write to `bronze/` or `silver/`. How would you configure this?

---

**Q19 (Conceptual)**  
What is the Bronze / Silver / Gold (Medallion) architecture in a data lake? Which container holds raw ingested data and which holds business-ready aggregated data?

---

**Q20 (Tricky)**  
A storage account is created with Hierarchical Namespace disabled. The team later realises they need ADLS Gen2 features. Can they enable HNS on the existing account? What is the correct approach?

---

**Q21 (Scenario)**  
An ADLS Gen2 storage account has a `bronze/` container with 50 TB of data. The team needs to give a third-party vendor read-only access to the `bronze/orders/2024/` directory only — not the entire container. How do you implement this?

---

**Q22 (Deep dive)**  
Explain how ADLS Gen2 supports POSIX-style ACLs. What is the difference between Access ACLs and Default ACLs? Why does this matter when Spark creates new files in a directory?

---

## Mixed / Senior-Level Questions

**Q23**  
A company's data team stores all their data in standard Azure Blob Storage. They are migrating to Databricks for their Spark workloads. What specific features of ADLS Gen2 make it the better choice for this migration, and what would be the first step in the migration?

---

**Q24**  
You are designing an Azure data lake for a financial services company. They require: (1) all data encrypted at rest, (2) all data in Australia, (3) 99.99% availability SLA, (4) the ability to recover from accidental deletion within 7 days. Which Azure storage configuration achieves all four requirements?

---

**Q25**  
A data engineer says: "We don't need ADLS Gen2 — we just use Blob Storage and it works fine with Spark." Under what specific conditions is this true, and under what conditions will they hit a performance or correctness problem?

---

**Q26**  
What is Azure Blob Lifecycle Management? Write the rule logic (in plain English) for a data lake that: moves Bronze data to Cool tier after 30 days, moves it to Archive after 1 year, and permanently deletes it after 7 years.

---

**Q27**  
What is the difference between **authentication** and **authorisation** in the context of Azure Storage? Give one example of each using Azure Storage features.

---

**Q28**  
A team has a storage account in East US region and their Spark cluster runs in Australia East. What performance problem will they face, and how do you fix it?

---

**Q29**  
What is Azure Storage's **soft delete** feature? How does it differ from enabling **versioning**? When would you use one vs. the other for a production data lake?

---

**Q30 (System design)**  
Design the complete Azure storage architecture for a fintech startup that needs to:
- Ingest 5 million payment events per day from a mobile app
- Store raw events permanently (compliance requirement)
- Produce a daily cleaned dataset for the analytics team
- Produce real-time fraud scores (updated every 5 minutes)
- Allow the data science team to train ML models on 2 years of history

For each layer, specify: storage service, container name, access tier, redundancy, and who has access (role).
