# Day 2 — Interview Questions: ADLS Gen2 Access & SAS Tokens

> 30 questions across all 3 concepts. Attempt your own answer first, then check `interview-solutions.md`.

---

## Concept 1: SAS Tokens

**Q1 (Warm-up)**
What is a Shared Access Signature (SAS) token in Azure Storage? How is it different from using an account access key?

---

**Q2 (Warm-up)**
What are the three types of SAS tokens in Azure Storage? Which one is the most secure for production use, and why?

---

**Q3 (Conceptual)**
A SAS token URL contains the parameter `sp=rl`. What does this mean, and what operations can the holder of this token perform?

---

**Q4 (Scenario)**
A developer generates an Account SAS token with Read, Write, Delete, List permissions and an expiry of 1 year. They share this URL with a vendor who needs to read one specific CSV file from the `bronze` container. What are the three security problems with this approach?

---

**Q5 (Conceptual)**
What happens when an Azure storage account's **Access Key 1** is rotated? Which existing SAS tokens are affected and which are not?

---

**Q6 (Scenario)**
A data analyst accidentally commits a SAS token to a public GitHub repository. The token has a 30-day expiry and Read + Write permissions on the `bronze` container. What are the immediate steps to contain the damage?

---

**Q7 (Tricky)**
What is a **User Delegation SAS**? How does it differ from an Account SAS or Service SAS, and why is it considered more secure?

---

**Q8 (Scenario)**
You need to give a third-party data vendor read-only access to files inside `bronze/vendor-xyz/` for the next 48 hours. The vendor must not be able to access any other container or directory. Describe the exact SAS token configuration you would use.

---

## Concept 2: Python SDK

**Q9 (Warm-up)**
What Python package do you install to access ADLS Gen2 programmatically? Why is it preferred over the `azure-storage-blob` package for ADLS Gen2?

---

**Q10 (Warm-up)**
In the `azure-storage-file-datalake` SDK, what is the difference between a `DataLakeServiceClient`, a `FileSystemClient`, and a `DataLakeFileClient`?

---

**Q11 (Conceptual)**
What is `DefaultAzureCredential` from the `azure-identity` package? In what order does it try different authentication methods?

---

**Q12 (Scenario)**
A data engineer writes a Python script that connects to ADLS Gen2 using a hardcoded SAS token:
```python
sas_token = "?sv=2022-11-02&ss=b&srt=sco&sp=rwdlcup..."
service_client = DataLakeServiceClient(account_url, credential=sas_token)
```
The script works in development. How should this be rewritten for production deployment on an Azure Databricks cluster?

---

**Q13 (Conceptual)**
When uploading a file using the ADLS Gen2 Python SDK, why do you need to call `append_data()` AND `flush_data()` separately? What happens if you skip `flush_data()`?

---

**Q14 (Scenario)**
A Python script tries to create a directory in ADLS Gen2 but fails with `ResourceExistsError`. The developer catches the error and ignores it:
```python
try:
    dir_client.create_directory()
except ResourceExistsError:
    pass
```
Is this a good pattern? When could silently ignoring `ResourceExistsError` cause a bug?

---

**Q15 (Tricky)**
You use `file_system_client.get_paths(path="orders/2024/01", recursive=False)` to list files. Your directory structure is:
```
orders/2024/01/15/file1.parquet
orders/2024/01/15/file2.parquet
orders/2024/01/16/file1.parquet
```
What does the listing return when `recursive=False`? What does it return when `recursive=True`?

---

**Q16 (Conceptual)**
What is the ABFS (Azure Blob File System) URL format used by Spark and Databricks to access ADLS Gen2? What does each part of the URL represent?

---

**Q17 (Scenario)**
A Spark job running on Azure Databricks needs to read Parquet files from ADLS Gen2. The data engineering team is debating two approaches:
- Option A: Mount the ADLS Gen2 container to `/mnt/bronze/`
- Option B: Use the `abfss://` URL directly with Managed Identity

What is the difference, and which is recommended in modern Databricks environments?

---

## Concept 3: AzCopy

**Q18 (Warm-up)**
What is AzCopy and what is it used for? Give two scenarios where you would prefer AzCopy over the Python SDK.

---

**Q19 (Warm-up)**
What is the difference between `azcopy copy` and `azcopy sync`? When would you use sync instead of copy?

---

**Q20 (Conceptual)**
AzCopy supports **server-to-server transfers**. What does this mean, and how is it different from a regular copy through a local machine?

---

**Q21 (Scenario)**
Your company has 50 TB of historical sales data on an on-premises Windows file server. You need to migrate all of it to ADLS Gen2 within 48 hours. Your internet connection is 1 Gbps. Describe your migration approach using AzCopy.

---

**Q22 (Tricky)**
An AzCopy command is run to copy 100,000 files and fails halfway through (network interruption at file 60,000). When you rerun the same AzCopy command, does it restart from the beginning or resume from file 60,001? Explain how this works.

---

**Q23 (Scenario)**
You run:
```bash
azcopy copy "C:\data\" "https://stadlsprod.dfs.core.windows.net/bronze/?sv=..." --recursive
```
Two hours later, 3 new files are added to `C:\data\`. You run the same command again. What happens to the existing 100,000 files in ADLS Gen2, and what is the more efficient command you should use for this ongoing sync scenario?

---

## Mixed / Senior-Level Questions

**Q24**
A data engineer argues: "We should just use storage account access keys everywhere — SAS tokens are too complicated to manage." What are three specific risks of using access keys instead of SAS tokens for external sharing?

---

**Q25**
Explain the principle of **least privilege** in the context of ADLS Gen2 access. Give three concrete examples of applying it when granting access to different roles: an ingestion pipeline, a data analyst, and a vendor.

---

**Q26**
A company stores sensitive customer PII in ADLS Gen2. They need to give a data analyst access to a Gold layer table but must ensure the analyst cannot access Bronze (raw PII). They also need an audit trail of who accessed what. What Azure features would you use to implement this?

---

**Q27**
What is the difference between **authentication** and **authorisation** in the context of ADLS Gen2 access? Give one example of each using Azure features.

---

**Q28**
A Python script reads a 10 GB Parquet file from ADLS Gen2 into memory using:
```python
content = file_client.download_file().readall()
```
What is the problem with this approach for large files, and how would you fix it?

---

**Q29**
You need to give a Databricks cluster access to ADLS Gen2. A colleague suggests using the storage account access key stored in `spark.conf.set()`. What is the problem with this approach, and what is the Azure-recommended alternative?

---

**Q30 (System design)**
Design the complete access control architecture for an ADLS Gen2 data lake used by the following teams:

| Team | What they need |
|---|---|
| Ingestion pipeline (ADF/Databricks) | Write to `bronze/`, read from `bronze/` |
| Transform pipeline (Databricks) | Read from `bronze/`, write to `silver/` and `gold/` |
| Data analysts (Power BI, SQL) | Read from `gold/` only |
| Data science team | Read from `silver/` and `gold/`, write to `ml-data/` |
| External vendor | Read-only access to `bronze/vendor-data/` for 7 days |

For each team, specify: the identity type (Managed Identity, User, Service Principal, SAS), the RBAC role or ACL permission, and the scope (account / container / directory level).
