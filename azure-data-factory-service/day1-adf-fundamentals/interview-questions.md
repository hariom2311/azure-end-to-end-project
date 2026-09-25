# Day 1 — Interview Questions: ADF Core Concepts, Linked Services, Datasets & Copy Activity

> 30 questions across all 3 concepts. Attempt your own answer first, then check `interview-solutions.md`.

---

## Concept 1: What is Azure Data Factory (Q1–Q10)

**Q1 (Warm-up)**  
What is Azure Data Factory? Name three things it can do that a basic Python script with a scheduler cannot do out of the box.

---

**Q2 (Warm-up)**  
What is the difference between ETL and ELT? In a modern Azure data lake architecture with ADF and Databricks, which pattern is more common and why?

---

**Q3 (Conceptual)**  
What are the four panels in the ADF Studio UI? Describe what you use each one for during the development and operations lifecycle of a pipeline.

---

**Q4 (Scenario)**  
A data engineer says: "We already have Python ETL scripts that run on an Azure VM with cron. Why should we migrate to ADF?" Give three specific, concrete reasons why ADF would be better in a production environment.

---

**Q5 (Conceptual)**  
What is an Integration Runtime (IR) in Azure Data Factory? What is the difference between the Azure Integration Runtime and a Self-Hosted Integration Runtime? When would you need a Self-Hosted IR?

---

**Q6 (Scenario)**  
Your company has an on-premises Oracle database that is not publicly accessible from the internet. You need to ingest data from it into ADLS Gen2 using ADF. What type of Integration Runtime do you need to configure, and what must be installed on-premises?

---

**Q7 (Tricky)**  
ADF has a "Debug" run and a "Trigger" run. What is the difference between them? Does a Debug run consume the same compute as a Trigger run? Does a Debug run appear in the Monitor panel?

---

**Q8 (Conceptual)**  
What is the difference between an ADF **pipeline** and an ADF **activity**? Give one example of a pipeline that contains three activities and explain why each activity is there.

---

**Q9 (Scenario)**  
A pipeline is scheduled to run at 6:00 AM every day. On Tuesday, the pipeline runs for 4 hours (8:00 AM finish). On Wednesday, the pipeline fails at 7:30 AM after 1.5 hours. You need to know exactly which activity failed and what error it produced. How do you find this information in ADF Studio?

---

**Q10 (Tricky)**  
What is "Publish All" in ADF? If a data engineer makes changes to a pipeline in ADF Studio but does not click Publish All, what happens when a trigger fires? Does the trigger use the saved draft or the last published version?

---

## Concept 2: Linked Services and Datasets (Q11–Q20)

**Q11 (Warm-up)**  
What is a Linked Service in ADF? What information does a Linked Service store? Give two examples of Linked Services you would create for a pipeline that reads from an HTTP API and writes to ADLS Gen2.

---

**Q12 (Warm-up)**  
What is a Dataset in ADF? What is the difference between a Linked Service and a Dataset? Use an analogy to explain the relationship between them.

---

**Q13 (Conceptual)**  
Why should you create one Linked Service per source system rather than one Linked Service per pipeline? What is the maintenance benefit of this approach?

---

**Q14 (Scenario)**  
A pipeline reads from an ADLS Gen2 source container and writes to an ADLS Gen2 sink container — both in the same storage account. How many Linked Services and how many Datasets do you need? Explain your reasoning.

---

**Q15 (Conceptual)**  
What is an ADF Dataset parameter? Give a concrete example of a Dataset that uses a parameter, and show the expression syntax used in the folder path.

---

**Q16 (Tricky)**  
A Dataset is configured with path `bronze/payments/@{dataset().run_date}/payments_raw.parquet`. The pipeline passes `run_date = 2026-09-25`. What is the resolved path at runtime? What happens if the pipeline does not pass a value for `run_date`?

---

**Q17 (Scenario)**  
You have 15 pipelines, all writing to the same ADLS Gen2 account. The storage account access key is rotated for security compliance. How many places in ADF do you need to update? What ADF feature would eliminate this problem in a production environment?

---

**Q18 (Conceptual)**  
What is the difference between a **Parquet** Dataset and a **DelimitedText (CSV)** Dataset in ADF? In which layer of the Bronze/Silver/Gold architecture would you use each format, and why?

---

**Q19 (Tricky)**  
A data engineer creates a Dataset with the schema imported (column names and types defined in the Dataset). Later, the source system adds three new columns. What happens to the ADF pipeline — does it fail, skip the new columns, or include them automatically? How do you handle schema evolution in ADF?

---

**Q20 (Scenario)**  
You need to connect ADF to an Azure Key Vault to securely retrieve database passwords, ADLS access keys, and API tokens — instead of hardcoding them in Linked Services. Describe the two steps required to configure Key Vault integration in ADF, and what the Linked Service configuration looks like after integration.

---

## Concept 3: Copy Activity, DIUs, Fault Tolerance & Mixed Senior (Q21–Q30)

**Q21 (Warm-up)**  
What does a Copy Activity do in ADF? Name the four main configuration tabs in a Copy Activity and briefly describe what you configure in each one.

---

**Q22 (Warm-up)**  
What are DIUs (Data Integration Units) in ADF's Copy Activity? What does "Auto" DIU setting mean, and when would you override it with a manual value?

---

**Q23 (Conceptual)**  
What is the Mapping tab in a Copy Activity used for? Give three things you can configure there that go beyond simple column name matching.

---

**Q24 (Scenario)**  
You are copying 500 GB of CSV files from Azure Blob Storage to ADLS Gen2 as Parquet. The copy is taking 3 hours. Your manager wants it done in under 1 hour. What ADF settings would you change to improve throughput? Name at least two.

---

**Q25 (Conceptual)**  
What is fault tolerance in a Copy Activity? What happens by default when ADF encounters an incompatible row (e.g., a string value in a column expected to be an integer)? How do you configure ADF to skip bad rows and log them instead of failing the pipeline?

---

**Q26 (Scenario)**  
A Copy Activity reads 10 million rows from an HTTP CSV source and writes them to ADLS Gen2 Parquet. After the run, only 9,998,500 rows appear in the Parquet file. You have fault tolerance enabled with logging. What is your diagnostic process? Where do you look, and what do you check?

---

**Q27 (Tricky)**  
What is the difference between a **Copy Activity** and an ADF **Data Flow**? A data engineer needs to deduplicate records and join two tables during ingestion. Which activity should they use, and why? What is the performance difference?

---

**Q28 (Scenario)**  
You have a pipeline `pl_ingest_http_to_bronze` that runs daily at 6:00 AM. The pipeline must only start if a new source file exists at the HTTP endpoint. If no new file exists, the pipeline should exit gracefully without failing. How would you implement this check in ADF? What activities would you use?

---

**Q29 (Tricky)**  
What is the ADF expression `@pipeline().parameters.run_date` and where can you use it? Write the expression for the following scenarios:
- The sink folder path should include the run date: `bronze/payments/2026-09-25/`
- The pipeline should use `stadlsprod001` in prod and `stadlsdev001` in dev, controlled by an `env` parameter

---

**Q30 (System design)**  
Design a complete multi-source ingestion architecture in ADF for an EV charging company that needs to ingest data from:
- An HTTP API providing daily payment CSV files
- An on-premises SQL Server with session data (not internet accessible)
- An Azure Event Hubs stream of real-time charger telemetry

Requirements:
- All data lands in ADLS Gen2 Bronze layer, partitioned by date
- Failed ingestion sends a Teams/email alert
- The daily batch runs at 5:00 AM on a schedule
- The pipeline handles 20 payment tables (not just one) using a metadata-driven approach
- Each pipeline run is idempotent (re-running the same date does not create duplicate data)

For each source, specify: Linked Service type, Integration Runtime, Dataset format, activity type, trigger type, and how you handle failure alerting.
