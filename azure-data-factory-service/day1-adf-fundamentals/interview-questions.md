# Day 1 — Interview Questions: ADF Introduction & Terminologies

> 30 questions across all 3 concepts. Attempt your own answer first, then check `interview-solutions.md`.

---

## Concept 1: What is ADF

**Q1 (Warm-up)**
What is Azure Data Factory? What problem does it solve that you cannot solve with a cron job and a Python script?

---

**Q2 (Warm-up)**
What is the difference between ETL and ELT? In a modern Azure data lake using ADF + Databricks, which pattern is preferred and why?

---

**Q3 (Conceptual)**
ADF Studio has 4 panels: Author, Monitor, Manage, and Learn. What do you do in each panel? Which panel would you go to if a pipeline failed at 3am?

---

**Q4 (Scenario)**
A data engineer argues: "We don't need ADF — I can write a Python script with `requests` and schedule it with cron." Give three specific scenarios where ADF is the better choice over a Python script.

---

**Q5 (Conceptual)**
What is an Integration Runtime (IR) in ADF? What is the difference between the Azure IR and a Self-Hosted IR? When would you need to install a Self-Hosted IR?

---

**Q6 (Tricky)**
ADF has a concept of "Publish All". What does publishing do? What is the difference between saving in ADF Studio (with Git connected) and publishing?

---

**Q7 (Scenario)**
Your company has 5 data engineers all working on the same ADF instance. They frequently overwrite each other's pipeline changes. What ADF feature would you enable to prevent this, and how does it solve the problem?

---

**Q8 (Conceptual)**
What are Data Integration Units (DIUs) in ADF? If a Copy Activity is running slowly on a 200 GB dataset, what would you change and what is the trade-off?

---

**Q9 (Warm-up)**
What is fault tolerance in the ADF Copy Activity? What happens to rows that fail the fault tolerance check?

---

**Q10 (Tricky)**
You click "Debug" on a pipeline and it succeeds. You then close ADF Studio without clicking "Publish All". Your colleague runs a scheduled trigger the next morning. Does the trigger run your latest changes or the previous version? Explain why.

---

## Concept 2: ADF Terminologies

**Q11 (Warm-up)**
What is a Linked Service in ADF? Why should you create one Linked Service per source system rather than one per pipeline?

---

**Q12 (Warm-up)**
What is the difference between a Linked Service and a Dataset in ADF? Can two Datasets share the same Linked Service?

---

**Q13 (Conceptual)**
Name four activity types available in ADF and describe what each one does. Include at least one from each category: Data Movement, Data Transformation, and Control Flow.

---

**Q14 (Scenario)**
You are building a pipeline that reads from a REST API that requires a Bearer token. The token is obtained by calling `POST /api/auth/login/` with username and password. Describe exactly which ADF activities and in what order you would use to implement this.

---

**Q15 (Conceptual)**
What is an ADF Pipeline Parameter? How does it differ from an ADF Dataset Parameter? Give an example of when you need both.

---

**Q16 (Tricky)**
Write the ADF expression to:
1. Build the path `bronze/payments/2026-01-15/` dynamically from a `run_date` pipeline parameter
2. Get today's date as a string in `yyyy-MM-dd` format

---

**Q17 (Scenario)**
A pipeline has two activities connected with an "On Success" dependency. Activity A (Copy) succeeds but copies 0 rows because the source returned an empty response. Activity B (Databricks transform) then runs on an empty dataset and fails. How would you prevent Activity B from running when Activity A produces 0 rows?

---

**Q18 (Conceptual)**
What is the difference between the **Copy Activity** and a **Data Flow** in ADF? When would you use each one?

---

**Q19 (Scenario)**
Your ADF pipeline needs to process one file per day dropped into `bronze/landing/` by an external team — sometimes at 6am, sometimes at noon, sometimes never. How would you design the trigger and any waiting logic for this pipeline?

---

**Q20 (Tricky)**
What is the ForEach Activity in ADF? What is the difference between running it in Sequential vs. Batch mode? Give a real data engineering example of using ForEach.

---

## Concept 3: Hands-on / Mixed Senior

**Q21 (Scenario)**
You have a REST API that returns paginated data — each response has 100 records and a `next_page` URL. ADF's Copy Activity calls the API once and gets only the first 100 records. How would you fix this to get all pages?

---

**Q22 (Conceptual)**
What is ADF Git integration? How does the branch model work — what is the collaboration branch vs. the publish branch?

---

**Q23 (Scenario)**
Your ADF pipeline calls `POST /api/auth/login/` and stores the token using a Web Activity. The token expires after 1 hour. Your pipeline runs 50 ForEach iterations — each one calls a different API endpoint. The pipeline takes 90 minutes. What problem do you have and how would you fix it?

---

**Q24 (Tricky)**
ADF has three trigger types: Schedule, Tumbling Window, and Storage Event. A pipeline loads daily sales data and must have complete data for every day — if Monday fails, Monday must be automatically retried even after Tuesday runs. Which trigger type would you use and why?

---

**Q25 (Scenario)**
A Copy Activity fails with: `"SqlException: The INSERT statement conflicted with the FOREIGN KEY constraint"`. Looking at the Monitor panel, what information would you use to diagnose this, and where would you find the exact error row?

---

**Q26 (Conceptual)**
What authentication method should a production ADF pipeline use to access ADLS Gen2? Why is using an Account key a bad practice in production?

---

**Q27 (Scenario)**
A metadata-driven pipeline uses a Lookup Activity to read 50 table names, then a ForEach to process each with a Copy Activity. The total pipeline runs for 4 hours. A stakeholder asks you to reduce it to under 1 hour. What ADF settings would you change?

---

**Q28 (Tricky)**
What is the Web Activity in ADF used for? Give two real production use cases beyond just "calling an API to get data".

---

**Q29 (Scenario)**
You are handed an ADF factory with 200 pipelines and 80 linked services, all pointing to the same Azure SQL Database with hardcoded credentials. The DBA rotates the database password. What is the blast radius, and how would you redesign this to prevent it in future?

---

**Q30 (System design)**
Design a complete ADF solution for the VoltGrid EV platform nightly ingestion:

**API:**
- `POST /api/auth/login/` — returns a token (valid 1 hour)
- `GET /api/db/payments/` — paginated, 100 records/page
- `GET /api/db/sessions/` — paginated, 100 records/page
- `GET /api/db/customers/` — paginated, 100 records/page
- `GET /api/db/vehicles/` — paginated, 100 records/page
- `GET /api/db/stations/` — paginated, 100 records/page

**Requirements:**
- Run every night at 1:00 AM UTC
- All 5 data endpoints must complete within 2 hours
- If any individual endpoint fails, others must continue (no cascading failure)
- Failed endpoints must be retried once before alerting
- A Teams webhook alert must fire if more than 2 endpoints fail
- Data must land in ADLS Gen2 `bronze/{table_name}/{run_date}/` as JSON
- Adding a 6th endpoint should require minimal pipeline changes

For each requirement, name the specific ADF component and configuration you would use.
