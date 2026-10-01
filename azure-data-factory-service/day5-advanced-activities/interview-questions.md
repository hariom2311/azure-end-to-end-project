# Day 5 — Interview Questions: ADF Advanced Activities

> 30 questions on Mapping Data Flow, Databricks Notebook, Stored Procedure, Validation, and Azure Function activities.
> Attempt first, then check `interview-solutions.md`.

---

## Mapping Data Flow

**Q1 (Warm-up)**
What is the difference between a Copy Activity and a Mapping Data Flow? In the VoltGrid pipeline, which one would you use to ingest raw JSON from the API, and which one would you use to clean and type the data into Silver Parquet?

---

**Q2 (Conceptual)**
What does Data Flow Debug mode do? Why does it take 3–5 minutes to start? Can you run a pipeline with a Data Flow Activity without turning on debug mode?

---

**Q3 (Scenario)**
Your Bronze payments JSON has an `amount` field stored as a string (`"45.80"`). In the Silver layer, `amount_aud` must be a numeric `DOUBLE`. Which Mapping Data Flow transformation do you use and what is the exact expression?

---

**Q4 (Tricky)**
You add a Filter transformation in a Data Flow with the expression `equals(status, 'active')`. After running, you notice that rows with `status = 'Active'` (capital A) are also being filtered out. Why, and how do you fix it?

---

**Q5 (Conceptual)**
Name four Mapping Data Flow transformations and describe what each one does in one sentence. Give a VoltGrid-specific example for at least two of them.

---

**Q6 (Scenario)**
After adding a Data Flow Activity to the pipeline, the first Debug run takes 6 minutes even though the data is only 500 rows. A student asks "why is it so slow?" What do you tell them?

---

**Q7 (Tricky)**
A Data Flow has two sources: Bronze payments JSON and a small reference table from Azure SQL (lookup for payment method descriptions). The join produces no rows even though both sources have data. What are two likely causes?

---

**Q8 (Conceptual)**
What is the difference between the **Lookup** transformation inside a Mapping Data Flow and the **Lookup Activity** in a pipeline? When would you use each?

---

---

## Databricks Notebook Activity

**Q9 (Warm-up)**
What does the Databricks Notebook Activity do? What is the minimum setup required before you can add it to a pipeline?

---

**Q10 (Conceptual)**
You have a choice between Mapping Data Flow and Databricks Notebook Activity for the Silver layer transform. List two scenarios where you would choose Databricks over Data Flow.

---

**Q11 (Scenario)**
You pass `run_date = @variables('v_ingestion_date')` as a base parameter to a Databricks notebook. Inside the notebook, write the Python line that reads this parameter value.

---

**Q12 (Tricky)**
A Databricks Notebook Activity fails with the error "Cluster not found." The cluster ID is hardcoded in the linked service. What went wrong, and what is the recommended fix to avoid this in production?

---

**Q13 (Scenario)**
The Databricks Notebook Activity in Monitor shows status Succeeded with `runId: 12345`. But when you check the Silver container in ADLS, no Parquet file was written. What are two reasons this could happen, and where do you look to diagnose?

---

---

## Stored Procedure Activity

**Q14 (Warm-up)**
What does the Stored Procedure Activity do? What types of databases does it support?

---

**Q15 (Conceptual)**
Why would you use a Stored Procedure Activity to write an audit record instead of using a Copy Activity to insert a row into Azure SQL?

---

**Q16 (Scenario)**
You call `usp_log_pipeline_run` from ADF after the copy. The stored procedure has a parameter `@rows_copied INT`. You pass the value using:
```
@activity('act_copy_payments').output.rowsCopied
```
The ADF parameter type must be set to `Int32`. What happens if you set the type to `String` instead?

---

**Q17 (Tricky)**
The Stored Procedure Activity returns `returnCode: 0`. A student says "the procedure ran successfully and inserted the row." Another student says "returnCode 0 just means the procedure was called — it doesn't confirm the INSERT happened." Who is right and how would you verify the INSERT?

---

**Q18 (Scenario)**
You want to update a `watermark` table in Azure SQL with the last successful run timestamp after every pipeline run. The table has one row per pipeline name. Write the SQL logic (not ADF config) that would correctly upsert the watermark row.

---

---

## Validation Activity

**Q19 (Warm-up)**
What does the Validation Activity do? How does it differ from Get Metadata + If Condition for checking file existence?

---

**Q20 (Conceptual)**
The Validation Activity has three settings: Timeout, Sleep, and Minimum size. Explain what each controls and give a real-world value for each in the VoltGrid context.

---

**Q21 (Scenario)**
A Validation Activity has `Timeout = 0.00:05:00` and `Sleep = 60`. The Bronze file arrives 4 minutes and 30 seconds after the pipeline starts. Does the pipeline succeed or fail? Explain why.

---

**Q22 (Tricky)**
The Validation Activity succeeds (file found, size > minimum), but the next Mapping Data Flow fails because it reads 0 rows from the Bronze file. How is this possible and what is the root cause?

---

**Q23 (Scenario)**
You have two upstream teams: Team A drops `payments.json` and Team B drops `sessions.json`. Your pipeline must wait for both files before starting the Silver transform. Design the ADF activity flow to handle this — name every activity and its type.

---

---

## Azure Function Activity

**Q24 (Warm-up)**
What is the difference between a Web Activity and an Azure Function Activity? Give one scenario where you would use each.

---

**Q25 (Conceptual)**
How does ADF authenticate to an Azure Function when using the Azure Function Activity? Where should the function key be stored, and why?

---

**Q26 (Scenario)**
You want the Azure Function Activity to fire only when `act_copy_payments` fails. Which dependency condition do you use, and what colour is the dependency arrow in the ADF canvas?

---

**Q27 (Tricky)**
The Azure Function Activity in Monitor shows status Succeeded (HTTP 200). But no failure alert was received. What are two reasons the Function might have returned 200 without sending the alert, and where do you check?

---

**Q28 (Scenario)**
You want to send a Teams notification after every successful pipeline run (not just on failure). Describe the exact wiring: which activity does the Function Activity depend on, what dependency condition, and what body JSON would you send?

---

---

## Mixed / Senior

**Q29 (System design)**
Design a complete end-to-end daily pipeline for the VoltGrid payments endpoint that: (1) authenticates via Key Vault, (2) copies Bronze JSON to ADLS, (3) waits for the file to confirm it landed, (4) runs a Mapping Data Flow to produce Silver Parquet, (5) logs the run to Azure SQL, and (6) sends a Teams alert on failure via Azure Function. List every activity name, its type, and one key configuration setting.

---

**Q30 (Tricky)**
A student argues: "We can replace the Validation Activity with a Get Metadata + If Condition + Until loop — they both poll for a file." Is this true? What are the advantages and disadvantages of each approach?

---
