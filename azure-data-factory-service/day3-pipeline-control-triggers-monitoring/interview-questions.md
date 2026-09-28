# Day 3 — Interview Questions: Pipeline Control, Triggers & Monitoring

> 30 questions on ADF variables, parameters, system variables, dynamic content functions, triggers, Integration Runtime, Monitor, and alerts. Attempt first, then check `interview-solutions.md`.

---

## Concept 1: Variables, Parameters, and Dynamic Content

**Q1 (Warm-up)**
What is the difference between a Pipeline Parameter and a Pipeline Variable in ADF? Give one real example of each from the `pl_bronze_api_payments` pipeline.

---

**Q2 (Warm-up)**
Write the ADF expression to build this Bronze sink path dynamically:
```
api/payments/ingestion_date=2026-09-28/page_1.json
```
Assume the date comes from a variable `v_ingestion_date` and the page number from a pipeline parameter `p_page`.

---

**Q3 (Conceptual)**
What are ADF system variables? Name three and give one practical use case for each.

---

**Q4 (Tricky)**
What is the difference between `@trigger().scheduledTime` and `@utcnow()`? Which should you use in the Bronze sink path, and why does it matter?

---

**Q5 (Scenario)**
A pipeline runs at 01:00 UTC but is delayed due to high load and actually starts executing at 01:47 UTC. The sink path uses `@formatDateTime(utcnow(), 'yyyy-MM-dd')`. Is there a risk? What is the correct expression to use instead?

---

**Q6 (Conceptual)**
What are the two syntax forms for ADF dynamic content expressions? Write an example of each using the VoltGrid token.

---

**Q7 (Tricky)**
Why does ADF prohibit this Set Variable expression inside an Until loop?
```
v_current_page = @add(variables('v_current_page'), 1)
```
What is the correct workaround?

---

**Q8 (Scenario)**
Write the ADF expression for the login Web Activity body that reads username and password from two upstream Web Activity outputs and builds a JSON string. The activity names are `act_get_username` and `act_get_password`.

---

**Q9 (Conceptual)**
What does the `@if()` function do in ADF? Write an expression that sets a watermark to `'1900-01-01T00:00:00Z'` for a full load and to the value of `p_watermark` parameter for an incremental load.

---

**Q10 (Tricky)**
In ADF, a Pipeline Parameter has a default value of `100` for `p_page_size`. A Schedule Trigger is created with `p_page_size = 50`. A developer clicks "Trigger Now" without specifying `p_page_size`. What value does the pipeline receive?

---

## Concept 2: Triggers

**Q11 (Warm-up)**
What is the difference between a Schedule Trigger and a Tumbling Window Trigger in ADF? In what scenario would you always use Tumbling Window over Schedule?

---

**Q12 (Conceptual)**
The `pl_bronze_api_payments` pipeline has a Schedule Trigger at 01:00 UTC daily. The pipeline fails on Monday night. On Tuesday, the trigger fires and runs Tuesday's data. What happened to Monday's data, and how would you fix this architecturally?

---

**Q13 (Tricky)**
You create a Tumbling Window Trigger with start time `2026-09-01T00:00:00Z` and recurrence of 1 day. Today is 2026-09-28. You activate it now. What happens immediately?

---

**Q14 (Scenario)**
A data partner drops a CSV file into `bronze/landing/payments/` every day, but the delivery time varies from 6am to 4pm. You need the pipeline to process the file within 5 minutes of arrival. Which trigger type would you use and what are its key configuration fields?

---

**Q15 (Conceptual)**
What are the two Tumbling Window system variables? How do you use them to correctly partition Bronze data by the scheduled date rather than the actual execution date?

---

**Q16 (Tricky)**
A Tumbling Window Trigger with `max concurrency = 1` and start date 7 days ago is activated. The pipeline takes 20 minutes to run. How long before all 7 windows complete? What happens if you set max concurrency to 7?

---

**Q17 (Scenario)**
You have a pipeline that must run every 6 hours. On weekdays it should fetch 100 records per page; on weekends it should fetch 500 records per page (API traffic is lower). Can you achieve this with a single trigger? If not, what is the recommended design?

---

**Q18 (Conceptual)**
What is a Storage Event Trigger? Give a real use case where it is clearly better than any schedule-based trigger.

---

**Q19 (Tricky)**
You connect a Schedule Trigger to `pl_bronze_api_payments`. A colleague connects a second Schedule Trigger (different timing) to the same pipeline. Both triggers are active. Is this allowed? What happens if both fire at the same time?

---

**Q20 (Scenario)**
A Tumbling Window Trigger has retry count = 2, retry interval = 30 minutes. The pipeline fails. Walk through the exact sequence of events ADF takes before finally marking the window as Failed.

---

## Concept 3: Integration Runtime, Monitor & Alerts

**Q21 (Warm-up)**
What is an Integration Runtime in ADF? Why does the VoltGrid project not need a Self-Hosted IR?

---

**Q22 (Conceptual)**
When would you pin the Azure IR to a specific Azure region instead of using AutoResolve? What is the risk of leaving it on AutoResolve when your ADLS Gen2 is in `Central India`?

---

**Q23 (Scenario)**
Your company has a SQL Server 2019 database on-premises behind a firewall with no public endpoint. You need to copy its tables to ADLS Gen2 nightly. What Integration Runtime do you need and what are the installation steps at a high level?

---

**Q24 (Conceptual)**
In the ADF Monitor panel, what information is available in the **Input** tab vs. the **Output** tab of a failed Copy Activity? Which tab do you check first to diagnose a 401 error?

---

**Q25 (Scenario)**
A pipeline run shows `act_copy_payments` succeeded with `rowsRead: 100` but `rowsCopied: 0`. What are the two most likely causes and how do you diagnose each using the Monitor panel?

---

**Q26 (Tricky)**
What is the difference between Monitor → Pipeline runs vs. Monitor → Trigger runs? A Tumbling Window Trigger fires but no pipeline run appears in Pipeline runs. What likely happened?

---

**Q27 (Conceptual)**
What are the two methods to send an email alert when an ADF pipeline fails? Compare them on: setup effort, scope, and what information the alert contains.

---

**Q28 (Scenario)**
You set up an Azure Monitor alert for `Failed pipeline runs count > 0`. A pipeline fails at 01:00 UTC. At what time would you expect to receive the email? What factors could delay it?

---

**Q29 (Tricky)**
In the pipeline, you add a Web Activity `act_alert_failure` with an **On Failure** dependency on `act_copy_payments`. The `act_copy_payments` fails. Will the overall pipeline run status show as Succeeded or Failed? Will `act_alert_failure` appear as Succeeded?

---

**Q30 (System design)**
Design the complete operational setup for the `pl_bronze_api_payments` pipeline going into production:
- Trigger: nightly at 01:00 UTC, guarantees every night is ingested even if a run fails
- Dynamic sink path: partitioned by the scheduled date (not wall-clock time)
- Alert: email within 5 minutes of any failure
- Alert: Teams message with pipeline name and run ID when Copy Activity specifically fails
- IR: VoltGrid API is on the public internet, ADLS Gen2 is in Central India
- Retry: pipeline retries 2 times before alerting

For each requirement, name the exact ADF component, configuration value, and expression (if applicable).
