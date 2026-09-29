# Day 4 — Interview Questions: ADF Activities

> 30 questions on General and Iteration & Conditionals activities. Attempt first, then check `interview-solutions.md`.

---

## General Activities

**Q1 (Warm-up)**
What is the difference between a Web Activity and a Copy Activity in ADF? In the `pl_bronze_api_payments` pipeline, which activities are Web Activities and what does each one do?

---

**Q2 (Warm-up)**
What is the difference between Set Variable and Append Variable? When would you use Append Variable over Set Variable?

---

**Q3 (Conceptual)**
The Get Metadata activity can return several fields. Name four of them and give a real use case for each in a data pipeline.

---

**Q4 (Scenario)**
You run `act_copy_payments` and it succeeds with `rowsCopied: 100`. You then run Get Metadata on the Bronze output folder and it returns `exists: true, childItems: []` (empty folder). How is this possible and what does it tell you?

---

**Q5 (Tricky)**
You have a Web Activity that calls `POST /api/auth/login/` and returns `{"token": "abc123", "expires_in": 3600}`. Write the expressions to extract: (a) the token value, (b) the expiry in minutes.

---

**Q6 (Conceptual)**
What is the Execute Pipeline activity? What is the difference between `Wait on completion: Yes` vs `No`? Give a real scenario where you would use each.

---

**Q7 (Scenario)**
A pipeline runs every night. After copying payments to Bronze, you need to verify the file is present and larger than 1KB before triggering a Databricks job. Which two activities would you chain together to implement this check?

---

**Q8 (Tricky)**
You use a Delete Activity to clean up the landing zone after ingestion. A colleague says "just overwrite the file next time — no need to delete." What is wrong with this argument in an ADLS Gen2 context?

---

**Q9 (Conceptual)**
What does the Lookup Activity return? What is the difference between `First row only: Yes` vs `No`, and when would you use each?

---

**Q10 (Scenario)**
You have a Lookup Activity that reads a config file with 5 endpoint configs. You reference `@activity('lkp').output.value[2].endpoint` in a downstream expression. What does index `[2]` return, and what happens if the config file only has 2 rows?

---

**Q11 (Warm-up)**
When would you use the Fail Activity instead of just letting the pipeline fail naturally? What advantage does it give you in the Monitor panel?

---

**Q12 (Tricky)**
You add a Wait Activity with 300 seconds between two Copy Activities to respect a rate limit. The pipeline has a default 12-hour timeout. Is there any risk? What is the maximum Wait time ADF allows?

---

**Q13 (Scenario)**
A pipeline copies data from 3 SQL tables to Bronze using 3 Copy Activities chained in sequence. The team asks you to make it run faster without changing the Copy Activities themselves. What change do you make and what activity replaces the sequential chain?

---

## Iteration & Conditionals Activities

**Q14 (Warm-up)**
What is the If Condition activity? What must the expression evaluate to? Can the False branch be left empty?

---

**Q15 (Scenario)**
Write the If Condition expression to check if a pipeline parameter `p_load_type` equals `"full"` (case-sensitive). What happens if someone passes `"Full"` with a capital F?

---

**Q16 (Conceptual)**
What is the difference between If Condition and Switch Activity? Give an example of when Switch is clearly better than using nested If Conditions.

---

**Q17 (Warm-up)**
What is the ForEach Activity? What is the difference between Sequential and Batch mode? What is the maximum batch count?

---

**Q18 (Scenario)**
A Lookup Activity returns 8 endpoint configs. A ForEach with `batch count = 3` processes them. Draw what the execution looks like — which items run in which batch?

---

**Q19 (Tricky)**
Inside a ForEach, you reference `@item().endpoint`. The ForEach is iterating over:
```json
[{"endpoint": "/payments/"}, {"endpoint": "/sessions/"}]
```
What does `@item()` return during the second iteration?

---

**Q20 (Conceptual)**
What is the Until Activity? How does it differ from ForEach? Give the exact scenario where you must use Until instead of ForEach.

---

**Q21 (Tricky)**
Why does ADF prohibit `v_counter = @add(variables('v_counter'), 1)` inside an Until loop? Write the correct two-activity workaround.

---

**Q22 (Scenario)**
An Until loop runs and never stops — the pipeline runs for hours until the timeout kills it. What are two likely causes and how do you debug each?

---

**Q23 (Conceptual)**
What does the Filter Activity do? What does it return? How is it different from adding a WHERE clause in a Lookup query?

---

**Q24 (Scenario)**
A Lookup returns 10 files with their sizes. You only want to process files larger than 50MB. Write the Filter Activity condition expression, and then show how you pass the result to a ForEach.

---

**Q25 (Tricky)**
You have a ForEach with `Is Sequential: false, Batch count: 5`. Inside the ForEach, you have an Append Variable activity that adds `@item().name` to `v_processed_names`. After the ForEach, `v_processed_names` has only 3 items even though the ForEach processed 5 items. What likely happened?

---

## Senior / Mixed

**Q26 (Scenario)**
Design a pipeline that: (1) reads an endpoint config from ADLS Gen2, (2) filters only active endpoints, (3) for each active endpoint, copies data to Bronze using the auth token from Key Vault. Name every activity and its type.

---

**Q27 (Tricky)**
A ForEach has `Is Sequential: false, Batch count: 50`. The inner Copy Activity calls the VoltGrid API. After running, you see 429 Too Many Requests errors in 30 of the 50 Copy Activities. What is the root cause and what are two ways to fix it?

---

**Q28 (Scenario)**
You need to paginate through a REST API that returns `total_pages` in the first response. You don't know total_pages until the pipeline starts. Which activity combination would you use and why can't ForEach do this alone?

---

**Q29 (Tricky)**
An Execute Pipeline activity calls a child pipeline with `Wait on completion: Yes`. The child pipeline has an Until loop that runs for 3 hours. The parent pipeline has a 1-hour timeout. What happens?

---

**Q30 (System design)**
The VoltGrid platform has 5 endpoints. Design a complete pipeline `pl_bronze_all_endpoints` that: reads endpoint config from a JSON file, authenticates once (shared token), loops over all endpoints in parallel, checks each endpoint returned > 0 rows, logs the row count per endpoint to an array variable, and sends a Teams alert if any endpoint returns 0 rows. List every activity, its type, and the key expression/setting for each.
