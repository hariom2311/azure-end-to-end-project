# Day 5 — Interview Questions: Remaining ADF Activities

> 30 questions on Lookup, Delete, Script, Wait, WebHook, Filter, ForEach, Switch, and Until.
> Attempt first, then check `interview-solutions.md`.

---

## Lookup

**Q1 (Warm-up)**
What does the Lookup Activity return? What is the difference between setting `First row only: Yes` vs `No`, and when would you use each?

---

**Q2 (Scenario)**
A Lookup Activity reads a JSON file with 5 rows. You write the expression `@activity('lkp').output.value[4].table_name`. What does index `[4]` return, and what happens if you write `[5]`?

---

**Q3 (Tricky)**
You use Lookup to read a watermark from an Azure SQL table: `SELECT MAX(updated_at) AS last_run FROM pipeline_audit`. The table is empty (no rows yet). What does `output.firstRow` contain, and how do you handle this in downstream expressions?

---

**Q4 (Conceptual)**
What is a metadata-driven pipeline? How does the Lookup Activity enable it? Give a concrete example with the tables.json config file.

---

## Delete

**Q5 (Warm-up)**
You add a Delete Activity pointing to a file that does not exist. Does the activity fail or succeed? What does the Output tab show?

---

**Q6 (Scenario)**
You want to delete all files inside the folder `bronze/landing/payments/` but keep the folder itself. Which Delete Activity setting do you configure and what value do you set?

---

**Q7 (Tricky)**
A colleague says "we can just overwrite files on the next run instead of deleting them — the Copy Activity will replace the existing file." What is the risk of this approach in ADLS Gen2, and when would Delete be the safer choice?

---

**Q8 (Conceptual)**
What is the purpose of the "Enable logging" setting on a Delete Activity? Where does the log go, and what does it contain?

---

## Script

**Q9 (Warm-up)**
What is the difference between Script Activity and Stored Procedure Activity? When would you choose Script over Stored Procedure?

---

**Q10 (Scenario)**
You use a Script Activity with type `Query` and the SQL:
```sql
SELECT COUNT(*) AS total FROM payments WHERE status = 'completed'
```
Write the ADF expression to reference the count value in a downstream activity.

---

**Q11 (Tricky)**
You write a Script Activity with this SQL using dynamic content:
```
TRUNCATE TABLE @{variables('v_schema')}.payments
```
A student says this will cause a SQL injection risk. Are they right? What is the safer approach?

---

**Q12 (Conceptual)**
Can a Script Activity return a result that the next activity uses? If yes, show the expression syntax. If no, explain what to use instead.

---

## Wait

**Q13 (Warm-up)**
What does the Wait Activity do? What is the maximum wait time allowed in ADF?

---

**Q14 (Scenario)**
You place a 60-second Wait inside a ForEach that loops over 10 items in parallel (batch count = 10). How much total wait time does this add to the pipeline? Explain your answer.

---

**Q15 (Tricky)**
A colleague suggests: "instead of a Wait, we should use a Until loop that checks a condition every 5 seconds." For simple rate limiting (pause N seconds between calls), which is better and why?

---

## WebHook

**Q16 (Warm-up)**
Explain the difference between Web Activity and WebHook Activity. Which one pauses the pipeline while waiting for a response?

---

**Q17 (Scenario)**
A WebHook Activity has `Timeout = 0.00:30:00`. The external service takes 35 minutes to complete and send the callback. What happens to the pipeline?

---

**Q18 (Tricky)**
In the WebHook Activity body, you reference `@activity('act_webhook').output.callBackUri`. Why must this be included in the body, and what happens if you forget to include it?

---

**Q19 (Conceptual)**
An external service POSTs back to the ADF callback URL with `{ "statusCode": "500", "error": "processing failed" }`. What does the WebHook Activity report in Monitor?

---

## Filter

**Q20 (Warm-up)**
What does the Filter Activity do? What does `@activity('act_filter').output.filterCount` return?

---

**Q21 (Scenario)**
A Lookup returns 8 rows. A Filter with condition `@greater(item().size, 1000000)` is applied. After filtering, `filterCount = 3`. How many items does the downstream ForEach process?

---

**Q22 (Tricky)**
You apply a Filter with condition `@equals(item().status, 'active')`. Two rows have `status = 'Active'` (capital A). Are these rows kept or dropped? What is the fix?

---

## ForEach

**Q23 (Warm-up)**
What is the difference between `Is Sequential: true` and `Is Sequential: false` in a ForEach Activity? What is the maximum batch count allowed?

---

**Q24 (Scenario)**
A ForEach iterates over `["payments", "sessions", "customers"]`. Inside the ForEach, a Web Activity uses the expression `@item()`. What does `@item()` return during the second iteration?

---

**Q25 (Tricky)**
A ForEach with `Is Sequential: false, Batch count: 5` contains an Append Variable activity that appends `@item().table` to `v_processed`. After the ForEach, `v_processed` has only 3 items instead of 5. What likely happened and why?

---

**Q26 (Conceptual)**
Can you nest a ForEach inside another ForEach in ADF? What limitation must you be aware of?

---

## Switch

**Q27 (Warm-up)**
What is the difference between If Condition and Switch Activity? Give a scenario where Switch is clearly the better choice.

---

**Q28 (Scenario)**
A Switch Activity has cases for `"dev"`, `"staging"`, and `"prod"`. The pipeline is triggered with `p_env = "Prod"` (capital P). Which branch runs?

---

**Q29 (Tricky)**
You have a Switch with 3 cases and a Default. The expression evaluates to `"dev"`. Can the Default branch also run in the same execution? Explain.

---

## Until

**Q30 (Scenario + Tricky)**
An Until loop has expression `@greaterOrEquals(int(variables('v_count')), 5)`. The inner canvas has only one Set Variable: `v_count = @string(add(int(variables('v_count')), 1))`. A student says this will fail at runtime. Are they right, and if so, why? Write the correct implementation using two variables.

---
