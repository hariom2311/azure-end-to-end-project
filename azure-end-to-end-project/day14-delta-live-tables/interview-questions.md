# Day 14 — Interview Questions: Delta Live Tables

> 12 questions covering DLT architecture, expectations, streaming vs batch tables, pipeline modes, and comparisons with manual Structured Streaming.
> Attempt each question before checking `interview-solutions.md`.

---

## DLT Fundamentals

**Q1 (Warm-up)**
What is Delta Live Tables? What problem does it solve compared to writing Spark Structured Streaming pipelines manually?

---

**Q2 (Conceptual)**
What is the difference between a DLT **streaming table**, a **materialized view**, and a **view**? Give one example of when you would use each in the EV charging pipeline.

---

**Q3 (Conceptual)**
What is the difference between `dlt.read_stream()` and `dlt.read()` inside a DLT table function? When would you use each?

---

## Expectations and Data Quality

**Q4 (Warm-up)**
What are the three DLT expectation violation modes? What happens to the bad row and to the pipeline in each case?

---

**Q5 (Scenario)**
In this project, the `valid_battery_pct` rule uses `@dlt.expect_or_drop` but the `not_corrupt` rule uses `@dlt.expect`. Why is `expect_or_drop` wrong for `not_corrupt`, and why is `expect` the right choice?

---

**Q6 (Tricky)**
A DLT pipeline passes all expectations but the Silver table row count is lower than the Bronze row count. No rows failed any expectation. What are three possible explanations?

---

## Pipeline Modes and Performance

**Q7 (Conceptual)**
What is the difference between Triggered mode and Continuous mode in DLT? For the EV charging pipeline, which mode would you choose and why?

---

**Q8 (Scenario)**
Your DLT pipeline is in Triggered mode and scheduled to run every hour. At 09:00 it starts and takes 55 minutes to process the backlog. At 10:00 the scheduler fires again but the first run is still going. What happens?

---

## DLT vs Manual Streaming

**Q9 (Comparison)**
Compare the Day 13 `foreachBatch` Silver transform with the Day 14 DLT Silver table across these dimensions: checkpointing, DQ enforcement, lineage, retries, schema evolution.

---

**Q10 (Scenario)**
A data engineer argues: "DLT is just a wrapper around Structured Streaming — it doesn't add any real value, it just hides the code." Name three things DLT provides that you cannot easily replicate with manual Structured Streaming code.

---

## Project-Specific Questions

**Q11 (Design)**
The Gold `gold_charging_progress_live` table uses a 5-minute tumbling window. A product manager asks for the same aggregation but with a 5-minute **sliding** window (new window every 1 minute). What change do you make in the DLT code and what is the storage and compute cost implication?

---

**Q12 (Advanced)**
The `gold_daily_station_energy` table uses `dlt.read()` (batch snapshot). A new requirement says this table must reflect data from the last 30 minutes only, not the full history. What is wrong with using `dlt.read()` for this, and how would you fix it?
