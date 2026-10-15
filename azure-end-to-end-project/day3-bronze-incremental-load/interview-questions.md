# Day 3 — Interview Questions: Unity Catalog Setup + Control Table

> 15 questions covering Access Connectors, Unity Catalog credential chain, External Delta tables, control table design, and Databricks notebooks as ADF integration points.
> Attempt first, then check `interview-solutions.md`.

---

## Unity Catalog and Access Connector

**Q1 (Warm-up)**
What is an Azure Databricks Access Connector? How is it different from a Service Principal, and why is it preferred for giving Databricks access to ADLS Gen2?

---

**Q2 (Conceptual)**
Explain the three-layer Unity Catalog chain: Access Connector → Storage Credential → External Location. What does each layer do, and what would break if you removed the External Location?

---

**Q3 (Scenario)**
A data engineer creates the External Location `ext-loc-ev-bronze` pointing to `abfss://bronze@evdatalakedev2.dfs.core.windows.net/`. The test connection shows ✅ Read, ✅ List but ❌ Write. What is the most likely cause and how do they fix it?

---

**Q4 (Conceptual)**
After creating the External Location, a Databricks notebook reads from `abfss://bronze@evdatalakedev2.dfs.core.windows.net/api/payments/` with no `spark.conf.set` and no storage access key. How does the read succeed — what injects the credentials automatically?

---

**Q5 (Tricky)**
The External Location test connection shows ❌ File Events Resource Provision and ❌ File Events Resource Teardown alongside ✅ Read/Write. Should the engineer block the pipeline until these are fixed? What are File Events used for and why are they not critical for this project?

---

## Delta Control Table

**Q6 (Warm-up)**
What is the difference between a Managed Delta table and an External Delta table? This project creates the control table with `LOCATION 'abfss://...'`. What does that make it, and what happens to the data if the table is dropped from Unity Catalog?

---

**Q7 (Conceptual)**
The control table seeds `last_watermark` with `'1900-01-01T00:00:00Z'`. Why is the epoch year used instead of `null` or today's date? What would break if `null` was inserted and ADF tried to pass it into the API URL?

---

**Q8 (Scenario)**
The engineer runs the `CREATE TABLE` statement but gets: `DeltaAnalysisException: Location 'abfss://bronze@evdatalakedev2.dfs.core.windows.net/ctl/pipeline_control' already exists`. The path has files from a previous failed attempt. What are the two options to resolve this, and which is safer?

---

**Q9 (Conceptual)**
The control table has an `is_active` column. A data engineer asks: "Why not just delete the row when a source is deactivated?" Give two specific reasons why soft-delete (`is_active = false`) is better than hard delete for a control table.

---

**Q10 (Tricky)**
The `pipeline_control` table has no `DEFAULT` clauses — all NOT NULL columns require explicit values at insert time. What is the operational consequence of this design, and when would adding `DEFAULT` clauses be appropriate vs. when would it be a risk?

---

## Databricks Notebooks as ADF Integration Points

**Q11 (Warm-up)**
The notebook `nb_read_control_table` ends with `dbutils.notebook.exit(json.dumps(result))`. What does this do, and how does ADF access the value returned by this call?

---

**Q12 (Conceptual)**
Why does `nb_read_control_table` return a JSON-serialised string rather than returning individual values? Why can't the notebook just `print()` the values for ADF to pick up?

---

**Q13 (Scenario)**
An ADF pipeline calls `nb_read_control_table`. The activity succeeds but the downstream Set Variable activity that reads `activity('act_read_ctl').output.runOutput` returns an empty string. What are two possible causes in the notebook itself?

---

**Q14 (Conceptual)**
`nb_update_watermark` updates both `last_watermark` and `load_type` in a single SQL UPDATE. Why is `load_type` also updated to `'incremental'` here rather than being left for ADF to manage via a parameter?

---

**Q15 (Tricky)**
After running `nb_update_watermark` to test it manually, the engineer must reset the control table before Day 4. Why is this reset necessary — what would happen in Day 4 if the watermark was left at today's date from the manual test?

---
