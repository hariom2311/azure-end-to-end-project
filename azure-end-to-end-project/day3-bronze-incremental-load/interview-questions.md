# Day 3 — Interview Questions: Bronze Incremental Load

> 25 questions covering Unity Catalog External Locations, Access Connectors, Delta control tables, ADF Until loops, incremental watermarks, idempotency, pagination design, and file naming conventions.
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
A data engineer creates the External Location `ext-loc-ev-bronze` pointing to `abfss://bronze@evdatalakedev.dfs.core.windows.net/`. The test connection shows ✅ Read, ✅ List but ❌ Write. What is the most likely cause and how do they fix it?

---

**Q4 (Conceptual)**
After creating the External Location, a Databricks notebook reads from `abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/` with no `spark.conf.set` and no storage access key. How does the read succeed — what injects the credentials automatically?

---

**Q5 (Tricky)**
The External Location test connection shows ❌ File Events Resource Provision and ❌ File Events Resource Teardown alongside ✅ Read/Write. Should the engineer block the pipeline until these are fixed? What are File Events used for and why are they not critical for this day's work?

---

## Delta Control Table

**Q6 (Warm-up)**
What is the difference between a Managed Delta table and an External Delta table? This project creates the control table with `LOCATION 'abfss://...'`. What does that make it, and what happens to the data if the table is dropped from Unity Catalog?

---

**Q7 (Conceptual)**
The control table has a `last_watermark` column initialised to `1900-01-01T00:00:00Z`. Why is the epoch (year 1900) used instead of `null` or today's date? What would break if `null` was used and ADF tried to pass it into the API URL?

---

**Q8 (Scenario)**
ADF's notebook activity `act_read_ctl` returns a JSON string via `dbutils.notebook.exit()`. In the next ADF Set Variable activity, the engineer uses:
```
@activity('act_read_ctl').output.runOutput.endpoint
```
But it returns `null`. What is wrong and what is the correct expression?

---

**Q9 (Conceptual)**
The control table has an `is_active` column. A data engineer asks: "Why not just delete the row when a source is deactivated?" Give two specific reasons why soft-delete (`is_active = false`) is better than hard delete for a control table.

---

**Q10 (Tricky)**
After a successful full load, the notebook `nb_update_watermark` sets `last_watermark = v_run_ts` where `v_run_ts = utcnow()` captured at pipeline start. A data engineer argues: "We should use `utcnow()` at the END of the pipeline, not the start." Who is right and why?

---

## Pagination and Until Loop

**Q11 (Warm-up)**
The API returns `pagination.total_pages = 321` for a page_size of 1000. This means the full payments dataset has approximately how many records? How is `total_pages` used to control the ADF Until loop?

---

**Q12 (Conceptual)**
In ADF, a Set Variable activity inside an Until loop tries to increment `v_current_page` using:
```
@add(variables('v_current_page'), 1)
```
But after 3 loop iterations `v_current_page` is still `2`. What is wrong?

---

**Q13 (Scenario)**
An Until loop runs page 1 through page 5 correctly, but on page 6 the API returns a 429 (Too Many Requests) error. The Copy Activity fails. What happens to the pipeline? What should the retry policy on the Copy Activity be set to, and at what level?

---

**Q14 (Conceptual)**
Why is page 1 fetched separately as a Web Activity (`act_fetch_page1_meta`) before the Until loop, rather than including it inside the loop with all other pages?

---

**Q15 (Tricky)**
The Until loop exit condition is:
```
@greaterOrEquals(variables('v_current_page'), add(variables('v_total_pages'), 1))
```
Why is `add(variables('v_total_pages'), 1)` used instead of just `variables('v_total_pages')`? What would happen if the condition was `@equals(variables('v_current_page'), variables('v_total_pages'))`?

---

## Incremental Load and Idempotency

**Q16 (Warm-up)**
What does "incremental load" mean in this context? What is the `updated_after` parameter used for, and where does its value come from on the second pipeline run?

---

**Q17 (Conceptual)**
What does "idempotent" mean for a Bronze ingestion pipeline? Give a concrete example of how running `pl_bronze_incremental_load` twice on the same day produces the same result in ADLS.

---

**Q18 (Scenario)**
A pipeline runs at 02:00 UTC on 2026-10-15 and loads 5 pages of payments. At 03:00 UTC the pipeline is re-triggered because someone thought it failed. It loads the same 5 pages again with the same filenames. Is there a data problem? Why or why not?

---

**Q19 (Conceptual)**
The `p_load_type_override = 'full'` parameter forces a full reload without changing the control table's `last_watermark` before the run. How does the pipeline achieve this — trace through the expression in `act_set_watermark`?

---

**Q20 (Tricky)**
A full load runs for 321 pages. On page 200 the ADF pipeline fails. The engineer fixes the issue and reruns. The rerun starts from page 1 again — not page 200. Is this a problem for data quality in Bronze? Why or why not, and what would happen downstream in Silver?

---

## File Naming and Path Design

**Q21 (Warm-up)**
The file naming convention is `<file_prefix>_page_NNN.json` — e.g. `payments_page_042.json`. Why is the page number zero-padded to 3 digits? What problem occurs if it is not zero-padded?

---

**Q22 (Conceptual)**
All configuration values — endpoint, sink path, file prefix, page size — come from the control table instead of being hardcoded in ADF parameters or dataset definitions. Name 2 operational benefits this provides.

---

**Q23 (Scenario)**
A data engineer wants to add sessions ingestion. The sessions endpoint is `/api/db/sessions/` and the data should land at `bronze/api/sessions/`. What is the complete change needed to support this — ADF changes, Databricks changes, and control table changes?

---

**Q24 (Conceptual)**
The sink path uses `ingestion_date=YYYY-MM-DD` as a folder name rather than just `YYYY-MM-DD`. Why is the `ingestion_date=` prefix included — what is this pattern called and what tool automatically uses it?

---

**Q25 (System design)**
The VoltGrid payments dataset has 320,000+ records with page_size=1000, giving ~320 pages. A full load takes ~25 minutes. An engineer proposes adding parallelism — run 5 pages at once to cut the time to 5 minutes. Design how you would implement this in ADF while keeping the pipeline idempotent. What are the constraints and risks?

---
