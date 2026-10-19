# Day 7 — Interview Questions: Pipeline Idempotency + Blob Storage Ingestion

> 25 questions covering run isolation with RunId, Azure Blob Storage SAS authentication, multi-format ingestion (XML, PDF/Binary, JSON), date-partitioned folder design, parallel copy activities, and schedule triggers.
> Attempt first, then check `interview-solutions.md`.

---

## Pipeline Idempotency and RunId

**Q1 (Warm-up)**
The pipeline `pl_bronze_incremental_load_v2` has an overwrite bug when it runs twice on the same day. Describe the exact scenario where Run 2 corrupts Run 1's Bronze data, and identify the root cause.

---

**Q2 (Conceptual)**
`pipeline().RunId` is used to create a unique sink subfolder per run. What is `pipeline().RunId`, where does ADF assign it, and why is it preferred over `utcnow()` for this purpose?

---

**Q3 (Scenario)**
A pipeline uses the sink path `blob/invoices/ingestion_date=2026-10-18/`. It runs at 09:00 and produces 15 PDF files. It runs again at 14:00 and produces 3 new PDF files. What is the state of Bronze after the second run, and is this a data problem?

---

**Q4 (Scenario — same as Q3 with the fix)**
The same pipeline now uses `blob/invoices/ingestion_date=2026-10-18/run_id=@{pipeline().RunId}/`. Same two runs. What is the state of Bronze after the second run? How many files total, and in how many subfolders?

---

**Q5 (Conceptual)**
The master pipeline `pl_bronze_master` calls child pipeline `pl_bronze_incremental_load_v3` for each source. The child uses `pipeline().RunId` in the sink path. Does each child run get its own unique RunId, or do all children share the master pipeline's RunId?

---

**Q6 (Tricky)**
A data engineer tries to store `pipeline().RunId` in a pipeline String variable at the start of the run so it can be reused across activities. What happens, and is this necessary?

---

## Blob Storage and SAS Authentication

**Q7 (Warm-up)**
What is a SAS token? What does `sp=rl` mean in the SAS URL `?sp=rl&st=...`?

---

**Q8 (Conceptual)**
The linked service `ls_blob_source` uses a **SAS URI** rather than an Account Key or Managed Identity. What are the two main reasons to choose SAS for a source storage account you do not own?

---

**Q9 (Scenario)**
The SAS token in `ls_blob_source` has `se=2027-10-19T22:34:08Z` (expiry one year from creation). The pipeline runs daily. What happens on 2027-10-20, and what is the correct process to renew?

---

**Q10 (Tricky)**
A colleague says: "We should use the same `ls_adls_bronze` linked service for reading from `dataenggdailystorage` — it's just another blob storage." Why is this wrong? What specifically breaks if you try?

---

**Q11 (Conceptual)**
The SAS URL provided has `sr=c` (resource type = container). Could `sr=b` (resource type = blob) be used instead? What is the practical difference when reading an entire folder of files?

---

## Dataset Design and Format Handling

**Q12 (Warm-up)**
The pipeline uses three different source dataset types: `XmlSource`, `BinarySource`, and `JsonSource`. Which dataset type is correct for PDF files, and why?

---

**Q13 (Conceptual)**
The sink dataset `ds_bronze_blob_sink` is typed as `Binary` even though one of the sources is `JsonSource`. Why does the sink use Binary type rather than Json, and what does this mean for the files written to ADLS?

---

**Q14 (Scenario)**
`ds_blob_invoices_src` has a parameter `p_date` (type String, format `YYYY/MM/DD`). The folder path in the dataset is `@concat('invoices/', dataset().p_date)`. The pipeline passes `@formatDateTime(addDays(utcnow(), -1), 'yyyy/MM/dd')`. Trace the exact folder path that ADF reads when the pipeline runs on 2026-10-19.

---

**Q15 (Conceptual)**
`ds_blob_reports_src` has a parameter `p_year_month` (format `YYYY/MM`). Reports are monthly aggregates. On 2026-10-19 the pipeline runs and reads `reports/2026/10/`. The files are `kpi_report_202610.json`, `sla_report_202610.json`, `state_breakdown_202610.json`. Do those files exist in the folder yet on 2026-10-19? Should the pipeline read current month or previous month?

---

## Pipeline Activities and Date Expressions

**Q16 (Warm-up)**
Write the ADF expression that produces yesterday's date in `YYYY-MM-DD` format, given that the pipeline runs at 02:00 UTC.

---

**Q17 (Conceptual)**
The three copy activities (`act_copy_config`, `act_copy_invoices`, `act_copy_reports`) all have `dependsOn: []` — they run in parallel. What is the benefit and what is the risk?

---

**Q18 (Scenario)**
`act_copy_invoices` runs on 2026-10-19 at 02:00 UTC. The source folder is `invoices/2026/10/18/` (yesterday). The folder exists but is empty — no PDFs were generated for that date (e.g., it was a public holiday). What happens — does the pipeline fail, succeed with 0 files, or throw an error?

---

**Q19 (Conceptual)**
The config source (`ds_blob_config_src`) reads all `*.xml` files from `source/config/` without any date parameter. Config files are reference data — they change rarely. Why is a full load every day acceptable for this source but NOT acceptable for the invoice or session API sources?

---

**Q20 (Tricky)**
The sink `p_folder` for invoices is:
```
blob/invoices/ingestion_date=2026-10-18/run_id=abc-123.../
```
Silver needs to read all invoice files for a given date. Write the ADLS path (with wildcard) that Silver would use to read all runs for 2026-10-18.

---

## Triggers and Scheduling

**Q21 (Warm-up)**
The trigger `tr_blob_ingestion_daily` fires at 02:00 UTC daily. The invoices pipeline reads yesterday's folder. At 02:00 UTC, what is the local time in Sydney (AEDT, UTC+11) and is it safe to assume the previous day's invoice folder is fully populated by then?

---

**Q22 (Conceptual)**
`tr_blob_ingestion_daily` is a **Schedule trigger**, not a Tumbling Window trigger. What is the key difference in failure behaviour? When would you switch to Tumbling Window for this pipeline?

---

**Q23 (Scenario)**
The trigger fires at 02:00 UTC on 2026-10-19. The `act_copy_invoices` activity fails because the source SAS token expired. `act_copy_config` and `act_copy_reports` succeed. The trigger fires again on 2026-10-20. What happens to the 2026-10-18 invoice data — is it recovered?

---

**Q24 (Conceptual)**
The pipeline copies files without transforming them — raw XML, PDF bytes, and JSON land in Bronze exactly as they are in the source. This is the Bronze layer principle. What is the justification for this design, and what layer handles parsing and schema enforcement?

---

**Q25 (System design)**
The source `dataenggdailystorage` is an external storage account you do not own — the EV hardware vendor populates it. The vendor might rename folders (e.g. change `config/` to `configuration/`), add new file formats, or change the SAS token without warning. Design a strategy that makes the Bronze pipeline resilient to these three changes with minimal rework when they occur.

---
