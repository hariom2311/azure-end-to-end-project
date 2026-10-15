# Day 4 — Interview Questions: ADF Incremental Pipeline

> 25 questions covering ADF linked services for Databricks, pipeline variable design, pagination with Until loop, incremental load patterns, idempotency, file naming, and trigger design.
> Attempt first, then check `interview-solutions.md`.

---

## ADF + Databricks Integration

**Q1 (Warm-up)**
What role must ADF's system-assigned Managed Identity have on the Databricks workspace to trigger notebook jobs? Why `Contributor` and not a more restrictive role?

---

**Q2 (Conceptual)**
When creating the `ls_databricks` linked service, you select **New job cluster** rather than an existing interactive cluster. What is the cost and operational benefit of this choice for a production daily pipeline?

---

**Q3 (Scenario)**
The `act_read_ctl` Databricks Notebook activity succeeds (green in Monitor) but the downstream `act_set_endpoint` Set Variable activity returns null. What are two distinct causes and how do you diagnose each?

---

**Q4 (Conceptual)**
The notebook returns `dbutils.notebook.exit(json.dumps(result))`. In ADF, you access the endpoint field with:
```
@json(activity('act_read_ctl').output.runOutput).endpoint
```
Why is `@json()` needed — what would happen without it?

---

**Q5 (Tricky)**
The pipeline has `p_source_name` as a parameter rather than hardcoding `'payments'` everywhere. A junior engineer says: "Just hardcode it — we only have one source right now." Why is the parameterised approach correct from day one, even for a single source?

---

## Pipeline Variables and Watermark Logic

**Q6 (Warm-up)**
The pipeline captures `v_run_ts = @utcnow()` at the **start** of the pipeline run. Why is the timestamp captured at the start rather than at the end, after all pages are loaded?

---

**Q7 (Conceptual)**
Trace through the `act_set_watermark` expression when `p_load_type_override` is empty (not supplied by the caller):
```
@if(
  and(not(empty(pipeline().parameters.p_load_type_override)),
      equals(pipeline().parameters.p_load_type_override, 'full')),
  '1900-01-01T00:00:00Z',
  json(activity('act_read_ctl').output.runOutput).last_watermark
)
```
What value does `v_last_watermark` receive, and from where?

---

**Q8 (Scenario)**
A pipeline run on 2026-10-20 reads `last_watermark = '2026-10-14T02:00:00Z'` from the control table. Six days passed since the last successful run (system was down). What does the pipeline send as `updated_after`, and will all 6 days of data be captured in this single run?

---

**Q9 (Conceptual)**
The pipeline has 11 variables. Why are `v_endpoint`, `v_sink_base_path`, `v_file_prefix`, and `v_page_size` Set Variable activities placed immediately after `act_read_ctl` rather than reading directly from `activity('act_read_ctl').output.runOutput` in every subsequent activity?

---

**Q10 (Tricky)**
`v_page_size` is typed as `String` even though page size is a number. The dataset parameter `p_page_size` is typed as `Int`. Explain why the mismatch exists and how it is resolved in the ADF expression chain.

---

## Pagination and Until Loop

**Q11 (Warm-up)**
Why is page 1 fetched with a **Web Activity** (`act_fetch_page1_meta`) rather than a Copy Activity like all other pages?

---

**Q12 (Conceptual)**
The Until loop exit condition is:
```
@greaterOrEquals(variables('v_current_page'), add(variables('v_total_pages'), 1))
```
Walk through what happens on the final loop iteration when `v_current_page = 321` and `v_total_pages = 321`.

---

**Q13 (Scenario)**
A data engineer reviews the pipeline and notices `act_increment_page` appears on the main pipeline canvas, not inside the Until loop. The pipeline ran once and fetched 321 copies of page 2. What happened and how do you fix it?

---

**Q14 (Conceptual)**
Inside the Until loop, `act_copy_page` and `act_increment_page` are sequential (increment depends on copy success). Why should they not run in parallel?

---

**Q15 (Tricky)**
The API returns `total_pages = 0` for an incremental run that starts after a day with no updates. What happens in the pipeline — does it fail, succeed silently, or loop forever? What should the pipeline do in this case?

---

## Incremental Load and Idempotency

**Q16 (Warm-up)**
What does "incremental load" mean for this pipeline? What is the `updated_after` parameter, and where does its value come from on the second pipeline run?

---

**Q17 (Conceptual)**
The pipeline is idempotent. Define idempotency in this context. Give a concrete example of what happens when the pipeline runs twice on 2026-10-14 with the same `updated_after` value.

---

**Q18 (Scenario)**
The pipeline runs at 02:00 UTC, loads 5 pages, then `act_update_watermark` fails. The pipeline is re-triggered at 03:00 UTC. What watermark does it use, and what happens to the 5 pages already written to Bronze?

---

**Q19 (Conceptual)**
After a successful full load, the control table shows `load_type = 'incremental'`. The next day's trigger runs with `p_load_type_override` empty. Trace the complete watermark resolution: what value reaches the API as `updated_after`?

---

**Q20 (Tricky)**
A full pipeline run fails on page 200 of 321. The engineer triggers a re-run with `p_load_type_override = full`. Pages 1–199 are written again, overwriting the files from the failed run. Is this a data problem? What happens downstream in Silver?

---

## File Naming, Path Design, and Trigger

**Q21 (Warm-up)**
The file naming convention uses `padLeft(string(variables('v_current_page')), 3, '0')`. What does this produce for page 7, page 42, and page 321? Why is zero-padding necessary?

---

**Q22 (Conceptual)**
The sink folder uses `ingestion_date=YYYY-MM-DD` (with the `ingestion_date=` prefix) rather than just `YYYY-MM-DD`. What is this pattern called and what does Databricks do with it automatically when reading the Bronze layer?

---

**Q23 (Scenario)**
The team wants to add sessions data. The sessions API endpoint is `/api/db/sessions/`. List the exact changes needed: control table, ADF, datasets, and Databricks — in order of what changes and what stays the same.

---

**Q24 (Conceptual)**
The trigger uses **Tumbling Window** rather than **Schedule** trigger. What is the key operational difference between the two? What happens with Tumbling Window if a pipeline run fails — does the next window fire anyway?

---

**Q25 (System design)**
The current pipeline runs pages sequentially in the Until loop. A full load of 321 pages at ~4 seconds per page takes ~20 minutes. Design how you would add parallelism to cut the time to ~4 minutes, while keeping the pipeline idempotent and the watermark update correct.

---
