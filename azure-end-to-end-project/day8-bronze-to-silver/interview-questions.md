# Day 8 — Interview Questions: Bronze to Silver (Payments)

## Conceptual

**Q1. What is the difference between Bronze and Silver in the medallion architecture?**

**Q2. Why do we use MERGE (upsert) instead of overwrite when writing to Silver?**

**Q3. What is idempotency and why does it matter for data pipelines?**

**Q4. Why are bad records routed to a quarantine zone instead of simply dropped?**

**Q5. What is Australian GST and how do you split it from a GST-inclusive amount?**

**Q6. What is the difference between UTC, AEST, and AEDT? How does PySpark handle DST automatically?**

**Q7. What is a billing reconciliation flag and when would you use it in a payments pipeline?**

**Q8. Why do we dedup at the Silver layer even though ADF already de-duplicates in the control table?**

**Q9. What is the purpose of `sl_ingested_at` and `sl_batch_id` columns in a Silver table?**

**Q10. What happens when you run the Silver notebook twice for the same `p_run_date`?**

---

## Technical / PySpark

**Q11. Write a PySpark expression to calculate GST amount from a GST-inclusive `amount_aud` column.**

**Q12. Write a Window function to keep only the latest record per `payment_id` ordered by `payment_timestamp_utc`.**

**Q13. How do you convert a UTC timestamp column to Australian Eastern Time in PySpark?**

**Q14. How do you split a DataFrame into good and bad records based on a `rejection_reason` column?**

**Q15. Write the Delta MERGE statement to upsert `sl_payments` using `payment_id` as the key.**

**Q16. How do you create an external Delta table in Databricks pointing to an ADLS Gen2 path?**

**Q17. What is `input_file_name()` used for in a Spark read operation?**

**Q18. How do you read only a specific partition folder (e.g., `run_id=2026-10-14`) from a volume?**

**Q19. What does `.option("multiline", "true")` do when reading JSON in Spark?**

**Q20. How do you write bad records to a quarantine Delta table in append mode?**

---

## Scenario-based

**Q21. Your Silver pipeline ran and `billing_anomaly_flag = true` for 15% of records. What do you investigate?**

**Q22. The quarantine table has 500 rows with `rejection_reason = invalid_amount`. What are the likely causes and how do you fix them?**

**Q23. A payment record appears twice in Bronze with the same `payment_id` but different `amount_aud`. Which one does the dedup rule keep and why?**

**Q24. The Silver notebook failed mid-run after writing to quarantine but before the MERGE. What is the state of the Silver table and is it safe to re-run?**

**Q25. Your manager asks: "Can we trust the `excl_gst_aud` column for tax reporting?" What checks would you perform?**
