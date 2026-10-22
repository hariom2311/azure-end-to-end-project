# Day 8 — Interview Solutions: Bronze to Silver (Payments)

## Conceptual

**A1. Bronze vs Silver**
Bronze stores raw data exactly as received from the source — no transformations, append-only, used for reprocessing. Silver applies business rules: schema enforcement, dedup, timezone normalisation, derived columns, quarantine routing. Silver is the "trusted" layer analysts and downstream pipelines consume.

**A2. MERGE instead of overwrite**
Overwrite would delete all existing Silver data and replace it. MERGE matches on the primary key (`payment_id`): existing rows get updated, new rows get inserted, no data is lost. This allows incremental daily runs without reprocessing history.

**A3. Idempotency**
Running the same pipeline twice produces the same result as running it once. In this pipeline: the MERGE matches on `payment_id`, so re-running the same `p_run_date` overwrites the same rows — no duplicates are created. Quarantine writes use `mode("append")` but the bad records are identical, so downstream dedup handles that.

**A4. Quarantine instead of drop**
Dropping bad records loses data permanently. Quarantine preserves the original row + rejection reason so a data engineer can inspect, fix the source, and reprocess. It also feeds data quality dashboards and SLA metrics.

**A5. Australian GST split**
GST is 10% of the pre-tax price, meaning it is 1/11th of the GST-inclusive total.
```
gst_amount  = amount_aud / 11
excl_gst    = amount_aud - gst_amount
```
Example: $110 inclusive → $10 GST, $100 ex-GST.

**A6. UTC / AEST / AEDT**
UTC is the global reference. AEST (Australian Eastern Standard Time) = UTC+10, used April–October. AEDT (Australian Eastern Daylight Time) = UTC+11, used October–April. PySpark's `from_utc_timestamp(col, "Australia/Sydney")` uses the IANA timezone database and automatically applies the correct offset for each timestamp's date — no manual DST handling needed.

**A7. Billing reconciliation flag**
A payment with `status = 'failed'` should have no money collected. If `amount_aud > 0` on a failed payment, it suggests a billing system discrepancy — the customer may have been charged but the payment gateway reports failure. The flag lets the finance team review these rows without blocking the pipeline.

**A8. Dedup at Silver even with control table**
The ADF control table prevents the same API page from being re-fetched, but the same record can still appear in multiple Bronze partitions if: (a) the API returns overlapping pages, (b) an ADF retry re-lands a full page, or (c) a record's `updated_at` timestamp changes and it appears in two runs. Silver dedup is the safety net.

**A9. sl_ingested_at and sl_batch_id**
`sl_ingested_at` = when the row was written to Silver (timestamp). `sl_batch_id` = which pipeline run wrote it (run_date or "full"). Together they provide lineage: you can trace any Silver row back to the Bronze run that produced it.

**A10. Re-running same p_run_date**
The MERGE matches `payment_id`. Existing rows are updated with the same values (effectively a no-op at the data level). Quarantine gets the same bad rows appended again — but they are identical so no new information is added. The result is identical to the first run. Pipeline is safe to re-run.

---

## Technical / PySpark

**A11. GST calculation**
```python
df = df.withColumn("gst_amount_aud", F.round(F.col("amount_aud") / 11, 4)) \
       .withColumn("excl_gst_aud",   F.round(F.col("amount_aud") - F.col("amount_aud") / 11, 4))
```

**A12. Dedup with Window**
```python
from pyspark.sql import Window, functions as F

w = Window.partitionBy("payment_id").orderBy(F.col("payment_timestamp_utc").desc())
df_dedup = (
    df.withColumn("_rn", F.row_number().over(w))
      .filter(F.col("_rn") == 1)
      .drop("_rn")
)
```

**A13. UTC to AEST/AEDT**
```python
df = df.withColumn("payment_timestamp_utc",  F.to_timestamp("payment_timestamp_utc")) \
       .withColumn("payment_timestamp_aest", F.from_utc_timestamp("payment_timestamp_utc", "Australia/Sydney"))
```

**A14. Split good / bad**
```python
df_checked = df.withColumn(
    "rejection_reason",
    F.when(F.col("payment_id").isNull(), "null_payment_id")
     .when(F.col("amount_aud") <= 0,    "invalid_amount")
     .otherwise(None)
)
df_bad  = df_checked.filter(F.col("rejection_reason").isNotNull())
df_good = df_checked.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")
```

**A15. Delta MERGE**
```python
from delta.tables import DeltaTable

tgt = DeltaTable.forName(spark, "ev_catalog.silver.sl_payments")
(tgt.alias("t")
    .merge(df_silver.alias("s"), "t.payment_id = s.payment_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
```

**A16. Create external Delta table**
```sql
CREATE TABLE IF NOT EXISTS ev_catalog.silver.sl_payments (
    payment_id STRING,
    amount_aud DOUBLE,
    ...
)
USING DELTA
LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/payments/';
```
The `LOCATION` clause makes it external — data lives in ADLS, not the managed metastore.

**A17. input_file_name()**
Returns the full path of the source file for each row. Useful for lineage — you can trace which Bronze file a Silver row came from. Added as `_src` column before any filtering.

**A18. Read specific partition**
```python
# Incremental: read one run_id partition
df = spark.read.format("json").load("/Volumes/ev_catalog/bronze/api/payments/run_id=2026-10-14/")

# Full: read all partitions
df = spark.read.format("json").load("/Volumes/ev_catalog/bronze/api/payments/")
```

**A19. multiline JSON option**
Without it, Spark treats each line as a separate JSON object. With `multiline=true`, Spark reads the entire file as one JSON document — needed when the API returns a pretty-printed JSON array or a single object spanning multiple lines.

**A20. Write to quarantine**
```python
if df_bad.count() > 0:
    (df_bad
     .withColumn("quarantine_ts", F.current_timestamp())
     .write.format("delta")
     .mode("append")
     .save("abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/payments/"))
```

---

## Scenario-based

**A21. 15% billing_anomaly_flag = true**
Investigate: (1) Check the payment gateway API — is `status=failed` being set correctly? (2) Check for duplicate API calls where a retry set status to failed but the original succeeded. (3) Look at the `payment_method` distribution — card failures are common, direct debit should rarely fail with an amount. (4) Raise with finance team to cross-check against bank settlement files.

**A22. 500 rows with invalid_amount**
Likely causes: (a) API returned `amount_aud: null` for in-progress sessions not yet billed, (b) negative amounts from refunds — fix by adjusting the check to `amount_aud IS NULL` only, treating negatives as valid refunds, (c) schema change in API — amount field renamed. Fix by updating the quality check condition and reprocessing those Bronze partitions.

**A23. Duplicate payment_id, different amount_aud**
The dedup rule keeps the record with the **latest `payment_timestamp_utc`**. This is the more recent update from the API. The assumption is that the API emits corrected records with the same `payment_id` when an amount is amended (e.g., partial refund applied). If business logic requires keeping the original amount, the Window order should be `.asc()` instead.

**A24. Mid-run failure — safe to re-run?**
Yes. The Silver MERGE had not yet executed, so the Silver table is unchanged. The quarantine table has the bad rows appended but they are the same rows that will be written again on re-run — identical data. Re-run the notebook with the same `p_run_date`. The MERGE will complete, and the quarantine will have a duplicate batch of the same bad rows (acceptable — quarantine is append-only for audit purposes).

**A25. Trusting excl_gst_aud for tax reporting**
Checks: (1) Verify `amount_aud` was correctly cast to DOUBLE before the calculation — a failed cast produces null, not an error. (2) Confirm all payments in Silver have `gst_amount_aud + excl_gst_aud = amount_aud` (tolerance of 0.01 for rounding). (3) Check that `status = 'failed'` payments are excluded from tax totals — they should not contribute GST. (4) Validate total GST collected against the monthly kpi_report `gst_collected_aud` field from the blob reports pipeline.
