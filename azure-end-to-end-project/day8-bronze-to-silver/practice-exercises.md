# Day 8 — Practice Exercises: Bronze to Silver (Payments)

## Exercise 1 — Create the Silver Schema

**Task:** Create the `ev_catalog.silver` external schema in Databricks pointing to the silver ADLS container.

**Steps:**
1. Go to Catalog → External Data → External Locations → Create
2. Name: `ext-loc-ev-silver`, URL: `abfss://silver@evdatalakedev2.dfs.core.windows.net/`, credential: `mi-ev-credential`
3. Test the connection
4. Run in a notebook:
```sql
CREATE SCHEMA IF NOT EXISTS ev_catalog.silver
MANAGED LOCATION 'abfss://silver@evdatalakedev2.dfs.core.windows.net/';
```
5. Verify with: `DESCRIBE SCHEMA ev_catalog.silver;`

**Expected result:** Schema location shows the silver ADLS URL.

---

## Exercise 2 — Full Load Run

**Task:** Import `nb_silver_payments.py` into Databricks and run a full historical load.

**Steps:**
1. In Databricks workspace, create a new notebook and paste the code from `nb_silver_payments.py`
2. Attach to a cluster (Serverless or the existing interactive cluster)
3. Leave the `p_run_date` widget blank
4. Run all cells

**Expected result:**
- `ev_catalog.silver.sl_payments` is created
- Cell 11 shows row counts grouped by `status`
- No errors in Cell 4 quarantine write (or a count of quarantined rows is printed)

---

## Exercise 3 — Incremental Load

**Task:** Run the notebook for a specific date to test incremental loading.

**Steps:**
1. Set `p_run_date` widget to a date that has Bronze data (e.g., `2026-10-14`)
2. Run all cells
3. Note the row count in Silver
4. Run again with the same date
5. Check that row count did not increase (MERGE is idempotent)

**Verify:**
```sql
SELECT COUNT(*), sl_batch_id
FROM ev_catalog.silver.sl_payments
GROUP BY sl_batch_id
ORDER BY sl_batch_id;
```

---

## Exercise 4 — Verify GST Calculation

**Task:** Confirm the GST split is correct for all rows.

**Run in Databricks SQL:**
```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN ABS(excl_gst_aud + gst_amount_aud - amount_aud) > 0.01 THEN 1 END) AS gst_mismatch_rows,
    ROUND(SUM(gst_amount_aud), 2)  AS total_gst_collected,
    ROUND(SUM(excl_gst_aud), 2)    AS total_ex_gst_revenue,
    ROUND(SUM(amount_aud), 2)      AS total_inclusive_revenue
FROM ev_catalog.silver.sl_payments
WHERE status = 'success';
```

**Expected:** `gst_mismatch_rows = 0`

---

## Exercise 5 — Check Billing Anomalies

**Task:** Find all billing anomaly records and understand why they were flagged.

```sql
SELECT
    payment_id,
    customer_id,
    amount_aud,
    status,
    payment_method,
    billing_anomaly_flag
FROM ev_catalog.silver.sl_payments
WHERE billing_anomaly_flag = true
ORDER BY payment_timestamp_utc DESC
LIMIT 20;
```

**Follow-up:** How many are from which `payment_method`?
```sql
SELECT payment_method, COUNT(*) AS anomaly_count
FROM ev_catalog.silver.sl_payments
WHERE billing_anomaly_flag = true
GROUP BY payment_method;
```

---

## Exercise 6 — Inspect Quarantine Table

**Task:** Check if any records were rejected and understand why.

```sql
-- If quarantine table was created as Delta
SELECT rejection_reason, COUNT(*) AS cnt, MAX(quarantine_ts) AS latest
FROM delta.`abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/payments/`
GROUP BY rejection_reason
ORDER BY cnt DESC;
```

**Expected:** Understand which quality check is failing most. For each reason, look at 2–3 sample rows to identify the root cause.

---

## Exercise 7 — Timezone Verification

**Task:** Confirm UTC → AEST conversion is correct.

```sql
SELECT
    payment_id,
    payment_timestamp_utc,
    payment_timestamp_aest,
    -- AEST should be UTC+10 or UTC+11 depending on DST
    ROUND((unix_timestamp(payment_timestamp_aest) - unix_timestamp(payment_timestamp_utc)) / 3600, 1) AS offset_hrs
FROM ev_catalog.silver.sl_payments
LIMIT 10;
```

**Expected:** `offset_hrs` is either `10.0` (AEST, standard time) or `11.0` (AEDT, daylight saving).

---

## Exercise 8 — Add the Notebook to ADF Pipeline

**Task:** Connect the Silver notebook to the existing ADF bronze ingestion pipeline so it runs automatically after Bronze loads.

**Steps:**
1. Open ADF Studio → Pipelines → `pl_api_to_bronze` (Day 4 pipeline)
2. Add a **Databricks Notebook** activity after the ForEach copy activity
3. Set dependency: success of ForEach
4. Linked service: `ls_databricks`
5. Notebook path: path to your `nb_silver_payments` notebook in workspace
6. Base parameters: `p_run_date` = `@formatDateTime(utcnow(), 'yyyy-MM-dd')`
7. Save and trigger a debug run

**Verify:** After ADF completes, check Silver table has new rows with today's `sl_batch_id`.

---

## Bonus — Write a Data Quality Summary Query

Write a single SQL query that produces this summary for `sl_payments`:

| Metric                    | Value  |
|---------------------------|--------|
| Total Silver rows         | XXXXX  |
| Distinct customers        | XXXX   |
| Success payments          | XXXXX  |
| Failed payments           | XXX    |
| Billing anomalies         | XX     |
| Total revenue (ex-GST)    | $XXXXX |
| Total GST collected       | $XXXXX |
| Earliest payment date     | YYYY-MM-DD |
| Latest payment date       | YYYY-MM-DD |

```sql
SELECT
    COUNT(*)                                              AS total_rows,
    COUNT(DISTINCT customer_id)                           AS distinct_customers,
    COUNT(CASE WHEN status = 'success' THEN 1 END)        AS success_payments,
    COUNT(CASE WHEN status = 'failed' THEN 1 END)         AS failed_payments,
    COUNT(CASE WHEN billing_anomaly_flag THEN 1 END)      AS billing_anomalies,
    ROUND(SUM(CASE WHEN status='success' THEN excl_gst_aud END), 2)   AS total_revenue_ex_gst,
    ROUND(SUM(CASE WHEN status='success' THEN gst_amount_aud END), 2) AS total_gst_collected,
    MIN(DATE(payment_timestamp_aest))                     AS earliest_payment,
    MAX(DATE(payment_timestamp_aest))                     AS latest_payment
FROM ev_catalog.silver.sl_payments;
```
