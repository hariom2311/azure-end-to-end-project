# Topic 14 — Real-world Scenario Curveballs
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 4 Questions | 2–8 years experience | Incident-style scenarios

---

### Q32. You wake up at 3:00 AM to an alert: "Gold FactChargingSession table has 0 new rows for the last 3 hours during the batch window." Walk me through your incident response.

**Answer:**

**Immediate triage (0–5 minutes):**

1. Open Azure Monitor → ADF pipeline runs for `pl_bronze_api_master_v4` and `pl_silver_api_transformation`.
   - Is the Bronze ingest pipeline running? Completed? Failed?
2. Open Databricks Jobs → Silver job run history.
   - Last run status? If failed, what error?

---

**Case 1 — ADF Bronze pipeline failed:**

Check the pipeline run details → which activity failed:
- `act_get_watermark` (SQL lookup) failed → Azure SQL is down or connection timed out → check SQL server health in Azure Portal.
- `act_copy_api` failed → check which entity → is the VoltGrid API down? (HTTP 503)
- `act_write_watermark` failed → SQL write permission issue.

Fix: Resolve root cause, manually trigger `pl_bronze_api_master_v4` rerun. Watermark pattern ensures it picks up from where it left off — no data loss.

---

**Case 2 — Databricks Silver job failed:**

Open the failed job run → cluster logs → driver logs:
- `OutOfMemoryError: GC overhead limit exceeded` → Silver job ran out of memory. Scale cluster or reduce batch size.
- `AnalysisException: Column 'wallet_provider' not found` → API schema evolved, new column arrived in Bronze that Silver's schema doesn't know about.
- `DeltaOptimisticLockException` → Two concurrent jobs tried to write to the same Silver table. Check if a backfill job ran simultaneously.

---

**Case 3 — Gold job failed:**

Check Gold job logs. Most common cause: Silver table is empty (because Silver job failed above) — Gold aggregation runs on empty Silver, writes zero rows to FactChargingSession.

---

**Case 4 — Everything ran successfully but Gold still shows 0 rows:**

Check the ADF Gold → Cosmos DB sync job — it may have failed silently. Check Cosmos DB metrics for last write time. Also verify the Power BI dataset refresh — Import mode cache may be stale.

---

**Communication protocol:**
- Within 15 minutes: post in Teams `#data-incidents`: "Investigating Gold pipeline 0-row alert. Root cause: [Silver job OOM]. ETA for fix: 30 minutes."
- After fix: post resolution and root cause. Add monitoring for the specific failure mode.

**Post-incident improvements:**
- Add a DQ check: after each Gold job run, assert `new_rows > 0`. If zero, fail the job loudly.
- Add a Databricks job memory alert: if cluster memory > 85%, send warning before OOM crash.

---

### Q33. A senior stakeholder says the "Charger Uptime" KPI on the dashboard has been wrong for the past 2 weeks — it's showing 98% uptime but ops knows there were multiple outages. How do you investigate and fix this?

**Answer:**

**Hypotheses to test:**

**Hypothesis 1 — Uptime formula is wrong:**

Check `mart_charger_uptime_daily` calculation:
```python
# Current formula might be wrong:
uptime_pct = sessions_count / total_possible_hours * 100  # sessions ≠ uptime

# Correct formula should be:
uptime_pct = (total_hours - offline_duration_hours) / total_hours * 100
```

Where `offline_duration_hours` comes from `sl_connector_status` (when `connector_status = 'OFFLINE'`).

---

**Hypothesis 2 — Connector status streaming had a data gap:**

Check if the `sl_connector_status` streaming table has a data gap during the affected 2-week period:
```sql
SELECT event_date, COUNT(*) as rows
FROM sl_connector_status
WHERE event_date BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY event_date
ORDER BY event_date
```

If certain dates have 0 rows, the streaming job was down and offline events were missed → uptime appeared 100% because no OFFLINE events were recorded.

---

**Hypothesis 3 — Watermark sent real offline events to quarantine:**

If the OFFLINE status events arrived >10 minutes late (network delay), they were routed to `silver/quarantine/late_events/` instead of being counted:
```sql
SELECT event_date, COUNT(*) as late_offline_events
FROM silver_quarantine_late_events
WHERE status = 'OFFLINE' AND event_date BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY event_date
```

---

**Fix:**

1. For Hypothesis 1: correct the uptime formula in the Gold notebook, rebuild `mart_charger_uptime_daily`.
2. For Hypothesis 2: replay Bronze data from `bronze/streaming/connector_status_raw/` for the gap dates → rerun Silver → rerun Gold mart.
3. For Hypothesis 3: adjust the watermark to 30 minutes (if network latency is routinely > 10 min) and replay quarantined events through the Silver pipeline.

**Communicate fix to stakeholder:** Provide corrected uptime numbers, explain root cause, show the corrected 2-week trend, and commit to an accuracy SLA check: if uptime changes by >5% between consecutive Gold runs without a corresponding maintenance event, alert the engineering team.

---

### Q34. The project needs to expand from Australia to New Zealand. What would need to change in the data model and pipelines?

**Answer:**

**Data model changes:**

**DimCountry:** Currently has one row (`Australia`). Add `New Zealand` row. Add `nz_gst_rate = 0.15` (NZ GST is 15%, not 10% like Australia).

**DimState:** Add NZ regions: Auckland, Wellington, Canterbury, Waikato, Bay of Plenty, etc.

**DimTime:** NZ is UTC+12 (NZST) / UTC+13 (NZDT). DimTime currently only handles AEST/AEDT. Add `nzst_local_time`, `nzdt_local_time` columns. The Silver `normalise_timestamps` function needs NZ timezone handling.

**FactChargingSession / GST Split (Rule J) update:**
```python
# Before: hardcoded Australian GST
gst_amount = gross_amount * 0.10 / 1.10

# After: parameterised by country
gst_rate = country_gst_rate  # 0.10 for AU, 0.15 for NZ
gst_amount = gross_amount * gst_rate / (1 + gst_rate)
```

---

**Pipeline changes:**

**Bronze:** New source systems from NZ chargers, NZ CRM, NZ payment gateway. Add NZ-specific entities to `pipeline_metadata_config.json` — the metadata-driven pipeline handles this without new pipelines (just new config rows).

**Silver:** Fault code mappings may differ if NZ chargers use different OCPP firmware. Add NZ-specific standardisation rules where needed.

**Serving layer:**
- Power BI RLS: add `NZ Country Manager` role filtered to `DimCountry[country_code] = 'NZ'`.
- Cosmos DB: add NZ collections with NZ-specific partitioning.

**Compliance:**
NZ Privacy Act 2020 has similar but slightly different requirements to the Australian Privacy Act. Review PII handling rules for NZ customers.

**The key advantage of this architecture:** Because we used DimCountry → DimState → DimCity as a geo hierarchy from day one (even though only Australia was in scope), the foundation is already extensible. We don't need to restructure the star schema — just add rows and update formulas.

---

### Q35. A data engineer on your team accidentally ran a DELETE on the Silver `sl_customers` table, removing 2,000 customer records. How do you recover?

**Answer:**

**Why this is recoverable — Delta Lake ACID + time travel:**

Every Delta write (including DELETE) creates a new version in the transaction log. The deleted records are not immediately removed from storage — they are marked as deleted in the log but the Parquet files remain until `VACUUM` runs.

**Recovery steps:**

**Step 1 — Immediately stop any further writes:**
Pause the Silver batch job (ADF pipeline) to prevent new writes from compounding the issue.

**Step 2 — Identify the version before the DELETE:**
```python
# Check history
spark.sql("DESCRIBE HISTORY delta.`/path/to/silver/sl_customers`").show(20, truncate=False)

# Output:
# version | timestamp              | operation | operationParameters
# 145     | 2025-06-15 09:30:00    | DELETE    | {"predicate": "..."}
# 144     | 2025-06-15 08:00:00    | MERGE     | {...}
```

Version 145 is the DELETE. Version 144 is safe.

**Step 3 — Read the previous version:**
```python
good_df = spark.read.format("delta") \
    .option("versionAsOf", 144) \
    .load("/path/to/silver/sl_customers")

print(f"Good version rows: {good_df.count()}")  # should include the 2,000 deleted rows
```

**Step 4 — Restore the table:**
```python
spark.sql("""
  RESTORE TABLE delta.`/path/to/silver/sl_customers`
  TO VERSION AS OF 144
""")

# Verify
spark.sql("SELECT COUNT(*) FROM delta.`/path/to/silver/sl_customers`").show()
```

`RESTORE` is atomic — it creates a new version (146) that points back to version 144's data.

**Step 5 — Verify and resume:**
- Assert row count is restored.
- Check if any valid writes happened between version 144 and 145. If yes, replay only those valid writes.
- Resume the Silver batch pipeline.

**Step 6 — Prevention:**

```sql
-- Remove DELETE privilege from data engineers on Silver tables
REVOKE DELETE ON TABLE dbw_ev_intelligence_dev.silver.sl_customers FROM `grp-data-engineers`;
-- Engineers should use MERGE (upsert), not DELETE
```

- Add pre-commit hook in CI/CD that flags any notebook containing `DELETE FROM` on Silver tables and requires team-lead approval.
- VACUUM retention: ensure `RETAIN 168 HOURS` (7 days) — gives a 7-day recovery window.
- Add a row count assertion test that runs after every Silver job: if `sl_customers` row count drops by >10%, fail the job with an alert.
