# Topic 13 — Performance, Optimisation & Cost
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 2 Questions | 2–8 years experience | Scenario-based

---

### Q30. Power BI DirectQuery on Synapse is slow for the Revenue by State dashboard. Walk me through how you would diagnose and fix it.

**Answer:**

**Step 1 — Diagnose where time is spent:**

Power BI Performance Analyser (View → Performance Analyser) shows per-visual query durations:
- `DAX query duration: 200ms` — fast, issue is not in Power BI.
- `Direct query duration: 28 seconds` — this is the Synapse round-trip.

**Step 2 — Capture the SQL sent to Synapse:**

Power BI sends translated SQL to Synapse. Check Synapse Query Store or Azure Monitor Diagnostics:
```sql
-- Power BI generated this (typical)
SELECT s.state_name, SUM(p.amount_aud) as revenue
FROM FactPayments p
JOIN DimStation st ON p.station_key = st.station_key
JOIN DimState s ON st.state_key = s.state_key
JOIN DimTime t ON p.time_key = t.time_key
WHERE t.year = 2025 AND t.month = 6
GROUP BY s.state_name
```

**Step 3 — Identify bottleneck:**

Check Synapse execution plan. Common issues in this project:
- **Full table scan on FactPayments** (50K+ rows) without partition pruning — Power BI's filter on `DimTime.year/month` doesn't translate to a partition filter on the fact table's `load_date` partition column.
- **No statistics:** Synapse dedicated pool needs `CREATE STATISTICS` on join columns for the query optimizer to choose the right join strategy.

**Step 4 — Fix options (in order of effort):**

**Fix A — Use pre-built mart (immediate, no code change):**
The `mart_revenue_by_geo_month` mart is already aggregated by state and month. Switch the Power BI dataset to Import mode from this mart — query goes from 28 seconds to <1 second (VertiPaq in-memory).

**Fix B — Add Synapse partition pruning (medium effort):**
```sql
CREATE TABLE FactPayments
WITH (
  DISTRIBUTION = HASH(station_key),
  PARTITION (year_month RANGE RIGHT FOR VALUES ('2025-01', '2025-02', '2025-03', ...))
)
AS SELECT *, FORMAT(payment_ts, 'yyyy-MM') as year_month FROM FactPayments_staging;
```

**Fix C — Create Synapse statistics (quick win):**
```sql
CREATE STATISTICS stat_FactPayments_station_key ON FactPayments(station_key);
CREATE STATISTICS stat_FactPayments_time_key ON FactPayments(time_key);
CREATE STATISTICS stat_DimTime_year_month ON DimTime(year, month);
```

**Fix D — Materialised view in Synapse:**
```sql
CREATE MATERIALIZED VIEW mv_revenue_by_state_month
WITH (DISTRIBUTION = ROUND_ROBIN)
AS
SELECT s.state_name, t.year, t.month, SUM(p.amount_aud) as revenue
FROM FactPayments p
JOIN DimStation st ON p.station_key = st.station_key
JOIN DimState s ON st.state_key = s.state_key
JOIN DimTime t ON p.time_key = t.time_key
GROUP BY s.state_name, t.year, t.month;
```
Power BI DirectQuery hits this materialised view instead of the base fact table — query rewrites automatically.

**Recommendation for VoltGrid:** The Revenue dashboard is the most-used dashboard. Switch to Import mode from `mart_revenue_by_geo_month` (Fix A) for the daily aggregate view. Retain DirectQuery only for the Live Charging dashboard where near-real-time data is essential.

---

### Q31. Delta Lake OPTIMIZE and ZORDER — when and why would you use them in this project?

**Answer:**

**Why OPTIMIZE is needed:**

Delta Lake writes data in small files during streaming (one file per micro-batch per partition) and during frequent MERGEs. Over time, `bronze/iot/charger_telemetry_raw/event_date=2025-06-15/` might contain 10,000 tiny 50KB Parquet files instead of a few 128MB files. Spark reads each file with an overhead — 10,000 files = 10,000 file open/read operations = very slow queries.

```python
# Compact small files into larger ones (default target: 1 file per 1GB of data)
spark.sql("OPTIMIZE delta.`/path/to/silver/sl_iot_charger_telemetry`")
```

**When to run OPTIMIZE in this project:**
- After every batch Silver job run (post-MERGE, which creates many small files).
- Scheduled on streaming tables daily at 3:00 AM AEST (least-active period).
- Monitor: if `DESCRIBE DETAIL` shows `numFiles > 1000` for a single partition, OPTIMIZE is overdue.

---

**ZORDER — co-locate related data:**

ZORDER reorders data within each file so that rows with similar values for a column are physically adjacent. This enables data skipping — if you query `WHERE charger_id = 'CHG-0042'`, Spark reads only files where CHG-0042 appears, skipping the rest.

```python
# For FactChargingSession — most queries filter by station and time
spark.sql("""
  OPTIMIZE delta.`/path/to/gold/FactChargingSession`
  ZORDER BY (station_key, time_key)
""")

# For FactDeviceTelemetry — most queries filter by charger and date
spark.sql("""
  OPTIMIZE delta.`/path/to/gold/FactDeviceTelemetry`
  ZORDER BY (charger_key, event_date)
""")
```

**When to ZORDER:**
- Apply on columns used in WHERE clauses for the most common dashboard queries.
- For FactChargingSession: `station_key, time_key` (Revenue by Station/Month queries).
- For FactDeviceTelemetry: `charger_key, event_date` (Maintenance dashboard filters by charger and date range).
- Do NOT ZORDER on high-cardinality random columns (e.g., `session_id` UUIDs) — no benefit.

---

**VACUUM — remove old Delta files:**
```python
# Bronze: 90-day retention policy — vacuum older files
spark.sql("VACUUM delta.`/path/to/bronze/iot/charger_telemetry_raw` RETAIN 2160 HOURS")
# 2160 hours = 90 days

# Silver: 30-day retention (shorter — Silver is derived from Bronze)
spark.sql("VACUUM delta.`/path/to/silver/sl_iot_charger_telemetry` RETAIN 720 HOURS")
```

Important: never VACUUM with less than 7 days (168 hours) retention — active readers and time travel queries within that window will fail.

---

**Cost optimisation in this project:**

| Technique | Saving |
|---|---|
| Terminate Databricks cluster after each batch job | ~₹18/hour saved per idle cluster |
| Lifecycle policy: Hot → Cool (30d) → Archive (90d) | ~60–80% storage cost reduction on old Bronze data |
| Auto Loader instead of full directory listing | Reduces LIST API calls (charged per 10,000 operations) |
| Import mode for aggregated marts in Power BI | Reduces Synapse query volume — fewer DTUs consumed |
| VACUUM old Delta files | Prevents runaway storage growth on high-frequency streaming tables |
