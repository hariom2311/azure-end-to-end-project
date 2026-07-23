# Topic 04 — Delta Lake & Medallion Architecture
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 2 Questions | 2–8 years experience | Scenario-based

---

### Q11. What is Delta Lake's time travel feature, and can you give a real scenario from this project where you would use it?

**Answer:**

**What time travel is:**

Delta Lake keeps a transaction log (`_delta_log/`) that records every write operation (commit, schema change, delete). You can query any previous version of a table:

```python
# Query table as it was 7 days ago
df = spark.read.format("delta").option("timestampAsOf", "2025-06-01").load(silver_path)

# Query specific version number
df = spark.read.format("delta").option("versionAsOf", 42).load(silver_path)

# Show history
spark.sql("DESCRIBE HISTORY delta.`/path/to/silver/sl_charging_sessions`")
```

**Real scenarios in VoltGrid:**

**Scenario 1 — Silver transformation bug corrupted revenue data:**
A code change in the Silver GST split logic (Rule J) was deployed with a bug: it divided by 1.1 instead of multiplying. All sessions written that night have wrong `net_amount`. With time travel, we restore the previous version:
```python
# Restore Silver sessions to pre-bug version
previous_df = spark.read.format("delta").option("versionAsOf", 99).load(silver_sessions_path)
previous_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_sessions_path)
```

**Scenario 2 — Late-arriving billing dispute:**
A franchise partner disputes their June invoice claiming sessions were counted incorrectly. Finance needs to see `FactChargingSession` exactly as it was on June 30th — not the current state (which may have been corrected since). Time travel lets us query the exact state of the Gold table at any past timestamp.

**Scenario 3 — Audit trail for Australian Privacy Act compliance:**
A regulator asks: "What PII data did your system hold for customer C-10042 on March 15th?" Time travel on `sl_customers` shows the exact record state at that date — email hash, loyalty tier, last 4 of card — without needing a separate audit database.

**Retention:** Bronze Delta tables are configured with `delta.logRetentionDuration = "interval 90 days"` to support 90-day replays.

---

### Q12. What is schema evolution and how does Bronze handle it when the Payment API suddenly adds a new field?

**Answer:**

**The problem:**

Payment API returns:
```json
{"payment_id": "P001", "amount": 55.89, "gateway": "stripe", "status": "SUCCESS"}
```

After an API update, it returns:
```json
{"payment_id": "P001", "amount": 55.89, "gateway": "stripe", "status": "SUCCESS", "wallet_provider": "apple_pay"}
```

Without schema evolution handling, the Databricks job fails with `AnalysisException: column 'wallet_provider' not found in schema`.

**How Auto Loader handles it at Bronze:**

```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load(bronze_path)
```

`schemaEvolutionMode = "addNewColumns"`:
- Auto Loader detects the new `wallet_provider` column.
- Automatically updates the inferred schema.
- New column is added to the Bronze Delta table with `NULL` for all existing rows.
- Processing continues without manual intervention.

**What you must NOT do:** `schemaEvolutionMode = "failOnNewColumns"` is the safe default for production Silver and Gold — you don't want unreviewed schema changes silently propagating to business dashboards.

**Downstream handling:** When Silver picks up the new `wallet_provider` column from Bronze:
- The Silver job's entity config defines explicit column mapping — `wallet_provider` is unknown, so it goes into a `_extra_fields` struct or gets quarantined until the Silver schema is intentionally updated.
- This separation means a Bronze schema change never silently breaks a Gold dashboard.
