# Topic 03 — Azure Databricks & PySpark
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 3 Questions | 2–8 years experience | Scenario-based

---

### Q8. In the Silver layer, you process 28 different entity types. How did you design the Silver transformation notebooks to avoid maintaining 28 separate notebooks?

**Answer:**

The three-notebook progression (v1 → v2 → v3) built towards a single production notebook that handles all 17+ API entities:

**v1 — Explicit, single entity (payments):**
```python
# Every step written out: read Bronze JSON, cast types,
# rename columns, validate ranges, write Delta
```
Purpose: understand each transformation step in isolation.

**v2 — ForEach loop over entity config list:**
```python
ENTITIES = [
  {"name": "payments", "bronze_path": "...", "silver_path": "...", "dedup_key": "payment_id"},
  {"name": "customers", "bronze_path": "...", "silver_path": "...", "dedup_key": "customer_id"},
  # ...
]
for entity in ENTITIES:
    df = spark.read.json(entity["bronze_path"])
    df = apply_common_transforms(df)
    df.write.format("delta").mode("overwrite").save(entity["silver_path"])
```

**v3 — Production with incremental Delta MERGE:**
```python
# Widget parameters from ADF: load_type (full/incremental), entity_name
# Helper functions: apply_common_transforms(), validate_schema(), quarantine_bad_records()
# Delta MERGE for idempotent upserts
deltaTable.alias("target").merge(
    df.alias("source"),
    f"target.{entity['dedup_key']} = source.{entity['dedup_key']}"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

**Common transform functions applied to all entities:**
- `add_ingestion_metadata()` — adds `_silver_ts`, `_source_layer`
- `normalise_timestamps()` — converts all timestamps to UTC (Rule G)
- `quarantine_nulls()` — checks mandatory fields, routes bad rows to `silver/quarantine/`
- `flatten_json()` — unpacks nested JSON arrays/structs

**Why this is better:** One bug fix in `normalise_timestamps()` propagates to all 17 entities simultaneously. The entity config list is the only place entity-specific logic lives (dedup key, bronze path, silver path).

---

### Q9. The IoT charger sends a temperature reading of 82°C. Walk me through what happens to that record from ingestion to alerting — every system it passes through.

**Answer:**

This traces the full pipeline for a critical real-time event:

**Step 1 — Device layer:**
- Physical charger sends OCPP telemetry JSON over MQTT/WebSocket to Azure IoT Hub.
- Payload: `{"charger_id": "CHG-0042", "connector_id": 1, "temperature_c": 82.0, "voltage": 415, "event_ts": "2025-06-15T09:14:33Z"}`.

**Step 2 — Azure IoT Hub → Event Hubs:**
- IoT Hub forwards the message to `evh-ev-intelligence-dev`, topic `iot-telemetry` (4 partitions).
- Message is partitioned by `charger_id` — events from the same charger always hit the same partition, preserving order.

**Step 3 — Databricks Structured Streaming (Bronze write):**
- Always-on streaming job reads from `iot-telemetry` topic.
- Adds metadata: `_ingestion_ts = now()`, `_source_file = "iot-telemetry"`, `_pipeline_run_id = <job-run-id>`.
- Writes to `bronze/iot/charger_telemetry_raw/event_date=2025-06-15/` as Delta append.
- Checkpoint updated in `bronze/_checkpoints/iot-telemetry/`.

**Step 4 — Silver transformation (streaming):**
- A separate streaming job reads Bronze charger_telemetry.
- Deduplication (Rule A): checks `charger_id + event_ts + connector_id` — no duplicate, record passes.
- Watermark (Rule B): event is within 10-minute window — record passes.
- Fault code mapping (Rule C): `fault_code = "OVERTEMP"` → `fault_description = "Overheating Risk"`.
- Temperature flag (Rule E): `temperature_c = 82.0 > 75.0` → `overheating_flag = TRUE`.
- Record written to `silver/sl_iot_charger_telemetry/` via Delta MERGE.

**Step 5 — Databricks alert rule:**
- Streaming job has a threshold check: `WHERE overheating_flag = TRUE`.
- Alert condition satisfied → fires Azure Functions trigger via HTTP webhook.

**Step 6 — Azure Functions:**
- Function receives event payload: `charger_id = CHG-0042`, `station_id`, `temperature = 82`.
- Sends CRITICAL alert via three channels:
  - Email: Azure Communication Services → maintenance team
  - SMS: Twilio → on-call engineer
  - Teams: webhook POST to `#ops-alerts` channel

**Step 7 — FactMaintenance row created:**
- Gold job sees `overheating_flag = TRUE` in Silver → creates maintenance event row in `FactMaintenance` with `root_cause = "Overheating"`, `fault_description = "Overheating Risk"`, `charger_key`, `station_key`, `time_key`.
- `mart_predictive_maintenance_risk` increments `overheating_count` for CHG-0042.

**Step 8 — Power BI dashboard:**
- Predictive Maintenance dashboard refreshes — CHG-0042 shows elevated risk score, red status indicator.

**Total latency from device to alert: ~30–90 seconds** (IoT Hub buffering + Databricks micro-batch cadence + Functions cold start).

---

### Q10. Explain how Delta MERGE (upsert) works and why you used it for the Silver layer instead of just overwriting the table every time.

**Answer:**

**Delta MERGE syntax:**
```python
from delta.tables import DeltaTable

silver_table = DeltaTable.forPath(spark, silver_path)
silver_table.alias("target").merge(
    source_df.alias("source"),
    "target.session_id = source.session_id"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()
```

**What MERGE does:**
- For each row in `source_df`, checks if a row with the same `session_id` exists in the Silver Delta table.
- **Matched:** Update all columns (handles corrected records from Bronze).
- **Not matched:** Insert as new row.

**Why MERGE over overwrite:**

| Scenario | Overwrite (full reload) | MERGE (upsert) |
|---|---|---|
| Bronze has 1M rows, only 500 new today | Rewrites 1M rows | Processes 500 rows |
| A Bronze record was corrected | Old Silver row replaced on next full reload | MERGE updates the specific row immediately |
| Silver table has 30 days history, Bronze only keeps 7 days | History lost on overwrite | History preserved |
| Pipeline fails halfway | Partial write leaves corrupt table | MERGE is atomic — either all or nothing |

**Idempotency:** If the same Bronze batch is processed twice (due to a retry), MERGE updates existing Silver rows to the same values — net result identical to running once. Overwrite would cause duplicate rows if run twice without TRUNCATE first.

**In this project:** `sl_tariffs_scd2` specifically cannot use overwrite — SCD2 requires keeping historical rows with `is_current = FALSE` alongside the new `is_current = TRUE` row. MERGE handles this with conditional update: `WHEN MATCHED AND source.rate_per_kwh != target.rate_per_kwh THEN UPDATE SET effective_to = source.effective_from - interval 1 day, is_current = FALSE`.
