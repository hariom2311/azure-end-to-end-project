# Topic 07 — Streaming: Event Hubs & Structured Streaming
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 2 Questions | 2–8 years experience | Scenario-based

---

### Q18. Explain exactly-once semantics in Databricks Structured Streaming. How do you achieve it with Azure Event Hubs?

**Answer:**

**The problem:** Event Hubs is an at-least-once delivery system. If a streaming job crashes after processing a micro-batch but before committing the offset, the same events are re-delivered on restart. Without exactly-once guarantees, this creates duplicate rows in Bronze.

**How exactly-once is achieved:**

**Step 1 — Checkpoint-based offset tracking:**
```python
df = spark.readStream \
    .format("eventhubs") \
    .options(**eh_conf) \
    .load()

df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://bronze@evdatalakedev.dfs.core.windows.net/_checkpoints/iot-telemetry/") \
    .trigger(processingTime="30 seconds") \
    .start(bronze_path)
```

The checkpoint directory stores the last committed Event Hub partition offset. On restart, Structured Streaming reads from exactly the last committed offset — no events are skipped and no events are replayed past the checkpoint.

**Step 2 — Idempotent Delta writes:**
Delta Lake's ACID transactions ensure that if a micro-batch write fails after partial commit, the transaction is rolled back. When the job restarts, it re-processes the same micro-batch and Delta writes it atomically — no partial rows.

**Step 3 — One checkpoint per topic:**
We have 10 Event Hub topics → 10 separate checkpoint directories (`_checkpoints/iot-telemetry/`, `_checkpoints/charger-faults/`, etc.). If the charger-faults job crashes, it only affects that topic — iot-telemetry continues writing without interruption.

**Monitoring:** Consumer lag > 5 minutes triggers an Azure Monitor alert. This indicates either the streaming job has crashed or the Event Hub topic is receiving more events than the job can process (scaling needed).

---

### Q19. A charger sends 5 identical telemetry events within 1 second (IoT device retry storm). How does the system handle this without counting the fault 5 times in the Maintenance dashboard?

**Answer:**

This is a multi-layer problem — each layer handles one aspect:

**Layer 1 — Bronze (append all 5):**
All 5 identical events land in `bronze/iot/charger_telemetry_raw/`. Bronze is append-only — no deduplication here. This is correct: we preserve the raw evidence of the retry storm, which is useful for device debugging.

**Layer 2 — Silver deduplication (Rule A):**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

# Dedup key: charger_id + event_ts + connector_id
window_spec = Window.partitionBy("charger_id", "event_ts", "connector_id") \
                    .orderBy(desc("_ingestion_ts"))

df = df.withColumn("row_num", row_number().over(window_spec)) \
       .filter(col("row_num") == 1) \
       .drop("row_num")
```
5 events with same `charger_id + event_ts + connector_id` → Silver keeps 1 (the latest `_ingestion_ts`).

**Layer 3 — Streaming watermark (Rule B):**
If any of the 5 events is delayed by more than 10 minutes from the watermark boundary, it goes to quarantine. But for a retry storm (same-second events), they all fall within the watermark — all 5 pass to dedup, where 4 are removed.

**Layer 4 — FactMaintenance (Gold):**
FactMaintenance creates one row per maintenance event, keyed on `charger_id + fault_ts`. Even if somehow 2 duplicate events slipped through Silver, the Gold MERGE on `charger_id + fault_ts` ensures only 1 maintenance row per fault.

**Result:** The Predictive Maintenance dashboard counts 1 fault for CHG-0042, not 5. The Bronze table shows 5 rows (for audit purposes). This is the correct separation of concerns.
