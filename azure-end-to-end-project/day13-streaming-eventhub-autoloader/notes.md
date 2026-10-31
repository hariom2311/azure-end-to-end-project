# Day 13 — Real-Time Streaming: Event Hub + Structured Streaming + Databricks

## Architecture Overview

```
Local Python Producer
  send_vehicle_battery_events.py
          │  10 events/sec (AMQP)
          ▼
  Azure Event Hub
  Namespace : evh-ev-intelligence-dev
  Topic     : vehicle-battery-live
  Partitions: 4   Retention: 24h
          │
          │  Spark Kafka connector (built-in, no Maven JAR)
          │  SASL_SSL auth via Kafka endpoint
          │  trigger: 30 seconds
          ▼
  Bronze Delta Table
  bronze/event-stream/vehicle_battery_live/
  partitioned by event_date
          │
          │  foreachBatch + DQ rules
          │  trigger: 60 seconds
          ▼
  Silver Delta Table                    ──► Quarantine (bad rows)
  silver/sl_vehicle_battery_live/           silver/quarantine/vehicle_battery_invalid/
          │
          │  foreachBatch + tumbling window
          │  trigger: 120 seconds
          ▼
  Gold Mart
  gold/mart_charging_progress_live/
  5-minute windows per vehicle+station
```

---

## Part 0: Fixing the Maven Allowlist Error

When you install `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22` on a Unity Catalog
cluster you may see:

```
PERMISSION_DENIED: 'com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22'
is not in the artifact allowlist
```

### Why it happens

Unity Catalog clusters enforce an **artifact allowlist** — only pre-approved Maven JARs can be
installed. This is a security control by the metastore admin.

### Fix A — Add to allowlist (needs metastore admin)

Run in Databricks SQL Editor as metastore admin:
```sql
ALTER METASTORE ADD ARTIFACT 'com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22';
```
Then retry the library install on the cluster.

### Fix B — Use the Kafka endpoint instead (recommended, no JAR needed)

Event Hub Standard tier exposes a **Kafka-compatible endpoint** on port 9093.
Spark has a **built-in Kafka connector** — no Maven library, no allowlist issue.

This is what the Bronze notebook now uses. The only change needed:

1. **Upgrade Event Hub to Standard tier** (Basic does not expose the Kafka endpoint)
   - Portal → your namespace → Settings → Pricing tier → Standard → Save

2. **Add a new Key Vault secret** — `eventhub-vehicle-battery-sasl-config`

   Value (paste as a single line, no line breaks):
   ```
   org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="<your-listen-connection-string>";
   ```
   Where `<your-listen-connection-string>` is the full connection string from
   `databricks-listen-policy` (same one you already stored).

   The username is always the literal string `$ConnectionString` — that is not a variable.

3. **No Maven library install needed** — remove the old EventHubs JAR from the cluster if installed.

### Comparison: EventHubs connector vs Kafka connector

| | EventHubs connector | Kafka connector (current) |
|---|---|---|
| Maven JAR | Required | Not needed |
| Allowlist issue | Yes on UC clusters | No |
| Event Hub tier | Basic+ | **Standard+** |
| Auth | SAS connection string | SASL/SSL with PLAIN mechanism |
| Metadata columns | `enqueuedTime`, `partition`, `offset` | `timestamp`, `partition`, `offset` |
| `body` column name | `body` | `value` |
| Behaviour | Identical — both read same events |

---

## Part 1: Azure Setup — Step-by-Step

### Step 1: Create Event Hub Namespace

1. Azure Portal → **Create a resource** → **Event Hubs**
2. Settings:
   - **Namespace name:** `evh-ev-intelligence-dev`
   - **Pricing tier:** Basic (sufficient for 1 topic, only `$Default` consumer group)
   - **Throughput units:** 1
   - **Region:** Central India (same as your ADF + Databricks)
   - **Resource group:** `rg-ev-intelligence-dev`
3. Click **Review + Create** → **Create**

### Step 2: Create Event Hub Topic

1. Open the namespace → **Event Hubs** (left panel) → **+ Event Hub**
2. Settings:
   - **Name:** `vehicle-battery-live`
   - **Partition count:** 4 (more partitions = more parallelism; Basic max = 32)
   - **Message retention:** 1 day (24 hours — Basic tier limit)
   - **Cleanup policy:** Delete
3. **Create**

### Step 3: Create SAS Policies

Go to: Namespace → Event Hubs → `vehicle-battery-live` → **Shared Access Policies**

Create two policies:

| Policy name | Permission | Used by |
|---|---|---|
| `producer-policy` | Send | Local Python script |
| `databricks-listen-policy` | Listen | Databricks notebooks |

For each: click **Add** → set name + permission → **Create**

After creation, click each policy and copy the **Connection string–primary key**.

### Step 4: Store Secrets in Key Vault

Go to your existing Key Vault (`kv-ev-intelligence-dev`) → **Secrets** → **+ Generate/Import**:

| Secret name | Value |
|---|---|
| `eventhub-vehicle-battery-conn-str` | Producer connection string (from `producer-policy`) |
| `eventhub-vehicle-battery-listen-conn-str` | Listener connection string (from `databricks-listen-policy`) |

### Step 5: Install Maven Library on Databricks Cluster

The Spark EventHubs connector is not pre-installed — you must install it:

1. Databricks → **Compute** → your cluster (`dev-cluster`) → **Libraries**
2. Click **Install new** → **Maven**
3. Coordinates: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`
4. **Install**
5. Wait for cluster to restart with the library loaded

> **Why Maven?** The connector is a JVM library. It bridges Spark Structured Streaming to the AMQP protocol used by Event Hub.

### Step 6: Create Databricks Secret Scope (if not done)

In Databricks, create a secret scope backed by your Key Vault:

```
databricks secrets create-scope --scope ev-kv-scope \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/kv-ev-intelligence-dev \
  --dns-name https://kv-ev-intelligence-dev.vault.azure.net/
```

Or use the Databricks UI: `https://<workspace>.azuredatabricks.net/?#secrets/createScope`

### Step 7: Upload Notebooks to Databricks Workspace

1. Databricks → **Workspace** → your user folder → create folder `day13`
2. Import each `.ipynb` file:
   - `nb_bronze_stream_vehicle_battery.ipynb`
   - `nb_silver_stream_vehicle_battery.ipynb`
   - `nb_gold_stream_charging_progress.ipynb`

### Step 8: Create the Streaming Job

Databricks → **Workflows** → **Create Job**

| Task name | Notebook | Cluster | Trigger |
|---|---|---|---|
| `bronze-stream` | `day13/nb_bronze_stream_vehicle_battery` | `dev-cluster` | None (runs forever) |
| `silver-stream` | `day13/nb_silver_stream_vehicle_battery` | `dev-cluster` | None |
| `gold-stream` | `day13/nb_gold_stream_charging_progress` | `dev-cluster` | None |

All three tasks run **in parallel** (no dependencies between them).  
Each notebook has a `while query.isActive: time.sleep(N)` keep-alive loop at the end.

Job name: `job-ev-streaming-pipeline`

### Step 9: Run the Producer Locally

```powershell
# Install dependency
pip install azure-eventhub

# Set env var (Windows PowerShell)
$env:EH_CONN_STR = "Endpoint=sb://evh-ev-intelligence-dev.servicebus.windows.net/;SharedAccessKeyName=producer-policy;SharedAccessKey=<key>;EntityPath=vehicle-battery-live"

# Run
python producer/send_vehicle_battery_events.py
```

You will see output like:
```
[Tick 0001] 10 events sent | Sample: VEH-001 bat=12.3% rate=75.0kW temp=26.4°C eta=44min
[Tick 0002] 10 events sent | Sample: VEH-001 bat=12.4% rate=75.0kW temp=25.9°C eta=43min
```

### Step 10: Monitor in Databricks

1. Trigger the job from **Workflows** → `job-ev-streaming-pipeline` → **Run now**
2. Watch each task's live output — batch progress prints every 30/60/120s
3. After ~5 minutes run the verify cells (Cell 9 in Bronze, Cell 7 in Silver, Cell 7 in Gold)

---

## Part 2: Key Concepts

### Event Hub Concepts

| Concept | Description |
|---|---|
| **Namespace** | Top-level container — like an Azure Service Bus namespace. Holds multiple topics. |
| **Topic (Event Hub)** | Named stream — `vehicle-battery-live`. Producers send to a topic, consumers read from it. |
| **Partition** | Ordered log within a topic. 4 partitions = up to 4 parallel Spark readers. Events with the same partition key go to the same partition (order preserved). |
| **Consumer Group** | Named cursor position — allows multiple independent readers of the same topic. Basic tier only supports `$Default`. Standard+ allows custom groups. |
| **SAS Policy** | Shared Access Signature — gives Send, Listen, or Manage permissions to a specific connection string. |
| **Retention** | How long events stay in Event Hub before deletion. Basic: 1 day max. Standard: 7 days max. |
| **Throughput Unit (TU)** | 1 TU = 1 MB/s ingress, 2 MB/s egress. Scale TUs up under load. |

### Spark Structured Streaming Concepts

| Concept | Description |
|---|---|
| **readStream** | Creates a streaming DataFrame — each micro-batch gets a slice of new data |
| **writeStream** | Writes stream output — append/complete/update output modes |
| **trigger(processingTime)** | Run a micro-batch every N seconds |
| **trigger(availableNow)** | Process all pending data then stop (for scheduled jobs via ADF) |
| **checkpointLocation** | ADLS path where Spark saves its position — guarantees exactly-once on restart |
| **foreachBatch** | Calls your function with each micro-batch as a static DataFrame — enables MERGE, dedup, DQ |
| **outputMode("append")** | Only new rows written each batch (suitable for Bronze raw ingest) |
| **outputMode("complete")** | Entire result rewritten each batch (suitable for small aggregations) |

### Why foreachBatch for Silver and Gold?

- `writeStream.format("delta")` with `outputMode("append")` does simple append — no dedup, no MERGE
- MERGE semantics (upsert on `event_id`) require a static DataFrame, not a stream
- `foreachBatch` gives you a static batch DataFrame each trigger — you can run any Spark/Delta operation inside it
- This is the standard pattern for streaming upserts into Delta

### Tumbling vs Sliding Windows

| | Tumbling Window | Sliding Window |
|---|---|---|
| Definition | Fixed non-overlapping intervals | Fixed size, moves by a step |
| Example | 5-min window every 5 min | 5-min window every 1 min |
| Use case | Aggregating per period (billing, KPIs) | Running averages, anomaly detection |
| Spark syntax | `window(ts, '5 minutes')` | `window(ts, '5 minutes', '1 minute')` |
| Our use | 5-min battery charging progress | Not used today |

### Checkpoint and Exactly-Once

```
ADLS Checkpoint Directory
  ├── commits/       — which batches are committed
  ├── offsets/       — what offsets/Event Hub positions were read
  └── metadata       — stream metadata

On restart:
  Spark reads the latest committed offset → continues from exactly there
  Events already processed are NOT re-read → exactly-once delivery
  Events in Event Hub but not yet read → picked up in the next batch
```

### Event Hub vs Kafka vs Azure Service Bus

| | Azure Event Hub | Apache Kafka | Azure Service Bus |
|---|---|---|---|
| Protocol | AMQP / Kafka-compatible | Native Kafka | AMQP |
| Use case | High-throughput telemetry | High-throughput + complex routing | Transactional messaging |
| Consumer model | Consumer groups, pull | Consumer groups, pull | Queues/Topics, push or pull |
| Retention | 1–90 days | Configurable | Until consumed (max 14 days) |
| Ordering | Per partition | Per partition | Per session |
| Spark integration | EventHubs connector (Maven) | Built-in | Not native |
| Scale | Up to 40 TU (Standard) | Unlimited (self-hosted) | Up to 80 GB |
| Kafka compatibility | Yes (Standard+) | Native | No |

---

## Part 3: ADLS Path Summary

| Layer | Path |
|---|---|
| Bronze data | `abfss://bronze@evdatalakedev2.dfs.core.windows.net/event-stream/vehicle_battery_live/` |
| Bronze checkpoint | `abfss://bronze@evdatalakedev2.dfs.core.windows.net/_checkpoints/vehicle-battery-live/` |
| Silver data | `abfss://silver@evdatalakedev2.dfs.core.windows.net/sl_vehicle_battery_live/` |
| Silver quarantine | `abfss://silver@evdatalakedev2.dfs.core.windows.net/quarantine/vehicle_battery_invalid/` |
| Silver checkpoint | `abfss://silver@evdatalakedev2.dfs.core.windows.net/_checkpoints/vehicle-battery-live/` |
| Gold mart | `abfss://gold@evdatalakedev2.dfs.core.windows.net/mart_charging_progress_live/` |
| Gold checkpoint | `abfss://gold@evdatalakedev2.dfs.core.windows.net/_checkpoints/charging-progress-live/` |

---

## Part 4: Practice Exercises

### Exercise 1 — Producer Modification
Modify `send_vehicle_battery_events.py` to:
- Add a `10%` chance each tick that a vehicle sends a corrupt event (missing `event_id`)
- Observe those events get quarantined in the Silver notebook
- Query the quarantine table and count corrupt events per vehicle

### Exercise 2 — DQ Rule Extension
In the Silver notebook, add a 10th DQ rule:
- **R10:** `battery_pct_end > battery_pct_start` within any single vehicle's session (i.e., battery should not go backwards while charging)
- This requires a window function across rows — think about how to implement this in `foreachBatch`

### Exercise 3 — Sliding Window
Change the Gold notebook from a 5-minute tumbling window to a 5-minute sliding window with a 1-minute slide:
- `F.window(F.col("event_ts_ts"), "5 minutes", "1 minute")`
- Observe how the row count in the Gold mart changes (each event now appears in 5 windows instead of 1)

### Exercise 4 — Schema Evolution
Add a new field `grid_frequency_hz` (float, default 50.0) to the producer event.
- The Bronze schema uses `mergeSchema=true` so it should accept the new column
- Verify the new column appears in Bronze
- Update the Silver `EVENT_SCHEMA` to include it
- Observe how Silver handles the backfill of old rows (they get `null` for `grid_frequency_hz`)

### Exercise 5 — Watermarking
The Gold tumbling window aggregation has a latency problem: late-arriving Silver events (batched up to 60s after event time) can fall into the wrong window.
- Add a watermark to the Silver stream before the Gold aggregation: `.withWatermark("event_ts_ts", "2 minutes")`
- Research what Structured Streaming does with events that arrive after the watermark
- What is the trade-off between watermark delay and result accuracy?

### Exercise 6 — Monitoring Query
Write a SQL query that shows:
- For each vehicle, the battery charging curve over time (window_start vs battery_pct_end)
- The window where `overtemp_flag = true` occurred
- The estimated time remaining at each window

---

## Part 5: Interview Questions and Answers

---

**Q1. What is Azure Event Hub and how does it differ from a traditional message queue?**

Event Hub is a **big data streaming platform** — it is designed for high-throughput, time-ordered event ingestion (millions of events/sec). It uses a **pull model** via consumer groups: multiple readers each maintain their own position (offset) in the log. Unlike a queue (Service Bus), events are not deleted after being consumed — they stay for the retention period and can be re-read (replay).

A queue (Service Bus) delivers each message to exactly one consumer then deletes it. Event Hub delivers the same message to every consumer group independently.

---

**Q2. What is a partition in Event Hub and why does it matter for Spark?**

A partition is an ordered, independent log within the Event Hub topic. Events within a partition are strictly ordered by arrival time and offset. Events across partitions have no ordering guarantee.

For Spark: the EventHubs connector maps each partition to a Spark task. With 4 partitions, Spark can read in parallel with up to 4 concurrent tasks. More partitions = more parallelism = higher throughput. You cannot reduce partition count after creation.

---

**Q3. Explain `foreachBatch` — when do you use it and why?**

`foreachBatch` is a `writeStream` sink that calls a user function with each micro-batch as a **static DataFrame**. You use it when:

1. You need **MERGE** semantics (streaming write mode doesn't support MERGE)
2. You need to **split one batch** into multiple sinks (clean to Silver, bad to quarantine)
3. You need **custom deduplication** within each batch before writing
4. You need to call non-streaming APIs (e.g., external REST calls per batch)

Inside `foreachBatch` you have the full Spark API — any DataFrame transformation, DeltaTable MERGE, window functions, etc.

---

**Q4. What is a checkpoint in Structured Streaming and what happens if you delete it?**

A checkpoint is a directory in ADLS where Spark saves its streaming state:
- **offsets/** — what Event Hub offsets/positions have been read in each batch
- **commits/** — which batches have been successfully written to the sink
- **metadata** — stream name and configuration

If you delete the checkpoint, Spark loses its position. The next run starts from the configured `startingPosition` (in our case `-1` = latest). You will **not** re-read historical events, but you may **miss events** that arrived while the stream was down. For Event Hub with 24h retention, you could set `startingPosition` to an absolute timestamp to replay missed events.

---

**Q5. What is the difference between tumbling and sliding windows? When would you use each?**

- **Tumbling window:** Fixed-size, non-overlapping intervals. Each event belongs to exactly one window. Use for billing, KPI reporting, partitioned aggregations.
  - `window("event_ts", "5 minutes")` → windows at :00, :05, :10, ...

- **Sliding window:** Fixed-size windows that overlap. Each event may belong to multiple windows. Use for rolling averages, anomaly detection, smoothed metrics.
  - `window("event_ts", "5 minutes", "1 minute")` → at :00 (covering -5:00 to :00), at :01 (covering -4:00 to :01), ...

In our EV project: tumbling window for battery progress per charging session (discrete 5-minute periods). Sliding window would be better for detecting temperature spikes that span a window boundary.

---

**Q6. What does `maxEventsPerTrigger` control in the EventHubs connector?**

It caps how many events are read from Event Hub in a single micro-batch. This prevents a backpressure situation where a large backlog causes a single batch to be enormous, causing OOM errors or very long batch times.

If your producer sends 10,000 events/sec and your trigger is 30s, without the cap a batch could have 300,000 events. With `maxEventsPerTrigger = 1000`, each batch caps at 1,000 events, and the stream catches up gradually over multiple batches.

---

**Q7. What is the Event Hub Kafka endpoint and when would you use it?**

Standard and Premium tier Event Hubs expose a **Kafka-compatible endpoint**. This means you can point an existing Kafka producer/consumer (or Spark Kafka connector) at Event Hub without any code changes — just change the bootstrap server to `evh-ev-intelligence-dev.servicebus.windows.net:9093` and use SASL/SSL authentication.

Use this when migrating from on-prem Kafka to Azure, or when using tools that only support Kafka (e.g., Debezium, Kafka Connect).

---

**Q8. Why is watermarking important in streaming aggregations?**

Without a watermark, Spark must keep all window state in memory indefinitely because it doesn't know when a window is "closed" — late data might still arrive.

A watermark tells Spark: "discard any event more than N minutes late." This lets Spark:
1. Finalize windows and evict them from memory (state cleanup)
2. Drop genuinely late events rather than updating already-committed windows

Trade-off: a larger watermark = more complete results but more memory use. A smaller watermark = less memory but more dropped late events.

---

**Q9. How does exactly-once delivery work in Structured Streaming to Delta?**

Exactly-once is achieved through two mechanisms working together:

1. **Checkpoint (read side):** Spark records which offsets it has read. On restart it resumes from the last committed offset — events are never re-read.

2. **Delta idempotent writes (write side):** Each batch write has a `txnVersion` and `txnAppId` written to the Delta log. If a batch fails after writing data but before committing the checkpoint, on restart Spark will try to re-write the same batch. Delta detects the duplicate `txnVersion` and silently skips it.

The combination means: each event from Event Hub is written to Delta exactly once, even across crashes and restarts.

---

**Q10. How would you scale this pipeline if the producer sends 1 million events/second?**

1. **Event Hub:** Scale from 1 TU to 10+ TUs (Standard tier). Increase partitions to 32 (max). Premium tier for > 40 TU.
2. **Spark cluster:** Add more workers. Each partition maps to one task — with 32 partitions, 32 parallel readers.
3. **Trigger interval:** Keep at 30s for Bronze — micro-batches stay small.
4. **maxEventsPerTrigger:** Increase or remove to let large batches through.
5. **ADLS throughput:** Ensure ADLS Gen2 account is in the same region. Enable hierarchical namespace for better write performance.
6. **Silver and Gold:** Scale foreachBatch workers. Consider partitioning Silver by `vehicle_id` for faster MERGE.
7. **Monitoring:** Use Databricks Ganglia / Spark UI to identify bottleneck — usually is it CPU (parsing), network (ADLS write), or memory (window state)?

---

## Part 6: Troubleshooting Common Errors

| Error | Cause | Fix |
|---|---|---|
| `ClassNotFoundException: org.apache.spark.eventhubs.*` | Maven library not installed on cluster | Install `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22` via Compute → Libraries |
| `IllegalArgumentException: Connection string must include EntityPath` | Connection string missing the topic path | Use the **entity-level** connection string (from the topic's SAS policy, not the namespace) |
| `org.apache.spark.SparkException: Job aborted due to stage failure: Task ... lost` + OOM | Batch too large | Reduce `maxEventsPerTrigger` |
| `AnalysisException: path ... does not exist` on checkpoint | ADLS path wrong or no write permission | Verify path and grant WRITE on the external location to the SP |
| Silver MERGE fails: `ConcurrentAppendException` | Two Silver stream jobs writing simultaneously | Ensure only one Silver task runs at a time (one Databricks job task, not two parallel runs) |
| `KeyError: EH_CONN_STR` in producer | Environment variable not set | Run `$env:EH_CONN_STR = "..."` before `python send_vehicle_battery_events.py` |
| Gold query produces no rows | Silver stream hasn't run yet / checkpoint ahead of Silver | Wait for Silver to complete at least one batch, then trigger Gold |
