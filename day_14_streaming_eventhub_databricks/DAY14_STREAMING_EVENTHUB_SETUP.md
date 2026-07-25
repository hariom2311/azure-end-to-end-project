# Day 14 — Real-Time Streaming: Azure Event Hubs + Databricks Structured Streaming
**Session:** ~2 hours | **Goal:** Send live vehicle battery charging events from a Python producer to Azure Event Hubs, consume them in Databricks Structured Streaming, and land raw JSON into Bronze ADLS under `bronze/event-stream/vehicle_battery_live/`.

> **Prerequisites:** Day 1 resources must exist — `evdatalakedev` ADLS Gen2, `kv-ev-intelligence-dev` Key Vault, `dbw-ev-intelligence-dev` Databricks workspace, `dev-cluster`.
> **New resource today:** One new Event Hub topic `vehicle-battery-live` inside the existing `evh-ev-intelligence-dev` namespace.

---

## Glossary — New Concepts in Day 14

| Term | Plain English Definition |
|---|---|
| **Azure Event Hubs** | A managed message broker on Azure. Producers send events to it; consumers read them. Think of it as a high-throughput queue. Already exists in this project as `evh-ev-intelligence-dev`. |
| **Event Hub Topic (Entity)** | A named channel inside an Event Hub namespace. Each topic is independent. Today we add `vehicle-battery-live` alongside the existing `iot-telemetry` and `maintenance-alerts` topics. |
| **Partition** | An Event Hub topic is split into N partitions. Events are distributed across partitions. Consumers read from each partition in order. More partitions = more parallelism. |
| **Consumer Group** | A named view of an Event Hub topic. Each consumer group reads all events independently from the start. `$Default` is always present. Databricks Structured Streaming uses `$Default` by default. |
| **Connection String** | The credential string used by producers and consumers to authenticate to Event Hubs. Format: `Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=vehicle-battery-live`. Stored in Key Vault — never hardcoded. |
| **azure-eventhub (Python SDK)** | Python library for sending events to Event Hubs. Install via `pip install azure-eventhub`. |
| **Databricks Structured Streaming** | A Spark engine that reads from Event Hubs as a continuous stream, processing micro-batches every N seconds. Writes to Delta/Parquet on ADLS in near real-time. |
| **Checkpoint** | A folder on ADLS where Structured Streaming saves the last-read offset per partition. If the cluster restarts, streaming resumes exactly where it left off — no events lost, no duplicates. |
| **Watermark** | A delay tolerance window. Events arriving up to 10 minutes late are still accepted. Events beyond 10 minutes go to quarantine. Prevents late IoT events from blocking the streaming state. |
| **Micro-batch** | Structured Streaming reads events in small batches (every 30 seconds by default) rather than one event at a time. Each micro-batch is one ACID Delta write. |
| **EventDataBatch** | A container that groups multiple events into one AMQP frame. More efficient than sending events one by one — fewer network round-trips. |

---

## What You Will Have at the End of Day 14

- Event Hub topic `vehicle-battery-live` inside `evh-ev-intelligence-dev` (4 partitions)
- Connection string stored in Key Vault as `eventhub-vehicle-battery-conn-str`
- Python producer `send_vehicle_battery_events.py` — sends 1 event/second per vehicle (10 vehicles = 10 events/second)
- Databricks notebook `01_stream_vehicle_battery_to_bronze.ipynb` — reads Event Hub, writes Bronze Delta
- Raw JSON landing at `bronze/event-stream/vehicle_battery_live/` partitioned by `event_date`
- Checkpoint at `bronze/_checkpoints/vehicle-battery-live/`

---

## Architecture for Today

```
┌─────────────────────────────────────────────────────────────────┐
│  LOCAL MACHINE                                                   │
│                                                                  │
│  send_vehicle_battery_events.py                                  │
│  ├── Simulates 10 vehicles charging simultaneously              │
│  ├── Sends 1 event per vehicle per second                       │
│  └── Uses azure-eventhub SDK + connection string from Key Vault  │
└──────────────────────────┬──────────────────────────────────────┘
                           │  AMQP / HTTPS
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  AZURE EVENT HUBS                                                │
│  Namespace: evh-ev-intelligence-dev                             │
│  Topic:     vehicle-battery-live  (4 partitions)               │
│  Retention: 1 day                                               │
└──────────────────────────┬──────────────────────────────────────┘
                           │  Structured Streaming (AMQP)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  AZURE DATABRICKS                                                │
│  Cluster: dev-cluster                                           │
│  Notebook: 01_stream_vehicle_battery_to_bronze.ipynb           │
│                                                                  │
│  readStream (Event Hubs)                                         │
│    → parse JSON body                                             │
│    → add _ingestion_ts, _source, _pipeline_run_id              │
│    → writeStream (Delta, append)                                 │
│    → trigger every 30 seconds                                   │
│    → checkpoint: bronze/_checkpoints/vehicle-battery-live/      │
└──────────────────────────┬──────────────────────────────────────┘
                           │  abfss:// OAuth
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  ADLS GEN2 — evdatalakedev                                      │
│                                                                  │
│  bronze/                                                         │
│  ├── event-stream/                                               │
│  │   └── vehicle_battery_live/                                   │
│  │       ├── event_date=2025-07-15/                              │
│  │       │   ├── part-00000-...snappy.parquet                   │
│  │       │   └── part-00001-...snappy.parquet                   │
│  │       └── _delta_log/                                         │
│  └── _checkpoints/                                               │
│      └── vehicle-battery-live/                                   │
│          └── offsets/  (partition offsets saved per micro-batch) │
└─────────────────────────────────────────────────────────────────┘
```

---

## Event Schema — Vehicle Battery Live

Each event your Python script sends:

```json
{
  "event_id":                    "evt-8f3a-20250715-093042",
  "vehicle_id":                  "VH-0042",
  "session_id":                  "SES-20250715-0042",
  "station_id":                  "STN-007",
  "charger_id":                  "CHG-007-02",
  "battery_pct":                 67.4,
  "charging_rate_kw":            22.0,
  "battery_temp_c":              31.2,
  "state_of_charge_target_pct":  90,
  "estimated_minutes_to_full":   44,
  "event_ts":                    "2025-07-15T09:30:42Z"
}
```

| Field | Type | Description |
|---|---|---|
| `event_id` | string | Unique ID per event |
| `vehicle_id` | string | Vehicle identifier (VH-0001 to VH-0010) |
| `session_id` | string | Charging session this event belongs to |
| `station_id` | string | Station where the vehicle is plugged in |
| `charger_id` | string | Specific charger connector |
| `battery_pct` | float | Current battery % (0.0–100.0) — increases over time |
| `charging_rate_kw` | float | Power draw in kW at this moment |
| `battery_temp_c` | float | Battery temperature in Celsius |
| `state_of_charge_target_pct` | int | Driver's target charge level |
| `estimated_minutes_to_full` | int | Minutes left to reach target % |
| `event_ts` | string | UTC ISO-8601 timestamp |

---

## Part 1 — Add Event Hub Topic (5 min)

> **Cost: ₹0** — adding a topic to an existing namespace has no extra cost. The namespace already runs on Basic tier.

### 1.1 Open the existing Event Hub namespace

1. Azure Portal → search **Event Hubs** → click `evh-ev-intelligence-dev`
2. Left menu → **Event Hubs** (under Entities)
3. You should see existing topics: `iot-telemetry`, `maintenance-alerts`

### 1.2 Create the new topic

1. Click **+ Event Hub** (top of the list)
2. Fill in:

| Field | Value |
|---|---|
| **Name** | `vehicle-battery-live` |
| **Partition count** | `4` |
| **Retention (hours)** | `24` (1 day) |
| **Cleanup policy** | Delete |

3. Click **Review + create** → **Create**

Topic `vehicle-battery-live` now appears in the list.

### 1.3 Get the connection string

1. Click on `vehicle-battery-live` topic
2. Left menu → **Shared access policies**
3. Click **+ Add** to create a new policy:
   - **Policy name:** `producer-policy`
   - Check: **Send** only (not Listen or Manage — producer only needs Send)
4. Click **Create**
5. Click on `producer-policy` → copy **Primary connection string**

It looks like:
```
Endpoint=sb://evh-ev-intelligence-dev.servicebus.windows.net/;SharedAccessKeyName=producer-policy;SharedAccessKey=<key>;EntityPath=vehicle-battery-live
```

### 1.4 Store connection string in Key Vault

1. Azure Portal → `kv-ev-intelligence-dev` → **Secrets** → **+ Generate/Import**
2. Fill in:
   - **Name:** `eventhub-vehicle-battery-conn-str`
   - **Value:** paste the connection string from above
3. Click **Create**

---

## Part 2 — Install Python Dependency (2 min)

On your local machine:

```bash
pip install azure-eventhub
```

That is the only external dependency needed for the producer script.

---

## Part 3 — Python Producer Script

See file: `send_vehicle_battery_events.py`

**How to configure:**

Open `send_vehicle_battery_events.py` and set the connection string at the top. Two options:

**Option A — Environment variable (recommended):**
```powershell
# Windows PowerShell
$env:EH_CONN_STR = "Endpoint=sb://evh-ev-intelligence-dev.servicebus.windows.net/;SharedAccessKeyName=producer-policy;SharedAccessKey=<your-key>;EntityPath=vehicle-battery-live"

python send_vehicle_battery_events.py
```

**Option B — Direct in script (for local testing only — never commit to git):**
Edit line 10 in the script:
```python
CONN_STR = "Endpoint=sb://evh-ev-intelligence-dev..."
```

**What the script does:**
- Simulates 10 vehicles (VH-0001 to VH-0010) each with a charging session
- Every second: sends one event per vehicle (10 events total per loop)
- Battery % increases realistically each tick (based on `charging_rate_kw`)
- Stops automatically when all 10 vehicles reach their `state_of_charge_target_pct`
- Prints a summary line every 10 seconds

**Expected output:**
```
[2025-07-15 09:30:00] Sent batch of 10 events | VH-0001: 45.2% | VH-0002: 62.8% | ...
[2025-07-15 09:30:10] Sent batch of 10 events | VH-0001: 48.7% | VH-0002: 65.1% | ...
...
[2025-07-15 09:45:22] VH-0003 reached target 80% — stopping for this vehicle
[2025-07-15 09:52:11] All vehicles reached target. Total events sent: 8,220. Exiting.
```

---

## Part 4 — Databricks Streaming Notebook

See file: `01_stream_vehicle_battery_to_bronze.ipynb`

**What the notebook does step by step:**

```
Cell 1: Read secrets from Key Vault (connection string + ADLS OAuth)
Cell 2: Configure Spark with ADLS Gen2 OAuth credentials
Cell 3: Define Event Hub read configuration (topic, consumer group, max events per trigger)
Cell 4: readStream from Event Hubs — raw body is binary
Cell 5: Parse binary body → JSON string → explode into columns
Cell 6: Add Bronze metadata columns (_ingestion_ts, _source, _is_corrupt)
Cell 7: writeStream to Delta table at bronze/event-stream/vehicle_battery_live/
         - mode: append
         - trigger: processingTime = "30 seconds"
         - checkpoint: bronze/_checkpoints/vehicle-battery-live/
         - partition by: event_date
Cell 8: awaitTermination() — keeps streaming job alive
```

**How to run:**
1. Open `dbw-ev-intelligence-dev` Databricks workspace
2. Import the notebook or create it manually using the cells in the `.ipynb` file
3. Attach to `dev-cluster`
4. Run **Cell 1 → Cell 8** in order (Run All works too)
5. Cell 8 blocks — the streaming query is now live
6. In a second browser tab, start your Python producer script
7. Within 30–60 seconds you should see files appearing in Bronze

**How to verify the stream is working (in a new notebook cell or separate notebook):**
```python
# Read the Bronze Delta table and check row count (run this in a separate notebook cell)
df = spark.read.format("delta").load(
    "/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/event-stream/vehicle_battery_live/"
)
print(f"Total events in Bronze: {df.count()}")
df.orderBy("event_ts", ascending=False).show(5, truncate=False)
```

---

## Part 5 — Verify in Azure Portal (5 min)

### 5.1 Check Event Hub metrics

1. Azure Portal → `evh-ev-intelligence-dev` → click `vehicle-battery-live` topic
2. **Overview** tab → look at **Incoming messages** graph
3. Should show ~10 messages/second while the producer is running

### 5.2 Check Bronze folder

1. Azure Portal → `evdatalakedev` → **Storage browser** → **Blob containers** → `bronze`
2. Navigate: `event-stream` → `vehicle_battery_live`
3. You should see Delta table structure:
   ```
   bronze/event-stream/vehicle_battery_live/
   ├── _delta_log/
   │   ├── 00000000000000000000.json
   │   ├── 00000000000000000001.json
   │   └── ...
   └── event_date=2025-07-15/
       ├── part-00000-abc123.snappy.parquet
       └── part-00001-def456.snappy.parquet
   ```
4. New parquet files appear every 30 seconds (one per micro-batch)

---

## Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| `AuthenticationError` in Python | Wrong connection string | Re-copy from Key Vault secret `eventhub-vehicle-battery-conn-str` |
| `EntityPath` mismatch error | Connection string EntityPath ≠ `vehicle-battery-live` | Confirm the connection string ends with `;EntityPath=vehicle-battery-live` |
| Notebook Cell 1 fails: `secret not found` | Key vault secret name typo | Secret name must be exactly `eventhub-vehicle-battery-conn-str` |
| No files in Bronze after 2 minutes | Streaming job not running or wrong path | Check Cell 7 output — any error in the streaming query? |
| `DeltaIllegalStateException: checkpoint mismatch` | Old checkpoint from different schema | Delete `bronze/_checkpoints/vehicle-battery-live/` and restart stream |
| `OutOfMemoryError` on dev-cluster | dev-cluster is small (14GB RAM) | Reduce `maxEventsPerTrigger` to 500 in Cell 3 |
| Events in Event Hub but 0 rows in Delta | Consumer group lag | Wait one more trigger interval (30 sec) — first micro-batch takes longer |
| `AnalysisException: column not found` | JSON parse schema mismatch | Print raw body in Cell 5 to check actual JSON structure |

---

## Key Numbers for This Setup

| Metric | Value |
|---|---|
| Event Hub partitions | 4 |
| Events per second (producer) | 10 (1 per vehicle × 10 vehicles) |
| Micro-batch interval | 30 seconds |
| Expected rows per micro-batch | ~300 (10 events/sec × 30 sec) |
| Checkpoint location | `bronze/_checkpoints/vehicle-battery-live/` |
| Bronze partition column | `event_date` |
| Retention in Event Hub | 24 hours (1 day) |
| Consumer group used | `$Default` |

---

## Cost Summary

| Resource | Cost |
|---|---|
| Event Hub topic (Basic tier, 1 TU) | Already paying for namespace — ₹0 extra |
| Databricks `dev-cluster` (while running) | ~₹18–22/hour |
| ADLS Gen2 storage (small Parquet files) | ~₹0 |
| **Total for Day 14** | **~₹36–44 for a 2-hour session** |

> Always terminate the `dev-cluster` after the session. A streaming job left running overnight = ~₹400+ wasted.

---

## What Comes Next

| Day | What gets built |
|---|---|
| Day 15 | Silver transformation — read `bronze/event-stream/vehicle_battery_live/`, validate battery range (0–100%), dedup by `event_id`, write `silver/sl_vehicle_battery_live/` Delta |
| Day 16 | Gold mart — `mart_charging_progress_live` aggregated by vehicle + station + 5-min windows for the Live Charging Power BI dashboard |
