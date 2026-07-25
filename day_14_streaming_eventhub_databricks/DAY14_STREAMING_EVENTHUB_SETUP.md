# Day 14 — Real-Time Streaming: Azure Event Hubs + Databricks Structured Streaming
**Session:** ~2.5 hours | **Goal:** Build a complete streaming pipeline from scratch — provision an Event Hub namespace and topic, send live vehicle battery events from a Python producer, consume them in Databricks Structured Streaming, and land raw JSON as a Delta table in Bronze ADLS under `bronze/event-stream/vehicle_battery_live/`.

> **Prerequisites:** Day 1 resources must exist — `evdatalakedev` ADLS Gen2, `kv-ev-intelligence-dev` Key Vault, `dbw-ev-intelligence-dev` Databricks workspace, `dev-cluster`, secret scope `kv-ev-scope`.
> **Starting from scratch today:** Event Hubs namespace, topic, consumer group, Databricks Maven library — all created in this session.

---

## Glossary — New Concepts in Day 14

| Term | Plain English Definition |
|---|---|
| **Azure Event Hubs** | A managed, high-throughput message broker on Azure. Think of it as a post box — producers drop events in, consumers read them out. It is designed for millions of events per second. |
| **Event Hub Namespace** | The top-level container resource for Event Hubs, like a server that holds multiple topics. You pay at the namespace level (Throughput Units). One namespace can contain many topics. |
| **Throughput Unit (TU)** | A unit of capacity for an Event Hub namespace. 1 TU = 1 MB/s ingest or 2 MB/s egress. Basic tier minimum is 1 TU — more than enough for this project. |
| **Event Hub Topic (Entity)** | A named channel inside an Event Hub namespace. Producers send to a specific topic; consumers read from it. Each topic is independent. Today's topic: `vehicle-battery-live`. |
| **Partition** | Each topic is split into N partitions. Events are distributed across partitions (by partition key or round-robin). More partitions = more parallel consumer readers. Each partition is an ordered, immutable log. |
| **Consumer Group** | A named "view" of a topic. Multiple consumer groups can each read all events from the same topic independently — they don't affect each other. `$Default` always exists. Basic tier only supports `$Default` — additional consumer groups require Standard tier. |
| **Shared Access Policy (SAS)** | A named credential (key) on an Event Hub that grants specific permissions: Send, Listen, or Manage. Producers need Send only. Consumers need Listen only. Never give both to the same key. |
| **Connection String** | The credential string built from a SAS policy. Format: `Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy>;SharedAccessKey=<key>;EntityPath=<topic>`. |
| **Maven Library (Spark Connector)** | A JAR file that Databricks downloads from Maven Central and installs on a cluster. The Event Hubs Spark connector (`azure-eventhubs-spark`) adds `format("eventhubs")` to Structured Streaming. Without it, the notebook fails immediately. |
| **azure-eventhub (Python SDK)** | Python library for sending/receiving events from a local script. Different from the Spark connector — this one runs on your laptop, not inside Databricks. Install via `pip install azure-eventhub`. |
| **Databricks Structured Streaming** | A Spark engine that reads from Event Hubs as a continuous stream, processing small batches every N seconds. Each batch is one atomic Delta write — exactly-once semantics. |
| **Checkpoint** | A folder on ADLS where Structured Streaming saves the last-committed offset per partition after every micro-batch. If the Databricks cluster restarts, streaming resumes exactly where it left off — no events lost, no duplicates. |
| **Offset** | The position of an event within a partition — like a line number in a log file. Checkpointing saves the last-read offset so restarts are safe. |
| **Micro-batch** | Structured Streaming processes events in periodic small batches (every 30 seconds here) rather than one event at a time. Each micro-batch is one ACID Delta write. |
| **EventDataBatch** | Python SDK concept — groups multiple events into one AMQP network frame. More efficient than sending events one by one. The producer script uses this. |
| **AMQP** | Advanced Message Queuing Protocol — the network protocol Event Hubs uses. The Python SDK and Spark connector both use AMQP under the hood. You don't configure it directly. |

---

## What You Will Have at the End of Day 14

- Azure Event Hub namespace `evh-ev-intelligence-dev` (Basic, 1 TU, Central India)
- Event Hub topic `vehicle-battery-live` (4 partitions, 1-day retention)
- SAS policy `producer-policy` (Send only) — for Python producer
- SAS policy `databricks-listen-policy` (Listen only) — for Databricks consumer
- Dedicated consumer group `databricks-consumer` on the topic
- Connection strings stored in Key Vault:
  - `eventhub-vehicle-battery-conn-str` — producer connection string
  - `eventhub-vehicle-battery-listen-conn-str` — Databricks listen connection string
- Maven library `azure-eventhubs-spark` installed on `dev-cluster`
- Python producer `send_vehicle_battery_events.py` running locally — 10 events/second
- Databricks notebook `01_stream_vehicle_battery_to_bronze.ipynb` — reads Event Hub, writes Bronze Delta
- Delta table at `bronze/event-stream/vehicle_battery_live/` partitioned by `event_date`
- Checkpoint at `bronze/_checkpoints/vehicle-battery-live/`

---

## Architecture for Today

```
┌─────────────────────────────────────────────────────────────────────┐
│  LOCAL MACHINE                                                       │
│                                                                      │
│  send_vehicle_battery_events.py                                      │
│  ├── Simulates 10 vehicles charging simultaneously                  │
│  ├── Sends 1 event per vehicle per second (10 events/sec total)     │
│  ├── Uses azure-eventhub Python SDK                                  │
│  └── Auth: producer-policy connection string (Send only)            │
└────────────────────────────┬────────────────────────────────────────┘
                             │  AMQP  (port 5671 / 443 fallback)
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  AZURE EVENT HUBS                                                    │
│  Namespace:  evh-ev-intelligence-dev  (Basic, 1 TU, Central India) │
│  Topic:      vehicle-battery-live  (4 partitions, 1-day retention) │
│  ├── Partition 0  ──────────────────────────────────────────────┐  │
│  ├── Partition 1  ──────── events distributed round-robin ──────┤  │
│  ├── Partition 2  ──────────────────────────────────────────────┤  │
│  └── Partition 3  ──────────────────────────────────────────────┘  │
│  Consumer group: databricks-consumer                                │
└────────────────────────────┬────────────────────────────────────────┘
                             │  AMQP  (Listen policy)
                             │  Spark connector: azure-eventhubs-spark
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  AZURE DATABRICKS                                                    │
│  Workspace: dbw-ev-intelligence-dev                                 │
│  Cluster:   dev-cluster  (+ Maven lib azure-eventhubs-spark)       │
│  Notebook:  01_stream_vehicle_battery_to_bronze.ipynb              │
│                                                                      │
│  readStream("eventhubs")                                             │
│    → body (binary) → cast to string → from_json → typed columns    │
│    → add: _ingestion_ts, _source, _is_corrupt, event_date          │
│    → writeStream("delta", append, trigger=30s)                      │
│    → checkpoint: bronze/_checkpoints/vehicle-battery-live/          │
└────────────────────────────┬────────────────────────────────────────┘
                             │  abfss:// + Service Principal OAuth
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  ADLS GEN2 — evdatalakedev                                          │
│                                                                      │
│  bronze/                                                             │
│  ├── event-stream/                                                   │
│  │   └── vehicle_battery_live/           ← Delta table root        │
│  │       ├── _delta_log/                 ← transaction log          │
│  │       │   ├── 00000000000000000000.json                          │
│  │       │   └── 00000000000000000001.json                          │
│  │       └── event_date=2025-07-15/      ← date partition          │
│  │           ├── part-00000-....snappy.parquet                      │
│  │           └── part-00001-....snappy.parquet                      │
│  └── _checkpoints/                                                   │
│      └── vehicle-battery-live/                                       │
│          ├── offsets/     ← last Event Hub offset per partition     │
│          └── commits/     ← which micro-batches fully committed     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Event Schema — Vehicle Battery Live

Each event your Python script sends looks like this:

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
| `event_id` | string | Unique ID per event — `evt-<random8hex>-<YYYYMMDD>-<HHmmss>` |
| `vehicle_id` | string | Vehicle identifier (VH-0001 to VH-0010) |
| `session_id` | string | Charging session this event belongs to |
| `station_id` | string | Station where the vehicle is plugged in (STN-001 to STN-005) |
| `charger_id` | string | Specific charger connector at that station |
| `battery_pct` | float | Current battery % (0.0–100.0) — increases each second |
| `charging_rate_kw` | float | Power draw in kW at this moment (7.4 / 22.0 / 50.0 / 150.0) |
| `battery_temp_c` | float | Battery temperature in Celsius — rises as battery charges above 60% |
| `state_of_charge_target_pct` | int | Driver's target charge level (80 / 85 / 90 / 95 / 100) |
| `estimated_minutes_to_full` | int | Minutes remaining to reach target % |
| `event_ts` | string | UTC ISO-8601 timestamp of the event |

---

## Part 1 — Create Event Hub Namespace (10 min)

> **Cost:** ~₹11/month for Basic tier 1 TU. Free if your subscription still has trial credits.

### 1.1 Open the portal and navigate to Event Hubs

1. Go to [https://portal.azure.com](https://portal.azure.com)
2. In the top search bar type **Event Hubs** → click **Event Hubs** (the service, not a specific resource)
3. Click **+ Create** (top left)

### 1.2 Fill in the namespace details

On the **Create Namespace** page:

| Field | Value |
|---|---|
| **Subscription** | your subscription |
| **Resource group** | `rg-ev-intelligence-dev` |
| **Namespace name** | `evh-ev-intelligence-dev` |
| **Location** | `Central India` |
| **Pricing tier** | `Basic` |
| **Throughput Units** | `1` |

> **Why Basic tier?** Basic is the cheapest. It supports up to 1 MB/s ingest — more than enough for 10 events/second. The only thing Basic lacks vs Standard is Kafka protocol support — we don't need that here.

Click **Review + create** → **Create**.

Wait ~1 minute for deployment to complete, then click **Go to resource**.

### 1.3 Confirm the namespace is ready

On the namespace overview page you should see:
- **Status:** Active
- **Pricing tier:** Basic
- **Throughput Units:** 1
- **Location:** Central India

---

## Part 2 — Create Event Hub Topic (5 min)

### 2.1 Create the topic

1. On the namespace page, left menu → **Event Hubs** (under Entities section)
2. Click **+ Event Hub** (top of the list)
3. Fill in:

| Field | Value | Why |
|---|---|---|
| **Name** | `vehicle-battery-live` | The topic name — producers and consumers reference this |
| **Partition count** | `4` | 4 parallel reader lanes — matches 4-partition pattern in this project |
| **Retention (hours)** | `24` | Events are kept for 1 day — if Databricks is down, it can catch up |
| **Cleanup policy** | `Delete` | Old events are deleted after retention period |

4. Click **Review + create** → **Create**

Topic `vehicle-battery-live` now appears in the list.

### 2.2 Consumer group — Basic tier note

> **Basic tier limitation:** The "+ Consumer group" button on the topic page is disabled on Basic tier. Basic only supports the built-in `$Default` consumer group — you cannot create additional ones.
>
> **This is fine for our setup.** Databricks is the only consumer reading `vehicle-battery-live`, so `$Default` works perfectly. The notebook is already configured to use `$Default`.
>
> If you ever need multiple independent consumers (e.g., Stream Analytics + Databricks reading the same topic simultaneously), you would need to upgrade to Standard tier (~₹830/month). For this project, Basic + `$Default` is sufficient.

To verify `$Default` exists:
1. Click on `vehicle-battery-live` topic
2. Left menu → **Consumer groups** (under Entities)
3. You will see `$Default` listed — this is what the Databricks notebook uses

---

## Part 3 — Create SAS Policies and Connection Strings (10 min)

You need two separate SAS policies:
- One for the **Python producer** → Send permission only
- One for the **Databricks consumer** → Listen permission only

> **Why separate policies?** Least-privilege principle. If the producer connection string leaks, an attacker can only send garbage events — they cannot read your data. If the listen key leaks, they can read events but cannot inject fake ones.

### 3.1 Create producer SAS policy (Send only)

1. Still on the `vehicle-battery-live` topic page
2. Left menu → **Shared access policies**
3. Click **+ Add**
4. Fill in:
   - **Policy name:** `producer-policy`
   - Check: **Send** ✅
   - Leave: **Listen** ☐ and **Manage** ☐
5. Click **Create**

### 3.2 Copy producer connection string

1. Click on `producer-policy` in the list
2. A right panel opens — copy **Primary connection string**

It looks like:
```
Endpoint=sb://evh-ev-intelligence-dev.servicebus.windows.net/;SharedAccessKeyName=producer-policy;SharedAccessKey=AbCdEf...==;EntityPath=vehicle-battery-live
```

Save this — you will paste it into Key Vault in Part 4.

### 3.3 Create Databricks listen SAS policy (Listen only)

1. Click **+ Add** again
2. Fill in:
   - **Policy name:** `databricks-listen-policy`
   - Check: **Listen** ✅
   - Leave: **Send** ☐ and **Manage** ☐
3. Click **Create**

### 3.4 Copy Databricks listen connection string

1. Click on `databricks-listen-policy`
2. Copy **Primary connection string**

It looks like:
```
Endpoint=sb://evh-ev-intelligence-dev.servicebus.windows.net/;SharedAccessKeyName=databricks-listen-policy;SharedAccessKey=XyZwVu...==;EntityPath=vehicle-battery-live
```

Save this separately — different from the producer string.

---

## Part 4 — Store Connection Strings in Key Vault (5 min)

Both connection strings go into Key Vault. Notebooks and scripts read them via `dbutils.secrets.get()` — credentials never hardcoded.

### 4.1 Store producer connection string

1. Azure Portal → search **Key vaults** → click `kv-ev-intelligence-dev`
2. Left menu → **Secrets** → click **+ Generate/Import**
3. Fill in:
   - **Upload options:** Manual
   - **Name:** `eventhub-vehicle-battery-conn-str`
   - **Value:** paste the producer connection string from Part 3.2
4. Click **Create**

### 4.2 Store Databricks listen connection string

1. Click **+ Generate/Import** again
2. Fill in:
   - **Name:** `eventhub-vehicle-battery-listen-conn-str`
   - **Value:** paste the Databricks listen connection string from Part 3.4
3. Click **Create**

### 4.3 Verify both secrets exist

Left menu → **Secrets** — you should now see:

```
eventhub-vehicle-battery-conn-str        (Enabled)
eventhub-vehicle-battery-listen-conn-str (Enabled)
```

---

## Part 5 — Install Event Hubs Spark Connector on Databricks Cluster (10 min)

> **This step is critical.** Without the Maven library installed on the cluster, `format("eventhubs")` in the streaming notebook will throw `java.lang.ClassNotFoundException`. Do this before running the notebook.

The Spark connector for Event Hubs is **not** bundled with Databricks by default. It must be installed as a Maven library.

**Maven coordinate:**
```
com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22
```

### 5.1 Open the Databricks cluster page

1. Go to your Databricks workspace `dbw-ev-intelligence-dev`
2. Left sidebar → **Compute** (the cluster icon)
3. Click on `dev-cluster` in the list

> If `dev-cluster` is **Terminated**, click **Start** and wait ~2 minutes for it to start before continuing.

### 5.2 Install the Maven library

1. On the `dev-cluster` page → click the **Libraries** tab
2. Click **Install new**
3. On the **Install library** dialog:
   - **Library source:** select **Maven**
   - **Coordinates:** paste exactly:
     ```
     com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22
     ```
   - Leave **Repository** and **Exclusions** blank
4. Click **Install**

### 5.3 Wait for installation to complete

The library status changes:
- **Pending** → installing
- **Installed** → ready ✅

This takes 1–3 minutes. The cluster does NOT need to restart — library installs on a running cluster take effect immediately for new notebook sessions.

### 5.4 Verify the library is installed

In the Libraries tab you should see:

```
Maven  |  com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22  |  Installed ✅
```

> **Which Databricks Runtime (DBR) version is `dev-cluster` on?**
> The coordinate `azure-eventhubs-spark_2.12:2.3.22` works with:
> - Databricks Runtime 10.x, 11.x, 12.x, 13.x (Scala 2.12)
> - If your cluster uses Scala 2.11 (older runtimes), use `_2.11` instead of `_2.12`
>
> To check: Compute → `dev-cluster` → Configuration tab → look at Runtime version.
> Any DBR 10+ uses Scala 2.12. Use `_2.12`.

---

## Part 6 — Install Python Dependency on Local Machine (2 min)

The Python producer script (`send_vehicle_battery_events.py`) runs on **your laptop**, not Databricks. It needs the Python Event Hubs SDK.

```bash
pip install azure-eventhub
```

Verify:
```bash
python -c "from azure.eventhub import EventHubProducerClient; print('OK')"
```

Expected output: `OK`

---

## Part 7 — Run the Python Producer (5 min)

### 7.1 Set the connection string as environment variable

**Windows PowerShell:**
```powershell
$env:EH_CONN_STR = "Endpoint=sb://evh-ev-intelligence-dev.servicebus.windows.net/;SharedAccessKeyName=producer-policy;SharedAccessKey=<your-key>;EntityPath=vehicle-battery-live"
```

Replace `<your-key>` with the actual key from the `producer-policy` primary connection string.

> **Where to find it again:** Azure Portal → `evh-ev-intelligence-dev` → Event Hubs → `vehicle-battery-live` → Shared access policies → `producer-policy` → Primary connection string.

**Mac / Linux terminal:**
```bash
export EH_CONN_STR="Endpoint=sb://evh-ev-intelligence-dev.servicebus.windows.net/;SharedAccessKeyName=producer-policy;SharedAccessKey=<your-key>;EntityPath=vehicle-battery-live"
```

### 7.2 Run the script

Navigate to the `day_14_streaming_eventhub_databricks` folder and run:

```bash
python send_vehicle_battery_events.py
```

**Expected output (first 30 seconds):**
```
=================================================================
  Day 14 — Vehicle Battery Live Producer
  Target:   vehicle-battery-live
  Vehicles: 10
  Interval: 1.0s per round
=================================================================
Starting producer... Press Ctrl+C to stop.

  [09:30:10] Sent 100 events total (10 active) | VH-0001: 12.4% | VH-0002: 18.7% | VH-0003: 9.1%
  [09:30:20] Sent 200 events total (10 active) | VH-0001: 14.1% | VH-0002: 20.3% | VH-0003: 11.0%
```

Keep this running in the background while you set up Databricks.

### 7.3 Verify events arrived in Event Hub (optional quick check)

1. Azure Portal → `evh-ev-intelligence-dev` → click `vehicle-battery-live`
2. **Overview** tab → scroll down to **Messages** graph
3. Should show incoming messages. The graph refreshes every 1–5 minutes — wait a moment if it shows 0.

---

## Part 8 — Run the Databricks Streaming Notebook (15 min)

### 8.1 Import the notebook into Databricks

**Option A — Upload the .ipynb file:**
1. Databricks workspace → left sidebar → **Workspace**
2. Navigate to: **Users** → your user folder
3. Click the **⋮** (three dots) → **Import**
4. Select **File** → browse to `01_stream_vehicle_battery_to_bronze.ipynb`
5. Click **Import**

**Option B — Create manually:**
1. Databricks workspace → **Workspace** → **+ Create** → **Notebook**
2. Name: `01_stream_vehicle_battery_to_bronze`
3. Language: Python
4. Copy each cell from the `.ipynb` file

### 8.2 Attach to dev-cluster

1. Open the notebook
2. Top bar → click the cluster dropdown (shows "Detached" if not connected)
3. Select `dev-cluster`
4. Confirm `dev-cluster` shows **Running** (green dot)

### 8.3 Run cells in order

Click **Run All** (top bar) or run each cell with Shift+Enter:

| Cell | What it does | Expected output |
|---|---|---|
| Cell 1 | Read Key Vault secrets | `Secrets loaded successfully.` |
| Cell 2 | Configure ADLS Gen2 OAuth | `ADLS OAuth configured for: evdatalakedev` |
| Cell 3 | Define paths + Event Hub config | Prints Bronze and checkpoint paths |
| Cell 4 | Start readStream from Event Hubs | Prints raw stream schema |
| Cell 5 | Parse JSON body → columns | Prints parsed schema with all event fields |
| Cell 6 | Add Bronze metadata columns | Prints final schema with `_ingestion_ts`, `_source`, `_is_corrupt`, `event_date` |
| Cell 7 | Start writeStream to Delta | `Streaming query started. ID: ...` |
| Cell 8 | Monitor loop — blocks | Prints batch progress every 30 seconds |

**Cell 8 output after first micro-batch (wait ~30 seconds):**
```
Stream is live. Printing progress every 30 seconds.
Cancel this cell to stop the stream.

[Batch 0] Rows this batch: 300 | Input rate: 10.0 rows/sec | Processing rate: 285.4 rows/sec
[Batch 1] Rows this batch: 300 | Input rate: 10.0 rows/sec | Processing rate: 312.7 rows/sec
```

> **Cell 8 blocks intentionally.** The streaming query runs inside Databricks — Cell 8 is just monitoring it. You can cancel Cell 8 and the stream continues running. The stream only stops when you call `streaming_query.stop()` or terminate the cluster.

---

## Part 9 — Verify Data in Bronze (5 min)

### 9.1 Check Bronze folder in Azure Portal

1. Azure Portal → `evdatalakedev` → **Storage browser** → **Blob containers** → `bronze`
2. Navigate: `event-stream` → `vehicle_battery_live`
3. You should see:
   ```
   bronze/event-stream/vehicle_battery_live/
   ├── _delta_log/
   │   ├── 00000000000000000000.json   ← first transaction (table creation)
   │   └── 00000000000000000001.json   ← second transaction (first micro-batch)
   └── event_date=2025-07-15/
       ├── part-00000-abc123.snappy.parquet
       └── part-00001-def456.snappy.parquet
   ```
4. A new `part-XXXXX.snappy.parquet` file appears every 30 seconds

### 9.2 Read and verify in a new Databricks notebook cell

**Cancel Cell 8 first OR open a second notebook tab.** Then run Cell 9 (already in the notebook) or paste this:

```python
STORAGE_ACCOUNT = dbutils.secrets.get(scope="kv-ev-scope", key="adls-account-name")
BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/event-stream/vehicle_battery_live/"

df = spark.read.format("delta").load(BRONZE_PATH)
print(f"Total rows in Bronze: {df.count():,}")
df.orderBy("_ingestion_ts", ascending=False).show(5, truncate=False)
```

Expected output:
```
Total rows in Bronze: 1,800

+---------------------+-----------+-------------------+--------+------------+-----------+----------------+---------------+-----------------------------+------------------------+--------------------------+
|event_id             |vehicle_id |...                |battery_pct|charging_rate_kw|...
+---------------------+-----------+-------------------+--------+------------+-----------+----------------+
|evt-8f3a-20250715-...|VH-0001    |...                |72.4   |22.0            |...
...
```

---

## Part 10 — Stop the Stream When Done

### 10.1 Stop the streaming query

In the notebook, run in any cell:
```python
streaming_query.stop()
print("Stream stopped.")
```

Or cancel Cell 8 — this cancels the monitoring loop but the stream may still run. To be safe, always call `.stop()`.

### 10.2 Stop the Python producer

Press **Ctrl+C** in the terminal where `send_vehicle_battery_events.py` is running.

### 10.3 Terminate the Databricks cluster

> **Important — avoid cost.** A running cluster costs ~₹18–22/hour even with no notebooks attached.

1. Databricks workspace → **Compute** → `dev-cluster`
2. Click **Terminate**
3. Confirm

---

## Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| `python: EH_CONN_STR environment variable is not set` | Env var not set before running script | Set `$env:EH_CONN_STR` in the same PowerShell session, then run the script |
| `AuthenticationError: ...unauthorized` in Python | Wrong connection string or wrong key | Re-copy producer connection string from `producer-policy` → Primary connection string |
| `EntityPath not found` error | Topic name in conn string ≠ `vehicle-battery-live` | Check conn string ends with `;EntityPath=vehicle-battery-live` |
| Cell 4 fails: `java.lang.ClassNotFoundException: ...EventHubsSourceProvider` | Maven library not installed | Re-do Part 5 — install `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22` on `dev-cluster` |
| Cell 4 fails: `Library installed but class not found` | Notebook attached before library installed | Detach and re-attach notebook to `dev-cluster` |
| Cell 1 fails: `Secret does not exist` | Key Vault secret name typo | Exact name must be `eventhub-vehicle-battery-listen-conn-str` (for Cell 1 in notebook) |
| Cell 3 fails: `AttributeError: EventHubsUtils` | Maven lib installed but cluster needs detach/reattach | Detach notebook from cluster → re-attach |
| No files in Bronze after 2 minutes | writeStream not started or wrong path | Check Cell 7 ran without error. Check `streaming_query.isActive` returns `True` |
| `DeltaIllegalStateException: checkpoint mismatch` | Restarted with incompatible schema change | Delete checkpoint folder: `dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)` then restart stream |
| `OutOfMemoryError` on dev-cluster | Too many events per micro-batch | Reduce `maxEventsPerTrigger` from 1000 to 200 in Cell 3 |
| Events in Event Hub but 0 rows in Delta | Wrong consumer group or first batch delay | Confirm `databricks-consumer` group exists. Wait one full 30-second trigger cycle. |
| `AnalysisException: column X not found` | JSON schema in Cell 5 doesn't match actual event | Print `raw_stream_df` body in a test cell: `display(raw_stream_df.selectExpr("CAST(body AS STRING)").limit(3))` |
| Databricks cluster won't start | Quota limit | Azure Portal → Subscriptions → Usage + quotas → check vCPU quota for Central India |

---

## Key Numbers for This Setup

| Metric | Value |
|---|---|
| Event Hub namespace tier | Basic |
| Throughput Units | 1 |
| Event Hub partitions | 4 |
| Consumer group for Databricks | `$Default` (Basic tier only supports this) |
| Maven library coordinate | `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22` |
| Events per second (producer) | 10 (1 per vehicle × 10 vehicles) |
| Micro-batch interval | 30 seconds |
| Expected rows per micro-batch | ~300 |
| Checkpoint path | `bronze/_checkpoints/vehicle-battery-live/` |
| Bronze partition column | `event_date` |
| Event retention in Event Hub | 24 hours |

---

## Cost Summary

| Resource | Cost |
|---|---|
| Event Hub namespace (Basic, 1 TU) | ~₹11/month (~₹0.37/day) |
| Event Hub ingress (first 1M events/month free on Basic) | ₹0 for this session |
| Databricks `dev-cluster` (while running) | ~₹18–22/hour |
| ADLS Gen2 storage (tiny Parquet files) | ~₹0 |
| **Total for a 2.5-hour session** | **~₹45–55** |

> Always terminate `dev-cluster` after the session. A cluster left running overnight = ~₹400+ wasted.

---

## What Comes Next

| Day | What gets built |
|---|---|
| Day 15 | Silver transformation — read `bronze/event-stream/vehicle_battery_live/`, validate battery range (0–100%), dedup by `event_id`, write `silver/sl_vehicle_battery_live/` Delta via MERGE |
| Day 16 | Gold mart — `mart_charging_progress_live` aggregated by vehicle + station + 5-minute windows, powering the Live Charging Power BI dashboard tile |
