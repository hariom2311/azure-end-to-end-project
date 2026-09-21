# Day 5 — Batch & Stream Processing

## Overview

Every data engineering system processes data in one of two modes: **batch** (process a large block of data at scheduled intervals) or **stream** (process each event as it arrives). Understanding both — and knowing when to use each — is the most common technical deep-dive in DE interviews. Day 5 covers the mechanics of both modes, the windowing and watermark logic that makes stream processing correct, the architectural patterns that combine them, and the orchestration layer that glues everything together.

**The 4 concepts:**
1. Batch Processing — Spark Architecture & Partitioning
2. Stream Processing — Azure Event Hubs, Consumers & Exactly-Once Semantics
3. Windowing & Watermarks — Handling Time in Streams
4. Pipeline Orchestration — Airflow DAGs, Sensors & Backfill

---

## Concept 1: Batch Processing — Spark Architecture & Partitioning

### What is batch processing?

**Batch processing** reads a bounded dataset (a file, a table partition, a day's worth of events), applies transformations, and writes output — all in one scheduled job. The data is complete before the job starts.

**When to use batch:**
- Data doesn't need to be fresh within seconds (daily reports, nightly ML feature tables)
- Transformations are expensive and should process many rows together (sort-merge joins, full aggregations)
- Reprocessing historical data (backfill, schema migration)

### Spark architecture

Apache Spark is the dominant batch processing engine. Its architecture has three layers:

```
┌─────────────────────────────────────────────┐
│                Driver Program                │
│  (builds the logical plan, schedules tasks) │
└───────────────────┬─────────────────────────┘
                    │ submits tasks
        ┌───────────▼───────────┐
        │    Cluster Manager    │  (YARN / Kubernetes / Spark Standalone)
        └───┬───────────────┬───┘
            │               │ allocates executors
     ┌──────▼──────┐ ┌──────▼──────┐
     │  Executor 1 │ │  Executor 2 │  (run on worker nodes)
     │  Task Task  │ │  Task Task  │
     └─────────────┘ └─────────────┘
```

**Driver:** Runs the user's Spark code. Builds a logical plan (DAG of transformations). Splits the DAG into stages and tasks. Sends tasks to executors. Collects results.

**Executor:** Runs tasks assigned by the driver. Holds data in memory (as partitions of an RDD/DataFrame). Reports progress and results back to the driver.

**Cluster Manager:** Allocates resources (CPU, memory) to executors across worker nodes.

### DAG of transformations

Spark builds a **Directed Acyclic Graph (DAG)** of transformations before executing anything. This is called **lazy evaluation** — transformations are not executed until an **action** is called.

```python
df = spark.read.parquet("s3://orders/2024/")   # NOT executed yet
df2 = df.filter(df.status == "completed")      # NOT executed yet
df3 = df2.groupBy("store_id").agg(sum("amount")) # NOT executed yet
df3.write.parquet("s3://silver/store_revenue/") # ACTION — triggers execution
```

**Why lazy evaluation?** Spark can optimise the full plan before running it:
- Predicate pushdown: push `filter` before expensive `join`
- Column pruning: only read the columns you actually use
- Stage fusion: combine compatible transformations into one pass

### Stages and the shuffle

A Spark job is split into **stages** separated by **shuffles**.

**Shuffle:** Moving data between executors so that rows with the same key end up on the same executor. Required for `groupBy`, `join`, `orderBy`, and `distinct`.

```
Stage 1 (no shuffle):        Stage 2 (after shuffle):
Executor 1: filter rows  →   Shuffle: rows with key A → Executor 1
Executor 2: filter rows  →          rows with key B → Executor 2
                             Executor 1: sum(amount) for key A
                             Executor 2: sum(amount) for key B
```

**Shuffle is expensive:** It involves serialising data, writing to disk, sending over the network, and deserialising. Minimising shuffles is the #1 Spark performance optimisation.

### Partitioning

A Spark DataFrame is divided into **partitions** — chunks of data that each executor processes independently.

**Default partitions on read:** Spark creates one partition per HDFS block or S3 file (typically 128 MB).

**Repartition vs. coalesce:**

| Operation | What it does | Shuffle? | When to use |
|---|---|---|---|
| `repartition(N)` | Redistribute data into exactly N partitions (even split) | YES | Before a wide shuffle join; to increase parallelism |
| `coalesce(N)` | Reduce partitions by merging (no full shuffle) | Partial | Before writing to fewer output files |

**The small files problem:** If a job produces 10,000 partitions of 1 MB each, downstream readers pay 10,000 file open calls. Always coalesce before writing:
```python
df.coalesce(20).write.parquet("s3://output/")
```

**Partition skew:** When one partition has 10× the data of others, one executor takes 10× longer while the rest sit idle. Fix with salting:
```python
# Add a random prefix to the key to spread load
df.withColumn("salted_key", concat(col("customer_id"), lit("_"), (rand() * 10).cast("int")))
```

### Broadcast joins

When one table is small (< a few hundred MB), broadcast it to every executor so each executor can join locally without a shuffle:

```python
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "product_id")
```

Without broadcast: Shuffle both tables → expensive.  
With broadcast: Send small table to all executors → no shuffle needed.

### Key Spark interview numbers

| Parameter | Default | Why it matters |
|---|---|---|
| `spark.sql.shuffle.partitions` | 200 | Default partition count after a shuffle — often too high for small datasets (200 tiny files) or too low for large ones |
| Executor memory fraction | 60% for execution, 40% for storage | Tune for spill-to-disk avoidance |
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | Tables smaller than this are auto-broadcast |

---

## Concept 2: Stream Processing — Azure Event Hubs, Consumers & Exactly-Once Semantics

### The simple idea — think of it like a WhatsApp group

Imagine a **WhatsApp group** for a bank's payment system:

- Every time a customer makes a payment, the payment app **sends a message** to the group
- Multiple teams are **reading** that same group — the fraud team, the analytics team, the audit team
- Each team reads at their **own pace** and remembers where they left off
- Messages stay in the group for **7 days** so a team that was offline can catch up

**Azure Event Hubs is exactly this** — a managed cloud service where applications send events, and multiple other applications read those events independently.

---

### Real example: A payment is made at a supermarket

**What happens step by step:**

```
1. Customer taps card at Woolworths checkout
         │
         ▼
2. Payment app sends an event to Event Hubs:
   {
     "event_id": "EVT-20240115-001",
     "card_id":  "4111-xxxx-xxxx-1234",
     "amount":   45.80,
     "merchant": "Woolworths Sydney CBD",
     "timestamp": "2024-01-15T09:01:05Z"
   }
         │
         ▼
3. Event Hubs stores the event in a partition (like a queue lane)
         │
    ┌────┴────────────────────────────┐
    │                                 │
    ▼                                 ▼
4a. Fraud team reads it              4b. Analytics team reads it
    → checks: 3 taps in 1 min?           → updates daily spend report
    → alerts if suspicious               → updates merchant dashboard
    (reads at their own checkpoint)       (reads at their own checkpoint)
```

**Key insight:** The payment app sends the event **once**. Both teams read it **independently**. Neither team blocks the other.

---

### Core concepts explained simply

**Event Hub** — the named stream. Like a specific WhatsApp group. You might have one Event Hub for `payments`, another for `logins`, another for `website-clicks`.

**Partition** — a lane inside the Event Hub. Events for the same card always go to the same lane (so they stay in order). More lanes = more teams can read in parallel.

```
Event Hub: payments
├── Partition 0  → all events for cards starting with 0–3
├── Partition 1  → all events for cards starting with 4–6
└── Partition 2  → all events for cards starting with 7–9
```

**Sequence number** — the position of an event in its partition (0, 1, 2, 3 …). Like the message number in a WhatsApp group. A consumer remembers its last-read sequence number so it knows where to continue after a restart.

**Consumer Group** — an independent reader. The fraud team is one consumer group; the analytics team is another. Each maintains its own bookmark (checkpoint). They never interfere with each other.

**Checkpoint** — the bookmark. After processing event #47, the consumer saves "I've read up to 47" to Azure Blob Storage. If it crashes and restarts, it picks up from 47.

**Retention** — how long Event Hubs keeps messages (default 1 day, up to 90 days). A team that was down for 2 days can replay all missed events if retention covers that window.

---

### The three delivery guarantees

> **Teaching analogy:** You send a parcel.

| Guarantee | Parcel analogy | What happens | Risk |
|---|---|---|---|
| At-most-once | Post it and forget | Checkpoint first, then process. If crash during processing → event lost | Data loss |
| At-least-once | Send with tracking + retry on no-reply | Process first, then checkpoint. If crash before checkpoint → reprocess | Duplicates |
| Exactly-once | Courier with signed receipt + no-duplicate logic | Process + checkpoint atomically, sink is idempotent | Complex but safe |

**In practice, most systems use at-least-once + an idempotent sink.**

---

### What "idempotent sink" means — with example

Idempotent = "doing it twice gives the same result as doing it once."

**Problem:** The analytics consumer processes payment EVT-001, writes to the database, then crashes before checkpointing. On restart it reprocesses EVT-001. Now EVT-001 is in the database **twice** → revenue is double-counted.

**Fix — use UPSERT (INSERT + ON CONFLICT UPDATE):**

```sql
-- If EVT-001 already exists → update in place (no duplicate)
-- If EVT-001 is new → insert it
INSERT INTO silver.payments (event_id, card_id, amount, merchant, event_time)
VALUES ('EVT-001', '4111-xxxx-1234', 45.80, 'Woolworths', '2024-01-15T09:01:05Z')
ON CONFLICT (event_id)
DO UPDATE SET amount = EXCLUDED.amount,
              merchant = EXCLUDED.merchant;

-- Processing EVT-001 ten times → same single row in the table
```

Now re-processing the same event is completely safe.

---

### Reading from Event Hubs into Spark (code)

```python
# Spark Structured Streaming reads Event Hubs as a live stream
connection_string = "Endpoint=sb://mybank.servicebus.windows.net/;..."

raw_stream = spark.readStream \
    .format("eventhubs") \
    .options(**{
        "eventhubs.connectionString": connection_string,
        "eventhubs.consumerGroup":    "analytics-team"
    }) \
    .load()

# Each row has: body, sequenceNumber, offset, enqueuedTime, partition
payments = raw_stream.select(
    from_json(col("body").cast("string"), payment_schema).alias("data")
).select("data.*")

# Write to Delta Lake Silver table (streaming write)
payments.writeStream \
    .format("delta") \
    .option("checkpointLocation", "abfss://checkpoints/payments/") \
    .outputMode("append") \
    .start("abfss://silver/payments/")
```

**What the checkpoint does:** Every micro-batch, Spark saves the last sequence number it processed to `abfss://checkpoints/payments/`. On restart, it reads from that sequence number — no events are skipped, no events are reprocessed (as long as the sink is idempotent).

---

### Azure Event Hubs key facts for interviews

| Fact | Detail |
|---|---|
| Partition count | Fixed at creation on Standard tier (2–32). Cannot be changed without recreating the hub. |
| Retention | 1–90 days on Standard/Premium. Use **Event Hub Capture** to archive to ADLS Gen2 forever. |
| Throughput Units | Standard tier: 1 TU = 1 MB/s ingress, 2 MB/s egress. Scale TUs for high volume. |
| Consumer groups | Up to 20 on Standard tier. Each group has its own independent checkpoint. |
| Kafka compatibility | Event Hubs exposes a Kafka-compatible endpoint — existing Kafka code works with just a connection string change. |
| Capture | Auto-archives all events to ADLS Gen2 as Avro/Parquet — enables replay without holding events in the hub. |

---

## Concept 3: Windowing & Watermarks — Handling Time in Streams

### The two clocks in streaming

**Event time:** When the event actually happened (embedded in the event payload). The "real" business time.

**Processing time:** When the event arrived at the stream processor. Always later than event time.

**The problem:** Events can arrive late. A mobile app event timestamped at 14:00 might not arrive until 14:05 because the phone was offline. If you compute aggregates by event time, what do you do with late arrivals?

```
Event time:   14:00  14:01  14:02  14:01 (late!)  14:03
Processing:   ─────────────────────────────────────────►
                                          ↑ arrives at 14:07
```

### Window types

**Tumbling window:** Fixed-size, non-overlapping. Every event belongs to exactly one window.
```
|─── 14:00–14:05 ───|─── 14:05–14:10 ───|─── 14:10–14:15 ───|
Window size = 5 min, hop = 5 min
```

**Sliding window:** Fixed-size, overlapping. One event can belong to multiple windows.
```
|──14:00–14:05──|
        |──14:02–14:07──|
                |──14:04–14:09──|
Window size = 5 min, hop = 2 min
Use case: rolling 5-minute average updated every 2 minutes
```

**Session window:** Dynamic size — grouped by gaps in activity. Closes when no events arrive for a defined gap.
```
User clicks: ● ● ●  [3 min gap]  ● ●  [5 min gap]  ●
             └─Session 1─┘       └─Session 2─┘     └─Session 3─┘
Gap threshold = 2 min
Use case: website session analytics
```

### Watermarks

A **watermark** is a declaration: "I have seen all events up to time T. Any event with event_time < T is considered late."

Watermarks trigger window computation. A window for [14:00–14:05] closes when the watermark passes 14:05.

```python
# Spark Structured Streaming example
from pyspark.sql.functions import window, col

events \
    .withWatermark("event_time", "5 minutes") \  # tolerate 5 min of lateness
    .groupBy(window(col("event_time"), "10 minutes")) \
    .agg(count("*").alias("event_count"))
```

**What "5 minutes" means:** The system will wait 5 minutes past the window boundary before finalising it. An event timestamped at 14:03 arriving at 14:09 (6 min late) for a [14:00–14:05] window that closed at 14:10 (5 min tolerance) → dropped. Same event arriving at 14:08 → included.

### Handling late data strategies

| Strategy | Mechanism | Trade-off |
|---|---|---|
| Drop late data | Watermark only; no late records accepted | Simple; loses data |
| Update results | Allow late records to trigger result updates (UPDATE in sink) | Accurate; sink must support updates |
| Side output / DLQ | Route late records to a separate late-data topic | Full fidelity; reconcile in batch |
| Increase watermark lag | Allow longer lateness tolerance | Increases output latency |

### Trigger modes in Spark Structured Streaming

| Mode | When output is emitted | Use case |
|---|---|---|
| `processingTime("1 minute")` | Every 1 minute of processing time | Dashboard refresh every minute |
| `once` | Process all available data and stop | Micro-batch used like a batch job |
| `continuous("1 second")` | True continuous, low-latency (experimental) | Sub-second latency requirements |
| `availableNow` | Like `once` but with better watermark advancement | Incremental batch runs |

---

## Concept 4: Pipeline Orchestration — Airflow DAGs, Sensors & Backfill

### What is orchestration?

Orchestration is the scheduling, sequencing, and monitoring of pipeline tasks. It answers:
- In what order do tasks run?
- What happens when a task fails?
- How do we retry? How do we alert?
- How do we reprocess past dates (backfill)?

**Apache Airflow** is the dominant open-source orchestrator. Alternatives: Prefect, Dagster, dbt Cloud, Azure Data Factory.

### Airflow concepts

**DAG (Directed Acyclic Graph):** A Python file defining tasks and their dependencies. No cycles — task B must complete before task C; you cannot have C depend on B which depends on C.

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

with DAG(
    dag_id="silver_transactions",
    schedule_interval="0 2 * * *",      # 02:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=True,                        # backfill missed runs
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    wait_for_raw = ExternalTaskSensor(
        task_id="wait_for_raw_load",
        external_dag_id="bronze_ingestion",
        external_task_id="upload_complete",
        timeout=3600,                    # fail if not ready within 1 hour
    )

    validate_raw = PythonOperator(
        task_id="validate_raw",
        python_callable=run_quality_checks,
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=run_silver_transformation,
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=send_slack_alert,
        trigger_rule="all_success",
    )

    wait_for_raw >> validate_raw >> transform_silver >> notify_success
```

### Execution date vs. data interval

Airflow's `execution_date` (now `data_interval_start`) is the **logical date** the run is processing, not the wall-clock time the run starts.

A DAG scheduled daily at 02:00 for `2024-01-15` runs at `2024-01-16 02:00` — processing the data interval `2024-01-15 00:00` to `2024-01-15 23:59`.

**Why this matters:** When writing a partition filter in your task:
```python
# Use data_interval_start, not datetime.now()
load_date = context["data_interval_start"].date()  # "2024-01-15"
df.filter(df.transaction_date == load_date).write.parquet(...)
```
Using `datetime.now()` makes the task non-idempotent — it processes different data depending on when it runs.

### Sensors

A **sensor** is a special task that polls an external condition until it is true, then succeeds and lets downstream tasks proceed.

| Sensor type | What it waits for |
|---|---|
| `FileSensor` | A file to appear at a path |
| `ExternalTaskSensor` | Another DAG's task to complete |
| `S3KeySensor` | An S3 key to exist |
| `SqlSensor` | A SQL query to return a non-empty result |
| `HttpSensor` | An HTTP endpoint to return 200 |

**Sensor mode:**

`mode="poke"` (default): Sensor runs in a worker slot, checking every N seconds. Ties up a slot.

`mode="reschedule"`: Sensor frees the worker slot between checks. More efficient for long-waits.

### Backfill

**Backfill** is re-running a DAG for historical `execution_dates` — either to populate a new pipeline with historical data or to reprocess after a bug fix.

```bash
# Backfill from the CLI
airflow dags backfill --start-date 2024-01-01 --end-date 2024-01-31 silver_transactions
```

**`catchup=True`:** When a DAG is unpaused after being paused for 7 days, Airflow automatically creates runs for all 7 missed intervals.

**`catchup=False`:** Only the next scheduled run is created. Missed runs are ignored. Use for pipelines where historical gaps don't matter (e.g., alerting, monitoring).

**Idempotency is required for backfill:** A task run twice for the same `execution_date` must produce the same result. Never use `INSERT` without a dedup check. Prefer `INSERT OVERWRITE` (replace the partition) or `MERGE` (upsert).

### Retry strategy and failure handling

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,  # 5, 10, 20 min between retries
    "email_on_failure": True,
    "email": ["de-team@company.com"],
}
```

**Trigger rules** control when a task runs based on its upstream tasks:

| Trigger rule | Task runs when... |
|---|---|
| `all_success` (default) | All upstream tasks succeeded |
| `all_failed` | All upstream tasks failed (for cleanup tasks) |
| `all_done` | All upstream tasks finished (any status) |
| `one_failed` | At least one upstream failed (for alerting) |
| `none_failed` | No upstream failed (skipped OK — for optional paths) |

### XComs (Cross-communication)

Tasks share small values via **XComs** (cross-communications). Stored in Airflow's metadata DB.

```python
def upload_complete(**context):
    row_count = run_load_job()
    context["ti"].xcom_push(key="row_count", value=row_count)

def validate(**context):
    row_count = context["ti"].xcom_pull(task_ids="upload_complete", key="row_count")
    if row_count == 0:
        raise ValueError("Zero rows loaded — aborting")
```

**XComs are for small metadata, not data.** Never push a DataFrame through XCom — pass the S3 path of the output instead.

### SLA and alerting

```python
with DAG(
    dag_id="silver_transactions",
    sla_miss_callback=send_sla_alert,
    default_args={"sla": timedelta(hours=3)},  # task must complete within 3h
) as dag:
    ...
```

**SLA miss:** If a task has not completed within its SLA, Airflow calls `sla_miss_callback`. It does NOT stop the task — it only triggers the alert.

---

## Summary

| Concept | Key interview point |
|---|---|
| Batch / Spark | Shuffle is expensive; broadcast small tables; lazy evaluation; partition skew |
| Event Hubs / EOS | Checkpoints are consumer-side; at-least-once is default; idempotent sink = safe re-processing |
| Windowing | Event time vs. processing time; watermark = lateness tolerance; session windows close on gap |
| Airflow | `execution_date` ≠ wall clock; sensors wait for external conditions; XComs for metadata only |
