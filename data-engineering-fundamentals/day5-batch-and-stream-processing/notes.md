# Day 5 — Batch & Stream Processing

## Overview

Every data engineering system processes data in one of two modes: **batch** (process a large block of data at scheduled intervals) or **stream** (process each event as it arrives). Understanding both — and knowing when to use each — is the most common technical deep-dive in DE interviews. Day 5 covers the mechanics of both modes, the windowing and watermark logic that makes stream processing correct, the architectural patterns that combine them, and the orchestration layer that glues everything together.

**The 5 concepts:**
1. Batch Processing — Spark Architecture & Partitioning
2. Stream Processing — Kafka, Consumers & Exactly-Once Semantics
3. Windowing & Watermarks — Handling Time in Streams
4. Lambda & Kappa Architecture — Batch + Stream Design Patterns
5. Pipeline Orchestration — Airflow DAGs, Sensors & Backfill

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

## Concept 2: Stream Processing — Kafka, Consumers & Exactly-Once Semantics

### What is stream processing?

**Stream processing** treats data as an unbounded, continuous sequence of events. Each event is processed as it arrives — or within a small window of time. The dataset has no defined end.

**When to use streaming:**
- Data must be fresh within seconds or minutes (fraud detection, live dashboards, alerting)
- Source systems push events continuously (clickstreams, IoT sensors, payment events)
- You need to react to individual events, not just periodic aggregates

### Apache Kafka architecture

Kafka is the dominant event streaming platform. It acts as a durable, distributed, replayable message bus between producers and consumers.

```
Producers                Kafka Cluster               Consumers
─────────               ──────────────               ─────────
Payment       ──────►  Topic: payments               Fraud service
service       ──────►  ├── Partition 0               Audit service
                       ├── Partition 1               Analytics job
Order         ──────►  └── Partition 2
service
```

**Topic:** A named stream of records. Like a table in a database but append-only.

**Partition:** Topics are split into partitions for parallelism. Each partition is an ordered, immutable log. Records in a partition have monotonically increasing **offsets**.

**Offset:** The position of a record within a partition. Consumers track their own offsets — they can replay events by resetting the offset.

**Retention:** Kafka retains messages for a configurable period (default 7 days) regardless of whether they have been consumed. This enables replays and multiple independent consumers.

### Consumer groups

A **consumer group** is a set of consumers that cooperate to consume a topic. Each partition is assigned to exactly one consumer in the group — parallelism scales with partition count.

```
Topic: payments (3 partitions)

Consumer Group A (analytics):     Consumer Group B (fraud):
Consumer A1 → Partition 0         Consumer B1 → Partition 0 + 1
Consumer A2 → Partition 1         Consumer B2 → Partition 2
Consumer A3 → Partition 2

(Both groups read all events independently)
```

**Key rule:** `#consumers ≤ #partitions`. If you have more consumers than partitions, some consumers are idle.

### Delivery semantics

| Semantic | Guarantee | Risk | How |
|---|---|---|---|
| At-most-once | Each event delivered 0 or 1 times | Data loss (if consumer crashes before processing) | Commit offset before processing |
| At-least-once | Each event delivered 1+ times | Duplicates (if consumer crashes after processing but before commit) | Commit offset after processing |
| Exactly-once | Each event delivered exactly 1 time | None — but complex to implement | Transactional producers + idempotent consumers |

### Exactly-once semantics (EOS)

**Producer side:** Enable idempotent producer (`enable.idempotence=true`). Kafka deduplicates retries using a sequence number.

**Consumer side:** The consumer's processing must be idempotent — processing the same event twice produces the same result. This is the responsibility of the consumer application.

**End-to-end EOS with Kafka Streams / Flink:**
- Use Kafka transactions: produce output and commit offset atomically
- If the consumer crashes, it reprocesses from the last committed offset — but the output topic/database deduplicates using the event key

**Practical EOS for a database sink:**
```sql
-- Upsert instead of insert — idempotent:
INSERT INTO silver.events (event_id, ...) VALUES (...)
ON CONFLICT (event_id) DO UPDATE SET ...=EXCLUDED....;
-- Re-processing the same event_id is a no-op: same result
```

### Kafka performance levers

| Lever | Effect |
|---|---|
| Increase partition count | More consumer parallelism (but cannot reduce partitions without recreating topic) |
| `batch.size` (producer) | Larger batches → higher throughput, higher latency |
| `linger.ms` (producer) | Wait up to N ms to build a larger batch |
| `fetch.min.bytes` (consumer) | Consumer waits until this many bytes available → lower CPU, higher latency |
| Compaction | Topic retains only the latest record per key (useful for CDC) |

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

## Concept 4: Lambda & Kappa Architecture — Batch + Stream Design Patterns

### The core problem

Pure batch: fresh data available only after the batch window closes (e.g., hourly, daily). Stale for interactive dashboards.

Pure stream: fast and fresh, but stateful aggregations over long windows (months of history) are expensive to maintain in a stream processor. Reprocessing historical data requires replaying the entire stream.

**Lambda and Kappa are two architectural patterns that solve this tension.**

### Lambda Architecture

**Two paths, one serving layer:**

```
Source events
    │
    ├──► Batch layer  (Spark on HDFS/S3)
    │       Reprocesses all historical data nightly
    │       Produces accurate, complete batch views
    │
    ├──► Speed layer  (Kafka + Flink/Spark Streaming)
    │       Processes only recent events (last few hours)
    │       Produces approximate/recent real-time views
    │
    └──► Serving layer (merges batch view + speed view)
            Answers queries using:
            - Batch view for old data (complete)
            - Speed view for recent data (approximate)
```

**The batch view** is authoritative and recomputed from scratch. It catches corrections, late arrivals, and is always accurate.

**The speed view** covers only the recent window (the "hot" data the batch hasn't processed yet). It is eventually superseded by the next batch run.

**Trade-off:** Two codebases doing essentially the same logic (batch and streaming). The logic must be kept in sync. Historically, this was unavoidable — but it's the biggest operational pain point of Lambda.

### Kappa Architecture

**One path, one technology:**

```
Source events
    │
    └──► Stream layer only  (Kafka + Flink / Spark Streaming)
             Processes all events (recent and historical)
             For reprocessing: replay from Kafka (long retention) or S3 event log
             Produces one unified view
```

**Key insight:** If you use Kafka with long retention (weeks/months) or store a replay log in S3, you can reprocess historical data by replaying the stream from the beginning. No separate batch layer needed.

**When Kappa works well:**
- Source events are immutable and replayable (Kafka with adequate retention)
- Transformations are stateless or have short state windows
- The team wants one codebase and one processing paradigm

**When Lambda is still preferred:**
- Reprocessing petabytes of history is impractical in a streaming engine
- The batch layer uses massively parallel SQL (Spark, Trino) that outperforms stream processors for large-scale historical aggregations
- Existing batch investment is significant and correct; only real-time layer needs adding

### Practical comparison

| Dimension | Lambda | Kappa |
|---|---|---|
| Codebases | 2 (batch + stream) | 1 (stream only) |
| Historical reprocessing | Full batch recompute | Replay from event log |
| Accuracy | Batch view is authoritative | Single view (stream) is authoritative |
| Operational complexity | High (two systems) | Lower (one system) |
| Best fit | Large-scale history + real-time dashboard | Event-driven systems with long Kafka retention |

### Modern convergence: the Lakehouse streaming approach

Delta Lake / Apache Iceberg with Spark Structured Streaming + batch both reading the same table format:

```
Kafka events ──► Spark Structured Streaming ──► Delta Lake Silver table
                                                       │
                              ┌────────────────────────┤
                              ▼                        ▼
                     Streaming read               Batch read
                  (real-time dashboard)       (nightly Gold model)
```

The Delta table is both the streaming sink and the batch source — one storage layer, one schema, one truth. This is the practical Kappa in modern DE stacks.

---

## Concept 5: Pipeline Orchestration — Airflow DAGs, Sensors & Backfill

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
| Kafka / EOS | Offsets are consumer-side; at-least-once is default; EOS requires idempotent sinks |
| Windowing | Event time vs. processing time; watermark = lateness tolerance; session windows close on gap |
| Lambda / Kappa | Lambda = two codebases, authoritative batch; Kappa = one codebase, stream replay |
| Airflow | `execution_date` ≠ wall clock; sensors wait for external conditions; XComs for metadata only |
