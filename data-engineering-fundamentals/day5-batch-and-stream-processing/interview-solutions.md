# Day 5 — Interview Solutions: Batch & Stream Processing

> Complete answers for all 40 questions in `interview-questions.md`.

---

## Concept 1: Batch Processing — Spark Architecture & Partitioning

**Q1 — Three components of Spark architecture**

| Component | Role |
|---|---|
| **Driver** | Runs the user's Spark application code. Builds the logical plan (DAG of transformations). Splits the DAG into stages and tasks. Dispatches tasks to executors. Collects results. There is one driver per Spark application. |
| **Executor** | Runs on worker nodes. Executes the tasks the driver sends. Holds partition data in memory (as RDD/DataFrame partitions). Reports task results and heartbeats back to the driver. Multiple executors per application. |
| **Cluster Manager** | Allocates physical resources (CPU cores, RAM) to the driver and executors. Manages the worker nodes. Examples: YARN (Hadoop), Kubernetes, Spark Standalone, AWS EMR. |

**The flow:** Driver submits application → Cluster Manager allocates executors → Driver sends tasks to executors → Executors process partitions and report results → Driver collects and returns final result.

---

**Q2 — Lazy evaluation**

**What it is:** In Spark, transformations (`filter`, `select`, `join`, `groupBy`) are not executed when called. They build a logical plan (a DAG). Execution only begins when an **action** (`write`, `count`, `collect`, `show`) is called.

**Why:** Because Spark can optimise the full plan before running it:

1. **Predicate pushdown:** Move `filter` before an expensive `join` so the join has fewer rows to process
2. **Column pruning:** For Parquet reads, only read the columns that are actually used
3. **Stage fusion:** Combine multiple transformations (filter → map → select) into a single pass over the data, avoiding intermediate materialisation

Without lazy evaluation, every `.filter()` call would immediately materialise a new dataset in memory — wasteful and unoptimisable.

---

**Q3 — What is a shuffle and what triggers one**

A **shuffle** is the redistribution of data across executors so that rows with the same key land on the same executor. It involves:
1. Each executor serialises its data and writes it to local disk (shuffle write)
2. Data is transferred over the network to the target executors (network I/O)
3. Target executors read and deserialise the incoming data (shuffle read)

**Three operations that cause shuffles:**

| Operation | Why it shuffles |
|---|---|
| `groupBy("store_id").agg(sum("amount"))` | All rows with the same `store_id` must be on the same executor to compute the sum |
| `df1.join(df2, "customer_id")` (sort-merge join) | Both DataFrames must be shuffled so matching `customer_id` rows are co-located |
| `orderBy("amount")` | A total sort requires one executor to have the globally sorted range; requires a full shuffle |

**Shuffle is the #1 performance bottleneck** in Spark. Strategies to reduce it: broadcast joins for small tables, pre-partitioning data in the source format, and choosing a join strategy that avoids re-shuffling.

---

**Q4 — One slow task (partition skew)**

**Root cause: Partition skew.** One partition contains vastly more data than the others (e.g., `customer_id = NULL` or a single "super-customer" with millions of orders). The task processing that partition takes 40 minutes; every other task finishes in 30 seconds because their partitions are small.

**Diagnosis:** In the Spark UI → Stages → look at the task duration distribution. The skewed partition will show as a lone outlier.

**Fix — Salting:**
```python
from pyspark.sql.functions import concat, col, lit, rand

# Add a random suffix (0–9) to spread one hot key across 10 partitions
df_salted = df.withColumn(
    "salted_key",
    concat(col("customer_id"), lit("_"), (rand() * 10).cast("int"))
)

result = df_salted.groupBy("salted_key").agg(sum("amount"))

# Post-aggregation: strip the salt and sum again
from pyspark.sql.functions import split
result_final = result \
    .withColumn("customer_id", split(col("salted_key"), "_")[0]) \
    .groupBy("customer_id").agg(sum("sum(amount)"))
```

**Alternative fix:** If the skew is due to NULL keys (a common real-world case), filter nulls before the join and handle them separately.

---

**Q5 — repartition vs. coalesce**

| | `repartition(N)` | `coalesce(N)` |
|---|---|---|
| What it does | Fully redistributes data into exactly N partitions using a full shuffle | Merges existing partitions to reduce the count — no full shuffle |
| Shuffle? | Always yes — triggers a full sort-merge shuffle | No (partial — merges adjacent partitions in place) |
| Can it increase partition count? | Yes | No — coalesce only reduces |
| Output distribution | Evenly distributed (each partition roughly same size) | Uneven — some partitions may be larger |
| When to use | Before a wide join to increase parallelism; to even out skew | Before writing output to reduce small files |

```python
# Use repartition to increase parallelism before a join
df_repartitioned = df.repartition(400, "customer_id")  # hash-partition by key

# Use coalesce to reduce output files before writing
df.coalesce(20).write.parquet("s3://output/")
```

---

**Q6 — Broadcast join for 500 GB + 2 MB**

**Strategy: Broadcast hash join.** The 2 MB product lookup table is small enough to fit in the memory of every executor. Spark sends a copy of the small table to every executor (broadcast it), and each executor performs the join locally using its in-memory copy.

**Result:** No shuffle of the 500 GB orders table at all. Each executor reads its partition of orders and joins using the local broadcast copy.

**Explicit broadcast:**
```python
from pyspark.sql.functions import broadcast

result = orders_df.join(broadcast(products_df), "product_id")
```

**Automatic broadcast:** Spark auto-broadcasts tables smaller than `spark.sql.autoBroadcastJoinThreshold` (default 10 MB). The 2 MB table qualifies automatically — but explicitly using `broadcast()` makes intent clear and bypasses the threshold check.

**Cost if not broadcast:** Spark falls back to sort-merge join — shuffles all 500 GB of orders by `product_id`. This is 250× worse.

---

**Q7 — Column pruning and predicate pushdown for Parquet**

**Column pruning:** Parquet stores data in columns. If your query uses only 3 of 50 columns, Spark reads only those 3 column files — it never touches the other 47. This is called **column pruning** and is automatic for Parquet.

```python
# Spark reads only order_id, amount, status from disk — skips 47 other columns
df.select("order_id", "amount", "status").filter(df.status == "completed")
```

**Predicate pushdown:** Parquet stores row-group metadata (min/max values per column per row group). If you filter `WHERE status = 'completed'`, Spark checks row-group statistics and skips entire row groups that cannot contain matching rows. Enabled by `spark.sql.parquet.filterPushdown=true` (default on).

**Combined effect:** Instead of reading 100 GB and filtering in memory, Spark might read 3 GB (3 columns) and skip 60% of row groups (via predicate pushdown) → reading ~1.2 GB total. 80× reduction with no code change.

---

**Q8 — spark.sql.shuffle.partitions**

**Default:** 200.

This controls how many partitions are created after a shuffle operation (e.g., after `groupBy`, `join`). It does not affect how many partitions the input data has — only the output of a shuffle stage.

**When to increase:** For large datasets. 200 partitions × 128 MB each = 25.6 GB comfortable range. For 1 TB of shuffle data, 200 partitions means ~5 GB per task → too large for executor memory → spill to disk. Set to 2000–4000.

**When to decrease:** For small datasets. A daily job on 1 GB of data creates 200 partitions of 5 MB each → 200 tiny tasks with high scheduling overhead and 200 tiny output files. Set to 20–50.

**Problems from too-high value on small data:**
- 200 task scheduling events for work that could be done in 10 tasks
- 200 tiny output files (small files problem) — downstream readers pay 200 file open calls
- Executor thread overhead for many tiny tasks

**Modern alternative:** `spark.sql.adaptive.enabled=true` (AQE — Adaptive Query Execution) dynamically adjusts partition counts based on actual data sizes at runtime. With AQE on, the static value matters less.

---

## Concept 2: Stream Processing — Azure Event Hubs, Consumers & Exactly-Once Semantics

**Q9 — Azure Event Hub partitions**

A **partition** is an ordered, immutable log of events within an Event Hub. Each partition is stored and replicated across the Event Hubs cluster. Events within a partition have monotonically increasing **sequence numbers** (equivalent to Kafka offsets).

**Why partition count matters for consumers:** In a consumer group, each partition is assigned to exactly one reader. Parallelism is bounded by the number of partitions.

```
3 partitions, 3 readers → 1 partition per reader (maximum parallelism)
3 partitions, 1 reader  → 1 reader reads all 3 partitions (no parallelism)
3 partitions, 6 readers → 3 readers active, 3 idle (waste)
```

If you need 10-way parallelism, you need at least 10 partitions. You cannot increase consumer parallelism beyond the partition count without creating a new Event Hub with more partitions (Standard tier does not allow changing partition count after creation).

---

**Q10 — Event Hubs consumer checkpoint**

A **checkpoint** (also called an offset or sequence number) is a sequential integer that uniquely identifies each event's position within a partition (starting from 0).

**Who tracks it:** The **consumer** tracks its own checkpoint. Checkpoints are stored in Azure Blob Storage or ADLS Gen2 (via the `EventProcessorClient`). The Event Hubs namespace stores the events — but each consumer group independently decides when to checkpoint its position.

**This design is intentional:** Different consumer groups maintain independent checkpoints for the same Event Hub, enabling multiple applications to read at different positions independently.

```
Partition 0: [seq 0] [seq 1] [seq 2] [seq 3] [seq 4]
Consumer Group "analytics"  checkpoint: 3 (has processed 0-2, next read from 3)
Consumer Group "fraud"      checkpoint: 1 (has processed 0, next read from 1)
```

---

**Q11 — Three delivery semantics**

| Semantic | Guarantee | How | Default? |
|---|---|---|---|
| At-most-once | 0 or 1 deliveries | Checkpoint BEFORE processing. If crash between checkpoint and processing → event lost forever | No |
| At-least-once | 1+ deliveries | Checkpoint AFTER processing. If crash between processing and checkpoint → event reprocessed | Yes (most Event Hubs clients) |
| Exactly-once | Exactly 1 delivery | Idempotent consumer + atomic checkpoint + output write | Requires explicit design |

**What causes duplicates in at-least-once:** The consumer processes event E, writes output, then crashes before committing the checkpoint. On restart, the consumer re-reads event E (same sequence number) and processes it again → duplicate output.

**Default for Event Hubs consumers:** At-least-once. The `EventProcessorClient` checkpoints periodically — events between the last checkpoint and a crash are reprocessed on restart.

---

**Q12 — 4 partitions, 6 readers**

**4 readers are actively reading.** Each partition is assigned to exactly one reader in the consumer group. With 4 partitions and 6 readers, 4 readers each own 1 partition. The remaining **2 readers are idle** — they belong to the consumer group but have no partition assigned.

This is wasted capacity. The rule: **you cannot have more parallelism than partitions**. To use all 6 readers, you need an Event Hub with at least 6 partitions. On the Standard tier, partition count is fixed at creation — you must create a new Event Hub with the desired partition count and migrate.

---

**Q13 — Consumer crashes before committing offset**

**What events are reprocessed:** The consumer committed offset up to event 1,000 before crashing. On restart, it reads from offset 1,001 onward — wait, no: it committed before processing these 1,000 events OR after?

If the crash happened **after processing but before committing**: The last committed offset might be 0. On restart it reprocesses events 1–1,000.

**What must be true for this to be safe:** The consumer's processing logic must be **idempotent** — processing the same event twice produces the same result. This means:

- **Database writes:** Use `UPSERT` (INSERT ... ON CONFLICT DO UPDATE), not INSERT. A second insert of the same event_id updates in place rather than creating a duplicate.
- **Counter increments:** Don't use `UPDATE counter = counter + 1`. Instead use an idempotent approach: deduplicate on `event_id` first, then count distinct events.
- **File writes:** Write to a path derived from the `event_id` (deterministic naming) and overwrite on duplicate.

---

**Q14 — Exactly-once to PostgreSQL without distributed transactions**

**Strategy: Idempotent upsert at the database level.**

```sql
INSERT INTO payments (payment_id, customer_id, amount, status, processed_at)
VALUES (:payment_id, :customer_id, :amount, :status, NOW())
ON CONFLICT (payment_id)
DO UPDATE SET
    status       = EXCLUDED.status,
    processed_at = EXCLUDED.processed_at
WHERE payments.payment_id = EXCLUDED.payment_id;
```

**How it achieves exactly-once:** The `payment_id` has a UNIQUE constraint. If the consumer processes event E twice (due to at-least-once delivery), the first INSERT inserts the row. The second INSERT hits the conflict clause and updates in place — same data, same result. The output is indistinguishable from a single-delivery scenario.

**The committed offset is separate from the database write** — it doesn't need to be atomic. Even if the offset is committed after the upsert, a replay just re-runs the upsert (idempotent).

---

**Q15 — Event Hub design for 1M events/second**

**Partition count:** Target ~100 MB/s per partition (a comfortable throughput ceiling). At 1M events/second assuming 1 KB average event size = 1 GB/s total. `1 GB/s ÷ 100 MB/s per partition = ~10 partitions minimum.` Add headroom: **32 partitions** on Standard tier (max 32), or up to 2000 on Dedicated tier.

**Throughput Units:** 1M events/sec at 1 KB = 1 GB/s ingress. Standard tier: 1 TU = 1 MB/s ingress → need 1000 TUs. Use **Event Hubs Premium or Dedicated** for this scale — they offer Processing Units with dynamic scaling and no per-TU cap.

**Partitioning key:** `payment_card_id` (or `customer_id`). This ensures all events for the same card land on the same partition → **ordering guarantee per card** → a consumer processing fraud rules sees card events in sequence, never out of order for a single card.

**If you change the partitioning key to `null` (round-robin):**
- Throughput distribution improves (events spread evenly across partitions)
- But ordering is lost — events for the same card can land on different partitions and be processed by different readers in any order
- Fraud rules that depend on sequence ("3 transactions within 1 minute for the same card") break silently

**Rule:** Choose the partitioning key based on the ordering requirement, not just throughput. Ordering is only guaranteed within a partition, never across partitions.

---

## Concept 3: Windowing & Watermarks

**Q16 — Event time vs. processing time**

**Event time:** When the event actually occurred in the real world — embedded in the event payload by the source system.

**Processing time:** When the stream processor receives and processes the event.

**Real-world example where they differ significantly:**

A retail mobile app lets customers browse and add to cart while offline. A customer opens the app at 14:00, adds 3 items between 14:00 and 14:30 while on the subway (no connectivity). The phone reconnects at 15:10 and sends all 3 events to Event Hubs.

- Event times: 14:02, 14:15, 14:28 (real browsing times)
- Processing times: 15:10, 15:10, 15:10 (all arrive at once)

If you aggregate by processing time, all 3 events appear in the 15:00–16:00 hour — but they should contribute to the 14:00–15:00 window for accurate reporting. Processing time would completely misrepresent when the user was active.

**The rule:** Always use event time for business-meaningful windows. Use processing time only for pipeline-internal metrics (lag monitoring, throughput measurement).

---

**Q17 — Three window types**

**Tumbling window:** Fixed size, non-overlapping. Each event belongs to exactly one window.
- Use case: Hourly sales totals for a dashboard. Revenue from 09:00–10:00, 10:00–11:00, etc.

**Sliding window:** Fixed size, overlapping by a configurable step. Each event belongs to multiple windows.
- Use case: 5-minute moving average of server error rate, updated every 30 seconds. You want continuous, smooth alerting — not one alert per window boundary.

**Session window:** Dynamic size, defined by inactivity gaps. Closes when no event arrives for longer than the gap threshold.
- Use case: User session analytics for a website. A session ends when the user has been idle for 30 minutes. Sessions have no fixed duration.

---

**Q18 — Watermark mechanics**

A **watermark** is a timestamp assertion: "I have seen all events with `event_time ≤ W`. Any event arriving with `event_time < W` is late."

The watermark for time T is: `W = max(observed_event_time) - watermark_lag`

**How it triggers window closing:** A window [14:00–15:00] closes when `W > 15:00` — i.e., when `max(observed_event_time) > 15:00 + watermark_lag`. At that point, the system considers the window complete and emits the result.

**Example with watermark_lag = 5 minutes:**
- At processing_time=15:02, max observed event_time = 15:07 → W = 15:07 - 5min = 15:02 — window not yet closed
- At processing_time=15:06, max observed event_time = 15:11 → W = 15:06 — window [14:00–15:00] now has W > 15:00 → window closes, result emitted

The larger the watermark lag, the longer you wait for late events — increasing output latency but reducing data loss.

---

**Q19 — Is the late event included? (Watermark calculation)**

**Given:**
- Event time: 09:03
- Processing time: 09:14
- Watermark lag: 5 minutes
- Window: [09:00–09:05]

**Calculation:**
```
Watermark at processing_time 09:14:
  = max(observed_event_time) - watermark_lag
  Assume by 09:14 we've seen events up to ~09:09 (processing time based)
  W = 09:09 - 5 min = 09:04

Window [09:00–09:05] closes when W > 09:05:
  W = 09:04 < 09:05 → window not yet closed at time of arrival

Conclusion: YES, the event (event_time=09:03) IS included.
```

**The window [09:00–09:05] closes when the watermark passes 09:05** — i.e., when the stream processor has seen an event with event_time ≥ 09:10 (since W = event_time - 5min > 09:05 → event_time > 09:10). If the 09:03 event arrives before that threshold is crossed, it is included.

---

**Q20 — 20-minute offline burst events**

**What happens:** With a 2-minute watermark, the system will have closed windows up to 18 minutes before the burst arrives. Events with `event_time = T-20` to `T-2` arrive 20–2 minutes late. Those older than 2 minutes past the current watermark are **dropped as late data**.

**How many are dropped:** If the user was offline for 20 minutes and sends 50 events spanning that 20-minute period, approximately 45 events (18 minutes' worth) are dropped (event_time outside the 2-minute watermark tolerance). Only the last ~2 minutes of events are included.

**What to do instead:**

1. **Increase the watermark lag:** A 30-minute watermark accepts all events but adds 30 minutes of output latency to every window — unacceptable for a real-time dashboard.

2. **Side output / DLQ approach:** Route late events to a separate late-data topic. Process them in a nightly batch job that updates the historical results (UPDATE the windowed results table). This gives full fidelity without increasing streaming latency.

3. **Mobile-specific pattern:** Use an **event buffering strategy** at the mobile client — batch events with their event_time and send as one batch. Tag the batch with a `sent_at` timestamp. A streaming pre-processor sees the batch, extracts individual events with correct event_times, and replays them through the stream with high-priority processing.

---

**Q21 — Session windows vs. tumbling/sliding**

**Tumbling and sliding:** Have a fixed, pre-defined duration. The window boundary is a time point — `[09:00–09:10]`, `[09:05–09:15]`. You know exactly when windows start and end before any data arrives.

**Session windows:** Have a dynamic, data-driven duration. The window opens when the first event for a user arrives and extends each time a new event arrives within the gap threshold. The window size is unknown until it closes.

**What determines when a session closes:** The **gap duration** — if no event for that user arrives within N minutes, the session is considered complete and the window closes.

**Why this is harder to implement:** The stream processor must maintain per-user state (last event time) indefinitely, because a new event for any user could extend their session. For tumbling windows, state is discarded as soon as the window closes. Session window state for inactive users must be garbage-collected based on the session gap threshold.

---

**Q22 — Late event changes a finalised window aggregate**

**The scenario:** Window [14:00–15:00] emitted result at 15:05. At 15:12, a late event arrives for 14:47 that changes the aggregate.

**Two design choices:**

**Option A — Drop the late event (simplest):**
- The window is closed; the late event is discarded
- Pro: Simple, no downstream complications
- Con: Result is permanently wrong — the 14:00–15:00 window shows an aggregate that excludes a real event
- Use when: Approximate results are acceptable (dashboards, monitoring) and the business impact of late events is negligible

**Option B — Emit an updated result (retraction/update mode):**
- The stream processor emits a retraction: "the old value for [14:00–15:00] is invalid" and a new emission: "the new value is X+late_event_amount"
- The sink table must support UPDATEs (not append-only)
- Flink supports this natively with `RETRACT` and `UPSERT` changelog modes
- Pro: Fully accurate results
- Con: Downstream consumers must handle retractions (the revised value); append-only sinks (Event Hub topic, immutable ADLS files) cannot support this
- Use when: Financial accuracy is required and the sink supports updates (Delta Lake, PostgreSQL)

---

## Concept 4: Lambda & Kappa Architecture

**Q23 — Lambda architecture**

**Problem it solves:** Pure streaming is fast but struggles with large-scale historical reprocessing and complex aggregations over months of data. Pure batch is accurate but stale. Lambda provides both: speed for recency, accuracy for history.

**Two layers:**

**Batch layer:** Recomputes all results from scratch using the full historical dataset at scheduled intervals (nightly). Produces the **batch view** — authoritative and complete. Technology: Spark on HDFS/S3 or a data warehouse.

**Speed layer:** Processes only the most recent events in near-real-time. Produces the **speed view** — approximate but current (covers data from the last batch run to now). Technology: Event Hubs + Flink/Spark Streaming.

**Serving layer:** Merges the batch view (for historical queries) and the speed view (for recent data) to answer queries. When the next batch run completes, the speed view is discarded and replaced.

---

**Q24 — Lambda's operational pain point and Kappa's solution**

**Lambda's main pain point: Dual codebase maintenance.**

Both the batch layer and the speed layer implement the same business logic (e.g., "calculate revenue by store, by day"). Any time the logic changes — a new dimension, a bug fix, a business rule update — it must be updated in both codebases, kept in sync, tested in both modes, and deployed independently.

In practice, the two codebases inevitably drift:
- Batch uses SQL (Spark SQL), speed uses Java/Python (Flink DataStream API)
- Different frameworks have different semantics (NULL handling, decimal precision, time zones)
- A bug fix applied to batch may not be applied to streaming until weeks later
- Results from batch and speed for the same time period diverge → "which number is right?"

**How Kappa solves it:** Eliminates the batch layer entirely. One processing framework, one codebase. Historical reprocessing is done by replaying events from Event Hubs (with long retention or Capture to ADLS Gen2) or from an event log in object storage. One set of business logic, one set of tests, one deployment.

---

**Q25 — Real-time inventory + 3-year historical analysis**

**Recommendation: Lakehouse Kappa approach.**

**Real-time inventory (Requirement A — 1 second):**
- Stream processor: Event Hubs + Flink or Spark Structured Streaming
- Window: No window needed — stateful per-product inventory counter updated on each event
- Latency: Sub-second is achievable with Flink

**3-year historical analysis (Requirement B — batch):**
- Same Delta Lake / Iceberg table that receives streaming writes can be read by batch Spark jobs
- Monthly or nightly batch reads for historical aggregations
- No separate batch layer: Delta's time-travel gives access to any historical snapshot

**Why not full Lambda:** 3 years of history is large but replayable. Event Hubs Capture archives all events to ADLS Gen2 automatically — replay from there without Kafka retention limits. Maintaining two codebases for e-commerce inventory is disproportionate to the complexity.

**Why not pure batch:** 1-second latency for inventory is impossible with batch.

---

**Q26 — Which layer does the serving layer show?**

**The speed view (stream layer's data)** for the order placed 2 hours ago.

In Lambda, the serving layer shows:
- **Batch view** for all data processed in the last completed batch run (e.g., everything before midnight last night)
- **Speed view** for data since the last batch run (today's orders, including the 2-hour-old order)

**What happens at midnight (next batch run):**
1. The batch job reprocesses ALL data from scratch (including the 2-hour-old order from today)
2. Produces a new, authoritative batch view that includes today's data
3. The serving layer switches from the speed view to the new batch view for this order
4. The speed view for today's data is discarded (or aged out) — the batch view is now authoritative

If the order status was wrong in the speed view (approximate), the batch view corrects it. This is the "batch layer is authoritative" guarantee of Lambda.

---

**Q27 — Two codebases, one new dimension**

**Number of places to change: 2 minimum** — the batch job (Spark) and the streaming job (Flink/Spark Streaming). In practice: 4–6 places (batch logic, streaming logic, batch tests, streaming tests, possibly two separate CI/CD pipelines).

**Risks of dual-maintenance:**

1. **Logic drift:** The developer updates the batch aggregation to include `region` as a new dimension but forgets to update the streaming job. For recent data (speed view), the new dimension is missing. The dashboard shows `region` for all historical data but not today's data — confusing and misleading.

2. **Race to deploy:** The batch job is deployed first. For 4 hours (until the streaming job is deployed), the batch and speed views are computing different aggregations. The serving layer merges them → incorrect combined results.

3. **Test gap:** The streaming job has no integration tests for the new dimension because it's hard to set up an Event Hubs/Flink test environment. The batch job has comprehensive Spark tests. A downstream bug in the streaming path is discovered 3 weeks after deployment when a business analyst reports wrong numbers in the live dashboard.

---

**Q28 — Delta Lake as modern Kappa**

**How Delta Lake enables the Kappa approach:**

**Single storage layer, dual access pattern:**
```
Event Hubs events
    → Spark Structured Streaming (write mode: append)
    → Delta Lake Silver table
    
Concurrent readers:
    → Streaming read: Spark Structured Streaming reads Delta as a streaming source
      (uses Delta's transaction log to find new data since last checkpoint)
    → Batch read: Spark batch job reads the full table as of any snapshot
```

**The Delta transaction log** (`_delta_log/`) is the key: every write (streaming or batch) records a JSON commit. The streaming reader uses this log to find new data without full table scans. The batch reader uses it for snapshot isolation.

**Historical reprocessing without a batch layer:** Delta's **time-travel** (`VERSION AS OF N` or `TIMESTAMP AS OF T`) lets you query any historical state of the table. For reprocessing: run a Spark batch job reading `Delta.forPath(path).asOf(timestamp)` from the beginning, write to a new table, then swap. No separate batch layer or Event Hubs replay needed — Event Hubs Capture to ADLS Gen2 provides the raw event archive if a full replay is required.

**ACID transactions:** Multiple streaming writers and batch readers can operate concurrently without corrupting the table. Delta handles the concurrency with optimistic locking on the transaction log.

---

## Concept 5: Pipeline Orchestration — Airflow

**Q29 — What is an Airflow DAG and why acyclic**

A **DAG (Directed Acyclic Graph)** in Airflow is a Python file that defines:
- A set of tasks (units of work)
- The dependencies between them (task A must complete before task B starts)
- The schedule (`schedule_interval` — cron expression or timedelta)
- Retry, timeout, and alerting configuration

**Why acyclic (no cycles):** A dependency cycle would create an infinite loop: task A waits for task B, which waits for task A. The DAG could never start. More fundamentally, cycles violate the concept of ordered execution — a pipeline with a cycle has no defined start or end. Airflow enforces the acyclicity constraint at DAG parse time and raises an error if a cycle is detected.

---

**Q30 — execution_date vs. wall clock**

**`execution_date` (or `data_interval_start`):** The logical date the DAG run is processing — the start of the data interval, not the calendar date/time the run actually started.

**Wall clock:** The physical time at which the DAG run begins execution.

**Example:** A DAG scheduled daily at 02:00 UTC with `start_date = 2024-01-01`:
- The run for interval `2024-01-15` starts at `2024-01-16 02:00 UTC`
- `execution_date = 2024-01-15 00:00:00`
- Wall clock = `2024-01-16 02:00:00`

**Why this distinction matters for idempotent pipelines:**

```python
# WRONG — uses wall clock, processes different data on different runs
def load(**context):
    today = datetime.now().date()  # 2024-01-16 if run at 02:00
    df = read_orders(date=today)   # reads 01-16 data, not 01-15 data

# CORRECT — uses data interval, always processes the right partition
def load(**context):
    load_date = context["data_interval_start"].date()  # 2024-01-15
    df = read_orders(date=load_date)  # always correct
```

Without this distinction, backfill runs would read the current day's data (wrong partition) instead of the historical partition they are meant to process.

---

**Q31 — Sensors and reschedule mode**

An **Airflow sensor** is a special operator that polls an external condition until it is `True`, then succeeds and allows downstream tasks to proceed.

Common sensors:
- `FileSensor` — waits for a file to appear
- `ExternalTaskSensor` — waits for another DAG's task to complete
- `S3KeySensor` — waits for an S3 object to exist

**`mode="poke"` (default):** The sensor occupies a worker slot for its entire duration, waking up every `poke_interval` seconds to check the condition. **Problem:** If 10 sensors are waiting, they tie up 10 worker slots for hours — starving other tasks.

**`mode="reschedule"`:** The sensor checks the condition and, if False, releases the worker slot and reschedules itself for the next check interval. Between checks, it holds no slot. **Use this** for long-wait sensors (hours) or when worker slots are limited.

```python
wait_for_file = FileSensor(
    task_id="wait_for_upstream",
    filepath="/data/upstream_complete.flag",
    mode="reschedule",
    poke_interval=60,   # check every 60 seconds
    timeout=3600,        # fail after 1 hour
)
```

---

**Q32 — catchup=True with 19 missed runs**

**What happens:** When the DAG is first unpaused on January 20th with `start_date = January 1st` and `catchup=True`:

Airflow creates **19 DAG runs** — one for each missed interval (January 1–19). All 19 runs are queued immediately and execute concurrently (bounded by `max_active_runs`, default 16).

This is called **backfill via catchup**. Airflow treats each missed schedule interval as a separate run with its own `execution_date`.

**Risk:** If the pipeline is not idempotent, 19 concurrent runs writing to the same table cause conflicts, duplicates, and race conditions. Always set `max_active_runs=1` or use `catchup=False` for pipelines that are not idempotency-safe.

```python
with DAG(
    dag_id="silver_transactions",
    catchup=True,
    max_active_runs=1,  # process one historical run at a time
    ...
):
```

---

**Q33 — Parallel tasks and failure handling**

**With `all_success` trigger rule (default) on `merge_gold`:**
- `validate` fails
- `transform_A` and `transform_B` are **still scheduled and run** (they have no dependency on `validate`)
- `merge_gold` checks: one upstream (`validate`) failed → `merge_gold` is **skipped** (not run, marked as upstream_failed)

**Correct trigger rule for `merge_gold`:** `all_success` (the default). This ensures `merge_gold` only runs if all three upstream tasks (validate, transform_A, transform_B) succeed.

**If you want A and B to stop when validate fails:** Add `validate >> transform_A` and `validate >> transform_B` to the DAG — this makes A and B downstream of validate, so they only run when validate succeeds.

**For `notify_failure`:** Use `trigger_rule="one_failed"` — it fires as soon as any upstream task fails, without waiting for all tasks to complete.

---

**Q34 — `datetime.now()` in a pipeline task**

**The problem:** A task that uses `datetime.now()` processes a different data partition depending on when it runs — not the partition it is supposed to process.

**Backfill scenario:** You run a backfill for January 1–10. Each task calls `datetime.now()` → gets January 20 (today) → reads January 20's data. All 10 backfill runs process the wrong (current) date. The January 1–10 partitions remain empty. The backfill is silently incorrect.

**The symptom:** "Our backfill ran successfully but the historical data is still missing."

**The fix:**
```python
def transform(**context):
    # WRONG
    partition_date = datetime.now().date()

    # CORRECT — always use the logical data interval
    partition_date = context["data_interval_start"].date()
    # Or using the template shortcut:
    # partition_date = context["ds"]  # "2024-01-15" as a string

    df = spark.read.parquet(f"s3://bronze/orders/dt={partition_date}/")
    df.write.mode("overwrite").parquet(f"s3://silver/orders/dt={partition_date}/")
```

---

**Q35 — Retry strategy for a rate-limited API**

```python
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import requests
from datetime import timedelta

def call_api_with_retry(**context):
    response = requests.get("https://api.example.com/data")
    if response.status_code == 400:
        # Non-transient error — don't retry, raise immediately
        raise AirflowException(f"Bad request (HTTP 400): {response.text}")
    if response.status_code == 429:
        # Rate limit — let Airflow retry handle this
        raise AirflowException("Rate limited (HTTP 429) — will retry")
    response.raise_for_status()
    return response.json()

load_task = PythonOperator(
    task_id="load_from_api",
    python_callable=call_api_with_retry,
    retries=5,
    retry_delay=timedelta(minutes=5),
    retry_exponential_backoff=True,   # 5, 10, 20, 40, 80 min between retries
    email_on_failure=True,
    email=["de-oncall@company.com"],
    on_failure_callback=send_pagerduty_alert,
)
```

**How `retry_exponential_backoff` works:** Each retry waits `retry_delay × 2^(attempt-1)`:
- Attempt 1: wait 5 min
- Attempt 2: wait 10 min
- Attempt 3: wait 20 min
- Attempt 4: wait 40 min
- Attempt 5: wait 80 min
- All 5 retries exhausted → task fails → `email_on_failure` triggers

**The 400 vs 429 distinction is critical:** A 400 is a bug in your request (wrong parameters, malformed body) — retrying will never fix it. Raise immediately to surface the bug. A 429 is transient (quota reset in minutes) — exponential backoff resolves it.

---

## Mixed / Senior-Level Questions

**Q36 — "Batch is simpler" — correct or skill gap?**

**Correct technical decision when:**
- Data freshness requirement is measured in hours or days (nightly reports, monthly analytics)
- Processing requires aggregations over the full historical dataset (year-over-year comparisons, rolling 12-month cohort analysis)
- Reprocessing must be possible (schema changes, data corrections, new business rules applied retroactively)
- The data volume is so large that streaming would require impractically large state (100 billion events, 5-year history)

**Symptom of skill gap when:**
- The business has a clear real-time requirement ("fraud alerts within seconds") but the team uses batch "because streaming is complex"
- Streaming latency is minutes (micro-batch Spark) rather than sub-second — the "streaming is too complex" might mean "we don't know how to tune watermarks"
- The team is using a batch-streaming engine (Spark) that supports both modes with the same API — complexity is minimal

**The senior answer:** "I default to batch when freshness requirements allow it — it's simpler, cheaper, and easier to backfill. But I choose streaming when the business genuinely needs sub-minute data, and I know the team has the skills to operate it."

---

**Q37 — Increasing Event Hub partitions from 3 to 10**

**What must happen first:** On Azure Event Hubs **Standard tier**, partition count is fixed at creation and **cannot be changed**. You must create a new Event Hub with 10 partitions and migrate traffic to it.

On **Premium or Dedicated tier**, partition count can be increased (but not decreased) via the Azure portal or CLI:
```bash
az eventhubs eventhub update \
  --resource-group myRG \
  --namespace-name myNamespace \
  --name payments \
  --partition-count 10
```

**Risk of increasing partition count on a keyed Event Hub:** For an Event Hub partitioned by key (e.g., `payment_card_id`), the partition assignment is determined by `hash(key) % partition_count`. Changing the count from 3 to 10 changes this formula. **All existing keys get reassigned to potentially different partitions.**

This means:
1. **Ordering guarantees break for in-flight events.** Events for `card_id=CARD001` were on partition 2 (old) and are now on partition 7 (new). A reader that has checkpointed up to sequence 100 on partition 2 may miss in-flight events that land on partition 7.
2. **Checkpoint invalidation.** Existing checkpoints in Blob Storage reference the old partition layout. Readers must reset or carefully reconcile their checkpoints after the partition change.
3. **New readers.** You can now add 7 more readers to the consumer group for full parallelism.

**Safe approach:** Perform the partition change during a low-traffic window. Use Event Hubs Capture to ensure no events are lost during migration. Reset consumer checkpoints and replay from a known safe sequence number.

---

**Q38 — 50,000 tiny Parquet files**

**Cause:** The `groupBy("customer_id")` shuffle created many partitions — specifically, `spark.sql.shuffle.partitions` defaults to 200, but after the shuffle, many customers produced very small result sets (some partitions have only 1–2 rows). Writing 200 Spark partitions to S3 creates 200 files minimum — but with further automatic repartitioning or if the dataset is large enough, the default can produce more files. If the dataset has 50,000 distinct customers and each got its own task, the output is 50,000 files.

**Fix:**
```python
# Before writing, coalesce to a reasonable number of output files
# Rule of thumb: target 128–256 MB per file
# Total data size / target file size = file count

df_result \
    .groupBy("customer_id") \
    .agg(sum("amount").alias("total_revenue")) \
    .coalesce(20) \                         # 50,000 customers → 20 output files
    .write \
    .mode("overwrite") \
    .parquet("s3://silver/customer_revenue/")
```

**Better long-term fix:** Set `spark.sql.shuffle.partitions` to a reasonable value for the dataset size:
```python
spark.conf.set("spark.sql.shuffle.partitions", "50")  # instead of 200
```

Or enable Adaptive Query Execution (AQE) to auto-tune:
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

---

**Q39 — Window boundary fraud evasion bug**

**Is this a bug?** Yes — it is a **window boundary attack**, known as a "split transaction" pattern. A fraudster or testing user making 8 transactions deliberately straddles the window boundary: 3 in the expiring window (below threshold) and 5 in the new window (at threshold but not above). No alert fires because neither window individually exceeds 5.

**The root issue:** Tumbling windows create blind spots at boundaries. Each window is independent — no state is carried across.

**Fix: Use a sliding window instead of tumbling.**

A sliding window of size 1 minute with a 30-second hop checks a rolling "any 1-minute period" rather than fixed clock-aligned boundaries:
- Window [00:59:00–01:00:00]: contains 3 transactions (below threshold)
- Window [00:59:30–01:00:30]: contains 8 transactions (above threshold) → **ALERT FIRES**

```python
# Spark Structured Streaming: sliding window
events \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        col("card_id"),
        window(col("event_time"), "1 minute", "30 seconds")  # slide every 30s
    ) \
    .count() \
    .filter(col("count") > 5)
```

**Even better for fraud:** Use a **session window** per card — define a fraud session as any sequence of card events within 2 minutes of each other. All 8 transactions form one session (gap between 00:59:58 and 01:00:03 is 5 seconds < 2 minute gap threshold) → session count = 8 → ALERT.

---

**Q40 — IoT pipeline for 100,000 devices**

**Requirement A — Alert within 30 seconds of temperature > 90°C:**
- **Streaming:** Real-time threshold check per event
- Technology: Event Hubs (ingest) + Flink (stateless filter per event)
- Window: No window — per-event check, no aggregation needed
- Pattern: `IF temperature > 90 THEN emit_alert(device_id, temperature, event_time)`
- Latency SLA: < 5 seconds (well within 30 second requirement)
- Late data: Irrelevant for per-event alerting — process the event as it arrives

**Requirement B — Hourly average per device (accurate, handles late data):**
- **Streaming with batch-like semantics:** Spark Structured Streaming → Delta Lake
- Technology: Event Hubs → Spark Structured Streaming with 10-minute watermark → Delta Silver
- Window: Tumbling 1-hour window by device_id and event_time
- Watermark: 10 minutes (devices may have brief connectivity gaps)
- Late data: Routed to DLQ topic; nightly batch reconciliation updates hourly averages in Delta
- Output: Hourly summary table in Delta, updated every 5 minutes with available data

**Requirement C — Monthly anomaly detection across all devices:**
- **Batch:** Aggregating 100,000 × 6 readings/min × 44,640 min/month = 26.8 billion rows/month → only Spark batch is practical
- Technology: Spark batch job on Delta Silver table (monthly), Z-score per device per month
- Window: N/A (batch job reads full month partition)
- Late data: Not an issue (batch runs at month-end after data is complete)
- Output: Anomaly scores table in Delta Gold; alerts sent to device operations team

**Shared infrastructure:**
```
100,000 IoT devices
         │
         ▼
    Event Hub: device_telemetry
    (partitioned by device_id, 100 partitions)
         │
    ┌────┴─────────────────────┐
    │                          │
    ▼                          ▼
Flink: alert_check       Spark Structured Streaming
(per event, stateless)   (1-hour tumbling windows)
    │                          │
    ▼                          ▼
PagerDuty/              Delta Lake Silver
SMS alert               (device_id, hour, avg_temp)
                               │
                               ▼
                      Spark Batch (monthly)
                      Z-score anomaly detection
                               │
                               ▼
                      Delta Gold: anomaly_scores
```

**Late data handling summary:**
- Req A: No late handling (per-event, no window)
- Req B: 10-minute watermark + DLQ + nightly batch patch
- Req C: Batch at month end — no late data problem (all data settled by then)
