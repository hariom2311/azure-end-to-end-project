# Day 5 — Practice Exercises: Batch & Stream Processing

> Use `data/events_stream.csv` (streaming events with event_time vs. processing_time) and `data/daily_orders.csv` (batch orders across 4 days).  
> SQL exercises run in DuckDB or PostgreSQL. Python exercises use PySpark or plain pandas to simulate Spark behaviour.

---

## Setup — Load the data

```sql
-- DuckDB
CREATE TABLE events   AS SELECT * FROM read_csv_auto('data/events_stream.csv');
CREATE TABLE orders   AS SELECT * FROM read_csv_auto('data/daily_orders.csv');

-- Inspect
SELECT * FROM events LIMIT 5;
SELECT COUNT(*) FROM events;   -- 30 events
SELECT COUNT(*) FROM orders;   -- 40 orders across 4 days
```

---

## Exercise 1 — Batch Processing: Partitioning & Shuffle Analysis

**Concept:** Batch Processing — Spark Architecture & Partitioning

**Scenario:**  
You have `daily_orders.csv` covering Jan 15–18. Simulate how Spark would partition and process this data using SQL equivalents. Answer the partitioning and performance questions.

**Tasks:**

**1a. Partition by date (simulate Spark partitionBy).**  
Write a SQL query that counts orders and total revenue per `order_date`, simulating what a Spark job partitioned by `order_date` would produce as its first aggregation step. Which date has the highest revenue?

**1b. Identify partition skew.**  
Write a query that counts orders by `region`. If Spark partitioned by `region`, which region would cause partition skew? Calculate: what percentage of all orders would land on the skewed partition?

**1c. Simulate a broadcast join scenario.**  
Imagine a `product_prices` lookup table (use the products reference from Day 4 if available, or create a 3-row inline CTE). Write the join between `orders` and `product_prices`. In what scenario would Spark automatically broadcast `product_prices`? What is the default Spark broadcast threshold?

**1d. Simulate a GROUP BY shuffle.**  
Write a query that calculates `total_revenue` and `order_count` per `(store_id, category)`. This is the equivalent of a Spark `groupBy("store_id", "category").agg(sum("amount"), count("*"))` — which triggers a shuffle. List all store-category combinations in descending revenue order.

**1e. Small files problem.**  
If the orders table had 10,000 partitions (one per order_id), each containing 1 row of data, what problems would downstream consumers face? Write the SQL equivalent of `coalesce(5)` that would group these 40 orders into 5 output "files" (buckets). Hint: use `NTILE(5) OVER (ORDER BY order_id)` as a file bucket assignment.

---

## Exercise 2 — Stream Processing: Late Events & Offset Management

**Concept:** Stream Processing — Azure Event Hubs, Consumers & Exactly-Once Semantics

**The events dataset has late arrivals:**  
- `EVT007`: `event_time = 09:10`, `processing_time = 09:18` (8 minutes late)
- `EVT015`: `event_time = 09:04:30`, `processing_time = 09:20` (15.5 minutes late)
- `EVT020`: `event_time = 09:08:45`, `processing_time = 09:26` (17+ minutes late)

**Tasks:**

**2a. Identify all late events.**  
Write a query that calculates the lateness (in minutes) for each event as `processing_time - event_time`. Which events are "late" (lateness > 5 minutes)? Sort by lateness descending.

**2b. Simulate at-least-once processing with a dedup step.**  
Imagine events EVT001, EVT002, EVT003 are delivered twice (duplicate delivery). Write the SQL that a consumer would run to deduplicate on `event_id`, keeping only the first occurrence. This simulates the idempotent sink pattern.

**2c. Simulate offset tracking.**  
In Event Hubs, consumers commit checkpoints (sequence numbers) after processing. Write a table DDL and INSERT for an `offsets` table that tracks: `(consumer_group, event_hub, partition, last_committed_offset)`. Then write the query a consumer would use to determine "which events have I not yet processed?" — simulating reading from the committed checkpoint.

```sql
CREATE TABLE eventhub_offsets (
    consumer_group   TEXT,
    event_hub        TEXT,
    partition_id     INT,
    committed_offset BIGINT,
    updated_at       TIMESTAMP
);

-- The events table has a surrogate offset (use row_number() as offset proxy)
-- After processing EVT001–EVT010, the offset is 10.
-- Write the INSERT to record this, then the SELECT to find unprocessed events (EVT011–EVT030).
```

**2d. Exactly-once simulation.**  
Write the SQL equivalent of an idempotent upsert (exactly-once sink) that would process event EVT001 safely whether it arrives once or ten times. Use `INSERT ... ON CONFLICT DO UPDATE` (PostgreSQL) or `INSERT OR REPLACE` (SQLite/DuckDB).

---

## Exercise 3 — Windowing: Tumbling, Sliding & Session Windows

**Concept:** Windowing & Watermarks — Handling Time in Streams

**Tasks:**

**3a. Tumbling window — 5-minute buckets.**  
Write a SQL query that counts `purchase` events and sums `amount` in 5-minute tumbling windows based on `event_time`. Use:
```sql
date_trunc('minute', event_time) - 
    INTERVAL (EXTRACT(MINUTE FROM event_time)::INT % 5) MINUTE
```
to floor each event to its 5-minute window start. Which window has the highest purchase count?

**3b. Sliding window — 10-minute window, 5-minute hop.**  
A sliding window means each event is counted in multiple overlapping windows. Simulate this by joining the events table to a set of window boundaries:
```sql
WITH windows AS (
    SELECT 
        generate_series AS window_start,
        generate_series + INTERVAL '10 minutes' AS window_end
    FROM generate_series(
        TIMESTAMP '2024-01-15 09:00:00',
        TIMESTAMP '2024-01-15 09:30:00',
        INTERVAL '5 minutes'
    )
)
SELECT w.window_start, w.window_end, COUNT(*) AS event_count, SUM(e.amount) AS total_amount
FROM windows w
JOIN events e ON e.event_time >= w.window_start AND e.event_time < w.window_end
WHERE e.event_type = 'purchase'
GROUP BY w.window_start, w.window_end
ORDER BY w.window_start;
```
Run this query. How many windows does EVT001 (09:01) appear in? Why?

**3c. Session window.**  
A session is a sequence of events from the same user with no gap longer than 8 minutes.  
Write a query that identifies session boundaries for user `U001`:
1. Order `U001`'s events by `event_time`
2. Flag the start of a new session when the gap from the previous event exceeds 8 minutes
3. Assign a session number
4. Count events per session and calculate session duration (last event time - first event time)

**3d. Watermark simulation.**  
Given a 5-minute watermark tolerance, classify each of the three late events:
- `EVT007` (event_time=09:10, processing_time=09:18): is it inside or outside a [09:05–09:10] tumbling window with a 5-minute watermark?
- `EVT015` (event_time=09:04:30, processing_time=09:20): what about [09:00–09:05] window with same watermark?
- `EVT020` (event_time=09:08:45, processing_time=09:26): what about [09:05–09:10] window?

For each: calculate `watermark_time = processing_time - 5 minutes`. If `event_time < window_end` AND `event_time >= watermark_time - window_size`, the event is accepted. Show the calculation.

---

## Exercise 4 — Architecture: Lambda vs. Kappa Design

**Concept:** Lambda & Kappa Architecture

**Scenario:**  
A fintech company processes 2 million payment events per day. They need:
- A fraud detection alert within 3 seconds of a payment
- A daily reconciliation report (accurate, complete, handles late corrections)
- A monthly revenue dashboard (needs 2 years of history)

**Tasks:**

**4a. Design the Lambda architecture.**  
Draw (in text/ASCII) the Lambda architecture for this system. Label:
- The speed layer (what technology, what it produces, how fresh)
- The batch layer (what technology, what it processes, how fresh)
- The serving layer (how queries merge both views)
- Where exactly-once semantics are required

**4b. Identify the Lambda pain points.**  
List three specific problems the team will face maintaining this Lambda architecture over 12 months. For each problem, explain why it is painful.

**4c. Design the Kappa alternative.**  
Redesign using Kappa architecture. What changes? What are the constraints for Kappa to work in this scenario (Event Hubs retention / Capture, stream processor choice, reprocessing mechanism)?

**4d. Make the recommendation.**  
Given the requirements (3-second fraud alert + monthly historical dashboard), which architecture would you recommend for this specific use case and why? Is there a modern Lakehouse approach that simplifies the choice?

**4e. Write the serving layer merge query (Lambda).**  
In Lambda architecture, the serving layer merges a batch view (accurate but 24h stale) with a speed view (approximate but fresh). Write a SQL UNION ALL query that:
- Returns the batch view for `order_date < CURRENT_DATE` (accurate historical data)
- Returns the speed view for `order_date = CURRENT_DATE` (today's streaming approximate data)
- Uses a `source` column ('batch' or 'stream') to distinguish rows

Use `daily_orders` as the batch view and the `events` table (filtered to `event_type = 'purchase'`) as the speed view.

---

## Exercise 5 — Orchestration: Airflow DAG Design

**Concept:** Pipeline Orchestration — Airflow, Sensors & Backfill

**Tasks:**

**5a. Design the DAG dependency graph.**  
The Silver transactions pipeline has these tasks:
1. `wait_for_raw` — sensor waiting for Bronze table to be populated
2. `validate_raw` — run quality checks on Bronze data
3. `transform_silver` — apply Silver transformations
4. `validate_silver` — run quality checks on Silver output
5. `update_gold` — refresh Gold aggregation tables
6. `notify_success` — send Slack alert on completion
7. `notify_failure` — send PagerDuty alert if any task fails

Draw the DAG (text/ASCII). Which tasks run in sequence? Which could run in parallel? What trigger rule should `notify_failure` use?

**5b. Write the idempotent task.**  
A Spark task loads orders for a given `execution_date`. Write a Python function (pseudocode is fine) that:
- Takes `execution_date` from the Airflow context
- Loads only orders for that date from `daily_orders.csv`
- Writes to a partitioned output: `s3://silver/orders/order_date={execution_date}/`
- Is idempotent: running it twice for the same date produces the same result (overwrite, not append)

**5c. Backfill scenario.**  
A bug was found in the Silver transformation — it incorrectly excluded `status = 'refunded'` orders from the 16th and 17th of January. The bug is now fixed.
- Write the Airflow CLI command to backfill January 16–17 only
- What must be true about the transformation task for backfill to be safe?
- What happens to downstream Gold models that depend on Silver? Must they also be backfilled?

**5d. Sensor vs. schedule.**  
Currently the Silver DAG runs at 02:00 every day assuming Bronze is ready by then. Bronze sometimes finishes at 02:30 (30 minutes late) due to upstream delays, causing Silver to read incomplete data.

Redesign the trigger: replace the time-based start with an `ExternalTaskSensor` that waits for the Bronze DAG's `upload_complete` task to succeed. Write the sensor configuration (Python, Airflow operator format). What `timeout` would you set and what happens when it is exceeded?

**5e. XCom usage.**  
After `validate_raw` runs, it needs to pass the row count to `transform_silver` so the transform task can log "processing N rows" in its output. Write the two Python functions:
1. `validate_raw_task(**context)` — queries the row count and pushes it via XCom
2. `transform_silver_task(**context)` — pulls the row count from XCom and logs it

---

## Bonus Challenge — End-to-End Pipeline Design

Design a complete batch + stream processing solution for a ride-sharing company that needs:

1. **Real-time driver matching** — match available drivers to ride requests within 2 seconds
2. **Hourly surge pricing update** — recalculate surge pricing based on supply/demand in each zone
3. **Daily earnings report** — accurate per-driver earnings including corrections and chargebacks
4. **Monthly fraud analysis** — detect driver or rider fraud patterns across 12 months of data

For each requirement, specify:
- Batch or stream? Why?
- Technology choice (Event Hubs, Spark, Flink, Airflow, etc.)
- Window type (if streaming)
- Latency SLA
- How you would handle late data or corrections

Then draw the full architecture diagram showing how all four requirements share a common data infrastructure.
