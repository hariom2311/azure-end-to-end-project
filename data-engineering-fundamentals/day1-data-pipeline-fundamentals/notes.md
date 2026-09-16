# Day 1 — Data Pipelines & the ETL/ELT Paradigm

## Overview

A data pipeline is the backbone of every data-driven system. Before touching any tool or cloud service, a data engineer needs a solid mental model of how data moves, why pipelines fail, and what makes a pipeline production-grade. This day covers ten core concepts that appear in every real-world pipeline and every data engineering interview.

---

## Concept 1: What is a Data Pipeline

A **data pipeline** is a sequence of steps that move data from one or more **sources** to one or more **destinations**, applying logic along the way.

### The three components of every pipeline

| Component | Also called | Examples |
|---|---|---|
| Source | Origin, upstream | Database, REST API, CSV file, Kafka topic, S3 bucket |
| Transformation | Processing layer | Filtering, joining, aggregating, cleaning, enriching |
| Destination | Sink, target, downstream | Data warehouse, data lake, another database, dashboard |

### Why pipelines exist

Raw data is rarely useful in its original form. Pipelines:
- **Centralise** data scattered across many systems into one place
- **Clean and validate** data before it reaches analysts
- **Transform** data into shapes that answer business questions
- **Automate** work that would otherwise be done manually

### Pipeline as a directed graph

Think of a pipeline as a **directed acyclic graph (DAG)**:
- Each **node** is a processing step (read, transform, write)
- Each **edge** is data flowing from one step to the next
- **Directed** means data flows one way (no cycles — step A does not feed back into step A)

```
[Source DB] --> [Extract] --> [Clean] --> [Enrich] --> [Load] --> [Warehouse]
```

A single pipeline can fan out (one source feeds multiple destinations) or fan in (multiple sources merge into one destination).

### Key vocabulary

- **Upstream**: anything earlier in the pipeline than the current step
- **Downstream**: anything later in the pipeline than the current step
- **Orchestrator**: the tool that schedules and monitors pipeline runs (Apache Airflow, Prefect, dbt)
- **Run**: one execution of a pipeline from start to finish

---

## Concept 2: ETL vs. ELT

Both acronyms describe the same three steps — Extract, Transform, Load — but in different orders. The order matters because it determines **where** computation happens and **when** raw data is preserved.

### ETL — Extract, Transform, Load

```
[Source] --> EXTRACT --> TRANSFORM (outside) --> LOAD --> [Destination]
```

- Data is **transformed before** it enters the destination
- The destination only ever sees clean, processed data
- The raw data is often discarded after transformation

**When ETL made sense:**
- Storage was expensive (keep only what you need)
- Destination databases (Oracle, SQL Server) were not built for heavy computation
- On-premise data warehouses had limited capacity

**Drawbacks of ETL:**
- Raw data is lost — you cannot re-transform from scratch if business logic changes
- Transformations happen in a separate layer (custom code, SSIS, Informatica) that is harder to audit
- Schema changes in the source break the transformation layer before anything is stored

### ELT — Extract, Load, Transform

```
[Source] --> EXTRACT --> LOAD --> [Destination] --> TRANSFORM (inside)
```

- Raw data is **loaded first**, exactly as it arrived
- Transformation happens **inside** the destination system using SQL or Spark
- The raw layer is preserved — you can re-derive everything

**Why ELT became dominant:**
- Cloud warehouses (BigQuery, Snowflake, Redshift) can run SQL at massive scale cheaply
- Storage is cheap — keeping raw data costs almost nothing
- dbt, Spark SQL, and similar tools make in-warehouse transformation easy to test and version-control

**The medallion pattern (a common ELT implementation):**

| Zone | Also called | What it contains |
|---|---|---|
| Raw / Bronze | Landing zone | Exact copy of source data, no changes |
| Cleansed / Silver | Staging | Validated, typed, deduplicated data |
| Curated / Gold | Presentation | Business-ready aggregates and metrics |

### When to use which

| Situation | Prefer |
|---|---|
| Destination is a powerful cloud warehouse | ELT |
| Destination has limited compute (small DB, edge device) | ETL |
| You must mask PII before it touches any storage | ETL (transform before load) |
| You want to preserve raw data for reprocessing | ELT |
| Regulatory requirement to never store raw PII | ETL |

---

## Concept 3: Idempotency and Incremental vs. Full Loads

### Idempotency

A pipeline is **idempotent** if running it multiple times produces the same result as running it once.

This is the most important reliability property in pipeline design. If a pipeline fails halfway through and you re-run it, you should not end up with duplicate rows, partial data, or corrupted state.

**Non-idempotent (bad):**
```sql
-- Appends rows every time it runs — re-running creates duplicates
INSERT INTO orders_silver
SELECT * FROM orders_bronze WHERE created_at > '2024-01-01'
```

**Idempotent (good):**
```sql
-- DELETE then INSERT for the target partition — safe to re-run
DELETE FROM orders_silver WHERE date_partition = '2024-01-01';
INSERT INTO orders_silver
SELECT * FROM orders_bronze WHERE DATE(created_at) = '2024-01-01';
```

Or using MERGE (upsert):
```sql
MERGE INTO orders_silver AS target
USING orders_bronze AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

### Full Load

A **full load** (also called a full refresh) truncates the destination table and reloads all data from scratch every run.

```
Run 1: TRUNCATE orders_silver; INSERT all rows from orders_bronze
Run 2: TRUNCATE orders_silver; INSERT all rows from orders_bronze
```

**Pros:** Simple, always correct, no state to manage  
**Cons:** Slow and expensive for large tables; unnecessary computation

**Use full loads when:**
- The source table is small (< a few million rows)
- There is no reliable way to detect changed records
- The transformation logic changed and you need to recompute everything

### Incremental Load

An **incremental load** only processes records that are new or changed since the last run.

To do this you need a **watermark** — a marker of where the last run stopped. Common watermarks:
- A `created_at` or `updated_at` timestamp column
- An auto-incrementing integer ID
- A CDC (change data capture) log sequence number

```
Run 1 (full): load all records, save last_updated = '2024-01-15 12:00:00'
Run 2 (incremental): load records WHERE updated_at > '2024-01-15 12:00:00'
                     save new last_updated = '2024-01-15 18:00:00'
```

**Pros:** Fast, resource-efficient, scales to large tables  
**Cons:** Complex state management; missed records if watermark logic is wrong

**The watermark problem:** If a record is updated with a timestamp older than your watermark (late-arriving data), incremental loads will miss it. Solutions:
- Use a lookback window (re-process the last N hours)
- Use CDC instead of timestamps
- Accept the limitation and document it

---

## Concept 4: Data Lineage and Dependency Graphs

### What is data lineage?

**Data lineage** is the ability to answer: *"Where did this data come from, and where does it go?"*

For any column in any table, lineage tells you:
- Which source system it originated from
- Which transformations were applied
- Which downstream tables or reports depend on it

### Why lineage matters

**Debugging:** If `revenue` in the dashboard is wrong, lineage tells you which upstream table to check first.

**Impact analysis:** If the `orders` source changes its schema, lineage tells you every table, job, and report that will break.

**Compliance:** Under GDPR, if a user requests deletion, lineage tells you every system that holds a copy of their data.

**Auditing:** Regulators may ask where a number in a financial report came from. Lineage is the answer.

### Lineage as a DAG

Pipelines are modelled as a **directed acyclic graph (DAG)**:

```
orders_raw  ──┐
               ├──> orders_silver ──> revenue_gold ──> dashboard
customers_raw ─┘
```

- Each node is a dataset or transformation step
- Arrows show data flow direction
- "Acyclic" means no circular dependencies (A cannot depend on B if B depends on A)

### Column-level vs. table-level lineage

- **Table-level lineage**: `orders_silver` depends on `orders_raw` — coarse-grained
- **Column-level lineage**: `orders_silver.net_amount` = `orders_raw.amount` - `orders_raw.discount` — fine-grained, much more useful for debugging

Tools like dbt, Apache Atlas, and OpenLineage capture lineage automatically by parsing SQL.

---

## Concept 5: Pipeline Failure Modes and Retry Strategies

### The three delivery guarantees

Every pipeline makes an implicit promise about what happens when something goes wrong:

| Guarantee | What it means | Risk |
|---|---|---|
| **At-most-once** | Process each record 0 or 1 times | Data loss — if the step fails, the record is skipped |
| **At-least-once** | Process each record 1 or more times | Duplicates — if the step fails and retries, the record is processed again |
| **Exactly-once** | Process each record exactly 1 time | Complex to implement; requires idempotency + transactions |

In practice, most production pipelines target **at-least-once** with idempotent writes, which gives the safety of exactly-once without the full complexity.

### Common failure modes

| Failure | Cause | Mitigation |
|---|---|---|
| Network timeout | Flaky connection to source/sink | Retry with exponential backoff |
| Source schema change | Upstream team added/removed a column | Schema validation at extract; alerting |
| API rate limit | Too many requests to external API | Throttling, backoff, batching |
| Partial write | Pipeline killed mid-load | Write to staging table first; atomic swap |
| Bad data / parse error | Malformed record in source | Dead-letter queue; skip-and-alert |
| Resource exhaustion | OOM, disk full | Resource limits; monitoring |

### Retry strategies

**Exponential backoff:** Wait longer after each failure to avoid overwhelming a struggling system.
```
Attempt 1: fail → wait 2s
Attempt 2: fail → wait 4s
Attempt 3: fail → wait 8s
Attempt 4: fail → wait 16s → give up, send alert
```

**Dead-letter queue (DLQ):** When a record cannot be processed after N retries, move it to a separate "dead letter" store for manual inspection rather than blocking the entire pipeline.

```
[Source] --> [Pipeline] --> [Success: Destination]
                       \--> [Failure after 3 retries: Dead-letter queue]
```

**Checkpointing:** Save progress periodically so a restart picks up where the last checkpoint left off rather than from the beginning.

**Atomic writes (write-audit-publish pattern):**
```
1. Write transformed data to a STAGING table
2. Validate row counts, nulls, key metrics
3. Only if validation passes: atomically swap STAGING → PRODUCTION
4. If validation fails: alert and leave PRODUCTION unchanged
```

This ensures the production table is never in a half-written state.

---

## Summary

| Concept | One-line summary |
|---|---|
| Data Pipeline | A sequence of extract → transform → load steps represented as a DAG |
| ETL vs. ELT | Order determines where compute happens; ELT is the modern default |
| Idempotency & Loads | Re-runnable pipelines; incremental uses watermarks to process only new data |
| Data Lineage | Tracing data origin and impact; essential for debugging, compliance, governance |
| Failure Modes & Retries | At-least-once + idempotent writes; backoff, DLQ, checkpointing, atomic writes |
| Data Schemas & Schema Evolution | Contracts between producer and consumer; backward/forward compatibility strategies |
| Batch vs. Micro-batch vs. Streaming | Processing frequency trade-offs; latency vs. throughput vs. complexity |
| Data Partitioning & Bucketing | Physical data layout strategies that determine query and load performance |
| Pipeline Observability & Monitoring | SLAs, freshness checks, row count assertions, and alerting patterns |
| Orchestration & Dependency Management | How pipelines are scheduled, sequenced, and recovered at scale |

---

## Concept 6: Data Schemas & Schema Evolution

### What is a schema?

A **schema** is the agreed contract between the system that produces data and the system that consumes it. It defines:
- Which columns exist
- The data type of each column (`STRING`, `INTEGER`, `TIMESTAMP`, etc.)
- Whether columns are nullable or required
- Any constraints (primary keys, unique keys)

Every pipeline implicitly or explicitly relies on a schema. When the schema changes without coordination, pipelines break.

### Schema-on-write vs. schema-on-read

| | Schema-on-write | Schema-on-read |
|---|---|---|
| When schema is enforced | At write time (reject bad data) | At read time (interpret raw bytes) |
| Where it is used | Relational databases, Avro, Protobuf | Data lakes (raw CSV/JSON), Hive |
| Bad data handling | Rejected at ingest | Silently null or parse error at query time |
| Flexibility | Low (schema must exist before writing) | High (can write anything, figure it out later) |

Data lakes often start as schema-on-read (flexible) and evolve toward schema-on-write (governed) as they mature.

### Schema evolution: four types of change

| Change type | Example | Breaking? |
|---|---|---|
| Add a nullable column | Add `discount_code VARCHAR` | Non-breaking (old readers ignore it) |
| Remove a column | Delete `legacy_id` | Breaking (consumers expecting it will fail) |
| Rename a column | `cust_id` → `customer_id` | Breaking |
| Change a type | `order_amount INT` → `order_amount FLOAT` | Maybe breaking (depends on direction) |

### Backward and forward compatibility

**Backward compatible:** New schema can read data written with the old schema.
- Example: Add a nullable `discount_code` column with a default of `NULL`. Old data files have no `discount_code` — the new reader fills in `NULL`. Safe.

**Forward compatible:** Old schema can read data written with the new schema.
- Example: Producer adds a new field. Old consumer ignores unknown fields. Safe if consumer is written defensively.

**Fully compatible:** Both backward and forward. This is what you aim for in long-lived schemas.

### Practical schema management

**Schema registry:** A centralised service (e.g., Confluent Schema Registry for Kafka, AWS Glue Data Catalog) that stores versioned schemas. Before a producer writes, it registers its schema. Before a consumer reads, it fetches the schema for that version. Incompatible changes are rejected.

**Schema contracts in practice:**
```sql
-- Safe evolution: add nullable column (backward compatible)
ALTER TABLE orders_silver ADD COLUMN discount_code STRING;

-- Dangerous: changing type without migration
ALTER TABLE orders_silver ALTER COLUMN order_amount TYPE FLOAT;
-- Risk: existing FLOAT values read by old INT-expecting consumers will break
```

**Defensive reading pattern:**
```python
# Read only the columns you need, not SELECT *
# SELECT * breaks when columns are added/removed
df = spark.sql("SELECT order_id, customer_id, amount FROM orders_silver")
```

`SELECT *` is the most common source of schema evolution breakage in pipelines.

---

## Concept 7: Batch vs. Micro-batch vs. Streaming

Every pipeline must answer: *how often does data move?* The answer determines latency, throughput, cost, and complexity.

### The three processing models

**Batch processing**
- Data is collected over a period (hour, day, month) and processed all at once at the end of that period
- Highest throughput, highest latency, lowest complexity
- Examples: nightly ETL jobs, monthly billing runs, weekly ML model retraining

```
[Source accumulates data] ──> [Trigger at midnight] ──> [Process full day] ──> [Write to DW]
```

**Micro-batch processing**
- Data is collected in small time windows (seconds to minutes) and processed as mini-batches
- Middle ground: lower latency than batch, lower complexity than true streaming
- Examples: Spark Structured Streaming in trigger mode, AWS Glue Streaming

```
[Source] ──> [Collect 30 seconds of data] ──> [Process] ──> [Write] ──> [Repeat]
```

**Streaming (event-by-event)**
- Each record is processed as soon as it arrives, with no deliberate accumulation window
- Lowest latency (milliseconds), highest complexity, hardest to make exactly-once
- Examples: Kafka Streams, Apache Flink, real-time fraud detection

```
[Event arrives] ──> [Process immediately] ──> [Write result]
```

### Choosing the right model

| Question | Batch | Micro-batch | Streaming |
|---|---|---|---|
| How fresh must data be? | Hours/days OK | Minutes OK | Seconds required |
| How complex is the logic? | Any complexity | Moderate | Keep simple |
| Do you need windowed aggregations? | Easy | Easy | Harder (watermarks) |
| What is the budget? | Low | Medium | High |
| Team streaming expertise? | Not needed | Some | Required |

### Latency vs. throughput trade-off

- **Batch:** processes 1 million records in one go — high throughput, but the last record waits for the whole batch
- **Streaming:** processes each record immediately — low latency, but more overhead per record

**Lambda architecture** (historical): Run batch for correctness + streaming for speed, merge results. Complex to maintain — two codebases for one logical pipeline.

**Kappa architecture** (modern): Use streaming for everything; replay historical data through the stream when reprocessing is needed. Simpler — one codebase.

### Late-arriving data

In streaming, events arrive out of order (network delays, mobile apps that buffer offline). If you are computing a 5-minute window sum, an event arriving 3 minutes late will miss the window it belongs to.

**Watermark:** A point in event time before which the system assumes no more late events will arrive. Events older than the watermark are either:
- **Dropped** (simplest, some data loss)
- **Sent to a side output** (late event store for separate handling)
- **Trigger a window recomputation** (most correct, most complex)

---

## Concept 8: Data Partitioning & Bucketing

### Why physical layout matters

A 10 TB table queried with `WHERE order_date = '2024-01-15'` should not scan all 10 TB. The physical layout of data on disk — how it is split into files and folders — determines whether a query reads 10 GB or 10 TB.

### Partitioning

**Partitioning** splits a table into separate physical directories (in a lake) or storage segments (in a warehouse) based on the value of one or more columns.

```
orders/
├── order_date=2024-01-15/
│   ├── part-0001.parquet    # only Jan 15 data
│   └── part-0002.parquet
├── order_date=2024-01-16/
│   └── part-0001.parquet
└── order_date=2024-01-17/
    └── part-0001.parquet
```

When you query `WHERE order_date = '2024-01-15'`, the engine reads only the `order_date=2024-01-15/` directory — **partition pruning** eliminates 99%+ of the data scan.

**Good partition columns:**
- High query selectivity: `date`, `region`, `status`
- Low cardinality: not `order_id` (one file per order = millions of tiny files)
- Evenly distributed: not `status` if 95% of rows are `completed`

**The small files problem:** Too many partitions with too few rows each creates millions of tiny files. File open/close overhead dominates query time. Target files of 128 MB–1 GB each.

**Over-partitioning example (bad):**
```
-- Partitioning by both date AND hour AND customer_id
-- Creates millions of tiny directories
partitioned by (order_date, order_hour, customer_id)
```

### Bucketing (Hash partitioning)

**Bucketing** distributes rows across a fixed number of buckets based on the hash of a column. Unlike partitioning, all buckets always exist — the number does not grow with data volume.

```
Hash(customer_id) % 64 = bucket number (0–63)
```

**When bucketing helps:**
- Joins between two large tables bucketed on the same column — the join engine knows which bucket from table A matches which bucket from table B, eliminating a full shuffle
- Aggregations by the bucketed column — all rows for a given `customer_id` are in the same bucket

**Partitioning vs. Bucketing:**

| | Partitioning | Bucketing |
|---|---|---|
| Splits by | Column value (discrete) | Column hash (numeric range) |
| Number of splits | Grows with distinct values | Fixed at table creation |
| Best for | Date/time range queries | Join optimisation, high-cardinality columns |
| Partition pruning | Yes | No (scans all buckets) |

### Choosing a partition strategy

1. **Start with date** — almost every analytical query has a date filter
2. **Add a second dimension** only if cardinality is low and queries consistently filter by it (e.g., `region`)
3. **Never partition on high-cardinality columns** (user_id, order_id, UUID)
4. **Monitor file sizes** — aim for 128 MB–1 GB per file after partitioning

---

## Concept 9: Pipeline Observability & Monitoring

### What is pipeline observability?

**Observability** is the ability to understand the internal state of your pipeline from its external outputs (logs, metrics, alerts) — without having to manually inspect the data or re-run the code.

A pipeline can run without errors and still be wrong: it processed fewer rows than expected, a column has unexpected nulls, or data is stale because an upstream source stopped sending. These failures are invisible without observability.

### The four pillars of pipeline observability

**1. Freshness** — Is data up to date?
```sql
-- Alert if the max event timestamp in silver is more than 2 hours behind wall clock
SELECT MAX(event_timestamp) FROM silver.orders
-- If now() - MAX(event_timestamp) > 2 hours → alert
```

**2. Volume** — Did the expected amount of data arrive?
```python
# Compare today's row count to the 7-day average
today_count = get_row_count("silver.orders", date="2024-01-15")
avg_7d = get_avg_row_count("silver.orders", lookback_days=7)
if today_count < avg_7d * 0.7:  # 30% drop
    alert("Row count anomaly: orders_silver")
```

**3. Quality** — Are the values correct?
```sql
-- Null check
SELECT COUNT(*) FROM silver.orders WHERE order_id IS NULL;
-- Should be 0

-- Range check
SELECT COUNT(*) FROM silver.orders WHERE order_amount < 0;
-- Should be 0

-- Referential integrity
SELECT COUNT(*) FROM silver.orders o
LEFT JOIN silver.customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
-- Should be 0
```

**4. Pipeline health** — Did the job succeed?
- Run duration (alert if 3× the normal duration)
- Last successful run time
- Error rate in dead-letter queue
- Resource utilisation (memory, CPU, shuffle spill)

### SLAs and SLOs

- **SLA (Service Level Agreement):** External commitment — "The dashboard will show data no older than 4 hours."
- **SLO (Service Level Objective):** Internal target — "The silver layer pipeline completes within 90 minutes, 99% of the time."
- **SLI (Service Level Indicator):** The actual measurement — "Today's pipeline finished in 87 minutes."

Design your alerts to fire before the SLA is breached, not after. If the SLA is 4 hours of freshness and the pipeline normally takes 90 minutes, alert if the pipeline has not started within 3 hours.

### Common alert patterns

| Alert | Trigger condition | Severity |
|---|---|---|
| Pipeline not started | Expected start time + 30 min elapsed, no run | High |
| Pipeline running too long | Duration > 2× p95 historical | Medium |
| Row count anomaly | Count < 70% or > 150% of 7-day average | High |
| Null rate spike | Null % in key column > 1% (was 0%) | High |
| Dead-letter queue growing | DLQ size > 100 records | Medium |
| Freshness breach | Max event time > SLA lag threshold | Critical |

### Write-time data quality checks (assertions)

Run quality checks as part of the pipeline — fail the pipeline before bad data reaches production:

```python
def run_quality_checks(df, table_name):
    checks = {
        "no_null_order_id": df.filter(col("order_id").isNull()).count() == 0,
        "positive_amounts":  df.filter(col("amount") <= 0).count() == 0,
        "row_count_nonzero": df.count() > 0,
    }
    failures = [name for name, passed in checks.items() if not passed]
    if failures:
        raise DataQualityError(f"{table_name} failed checks: {failures}")
```

---

## Concept 10: Orchestration & Dependency Management

### What is orchestration?

**Orchestration** is the scheduling, sequencing, and monitoring of pipeline tasks. An orchestrator answers:
- When should each task run?
- Which tasks must complete before others can start?
- What happens when a task fails?
- How do I see the status of every pipeline at a glance?

Without an orchestrator, engineers use cron — which provides scheduling but no dependency management, no retry logic, no UI, and no cross-pipeline coordination.

### Core orchestration concepts

**Task:** The smallest unit of work — one Python function, one SQL query, one Spark job.

**DAG (Directed Acyclic Graph):** The full set of tasks and their dependencies for one pipeline. The orchestrator topologically sorts the DAG to determine execution order.

**Run / DAG run:** One execution of the DAG, tied to a specific logical date (called `execution_date` in Airflow).

**Trigger:** What causes a DAG run to start:
- **Schedule-based:** Cron expression (`0 2 * * *` = daily at 2 AM)
- **Event-based:** A sensor detects a new file in S3, a Kafka message, a REST call
- **Manual:** An engineer triggers it via UI or CLI
- **Dependency-based:** Another DAG's successful completion

### Task dependency patterns

**Linear chain** — task B runs after task A:
```
extract ──> transform ──> load
```

**Fan-out** — multiple tasks run in parallel after one completes:
```
extract ──> transform_orders ──>
         ──> transform_customers ──> load_gold
         ──> transform_products ──>
```

**Fan-in** — one task waits for multiple upstream tasks:
```
load_orders ──┐
              ├──> build_gold_summary
load_returns ─┘
```

**Sensor** — a task that polls until a condition is met before proceeding:
```
[wait for file in S3] ──> extract ──> transform ──> load
```

### Backfilling

When you deploy a new pipeline (or fix a broken one), you need to process historical data as if the pipeline had been running all along. This is called **backfilling**.

Good orchestrators (Airflow, Prefect) support backfill natively:
```bash
# Airflow: backfill orders pipeline for all days in January
airflow dags backfill orders_pipeline --start-date 2024-01-01 --end-date 2024-01-31
```

For backfills to work correctly, pipelines must be **idempotent** (Concept 3) — re-running for a past date must produce the same result as if it had run on that date originally.

### Cross-DAG dependencies

Large platforms have hundreds of DAGs. Some Gold tables depend on multiple Silver tables built by different DAGs. Cross-DAG dependencies require:

- **Dataset/asset triggers** (Airflow 2.4+): DAG B declares it depends on the `orders_silver` dataset. When DAG A writes to `orders_silver`, DAG B is automatically triggered.
- **ExternalTaskSensor**: DAG B polls for the successful completion of a specific task in DAG A before proceeding.

### SLA miss handling

Orchestrators can define SLAs per task. If a task does not complete within its SLA window, the orchestrator fires a callback (email, Slack, PagerDuty) before the downstream consumer's deadline is breached.

```python
# Airflow example
transform_task = PythonOperator(
    task_id="transform_orders",
    python_callable=transform_orders,
    sla=timedelta(hours=1),  # alert if this task takes > 1 hour
    on_sla_miss=notify_oncall,
)
```

---

## Summary

| Concept | One-line summary |
|---|---|
| Data Pipeline | A sequence of extract → transform → load steps represented as a DAG |
| ETL vs. ELT | Order determines where compute happens; ELT is the modern default |
| Idempotency & Loads | Re-runnable pipelines; incremental uses watermarks to process only new data |
| Data Lineage | Tracing data origin and impact; essential for debugging, compliance, governance |
| Failure Modes & Retries | At-least-once + idempotent writes; backoff, DLQ, checkpointing, atomic writes |
| Schema & Schema Evolution | Contracts between producers and consumers; backward/forward compatibility |
| Batch vs. Micro-batch vs. Streaming | Processing frequency trade-offs; latency vs. throughput vs. complexity |
| Partitioning & Bucketing | Physical layout that determines scan cost and join efficiency |
| Observability & Monitoring | Freshness, volume, quality checks, and SLA-aligned alerting |
| Orchestration & Dependencies | Scheduling, sequencing, backfill, and cross-pipeline coordination |

---

## Further Reading

- [The Data Engineering Lifecycle — Fundamentals of Data Engineering (O'Reilly)](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/)
- [dbt Docs — Understanding DAGs](https://docs.getdbt.com/terms/dag)
- [OpenLineage — Open Standard for Data Lineage](https://openlineage.io/)
- [Martin Kleppmann — Designing Data-Intensive Applications, Ch. 11](https://dataintensive.net/)
