# Day 1 — Data Pipelines & the ETL/ELT Paradigm

## Overview

A data pipeline is the backbone of every data-driven system. Before touching any tool or cloud service, a data engineer needs a solid mental model of how data moves, why pipelines fail, and what makes a pipeline production-grade. This day covers five core concepts that appear in every real-world pipeline and every data engineering interview.

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

---

## Further Reading

- [The Data Engineering Lifecycle — Fundamentals of Data Engineering (O'Reilly)](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/)
- [dbt Docs — Understanding DAGs](https://docs.getdbt.com/terms/dag)
- [OpenLineage — Open Standard for Data Lineage](https://openlineage.io/)
- [Martin Kleppmann — Designing Data-Intensive Applications, Ch. 11](https://dataintensive.net/)
