# Day 1 — Interview Solutions: Data Pipelines & ETL/ELT

> Complete answers for all questions in `interview-questions.md`. Read the question first, attempt your answer, then check here.

---

## Concept 1: What is a Data Pipeline

**Q1 — What is a data pipeline?**

A data pipeline is an automated sequence of steps that moves and transforms data from source systems to destination systems.

**Three core components:**

| Component | Role | Example |
|---|---|---|
| Source | Where data originates | PostgreSQL orders database, Stripe API, S3 CSV upload |
| Transformation | Where logic is applied | Filter cancelled orders, join with customer table, convert currency |
| Sink/Destination | Where processed data lands | Snowflake data warehouse, Elasticsearch index, S3 Parquet files |

**Real-world example:** An e-commerce company runs a nightly pipeline. The source is an RDS MySQL database. The transformation deduplicates orders, calculates `net_revenue = amount - discounts - refunds`, and joins with a customer dimension. The sink is a BigQuery table used by the BI team.

---

**Q2 — What does it mean for a pipeline to be a DAG? Why must it be acyclic?**

A **Directed Acyclic Graph (DAG)** has:
- **Directed edges**: data flows in one direction (A → B, not bidirectionally)
- **No cycles**: following edges from any node, you can never return to that node

Pipelines must be acyclic because:
- A cycle would mean a step depends on its own output — logical impossibility
- Cycles would cause infinite loops (Step A waits for B, B waits for A)
- Orchestrators (Airflow, Prefect) use the acyclic property to determine execution order via topological sort

A DAG lets the scheduler identify which steps can run in parallel (nodes with no shared dependencies) and which must be sequential.

---

**Q3 — Three-source pipeline DAG**

```
[PostgreSQL: transactions] ──────────────────┐
[CSV: finance file]        ──────────────────┤──> [JOIN: merge all three] ──> [LOAD: warehouse]
[REST API: customer data]  ──> [EXTRACT API] ┘
```

Fan-in occurs at the JOIN node — three independent streams merge.

**If the API is unavailable:**
- Option 1: Fail-fast — the entire pipeline fails and retries when the API recovers
- Option 2: Partial load — skip the API enrichment for this run, mark records as `customer_data_missing = true`, fill in later via a backfill job
- Option 3: Cache the last successful API response and use stale data for the current run (acceptable if customer data changes slowly)

The right choice depends on how critical customer data is and whether stale data causes downstream issues.

---

**Q4 — Is a cron-based Python script a "real" DAG?**

Partially accurate, but incomplete. A Python script running in cron is a pipeline, but it is only a DAG if:
1. The script internally has no circular dependencies between its steps
2. The overall sequence of operations can be represented as nodes with directional dependencies

What cron scripts lack:
- **Visibility**: no built-in UI to see run history, failure reasons, or step-level status
- **Dependency management**: if the script depends on another script's output, cron cannot coordinate this automatically
- **Parallelism**: cron runs steps sequentially by default; a real DAG scheduler can parallelize independent branches
- **Retry logic**: cron does not natively retry failed runs

So the script *is* a pipeline and *can* be modelled as a DAG, but it lacks the orchestration infrastructure that makes DAGs manageable at scale.

---

## Concept 2: ETL vs. ELT

**Q5 — ETL vs. ELT, and which is more common today**

| | ETL | ELT |
|---|---|---|
| Order | Extract → Transform → Load | Extract → Load → Transform |
| Where transform happens | Outside the destination (separate tool) | Inside the destination (SQL, Spark) |
| Raw data preserved? | No (usually discarded after transform) | Yes (raw layer retained) |
| Best for | Legacy DWs, PII masking before storage, edge/IoT | Cloud warehouses (Snowflake, BigQuery, Redshift) |

**ELT is more common in modern cloud architectures** because:
- Cloud warehouses have cheap, scalable compute — running SQL at scale is inexpensive
- Storage is cheap — keeping raw data costs almost nothing
- Tools like dbt make in-warehouse transformation easy to test, version, and document
- ELT preserves raw data, enabling reprocessing when business logic changes

---

**Q6 — Migrating from Oracle + Informatica ETL to Snowflake**

**Recommendation: Switch to ELT.**

Reasons:
- Snowflake has far more compute power than Oracle for transformation — SQL at scale is Snowflake's strength
- Informatica licences are expensive; dbt is open-source
- ELT lets you keep raw data and re-transform without re-running the full extract

**Factors that could keep ETL:**
- If data contains PII that must not touch Snowflake (regulatory requirement)
- If some transformations are too complex for SQL and require procedural code (rarely true in practice)
- If the Informatica mappings are extremely complex and migration risk is high (migration cost argument)

**Migration approach:** Incrementally. Keep Informatica running, build ELT models in dbt in parallel, validate outputs match, then decommission Informatica table by table.

---

**Q7 — Handling PII in ELT / Bronze zone**

The issue is that raw PII is sitting in the Bronze layer, accessible to anyone with lake access.

**Redesign options:**

1. **Tokenisation at ingest**: Replace PII fields (email, card last-four) with tokens before writing to Bronze. A separate secure vault maps tokens to real values. Only authorised services can de-tokenise.

2. **Column-level masking**: Keep raw data in a restricted Bronze zone (fine-grained access control). Silver and Gold tables expose masked versions (`email_domain` instead of full email). Most query users only access Silver+.

3. **ETL for PII fields only (hybrid)**: Use ELT for non-PII columns. For PII columns, mask/tokenise in the extract step before landing in Bronze.

4. **Dynamic data masking**: Snowflake, BigQuery, and Delta Lake all support dynamic data masking — the raw value is stored but masked at query time based on the caller's role.

**Best practice:** Use column-level access control in the lake + dynamic masking in the warehouse. Never store raw PII in a zone accessible to all engineers.

---

**Q8 — Medallion architecture and its relation to ELT**

**Medallion architecture:**

| Zone | Name | Contents | Who accesses |
|---|---|---|---|
| Raw | Bronze | Exact copy of source data, no changes | Data engineers only |
| Cleansed | Silver | Validated, typed, deduplicated, PII masked | Data engineers, some analysts |
| Curated | Gold | Business-ready aggregates, star schema, KPIs | Analysts, BI tools, ML teams |

**Relation to ELT:** Medallion is an implementation of ELT. The "Load" step produces Bronze; subsequent dbt/Spark jobs produce Silver and Gold. Transformation happens inside the warehouse/lake.

**Trade-offs vs. single-pass:**

| Aspect | Medallion | Single-pass |
|---|---|---|
| Reprocessing | Easy — re-run from Bronze | Must re-extract from source |
| Debugging | Inspect each layer independently | Harder — only see start and end |
| Storage cost | Higher (3× the data) | Lower |
| Complexity | Higher (more jobs, more tables) | Lower |
| Time to insight | Slower (3 layers to traverse) | Faster for simple cases |

Medallion wins for complex, evolving data platforms. Single-pass is fine for simple, stable pipelines.

---

**Q9 — E-commerce pipeline: 10M orders/day, updates within 48 hours**

**Choice: ELT with medallion, using CDC or merge-based incremental loads**

**Why ELT:**
- 10M rows/day is too large for full loads (prohibitive compute and time)
- Orders are updated for 48h — a simple watermark on `created_at` misses updates
- Cloud warehouse has the compute to run MERGE statements efficiently

**Design:**

```
[5 Regional DBs] --> [Extract via CDC (Debezium/Fivetran)] --> [raw.orders (Bronze)]
                                                                      |
                                                              [silver.orders]
                                                    (MERGE on order_id, handle SCD)
                                                                      |
                                                              [gold.order_metrics]
```

**Handling updates within 48h:**
- Use **CDC** (change data capture) — stream every INSERT and UPDATE from source DBs
- Apply a MERGE in Silver: if `order_id` exists, UPDATE; else INSERT
- This ensures the current state is always correct without reprocessing all 10M rows

**Challenges:**
- CDC setup is complex (requires Debezium or a managed connector like Fivetran)
- Regional databases may have different schemas — need a normalisation step
- Deduplication if CDC delivers the same event twice (at-least-once delivery)

---

## Concept 3: Idempotency and Incremental vs. Full Loads

**Q10 — What is idempotency?**

A pipeline is **idempotent** if running it N times produces the same result as running it once.

This matters because pipelines fail and must be retried. A non-idempotent pipeline produces duplicates or incorrect state when retried. An idempotent pipeline is safe to retry as many times as needed.

**Practical test:** Run your pipeline. Run it again. Did anything change in the destination? If yes, it is not idempotent.

---

**Q11 — Full load vs. incremental load**

| | Full Load | Incremental Load |
|---|---|---|
| What it does | Truncate destination, reload all source data | Load only new/changed records since last run |
| Watermark needed? | No | Yes (timestamp, ID, or CDC offset) |
| Idempotent by default? | Yes (truncate + reload) | Only if merge/upsert is used |
| Speed | Slow for large tables | Fast |
| Complexity | Low | Medium-High |

**Use full load when:** source is small, no reliable watermark exists, transformation logic changed.  
**Use incremental when:** source is large, records have reliable timestamps or IDs, latency matters.

---

**Q12 — Duplicate orders from failed + retried job**

**Root cause:** The pipeline uses `INSERT` without first deleting the target partition. When the job failed at 2:45 AM, some rows may already have been inserted. When it retried at 3:00 AM, it inserted the same rows again. The destination now has duplicates.

**Fix — two options:**

Option A — DELETE then INSERT (partition-based):
```sql
DELETE FROM orders_silver WHERE run_date = '2024-01-15';
INSERT INTO orders_silver
SELECT * FROM orders_raw WHERE DATE(created_at) = '2024-01-15';
```

Option B — MERGE (upsert):
```sql
MERGE INTO orders_silver AS target
USING (SELECT * FROM orders_raw WHERE DATE(created_at) = '2024-01-15') AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET target.status = source.status, ...
WHEN NOT MATCHED THEN INSERT VALUES (source.order_id, source.status, ...);
```

Option B is safer because it handles late-arriving updates too, not just inserts.

---

**Q13 — Watermark with lagged application server timestamps**

**Problem:** If an order is updated at 3:00 PM but the application server stamps it `2:30 PM` (30-minute lag), and your last watermark was `2:45 PM`, this update is invisible to incremental loads going forward.

**Options:**

1. **Lookback window:** Always re-process the last 60 minutes (or more) of data.
   ```
   WHERE updated_at > :last_watermark - INTERVAL '60 minutes'
   ```
   Risk: still misses records lagged more than the window.

2. **Use database server time instead of application time:** Add a `db_modified_at` column set by a database trigger (`DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`). Database clocks are typically accurate.

3. **Switch to CDC:** Change Data Capture reads the database write-ahead log, which uses database commit time — immune to application clock lag.

4. **Accept the limitation + monitor:** Run a weekly reconciliation job that does a full count comparison between source and destination, alerting on discrepancies.

In practice, option 2 or 3 is the right fix. Lookback windows are a band-aid.

---

**Q14 — Idempotency vs. exactly-once delivery**

**Exactly-once delivery** means a message or record is processed exactly one time by the pipeline infrastructure — even in the face of failures, retries, and network partitions.

**Idempotency** means re-processing the same record produces the same result.

**They are different:**
- Exactly-once is a **delivery guarantee** (about the infrastructure)
- Idempotency is a **correctness property** (about the write logic)

**You can have idempotency without exactly-once:** Process the same record multiple times (at-least-once), but because the write uses MERGE/upsert, the result is the same as if it was processed once.

**You cannot have exactly-once without significant infrastructure support:** Kafka transactions, distributed two-phase commits — complex and expensive.

**Production recommendation:** Target **at-least-once delivery + idempotent writes**. This is far simpler to implement than true exactly-once and gives the same observable result.

---

**Q15 — 500M row incremental sync: MySQL → BigQuery using event_id**

**Phase 1: Initial full load**

Because the table is 500M rows, export using parallel extraction:
```
Chunk by event_id ranges: 0-10M, 10M-20M, ... (50 parallel workers)
Export each chunk to CSV/Parquet in GCS
Load all files into BigQuery raw.events in one batch load job
Save watermark: max(event_id) = N
```

**Phase 2: Incremental loads (ongoing)**

```python
watermark = read_watermark()  # stored in a metadata table or GCS file
new_events = SELECT * FROM mysql.events WHERE event_id > watermark
             LIMIT 1000000  # cap per run to control memory
load to BigQuery raw.events (append)
new_watermark = max(event_id) from new_events
save_watermark(new_watermark)
```

Since events are never updated and `event_id` is monotonically increasing, plain APPEND is safe (no MERGE needed). The watermark is the highest `event_id` seen.

**Failure recovery:**
- If the load fails after extraction but before saving the watermark, the next run re-extracts the same events and appends again → duplicates
- Fix: deduplicate in BigQuery using `CREATE OR REPLACE TABLE raw.events AS SELECT DISTINCT * FROM raw.events` on the next run, OR use a staging table + INSERT IGNORE

**Better approach:** Use a managed connector (Fivetran, Airbyte) that handles watermark persistence and deduplication. Only build custom if connectors are unavailable.

---

## Concept 4: Data Lineage and Dependency Graphs

**Q16 — What is data lineage and why does it matter?**

Data lineage is the ability to trace any piece of data from its origin through all transformations to its final destination.

**Two concrete scenarios:**

1. **Debugging:** The `monthly_active_users` metric dropped 30% overnight. Lineage immediately tells you: this metric comes from `gold.user_activity`, which comes from `silver.sessions`, which comes from `raw.clickstream`. You check `raw.clickstream` first — and find the source team changed the event schema, dropping 30% of rows.

2. **GDPR deletion:** A user requests deletion. Without lineage, you search 200 tables hoping to find all copies. With lineage, you query the lineage graph for nodes that include `user_id = X` and get a complete list in seconds.

---

**Q17 — Table-level vs. column-level lineage**

**Table-level lineage:** `silver.orders` depends on `raw.orders` — you know which tables are related, but not which specific columns or transformations.

**Column-level lineage:** `silver.orders.net_amount` = `raw.orders.amount` - `raw.orders.discount_amount` — you know the exact derivation.

**Which is more useful:**
- Column-level is significantly more useful for debugging (you know which source column is wrong) and for impact analysis (a column rename only breaks the pipelines that reference that specific column)
- Table-level is sufficient for high-level dependency mapping and scheduling order

Column-level is harder to capture and maintain (requires SQL parsing), which is why many tools offer table-level by default and column-level as a premium feature.

---

**Q18 — Debugging wrong revenue metric**

Lineage:
```
raw.orders → silver.orders → gold.order_metrics → gold.revenue_summary → monthly_report
raw.returns → silver.returns → gold.order_metrics
```

**Investigation order (work backwards from the symptom):**

1. **Check `gold.revenue_summary`:** Is the value wrong here? Compare today vs. last month for the same period.

2. **Check `gold.order_metrics`:** Are the inputs to revenue_summary correct? Spot-check row counts and sum of revenue against a known-good period.

3. **Identify the divergence point:** If `gold.order_metrics` is wrong, the issue is in the join or aggregation logic. If it is correct, the issue is in `gold.revenue_summary`'s aggregation query.

4. **If `gold.order_metrics` is wrong:** Check both inputs:
   - `silver.orders`: Are row counts expected? Are there unexpected nulls in `amount`?
   - `silver.returns`: Has the returns data changed? Could a large batch of returns have been loaded for the wrong time period?

5. **Check `raw.orders` and `raw.returns`:** Are the source counts expected? Was there a data load issue?

6. **Check transformation logic:** Did anyone change the SQL in `gold.order_metrics` recently? Run `git log` on the dbt models.

**Most common root causes:** Source schema change (silent null introduction), a transformation logic bug deployed this week, or an unexpected volume of returns loaded for the wrong date partition.

---

**Q19 — GDPR deletion for user_id = 7291**

**Tables involved:** `raw.users`, `silver.users`, `gold.customer_ltv`, `gold.segment_profiles`, `churn_prediction feature store`

**Deletion order (most atomic/raw first):**

1. Delete from `raw.users` (source of truth — prevents re-ingestion of deleted data)
2. Delete from `silver.users` (derived from raw — straightforward row delete)
3. Delete from `churn_prediction feature store` (point-in-time features, delete all rows for this user)
4. For `gold.customer_ltv`: **challenge** — if the row is simply `customer_id, lifetime_value`, delete it. If the value is baked into an aggregate (`total_revenue_by_segment`), you cannot delete just one user's contribution without recomputing the aggregate
5. For `gold.segment_profiles`: same challenge as above

**Challenges with aggregated tables:**
- Aggregates like "Total Q1 revenue = $5.2M" baked into a dashboard cannot be surgically modified
- GDPR allows "right to erasure" but distinguishes between personal data (must delete) and statistical data (can retain if not attributable to the individual)
- Practical solution: remove the user's row from all tables. Accept that historical aggregate snapshots may retain the statistical contribution but the personal record is gone. Document this in your GDPR policy.

**Prevention:** Design Gold tables to store aggregations by non-PII dimensions (region, category, cohort) rather than by `customer_id` wherever possible.

---

**Q20 — Impact analysis for column split: full_name → first_name + last_name**

**Step 1:** Query the lineage graph for all nodes that reference `raw.customers.full_name`.

**Step 2:** Build the impact list:
```
raw.customers.full_name
    → silver.customers.full_name (direct copy)
        → gold.customer_orders (JOIN key? display field?)
            → Revenue Dashboard (does it display name?)
            → Customer Report (filtered by name?)
    → silver.customers.display_name (derived: UPPER(full_name))
    → Any ML features that use customer name
```

**Step 3:** Classify each impact:
- **Breaking change:** pipelines that read `full_name` by name — they will fail after the schema change
- **Non-breaking but needs update:** pipelines that will still work but need to concat `first_name || ' ' || last_name` for display

**Step 4:** Rollout plan:
1. Request the source team to add `first_name` and `last_name` as new columns *before* removing `full_name` (additive-only change first)
2. Update all downstream pipelines to use the new columns
3. Validate outputs match (compare `silver.customers.full_name` vs `first_name || ' ' || last_name`)
4. Once all pipelines are migrated, request the source team to remove `full_name`

**Key principle:** Never remove a column without a deprecation window. Add first, migrate all consumers, then remove.

---

## Concept 5: Pipeline Failure Modes and Retry Strategies

**Q21 — Three delivery guarantees**

| Guarantee | Meaning | Mechanism | Hardest? |
|---|---|---|---|
| At-most-once | Process 0 or 1 times | Fire and forget; no retry | No — easiest to implement |
| At-least-once | Process 1 or more times | Retry on failure; may duplicate | Medium |
| Exactly-once | Process exactly 1 time | Requires distributed transactions | Yes — hardest |

**Exactly-once is hardest** because it requires coordination between the producer, the pipeline, and the consumer to ensure no duplication or loss — even when any component fails mid-operation. This typically requires:
- Idempotent writes (so retries are safe)
- Transactional commits (write + acknowledge atomically)
- Deduplication IDs checked at the consumer

In practice, target **at-least-once + idempotent writes** — operationally equivalent to exactly-once without the full complexity.

---

**Q22 — What is a dead-letter queue?**

A **dead-letter queue (DLQ)** is a separate storage location where records that cannot be processed successfully (after all retries) are sent, rather than blocking the main pipeline.

**When to use it:**
- When a record is malformed and will never succeed (bad JSON, unrecognised currency, null required field)
- When you want to inspect failures without losing the record
- When one bad record should not block processing of subsequent good records

**What to store in the DLQ:**
- The original record (raw, unmodified)
- The error message and stack trace
- Timestamp of failure
- Number of retry attempts
- Pipeline name and step where it failed

**How teams use DLQs:** Monitor the DLQ size; alert if it grows above a threshold; manually inspect and re-process or discard records.

---

**Q23 — Retry strategy for weather API**

```python
import time
import random

def call_weather_api(order_id, max_retries=5):
    base_delay = 1  # seconds
    
    for attempt in range(1, max_retries + 1):
        try:
            response = weather_api.get(order_id)
            
            if response.status_code == 200:
                return response.json()
            
            elif response.status_code == 429:
                # Rate limited — respect Retry-After header if present
                retry_after = int(response.headers.get("Retry-After", base_delay * (2 ** attempt)))
                jitter = random.uniform(0, 1)
                wait = retry_after + jitter
                log(f"Rate limited. Waiting {wait:.1f}s before attempt {attempt + 1}")
                time.sleep(wait)
            
            elif response.status_code >= 500:
                # Transient server error
                wait = base_delay * (2 ** attempt) + random.uniform(0, 1)
                log(f"Server error {response.status_code}. Waiting {wait:.1f}s")
                time.sleep(wait)
            
            else:
                # Non-retryable error (400, 404) — fail immediately
                raise ValueError(f"Non-retryable error: {response.status_code}")
        
        except Exception as e:
            if attempt == max_retries:
                log(f"All {max_retries} attempts failed for order {order_id}: {e}")
                send_alert(f"Weather API failed for order {order_id}")
                write_to_dlq(order_id, error=str(e))
                return None  # continue processing other orders
            
    return None
```

**Rate limit handling:** For 60 req/min, add a global rate limiter (`time.sleep(1)` between calls, or use a token bucket).

---

**Q24 — Write-audit-publish pattern**

**The pattern:**
1. Write transformed data to a `_staging` table
2. Run validation checks on the staging table
3. Only if all validations pass: atomically swap staging → production (rename or INSERT OVERWRITE)
4. If validation fails: alert, do not update production

**When to prefer it:**
- Production table is read by live dashboards — a bad load must never reach it
- Financial or regulatory data — correctness is mandatory
- Large batch loads — halfway-written state is worse than stale data

**Trade-offs:**
- Requires 2× storage temporarily
- Slightly slower (extra validation step)
- More complex orchestration (need conditional step in DAG)
- Worth it for critical tables; overkill for internal staging tables

---

**Q25 — Fixing a double-insert bug**

**Problem:** `INSERT INTO target SELECT * FROM staging` with no deduplication. Running it twice inserts all rows twice.

**Fix — Option A: TRUNCATE + INSERT (full load):**
```sql
BEGIN TRANSACTION;
TRUNCATE TABLE target;
INSERT INTO target SELECT * FROM staging;
COMMIT;
```
The transaction ensures the table is never in a half-empty state. The TRUNCATE + INSERT is atomic within the transaction.

**Fix — Option B: MERGE (incremental-safe):**
```sql
MERGE INTO target AS t
USING staging AS s
ON t.primary_key = s.primary_key
WHEN MATCHED THEN UPDATE SET t.col1 = s.col1, t.col2 = s.col2, ...
WHEN NOT MATCHED THEN INSERT (primary_key, col1, col2) VALUES (s.primary_key, s.col1, s.col2);
```

**Additional mechanism to guarantee no duplicates if pipeline crashes mid-write:**
Use a **write marker / run ID**:
```sql
INSERT INTO pipeline_runs (run_id, table_name, status, started_at)
VALUES (:run_id, 'target', 'in_progress', NOW());

-- ... do the MERGE ...

UPDATE pipeline_runs SET status = 'completed', finished_at = NOW()
WHERE run_id = :run_id;
```
At the start of each run, check if the previous run for this time period completed. If yes, skip. If in-progress, the previous run crashed — re-run safely because MERGE is idempotent.

---

**Q26 — Financial transaction pipeline: system design**

**Requirements:** No loss, no duplicates, auto-recover, 5-minute alert SLA.

**Design:**

```
[Bank Source API]
       |
       v
[Extract + Write to DLQ if API fails]
       |
       v
[Staging table: transactions_staging]
       |
       v
[Audit: count > 0, no null transaction_ids, sum positive, no duplicates]
       |
   Pass ──> [MERGE into transactions_silver ON transaction_id]
   Fail ──> [Alert (PagerDuty)] + [Leave transactions_silver unchanged]
       |
       v
[Update watermark: last processed timestamp]
```

**Delivery guarantee:** At-least-once extraction + idempotent MERGE = effectively exactly-once result.

**Failure detection:** 
- Heartbeat check: if no new rows in `transactions_silver` for >10 min during business hours → alert
- Row count check: compare extracted count vs. loaded count at end of each run
- Pipeline monitoring: if pipeline DAG fails → immediate alert via Airflow/Prefect

**Retry strategy:** Exponential backoff (1s, 2s, 4s, 8s, 16s) for transient API errors; max 5 retries before alerting.

**Dead-letter:** Any transaction that fails validation (bad currency, null amount) goes to `transactions_dlq` with error reason. Team reviews daily.

---

## Mixed / Senior-Level Questions

**Q27 — VARCHAR(50) to VARCHAR(255) schema change**

**It depends on where the data flows.**

For most destinations (Snowflake, BigQuery, Postgres): VARCHAR size is not enforced at read time — data that was 50 chars wide is still valid at 255 chars wide. No immediate breakage.

**What could go wrong:**
- Some ETL tools or schemas infer type metadata at pipeline setup time. If the tool stored `VARCHAR(50)` in its schema registry, the mismatch may cause a validation error.
- If your pipeline truncates values to 50 chars during transformation (defensive coding: `SUBSTR(col, 1, 50)`), the truncation would now silently lose data if the source starts sending longer values.
- Downstream systems with `VARCHAR(50)` columns would overflow if source values now exceed 50 chars.

**Action:** Check if any transformation step enforces a length constraint. Check all downstream sinks for column definitions. Run `SELECT MAX(LENGTH(column)) FROM source` to see if values are already approaching 255.

---

**Q28 — Pipeline bug vs. data quality issue**

**Pipeline bug:** The code/logic has an error that causes incorrect behaviour regardless of the input data.
- Example: A join uses the wrong key (`ON a.order_id = b.customer_id`) — every row is affected, always wrong.
- Response: Fix the code, re-run the pipeline for the affected date range, validate.

**Data quality issue:** The data from the source is incorrect, inconsistent, or unexpected — the pipeline logic is correct but the input is bad.
- Example: The source team loaded test orders into production with `order_amount = -9999`. The pipeline correctly loaded them; the data is just wrong.
- Response: Identify and quarantine the bad records, coordinate with the source team to fix at origin, add validation rules to catch this class of issue in future.

**How the response differs:**
- Bug: code review, regression test, fix and re-deploy
- Data quality: upstream coordination, quarantine bad records, add pipeline-level validation (not just fix-and-move-on)

---

**Q29 — Pipeline runs 20 min normally, 4 hours at end of month**

**Options:**

1. **Partitioning:** Ensure the source table is partitioned by date. The pipeline query should filter by partition (`WHERE date_partition = :run_date`) so end-of-month does not scan the full history. Most slow pipelines fail this — they scan everything and filter in memory.

2. **Parallelism:** Break the end-of-month load into chunks processed in parallel. Instead of one job processing 30 days, run 30 parallel jobs (one per day) on the last day of the month.

3. **Incremental design:** If running a full load on the last day of the month, switch to incremental. Only process the records added since the last successful run.

4. **Resource scaling:** If using Spark or a similar engine, increase executor count for the known-heavy runs (triggered by the date in the run config).

5. **Separate SLA:** Accept that end-of-month runs take longer, but schedule them earlier (start at midnight, SLA is 6 AM instead of 30 min for daily runs). Not ideal but sometimes pragmatic.

**Root cause first:** Profile the query. Is it a full table scan? A poorly optimised join? A single-threaded Python loop? Fix the root cause before adding resources.

---

**Q30 — When to intentionally choose a non-idempotent pipeline**  

**Legitimate scenario: Append-only audit/event logs**

If you are building an audit trail where every pipeline run must produce a new record (e.g., `pipeline_audit_log` that records each run's row counts, timestamps, and status), you *want* each run to append a new row. Making this idempotent (by upserting on run_id) is correct, but if you use a plain INSERT without a run_id, the log is non-idempotent — and that is intentional. You *want* every re-run to appear in the log.

**Trade-offs accepted:**
- If the pipeline is re-run for a failure recovery, the audit log shows the re-run as a separate entry — which is actually desirable for audit purposes
- The log table grows indefinitely — manageable with retention policies

**Another scenario: Real-time streaming with at-most-once semantics**
For very high-throughput, low-latency streams where occasional data loss is acceptable (e.g., metrics telemetry, non-critical event counters), at-most-once delivery avoids the overhead of idempotency tracking. You accept that some events may be lost in exchange for lower latency and simpler infrastructure.

**Key point:** Non-idempotency is a conscious trade-off, not an accident. Document it, test it, and ensure the downstream consumers understand the behaviour.

---

## Concept 6: Data Schemas & Schema Evolution

**Q31 — Schema-on-write vs. schema-on-read**

**Schema-on-write:** The schema is enforced at the time data is written. If the data does not conform, it is rejected.
- Example: Writing to a PostgreSQL table. The table has `order_amount INTEGER NOT NULL`. If a producer sends `NULL`, the database rejects the row immediately.

**Schema-on-read:** Data is written in any format without validation. The schema is applied when data is read — the reader interprets raw bytes.
- Example: Dropping a JSON file into S3. Any JSON is accepted at write time. When an analyst queries it via Athena, missing fields become `NULL` and type mismatches become parse errors.

**Which is better:** Neither — they serve different stages of maturity. Raw/Bronze zones use schema-on-read for flexibility (you do not always know the schema upfront). Silver/Gold zones enforce schema-on-write for correctness. A mature platform enforces schema-on-write everywhere except the raw landing zone.

---

**Q32 — Backward compatible vs. breaking schema change**

Given table: `(user_id INT, email VARCHAR, created_at TIMESTAMP)`

**Backward compatible (non-breaking):**
```sql
ALTER TABLE users ADD COLUMN phone_number VARCHAR DEFAULT NULL;
```
Old pipelines that read this table and do not reference `phone_number` continue to work. The new column is invisible to them.

**Breaking change:**
```sql
ALTER TABLE users DROP COLUMN email;
-- OR
ALTER TABLE users RENAME COLUMN email TO email_address;
```
Any pipeline that references `email` by name now throws a column-not-found error. All consumers must be updated before or simultaneously with this change.

**Rule of thumb:** Adding nullable columns is safe. Removing, renaming, or changing the type of existing columns is breaking unless all consumers are updated first.

---

**Q33 — Source team renames cust_id to customer_id next Tuesday**

**Do not wait for Tuesday morning to find out your pipeline broke.**

**Immediate response:**
1. Ask the source team to implement this as a **two-phase migration**: add `customer_id` as a new column (alias/copy) first, keep `cust_id` for 2–4 weeks, then remove `cust_id` after all consumers are migrated.
2. If they refuse, negotiate a **maintenance window** where both your pipeline and theirs are taken down simultaneously, your pipeline is updated, and both come back up together.

**Action plan:**
1. Identify every query, pipeline, and transformation that references `cust_id` (grep/lineage graph)
2. Update each reference to use `customer_id`
3. Deploy and test in a staging environment against the new schema
4. Coordinate go-live with the source team

**What NOT to do:** Simply update your pipeline the night before and hope nothing else references `cust_id`. Always search the full lineage.

---

**Q34 — Why `SELECT *` is dangerous in pipelines**

**Failure mode 1 — Column added upstream:**
```sql
-- Your pipeline does:
SELECT * FROM raw.orders  -- was 6 columns, now 7 after source adds discount_code

-- Your Silver table was created with 6 columns
INSERT INTO silver.orders SELECT * FROM raw.orders
-- Error: INSERT has more columns than target table expects
```
The pipeline crashes. If you had named columns explicitly, the new column would simply be ignored.

**Failure mode 2 — Column removed upstream:**
```sql
-- Source removes currency column
SELECT * FROM raw.orders  -- now returns 5 columns instead of 6

-- Downstream transformation references column by position (not name)
df.columns[5]  -- was currency, now points to wrong column (or throws IndexError)
```
No error is raised at read time — wrong data silently flows downstream. This is worse than a crash because you may not notice for days.

**Best practice:**
```sql
-- Always name the columns you need
SELECT order_id, customer_id, order_date, order_amount, status
FROM raw.orders
```
This way, schema changes in columns you do not use are invisible, and changes to columns you do use produce an explicit error immediately.

---

**Q35 — Schema management for 50 producers, 20 consumers on Kafka**

**Tool: Schema Registry** (Confluent Schema Registry is the standard; AWS Glue Schema Registry is the managed alternative).

**How it works:**
1. Each producer registers its schema (Avro, Protobuf, or JSON Schema) with the registry before publishing
2. The registry assigns a schema ID and version
3. Each message includes the schema ID in its header (not the full schema — just 4 bytes)
4. Each consumer fetches the schema from the registry using the ID and deserializes accordingly

**Compatibility rules configured in the registry:**
- `BACKWARD`: new schema can read data written with old schema (add nullable fields only)
- `FORWARD`: old schema can read data written with new schema (remove fields only)
- `FULL`: both backward and forward (add nullable OR remove with defaults)
- `NONE`: no compatibility enforced (dangerous in production)

**Enforcement:**
- The registry **rejects** schema registrations that violate the configured compatibility rule
- This prevents producers from accidentally publishing incompatible schemas
- Consumers can always deserialise older messages using the version stored in the registry

**Additional practice:** Run schema compatibility checks in CI/CD before deployment. If a producer's new code registers an incompatible schema, the CI pipeline fails.

---

## Concept 7: Batch vs. Micro-batch vs. Streaming

**Q36 — Three processing models with use cases**

| Model | Characteristics | Real-world use case |
|---|---|---|
| Batch | Processes accumulated data at scheduled intervals; highest throughput, highest latency | Nightly invoice generation, weekly ML model retraining, monthly billing |
| Micro-batch | Processes small windows (seconds to minutes) on a rolling schedule | Spark Structured Streaming refreshing a dashboard every 30 seconds, near-real-time anomaly detection |
| Streaming (event-by-event) | Processes each event as it arrives; lowest latency, highest complexity | Real-time fraud detection (must decide in <500ms), live sports score feeds, stock price tickers |

---

**Q37 — Lambda vs. Kappa architecture**

**Lambda architecture:**
- Runs two parallel pipelines: a **batch layer** (correct, high-latency) and a **speed layer** (approximate, low-latency)
- Results are merged at query time: recent data from the speed layer, historical data from the batch layer
- Problem: two codebases implementing the same business logic. When logic changes, both must be updated. They frequently drift.

**Kappa architecture:**
- One pipeline: everything is streaming
- Historical reprocessing is done by replaying the event log (Kafka with long retention) through the same streaming pipeline
- Simpler: one codebase, one set of business logic

**Which is preferred today:** Kappa, for most use cases. Managed stream processing (Flink, Spark Structured Streaming) has matured enough that the "streaming is too complex" argument no longer holds for most teams. Lambda is still appropriate when batch and streaming truly have different correctness requirements (e.g., ML training needs batch semantics that streaming cannot replicate easily).

---

**Q38 — Real-time dashboard + daily summary: one pipeline or two?**

**Two separate pipelines.**

**Real-time dashboard (last 5 minutes, updated every 10 seconds):**
- Processing model: **micro-batch** (every 10 seconds)
- Why not true streaming: 10-second updates do not require event-by-event processing; micro-batch is far simpler and cheaper
- Implementation: Spark Structured Streaming with a 10-second trigger, writing to a fast-read store (Redis, ClickHouse, or a materialized view)

**Daily finance summary:**
- Processing model: **batch** (runs once per day)
- Why not streaming: finance needs a complete, consistent snapshot of the day. Batch gives exactly that. Streaming would add unnecessary complexity.
- Implementation: Airflow-scheduled Spark or dbt job running at midnight

**Why not one pipeline:** Mixing real-time and daily requirements in one pipeline forces the daily batch to run every 10 seconds (wasteful) or forces the real-time dashboard to wait until midnight (defeats the purpose). Separate pipelines let each be optimised independently.

---

**Q39 — What is a watermark in streaming?**

In streaming, events carry an **event time** (when the event actually happened) which differs from **processing time** (when the system receives it). Events arrive out of order due to network delays, retries, or mobile devices buffering offline.

A **watermark** is a system's declaration: *"I believe all events with event time before T have now arrived. I will not wait for any more events older than T."*

```
Watermark = max(event_time seen so far) - allowed_lateness
```

**Why it is needed:** Without a watermark, a windowed aggregation (e.g., "sum of orders in the 14:00–14:05 window") would wait forever for late arrivals. The watermark tells the engine when it is safe to close and emit the window result.

**What happens to events arriving after the watermark:**
- **Drop**: simplest — late events are ignored. Some data loss is accepted.
- **Side output**: late events are routed to a separate stream for separate handling (backfill, alerting, manual review)
- **Window recomputation**: the window is updated and a corrected result is emitted. Most correct but most complex — requires downstream consumers to handle result corrections.

---

**Q40 — Micro-batch vs. true event-by-event streaming**

The claim is **partially accurate but oversimplified**. Key differences:

| | Micro-batch (Spark) | True Streaming (Flink) |
|---|---|---|
| Processing unit | Mini-batches (fixed time interval) | Individual events |
| Latency floor | Batch interval (minimum ~100ms) | Sub-millisecond possible |
| State management | State is snapshotted per batch | Continuous stateful operators |
| Exactly-once | Achievable via batch transactions | Achievable via distributed snapshots (Chandy-Lamport) |
| Late data handling | Watermark per batch boundary | Continuous watermark, fine-grained |
| Complexity | Lower — batch semantics familiar to engineers | Higher — event-time reasoning, async operators |

**Key conceptual difference:** In micro-batch, the engine collects events for a fixed interval then processes the whole mini-batch as one atomic unit. In true streaming, each event triggers computation immediately and stateful operators maintain running results continuously. They look similar at a high level but their internal execution models are fundamentally different.

---

## Concept 8: Data Partitioning & Bucketing

**Q41 — What is partitioning and partition pruning?**

**Partitioning** organises table data into separate physical directories or storage segments based on the value of one or more columns. Instead of one large directory of files, data is split into subdirectories per partition value:

```
orders/order_date=2024-01-15/part-001.parquet
orders/order_date=2024-01-16/part-001.parquet
```

**Partition pruning** is the query engine's ability to skip entire partitions that do not match the query's filter:
```sql
SELECT * FROM orders WHERE order_date = '2024-01-15'
-- Engine reads ONLY the order_date=2024-01-15/ directory
-- Skips all other dates entirely
```

Without partitioning, `WHERE order_date = '2024-01-15'` on a 2 TB table still reads all 2 TB and filters in memory. With daily partitioning over 3 years, the same query reads ~1.8 GB (one day's data).

---

**Q42 — The small files problem**

**How it arises from partitioning:**
Over-partitioning creates too many small directories, each with too few rows. For example, partitioning a 10 GB table by `user_id` with 1 million distinct users creates 1 million directories, each with ~10 KB of data. Each Parquet file has fixed metadata overhead (~8 KB). At 10 KB of data, you are spending nearly half your read time on metadata.

**Consequences:**
- **Slow queries:** Each file requires a filesystem call to open, read metadata, and close. Opening 1 million files takes longer than reading 1 large file of the same total size.
- **Slow writes:** Spark must open a writer for each partition simultaneously — too many partitions causes driver OOM.
- **High cloud storage costs:** Object stores (S3, GCS) charge per API call. Listing millions of small files is expensive.
- **Slow metastore operations:** Tools like Hive Metastore or the Glue Catalog must track every partition — millions of partitions degrades catalog query performance.

**Fix:** Compact small files periodically (OPTIMIZE in Delta Lake, compaction jobs in Spark). Target 128 MB–1 GB per file.

---

**Q43 — Partitioning strategy for 5 TB events table**

**Query pattern:** Almost exclusively filtered by `event_date` and `country`.

**Strategy: Partition by `event_date`, optionally sub-partition by `country`**

**Step 1 — Always partition by date first:**
```
events/event_date=2024-01-15/
events/event_date=2024-01-16/
```
This alone reduces most queries from 5 TB to one day's data.

**Step 2 — Should you add `country`?**
- If there are ~50 countries, adding `country` as a second partition creates 50 × 365 = 18,250 directories per year
- If data is evenly distributed, each directory holds 5 TB / 18,250 ≈ 274 MB — fine
- If data is skewed (e.g., 80% of events from US), the US partition files will be huge and others tiny (small files problem)
- Only add `country` if queries **always** filter by both `event_date` AND `country`. If queries sometimes filter only by date, the second partition adds directory overhead without pruning benefit

**File size target:** 128 MB–1 GB per Parquet file per partition. If daily data is 14 GB, aim for 14–112 files per day partition.

---

**Q44 — Partitioning by user_id with 10 million distinct values**

**What goes wrong:**

The table is split into 10 million directories, each containing a tiny slice of data. For a 500 GB table, each partition averages 50 KB — far below the optimal 128 MB file size. This is the small files problem at its worst.

Query performance degrades because:
- Opening 10 million files has more overhead than reading 500 GB sequentially
- Metastore operations (listing partitions, updating stats) become extremely slow
- Writing new data requires opening a file handle per user — Spark drivers run out of memory managing millions of concurrent writers

**How to fix:**

Option 1 — **Do not partition by user_id.** Use bucketing instead:
```sql
CLUSTER BY (user_id) INTO 128 BUCKETS
```
128 buckets from 10 million users = ~78,000 users per bucket. Manageable file sizes, efficient hash-based joins.

Option 2 — **Partition by a lower-cardinality derived column.** If queries filter by date AND user, partition by date only:
```
events/event_date=2024-01-15/
```
Then within a date partition, use bucketing on `user_id` for join efficiency.

---

**Q45 — Partitioning vs. bucketing**

| | Partitioning | Bucketing |
|---|---|---|
| Mechanism | Physical directories per distinct value | Hash of column value → fixed N buckets |
| Number of divisions | Grows with distinct values | Fixed at table creation |
| Query benefit | Partition pruning (skip directories) | Efficient joins (no full shuffle) |
| Best column type | Low-cardinality, query filter columns (date, region) | High-cardinality join/group-by columns (user_id, order_id) |
| Small files risk | High if column has many distinct values | None (always N buckets regardless of data volume) |

**Scenario where bucketing outperforms partitioning:**
Two 100 GB tables joined on `customer_id`. Without bucketing, Spark must shuffle all 200 GB across the network to co-locate matching `customer_id` values. With both tables bucketed on `customer_id` into the same number of buckets, Spark knows that bucket 42 from table A matches bucket 42 from table B — no shuffle needed (sort-merge join).

**Can you use both?** Yes. A common pattern:
```sql
-- Partition by date (for time-range pruning), bucket by customer_id (for join efficiency)
PARTITIONED BY (order_date)
CLUSTERED BY (customer_id) INTO 64 BUCKETS
```
This gives pruning on date-filtered queries and shuffle-free joins on customer_id.

---

## Concept 9: Pipeline Observability & Monitoring

**Q46 — Four pillars of pipeline observability**

| Pillar | What it measures | Concrete check |
|---|---|---|
| **Freshness** | How up-to-date is the data? | `MAX(event_time) < NOW() - 2 hours` → alert |
| **Volume** | Did the expected amount of data arrive? | Today's row count < 70% of 7-day average → alert |
| **Quality** | Are column values correct and valid? | `COUNT(*) WHERE order_id IS NULL > 0` → fail pipeline |
| **Pipeline health** | Did the job run and complete on time? | Pipeline did not start within 30 min of scheduled time → alert |

---

**Q47 — SLA, SLO, SLI for a nightly ETL job**

**SLA (external commitment to a stakeholder):**
"The daily sales dashboard will reflect data from the previous day no later than 07:00 AM."

**SLO (internal engineering target):**
"The nightly ETL pipeline completes within 90 minutes of its 02:00 AM start time, 99% of days."

**SLI (the measurement):**
"Today's pipeline started at 02:00 AM and completed at 03:23 AM — duration 83 minutes. Max event timestamp in Silver = 01:58 AM."

**How they relate:**
The SLO (complete by 03:30) provides headroom before the SLA (dashboard ready by 07:00). If the SLO is regularly missed, the SLA is at risk. Engineers act on SLO breaches; stakeholders are only impacted by SLA breaches.

---

**Q48 — Pipeline "succeeded" but dashboard numbers are wrong**

Four possibilities where a pipeline exits 0 but data is wrong:

1. **Silent schema mismatch:** The source added a column and the pipeline uses `SELECT *` — data landed in the wrong columns without any error (Failure mode 2 from Q34).

2. **Filter condition is too broad or too narrow:** The incremental watermark was wrong — the pipeline reprocessed old records (duplicates) or skipped new ones (missing data). No exception is thrown.

3. **Join fanout / data explosion:** A many-to-one join was accidentally written as many-to-many — rows were multiplied. Row count looks high but not obviously wrong without a baseline.

4. **Timezone offset bug:** `order_date` was cast using the server timezone instead of UTC. Orders placed near midnight are attributed to the wrong day. The pipeline ran fine; the date partition is just shifted.

**How observability catches each:**
1. Column-level schema check at ingest compares column names/types against expected schema
2. Row count anomaly check (vs. 7-day average) catches duplicates and gaps
3. Sum-of-amount check against a known-good aggregate catches fanout
4. Freshness check on event timestamps (not just run completion) catches timezone drift

---

**Q49 — Data quality framework for 50-column Silver table**

**Prioritisation — not all columns are equal:**

| Priority | Column type | Checks to run |
|---|---|---|
| Critical | Primary keys, join keys, foreign keys | Non-null, unique, referential integrity |
| High | Business metrics (amount, quantity, dates) | Non-null, range bounds, no negative values where impossible |
| Medium | Categorical fields (status, type, region) | Allowed value set, unexpected new categories |
| Low | Optional enrichment fields (description, notes) | Spot-check null rate trend only |

**Failure handling — three tiers:**

| Severity | Condition | Action |
|---|---|---|
| Block | Primary key has nulls or duplicates | Fail pipeline, do not write to production, alert immediately |
| Warn | Null rate in `discount_code` rose from 2% to 15% | Write to production with warning, alert for investigation |
| Quarantine | Row-level: `order_amount < 0` | Move bad rows to `orders_silver_quarantine`, write clean rows to production |

**Implementation pattern:**
```python
CHECKS = [
    # (name, severity, sql_assertion_returns_zero_on_pass)
    ("pk_not_null",      "block",      "SELECT COUNT(*) FROM t WHERE order_id IS NULL"),
    ("pk_unique",        "block",      "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM t"),
    ("amount_positive",  "quarantine", "SELECT COUNT(*) FROM t WHERE order_amount < 0"),
    ("status_valid",     "warn",       "SELECT COUNT(*) FROM t WHERE status NOT IN ('completed','pending','cancelled')"),
]
```
Run block checks first. Only proceed to warn/quarantine checks if block checks pass.

---

**Q50 — What three standard checks do NOT catch**

Standard checks: row count, null check, range check.

**What they miss:**

1. **Semantic / business logic errors:** Row count is 50,000 (normal). Null rate is 0%. All amounts are positive. But the pipeline joined on the wrong key — every customer's orders are attributed to the wrong region. All checks pass; every number is wrong.

2. **Stale-but-valid data (frozen metrics):** The pipeline ran and loaded 50,000 rows with no nulls and valid ranges — but these are the same 50,000 rows from yesterday because the source API returned a cached response. Freshness check (max event time) would catch this, but row count / null / range checks would not.

**The gap:** These three checks verify *shape* (right number of rows, right column presence, valid value ranges). They do not verify *content correctness* (are these the right rows? Do the values reflect reality?). Catching content errors requires business-logic assertions: expected aggregates, cross-table consistency checks, and freshness monitoring.

---

## Concept 10: Orchestration & Dependency Management

**Q51 — What is a pipeline orchestrator and what does it solve over cron?**

A **pipeline orchestrator** is a system that schedules, sequences, monitors, and recovers pipeline tasks based on declared dependencies and schedules.

**What cron cannot do that an orchestrator solves:**

| Problem | Cron | Orchestrator |
|---|---|---|
| Task B must wait for Task A | You must manually offset the schedule and hope A finishes in time | Explicit dependency: `B.set_upstream(A)`; B only runs when A succeeds |
| Retry on failure | Cron does not retry | Configurable retry with backoff per task |
| Visibility | No UI; check syslog | Web UI with run history, duration, failure logs per task |
| Backfill | Manual re-run of scripts | `dags backfill --start-date X --end-date Y` |
| Cross-pipeline dependencies | Impossible without custom hacks | ExternalTaskSensor, dataset-aware triggers |
| SLA alerting | Not built in | SLA callbacks per task |
| Parallelism | One job at a time unless you write parallel shell scripts | Parallel task execution with configurable concurrency |

---

**Q52 — What is backfilling and what pipeline property does it require?**

**Backfilling** is the process of running a pipeline for past dates — either because the pipeline was newly deployed, was broken for a period, or the business logic changed and historical data needs to be reprocessed.

```bash
# Airflow: process all days from Jan 1 to Jan 31 for the orders pipeline
airflow dags backfill orders_pipeline --start-date 2024-01-01 --end-date 2024-01-31
```

This triggers one DAG run per day, each with its own `execution_date`. The orchestrator runs them in parallel (up to the configured concurrency limit).

**Required property: Idempotency** (Concept 3).

Each run must produce the same result regardless of when it executes. A backfill run for `2024-01-15` executed on `2024-03-01` must produce the same output as if it had run on `2024-01-15` originally.

If a pipeline is not idempotent, backfilling creates duplicates — the historical data already in the table is doubled by the backfill run.

---

**Q53 — Gold table depends on Silver tables ready at different times**

**Problem:** `revenue_summary` is scheduled at 03:30. `orders_silver` is ready by 03:00 (fine). `returns_silver` is ready by 04:30 (not fine — 1 hour late). The job fails 50% of the time because it runs before `returns_silver` is ready.

**Fix: Use a sensor instead of a time-based schedule.**

Replace the fixed 03:30 schedule with a **data-availability sensor** that waits until both Silver tables signal completion:

```python
# Airflow example
wait_for_orders_silver = ExternalTaskSensor(
    task_id="wait_for_orders_silver",
    external_dag_id="orders_silver_pipeline",
    external_task_id="load_orders_silver",
    timeout=3600,  # give up after 1 hour
)

wait_for_returns_silver = ExternalTaskSensor(
    task_id="wait_for_returns_silver",
    external_dag_id="returns_silver_pipeline",
    external_task_id="load_returns_silver",
    timeout=3600,
)

build_revenue_summary = PythonOperator(...)

[wait_for_orders_silver, wait_for_returns_silver] >> build_revenue_summary
```

Now `build_revenue_summary` starts automatically as soon as **both** sensors succeed — whether that is 03:30 or 04:35. No arbitrary time delay, no race condition.

**Alternative (Airflow 2.4+ Dataset triggers):** Declare `revenue_summary` as depending on the `orders_silver` and `returns_silver` datasets. The DAG triggers automatically when both datasets are updated — no sensor polling needed.

---

**Q54 — Schedule-based vs. event-based/data-aware triggers**

**Schedule-based trigger:**
- DAG runs at a fixed cron expression: `0 2 * * *` (daily at 2 AM)
- Simple, predictable, easy to reason about
- Problem: if upstream data is late, the DAG runs on stale data. If upstream finishes early, the DAG waits unnecessarily.

**Event-based / data-aware trigger:**
- DAG runs when a condition is met: a file lands in S3, an upstream DAG completes, a Kafka message arrives, a dataset is marked as updated
- Decouples timing from clock time — runs as soon as data is ready
- Problem: harder to debug ("why hasn't it triggered?"), requires sensors or event infrastructure

**When to prefer schedule-based:**
- Upstream sources are very reliable and deliver on a consistent clock schedule
- Simplicity matters more than a few minutes of latency
- External consumers (reports, emails) expect delivery at a fixed time

**When to prefer event-based:**
- Upstream delivery time varies significantly (15 min on normal days, 2 hours on month-end)
- Cascading dependencies across many DAGs — a schedule offset of 30 min per stage accumulates across 5 stages into 2.5 hours of unnecessary waiting
- Processing must start as soon as data arrives (streaming or near-real-time requirements)

---

**Q55 — 200 DAGs, raw.orders fails: blast radius and recovery**

**How the orchestrator helps assess blast radius:**

Using lineage metadata or the orchestrator's dependency graph, query all DAGs that have `raw.orders` as an upstream dependency (directly or transitively). In a well-configured platform, this is a single query:

```sql
-- Find all DAGs that depend on raw.orders (direct or transitive)
SELECT dag_id FROM dag_dependencies WHERE upstream_dataset = 'raw.orders'
```

This returns the 40 affected DAGs immediately.

**Pattern to automatically pause downstream DAGs:**

**Option 1 — ExternalTaskSensor timeout:**
Each downstream DAG has a sensor that waits for `raw.orders` to complete. Configure the sensor with a `timeout` and `on_failure_callback`:
```python
wait_for_raw_orders = ExternalTaskSensor(
    task_id="wait_for_raw_orders",
    external_dag_id="raw_orders_loader",
    timeout=7200,  # 2 hours
    on_failure_callback=lambda ctx: pause_dag(ctx["dag"].dag_id),
    mode="reschedule",  # poll, do not block a worker slot
)
```
If `raw.orders` does not complete within 2 hours, each downstream DAG pauses itself and sends an alert.

**Option 2 — Dataset-aware scheduling (Airflow 2.4+):**
All 40 DAGs declare they depend on the `raw_orders` dataset. If the `raw_orders` producer DAG fails, it never marks the dataset as updated — the 40 consumers simply never trigger. They queue until the next successful producer run.

**Recovery:**
Once `raw.orders` is healthy and runs successfully, the dataset is marked updated, and all 40 downstream DAGs trigger automatically in dependency order. No manual intervention needed.
