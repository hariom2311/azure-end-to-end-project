# Day 5 — Interview Questions: Batch & Stream Processing

> 40 questions across all 5 concepts. Attempt your own answer first, then check `interview-solutions.md`.

---

## Concept 1: Batch Processing — Spark Architecture & Partitioning

**Q1 (Warm-up)**  
What are the three main components of Spark's architecture? What does each one do?

---

**Q2 (Warm-up)**  
What is lazy evaluation in Spark? Why does Spark not execute transformations immediately?

---

**Q3 (Conceptual)**  
What is a shuffle in Spark? Name three Spark operations that cause a shuffle and explain why.

---

**Q4 (Scenario)**  
A Spark job takes 45 minutes and the Spark UI shows one task in Stage 2 taking 40 minutes while the other 199 tasks finish in 30 seconds. What is the likely cause and how do you fix it?

---

**Q5 (Conceptual)**  
What is the difference between `repartition(N)` and `coalesce(N)` in Spark? When would you use each?

---

**Q6 (Scenario)**  
You are joining a 500 GB orders table with a 2 MB product lookup table. What join strategy should Spark use? How do you force it explicitly?

---

**Q7 (Tricky)**  
A Spark job reads 100 GB of data but only needs 3 columns out of 50. What optimisation does Spark automatically apply for Parquet files, and what operator configuration enables predicate pushdown?

---

**Q8 (Deep dive)**  
What is `spark.sql.shuffle.partitions` and what is its default value? When would you increase it and when would you decrease it? What problems arise if it is set too high for a small dataset?

---

## Concept 2: Stream Processing — Kafka, Consumers & Exactly-Once Semantics

**Q9 (Warm-up)**  
What is a Kafka topic partition? Why does partition count matter for consumer parallelism?

---

**Q10 (Warm-up)**  
What is a Kafka consumer offset? Who tracks it — the broker, the consumer, or ZooKeeper?

---

**Q11 (Conceptual)**  
What are the three delivery semantics in stream processing? Which is the default for most Kafka consumers, and what causes duplicates in at-least-once delivery?

---

**Q12 (Scenario)**  
A Kafka topic has 4 partitions and a consumer group has 6 consumers. How many consumers are actively reading? What happens to the other 2?

---

**Q13 (Scenario)**  
A payment processing consumer crashes after processing 1,000 events but before committing the offset. On restart, what events does it reprocess? What must be true about the consumer's processing logic to make this safe?

---

**Q14 (Tricky)**  
You need to ensure a Kafka consumer that writes to PostgreSQL achieves exactly-once semantics. The consumer cannot use Kafka transactions. How do you implement this at the database level?

---

**Q15 (Deep dive)**  
Design a Kafka topic for a global payment system processing 1 million events per second. How many partitions would you create? What is your partitioning key and why? What would happen to ordering guarantees if you changed the key?

---

## Concept 3: Windowing & Watermarks

**Q16 (Warm-up)**  
What is the difference between event time and processing time in stream processing? Give a real-world example where they differ significantly.

---

**Q17 (Warm-up)**  
What are the three window types in stream processing? Give a use case for each.

---

**Q18 (Conceptual)**  
What is a watermark in stream processing? How does it determine when a window is closed and results are emitted?

---

**Q19 (Scenario)**  
A tumbling window computes 5-minute purchase totals. An event with `event_time = 09:03` arrives at `processing_time = 09:14`. The watermark lag is 5 minutes. Is this event included in the [09:00–09:05] window? Show the calculation.

---

**Q20 (Scenario)**  
Your streaming job uses a 10-minute tumbling window to count active users. You set a watermark of 2 minutes. A mobile app goes offline for 20 minutes and then sends a burst of 50 events when connectivity is restored. What happens to these events? What alternative would you use to capture them?

---

**Q21 (Tricky)**  
Why do session windows behave differently from tumbling and sliding windows in terms of size and trigger time? What determines when a session window closes?

---

**Q22 (Deep dive)**  
A streaming pipeline emits windowed aggregates to a downstream table. The window produces a result for [14:00–15:00] at 15:05. At 15:12, a late event arrives for 14:47 that changes the aggregate. What are the two design choices for handling this, and what are the trade-offs of each?

---

## Concept 4: Lambda & Kappa Architecture

**Q23 (Warm-up)**  
What problem does Lambda architecture solve? What are its two processing layers?

---

**Q24 (Conceptual)**  
What is the main operational pain point of Lambda architecture? How does Kappa architecture address it?

---

**Q25 (Scenario)**  
An e-commerce platform needs: (A) real-time inventory updates within 1 second, and (B) monthly sales analysis over 3 years of history. Which architecture would you choose for this system, and why?

---

**Q26 (Tricky)**  
In Lambda architecture, the batch layer recomputes results every night from scratch. The speed layer produces approximate results for the last few hours. A customer calls to say their order status is incorrect on the website. The order was placed 2 hours ago. Which layer's data does the serving layer show — batch or speed? What happens at midnight when the batch run completes?

---

**Q27 (Scenario)**  
A team is currently running a Lambda architecture with a Spark batch job and a Flink streaming job doing the same aggregation logic. A new business requirement adds a new dimension to the aggregation. How many places must the code be changed? What is the risk of this dual-maintenance burden?

---

**Q28 (Deep dive)**  
Explain how Delta Lake (or Apache Iceberg) enables a "modern Kappa" approach. How does a single Delta table serve both a streaming write path and a batch read path simultaneously? What feature of Delta makes historical reprocessing possible without a separate batch layer?

---

## Concept 5: Pipeline Orchestration — Airflow

**Q29 (Warm-up)**  
What is an Airflow DAG? Why must it be acyclic (no cycles allowed)?

---

**Q30 (Warm-up)**  
What is the difference between `execution_date` (or `data_interval_start`) and the wall-clock time a DAG run starts? Why does this distinction matter for idempotent pipelines?

---

**Q31 (Conceptual)**  
What is an Airflow sensor? When would you use `mode="reschedule"` instead of `mode="poke"`?

---

**Q32 (Scenario)**  
A DAG is scheduled to run daily at 02:00 UTC. It is deployed and unpaused for the first time on January 20th but `start_date` is set to January 1st and `catchup=True`. What happens? How many DAG runs are created?

---

**Q33 (Scenario)**  
A Silver DAG has three parallel tasks (validate, transform_A, transform_B) that all feed into a final `merge_gold` task. The `validate` task fails. What happens to `transform_A` and `transform_B` by default? What trigger rule on `merge_gold` ensures it only runs if all three succeed?

---

**Q34 (Tricky)**  
A pipeline task uses `datetime.now()` to determine which data partition to process. Why is this a problem for backfill? How should it be written instead?

---

**Q35 (Deep dive)**  
Design the retry strategy for a Silver load task that calls an external API that occasionally rate-limits with HTTP 429. The task should: retry up to 5 times, wait longer between each retry, not retry on a non-transient error (HTTP 400), and alert if all retries are exhausted. What Airflow configuration achieves this?

---

## Mixed / Senior-Level Questions

**Q36**  
A data engineer says "we use batch processing because streaming is too complex." Under what conditions is this a correct technical decision, and under what conditions is it a symptom of skill gap rather than a genuine trade-off?

---

**Q37**  
A Kafka topic was created with 3 partitions. The team now needs to scale consumer parallelism to 10 consumers. What must happen first? What is the risk of increasing partition count on an existing topic?

---

**Q38**  
Your Spark job reads data from S3, does a `groupBy` on `customer_id`, and writes partitioned Parquet back to S3. After the job completes, you find 50,000 tiny Parquet files (average 50 KB each). What caused this, and write the Spark code fix.

---

**Q39**  
A streaming fraud detection pipeline uses a 1-minute tumbling window to count transactions per card. A card with more than 5 transactions in 1 minute triggers a fraud alert. A legitimate card sends 8 transactions from 00:59:58 to 01:00:03 — 3 transactions land in the [00:59–01:00] window and 5 in the [01:00–01:01] window. No alert fires. Is this a bug? How would you redesign the windowing to catch this pattern?

---

**Q40**  
Design a complete data pipeline for a streaming + batch system that ingests IoT sensor data from 100,000 devices:
- Each device sends a temperature reading every 10 seconds
- Requirement A: Alert within 30 seconds if a device reading exceeds 90°C
- Requirement B: Hourly average temperature report per device (accurate, handles late data)
- Requirement C: Monthly anomaly detection across all devices and all time

Specify: technology choices, window types, latency targets, how you handle late data, and how the three pipelines share infrastructure.
