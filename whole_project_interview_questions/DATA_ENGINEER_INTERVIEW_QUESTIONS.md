# Data Engineer Interview Questions & Answers
### VoltGrid AU — Azure EV Charging Intelligence Platform
> Targeted at 2–8 years of experience | Scenario-based & Real-world Questions

---

## Table of Contents

1. [Architecture & System Design](#1-architecture--system-design)
2. [Azure Data Factory — Ingestion & Orchestration](#2-azure-data-factory--ingestion--orchestration)
3. [Azure Databricks & PySpark](#3-azure-databricks--pyspark)
4. [Delta Lake & Medallion Architecture](#4-delta-lake--medallion-architecture)
5. [Silver Layer — Data Quality & Transformations](#5-silver-layer--data-quality--transformations)
6. [Gold Layer — Dimensional Modelling & Star Schema](#6-gold-layer--dimensional-modelling--star-schema)
7. [Streaming — Event Hubs & Structured Streaming](#7-streaming--event-hubs--structured-streaming)
8. [Access Control, Security & Compliance](#8-access-control-security--compliance)
9. [Unity Catalog & Data Governance](#9-unity-catalog--data-governance)
10. [Serving Layer — Synapse, Cosmos DB & Power BI](#10-serving-layer--synapse-cosmos-db--power-bi)
11. [CI/CD, Monitoring & Observability](#11-cicd-monitoring--observability)
12. [SCD, Slowly Changing Dimensions & History](#12-scd-slowly-changing-dimensions--history)
13. [Performance, Optimisation & Cost](#13-performance-optimisation--cost)
14. [Real-world Scenario Curveballs](#14-real-world-scenario-curveballs)

---

## 1. Architecture & System Design

---

### Q1. Walk me through the VoltGrid end-to-end architecture. Why did you choose a Medallion (Bronze → Silver → Gold) approach over a single-hop load?

**Answer:**

The VoltGrid platform ingests data from 28 heterogeneous sources — IoT telemetry streams (OCPP), REST APIs (payments, fleet, weather), CSV batch files (CRM, sessions), XML files (grid power, audit logs), and PDF invoices. A single-hop approach would mix raw, messy data with business-ready aggregations, making debugging and reprocessing impossible.

The Medallion layers each serve a distinct contract:

| Layer | Purpose | Format | Access |
|---|---|---|---|
| **Bronze** | Immutable raw replica — exact copy of source | Delta (append-only) | Data engineers only |
| **Silver** | Cleansed, validated, deduplicated — business truth | Delta (MERGE/upsert) | Data engineers + analysts |
| **Gold** | Aggregated, modelled, reporting-ready | Delta + marts | Analysts, BI, APIs |

**Why not single-hop?**
- If a transformation bug corrupts Silver, Bronze is the reprocess baseline — no data loss.
- Bronze preserves audit trail with `_ingestion_ts`, `_source_file`, `_pipeline_run_id` — mandatory for GDPR/Australian Privacy Act audits.
- Silver has a stable schema that Gold aggregations depend on. Decoupling means a new source or schema change at Bronze doesn't break Power BI dashboards.
- Different teams own different layers: ingestion team owns Bronze, data engineering owns Silver, analytics owns Gold.

**Real scenario from this project:** Charger fault streaming events arrive with duplicate `charger_id + event_ts` combinations (IoT devices resend on network retry). If we wrote directly to Gold, these duplicates would inflate fault counts. The Bronze layer stores raw duplicates, Silver deduplicates on `charger_id + event_ts + connector_id`, and Gold gets clean counts.

---

### Q2. You have 28 source systems — CSV, JSON APIs, XML, PDF, and real-time streams. How did you design a single ingestion architecture to handle all of them?

**Answer:**

The key insight is to decompose by **delivery mechanism**, not source format:

**Pattern 1 — Batch (CSV/XML files):**
- ADF Copy Activity with parameterised pipelines (Day 5 v4 design).
- A single master pipeline reads a `pipeline_metadata_config.json` from ADLS that defines each entity's endpoint, watermark column, and target path.
- Adding a new entity = adding one JSON config row. No pipeline code changes.
- High-watermark table tracks last loaded `updated_at` / `event_date` per entity in Azure SQL.

**Pattern 2 — REST APIs (Payment Gateway, Fleet, Weather):**
- ADF REST connector with retry + exponential backoff.
- Auth token fetched from Azure Key Vault at runtime — never hardcoded.
- Pagination handled via ADF linked dataset pagination rules.

**Pattern 3 — Streaming (IoT, fault events, RFID scans):**
- Physical chargers send OCPP telemetry → Azure IoT Hub → Azure Event Hubs (10 dedicated topics).
- Databricks Structured Streaming reads each topic with a checkpoint per topic for exactly-once delivery.
- 10-minute watermark handles late/out-of-order events before routing to quarantine.

**Pattern 4 — PDF Invoices:**
- Logic Apps detects email attachment arrival → saves PDF binary to Blob Storage.
- Azure AI Document Intelligence extracts structured JSON → written to `bronze/invoices/pdf_extracted_json/`.
- ADF scheduled copy picks up extracted JSONs to Bronze Delta.

All patterns write to Bronze ADLS with standard metadata columns (`_ingestion_ts`, `_source_file`, `_pipeline_run_id`, `_is_corrupt`) regardless of source type — downstream Silver jobs don't need to know the origin.

---

### Q3. Why did you choose ADLS Gen2 with Hierarchical Namespace (HNS) over regular Azure Blob Storage for the lakehouse?

**Answer:**

Regular Blob Storage uses a flat namespace — paths like `bronze/sessions/2025/01/01/` are just key prefixes, not real directories. This creates two critical problems for a lakehouse:

**Problem 1 — Performance at scale:**
Delta Lake and Spark need true directory operations (rename, list, delete a folder atomically). On flat Blob Storage, renaming a directory means copying every blob and deleting originals — O(n) operations that can take minutes. With HNS, a directory rename is an atomic O(1) metadata operation.

**Problem 2 — POSIX-style ACLs:**
HNS supports POSIX Access Control Lists at the directory level. This is how we implement the security zones:
- `bronze/crm/charge_cards_raw/` → restricted RBAC reader role (PCI data)
- `silver/` → data engineer role
- `gold/` → analyst role

Without HNS, you can only control access at the container level, not per folder.

**Additional benefits in this project:**
- Databricks Auto Loader uses file notifications via ADLS HNS + Event Grid — faster than listing all files.
- Unity Catalog External Locations require HNS-enabled ADLS for proper credential scoping.
- Lifecycle policies (Hot → Cool → Archive) apply at the directory level, letting us tier old Bronze data cheaply.

---

### Q4. The system processes both batch (daily) and real-time (seconds latency) data. How do you avoid one pipeline type blocking or starving the other?

**Answer:**

The key design is **dedicated compute per workload type**:

**Streaming workloads:**
- Always-on Databricks clusters, one per streaming topic.
- Topics: `iot-telemetry`, `charger-faults`, `connector-status`, `rfid-scan`, `live-payments`, `station-utilisation`, `weather-alerts`, `fleet-live-trip`, `session-events`.
- These clusters are never shared with batch jobs — streaming throughput is predictable.
- Checkpoints are stored in ADLS at `bronze/_checkpoints/<topic>/` — if a cluster restarts, it resumes exactly where it left off.

**Batch workloads:**
- Scheduled Databricks jobs on autoscaling clusters that spin up for the job and terminate.
- ADF orchestrates the full dependency chain: Batch Ingest → Bronze complete → trigger Silver job → Silver complete → trigger Gold job → trigger Cosmos DB sync.
- Batch jobs run in off-peak windows (e.g., 2:00 AM AEST) to avoid competing with streaming cluster resources.

**Why this matters practically:**
If a batch Gold aggregation job runs on the same cluster as the IoT telemetry stream, a long-running Spark shuffle on the batch job can starve the streaming micro-batch, causing consumer lag to spike. Monitoring shows consumer lag > 5 min triggers an alert — by isolating workloads, this alert stays silent.

---

## 2. Azure Data Factory — Ingestion & Orchestration

---

### Q5. Explain how the high-watermark incremental load pattern works in ADF for this project. What happens if a pipeline fails halfway through?

**Answer:**

**How watermark tracking works:**

A watermark is the value of the "last processed" record's timestamp column (e.g., `updated_at`, `plug_in_date`). At the start of every pipeline run:

1. ADF reads the current watermark from `azure-sql-watermarks/pipeline_watermark` table: `SELECT watermark_value FROM pipeline_watermarks WHERE entity_name = 'charging_sessions'`.
2. ADF executes a Copy Activity: `GET /api/charging_sessions?updated_after=<watermark_value>`.
3. On success, ADF writes the new watermark (max `updated_at` of the copied batch) back to the table.
4. Next run picks up from the new watermark.

**What happens on mid-pipeline failure:**

If the Copy Activity succeeds but the watermark write fails:
- Next run re-processes the same records (duplicates land in Bronze).
- This is **by design** — Bronze is append-only. Duplicates at Bronze are acceptable; Silver has the deduplication MERGE that makes it idempotent.

If the Copy Activity itself fails:
- Watermark is never updated — next run re-reads the same window.
- ADF retry policy (3 retries, exponential backoff) handles transient failures.

**Idempotency guarantee:** The Silver MERGE operation uses `MERGE INTO silver_table USING source ON (primary_key) WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`. Even if the same Bronze record arrives twice (from a failed watermark update), the Silver MERGE only keeps one version — exactly-once semantics at Silver regardless of at-least-once at Bronze.

**In this project:** `bronze/api/charging_sessions/` might have two identical `session_id` rows after a failed run. When Silver runs the MERGE keyed on `session_id`, it upserts — no duplicate session in Silver.

---

### Q6. You designed a metadata-driven pipeline that handles 17 entities with a single pipeline pair. Walk me through that design and explain why this is better than 17 individual pipelines.

**Answer:**

**The metadata config approach:**

A `pipeline_metadata_config.json` file stored in `bronze/config/` defines each entity:

```json
[
  {
    "entity_name": "payments",
    "api_endpoint": "/api/payments",
    "watermark_column": "updated_at",
    "bronze_path": "bronze/api/payments/",
    "load_type": "incremental"
  },
  {
    "entity_name": "customers",
    "api_endpoint": "/api/customers",
    "watermark_column": "customer_created_at",
    "bronze_path": "bronze/api/customers/",
    "load_type": "full"
  }
  ...17 entities total
]
```

**Pipeline architecture:**

- **Master pipeline (`pl_bronze_api_master_v4`):** Reads the config JSON using a Lookup Activity. Passes each config row to a ForEach Activity with parallel execution enabled (`batchCount: 17` — all 17 run simultaneously).
- **Child pipeline (`pl_bronze_api_ingest_v4`):** Receives parameters (`entity_name`, `api_endpoint`, `watermark_column`, `bronze_path`). Performs: auth → read watermark → Copy Activity → write watermark → audit log.

**Why better than 17 individual pipelines:**

| Metric | 17 individual pipelines | 1 metadata-driven pair |
|---|---|---|
| Adding entity #18 | New pipeline + 2 datasets + testing | Add 1 JSON row |
| Fixing an auth bug | Fix in 17 places | Fix in 1 child pipeline |
| Parallel execution | Manual coordination | ForEach `batchCount` |
| Monitoring | 17 separate run histories | 1 master run + child details |
| Watermark tracking | 17 separate state stores | 1 parameterised CSV/table |

**Failure isolation:** Because the master uses a ForEach with child pipelines, one entity failing (e.g., fleet API is down) does not block the other 16. The master marks the failed entity as `FAILED` in the audit table, and the others complete.

---

### Q7. An ADF pipeline that reads from the Payment Gateway REST API has been failing intermittently with HTTP 429 (Too Many Requests). How do you handle this without data loss?

**Answer:**

HTTP 429 means the API is rate-limiting our calls. The solution has three layers:

**Layer 1 — ADF built-in retry:**
- In the Copy Activity settings, configure `Retry: 3`, `Retry interval: 60 seconds` (with exponential backoff: 60s, 120s, 240s).
- For 429 specifically, the Retry-After header tells us how long to wait — ADF respects this if the linked service is configured for it.

**Layer 2 — Pagination and batch sizing:**
- Instead of fetching all payments in one request, paginate: `GET /api/payments?page=1&page_size=500`.
- Smaller requests are less likely to hit rate limits.
- ADF Pagination rules in the REST linked service handle this automatically.

**Layer 3 — Pipeline-level watermark:**
- Because of high-watermark tracking, a partial failure only means the current page wasn't saved.
- On retry, ADF resumes from the last committed watermark — it doesn't re-read all historical data.

**Monitoring:**
- Set up an Azure Monitor alert on ADF pipeline run status = Failed for `pl_bronze_api_ingest_v4`.
- Log Analytics query surfaces the error type (429 vs 500 vs network) for faster diagnosis.

**In practice for VoltGrid:** The Payment Gateway is the most critical source — billing and revenue dashboards depend on it. We set a higher retry count (5) and an alert SLA: if payments data is not in Bronze by 3:00 AM, an ops team member is paged.

---

## 3. Azure Databricks & PySpark

---

### Q8. In the Silver layer, you process 28 different entity types. How did you design the Silver transformation notebooks to avoid maintaining 28 separate notebooks?

**Answer:**

The three-notebook progression (v1 → v2 → v3) built towards a single production notebook that handles all 17+ API entities:

**v1 — Explicit, single entity (payments):**
```python
# Every step written out: read Bronze JSON, cast types,
# rename columns, validate ranges, write Delta
```
Purpose: understand each transformation step in isolation.

**v2 — ForEach loop over entity config list:**
```python
ENTITIES = [
  {"name": "payments", "bronze_path": "...", "silver_path": "...", "dedup_key": "payment_id"},
  {"name": "customers", "bronze_path": "...", "silver_path": "...", "dedup_key": "customer_id"},
  # ...
]
for entity in ENTITIES:
    df = spark.read.json(entity["bronze_path"])
    df = apply_common_transforms(df)
    df.write.format("delta").mode("overwrite").save(entity["silver_path"])
```

**v3 — Production with incremental Delta MERGE:**
```python
# Widget parameters from ADF: load_type (full/incremental), entity_name
# Helper functions: apply_common_transforms(), validate_schema(), quarantine_bad_records()
# Delta MERGE for idempotent upserts
deltaTable.alias("target").merge(
    df.alias("source"),
    f"target.{entity['dedup_key']} = source.{entity['dedup_key']}"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

**Common transform functions applied to all entities:**
- `add_ingestion_metadata()` — adds `_silver_ts`, `_source_layer`
- `normalise_timestamps()` — converts all timestamps to UTC (Rule G)
- `quarantine_nulls()` — checks mandatory fields, routes bad rows to `silver/quarantine/`
- `flatten_json()` — unpacks nested JSON arrays/structs

**Why this is better:** One bug fix in `normalise_timestamps()` propagates to all 17 entities simultaneously. The entity config list is the only place entity-specific logic lives (dedup key, bronze path, silver path).

---

### Q9. The IoT charger sends a temperature reading of 82°C. Walk me through what happens to that record from ingestion to alerting — every system it passes through.

**Answer:**

This traces the full pipeline for a critical real-time event:

**Step 1 — Device layer:**
- Physical charger sends OCPP telemetry JSON over MQTT/WebSocket to Azure IoT Hub.
- Payload: `{"charger_id": "CHG-0042", "connector_id": 1, "temperature_c": 82.0, "voltage": 415, "event_ts": "2025-06-15T09:14:33Z"}`.

**Step 2 — Azure IoT Hub → Event Hubs:**
- IoT Hub forwards the message to `evh-ev-intelligence-dev`, topic `iot-telemetry` (4 partitions).
- Message is partitioned by `charger_id` — events from the same charger always hit the same partition, preserving order.

**Step 3 — Databricks Structured Streaming (Bronze write):**
- Always-on streaming job reads from `iot-telemetry` topic.
- Adds metadata: `_ingestion_ts = now()`, `_source_file = "iot-telemetry"`, `_pipeline_run_id = <job-run-id>`.
- Writes to `bronze/iot/charger_telemetry_raw/event_date=2025-06-15/` as Delta append.
- Checkpoint updated in `bronze/_checkpoints/iot-telemetry/`.

**Step 4 — Silver transformation (streaming):**
- A separate streaming job reads Bronze charger_telemetry.
- Deduplication (Rule A): checks `charger_id + event_ts + connector_id` — no duplicate, record passes.
- Watermark (Rule B): event is within 10-minute window — record passes.
- Fault code mapping (Rule C): `fault_code = "OVERTEMP"` → `fault_description = "Overheating Risk"`.
- Temperature flag (Rule E): `temperature_c = 82.0 > 75.0` → `overheating_flag = TRUE`.
- Record written to `silver/sl_iot_charger_telemetry/` via Delta MERGE.

**Step 5 — Databricks alert rule:**
- Streaming job has a threshold check: `WHERE overheating_flag = TRUE`.
- Alert condition satisfied → fires Azure Functions trigger via HTTP webhook.

**Step 6 — Azure Functions:**
- Function receives event payload: `charger_id = CHG-0042`, `station_id`, `temperature = 82`.
- Sends CRITICAL alert via three channels:
  - Email: Azure Communication Services → maintenance team
  - SMS: Twilio → on-call engineer
  - Teams: webhook POST to `#ops-alerts` channel

**Step 7 — FactMaintenance row created:**
- Gold job sees `overheating_flag = TRUE` in Silver → creates maintenance event row in `FactMaintenance` with `root_cause = "Overheating"`, `fault_description = "Overheating Risk"`, `charger_key`, `station_key`, `time_key`.
- `mart_predictive_maintenance_risk` increments `overheating_count` for CHG-0042.

**Step 8 — Power BI dashboard:**
- Predictive Maintenance dashboard refreshes — CHG-0042 shows elevated risk score, red status indicator.

**Total latency from device to alert: ~30–90 seconds** (IoT Hub buffering + Databricks micro-batch cadence + Functions cold start).

---

### Q10. Explain how Delta MERGE (upsert) works and why you used it for the Silver layer instead of just overwriting the table every time.

**Answer:**

**Delta MERGE syntax:**
```python
from delta.tables import DeltaTable

silver_table = DeltaTable.forPath(spark, silver_path)
silver_table.alias("target").merge(
    source_df.alias("source"),
    "target.session_id = source.session_id"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()
```

**What MERGE does:**
- For each row in `source_df`, checks if a row with the same `session_id` exists in the Silver Delta table.
- **Matched:** Update all columns (handles corrected records from Bronze).
- **Not matched:** Insert as new row.

**Why MERGE over overwrite:**

| Scenario | Overwrite (full reload) | MERGE (upsert) |
|---|---|---|
| Bronze has 1M rows, only 500 new today | Rewrites 1M rows | Processes 500 rows |
| A Bronze record was corrected | Old Silver row replaced on next full reload | MERGE updates the specific row immediately |
| Silver table has 30 days history, Bronze only keeps 7 days | History lost on overwrite | History preserved |
| Pipeline fails halfway | Partial write leaves corrupt table | MERGE is atomic — either all or nothing |

**Idempotency:** If the same Bronze batch is processed twice (due to a retry), MERGE updates existing Silver rows to the same values — net result identical to running once. Overwrite would cause duplicate rows if run twice without TRUNCATE first.

**In this project:** `sl_tariffs_scd2` specifically cannot use overwrite — SCD2 requires keeping historical rows with `is_current = FALSE` alongside the new `is_current = TRUE` row. MERGE handles this with conditional update: `WHEN MATCHED AND source.rate_per_kwh != target.rate_per_kwh THEN UPDATE SET effective_to = source.effective_from, is_current = FALSE`.

---

## 4. Delta Lake & Medallion Architecture

---

### Q11. What is Delta Lake's time travel feature, and can you give a real scenario from this project where you would use it?

**Answer:**

**What time travel is:**

Delta Lake keeps a transaction log (`_delta_log/`) that records every write operation (commit, schema change, delete). You can query any previous version of a table:

```python
# Query table as it was 7 days ago
df = spark.read.format("delta").option("timestampAsOf", "2025-06-01").load(silver_path)

# Query specific version number
df = spark.read.format("delta").option("versionAsOf", 42).load(silver_path)

# Show history
spark.sql("DESCRIBE HISTORY delta.`/path/to/silver/sl_charging_sessions`")
```

**Real scenarios in VoltGrid:**

**Scenario 1 — Silver transformation bug corrupted revenue data:**
A code change in the Silver GST split logic (Rule J) was deployed with a bug: it divided by 1.1 instead of multiplying. All sessions written that night have wrong `net_amount`. With time travel, we restore the previous version:
```python
# Restore Silver sessions to pre-bug version
previous_df = spark.read.format("delta").option("versionAsOf", 99).load(silver_sessions_path)
previous_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_sessions_path)
```

**Scenario 2 — Late-arriving billing dispute:**
A franchise partner disputes their June invoice claiming sessions were counted incorrectly. Finance needs to see `FactChargingSession` exactly as it was on June 30th — not the current state (which may have been corrected since). Time travel lets us query the exact state of the Gold table at any past timestamp.

**Scenario 3 — Audit trail for Australian Privacy Act compliance:**
A regulator asks: "What PII data did your system hold for customer C-10042 on March 15th?" Time travel on `sl_customers` shows the exact record state at that date — email hash, loyalty tier, last 4 of card — without needing a separate audit database.

**Retention:** Bronze Delta tables are configured with `delta.logRetentionDuration = "interval 90 days"` to support 90-day replays.

---

### Q12. What is schema evolution and how does Bronze handle it when the Payment API suddenly adds a new field?

**Answer:**

**The problem:**

Payment API returns:
```json
{"payment_id": "P001", "amount": 55.89, "gateway": "stripe", "status": "SUCCESS"}
```

After an API update, it returns:
```json
{"payment_id": "P001", "amount": 55.89, "gateway": "stripe", "status": "SUCCESS", "wallet_provider": "apple_pay"}
```

Without schema evolution handling, the Databricks job fails with `AnalysisException: column 'wallet_provider' not found in schema`.

**How Auto Loader handles it at Bronze:**

```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load(bronze_path)
```

`schemaEvolutionMode = "addNewColumns"`:
- Auto Loader detects the new `wallet_provider` column.
- Automatically updates the inferred schema.
- New column is added to the Bronze Delta table with `NULL` for all existing rows.
- Processing continues without manual intervention.

**What you must NOT do:** `schemaEvolutionMode = "failOnNewColumns"` is the safe default for production Silver and Gold — you don't want unreviewed schema changes silently propagating to business dashboards.

**Downstream handling:** When Silver picks up the new `wallet_provider` column from Bronze:
- The Silver job's entity config defines explicit column mapping — `wallet_provider` is unknown, so it goes into a `_extra_fields` struct or gets quarantined until the Silver schema is intentionally updated.
- This separation means a Bronze schema change never silently breaks a Gold dashboard.

---

## 5. Silver Layer — Data Quality & Transformations

---

### Q13. You implemented a quarantine pattern for bad records. How does it work end-to-end, and how do you alert the data team when quarantine volume spikes?

**Answer:**

**Quarantine routing logic:**

```python
from pyspark.sql.functions import col, lit, when

# Apply quality checks — returns tuple: (clean_df, quarantine_df)
def validate_and_split(df, entity_name):
    # Check 1: null mandatory fields
    null_mask = col("charger_id").isNull() | col("session_id").isNull()
    # Check 2: range violations
    range_mask = (col("energy_kwh") < 0) | (col("energy_kwh") > 500)
    # Check 3: referential integrity
    # (join to charger_master — if charger_id not found, flag it)

    bad_mask = null_mask | range_mask  # combine all quality masks

    clean_df = df.filter(~bad_mask)
    quarantine_df = df.filter(bad_mask).withColumn(
        "rejection_reason",
        when(null_mask, "NULL_MANDATORY_FIELD")
        .when(range_mask, "OUT_OF_RANGE")
        .otherwise("REFERENTIAL_INTEGRITY")
    ).withColumn("entity_name", lit(entity_name)) \
     .withColumn("quarantine_ts", current_timestamp())

    return clean_df, quarantine_df

clean_df, quarantine_df = validate_and_split(raw_df, "charging_sessions")
quarantine_df.write.format("delta").mode("append").save("silver/quarantine/")
clean_df  # proceed to Silver MERGE
```

**DQ Metrics table:**

Every job run writes a summary to `gold/data_quality_audit/`:
```
entity_name | run_date    | total_rows | clean_rows | quarantine_rows | quarantine_pct
sessions    | 2025-06-15  | 10000      | 9980       | 20              | 0.20%
```

**Alerting on quarantine spike:**

Azure Monitor query (Log Analytics KQL):
```kql
customEvents
| where name == "silver_dq_audit"
| where toreal(customDimensions.quarantine_pct) > 5.0  // alert if > 5% bad
| project timestamp, entity_name, quarantine_pct
```

Alert: if `quarantine_pct > 5%` for any entity → email + Teams notification to data engineering team.

**In practice:** An alert fired when the fleet API changed its `distance_km` field from a float to a string (`"125.4"` instead of `125.4`). The range check flagged all fleet records as quarantined. The team caught it within 30 minutes and updated the Silver cast logic.

---

### Q14. Explain how you implemented PCI DSS-compliant masking for charge card data. What exactly is masked, why, and where does masking happen?

**Answer:**

**What gets masked and why:**

| Field | Raw Bronze value | Silver/Gold value | Reason |
|---|---|---|---|
| `card_number` | `4242424242424242` | `************4242` | PCI DSS: full PAN must never be stored in non-PCI-scoped systems |
| `cvv` | `123` | discarded (`cvv_masked = "***"`) | PCI DSS: CVV must never be stored at all, even encrypted |
| `card_expiry` | `12/27` | kept as-is | Expiry alone is not sensitive; needed for card validation |
| `card_token` | UUID from gateway | kept as-is | Stable identifier for joins — token replaces PAN |

**Where masking happens — Bronze → Silver transition:**

```python
from pyspark.sql.functions import regexp_replace, lit, col

df = df.withColumn(
    "card_number_masked",
    regexp_replace(col("card_number"), r"^\d{12}", "************")
).withColumn(
    "cvv_masked",
    lit("***")
).drop("card_number", "cvv")  # remove raw values from Silver
```

**Why Bronze keeps the raw card number:**

Bronze is a restricted zone — `bronze/crm/charge_cards_raw/` has a dedicated RBAC role that only 2 people have. This is acceptable because:
1. Bronze is the source replication layer — changing it would break the immutability guarantee.
2. The raw data is needed for forensic audit if a payment dispute arises.
3. Access is logged at the Azure Monitor level — all reads of the restricted Bronze zone are auditable.

**Key principle:** The masking happens at the Bronze → Silver boundary, not at query time. This means Silver and Gold tables never contain raw PANs or CVVs — even if a data engineer runs a direct Spark query on Silver, they cannot reconstruct the card number.

**`card_token` as join key:** All downstream joins (Silver → Gold, FactPayments → DimChargeCard) use `card_token`, not `card_number`. This is the standard tokenisation pattern — the payment gateway holds the token↔PAN mapping, and VoltGrid's systems never need the PAN.

---

### Q15. Explain SCD Type 2 implementation for tariff rates. A tariff rate changed from $0.35/kWh to $0.42/kWh on July 1st. How do you ensure charging sessions from June use the old rate and sessions from July use the new rate?

**Answer:**

**SCD2 table structure in `sl_tariffs_scd2`:**

```
tariff_id | rate_per_kwh | peak_offpeak | effective_from | effective_to  | is_current
T001      | 0.35         | PEAK         | 2025-01-01     | 2025-06-30    | FALSE
T001      | 0.42         | PEAK         | 2025-07-01     | 9999-12-31    | TRUE
```

**When the tariff change arrives in Bronze:**

Silver transformation detects the rate change and:
1. Finds the current active row for `T001` (`is_current = TRUE`).
2. Closes it: `effective_to = 2025-06-30`, `is_current = FALSE`.
3. Inserts the new row: `rate_per_kwh = 0.42`, `effective_from = 2025-07-01`, `effective_to = 9999-12-31`, `is_current = TRUE`.

**PySpark MERGE for SCD2:**

```python
deltaTable.alias("target").merge(
    new_tariff_df.alias("source"),
    "target.tariff_id = source.tariff_id AND target.is_current = true"
).whenMatchedUpdate(
    condition="target.rate_per_kwh != source.rate_per_kwh",
    set={"effective_to": "source.effective_from - interval 1 day", "is_current": "false"}
).whenNotMatchedInsertAll(
).execute()

# Insert the new version separately
new_row_df.write.format("delta").mode("append").save(silver_tariff_path)
```

**Gold layer join — using effective dates:**

```python
# FactChargingSession joins DimTariff using the session date
fact_df.join(
    dim_tariff_df,
    (fact_df.tariff_id == dim_tariff_df.tariff_id) &
    (fact_df.session_date >= dim_tariff_df.effective_from) &
    (fact_df.session_date <= dim_tariff_df.effective_to)
)
```

A June 25th session gets `rate_per_kwh = 0.35`. A July 5th session gets `rate_per_kwh = 0.42`. Revenue calculations are historically accurate.

**Why `9999-12-31` for open-ended rows:**
- Simplifies the join condition — you don't need `OR effective_to IS NULL`.
- Standard SCD2 pattern across the industry.

---

## 6. Gold Layer — Dimensional Modelling & Star Schema

---

### Q16. Explain the star schema design for FactChargingSession. Why did you choose a star schema over a flat denormalised table for Power BI?

**Answer:**

**FactChargingSession structure:**

```
FactChargingSession (grain: one row per charging session)
├── session_key (surrogate PK)
├── customer_key (FK → DimCustomer)
├── station_key  (FK → DimStation)
├── charger_key  (FK → DimCharger)
├── vehicle_key  (FK → DimVehicle)
├── tariff_key   (FK → DimTariff — SCD2 snapshot at session time)
├── time_key     (FK → DimTime)
├── energy_kwh
├── duration_min
├── session_status
├── expected_amount  (energy_kwh × tariff_rate)
├── billed_amount    (from FactPayments)
├── reconciliation_status  (MATCH / MISMATCH)
└── difference_amount
```

**Why star schema over flat denormalised:**

**1. Query performance in Power BI:**
Power BI uses a columnar in-memory engine (VertiPaq). Star schemas with narrow fact tables compress extremely well — the `session_key`, `customer_key` integer foreign keys are tiny vs storing full customer name/address in every row. A flat table with 50K sessions × 30 customer columns = 1.5M values. Star schema: 50K FK integers in the fact + 5K rows in DimCustomer.

**2. Single point of truth for dimensions:**
If a customer's loyalty tier changes, you update DimCustomer (one row). In a flat table, you'd need to update 500 session rows for that customer — expensive and error-prone.

**3. Flexible drill-down without extra joins:**
Power BI's auto-relationship detection builds the drill-down hierarchy automatically from star schema FKs:
`Australia → NSW → Sydney → Station-101 → Charger-42 → Session-level`
This is the exact geo hierarchy needed for all 14 dashboards.

**4. SCD2 correctness:**
The `tariff_key` in FactChargingSession points to the specific DimTariff version effective at session time. If you denormalise, you'd store the rate as a column in the fact — correct, but you lose the ability to query "how many sessions used the old tariff vs the new tariff" with a simple DimTariff filter.

**Trade-off of star schema:** Queries require joins. Mitigation: pre-aggregated marts (`mart_revenue_by_geo_month`) pre-join and aggregate for the most common Power BI queries — these run in Import mode, no join cost at report time.

---

### Q17. How did you calculate the billing reconciliation metric, and what does a MISMATCH tell the business?

**Answer:**

**Reconciliation formula:**

```python
# Applied at FactChargingSession grain
fact_df = fact_df.withColumn(
    "expected_amount",
    col("energy_kwh") * col("tariff_rate_per_kwh")  # from DimTariff join
).withColumn(
    "reconciliation_status",
    when(
        abs(col("billed_amount") - col("expected_amount")) <= 0.01,  # 1-cent tolerance
        lit("MATCH")
    ).otherwise(lit("MISMATCH"))
).withColumn(
    "difference_amount",
    col("billed_amount") - col("expected_amount")
)
```

**What a MISMATCH means:**

Three root causes possible:

| Cause | Example | `difference_amount` |
|---|---|---|
| Wrong tariff rate applied | Session used peak rate but was off-peak | Positive (customer overcharged) |
| Energy meter miscalibrated | Meter reported 15.2 kWh, actual was 14.8 kWh | Small negative |
| Payment gateway rounding | Gateway rounds to 2 decimal places, VoltGrid uses 4 | Small (<$0.01) |

**Business impact:**

The `mart_billing_reconciliation` mart aggregates MISMATCHes by station and franchise partner. If Station-101 in Sydney has 12% MISMATCH rate (industry threshold is <1%), it indicates either:
- A meter calibration issue → maintenance alert
- A tariff configuration error → operations team fix
- A payment gateway integration bug → tech team fix

The Finance dashboard shows: `Total revenue leakage = SUM(difference_amount WHERE reconciliation_status = 'MISMATCH')`. For VoltGrid with 50K sessions, even $0.50 average leakage = $25,000 in unrecovered revenue — this was one of the core business problems the platform solved.

---

## 7. Streaming — Event Hubs & Structured Streaming

---

### Q18. Explain exactly-once semantics in Databricks Structured Streaming. How do you achieve it with Azure Event Hubs?

**Answer:**

**The problem:** Event Hubs is an at-least-once delivery system. If a streaming job crashes after processing a micro-batch but before committing the offset, the same events are re-delivered on restart. Without exactly-once guarantees, this creates duplicate rows in Bronze.

**How exactly-once is achieved:**

**Step 1 — Checkpoint-based offset tracking:**
```python
df = spark.readStream \
    .format("eventhubs") \
    .options(**eh_conf) \
    .load()

df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://bronze@evdatalakedev.dfs.core.windows.net/_checkpoints/iot-telemetry/") \
    .trigger(processingTime="30 seconds") \
    .start(bronze_path)
```

The checkpoint directory stores the last committed Event Hub partition offset. On restart, Structured Streaming reads from exactly the last committed offset — no events are skipped and no events are replayed past the checkpoint.

**Step 2 — Idempotent Delta writes:**
Delta Lake's `ACID` transactions ensure that if a micro-batch write fails after partial commit, the transaction is rolled back. When the job restarts, it re-processes the same micro-batch and Delta writes it atomically — no partial rows.

**Step 3 — One checkpoint per topic:**
We have 10 Event Hub topics → 10 separate checkpoint directories (`_checkpoints/iot-telemetry/`, `_checkpoints/charger-faults/`, etc.). If the charger-faults job crashes, it only affects that topic — iot-telemetry continues writing without interruption.

**Monitoring:** Consumer lag > 5 minutes triggers an Azure Monitor alert. This indicates either the streaming job has crashed or the Event Hub topic is receiving more events than the job can process (scaling needed).

---

### Q19. A charger sends 5 identical telemetry events within 1 second (IoT device retry storm). How does the system handle this without counting the fault 5 times in the Maintenance dashboard?

**Answer:**

This is a multi-layer problem — each layer handles one aspect:

**Layer 1 — Bronze (append all 5):**
All 5 identical events land in `bronze/iot/charger_telemetry_raw/`. Bronze is append-only — no deduplication here. This is correct: we preserve the raw evidence of the retry storm, which is useful for device debugging.

**Layer 2 — Silver deduplication (Rule A):**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

# Dedup key: charger_id + event_ts + connector_id
window_spec = Window.partitionBy("charger_id", "event_ts", "connector_id") \
                    .orderBy(desc("_ingestion_ts"))

df = df.withColumn("row_num", row_number().over(window_spec)) \
       .filter(col("row_num") == 1) \
       .drop("row_num")
```
5 events with same `charger_id + event_ts + connector_id` → Silver keeps 1 (the latest `_ingestion_ts`).

**Layer 3 — Streaming watermark (Rule B):**
If any of the 5 events is delayed by more than 10 minutes from the watermark boundary, it goes to quarantine. But for a retry storm (same-second events), they all fall within the watermark — all 5 pass to dedup, where 4 are removed.

**Layer 4 — FactMaintenance (Gold):**
FactMaintenance creates one row per maintenance event, keyed on `charger_id + fault_ts`. Even if somehow 2 duplicate events slipped through Silver, the Gold MERGE on `charger_id + fault_ts` ensures only 1 maintenance row per fault.

**Result:** The Predictive Maintenance dashboard counts 1 fault for CHG-0042, not 5. The Bronze table shows 5 rows (for audit purposes). This is the correct separation of concerns.

---

## 8. Access Control, Security & Compliance

---

### Q20. A new data analyst joins the team and needs access to Gold layer data for Power BI dashboards but must NOT see raw customer PII or charge card numbers. Walk me through how you set this up end-to-end.

**Answer:**

**Access granted — what the analyst CAN see:**

| Layer | Access | Reason |
|---|---|---|
| Bronze | None | Raw data, PII present |
| Silver | None | `sl_customers` has email hashes; `sl_charge_cards` has last4 — still restricted |
| Gold | Read | PII fully masked; card numbers are last4 only |
| Power BI | Report Viewer | RLS filters by their assigned state/franchise |

**Step-by-step setup:**

**Step 1 — Azure Entra ID group:**
Create AAD group `grp-voltgrid-analysts`. Add the new analyst's account.

**Step 2 — ADLS RBAC on Gold container only:**
```
Azure Portal → evdatalakedev → Access Control (IAM) → Add role assignment
Role: Storage Blob Data Reader
Scope: gold container (not the entire storage account)
Assignee: grp-voltgrid-analysts
```
Bronze and Silver containers are not in this assignment — the group gets no access to them.

**Step 3 — Unity Catalog permissions:**
```sql
GRANT SELECT ON SCHEMA dbw_ev_intelligence_dev.gold TO `grp-voltgrid-analysts`;
-- Do NOT grant bronze or silver schema access
```

**Step 4 — Azure Synapse RLS:**
```sql
-- In Synapse, add a row-level security predicate
CREATE FUNCTION dbo.fn_analyst_filter(@state VARCHAR(3))
RETURNS TABLE
AS RETURN
SELECT 1 AS access
WHERE @state = (SELECT assigned_state FROM analyst_permissions WHERE user_name = USER_NAME())
   OR USER_NAME() IN (SELECT user_name FROM executive_group);

ALTER TABLE FactChargingSession
ADD SECURITY POLICY analyst_rls
ADD FILTER PREDICATE dbo.fn_analyst_filter(state_code) ON FactChargingSession;
```

**Step 5 — Power BI RLS:**
In the Power BI data model:
- Create role `StateAnalyst` with filter: `DimState[state_name] = USERPRINCIPALNAME()`
- Assign the analyst's email to the `StateAnalyst` role in Power BI workspace.
- They only see data for their state in all 14 dashboards.

**Step 6 — Audit logging:**
Azure Monitor diagnostic settings on the `gold` ADLS container log all read operations. If the analyst somehow attempts to read `bronze/crm/charge_cards_raw/`, the access is denied (no RBAC) and logged — security team alert fires.

---

### Q21. Explain the difference between a Service Principal, a Managed Identity, and an Access Connector in the context of this project. When would you use each?

**Answer:**

**Service Principal (`sp-ev-intelligence-dev`):**

What it is: An application identity in Azure Entra ID. It has a `client_id` + `client_secret` (or certificate) pair. You manually create it, and you are responsible for rotating the secret.

When used in this project:
- Databricks notebooks that need to read/write ADLS directly via `spark.conf.set("fs.azure.account.oauth2.client.secret...", secret)`.
- The secret is stored in Key Vault, never in notebook code.

Risk: If `client_secret` leaks (e.g., accidentally logged), the SP can be used by anyone until the secret is rotated. **Secret must be rotated every 90 days.**

**Managed Identity (`mi-ev-intelligence-dev`):**

What it is: An identity whose credentials Azure manages automatically — no client secret, no rotation needed. Bound to a specific Azure resource (e.g., an ADF instance or VM).

When used in this project:
- ADF pipelines authenticate to ADLS and Key Vault using the ADF Managed Identity.
- No human ever sees or manages a password.

Why better than SP for ADF: ADF is a long-running managed service — you don't want to rotate an SP secret and break all ADF pipelines. Managed Identity rotates automatically, transparently.

**Access Connector (`ac-ev-intelligence-dev`):**

What it is: A dedicated Azure resource specifically designed to connect Databricks Unity Catalog to external storage. It has a System-Assigned Managed Identity.

When used in this project:
- Unity Catalog's Storage Credentials reference this Access Connector.
- When a Databricks cluster accesses `/Volumes/dbw_ev_intelligence_dev/bronze/bronze_volume/`, Unity Catalog uses the Access Connector's identity to authenticate to ADLS.
- Notebooks never need to configure SAS tokens or SP credentials for Unity Catalog paths.

**Decision rule:**
- Human user needing portal/CLI access → AAD user account.
- Databricks notebooks needing ADLS access → Service Principal (with Key Vault-backed secret).
- ADF pipelines needing ADLS/Key Vault access → Managed Identity (no secret management).
- Unity Catalog volumes needing ADLS access → Access Connector.

---

### Q22. What is the Australian Privacy Act 1988 requirement and how did the data platform comply with it?

**Answer:**

**Key APP (Australian Privacy Principles) requirements relevant to this platform:**

**APP 11 — Security of personal information:**
Personal information must be protected from misuse, interference, loss, and unauthorised access.

Compliance in VoltGrid:
- Customer email hashed with SHA-256 in Silver/Gold — not stored in plain text.
- PAN (card number) masked to last 4 digits — full PAN never leaves the restricted Bronze zone.
- CVV discarded at Bronze → Silver transition — never stored in the lakehouse.
- `bronze/crm/charge_cards_raw/` isolated with RBAC restricted reader role.
- All data encrypted at rest (ADLS Gen2 Microsoft-managed encryption) and in transit (HTTPS/TLS 1.2+).

**APP 12 — Access to personal information:**
An individual can request access to the personal information an organisation holds about them.

Compliance: With Delta Lake time travel and the structured Silver schema, we can query: "What data do we hold about customer C-10042 at any point in time?" — answerable in a single SQL query without manual file scanning.

**APP 3 — Collection of solicited personal information:**
Only collect what is necessary.

Compliance: CVV is collected at payment point but immediately discarded — never stored in the data platform. The `card_token` (from payment gateway) is sufficient for all downstream analytics.

**Audit trail:**
- `_ingestion_ts`, `_source_file`, `_pipeline_run_id` on every Bronze record — satisfies "when was this collected and from where."
- Azure Monitor logs all reads of restricted zones.
- The `gold/data_quality_audit/` table logs every DQ check run — demonstrates active data governance to regulators.

---

## 9. Unity Catalog & Data Governance

---

### Q23. Explain the 4-level namespace in Databricks Unity Catalog and how it's used in this project.

**Answer:**

Unity Catalog uses:
```
metastore . catalog . schema . table
```

**In VoltGrid:**
```
dbw_ev_intelligence_dev . dbw_ev_intelligence_dev . bronze . charger_telemetry
        (metastore)              (catalog)          (schema)    (table)
```

**Why 4 levels:**

Before Unity Catalog, Databricks used Hive metastore per workspace: `schema.table`. Tables in the `dev` workspace were invisible to the `prod` workspace.

Unity Catalog sits above all workspaces. The 4-level name is globally unique within a Databricks account — two workspaces in the same region share one metastore. This enables:
- Data engineers in `dev` workspace and analysts in `prod` workspace to access the same Gold tables.
- Central governance — permissions set on `catalog.schema` apply to all workspaces using that metastore.

**In practice for this project:**

```sql
-- Full reference (always works)
SELECT * FROM dbw_ev_intelligence_dev.gold.FactChargingSession;

-- Set defaults to use shorthand
USE CATALOG dbw_ev_intelligence_dev;
USE SCHEMA gold;
SELECT * FROM FactChargingSession;
```

**Permission example:**
```sql
GRANT SELECT ON SCHEMA dbw_ev_intelligence_dev.gold TO `grp-voltgrid-analysts`;
GRANT SELECT, MODIFY ON SCHEMA dbw_ev_intelligence_dev.silver TO `grp-data-engineers`;
DENY SELECT ON SCHEMA dbw_ev_intelligence_dev.bronze TO `grp-voltgrid-analysts`;
```

This is more maintainable than configuring ADLS ACLs per file path — Unity Catalog policies apply regardless of which cluster or workspace the query runs from.

---

### Q24. What is a Storage Credential in Unity Catalog and why is it needed? What would break if you deleted it?

**Answer:**

**What it is:**

A Storage Credential is a Unity Catalog metadata object that wraps an Azure identity (the Managed Identity of an Access Connector) and stores it securely at the metastore level.

In this project: `cred-ev-intelligence-dev` wraps the Managed Identity of `ac-ev-intelligence-dev`.

**Why it's needed — the trust chain:**

```
Notebook accesses /Volumes/...
  → Unity Catalog checks: user has READ VOLUME privilege? YES
  → UC looks up which External Location covers this path
  → External Location references Storage Credential: cred-ev-intelligence-dev
  → Storage Credential uses Access Connector Managed Identity
  → Azure IAM confirms MI has Storage Blob Data Contributor on evdatalakedev
  → ADLS returns the file
```

Without the Storage Credential, Unity Catalog has no identity to present to ADLS. The External Location cannot be validated.

**What breaks if deleted:**

Every External Location referencing `cred-ev-intelligence-dev` becomes invalid. That means:
- `evdatalakedev-bronze`, `evdatalakedev-silver`, `evdatalakedev-gold` External Locations all fail validation.
- All UC Volumes (`bronze_volume`, `silver_volume`, `gold_volume`) become inaccessible.
- Any notebook using `/Volumes/...` paths throws `IOException: Unable to access path`.
- Unity Catalog tables (Delta tables registered via External Locations) cannot be read.

**Why Access Connector over Service Principal for Storage Credentials:**

Access Connector's Managed Identity has no secret to manage — Azure rotates it automatically. A Storage Credential backed by a Service Principal would require secret rotation, which would break all External Locations if the secret expired. This is why Databricks recommends Access Connector for Unity Catalog.

---

## 10. Serving Layer — Synapse, Cosmos DB & Power BI

---

### Q25. The mobile app needs to show live charging session progress with <2 second latency. Why did you use Cosmos DB instead of Synapse Analytics for this?

**Answer:**

**Synapse Analytics limitations for low-latency APIs:**

| Metric | Synapse Serverless | Synapse Dedicated Pool | Cosmos DB |
|---|---|---|---|
| Query latency | 1–30 seconds (cold) | 50ms–2 seconds (if cached) | <10ms (indexed document) |
| Concurrent connections | Limited (burst to ~100) | Hundreds with reservation units | Millions (globally distributed) |
| Pricing model | Per TB scanned | Fixed capacity (DTUs) | Per RU/s (request units) |
| Read pattern | Ad-hoc SQL | SQL | Document lookup by key |

The mobile app pattern is: `GET /api/session/S-10042` → return one session document. This is a key lookup, not a complex SQL aggregation. Cosmos DB is purpose-built for this: partition the `live_station_status` collection by `station_id`, and a read is a single partition lookup — single-digit millisecond latency.

**Architecture in VoltGrid:**

```
Gold Delta (Databricks) 
    → ADF Copy (every 5 minutes) 
    → Cosmos DB collections:
        - live_station_status    (active sessions + charger availability)
        - session_live           (per-session progress: kWh, ETA, status)
        - charger_availability   (available/in-use/offline per charger)
        - customer_history       (last 10 sessions per customer)
```

**Why not Redis / Azure Cache:**
Cosmos DB provides persistent storage + global replication (if needed) + built-in partitioning. Redis is a cache with TTL — if the pod restarts, data is lost until Gold refreshes it again. For billing-related data (session kWh delivered), persistence is required.

**Power BI still uses Synapse:**
Synapse serves complex analytical queries (revenue by state, YoY comparisons) where a 2-second latency is acceptable. Power BI Import mode caches aggregated mart data in-memory — most dashboard tiles load in <1 second because the data is already loaded into VertiPaq.

---

### Q26. A franchise owner in Queensland is complaining that Power BI shows them data for stations in NSW. What went wrong, and how do you fix it?

**Answer:**

**Root cause diagnosis:**

Row-Level Security (RLS) in Power BI is applied via DAX filter expressions and user role assignments. The most common failure modes:

**Failure Mode 1 — RLS role not assigned:**
The franchise owner's email is in the Power BI workspace but not assigned to the `FranchiseOwner` role:
```
Power BI Workspace → Semantic model → Security → FranchiseOwner role → Members: [should contain their email]
```

**Failure Mode 2 — Wrong DAX filter in the role:**
The role definition uses a wrong column:
```dax
-- Wrong: comparing state_name instead of franchise_id
[state_name] = USERPRINCIPALNAME()

-- Correct for franchise owner:
DimFranchisePartner[owner_email] = USERPRINCIPALNAME()
```

**Failure Mode 3 — RLS not propagated to Synapse:**
If Power BI uses DirectQuery to Synapse, Synapse-level RLS must also be configured. If it's only set in Power BI (not Synapse), and someone queries Synapse directly via SQL, they bypass the filter.

**Fix for this scenario:**

1. Check the user's role membership in Power BI Security settings.
2. Verify the `DimFranchisePartner` table has `owner_email` populated correctly for Queensland stations.
3. DAX filter for `FranchiseOwner` role:
```dax
[partner_email] = USERPRINCIPALNAME()
```
4. Test: Power BI → "View As Role" → enter the franchise owner's email → confirm only Queensland stations appear.
5. For Synapse: add their email to the `franchise_rls_group` table and confirm the RLS predicate is filtering correctly.

**Prevention:** Add an automated test that logs in as a test franchise owner account and asserts the row count matches only their assigned stations. Run this test on every Power BI model deployment.

---

## 11. CI/CD, Monitoring & Observability

---

### Q27. How do you deploy a change to a Silver transformation notebook from development to production without breaking the production pipeline?

**Answer:**

**The CI/CD pipeline in Azure DevOps:**

**Repo structure:**
```
/databricks/
  silver/
    03_silver_all_entities_optimised_v3.ipynb
    tests/
      test_silver_payments.py
      test_silver_customers.py
/adf/
  arm_templates/
    pl_silver_api_transformation.json
```

**Deployment flow:**

**Step 1 — Feature branch:**
Developer creates branch `feature/silver-gst-fix`. Changes the GST split logic in `v3` notebook.

**Step 2 — PR gate (automated):**
Azure DevOps pipeline triggers on PR:
- `pylint` / `flake8` lint check on the notebook code.
- Unit tests run on a dev Databricks cluster: `pytest databricks/silver/tests/test_silver_payments.py`.
- DQ checks: run the notebook on a sample of Bronze data, assert `quarantine_pct < 1%`.
- Schema validation: assert output Silver schema matches the expected schema contract.

**Step 3 — Dev environment deployment:**
After PR approval and merge to `dev` branch:
- ADF ARM template deployed to `adf-ev-intelligence-dev` (Dev environment).
- Notebook deployed to `dev` Databricks workspace.
- End-to-end integration test: trigger the ADF Silver pipeline on dev data.

**Step 4 — QA / UAT:**
Same ARM template deployed with environment-specific parameters (different storage accounts, Key Vault names).

**Step 5 — Prod release (with approval gate):**
- Production release requires a manual approval from the team lead in Azure DevOps.
- Deployment happens in a maintenance window (2:00–4:00 AM AEST) when batch pipelines are idle.
- Blue-green: the new notebook version is deployed to `v_new` path, ADF is updated to point to it, then `v_old` is kept for 24 hours as rollback option.

**Rollback:** If the production Silver job fails after deployment, ADF pipeline is updated to point back to the previous notebook version in <5 minutes (without redeploying code).

---

### Q28. How do you know if your data pipeline is healthy? What monitoring do you have in place?

**Answer:**

**Four layers of monitoring in VoltGrid:**

**Layer 1 — Infrastructure (Azure Monitor + Log Analytics):**

| Metric | Alert threshold | Action |
|---|---|---|
| ADF pipeline run status = Failed | Any failure | Email + Teams to data engineering |
| Databricks job exit code ≠ 0 | Any non-zero | Alert with job name + error message |
| Streaming consumer lag > 5 min | 5 minutes | Streaming cluster auto-restart check |
| ADLS spend > monthly budget | 110% of budget | Cost alert to team lead |

**Layer 2 — Data freshness checks:**

A custom DQ audit table in `gold/data_quality_audit/` tracks the last successful write per entity:
```sql
SELECT entity_name, MAX(run_timestamp) as last_success
FROM gold.data_quality_audit
WHERE status = 'SUCCESS'
GROUP BY entity_name
HAVING DATEDIFF(minute, MAX(run_timestamp), CURRENT_TIMESTAMP) > 120  -- >2 hours stale
```
Alert fires if any entity hasn't been updated in 2 hours during the batch window.

**Layer 3 — Data volume anomaly detection:**

```kql
-- Azure Monitor: row count < 80% of 7-day average
let avg_rows = customEvents
  | where name == "silver_row_count"
  | summarize avg(toreal(customDimensions.row_count)) by entity_name;
customEvents
  | where name == "silver_row_count"
  | join avg_rows on entity_name
  | where toreal(customDimensions.row_count) < 0.8 * avg_row_count
```

If payments suddenly drops from 1,200 rows/day to 50 rows, the API may be returning empty pages (a common paging bug).

**Layer 4 — Business rule checks (domain-specific):**

- Total Gold revenue never decreases day-over-day unless there are refunds.
- Active sessions count at 2:00 PM AEST should be > 100 (if zero, streaming is likely down).
- `reconciliation_status = MISMATCH` rate < 1% across all sessions.

**Dashboards:** A Power BI "Data Quality Dashboard" reads the `gold/data_quality_audit/` table and shows: entity freshness, quarantine rates, DQ check pass/fail history — giving the business visibility into pipeline health without needing to read logs.

---

## 12. SCD, Slowly Changing Dimensions & History

---

### Q29. Explain the three most common SCD types and which ones are used in this project. Give a concrete scenario for each.

**Answer:**

**SCD Type 1 — Overwrite (no history):**

Current value replaces old value. No history kept.

Used for: `DimStation`, `DimCity`, `DimCharger` (for operational attributes).

Scenario: Station 101 changes its `site_type` from "Highway" to "Urban Commercial". The DimStation row is updated in place. Old sessions that happened when it was a "Highway" station will now show "Urban Commercial" when joined — this is acceptable because `site_type` is used for reporting categories, not financial calculations.

**SCD Type 2 — New row for each change (full history):**

A new row is inserted for each change. Old row is closed with `effective_to` and `is_current = FALSE`.

Used for: `DimTariff`, `DimCustomer` (loyalty tier).

Scenario: A customer `C-10042` upgrades from `Silver` to `Gold` loyalty tier on July 15th.

```
customer_key | customer_id | loyalty_tier | effective_from | effective_to  | is_current
SK-001       | C-10042     | SILVER       | 2024-01-01     | 2025-07-14    | FALSE
SK-002       | C-10042     | GOLD         | 2025-07-15     | 9999-12-31    | TRUE
```

FactChargingSession stores `customer_key = SK-001` for sessions before July 15th and `customer_key = SK-002` for sessions after. Revenue analysis by loyalty tier is historically accurate.

**SCD Type 3 — Keep previous value in a new column:**

Adds a `previous_value` column — only remembers the last change (not full history).

Not used in this project, but relevant for: `DimCharger.previous_firmware_version`. If a charger was upgraded from firmware `v2.1` to `v2.3`, keeping `previous_firmware` helps correlate whether a fault rate change started with the firmware upgrade.

**When to choose SCD2 over SCD1:**
Choose SCD2 when the changed attribute is used in metric calculations (tariff rates affect revenue, loyalty tier affects discount calculations). Choose SCD1 when the attribute is purely descriptive and historical accuracy isn't needed (station phone number).

---

## 13. Performance, Optimisation & Cost

---

### Q30. Power BI DirectQuery on Synapse is slow for the Revenue by State dashboard. Walk me through how you would diagnose and fix it.

**Answer:**

**Step 1 — Diagnose where time is spent:**

Power BI Performance Analyser (View → Performance Analyser) shows per-visual query durations:
- `DAX query duration: 200ms` — fast, issue is not in Power BI.
- `Direct query duration: 28 seconds` — this is the Synapse round-trip.

**Step 2 — Capture the SQL sent to Synapse:**

Power BI sends translated SQL to Synapse. Check Synapse Query Store or Azure Monitor Diagnostics:
```sql
-- Power BI generated this (typical)
SELECT s.state_name, SUM(p.amount_aud) as revenue
FROM FactPayments p
JOIN DimStation st ON p.station_key = st.station_key
JOIN DimState s ON st.state_key = s.state_key
JOIN DimTime t ON p.time_key = t.time_key
WHERE t.year = 2025 AND t.month = 6
GROUP BY s.state_name
```

**Step 3 — Identify bottleneck:**

Check Synapse execution plan. Common issues in this project:
- **Full table scan on FactPayments** (50K+ rows) without partition pruning → because Power BI's filter on `DimTime.year/month` doesn't translate to a partition filter on the fact table's `load_date` partition column.
- **No statistics:** Synapse dedicated pool needs `CREATE STATISTICS` on join columns for the query optimizer to choose the right join strategy.

**Step 4 — Fix options (in order of effort):**

**Fix A — Use pre-built mart (immediate, no code change):**
The `mart_revenue_by_geo_month` mart is already aggregated by state and month. Switch the Power BI dataset to Import mode from this mart — query goes from 28 seconds to <1 second (VertiPaq in-memory).

**Fix B — Add Synapse partition pruning (medium effort):**
```sql
-- Partition FactPayments by year_month in Synapse dedicated pool
CREATE TABLE FactPayments
WITH (DISTRIBUTION = HASH(station_key), PARTITION (year_month RANGE RIGHT FOR VALUES ('2025-01', '2025-02', ...)))
AS SELECT *, FORMAT(payment_ts, 'yyyy-MM') as year_month FROM FactPayments_staging;
```

**Fix C — Create Synapse statistics (quick):**
```sql
CREATE STATISTICS stat_FactPayments_station_key ON FactPayments(station_key);
CREATE STATISTICS stat_FactPayments_time_key ON FactPayments(time_key);
```

**Recommendation for VoltGrid:** The Revenue dashboard is the most-used dashboard. Switch to Import mode from `mart_revenue_by_geo_month` (Fix A). Retain DirectQuery only for the Live Charging dashboard where near-real-time data is essential.

---

### Q31. Delta Lake OPTIMIZE and ZORDER — when and why would you use them in this project?

**Answer:**

**Why OPTIMIZE is needed:**

Delta Lake writes data in small files during streaming (one file per micro-batch per partition) and during frequent MERGEs. Over time, `bronze/iot/charger_telemetry_raw/event_date=2025-06-15/` might contain 10,000 tiny 50KB Parquet files instead of a few 128MB files. Spark reads each file with an overhead — 10,000 files = 10,000 file open/read operations = very slow queries.

```python
from delta.tables import DeltaTable

# Compact small files into larger ones (default 1GB target size)
spark.sql("OPTIMIZE delta.`/path/to/silver/sl_iot_charger_telemetry`")
```

**When to run OPTIMIZE in this project:**
- After every batch Silver job run (post-MERGE, which creates many small files).
- Scheduled on streaming tables daily at 3:00 AM AEST (least-active period).
- Monitor: if `DESCRIBE DETAIL` shows `numFiles > 1000` for a single partition, OPTIMIZE is overdue.

**ZORDER — co-locate related data:**

ZORDER reorders data within each file so that rows with similar values for a column are physically adjacent. This enables data skipping — if you query `WHERE charger_id = 'CHG-0042'`, Spark reads only files where CHG-0042 appears, skipping the rest.

```python
# For FactChargingSession queries that filter by station and time
spark.sql("""
  OPTIMIZE delta.`/path/to/gold/FactChargingSession`
  ZORDER BY (station_key, time_key)
""")
```

**When to ZORDER:**
- Apply on columns used in WHERE clauses for the most common dashboard queries.
- For FactChargingSession: `station_key, time_key` (Revenue by Station/Month queries).
- For FactDeviceTelemetry: `charger_key, event_date` (Maintenance dashboard filters by charger and date range).

**VACUUM — remove old Delta files:**
```python
spark.sql("VACUUM delta.`/path/to/bronze/iot/charger_telemetry_raw` RETAIN 168 HOURS")
```
Removes old files beyond the retention period. Important for Bronze (90-day retention policy) to avoid runaway storage costs.

---

## 14. Real-world Scenario Curveballs

---

### Q32. You wake up at 3:00 AM to an alert: "Gold FactChargingSession table has 0 new rows for the last 3 hours during the batch window." Walk me through your incident response.

**Answer:**

**Immediate triage (0–5 minutes):**

1. Open Azure Monitor → ADF pipeline runs for `pl_bronze_api_master_v4` and `pl_silver_api_transformation`.
   - Is the Bronze ingest pipeline running? Completed? Failed?
2. Open Databricks Jobs → Silver job run history.
   - Last run status? If failed, what error?

**Case 1 — ADF Bronze pipeline failed:**

Check the pipeline run details → which activity failed:
- `act_get_watermark` (SQL lookup) failed → Azure SQL is down or connection timed out → check SQL server health in Azure Portal.
- `act_copy_api` failed → check which entity → is the VoltGrid API down? (HTTP 503)
- `act_write_watermark` failed → SQL write permission issue.

Fix: Resolve root cause, manually trigger `pl_bronze_api_master_v4` rerun. Watermark pattern ensures it picks up from where it left off — no data loss.

**Case 2 — Databricks Silver job failed:**

Open the failed job run → cluster logs → driver logs:
- `OutOfMemoryError: GC overhead limit exceeded` → Silver job ran out of memory. Scale cluster or reduce batch size.
- `AnalysisException: Column 'wallet_provider' not found` → API schema evolved, new column arrived in Bronze that Silver's schema doesn't know about.
- `DeltaOptimisticLockException` → Two concurrent jobs tried to write to the same Silver table. Check if a backfill job ran simultaneously.

**Case 3 — Gold job failed:**

Check Gold job logs. Most common cause: Silver table is empty (because Silver job failed above) — Gold aggregation runs on empty Silver, writes zero rows to FactChargingSession.

**Case 4 — Everything ran successfully but Gold still shows 0 rows:**

Check the ADF Gold → Cosmos DB sync job — it may have failed silently. Check Cosmos DB metrics for last write time.

**Communication:**
- Within 15 minutes: post status in Teams `#data-incidents` channel: "Investigating Gold pipeline 0-row alert. Root cause: [Silver job OOM]. ETA for fix: 30 minutes."
- After fix: post resolution and root cause. Add monitoring for the specific failure mode (e.g., memory alert on Silver cluster).

**Post-incident:**
Add a DQ check: after each Gold job run, assert `new_rows > 0`. If zero, fail the job loudly rather than writing silently empty results.

---

### Q33. A senior stakeholder says the "Charger Uptime" KPI on the dashboard has been wrong for the past 2 weeks — it's showing 98% uptime but ops knows there were multiple outages. How do you investigate and fix this?

**Answer:**

**Hypotheses to test:**

**Hypothesis 1 — Uptime formula is wrong:**

Check `mart_charger_uptime_daily` calculation:
```python
# Current formula might be wrong:
uptime_pct = sessions_count / total_possible_hours * 100  # sessions, not actual uptime

# Correct formula should be:
uptime_pct = (total_hours - offline_duration_hours) / total_hours * 100
```

Where `offline_duration_hours` comes from `sl_connector_status` (when `connector_status = 'OFFLINE'`).

If the current code counts sessions as a proxy for uptime and a charger had 0 sessions but was actually online (just unused), it would show 0% uptime. Or if the offline event stream had a gap, offline duration = 0 = 100% uptime.

**Hypothesis 2 — Connector status streaming had a gap:**

Check if the `sl_connector_status` streaming table has a data gap during the affected 2-week period:
```sql
SELECT event_date, COUNT(*) as rows
FROM sl_connector_status
WHERE event_date BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY event_date
ORDER BY event_date
```

If certain dates have 0 rows, the streaming job was down and offline events were missed → uptime appeared 100% because no OFFLINE events were recorded.

**Hypothesis 3 — Watermark for quarantine ate real offline events:**

If the OFFLINE status events arrived >10 minutes late (network delay), they were routed to `silver/quarantine/late_events/` instead of being counted. Check quarantine:
```sql
SELECT event_date, COUNT(*) as late_offline_events
FROM silver_quarantine_late_events
WHERE status = 'OFFLINE' AND event_date BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY event_date
```

**Fix:**

1. For Hypothesis 1: correct the uptime formula in the Gold notebook, rebuild `mart_charger_uptime_daily`.
2. For Hypothesis 2: replay Bronze data from `bronze/streaming/connector_status_raw/` for the gap dates → rerun Silver → rerun Gold mart.
3. For Hypothesis 3: adjust the watermark to 30 minutes (if network latency is routinely > 10 min) and replay quarantined events through the Silver pipeline.

**Communicate fix to stakeholder:** Provide corrected uptime numbers, explain root cause, show the corrected 2-week trend, and commit to an accuracy SLA check: if uptime changes by >5% between consecutive Gold runs without a corresponding maintenance event, alert the engineering team.

---

### Q34. The project needs to expand from Australia to New Zealand. What would need to change in the data model and pipelines?

**Answer:**

**Data model changes:**

**DimCountry:** Currently has one row (`Australia`). Add `New Zealand` row. Add `nz_gst_rate = 0.15` (NZ GST is 15%, not 10% like Australia).

**DimState:** Add NZ regions: Auckland, Wellington, Canterbury, Waikato, Bay of Plenty, etc.

**DimCity:** Add NZ cities.

**DimTime:** NZ is UTC+12 (NZST) / UTC+13 (NZDT). The DimTime table currently only handles AEST/AEDT. Add `nzst_local_time`, `nzdt_local_time` columns. The Silver `normalise_timestamps` function needs NZ timezone handling.

**FactChargingSession:** The `reconciliation_status` formula needs to use `country_gst_rate` from DimCountry rather than a hardcoded 10%.

**GST Split (Rule J) update:**
```python
# Before: hardcoded Australian GST
gst_amount = gross_amount * 0.10 / 1.10

# After: parameterised by country
gst_rate = country_gst_rate  # 0.10 for AU, 0.15 for NZ
gst_amount = gross_amount * gst_rate / (1 + gst_rate)
```

**Pipeline changes:**

**Bronze:** New source systems from NZ chargers, NZ CRM, NZ payment gateway. Add NZ-specific entities to `pipeline_metadata_config.json` — the metadata-driven pipeline handles this without new pipelines (just new config rows).

**Silver:** Fault code mappings may differ if NZ chargers use different OCPP firmware. Add NZ-specific standardisation rules.

**Serving layer:**
- Power BI RLS: add `NZ Country Manager` role filtered to `DimCountry[country_code] = 'NZ'`.
- Cosmos DB: add NZ collections with NZ-specific partitioning.

**Compliance:**
NZ Privacy Act 2020 has similar but slightly different requirements to the Australian Privacy Act. Review PII handling rules for NZ customers.

**The key advantage of this architecture:** Because we used DimCountry → DimState → DimCity as a geo hierarchy from day one (even though only Australia was in scope), the foundation is already extensible. We don't need to restructure the star schema — just add rows and update a few formulas.

---

### Q35. A data engineer on your team accidentally ran a DELETE on the Silver `sl_customers` table, removing 2,000 customer records. How do you recover?

**Answer:**

**Why this is recoverable — Delta Lake ACID + time travel:**

Every Delta write (including DELETE) creates a new version in the transaction log. The deleted records are not immediately removed from storage — they are marked as deleted in the log but the Parquet files remain until `VACUUM` runs.

**Recovery steps:**

**Step 1 — Immediately stop any further writes:**
Pause the Silver batch job (ADF pipeline) to prevent new writes from compounding the issue.

**Step 2 — Identify the version before the DELETE:**
```python
from pyspark.sql.functions import col

# Check history
spark.sql("DESCRIBE HISTORY delta.`/path/to/silver/sl_customers`").show(20, truncate=False)

# Output shows:
# version | timestamp              | operation | operationParameters
# 145     | 2025-06-15 09:30:00    | DELETE    | {"predicate": "..."}
# 144     | 2025-06-15 08:00:00    | MERGE     | {...}
```

Version 145 is the DELETE. Version 144 is safe.

**Step 3 — Read the previous version:**
```python
# Read the good state (before DELETE)
good_df = spark.read.format("delta") \
    .option("versionAsOf", 144) \
    .load("/path/to/silver/sl_customers")

# Verify row count
print(f"Good version rows: {good_df.count()}")  # should include the 2,000 deleted rows
```

**Step 4 — Restore the table:**
```python
# Restore to version 144
spark.sql("""
  RESTORE TABLE delta.`/path/to/silver/sl_customers`
  TO VERSION AS OF 144
""")

# Verify
spark.sql("SELECT COUNT(*) FROM delta.`/path/to/silver/sl_customers`").show()
```

`RESTORE` is atomic — it creates a new version (146) that points back to version 144's data.

**Step 5 — Verify and resume:**
- Assert row count is restored: should be the original count including the 2,000 records.
- Check if any valid writes happened between version 144 and 145 (i.e., between the last MERGE and the accidental DELETE). If yes, replay only those valid writes.
- Resume the Silver batch pipeline.

**Step 6 — Prevention:**

- Add Unity Catalog privilege check: `REVOKE DELETE ON TABLE sl_customers FROM data_engineers;` — engineers should use MERGE (upsert) not DELETE.
- Add pre-commit hook in the CI/CD pipeline that flags any notebook containing `DELETE FROM` on Silver tables and requires team-lead approval.
- VACUUM retention: ensure `RETAIN 168 HOURS` (7 days) — gives a 7-day recovery window.

---

## Quick Reference: Key Numbers from This Project

| Metric | Value |
|---|---|
| Total sources | 28 |
| Data scale | ~50,000 records |
| Bronze tables | 22+ raw tables |
| Silver tables | 25 cleansed tables |
| Gold dimensions | 13 |
| Gold fact tables | 9 |
| Aggregated marts | 10 |
| Power BI dashboards | 14 views |
| Streaming topics (Event Hub) | 10 |
| Streaming watermark | 10 minutes |
| IoT temperature threshold | 75°C (overheating flag) |
| Cosmos DB read SLA | < 2 seconds |
| Bronze retention | 90 days |
| Australian GST rate | 10% |
| Complaint SLA — P1 | 4 hours |
| Complaint SLA — P2 | 8 hours |
| Complaint SLA — P3 | 24 hours |
| SP client secret rotation | Every 90 days |
| SAS token permissions for external users | `sp=rl` (read + list only) |
| Geo hierarchy levels | Country → State → City → Station → Charger → Session |

---

*Questions authored for VoltGrid AU — Azure EV Data Engineering Project*
*Coverage: 2–8 years Data Engineer experience | Azure, Databricks, Delta Lake, ADF, PySpark*
