# Topic 01 — Architecture & System Design
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 4 Questions | 2–8 years experience | Scenario-based

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
