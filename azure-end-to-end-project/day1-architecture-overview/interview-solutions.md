# Day 1 — Interview Solutions: Architecture Overview

---

## Medallion Architecture

**A1**
Medallion Architecture is a data organisation pattern with three layers that data flows through one-way:

| Layer | Purpose | Key Characteristics |
|---|---|---|
| **Bronze** | Raw landing zone — exact copy of source data | Append-only, no transforms, immutable, supports replay |
| **Silver** | Cleansed and validated — single source of truth | Dedup, normalise, validate, DQ checks, MERGE upsert |
| **Gold** | Business-curated — aggregated and modelled | Star schema, pre-computed marts, serves BI and APIs |

Data moves: Bronze → Silver → Gold. Each layer has a different purpose — Bronze is for reliability and lineage, Silver is for correctness, Gold is for business performance.

---

**A2**
Bronze is append-only because it is the **audit and replay layer**. If Bronze is modified or deleted:
- You lose the ability to re-run Silver transformations from scratch when a bug is found
- You lose the audit trail — you can no longer prove what the source actually sent
- You cannot roll back to a clean state after a bad transformation

Example: if a Silver transformation has a bug that corrupts 10,000 rows, you can reprocess from Bronze because the raw data is untouched. If Bronze was modified, there is nothing to replay from.

---

**A3**
- **Bronze writes: Append** — every new record from the source is added to Bronze, even if it is a duplicate. Nothing is modified or deleted.
- **Silver writes: MERGE (upsert)** — on each run, Silver checks if a record with the same key already exists. If yes → update it. If no → insert it.

Why MERGE in Silver: batch sources often resend records (incremental load overlaps, watermark offsets). If Silver used Append like Bronze, you would get duplicates in Silver too. MERGE makes Silver runs **idempotent** — you can re-run the Silver job multiple times with the same input and always get the correct result with no duplicates.

---

**A4**
Reading directly from source and writing to Gold without intermediate layers has several problems:
1. **No audit trail** — if Gold data is wrong, there is no raw copy to investigate what the source actually sent
2. **No replay** — if the transformation logic changes (bug fix, new business rule), you cannot reprocess historical data — the source data is gone
3. **No separation of concerns** — Bronze-to-Silver concerns (dedup, masking, DQ) are mixed with Silver-to-Gold concerns (aggregation, star schema), making the code impossible to maintain
4. **No quarantine** — bad records have nowhere to go — they either block the pipeline or get silently dropped
5. **Cost** — re-fetching from APIs or databases every time any transformation changes is expensive and rate-limited

The Medallion pattern pays the storage cost once and reprocesses cheaply from Bronze any time.

---

**A5**
Delta Lake over plain Parquet adds:
1. **ACID transactions** — writes either fully succeed or fully fail; readers always see a consistent snapshot
2. **Time travel** — `VERSION AS OF N` or `TIMESTAMP AS OF` lets you query historical versions of the table
3. **MERGE (upsert)** — `MERGE INTO target USING source ON key` — impossible with plain Parquet files
4. **Schema enforcement** — rejects writes with wrong column types; `mergeSchema` option for controlled evolution
5. **Transaction log (`_delta_log/`)** — every operation recorded, enables full audit and replay
6. **OPTIMIZE + Z-ORDER** — compacts small files and co-locates related rows for query performance
7. **Streaming + batch unification** — same Delta table can be read by batch jobs and streaming jobs simultaneously

---

## Source Systems and Ingestion

**A6**
28 sources split:
- **Batch (20 sources):** CSV, XML, REST APIs — ingested on schedule by ADF
- **Streaming (8 sources):** IoT JSON from Event Hub, ingested continuously by Databricks Structured Streaming

**Batch examples:**
- Charging Sessions DB (CSV, daily via ADF Copy)
- Payment Gateway (JSON REST API, daily via ADF REST connector)

**Streaming examples:**
- IoT Charger Telemetry — OCPP events → Azure IoT Hub → Event Hub → Databricks (1–3 sec latency)
- Charger Fault Stream — fault JSON → Event Hub → Databricks (real-time)

---

**A7**
High-watermark incremental load tracks the maximum value of a column (usually `updated_at` or `event_date`) from the last successful pipeline run — this is the "watermark". The next run fetches only records where `updated_at > last_watermark`.

Why not full load daily:
- Full load on a 50K-record source takes the same time every run — wasteful
- REST APIs have rate limits and pagination — fetching 125K payment records daily when only 500 changed is expensive
- With incremental load, only changed rows are fetched — faster, cheaper, and source-friendly
- The watermark is stored in an Azure SQL table and updated at the end of each successful run

---

**A8**
The Bronze layer partitions session records by `plug_in_date` (the actual date in the record), NOT by `load_date` (when ADF ran). This is critical: if a file delayed until 8 AM still contains records with `plug_in_date = 2026-05-31` (yesterday), they land in the correct `2026/05/31/` partition, not a "catch-up" partition that would be hard to query later. Silver then processes from Bronze using the record's own timestamp, not when it arrived, so the historical data is correct regardless of delivery delay.

---

**A9**

| | IoT Telemetry | CSV Sessions |
|---|---|---|
| Source | Physical EV chargers (OCPP protocol) | Session database file export |
| Azure service | Azure IoT Hub → Azure Event Hubs | Azure Data Factory (Copy Activity) |
| Trigger | Continuous — device pushes every 1–3 sec | Scheduled — ADF trigger at 3 AM daily |
| Databricks role | Structured Streaming consumer | Not involved at Bronze — ADF writes directly |
| Bronze format | Delta Lake, append-only stream write | Delta Lake, append-only batch write |
| Latency | Near real-time (seconds) | Batch (daily) |

---

**A10**
**Exactly-once** means every event is processed exactly one time — not zero times (loss) and not more than once (duplicates). In streaming systems, network failures can cause events to be replayed — without exactly-once, you get duplicate records.

How Databricks + Event Hub achieves it:
1. **Checkpointing:** Databricks writes the last-processed Event Hub sequence number to a checkpoint location (ADLS path). On restart, it resumes from exactly where it stopped.
2. **Idempotent writes:** Delta Lake's MERGE operation at Silver ensures that even if an event is processed twice (rare cases), the result is correct because MERGE on a unique key is idempotent.
3. Together: checkpoint prevents reprocessing + MERGE handles the rare case where it does happen.

---

## Silver Layer Transformations

**A11**
4 columns added to every Bronze table:

| Column | Purpose |
|---|---|
| `_ingestion_ts` | UTC timestamp when the record landed in Bronze — answers "when did we receive this?" |
| `_source_file` | Source filename or API endpoint URL — answers "where did this record come from?" |
| `_pipeline_run_id` | ADF or Databricks run ID — answers "which specific pipeline execution wrote this?" enables lineage |
| `_is_corrupt` | Boolean — `TRUE` if the record is malformed (bad JSON, missing key field) — records are not dropped, they are flagged and quarantined |

---

**A12**
AEST = UTC+10. `2026-06-01 17:30:00 AEST` → subtract 10 hours → `2026-06-01 07:30:00 UTC`.

In the Silver table: `start_ts = "2026-06-01T07:30:00Z"` (UTC)

In `DimTime`, the corresponding row carries both:
- `datetime_utc = "2026-06-01T07:30:00Z"`
- `datetime_aest = "2026-06-01T17:30:00"` (for reporting in local time)

Note: June is Australian winter → AEST (UTC+10), not AEDT (UTC+11). The transformation must handle the DST switch: Oct–Apr = AEDT (UTC+11), May–Sep = AEST (UTC+10).

---

**A13**
Both duplicate records land in Bronze (Bronze is append-only — no dedup at Bronze, by design). At Silver, **Rule A** applies:
- Dedup key for charger telemetry: `charger_id + event_ts + connector_id`
- When both records are processed by the Silver MERGE, the first write inserts the row; the second MERGE finds the same key already exists and either updates it (if any column differs) or leaves it as-is
- Result: only one row for this telemetry event in Silver

This is why exactly-once is the goal but not strictly required at Bronze — Silver's idempotent MERGE is the safety net.

---

**A14**
SCD Type 2 (Slowly Changing Dimension Type 2) tracks historical versions of a row. Instead of overwriting the old value, a new row is inserted with a date range, and the old row's `effective_to` is set to the change date.

**Example from this project — DimTariff:**
- Jan 2026: tariff rate = $0.45/kWh
- Jul 2026: rate changes to $0.52/kWh (peak)

SCD2 result:
```
tariff_key  rate   effective_from  effective_to  is_current
1           0.45   2026-01-01      2026-06-30    FALSE
2           0.52   2026-07-01      9999-12-31    TRUE
```

**Why Type 1 (overwrite) would be wrong:** If you overwrite the old rate, then any session from Jan–Jun 2026 that is joined to `DimTariff` would show the new rate $0.52/kWh — making historical billing calculations wrong. The billing reconciliation mart would show mismatches for all historical sessions. SCD2 preserves the correct rate for every historical session by joining on `session_date BETWEEN effective_from AND effective_to`.

---

**A15**
A streaming event 12 minutes late with a 10-minute watermark: it does NOT get silently dropped. It is routed to `silver/quarantine/late_events/` with the reason recorded. It is still stored — just outside the main Silver table. This design choice is intentional:
- Silent dropping would hide data loss
- The quarantine path allows investigation, manual reconciliation, or reprocessing
- The DQ audit table records the count of late events per run — visible in the DQ dashboard

---

## Gold Layer — Star Schema

**A16**
A star schema is a data modelling pattern for analytical queries:
- **Dimension tables** — descriptive context: who, what, where, when. Contain attributes used for filtering, grouping, and labelling. Example: `DimCustomer` has `loyalty_tier`, `city`, `signup_date`.
- **Fact tables** — measurable events or metrics: how much, how many, how long. Contain numeric measures and foreign keys pointing to dimension tables. Example: `FactChargingSession` has `energy_kwh`, `duration_min`, `billed_amount`.

Query pattern: SELECT Dimension.attribute, SUM(Fact.metric) FROM Fact JOIN Dimension ON Fact.dim_key = Dimension.dim_key GROUP BY Dimension.attribute

It is called "star" because the diagram looks like a star — Fact table in the centre, Dimension tables radiating outward.

---

**A17**
`reconciliation_status` compares what was expected from the session vs what was actually paid:

1. **`FactChargingSession`** contributes `energy_kwh` and `session_date`
2. **`DimTariff` (SCD2)** joined on `session_date BETWEEN effective_from AND effective_to` → gives `rate_per_kwh`
3. **`sl_payments` (Silver)** joined on `session_id` → gives actual `amount_paid`

Calculation:
- `expected_amount = energy_kwh × rate_per_kwh`
- `reconciliation_status = CASE WHEN ABS(expected_amount - amount_paid) < 0.01 THEN 'MATCH' ELSE 'MISMATCH' END`
- `difference_amount = expected_amount - amount_paid`

The `mart_billing_reconciliation` pre-computes this at session level for the Finance dashboard.

---

**A18**
Row-level security (RLS) is enforced at two levels:

1. **Power BI RLS:** A security role is defined in the Power BI data model. The rule: `DimStation[partner_key] = LOOKUPVALUE(UserPartnerMap[partner_key], UserPartnerMap[email], USERPRINCIPALNAME())`. When the franchise owner logs in, Power BI resolves their email to their `partner_key` and filters all visuals to only show rows where `DimStation.partner_key` matches.

2. **Azure Synapse RLS:** `CREATE SECURITY POLICY StationFilter ADD FILTER PREDICATE fn_StationSecurity(partner_key) ON DimStation`. Any query through Synapse automatically applies this filter based on the logged-in identity.

Result: the franchise owner sees only their 5 stations across all 14 dashboards, even though the underlying data has all 120 stations.

---

**A19**
Pre-computing marts gives two performance benefits:

1. **Query speed:** `SELECT state, SUM(amount) FROM mart_revenue_by_geo_month WHERE month = '2026-06'` scans a small, pre-grouped table. The alternative — `SELECT state, SUM(amount) FROM FactPayments JOIN DimStation JOIN DimTime WHERE ...` — scans millions of raw fact rows every time Power BI refreshes.

2. **Cost:** Synapse dedicated pool charges by query execution time. Aggregating millions of fact rows on every dashboard load is expensive. Marts are computed once per day by a Databricks job and loaded to Synapse — Power BI reads pre-grouped data with simple SELECT statements.

Trade-off: mart data is 24 hours stale (batch computed). For near-real-time needs, DirectQuery to Synapse serverless is used — accepts slower query performance in exchange for live data.

---

**A20**
The **old tariff rate ($0.45/kWh)** is used for the 15 Jun session — and this is correct by design.

How: `FactChargingSession` stores the `tariff_key` foreign key at the time the session was written to Gold. When joining `DimTariff` for the report, it uses `session_date BETWEEN effective_from AND effective_to`:
- 15 Jun 2026 falls BETWEEN `2026-01-01` and `2026-06-30` → tariff_key for the old rate
- 15 Aug 2026 falls BETWEEN `2026-07-01` and `9999-12-31` → tariff_key for the new rate

The SCD2 design ensures that historical sessions always reflect the rate that was in effect at the time, regardless of when the report is generated.

---

## Security and Data Quality

**A21**
At **Bronze:** the full PAN `4242424242424242` and CVV `123` land as-is — Bronze is raw and immutable. However, the Bronze path for charge cards (`bronze/crm/charge_cards_raw/`) has a **restricted RBAC role** on ADLS — only the data pipeline service identity can read it. Human analysts do not have access.

At **Silver (Rule F — PCI masking):**
- Card number: `4242424242424242` → `************4242` (last 4 only, rest replaced with `*`)
- CVV: `123` → discarded entirely — `cvv_masked = "***"` stored, actual value gone
- `card_token` (a non-sensitive identifier) is used as the join key for all downstream tables

Why: PCI-DSS (Payment Card Industry Data Security Standard) mandates that full PANs and CVVs cannot be stored in analytics systems. Silver is the last point where this data is accessible in any form; Gold and beyond never see the raw PAN or CVV.

---

**A22**

| Records | DQ Check Failed | Destination | rejection_reason |
|---|---|---|---|
| 12 with `session_id = null` | Null check — `session_id` is mandatory | quarantine | `null_mandatory_field: session_id` |
| 3 with `end_ts < start_ts` | Business rule — `end_ts` must be after `start_ts` | quarantine | `business_rule_violation: end_ts < start_ts` |
| 5 with `energy_kwh = 920.0` | Range check — kWh must be 0–500 | quarantine | `range_check_failed: energy_kwh=920.0 exceeds max 500` |
| SES-002 with `charger_id = CHG-999` | Referential check — CHG-999 not in charger_master | quarantine | `referential_integrity: charger_id CHG-999 not found in charger_master` |
| Remaining valid records | All checks pass | Silver main table | (none) |

Total quarantined: 12 + 3 + 5 + 1 = 21 records. DQ metrics written to `gold/data_quality_audit/`. None are silently dropped.

---

**A23**
Managed Identity (MI) vs Service Principal:

| | Service Principal | Managed Identity |
|---|---|---|
| Credential | Client ID + Secret — you manage it | None — Azure manages it automatically |
| Rotation | Must rotate secret before expiry (e.g. every 180 days) | No rotation needed — Azure handles it |
| Risk | If secret leaks, attacker has access until rotated | No secret exists — nothing to leak |
| Lifecycle | Independent of resource — lives in Entra ID | Tied to resource — deleted when resource is deleted |

For service-to-service connections (ADF → ADLS, Databricks → Key Vault), Managed Identity is preferred because: no credential to manage, no rotation risk, no accidental secret exposure in logs. SP is used where MI is not supported (e.g., Databricks connecting to external ADLS via OAuth).

---

## Serving and Real-Time

**A24**
Full path for live charger availability API:

1. **IoT Charger** sends OCPP connector status events every few seconds
2. **Azure IoT Hub** receives the events → forwards to **Azure Event Hubs** topic `connector-status`
3. **Databricks Structured Streaming** reads from Event Hub → processes to `sl_connector_status` in Silver (near real-time, seconds lag)
4. A **Gold sync job** runs hourly — ADF or Databricks copies pre-aggregated station availability from Gold Delta to **Azure Cosmos DB** collection `live_station_status`
5. The **Fleet Web / Mobile API** queries Cosmos DB — response in <2 seconds because Cosmos DB is optimised for low-latency point reads, unlike ADLS or Synapse which are optimised for analytical scans

Why not query Synapse directly: Synapse serverless SQL queries on Delta files take 3–15 seconds per query and would not meet the 2-second API SLA. Cosmos DB pre-indexes the data for fast key-based reads.

---

**A25**
Onboarding VoltGrid New Zealand:

**Source types and ingestion:**
- NZ IoT chargers → Azure IoT Hub (same hub, different device group or separate hub) → Event Hubs → Databricks Streaming → Bronze
- NZ CRM CSV → ADF Copy → Bronze daily
- NZ ERP invoices → Logic Apps (email trigger) → Blob → Document Intelligence → Bronze JSON

**Dimension table changes:**
- `DimCountry`: add row for New Zealand (country_code = NZ)
- `DimState`: add NZ regions (Auckland, Wellington, Canterbury, etc.)
- `DimCity`: add NZ cities
- `DimStation`: add NZ stations (linked to NZ city_key and partner_key)
- `DimFranchisePartner`: add NZ partner row(s)
- `DimTariff`: add NZ tariff rates (GST rate = 15% in NZ, not 10% Australia)

**Row-level security:**
- NZ franchise owners: Power BI RLS filter `DimCountry.country_code = 'NZ'` AND `DimStation.partner_key = <NZ partner's key>`
- AU executives: can see both AU and NZ (no country filter)
- State/territory managers: limited to AU state rows only

**Alerting for NZ chargers:**
- Same Databricks alert rules apply — threshold checks on `sl_iot_charger_telemetry` (overheating, voltage spike, OFFLINE > 15 min) filter on NZ `station_id`
- Azure Functions trigger → email/SMS to NZ operations team via Azure Communication Services or NZ SMS provider

---
