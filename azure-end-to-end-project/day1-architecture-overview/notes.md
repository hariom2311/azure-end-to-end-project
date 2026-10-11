# Day 1 — Architecture Overview: VoltGrid AU Azure Data Engineering Platform

**Duration:** 2 hours | **Goal:** Understand the full project architecture, all Azure services used, data flow from source to dashboard, and how every layer connects before writing a single line of code.

> **Project:** VoltGrid Mobility Solutions Australia — EV Charging Franchise Intelligence Platform
> **Scale:** ~50K records | Medallion Lakehouse | Bronze → Silver → Gold | Azure Native

---

## Part 1 — Project Overview

| Item | Detail |
|---|---|
| Client | VoltGrid Mobility Solutions Australia |
| Domain | EV Charging Infrastructure & Fleet Intelligence |
| Geography | Australia — State / Territory / City / Station / Charger |
| Business Model | Franchise-owned EV charging stations |
| Data Scale | ~50K records across all sources |
| Architecture | Medallion Lakehouse (Bronze → Silver → Gold) |
| Cloud | Microsoft Azure |
| Processing | Batch + Structured Streaming (PySpark on Databricks) |
| Serving | Power BI dashboards + low-latency REST APIs |

**Business problems this platform solves:**

| Problem | Solution |
|---|---|
| No centralised visibility across franchise stations | Gold layer aggregates all stations into a unified Power BI view |
| Revenue leakage from billing mismatches | `mart_billing_reconciliation` compares expected vs actual payment |
| No real-time charger health monitoring | Streaming pipeline from IoT → Event Hub → Silver → alert rules |
| No predictive maintenance | `mart_predictive_maintenance_risk` tracks overheating patterns |
| Invoice tracking scattered across email and PDFs | Logic Apps + Document Intelligence extracts PDFs → Bronze → Gold |
| No geo-drill capability | DimCountry → DimState → DimCity → DimStation → DimCharger hierarchy |

---

## Part 2 — High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SOURCE SYSTEMS                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │  IoT Charger │  │  RDBMS / CSV │  │  REST APIs   │  │ Invoice PDFs │   │
│  │  Telemetry   │  │  (CRM / ERP/ │  │ (Payment /   │  │  (Email /    │   │
│  │  (OCPP)      │  │   Sessions)  │  │ Fleet/Weather)│  │   Blob)      │   │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │
└─────────┼─────────────────┼─────────────────┼─────────────────┼───────────┘
          │                 │                 │                 │
          ▼                 ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         INGESTION LAYER                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ Azure IoT Hub│  │  Azure Data  │  │  Azure Data  │  │ Logic Apps + │   │
│  │     +        │  │  Factory     │  │  Factory     │  │   Blob +     │   │
│  │ Event Hubs   │  │  (CDC/Copy)  │  │  (REST conn.)│  │ Doc Intel.   │   │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │
└─────────┼─────────────────┼─────────────────┼─────────────────┼───────────┘
          └─────────────────┴─────────────────┴─────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ADLS Gen2  —  BRONZE LAYER (Raw)                         │
│  Append-only Delta. No transforms. Supports audit, replay, reprocessing.    │
│  All records get: _ingestion_ts, _source_file, _pipeline_run_id, _is_corrupt│
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Azure Databricks   │
                    │  (PySpark + Stream) │
                    └──────────┬──────────┘
                               │
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ADLS Gen2  —  SILVER LAYER (Cleansed)                    │
│  Validated, deduplicated, business-rule applied. MERGE / upsert idempotent. │
│  Bad records → silver/quarantine/ with rejection reason column.             │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Azure Databricks   │
                    │  (Gold aggregation) │
                    └──────────┬──────────┘
                               │
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ADLS Gen2  —  GOLD LAYER (Business Curated)              │
│  Star schema: 13 Dimensions + 9 Facts + 10 Aggregated Marts                 │
│  Serves Power BI, Synapse Analytics, Cosmos DB, and REST APIs               │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
  Azure Synapse Analytics  Cosmos DB (<2s)   Azure Functions
          │                    │                    │
          ▼                    ▼                    ▼
     Power BI (14 views)  Fleet Web / App    Email/SMS/Teams Alerts
```

---

## Part 3 — Source Systems (28 sources)

| # | Source | Format | Ingestion | Frequency |
|---|---|---|---|---|
| 1 | IoT Charger Telemetry (OCPP) | JSON stream | Event Hub → Databricks Streaming | Near real-time (1–3 sec) |
| 2 | Charging Sessions DB | CSV | ADF Copy | Daily |
| 3 | Payment Gateway | JSON (REST API) | ADF REST connector | Daily |
| 4 | Customer Master (CRM) | CSV | ADF Copy | Daily |
| 5 | Vehicle Master | CSV | ADF Copy | Daily |
| 6 | Station Master | CSV | ADF Copy | Weekly |
| 7 | Charger Master | CSV | ADF Copy | Weekly |
| 8 | Franchise Partners | CSV | ADF Copy | Weekly |
| 9 | Charge Cards | CSV | ADF Copy | Daily |
| 10 | Complaints | CSV | ADF Copy | Daily |
| 11 | Employee Master | CSV | ADF Copy | Weekly |
| 12 | Invoice Metadata | CSV | ADF Copy | Daily |
| 13 | Invoice PDFs | PDF (Blob) | Logic Apps + Document Intelligence | On arrival |
| 14 | Tariff Master | CSV (SCD2) | ADF Copy | On change |
| 15 | Maintenance Events | CSV | ADF Copy | Daily |
| 16 | Fleet Usage | JSON (REST API) | ADF REST connector | Daily |
| 17 | Weather API | JSON (REST API) | ADF REST connector | Hourly |
| 18 | Customer Support Tickets | XML | ADF Copy | Daily |
| 19 | Grid Power Status | XML | ADF Copy | Hourly |
| 20 | Station Audit Logs | XML | ADF Copy | Daily |
| 21 | Charger Fault Stream | JSON stream | Event Hub → Databricks Streaming | Real-time |
| 22 | Connector Status Stream | JSON stream | Event Hub → Databricks Streaming | Real-time |
| 23 | Charging Session Events | JSON stream | Event Hub → Databricks Streaming | Real-time |
| 24 | Live Payment Status | JSON stream | Event Hub → Databricks Streaming | Real-time |
| 25 | RFID Card Scan Events | JSON stream | Event Hub → Databricks Streaming | Real-time |
| 26 | Station Utilisation Stream | JSON stream | Event Hub → Databricks Streaming | Real-time |
| 27 | Weather Alert Stream | JSON stream | Event Hub → Databricks Streaming | Real-time |
| 28 | Fleet Live Trip Stream | JSON stream | Event Hub → Databricks Streaming | Real-time |

**Two ingestion paths:**
- **Batch (ADF):** CSV, XML, REST APIs → Copy Activity → Bronze Delta
- **Streaming (Event Hub → Databricks):** IoT JSON → Structured Streaming → Bronze Delta (with checkpointing, exactly-once, 10-min watermark)
- **PDF (Logic Apps):** Email attachment → Blob → Document Intelligence → Bronze JSON

---

## Part 4 — Ingestion Patterns

| Pattern | Where Used |
|---|---|
| High-watermark incremental load | All CSV batch sources — `updated_at` column tracks last position |
| Exactly-once streaming | Event Hub + Databricks checkpointing |
| Idempotent MERGE (upsert) | Silver layer Delta writes — safe to re-run |
| Schema evolution (Auto Loader) | Bronze landing — handles new columns without breaking |
| Retry + exponential backoff | ADF REST API pipelines |
| Quarantine routing | Corrupt / schema-violating records → `bronze/quarantine/` |
| SCD Type 2 | Tariff, customer subscription, charger firmware history |

---

## Part 5 — Bronze Layer

**Purpose:** Immutable raw replica of every source record. No transformations applied. Supports audit, replay, and full reprocessing.

**Added columns on every Bronze table:**

| Column | Value |
|---|---|
| `_ingestion_ts` | UTC timestamp when the record landed |
| `_source_file` | Source filename or API endpoint URL |
| `_pipeline_run_id` | ADF / Databricks run ID for lineage |
| `_is_corrupt` | `TRUE` for malformed records — routed to quarantine |

**Bronze folder structure (key paths):**
```
bronze/
├── iot/charger_telemetry_raw/        (streaming JSON, partitioned by event_date/charger_id)
├── sessions/charging_sessions_raw/   (CSV batch, partitioned by plug_in_date)
├── payments/payment_transactions_raw/(API JSON, partitioned by load_date)
├── crm/customers_raw/                (full snapshot per load)
├── crm/charge_cards_raw/             (RBAC-restricted — PCI data)
├── fleet/fleet_usage_raw/
├── weather/weather_api_raw/
├── erp/invoices_raw/
├── erp/tariffs_raw/                  (SCD2 source)
├── xml/support_tickets_raw/          (parsed from XML, stored as JSON)
├── streaming/charger_faults_raw/     (real-time stream)
└── invoices/pdf_extracted_json/      (Document Intelligence output)
```

---

## Part 6 — Silver Layer

**Purpose:** Business-friendly, validated, deduplicated data. Single source of truth for all Gold aggregations. Bad records are quarantined, not dropped.

### Key Transformation Rules

| Rule | What It Does | Applied To |
|---|---|---|
| A — Dedup | Remove duplicate telemetry on `charger_id + event_ts + connector_id` | All streaming tables |
| B — Late events | Events >10 min late → quarantine | All streaming silver tables |
| C — Fault code mapping | `OVERTEMP` → "Overheating Risk", `VOLTAGE_SPIKE` → "Voltage Abnormality" | sl_iot_charger_telemetry |
| D — Billing reconciliation | `expected = energy_kwh × tariff_rate` vs payment amount | sl_invoices joined sl_payments |
| E — Temperature flag | `temp > 75°C` → `overheating_flag = TRUE` | sl_iot_charger_telemetry |
| F — PCI masking | Card number → last 4 digits only; CVV discarded | sl_charge_cards |
| G — Timezone normalisation | All timestamps → UTC; AEST equivalents in DimTime | All silver tables |
| H — SCD Type 2 | Tracks history on tariff rates, loyalty tier, firmware version | sl_tariffs_scd2 |
| I — SLA breach flag | P1 >4h, P2 >8h, P3 >24h → `sla_breach = TRUE` | sl_complaints |
| J — GST split | 10% GST: `gross = net + gst` | sl_invoices, sl_payments |

### Silver Tables (25 tables)

| Table | Source | Key Transformation |
|---|---|---|
| `sl_iot_charger_telemetry` | IoT stream | Dedup, fault mapping, overtemp flag |
| `sl_charging_sessions` | Sessions CSV | UTC normalise, duration validate |
| `sl_payments` | Payment API | Flatten, GST split, status normalise |
| `sl_customers` | CRM CSV | PII masking (email hash), loyalty tier standardise |
| `sl_complaints` | Complaints CSV | SLA breach flag, priority standardise |
| `sl_charge_cards` | Cards CSV | PAN masking, CVV discard |
| `sl_fleet_usage` | Fleet API | Flatten data array, metrics standardise |
| `sl_weather` | Weather API | Flatten, range validate |
| `sl_invoices` | Invoice CSV + PDF JSON | GST split, status normalise, pdf merge |
| `sl_tariffs_scd2` | Tariff CSV | SCD2 effective_from/to, is_current |
| `sl_maintenance_events` | Maintenance CSV | Root cause standardise, MTTR validate |
| `sl_grid_power_status` | Grid XML | XML parse, outage_flag boolean |
| `sl_station_audit_logs` | Audit XML | XML parse, action categorise |
| `sl_station_utilisation` | Stream JSON | Hourly dedup rollup, pct range validate |
| `sl_connector_status` | Stream JSON | Status enum validate, offline duration calc |
| `sl_charger_faults` | Stream JSON | Fault code → description, severity rank |
| `sl_rfid_scan_events` | Stream JSON | Auth status standardise |
| `sl_live_payments` | Stream JSON | Status normalise, amount validate |

---

## Part 7 — Gold Layer (Star Schema)

**Purpose:** Aggregated, modelled, reporting-ready tables. Serves Power BI and APIs directly.

### Dimensions (13)

| Dimension | Grain | SCD | Key Fields |
|---|---|---|---|
| `DimCountry` | Country | Type 1 | country_key, country_name, country_code |
| `DimState` | AU State/Territory | Type 1 | state_key, state_code, state_name |
| `DimCity` | City (48 cities) | Type 1 | city_key, city_name, state_key, postcode |
| `DimStation` | Station (120 stations) | Type 1 | station_key, city_key, site_type, status, lat, lng |
| `DimCharger` | Charger (300 chargers) | Type 2 | charger_key, station_key, connector_type, max_kw, firmware_version |
| `DimCustomer` | Customer | Type 2 | customer_key, customer_id, loyalty_tier, signup_date |
| `DimVehicle` | Vehicle | Type 1 | vehicle_key, customer_key, model, VIN |
| `DimEmployee` | Employee | Type 1 | employee_key, department, station_key |
| `DimFranchisePartner` | Partner | Type 1 | partner_key, partner_name, status |
| `DimChargeCard` | Card | Type 1 | card_key, customer_key, card_number_masked, status |
| `DimTariff` | Tariff version | Type 2 | tariff_key, rate_per_kwh, peak_offpeak, effective_from, effective_to, is_current |
| `DimWeather` | City + observation | Type 1 | weather_key, city_key, temperature_c, humidity_pct, condition |
| `DimTime` | Hour | — | time_key, date, year, month, day, hour, day_of_week, is_weekend, financial_year_au |

### Facts (9)

| Fact Table | Grain | Key Metrics |
|---|---|---|
| `FactChargingSession` | One row per session | energy_kwh, duration_min, expected_amount, billed_amount, reconciliation_status |
| `FactEnergyConsumption` | Charger + hour | total_kwh, session_count, peak_kw |
| `FactPayments` | One row per payment | amount_aud, gst, gateway, status, refund_flag |
| `FactMaintenance` | One row per event | root_cause, mttr_hours, overheating_flag, fault_description |
| `FactFleetUtilisation` | Vehicle + day | distance_km, trip_count, battery_pct_start, battery_pct_end |
| `FactDeviceTelemetry` | Charger + event | voltage, current, temperature, power_kw, fault_code |
| `FactComplaints` | One row per ticket | category, priority, sla_breach, resolution_days |
| `FactStationUtilisation` | Station + hour | active_chargers, utilisation_pct, sessions_count |
| `FactInvoice` | One row per invoice | amount_aud, gst_amount, status, pdf_name, pdf_extracted |

### Aggregated Marts (10 pre-computed for Power BI speed)

| Mart | Grain | Used By |
|---|---|---|
| `mart_revenue_by_geo_month` | State / City / Station × Month | Revenue Dashboard |
| `mart_charger_uptime_daily` | Charger × Day | Operations Dashboard |
| `mart_billing_reconciliation` | Session-level MATCH/MISMATCH | Finance Dashboard |
| `mart_complaint_sla_summary` | Station × Month | Complaints Dashboard |
| `mart_fleet_efficiency` | Vehicle × Week | Fleet Dashboard |
| `mart_predictive_maintenance_risk` | Charger × Day | Maintenance Dashboard |
| `mart_payment_method_split` | Payment mode × Month × Geo | Revenue Dashboard |
| `mart_energy_consumption_trend` | Station × Day | Energy Dashboard |
| `mart_franchise_performance` | Partner × Month | Franchise Dashboard |
| `mart_invoice_summary` | Station × Month | Finance Dashboard |

### Star Schema Diagram

```
                          ┌─────────────┐
                          │   DimTime   │
                          └──────┬──────┘
                                 │
         ┌──────────┬────────────┼────────────┬──────────┐
         │          │            │            │          │
   ┌─────▼──┐  ┌────▼───┐  ┌────▼───┐  ┌────▼───┐  ┌───▼────┐
   │DimCust │  │DimStat │  │DimChar │  │DimVehi │  │DimTarif│
   │omer    │  │ion     │  │ger     │  │cle     │  │f (SCD2)│
   └─────┬──┘  └────┬───┘  └────┬───┘  └────┬───┘  └───┬────┘
         └──────────┴────────────▼────────────┴──────────┘
                                 │
                      ┌──────────▼──────────┐
                      │  FactChargingSession │
                      │  (central fact)      │
                      └─────────────────────┘

  FactDeviceTelemetry ◄── DimCharger, DimStation, DimWeather, DimTime
  FactPayments        ◄── DimCustomer, DimChargeCard, DimStation, DimTime
  FactMaintenance     ◄── DimCharger, DimStation, DimEmployee, DimTime
  FactFleetUtil.      ◄── DimVehicle, DimCustomer, DimTime
  FactComplaints      ◄── DimCustomer, DimStation, DimTime
  FactStationUtil.    ◄── DimStation, DimTime
  FactInvoice         ◄── DimCustomer, DimStation, DimTime
  FactEnergyConsump.  ◄── DimCharger, DimStation, DimTime

  Geo hierarchy: DimCountry → DimState → DimCity → DimStation → DimCharger
```

---

## Part 8 — Serving Layer

| Service | Purpose | Read Latency |
|---|---|---|
| **Azure Synapse Analytics** | Serverless SQL for ad-hoc queries; dedicated pool for aggregated marts | Seconds |
| **Power BI** | 14 dashboards — DirectQuery to Synapse + Import mode for marts | Sub-second (import) |
| **Azure Cosmos DB** | Pre-loaded from Gold via ADF — serves fleet web/mobile APIs | <2 seconds (API SLA) |
| **Azure Functions** | Event-driven alerting (overheating, payment failures, SLA breach) | Near real-time |

**Row-level security in Power BI + Synapse:**
- Executive → all Australia data
- State Manager → own state/territory only
- Franchise Owner → own stations only

---

## Part 9 — Data Quality Framework

**At Bronze → Silver:**

| Check | What It Validates |
|---|---|
| Schema check | Columns match expected schema |
| Null check | Mandatory fields not null |
| Duplicate check | Dedup key uniqueness |
| Range check | kWh 0–500, temp -10°C–120°C, amount > 0 |
| Referential check | `charger_id` exists in charger_master |
| Freshness check | No batch data older than 24h |
| Business rule | Session `end_ts > start_ts` |

**At Silver → Gold:**

| Check | What It Validates |
|---|---|
| Reconciliation check | Expected vs billed amount |
| SCD2 continuity | No overlapping effective dates |
| Aggregation sense | Total revenue ≥ 0 per station/month |

**Bad records:** routed to `silver/quarantine/` with a `rejection_reason` column — never silently dropped.
**DQ metrics:** written to `gold/data_quality_audit/` — visible in a Power BI DQ dashboard.

---

## Part 10 — Security Architecture

| Area | What Is Done |
|---|---|
| **Identity** | Azure Active Directory / Entra ID for all identities |
| **Service auth** | Managed Identity for service-to-service (ADF → ADLS, Databricks → Key Vault) |
| **Secrets** | Azure Key Vault — API keys, DB connection strings, storage account keys |
| **RBAC on ADLS** | `bronze/crm/charge_cards_raw/` — restricted role; `silver/` — data engineer; `gold/` — analyst |
| **PII masking** | Customer email hashed in Silver/Gold (Australian Privacy Act 1988) |
| **PCI masking** | Card number → last 4 digits; CVV discarded at Bronze → Silver |
| **Encryption at rest** | ADLS Gen2 default encryption (Microsoft-managed keys) |
| **Encryption in transit** | HTTPS / TLS 1.2+ enforced everywhere |

---

## Part 11 — Orchestration & CI/CD

**Dependency chain per batch load:**
```
ADF ingest Bronze
    → ADF triggers Databricks Silver job
    → Silver complete → triggers Gold job
    → Gold complete → ADF copies Gold to Cosmos DB
```

**Databricks Jobs:**
- Bronze→Silver job (notebook per domain)
- Silver→Gold job
- Streaming job (always-on cluster per topic)
- Gold→Cosmos DB sync (hourly)

**CI/CD — Azure DevOps:**
- Repos: `/adf/`, `/databricks/`, `/synapse/`, `/infra/`
- Environments: Dev → QA → UAT → Prod
- PR gate: lint + unit tests + DQ checks → Build → Release

---

## Part 12 — Real-Time Alerting

```
Streaming Silver tables
    → Databricks threshold checks:
        overheating_flag = TRUE              → CRITICAL
        fault_code in [OVERTEMP, VOLTAGE_SPIKE, EMERGENCY_STOP]
        connector_status = OFFLINE > 15 min
        payment FAILED × 3 consecutive
        station utilisation_pct > 95%        → capacity alert
        sla_breach = TRUE                    → ops escalation
    → Azure Functions (event-driven trigger)
        → Email (Azure Communication Services)
        → SMS (Twilio / Azure)
        → Microsoft Teams webhook
```

---

## Part 13 — Monitoring & Observability

| What | Tool | Alert Condition |
|---|---|---|
| ADF pipeline failures | Azure Monitor + Log Analytics | Any pipeline status = Failed |
| Databricks job failures | Azure Monitor | Job exit code ≠ 0 |
| Streaming lag | Databricks metrics | Consumer lag > 5 min |
| Data freshness | Custom DQ audit table | No new rows in last 2 hours (batch) |
| Data volume anomaly | Azure Monitor | Row count < 80% of 7-day avg |
| Charger overheating | Databricks streaming alert | temp > 75°C |
| Payment failure spike | Databricks streaming alert | failure_rate > 10% in 5-min window |
| Storage cost | Azure Cost Management | ADLS spend > monthly budget |

---

## Part 14 — 14 Power BI Dashboard Views

| # | Dashboard | Primary Fact | Key KPIs |
|---|---|---|---|
| 1 | Network Expansion | DimStation | Total stations, active, pipeline, expansion by state |
| 2 | Geo Coverage | DimStation + DimCity | Stations per state/city, uncovered zones heat map |
| 3 | Energy Consumption | FactEnergyConsumption | kWh by geo/time, peak hours, growth trend |
| 4 | Revenue & Collections | FactPayments + FactInvoice | Gross revenue, net, GST, unpaid, refunds |
| 5 | Payment Method | FactPayments | Charge card %, wallet %, BPAY %, success rate |
| 6 | Charger Capacity | DimCharger + FactStationUtil. | AC/DC ratio, kW distribution, utilisation %, faults |
| 7 | Vehicle & Charging Pattern | FactChargingSession + DimVehicle | Vehicle mix, connector preference, avg session kWh |
| 8 | Live Charging | FactChargingSession | Active sessions, kWh delivered, ETA, availability |
| 9 | Complaints & Service | FactComplaints | Complaint count, SLA breach, fraud rate, resolution time |
| 10 | Staffing & Employees | DimEmployee | Employees per station, dept distribution |
| 11 | Invoice & Documents | FactInvoice | Paid, pending, overdue, PDF extraction rate |
| 12 | Franchise Performance | mart_franchise_performance | Revenue share, uptime, SLA score |
| 13 | Predictive Maintenance | FactMaintenance + FactDeviceTelemetry | Risk score, overheating alerts, MTTR |
| 14 | Sustainability | FactEnergyConsumption | CO2 saved, renewable %, energy efficiency |

**Drill-down hierarchy:**
```
Australia (national)
└── State/Territory (8)
    └── City (48)
        └── Station (120)
            └── Charger (300)
                └── Session (session-level detail)
```

---

## Part 15 — Azure Services Summary

| Service | Role in This Project |
|---|---|
| **Azure IoT Hub** | Receives OCPP telemetry from physical EV chargers |
| **Azure Event Hubs** | Message bus for all streaming topics (10 topics) |
| **Azure Data Factory** | Batch ingestion, REST API, XML/CSV copy, orchestration |
| **Azure Logic Apps** | Email invoice ingestion trigger |
| **Azure Blob Storage** | PDF invoice binary storage |
| **Azure AI Document Intelligence** | PDF invoice data extraction (fields + amounts) |
| **ADLS Gen2** | All lakehouse layers: Bronze / Silver / Gold |
| **Azure Databricks** | PySpark batch + Structured Streaming + Delta Lake writes |
| **Delta Lake** | ACID transactions, time travel, schema evolution, MERGE |
| **Azure Synapse Analytics** | Serverless + dedicated SQL pool serving layer |
| **Azure Cosmos DB** | Low-latency (<2 sec) reads for web/mobile APIs |
| **Azure Key Vault** | All secrets, API keys, connection strings |
| **Azure Active Directory / Entra ID** | Identity, RBAC, Managed Identity |
| **Azure Monitor + Log Analytics** | Pipeline, job, and infrastructure observability |
| **Azure Functions** | Event-driven alerting (overheating, payment failures) |
| **Azure Communication Services** | Email + SMS alert delivery |
| **Microsoft Teams Webhook** | Ops alerting channel |
| **Azure DevOps** | Repos, CI/CD pipelines, Dev → QA → UAT → Prod environments |
| **Power BI** | 14 dashboard views, row-level security |

---

## Quick Reference

```
Source Systems  : 28 sources (20 batch + 8 streaming)
Ingestion       : ADF (batch/REST/XML) + Event Hub (streaming) + Logic Apps (PDF)
Bronze          : Raw, append-only Delta, ~22 table groups, 4 audit columns added
Silver          : 25 tables, 10 transformation rules, quarantine for bad records
Gold            : 13 Dimensions + 9 Facts + 10 Marts → star schema
Serving         : Synapse → Power BI (14 views) + Cosmos DB → REST API (<2s)
Alerting        : Databricks thresholds → Azure Functions → Email/SMS/Teams
Security        : Key Vault + Managed Identity + RBAC + PII masking + PCI masking
```
