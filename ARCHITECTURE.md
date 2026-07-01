# VoltGrid AU — Azure Data Engineering Architecture
### Australia EV Charging Franchise Intelligence Platform
> Scale: ~50K records | Medallion Lakehouse | Bronze → Silver → Gold | Azure Native

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [High-Level Architecture Diagram](#2-high-level-architecture-diagram)
3. [Source Systems](#3-source-systems)
4. [Ingestion Layer](#4-ingestion-layer)
5. [Bronze Layer — Raw Landing](#5-bronze-layer--raw-landing)
6. [Silver Layer — Cleansed & Standardised](#6-silver-layer--cleansed--standardised)
7. [Gold Layer — Business Curated](#7-gold-layer--business-curated)
8. [Serving Layer](#8-serving-layer)
9. [Real-Time Alerting](#9-real-time-alerting)
10. [Data Quality Framework](#10-data-quality-framework)
11. [Security & Compliance](#11-security--compliance)
12. [Orchestration & CI/CD](#12-orchestration--cicd)
13. [Monitoring & Observability](#13-monitoring--observability)
14. [Star Schema — Gold Data Model](#14-star-schema--gold-data-model)
15. [Scenario Coverage by Layer](#15-scenario-coverage-by-layer)
16. [Dashboard Drill-Down Hierarchy](#16-dashboard-drill-down-hierarchy)
17. [Azure Services Summary](#17-azure-services-summary)

---

## 1. Project Overview

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

**Business problems solved:**

- No centralised visibility across franchise stations
- Revenue leakage from billing mismatches
- No real-time charger health monitoring
- No predictive maintenance
- Invoice tracking scattered across email and PDFs
- No geo-drill from Australia → State → City → Station → Charger level

---

## 2. High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SOURCE SYSTEMS                                      │
│                                                                              │
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
│                                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ Azure IoT Hub│  │  Azure Data  │  │  Azure Data  │  │ Logic Apps + │   │
│  │     +        │  │  Factory     │  │  Factory     │  │   Blob +     │   │
│  │ Event Hubs   │  │  (CDC/Copy)  │  │  (REST conn.)│  │ Doc Intel.   │   │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │
└─────────┼─────────────────┼─────────────────┼─────────────────┼───────────┘
          │                 │                 │                 │
          └─────────────────┴─────────────────┴─────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ADLS Gen2  —  BRONZE LAYER (Raw)                         │
│                                                                              │
│  container: bronze/                                                          │
│  ├── iot/charger_telemetry_raw/          (streaming JSON events)            │
│  ├── sessions/charging_sessions_raw/     (CSV batch)                        │
│  ├── payments/payment_gateway_raw/       (API JSON)                         │
│  ├── crm/customers_raw/                  (CSV batch)                        │
│  ├── crm/complaints_raw/                 (CSV batch)                        │
│  ├── crm/charge_cards_raw/               (CSV)                              │
│  ├── fleet/fleet_usage_raw/              (API JSON)                         │
│  ├── weather/weather_observations_raw/   (API JSON)                         │
│  ├── erp/invoices_raw/                   (CSV + PDF metadata)               │
│  ├── erp/tariffs_raw/                    (CSV SCD2)                         │
│  ├── erp/maintenance_raw/                (CSV)                              │
│  ├── xml/grid_power_status_raw/          (XML)                              │
│  ├── xml/support_tickets_raw/            (XML)                              │
│  ├── xml/station_audit_logs_raw/         (XML)                              │
│  ├── streaming/connector_status_raw/     (streaming JSON)                   │
│  ├── streaming/charger_faults_raw/       (streaming JSON)                   │
│  ├── streaming/live_payments_raw/        (streaming JSON)                   │
│  ├── streaming/rfid_scan_raw/            (streaming JSON)                   │
│  ├── streaming/station_utilisation_raw/  (streaming JSON)                   │
│  ├── streaming/weather_alerts_raw/       (streaming JSON)                   │
│  ├── streaming/fleet_live_trip_raw/      (streaming JSON)                   │
│  └── invoices/pdf_raw/                   (PDF binary + extracted JSON)      │
│                                                                              │
│  Format: Delta Lake (append-only, no transforms)                            │
│  Retention: 90 days raw, audit trail preserved                              │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Azure Databricks   │
                    │  (PySpark Jobs +    │
                    │  Structured Stream) │
                    └──────────┬──────────┘
                               │
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ADLS Gen2  —  SILVER LAYER (Cleansed)                    │
│                                                                              │
│  container: silver/                                                          │
│  ├── sl_iot_charger_telemetry       dedup + fault mapping + overtemp flag  │
│  ├── sl_charging_sessions           UTC normalised, duration validated      │
│  ├── sl_payments                    flattened, status normalised            │
│  ├── sl_customers                   PII masked, loyalty tier standardised   │
│  ├── sl_complaints                  SLA breach flag derived                 │
│  ├── sl_charge_cards                PAN masked (last 4), CVV discarded      │
│  ├── sl_fleet_usage                 flattened, metrics standardised         │
│  ├── sl_weather                     humidity/temp range validated           │
│  ├── sl_invoices                    status normalised, GST split            │
│  ├── sl_tariffs_scd2                SCD2 effective_from / effective_to      │
│  ├── sl_maintenance_events          root cause standardised                 │
│  ├── sl_grid_power_status           XML parsed, outage_flag bool            │
│  ├── sl_station_audit_logs          XML parsed, action categorised          │
│  ├── sl_station_utilisation         streaming dedup + hourly rollup         │
│  ├── sl_connector_status            streaming, status enum validated        │
│  ├── sl_charger_faults              fault_code → description mapped         │
│  ├── sl_rfid_scan_events            auth_status standardised                │
│  ├── sl_live_payments               streaming status normalised             │
│  └── sl_fleet_live_trips            speed/battery range validated           │
│                                                                              │
│  Format: Delta Lake (MERGE / upsert idempotent)                             │
│  Bad records → silver/quarantine/ with rejection reason                     │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Azure Databricks   │
                    │  (Gold aggregation  │
                    │   PySpark / DLT)    │
                    └──────────┬──────────┘
                               │
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ADLS Gen2  —  GOLD LAYER (Business Curated)              │
│                                                                              │
│  DIMENSIONS                                                                  │
│  ├── DimCountry            Australia only (extensible)                      │
│  ├── DimState              NSW, VIC, QLD, WA, SA, TAS, ACT, NT             │
│  ├── DimCity               48 cities across states                          │
│  ├── DimStation            120 franchise stations                           │
│  ├── DimCharger            300 chargers (AC/DC, kW, connector type)        │
│  ├── DimCustomer           SCD2 — loyalty tier, subscription changes       │
│  ├── DimVehicle            model, VIN, battery type                        │
│  ├── DimEmployee           department, station assignment                   │
│  ├── DimFranchisePartner   partner name, status, revenue share             │
│  ├── DimChargeCard         masked PAN, network, card_type, last4           │
│  ├── DimTariff             SCD2 — rate_per_kwh, peak/offpeak, effective    │
│  ├── DimWeather            city, temp, humidity, condition                 │
│  └── DimTime               date, hour, day_of_week, AEST/AEDT aware       │
│                                                                              │
│  FACTS                                                                       │
│  ├── FactChargingSession   session-level grain (energy, duration, revenue) │
│  ├── FactEnergyConsumption kWh aggregated by charger + time                │
│  ├── FactPayments          payment-level grain (amount, GST, status)       │
│  ├── FactMaintenance       event-level (fault, MTTR, root cause)           │
│  ├── FactFleetUtilisation  vehicle-level (distance, trips, battery)        │
│  ├── FactDeviceTelemetry   charger telemetry (voltage, current, temp)      │
│  ├── FactComplaints        ticket-level (category, SLA, priority)          │
│  ├── FactStationUtilisation hourly utilisation % per station               │
│  └── FactInvoice           invoice-level (amount, GST, status, pdf ref)   │
│                                                                              │
│  AGGREGATED MARTS (pre-built for Power BI performance)                      │
│  ├── mart_revenue_by_state_month                                            │
│  ├── mart_charger_uptime_daily                                              │
│  ├── mart_billing_reconciliation                                            │
│  ├── mart_complaint_sla_summary                                             │
│  ├── mart_fleet_efficiency                                                  │
│  └── mart_predictive_maintenance_risk                                       │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  Azure Synapse  │  │   Cosmos DB     │  │ Azure Functions │
│  Analytics      │  │  (low-latency)  │  │  (Alerting)     │
│  (SQL Pool)     │  │                 │  │                 │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   Power BI      │  │  Fleet Web /    │  │  Email / SMS /  │
│  Dashboards     │  │  Mobile APIs    │  │  Teams Alerts   │
│  (14 views)     │  │  (<2 sec SLA)   │  │                 │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

---

## 3. Source Systems

| # | Source | Format | Ingestion Type | Frequency |
|---|---|---|---|---|
| 1 | IoT Charger Telemetry (OCPP) | JSON stream | Event Hub → Databricks Streaming | Near real-time (1–3 sec) |
| 2 | Charging Sessions DB | CSV | ADF Copy (batch) | Daily |
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

---

## 4. Ingestion Layer

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         INGESTION PATTERNS                                   │
│                                                                              │
│  STREAMING PATH                                                              │
│  IoT Chargers (OCPP) ──► Azure IoT Hub ──► Azure Event Hubs                │
│                                                 │                           │
│                              Databricks Structured Streaming                │
│                              - checkpointing per topic                      │
│                              - exactly-once delivery                        │
│                              - 10-min watermark for late events             │
│                              - bad records → bronze/quarantine              │
│                                                 │                           │
│                                           Bronze Delta                      │
│                                                                              │
│  BATCH PATH (ADF parameterised pipelines)                                   │
│  CSV / XML files ──► ADF Copy Activity ──► ADLS Bronze                     │
│  REST APIs        ──► ADF REST Connector ──► ADLS Bronze (JSON)            │
│  Watermark column: updated_at / event_date for incremental loads            │
│  High-watermark table tracked in Azure SQL (pipeline_watermark)            │
│                                                                              │
│  PDF INVOICE PATH                                                            │
│  Email attachment ──► Logic Apps ──► Blob Storage ──►                       │
│  Azure AI Document Intelligence (extraction) ──► ADLS Bronze JSON           │
│                                                                              │
│  CDC PATH (future / RDBMS)                                                  │
│  ADF CDC connector or Debezium → Event Hub → Bronze                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Engineering patterns applied:**

| Pattern | Where Used |
|---|---|
| High-watermark incremental load | All CSV batch sources |
| Exactly-once streaming | Event Hub + Databricks checkpointing |
| Idempotent MERGE (upsert) | Silver layer Delta writes |
| Schema evolution (Auto Loader) | Bronze landing via Databricks Auto Loader |
| Retry + exponential backoff | ADF REST API pipelines |
| Quarantine routing | Corrupt / schema-violating records |
| SCD Type 2 | Tariff, customer subscription, charger firmware |

---

## 5. Bronze Layer — Raw Landing

**Purpose:** Immutable raw replica of every source record. No transformations. Supports audit, replay, and reprocessing.

```
bronze/
├── iot/
│   ├── charger_telemetry_raw/        partitioned by event_date / charger_id
│   ├── charger_fault_raw/            partitioned by event_date
│   ├── connector_status_raw/         partitioned by event_date
│   ├── rfid_scan_raw/                partitioned by event_date
│   └── station_utilisation_raw/      partitioned by event_date
├── sessions/
│   ├── charging_sessions_raw/        partitioned by plug_in_date
│   └── session_events_raw/           partitioned by event_date
├── payments/
│   ├── payment_transactions_raw/     partitioned by load_date
│   └── live_payment_status_raw/      partitioned by event_date
├── crm/
│   ├── customers_raw/                full snapshot per load
│   ├── complaints_raw/               partitioned by load_date
│   └── charge_cards_raw/             restricted access zone (RBAC)
├── fleet/
│   ├── fleet_usage_raw/              partitioned by load_date
│   └── fleet_live_trip_raw/          partitioned by event_date
├── weather/
│   ├── weather_api_raw/              partitioned by load_date
│   └── weather_alert_raw/            partitioned by event_date
├── erp/
│   ├── invoices_raw/                 partitioned by invoice_date
│   ├── tariffs_raw/                  full snapshot
│   └── maintenance_events_raw/       partitioned by event_date
├── xml/
│   ├── support_tickets_raw/          parsed from XML, stored as JSON
│   ├── grid_power_status_raw/        parsed from XML
│   └── station_audit_logs_raw/       parsed from XML
├── master/
│   ├── station_master_raw/
│   ├── charger_master_raw/
│   ├── vehicle_master_raw/
│   ├── employee_master_raw/
│   ├── franchise_partners_raw/
│   ├── australia_cities_raw/
│   └── australia_states_raw/
└── invoices/
    ├── pdf_binary/                   raw PDF blobs
    └── pdf_extracted_json/           Document Intelligence output
```

**Added columns at Bronze (all tables):**

| Column | Value |
|---|---|
| `_ingestion_ts` | UTC timestamp when record landed |
| `_source_file` | Source filename or API endpoint |
| `_pipeline_run_id` | ADF / Databricks run ID |
| `_is_corrupt` | Boolean flag for quarantined records |

---

## 6. Silver Layer — Cleansed & Standardised

**Purpose:** Business-friendly, validated, deduplicated data. Single source of truth for all Gold aggregations.

### Transformation Rules Applied

```
┌──────────────────────────────────────────────────────────────────────────┐
│  RULE A — Duplicate Telemetry Removal                                    │
│  Dedup key: charger_id + event_ts + connector_id                         │
│  Keep: latest ingestion_ts when duplicates found                         │
│  Applied to: sl_iot_charger_telemetry, sl_session_events                 │
├──────────────────────────────────────────────────────────────────────────┤
│  RULE B — Late & Out-of-Order Event Handling                             │
│  Watermark: 10-minute window on event_ts                                 │
│  Late records beyond watermark → silver/quarantine/late_events/          │
│  Applied to: all streaming silver tables                                 │
├──────────────────────────────────────────────────────────────────────────┤
│  RULE C — Fault Code Mapping                                             │
│  OVERTEMP        → "Overheating Risk"                                    │
│  VOLTAGE_SPIKE   → "Voltage Abnormality"                                 │
│  EMERGENCY_STOP  → "Emergency Stop Triggered"                            │
│  CONN_FAULT      → "Connector Fault"                                     │
│  FIRMWARE_ERR    → "Firmware Error"                                      │
│  NONE            → "Healthy"                                             │
│  Applied to: sl_iot_charger_telemetry, sl_charger_faults                 │
├──────────────────────────────────────────────────────────────────────────┤
│  RULE D — Billing Reconciliation                                         │
│  expected_amount = energy_kwh × tariff_rate_per_kwh                      │
│  Compare with: payment amount (gateway) and invoice amount (ERP)         │
│  Result: MATCH / MISMATCH + difference_amount                            │
│  Applied to: sl_invoices joined with sl_payments + sl_tariffs_scd2       │
├──────────────────────────────────────────────────────────────────────────┤
│  RULE E — Temperature Risk Flag                                          │
│  If temperature > 75°C → overheating_flag = TRUE                         │
│  Triggers maintenance event creation in FactMaintenance                  │
│  Applied to: sl_iot_charger_telemetry                                    │
├──────────────────────────────────────────────────────────────────────────┤
│  RULE F — Charge Card PCI Masking                                        │
│  card_number → keep only last 4 digits (************4242)                │
│  CVV → discarded, store only cvv_masked = "***"                          │
│  card_token used for all joins downstream                                │
│  Applied to: sl_charge_cards                                             │
├──────────────────────────────────────────────────────────────────────────┤
│  RULE G — Timezone Normalisation                                         │
│  All timestamps converted to UTC for storage                             │
│  AEST (UTC+10) and AEDT (UTC+11) handled correctly                       │
│  DimTime carries AEST-local equivalents for reporting                    │
│  Applied to: all silver tables with timestamp columns                    │
├──────────────────────────────────────────────────────────────────────────┤
│  RULE H — SCD Type 2 (Slowly Changing Dimensions)                       │
│  Tracks history on: tariff rates, customer loyalty tier,                 │
│  charger firmware version, subscription plan                             │
│  Columns added: effective_from, effective_to, is_current                 │
│  Applied to: sl_tariffs_scd2                                             │
├──────────────────────────────────────────────────────────────────────────┤
│  RULE I — Complaint SLA Breach Flag                                      │
│  If resolution_ts > sla_due_ts → sla_breach = TRUE                       │
│  P1 SLA: 4 hours, P2: 8 hours, P3: 24 hours                             │
│  Applied to: sl_complaints                                               │
├──────────────────────────────────────────────────────────────────────────┤
│  RULE J — GST Split                                                      │
│  GST rate: 10% (Australia)                                               │
│  gross_amount = net_amount + gst_amount                                  │
│  Applied to: sl_invoices, sl_payments                                    │
└──────────────────────────────────────────────────────────────────────────┘
```

### Silver Tables

| Table | Source | Key Transformations |
|---|---|---|
| `sl_iot_charger_telemetry` | IoT stream | Dedup, fault mapping, overtemp flag, watermark |
| `sl_charging_sessions` | Sessions CSV | UTC normalise, duration validate, status normalise |
| `sl_payments` | Payment JSON API | Flatten, GST split, status normalise |
| `sl_customers` | CRM CSV | PII masking (email hash), loyalty tier standardise |
| `sl_complaints` | Complaints CSV | SLA breach flag, priority standardise |
| `sl_charge_cards` | Cards CSV | PAN masking, CVV discard, token standardise |
| `sl_fleet_usage` | Fleet JSON API | Flatten data array, metrics standardise |
| `sl_weather` | Weather JSON API | Flatten, humidity/temp range validate |
| `sl_invoices` | Invoice CSV + PDF JSON | GST split, status normalise, pdf_extracted merge |
| `sl_tariffs_scd2` | Tariff CSV | SCD2 effective_from/to, is_current flag |
| `sl_maintenance_events` | Maintenance CSV | Root cause standardise, MTTR validate |
| `sl_grid_power_status` | Grid XML | XML parse, outage_flag boolean |
| `sl_station_audit_logs` | Audit XML | XML parse, action categorise |
| `sl_support_tickets` | Tickets XML | XML parse, priority/category standardise |
| `sl_station_utilisation` | Stream JSON | Hourly dedup rollup, pct range validate |
| `sl_connector_status` | Stream JSON | Status enum validate, offline duration calc |
| `sl_charger_faults` | Stream JSON | Fault code → description, severity rank |
| `sl_rfid_scan_events` | Stream JSON | Auth status standardise |
| `sl_live_payments` | Stream JSON | Status normalise, amount validate |
| `sl_fleet_live_trips` | Stream JSON | Speed/battery range validate |
| `sl_station_master` | Station CSV | Status standardise, lat/lng validate |
| `sl_charger_master` | Charger CSV | kW range validate, connector type standardise |
| `sl_vehicle_master` | Vehicle CSV | Model/make standardise |
| `sl_employee_master` | Employee CSV | Department standardise |
| `sl_franchise_partners` | Partners CSV | Status standardise |

---

## 7. Gold Layer — Business Curated

**Purpose:** Aggregated, modelled, reporting-ready tables. Serves Power BI and APIs directly.

### Dimensions

| Dimension | Grain | SCD | Key Fields |
|---|---|---|---|
| `DimCountry` | Country | Type 1 | country_key, country_name, country_code |
| `DimState` | AU State/Territory | Type 1 | state_key, state_code, state_name |
| `DimCity` | City | Type 1 | city_key, city_name, state_key, postcode |
| `DimStation` | Station | Type 1 | station_key, station_id, city_key, site_type, status, lat, lng |
| `DimCharger` | Charger | Type 2 | charger_key, charger_id, station_key, connector_type, max_kw, firmware_version |
| `DimCustomer` | Customer | Type 2 | customer_key, customer_id, loyalty_tier, signup_date |
| `DimVehicle` | Vehicle | Type 1 | vehicle_key, vehicle_id, customer_key, model, vin |
| `DimEmployee` | Employee | Type 1 | employee_key, employee_id, department, station_key |
| `DimFranchisePartner` | Partner | Type 1 | partner_key, partner_id, partner_name, status |
| `DimChargeCard` | Card | Type 1 | card_key, card_id, customer_key, rfid_uid, card_number_masked, status |
| `DimTariff` | Tariff version | Type 2 | tariff_key, tariff_id, rate_per_kwh, peak_offpeak, effective_from, effective_to, is_current |
| `DimWeather` | City + observation | Type 1 | weather_key, city_key, temperature_c, humidity_pct, condition |
| `DimTime` | Hour | — | time_key, date, year, month, day, hour, day_of_week, is_weekend, financial_year_au |

### Facts

| Fact Table | Grain | Linked Dimensions | Key Metrics |
|---|---|---|---|
| `FactChargingSession` | One row per session | DimCustomer, DimStation, DimCharger, DimVehicle, DimTariff, DimTime | energy_kwh, duration_min, session_status, expected_amount, billed_amount, reconciliation_status |
| `FactEnergyConsumption` | Charger + hour | DimCharger, DimStation, DimTime | total_kwh, session_count, peak_kw |
| `FactPayments` | One row per payment | DimCustomer, DimChargeCard, DimStation, DimTime | amount_aud, gst, gateway, status, refund_flag |
| `FactMaintenance` | One row per event | DimCharger, DimStation, DimEmployee, DimTime | root_cause, mttr_hours, overheating_flag, fault_description |
| `FactFleetUtilisation` | Vehicle + day | DimVehicle, DimCustomer, DimTime | distance_km, trip_count, battery_pct_start, battery_pct_end |
| `FactDeviceTelemetry` | Charger + event | DimCharger, DimStation, DimWeather, DimTime | voltage, current, temperature, power_kw, fault_code, overheating_flag |
| `FactComplaints` | One row per ticket | DimCustomer, DimStation, DimTime | category, priority, sla_breach, resolution_days |
| `FactStationUtilisation` | Station + hour | DimStation, DimTime | active_chargers, utilisation_pct, sessions_count |
| `FactInvoice` | One row per invoice | DimCustomer, DimStation, DimTime | amount_aud, gst_amount, status, pdf_name, pdf_extracted |

### Aggregated Marts (pre-computed for dashboard speed)

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

---

## 8. Serving Layer

```
┌─────────────────────────────────────────────────────────────────┐
│                      SERVING LAYER                               │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Azure Synapse Analytics                     │   │
│  │  - External tables pointing to Gold Delta on ADLS        │   │
│  │  - Serverless SQL pool for ad-hoc queries                │   │
│  │  - Dedicated SQL pool for pre-aggregated marts           │   │
│  │  - Row-level security by franchise / state               │   │
│  └───────────────────────┬──────────────────────────────────┘   │
│                           │                                      │
│                           ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                  Power BI                                │   │
│  │  DirectQuery to Synapse for live data                    │   │
│  │  Import mode for aggregated mart tables                  │   │
│  │  14 dashboard views (see Section 15)                     │   │
│  │  Row-level security: Executive / State Mgr / Franchise   │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Azure Cosmos DB                             │   │
│  │  Pre-loaded from Gold aggregates (ADF copy)              │   │
│  │  Collections: live_station_status, session_live,         │   │
│  │               charger_availability, customer_history     │   │
│  │  Read latency: <2 seconds (API SLA)                      │   │
│  └───────────────────────┬──────────────────────────────────┘   │
│                           │                                      │
│                           ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │         Fleet Web Portal & Mobile App APIs               │   │
│  │  - Live charger availability                             │   │
│  │  - Active session progress (kWh, ETA)                    │   │
│  │  - Charging history                                      │   │
│  │  - Invoice download                                      │   │
│  │  - Station finder with availability                      │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 9. Real-Time Alerting

```
Streaming Silver tables
       │
       ▼
Databricks alert rules (threshold checks)
       │
       ├── overheating_flag = TRUE          → CRITICAL alert
       ├── fault_code in ['OVERTEMP','VOLTAGE_SPIKE','EMERGENCY_STOP']
       ├── connector_status = 'OFFLINE' > 15 min
       ├── payment_status = 'FAILED' × 3 consecutive
       ├── station utilisation_pct > 95%   → capacity alert
       └── sla_breach = TRUE               → ops escalation
       │
       ▼
Azure Functions (event-driven trigger)
       │
       ├── Email notification (Azure Communication Services)
       ├── SMS (Twilio / Azure)
       └── Microsoft Teams channel webhook
```

---

## 10. Data Quality Framework

```
┌─────────────────────────────────────────────────────────────────┐
│                   DATA QUALITY CHECKS                            │
│                                                                  │
│  At Bronze → Silver transition:                                  │
│  ├── Schema check      columns match expected schema            │
│  ├── Null check        mandatory fields not null                 │
│  ├── Duplicate check   dedup key uniqueness                     │
│  ├── Range check       kwh 0–500, temp -10°C–120°C, amount >0  │
│  ├── Referential check charger_id exists in charger_master      │
│  ├── Freshness check   no data older than 24h in batch sources  │
│  └── Business rule     session end_ts > start_ts                │
│                                                                  │
│  At Silver → Gold transition:                                    │
│  ├── Reconciliation check  expected vs billed amount            │
│  ├── SCD2 continuity       no overlapping effective dates       │
│  └── Aggregation sense     total revenue ≥ 0 per station/month  │
│                                                                  │
│  Bad records → silver/quarantine/ (with rejection reason col)   │
│  DQ metrics written to → gold/data_quality_audit/ table         │
│  Power BI DQ dashboard reads from audit table                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 11. Security & Compliance

```
┌─────────────────────────────────────────────────────────────────┐
│                    SECURITY ARCHITECTURE                         │
│                                                                  │
│  Identity & Access                                               │
│  ├── Azure Active Directory / Entra ID for all identities        │
│  ├── Managed Identity for service-to-service auth               │
│  │   (ADF → ADLS, Databricks → Key Vault, etc.)                │
│  └── Azure RBAC roles on ADLS containers                        │
│      ├── bronze/crm/charge_cards_raw/ → restricted role only    │
│      ├── silver/ → data engineer role                           │
│      └── gold/ → analyst role                                   │
│                                                                  │
│  Secrets Management                                              │
│  └── Azure Key Vault                                             │
│      ├── API keys (payment gateway, weather, fleet)             │
│      ├── DB connection strings                                   │
│      └── Storage account keys                                   │
│                                                                  │
│  Data Privacy (Australian Privacy Act 1988)                      │
│  ├── PII masking: customer email hashed in Silver/Gold          │
│  ├── PAN masking: card number → last 4 only                     │
│  ├── CVV: discarded at Bronze → Silver transformation           │
│  ├── card_token used as stable join key (no raw PAN)            │
│  └── Audit logging: all reads of restricted Bronze zone         │
│                                                                  │
│  Encryption                                                      │
│  ├── At rest: ADLS Gen2 default encryption (Microsoft-managed)  │
│  └── In transit: HTTPS / TLS 1.2+ enforced everywhere          │
│                                                                  │
│  Row-Level Security (Power BI + Synapse)                         │
│  ├── Executive: all Australia data                               │
│  ├── State Manager: own state/territory only                    │
│  └── Franchise Owner: own stations only                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 12. Orchestration & CI/CD

```
┌─────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION                                  │
│                                                                  │
│  Azure Data Factory                                              │
│  ├── Parameterised pipelines (source, watermark, target)        │
│  ├── Trigger types: schedule, tumbling window, event            │
│  ├── Watermark table in Azure SQL tracks last load position     │
│  └── Pipeline audit table: run_id, status, rows, duration       │
│                                                                  │
│  Databricks Jobs                                                 │
│  ├── Bronze→Silver job (notebook per domain)                    │
│  ├── Silver→Gold job (DLT pipeline or notebook cluster)         │
│  ├── Streaming job (always-on cluster per topic)                │
│  └── Gold→Cosmos DB sync job (hourly)                           │
│                                                                  │
│  Dependency chain per batch load:                               │
│  ADF ingest → ADF trigger Databricks Silver job →              │
│  Silver complete → trigger Gold job → Gold complete →           │
│  ADF copy Gold delta to Cosmos DB                               │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    CI/CD — Azure DevOps                          │
│                                                                  │
│  Repos                                                           │
│  ├── /adf/          ADF ARM templates (pipelines, datasets)     │
│  ├── /databricks/   Notebooks + DLT pipeline definitions        │
│  ├── /synapse/      SQL scripts and views                       │
│  └── /infra/        Bicep / Terraform for Azure resources       │
│                                                                  │
│  Environments: Dev → QA → UAT → Prod                            │
│                                                                  │
│  Pipelines                                                       │
│  ├── PR gate: lint + unit tests + DQ checks                     │
│  ├── Build: package ADF / Databricks artefacts                  │
│  └── Release: parameterised deploy per environment              │
└─────────────────────────────────────────────────────────────────┘
```

---

## 13. Monitoring & Observability

| What | Tool | Alert Condition |
|---|---|---|
| ADF pipeline failures | Azure Monitor + Log Analytics | Any pipeline run status = Failed |
| Databricks job failures | Azure Monitor | Job exit code ≠ 0 |
| Streaming lag | Databricks metrics | Consumer lag > 5 min |
| Data freshness | Custom DQ audit table | No new rows in last 2 hours (batch) |
| Data volume anomaly | Azure Monitor | Row count < 80% of 7-day avg |
| Charger overheating | Databricks streaming alert | temp > 75°C |
| Payment failure spike | Databricks streaming alert | failure_rate > 10% in 5-min window |
| Pipeline SLA breach | Logic App timer check | Gold tables not updated by agreed SLA time |
| Storage cost | Azure Cost Management | ADLS spend > monthly budget |
| Query performance | Synapse / Power BI | Query > 30 sec duration |

---

## 14. Star Schema — Gold Data Model

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
         │          │            │            │          │
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

  Geo hierarchy: DimCountry ─► DimState ─► DimCity ─► DimStation ─► DimCharger
```

---

## 15. Scenario Coverage by Layer

### Bronze Layer Scenarios

| # | Scenario | Source | How Handled |
|---|---|---|---|
| B1 | Raw IoT telemetry preserved for replay | charger_telemetry_stream.json | Append-only Delta, never modified |
| B2 | Corrupt / malformed JSON record arrived | Any streaming source | `_is_corrupt = TRUE`, routed to bronze/quarantine |
| B3 | XML source with non-standard encoding | support_tickets.xml, audit_logs.xml | ADF XML connector handles encoding, stored as raw JSON |
| B4 | PDF invoice received via email | Logic Apps trigger | PDF binary → Blob, extracted JSON → bronze/invoices/pdf_extracted_json |
| B5 | API returned unexpected schema (schema drift) | Payment API, Fleet API | Auto Loader schema evolution captures new columns |
| B6 | Duplicate event from IoT device (resend) | charger_telemetry_stream | Stored as-is in Bronze; dedup happens at Silver |
| B7 | Late-arriving batch file (yesterday's data today) | Any CSV batch source | Partitioned by source date, not load date, so correct partition |
| B8 | Grid power XML only has 8 regions | grid_power_status.xml | All 8 AU states/territories parsed and stored |
| B9 | Restricted card data must be isolated | charge_cards_raw | Separate ADLS path, RBAC reader-restricted role |
| B10 | Pipeline audit trail needed | All sources | `_ingestion_ts`, `_source_file`, `_pipeline_run_id` added |

### Silver Layer Scenarios

| # | Scenario | Rule | How Handled |
|---|---|---|---|
| S1 | Duplicate telemetry event from same charger | Rule A | Dedup on charger_id + event_ts + connector_id, keep latest |
| S2 | Event arrived 12 min late (beyond watermark) | Rule B | Routed to silver/quarantine/late_events/ |
| S3 | fault_code = "OVERTEMP" in telemetry | Rule C | Mapped to "Overheating Risk", overheating_flag = TRUE |
| S4 | Charger temperature = 82°C | Rule E | overheating_flag = TRUE, FactMaintenance row triggered |
| S5 | Raw card number in Bronze | Rule F | PAN masked in Silver, CVV discarded, card_token retained |
| S6 | Session timestamp in AEST, not UTC | Rule G | Converted to UTC on write, AEST retained in DimTime |
| S7 | Tariff rate changed mid-month | Rule H | SCD2 closes old row (effective_to), opens new row (effective_from) |
| S8 | P1 complaint unresolved after 4 hours | Rule I | sla_breach = TRUE derived |
| S9 | Payment amount = 55.89, GST separate | Rule J | gst_amount computed, net_amount derived |
| S10 | Null station_id in charging session | DQ | Record flagged, routed to quarantine with rejection reason |
| S11 | Station utilisation stream: station seen twice in same minute | Rule A variant | Dedup on station_id + event_ts, keep latest |
| S12 | connector_status = OFFLINE for 20 min | Offline duration calc | sl_connector_status: offline_duration_min derived |
| S13 | Weather alert stream: temperature = -5°C (invalid for AU summer) | Range check | Flagged to quarantine |
| S14 | RFID card scan DENIED | Auth status | auth_status standardised, denial count aggregated |
| S15 | Invoice PDF extraction failed | Doc Intelligence | pdf_extracted = FALSE, manual review flag set |

### Gold Layer Scenarios

| # | Scenario | Dashboard | How Resolved |
|---|---|---|---|
| G1 | Billing mismatch: session kWh × tariff ≠ payment amount | Revenue / Finance | `mart_billing_reconciliation` — MISMATCH flag + difference_amount |
| G2 | Drill-down: Australia → NSW → Sydney → Station 101 | All dashboards | Geo hierarchy via DimCountry→DimState→DimCity→DimStation |
| G3 | SCD2 tariff: rate changed, old sessions should use old rate | FactChargingSession | Joined to DimTariff using session_date between effective_from and effective_to |
| G4 | Franchise owner can only see their own stations | All dashboards | Row-level security in Power BI: partner_key filter per user |
| G5 | Live charging session shown in mobile app | Live Charging | FactChargingSession + Cosmos DB sync < 2 min, API latency <2 sec |
| G6 | Charger offline complaint spike this week | Complaints / Ops | FactComplaints × FactMaintenance correlated by charger_key + date |
| G7 | Revenue by payment method (charge card vs wallet) | Revenue | FactPayments grouped by DimChargeCard.card_type |
| G8 | Predictive maintenance: 3 overheating events this week | Maintenance | mart_predictive_maintenance_risk: overheating_count, failure_probability_score |
| G9 | CO2 saved calculation for ESG reporting | Sustainability | FactEnergyConsumption: kwh × 0.7 kg CO2/kWh Australian grid factor |
| G10 | Underperforming franchise partner | Franchise | mart_franchise_performance: revenue, uptime, complaints per partner |
| G11 | Invoice unpaid for 30 days | Finance | FactInvoice: days_outstanding derived, overdue_flag = TRUE |
| G12 | Peak hour demand: 5pm–8pm weekdays | Energy | FactEnergyConsumption grouped by DimTime.hour, filtered is_weekend = FALSE |
| G13 | New station location recommendation | Expansion | mart_geo_coverage: uncovered demand zones (low station density, high sessions nearby) |
| G14 | Vehicle type mix to plan next charger install | Capacity | FactChargingSession grouped by DimVehicle.model → connector_type preference |
| G15 | Fast charger utilisation vs slow charger | Capacity | FactEnergyConsumption joined DimCharger.max_kw band |

---

## 16. Dashboard Drill-Down Hierarchy

```
AUSTRALIA (national view)
│
├── By State/Territory: NSW · VIC · QLD · WA · SA · TAS · ACT · NT
│        │
│        ├── By City (48 cities)
│        │        │
│        │        ├── By Station (120 stations)
│        │        │        │
│        │        │        ├── By Charger (300 chargers)
│        │        │        │        │
│        │        │        │        └── By Session (session-level detail)
│        │        │        │
│        │        │        └── Station KPIs:
│        │        │             revenue, uptime, complaints, employees, invoices
│        │        │
│        │        └── City KPIs: stations, sessions, revenue, complaints
│        │
│        └── State KPIs: total stations, energy consumed, revenue, franchise count
│
└── National KPIs: total network, AU revenue, CO2 saved, billing leakage

Each level filterable by:
  Date range | Franchise partner | Charger type | Vehicle type |
  Payment mode | Complaint category | Session status | Customer segment
```

**14 Dashboard Views:**

| # | Dashboard | Primary Fact | Key KPIs |
|---|---|---|---|
| 1 | Network Expansion | DimStation | Total stations, active, pipeline, expansion by state |
| 2 | Geo Coverage | DimStation + DimCity | Stations per state/city, uncovered zones heat map |
| 3 | Energy Consumption | FactEnergyConsumption | kWh by geo/time, peak hours, growth trend |
| 4 | Revenue & Collections | FactPayments + FactInvoice | Gross revenue, net, GST, unpaid, refunds |
| 5 | Payment Method | FactPayments | Charge card %, card %, wallet %, BPAY %, success rate |
| 6 | Charger Capacity | DimCharger + FactStationUtil. | AC/DC ratio, kW distribution, utilisation %, faults |
| 7 | Vehicle & Charging Pattern | FactChargingSession + DimVehicle | Vehicle mix, connector preference, avg session kWh |
| 8 | Live Charging | FactChargingSession (near-real) | Active sessions, kWh delivered, ETA, availability |
| 9 | Complaints & Service | FactComplaints | Complaint count, SLA breach, fraud rate, resolution time |
| 10 | Staffing & Employees | DimEmployee | Employees per station, dept distribution, shift coverage |
| 11 | Invoice & Documents | FactInvoice | Paid, pending, overdue invoices, PDF extraction rate |
| 12 | Franchise Performance | mart_franchise_performance | Revenue share, uptime, SLA score, profitability |
| 13 | Predictive Maintenance | FactMaintenance + FactDeviceTelemetry | Risk score, overheating alerts, MTTR, repeat faults |
| 14 | Sustainability | FactEnergyConsumption | CO2 saved, renewable %, energy efficiency per station |

---

## 17. Azure Services Summary

| Service | Role |
|---|---|
| **Azure IoT Hub** | Receives OCPP telemetry from physical chargers |
| **Azure Event Hubs** | Message bus for all streaming topics (10 topics) |
| **Azure Data Factory** | Batch ingestion, CDC, REST API, XML/CSV copy, orchestration |
| **Azure Logic Apps** | Email invoice ingestion trigger |
| **Azure Blob Storage** | PDF invoice binary storage |
| **Azure AI Document Intelligence** | PDF invoice data extraction |
| **ADLS Gen2** | All lakehouse layers: Bronze / Silver / Gold |
| **Azure Databricks** | PySpark batch + Structured Streaming, Delta Lake writes |
| **Delta Lake** | ACID transactions, time travel, schema evolution |
| **Azure Synapse Analytics** | Serverless + dedicated SQL pool serving layer |
| **Azure Cosmos DB** | Low-latency (<2 sec) reads for web/mobile APIs |
| **Azure Key Vault** | All secrets, API keys, connection strings |
| **Azure Active Directory / Entra ID** | Identity, RBAC, Managed Identity |
| **Azure Monitor + Log Analytics** | Pipeline, job, and infrastructure observability |
| **Azure Functions** | Event-driven alerting (overheating, payment failures) |
| **Azure Communication Services** | Email + SMS alert delivery |
| **Microsoft Teams Webhook** | Ops alerting channel |
| **Azure DevOps** | Repos, CI/CD pipelines, environments |
| **Power BI** | 14 dashboard views, row-level security |

---

*Architecture authored for VoltGrid Mobility Solutions Australia — Azure Data Engineering Project*
*Scale: 50K records | Medallion Lakehouse | All scenarios aligned to Bronze → Silver → Gold*
