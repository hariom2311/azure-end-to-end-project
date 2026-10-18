# pl_bronze_master — Pipeline Flow

**Pipeline:** `pl_bronze_master`
**Purpose:** Orchestrates all 20 VoltGrid Bronze sources sequentially by calling `pl_bronze_incremental_load_v2` with a different `p_source_name` for each. All config lives in the control table — the master pipeline is just a chain of 20 Execute Pipeline activities.

---

## Design Principle — Control Table Driven

Every source-specific detail (endpoint, sink path, load type, watermark) lives in `ev_catalog.ctl.pipeline_control`. The master pipeline only passes the source name. Adding a 21st source = 1 INSERT + 1 new ExecutePipeline activity.

---

## Source Inventory (20 Sources)

### Group 1 — File-Based Scenario APIs (3 sources)

| # | source_name | Endpoint | load_type | What it is |
|---|---|---|---|---|
| 1 | `payments` | `/api/db/payments/` | incremental | Payment transactions (load scenarios 1–5) |
| 2 | `fleet` | `/api/fleet/` | full | Fleet vehicle usage scenarios |
| 3 | `weather` | `/api/weather/` | full | Weather observation scenarios |

### Group 2 — DB CRUD: Fact / High-Frequency Tables (6 sources)

| # | source_name | Endpoint | load_type | What it is |
|---|---|---|---|---|
| 4 | `db_payments` | `/api/db/payments/` | incremental | Payment transactions (live DB) |
| 5 | `db_sessions` | `/api/db/sessions/` | incremental | EV charging sessions (fact) |
| 6 | `db_energy_prices` | `/api/db/energy-prices/` | incremental | Hourly energy pricing feed |
| 7 | `db_complaints` | `/api/db/complaints/` | incremental | Customer complaints |
| 8 | `db_maintenance_events` | `/api/db/maintenance-events/` | incremental | Charger maintenance events |
| 9 | `db_pipeline_audit` | `/api/db/pipeline-audit/` | incremental | Pipeline run observability log |

### Group 3 — DB CRUD: Dimension Tables (6 sources, slow-changing)

| # | source_name | Endpoint | load_type | What it is |
|---|---|---|---|---|
| 10 | `db_customers` | `/api/db/customers/` | incremental | Customer profiles (loyalty tier, contact) |
| 11 | `db_vehicles` | `/api/db/vehicles/` | incremental | Vehicle dim, SCD Type 2 |
| 12 | `db_chargers` | `/api/db/chargers/` | incremental | Charger equipment per station |
| 13 | `db_charge_cards` | `/api/db/charge-cards/` | incremental | Customer RFID / charge cards |
| 14 | `db_partners` | `/api/db/partners/` | incremental | Franchise partners |
| 15 | `db_employees` | `/api/db/employees/` | incremental | Staff (department, station assignment) |

### Group 4 — DB CRUD: Reference Tables (5 sources, full load)

| # | source_name | Endpoint | load_type | What it is |
|---|---|---|---|---|
| 16 | `db_states` | `/api/db/states/` | full | Australian states/territories (8 rows) |
| 17 | `db_cities` | `/api/db/cities/` | full | City to postcode / state mapping |
| 18 | `db_tariffs` | `/api/db/tariffs/` | full | Energy tariff rate schedules |
| 19 | `db_stations` | `/api/db/stations/` | full | Charging station infrastructure |

> Note: source #19 is `db_stations` — total is 19 DB sources + 3 file-based = 20 total (payments listed once as file-based, once as DB).

---

## Master Pipeline Flow Diagram

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                           pl_bronze_master                                   ║
║          Parameter: p_load_type_override (string, default "")                ║
╚══════════════════════════════════════════════════════════════════════════════╝
                                    │
          ══════════════════════════╪══ GROUP 1: File-based scenarios ══════════
                                    │
                    ┌───────────────┴──────────────────────┐
                    │         act_run_payments              │
                    │   p_source_name = "payments"          │
                    │   → /api/db/payments/ (incremental)   │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │         act_run_fleet                 │
                    │   p_source_name = "fleet"             │
                    │   → /api/fleet/ (full)                │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │         act_run_weather               │
                    │   p_source_name = "weather"           │
                    │   → /api/weather/ (full)              │
                    └───────────────┬──────────────────────┘
                                    │
          ══════════════════════════╪══ GROUP 2: DB Facts (incremental) ════════
                                    │
                    ┌───────────────┴──────────────────────┐
                    │       act_run_db_payments             │
                    │   p_source_name = "db_payments"       │
                    │   → /api/db/payments/ (incremental)   │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │       act_run_db_sessions             │
                    │   p_source_name = "db_sessions"       │
                    │   → /api/db/sessions/ (incremental)   │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │     act_run_db_energy_prices          │
                    │   p_source_name = "db_energy_prices"  │
                    │   → /api/db/energy-prices/ (incr.)    │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │      act_run_db_complaints            │
                    │   p_source_name = "db_complaints"     │
                    │   → /api/db/complaints/ (incremental) │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │   act_run_db_maintenance_events       │
                    │   p_source_name = "db_maint_events"   │
                    │   → /api/db/maintenance-events/ (i.)  │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │     act_run_db_pipeline_audit         │
                    │   p_source_name = "db_pipeline_audit" │
                    │   → /api/db/pipeline-audit/ (incr.)   │
                    └───────────────┬──────────────────────┘
                                    │
          ══════════════════════════╪══ GROUP 3: DB Dims (incremental) ═════════
                                    │
                    ┌───────────────┴──────────────────────┐
                    │      act_run_db_customers             │
                    │   p_source_name = "db_customers"      │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │      act_run_db_vehicles              │
                    │   p_source_name = "db_vehicles"       │
                    │   (SCD Type 2 dim)                    │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │      act_run_db_chargers              │
                    │   p_source_name = "db_chargers"       │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │      act_run_db_charge_cards          │
                    │   p_source_name = "db_charge_cards"   │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │      act_run_db_partners              │
                    │   p_source_name = "db_partners"       │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │      act_run_db_employees             │
                    │   p_source_name = "db_employees"      │
                    └───────────────┬──────────────────────┘
                                    │
          ══════════════════════════╪══ GROUP 4: DB Reference (full load) ══════
                                    │
                    ┌───────────────┴──────────────────────┐
                    │       act_run_db_states               │
                    │   p_source_name = "db_states"         │
                    │   → /api/db/states/ (full)            │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │       act_run_db_cities               │
                    │   p_source_name = "db_cities"         │
                    │   → /api/db/cities/ (full)            │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │       act_run_db_tariffs              │
                    │   p_source_name = "db_tariffs"        │
                    │   → /api/db/tariffs/ (full)           │
                    └───────────────┬──────────────────────┘
                                    │ Succeeded
                    ┌───────────────┴──────────────────────┐
                    │       act_run_db_stations             │
                    │   p_source_name = "db_stations"       │
                    │   → /api/db/stations/ (full)          │
                    └───────────────┬──────────────────────┘
                                    │
                                    ▼
                              PIPELINE END
                   (all 20 sources landed in Bronze)
```

---

## Activity Reference Table

| # | Activity Name | source_name | Endpoint | load_type | Depends On |
|---|---|---|---|---|---|
| 1 | `act_run_payments` | `payments` | `/api/db/payments/` | incremental | — |
| 2 | `act_run_fleet` | `fleet` | `/api/fleet/` | full | #1 |
| 3 | `act_run_weather` | `weather` | `/api/weather/` | full | #2 |
| 4 | `act_run_db_payments` | `db_payments` | `/api/db/payments/` | incremental | #3 |
| 5 | `act_run_db_sessions` | `db_sessions` | `/api/db/sessions/` | incremental | #4 |
| 6 | `act_run_db_energy_prices` | `db_energy_prices` | `/api/db/energy-prices/` | incremental | #5 |
| 7 | `act_run_db_complaints` | `db_complaints` | `/api/db/complaints/` | incremental | #6 |
| 8 | `act_run_db_maintenance_events` | `db_maintenance_events` | `/api/db/maintenance-events/` | incremental | #7 |
| 9 | `act_run_db_pipeline_audit` | `db_pipeline_audit` | `/api/db/pipeline-audit/` | incremental | #8 |
| 10 | `act_run_db_customers` | `db_customers` | `/api/db/customers/` | incremental | #9 |
| 11 | `act_run_db_vehicles` | `db_vehicles` | `/api/db/vehicles/` | incremental | #10 |
| 12 | `act_run_db_chargers` | `db_chargers` | `/api/db/chargers/` | incremental | #11 |
| 13 | `act_run_db_charge_cards` | `db_charge_cards` | `/api/db/charge-cards/` | incremental | #12 |
| 14 | `act_run_db_partners` | `db_partners` | `/api/db/partners/` | incremental | #13 |
| 15 | `act_run_db_employees` | `db_employees` | `/api/db/employees/` | incremental | #14 |
| 16 | `act_run_db_states` | `db_states` | `/api/db/states/` | full | #15 |
| 17 | `act_run_db_cities` | `db_cities` | `/api/db/cities/` | full | #16 |
| 18 | `act_run_db_tariffs` | `db_tariffs` | `/api/db/tariffs/` | full | #17 |
| 19 | `act_run_db_stations` | `db_stations` | `/api/db/stations/` | full | #18 |

---

## Bronze Output After a Full Run

```
ADLS bronze container
└── api/
    ├── payments/              ingestion_date=<today>/  payments_page_*.json
    ├── fleet/                 ingestion_date=<today>/  fleet_page_*.json
    ├── weather/               ingestion_date=<today>/  weather_page_*.json
    └── db/
        ├── payments/          ingestion_date=<today>/  db_payments_page_*.json
        ├── sessions/          ingestion_date=<today>/  db_sessions_page_*.json
        ├── energy_prices/     ingestion_date=<today>/  db_energy_prices_page_*.json
        ├── complaints/        ingestion_date=<today>/  db_complaints_page_*.json
        ├── maintenance_events/ingestion_date=<today>/  db_maintenance_events_page_*.json
        ├── pipeline_audit/    ingestion_date=<today>/  db_pipeline_audit_page_*.json
        ├── customers/         ingestion_date=<today>/  db_customers_page_*.json
        ├── vehicles/          ingestion_date=<today>/  db_vehicles_page_*.json
        ├── chargers/          ingestion_date=<today>/  db_chargers_page_*.json
        ├── charge_cards/      ingestion_date=<today>/  db_charge_cards_page_*.json
        ├── partners/          ingestion_date=<today>/  db_partners_page_*.json
        ├── employees/         ingestion_date=<today>/  db_employees_page_*.json
        ├── states/            ingestion_date=<today>/  db_states_page_*.json
        ├── cities/            ingestion_date=<today>/  db_cities_page_*.json
        ├── tariffs/           ingestion_date=<today>/  db_tariffs_page_*.json
        └── stations/          ingestion_date=<today>/  db_stations_page_*.json
```

---

## Failure Behaviour

Each `ExecutePipeline` has `dependencyConditions: ["Succeeded"]`. If source N fails:
- Sources N+1 onwards are **skipped**
- Sources 1 to N-1 already completed — their data is in Bronze and watermarks updated
- Re-run the master: completed sources will have 0 new pages (watermark is current), failed source retries from its last watermark

---

## Execution Modes

| `p_load_type_override` | Behaviour |
|---|---|
| `""` (empty, default) | Each source uses its own `load_type` from the control table |
| `"full"` | All 20 sources do full reload regardless of load_type |

Use `"full"` for the initial run when Bronze is empty, or for repairs.

---

## Why Sequential?

The 20 sources could be parallelised. Sequential is used here because:
1. **Shared Databricks cluster** — parallel runs would queue or trigger multiple job clusters
2. **Debuggability** — ADF Monitor shows exactly which source is running and which failed
3. **Watermark integrity** — each source updates its own watermark independently; no race condition
4. **Beginner-friendly** — one thing at a time is easier to reason about

For production at scale, group the sources into independent batches (facts parallel with dims, reference tables parallel with facts) using multiple master pipelines triggered simultaneously.
