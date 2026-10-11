# Day 1 — Practice Exercises: Architecture Overview

> These exercises are paper-based / discussion-based — no Azure access needed yet.
> Goal: understand the architecture well enough to explain every component before writing code.

---

## Exercise 1 — Read and Map the Architecture

**Objective:** Trace the full data path for one specific source end to end.

Pick **IoT Charger Telemetry** and trace its journey:

1. What physical device sends the data? What protocol does it use?
2. Which Azure service receives the stream first?
3. Which Azure service moves it from the stream into storage?
4. What folder does it land in inside Bronze? What format?
5. What columns are added when it arrives in Bronze that were NOT in the original source?
6. What happens to it at Silver? Name at least 3 transformation rules applied.
7. Which Gold table(s) use this data?
8. Which Power BI dashboard shows the final result?

Write your answers as a flow diagram:
```
IoT Device → ??? → ??? → Bronze path → Silver table → Gold fact → Dashboard
```

**Verify:** Your diagram matches the Architecture notes Part 2 to Part 14 flow.

---

## Exercise 2 — Identify the Right Azure Service

**Objective:** Given a requirement, choose the correct Azure service.

For each requirement below, name the correct Azure service and explain why:

| Requirement | Your Answer |
|---|---|
| Receive real-time JSON events from 300 physical EV chargers at 1–3 second intervals | |
| Store raw CSV files, Parquet files, and Delta tables at low cost for long-term retention | |
| Extract invoice data (amounts, dates, vendor names) from scanned PDF files automatically | |
| Run PySpark notebooks to transform Bronze data into Silver | |
| Store secrets (passwords, API keys) so notebooks never have hardcoded credentials | |
| Serve pre-aggregated mart data to Power BI dashboards with sub-second query time | |
| Provide a REST API that returns live charger availability in under 2 seconds | |
| Send an SMS when a charger overheating event is detected | |
| Orchestrate a daily batch pipeline that copies CSV files to Bronze every night | |
| Trigger a Databricks job automatically when the ADF ingestion pipeline completes | |

---

## Exercise 3 — Medallion Layer Design

**Objective:** Given raw data, design what it looks like at each layer.

Imagine a payment record comes in from the VoltGrid Payment Gateway API:

**Raw API response (as received):**
```json
{
  "payment_id": "PAY-000001",
  "session_id": "SES-12345",
  "amount_aud": -15.50,
  "gst_included": true,
  "card_number": "4242424242424242",
  "cvv": "123",
  "status": "sucess",
  "paid_at": "2026-06-01T17:30:00+10:00"
}
```

Answer these questions:

1. **Bronze:** What does this record look like in Bronze? What 4 columns are added? Is `amount_aud = -15.50` acceptable in Bronze — why or why not?

2. **Silver:** List all the transformations that should happen at Silver for this record:
   - What happens to `card_number`?
   - What happens to `cvv`?
   - What happens to `status = "sucess"` (typo)?
   - What happens to `paid_at` (which is in AEST, UTC+10)?
   - What happens to `gst_included = true` and `amount_aud = -15.50`?
   - Should this record go to the main Silver table or quarantine? Why?

3. **Gold:** Which Fact table does this payment record ultimately populate? Which Dimension tables does it join to? List the metrics columns in the Fact table that come from this record.

---

## Exercise 4 — SCD Type 2 Scenario

**Objective:** Understand how Slowly Changing Dimensions work in the architecture.

The tariff rate for EV charging changes:
- **1 Jan 2026:** Rate = AUD $0.45/kWh (flat rate)
- **1 Jul 2026:** Rate increases to AUD $0.52/kWh (peak hours), $0.38/kWh (off-peak)

A customer charges their vehicle on **15 Jun 2026** and again on **15 Aug 2026**.

1. Draw the `DimTariff` table with SCD2 columns — how many rows exist after 1 Jul 2026 rate change?
2. Which tariff row does the **15 Jun** session use when populating `FactChargingSession`?
3. Which tariff row does the **15 Aug** session use?
4. In the `DimTariff` table, what are the values of `effective_from`, `effective_to`, and `is_current` for the old rate row after the change?
5. Why does SCD2 matter for the billing reconciliation mart?

---

## Exercise 5 — Data Quality Scenario

**Objective:** Apply the DQ framework to a batch of incoming records.

The morning ADF pipeline pulls 1,000 charging session records from the CSV source. After Bronze landing, the Silver transformation job processes them.

**Examine these 5 records and classify each:**

| session_id | charger_id | start_ts | end_ts | energy_kwh | station_id |
|---|---|---|---|---|---|
| SES-001 | CHG-100 | 2026-06-01 09:00 | 2026-06-01 10:30 | 15.2 | STA-01 |
| SES-002 | CHG-999 | 2026-06-01 08:00 | 2026-06-01 09:00 | 8.4 | STA-05 |
| SES-003 | CHG-100 | 2026-06-01 11:00 | 2026-06-01 10:00 | 22.1 | STA-01 |
| SES-004 | CHG-102 | 2026-06-01 14:00 | 2026-06-01 15:00 | 850.0 | STA-02 |
| SES-005 | null | 2026-06-01 16:00 | 2026-06-01 17:00 | 12.0 | STA-03 |

For each record:
1. Does it pass or fail the DQ checks? Which check fails?
2. Where does it go — Silver main table or quarantine?
3. What `rejection_reason` would be written for failed records?

Assume: charger_master contains CHG-100, CHG-101, CHG-102 — CHG-999 does not exist.

---

## Exercise 6 — Security Design

**Objective:** Map the correct security role to each identity in the project.

For each identity and resource combination, write the correct RBAC role and explain why:

| Identity | Resource | Correct Role | Why |
|---|---|---|---|
| Your user account | Key Vault `kv-ev-intelligence-dev` | | |
| Service Principal `sp-ev-intelligence-dev` | Storage Account `evdatalakedev` | | |
| `AzureDatabricks` global enterprise app | Key Vault `kv-ev-intelligence-dev` | | |
| Data engineer (human) | ADLS container `silver/` | | |
| BI analyst (human) | ADLS container `gold/` | | |
| ADF Managed Identity | ADLS container `bronze/` | | |
| Franchise owner (Power BI user) | Power BI dataset | | |

---

## Exercise 7 — Dashboard Design

**Objective:** Given a business question, identify the correct Gold table and drill-down path.

For each business question, identify:
- Which dashboard view answers it
- Which Gold table(s) are queried
- Which Dimension tables are used for filtering/grouping

| Business Question | Dashboard | Gold Tables | Dimensions |
|---|---|---|---|
| "How much revenue did NSW generate in June 2026?" | | | |
| "Which charger had the most overheating events this month?" | | | |
| "What % of sessions used a charge card vs mobile wallet?" | | | |
| "How many complaints breached SLA in QLD last quarter?" | | | |
| "What is the predicted maintenance risk for charger CHG-042?" | | | |
| "Show active charging sessions right now across all stations" | | | |
| "Which franchise partner has the lowest charger uptime?" | | | |

---

## Exercise 8 — Architecture Critique

**Objective:** Critically evaluate design decisions.

Answer the following architecture questions:

1. **Why is Bronze append-only?** What would go wrong if you updated or deleted raw records in Bronze?

2. **Why does Silver use MERGE (upsert) instead of overwrite?** Give a scenario where overwrite would cause a problem.

3. **Why is Cosmos DB used for the Fleet Web API instead of querying Synapse directly?** What happens if you query Synapse for every API request?

4. **Why are aggregated marts pre-computed instead of computed at query time?** What is the trade-off?

5. **Why does the architecture use a 10-minute watermark for streaming events?** What happens if there is no watermark?

---
