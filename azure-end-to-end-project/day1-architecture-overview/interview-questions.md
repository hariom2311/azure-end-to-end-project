# Day 1 — Interview Questions: Architecture Overview

> 25 questions covering Medallion Architecture, Azure service selection, Bronze/Silver/Gold design, star schema, SCD2, data quality, streaming vs batch, security, and system design.
> Attempt first, then check `interview-solutions.md`.

---

## Medallion Architecture

**Q1 (Warm-up)**
What is Medallion Architecture? What are the three layers and what is the purpose of each?

---

**Q2 (Conceptual)**
Why is Bronze kept as an append-only, immutable layer? What happens if you modify raw data in Bronze?

---

**Q3 (Conceptual)**
What is the difference between how Bronze and Silver write data? Bronze uses append — what does Silver use, and why?

---

**Q4 (Scenario)**
A data engineer says: "Why do we need three layers? Can't we just transform the data directly when we read it from the source and write it to Gold?" What is wrong with this approach?

---

**Q5 (Tricky)**
In this project, Bronze uses Delta Lake format instead of plain Parquet. What does Delta Lake add that plain Parquet cannot provide? Name at least 3 capabilities.

---

## Source Systems and Ingestion

**Q6 (Warm-up)**
This project has 28 source systems. How are they split between batch and streaming sources? Give 2 examples of each.

---

**Q7 (Conceptual)**
What is a high-watermark incremental load? Why is it used for CSV batch sources in this project instead of a full load every day?

---

**Q8 (Scenario)**
A CSV file for charging sessions arrives at 3 AM daily. Sometimes the file is delayed and arrives at 8 AM. Some records in the file have `plug_in_date` from the previous day. How does the Bronze layer handle this correctly?

---

**Q9 (Conceptual)**
What is the difference between how IoT telemetry data and CSV session data are ingested into Bronze? Which Azure services are involved in each path?

---

**Q10 (Tricky)**
Exactly-once delivery is mentioned for streaming ingestion. What does "exactly-once" mean in a streaming context? How does Databricks Structured Streaming + Event Hub achieve it?

---

## Silver Layer Transformations

**Q11 (Warm-up)**
What are the 4 columns added to every Bronze table? What is the purpose of each?

---

**Q12 (Conceptual)**
A charging session record arrives in Bronze with `start_ts = "2026-06-01 17:30:00"` in AEST (UTC+10). What should it look like after the Silver timezone normalisation rule?

---

**Q13 (Scenario)**
A charger's IoT device sends the same telemetry event twice due to a network retry. Both records land in Bronze. What happens at Silver? How is the duplicate handled?

---

**Q14 (Conceptual)**
What is SCD Type 2? Give a concrete example from this project where SCD2 is used and explain why Type 1 (overwrite) would be wrong.

---

**Q15 (Tricky)**
A streaming event arrives 12 minutes after its `event_ts`. The watermark is 10 minutes. What happens to this record? Does it get silently dropped? Where does it go?

---

## Gold Layer — Star Schema

**Q16 (Warm-up)**
What is a star schema? What is the difference between a Dimension table and a Fact table?

---

**Q17 (Conceptual)**
The `FactChargingSession` table has a `reconciliation_status` column that can be `MATCH` or `MISMATCH`. How is this value computed? What three tables are involved?

---

**Q18 (Scenario)**
A franchise owner in NSW logs into the Power BI dashboard. They should only see their own 5 stations, not all 120 stations in Australia. How is this enforced in the architecture?

---

**Q19 (Conceptual)**
Why are Aggregated Marts like `mart_revenue_by_geo_month` pre-computed instead of always reading from `FactPayments` with GROUP BY in the Power BI query?

---

**Q20 (Tricky)**
A session happened on 15 Jun 2026 using tariff rate $0.45/kWh. The tariff changed to $0.52/kWh on 1 Jul 2026. A report generated on 1 Aug 2026 shows this session's revenue. Which tariff rate is used — the old or the new? How does the schema ensure correctness?

---

## Security and Data Quality

**Q21 (Conceptual)**
A charge card record arrives in Bronze with the full 16-digit PAN `4242424242424242` and the CVV `123`. What happens to these values at Silver and why?

---

**Q22 (Scenario)**
1,000 records land in Bronze from the charging sessions CSV. 12 records have `session_id = null`. 3 records have `end_ts < start_ts`. 5 records have `energy_kwh = 920.0`. What happens to each group of bad records at Silver?

---

**Q23 (Conceptual)**
Why does this architecture use Managed Identity for service-to-service auth (ADF → ADLS, Databricks → Key Vault) instead of using a Service Principal everywhere?

---

## Serving and Real-Time

**Q24 (Scenario)**
A mobile app needs to show live charger availability — which chargers at a given station are currently available, occupied, or offline. The SLA is a response time under 2 seconds. How does this architecture serve that API? Trace the path from data source to the app.

---

**Q25 (System design)**
A new franchise partner in New Zealand wants to join the VoltGrid network. Their chargers send OCPP telemetry. They have a customer CRM system and an ERP for invoicing. Design the changes needed to onboard them into this architecture:
- Which source types and ingestion paths would be used?
- What changes would be needed in the Dimension tables?
- What row-level security would be applied?
- What alerting would be set up for their chargers?

---
