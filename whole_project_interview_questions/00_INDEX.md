# Interview Questions — Topic Index
### VoltGrid AU — Azure EV Charging Intelligence Platform
> 35 Questions across 14 Topics | 2–8 years experience | Scenario-based & Real-world

---

| # | File | Topic | Questions |
|---|---|---|---|
| 01 | [01_architecture_and_system_design.md](01_architecture_and_system_design.md) | Architecture & System Design | Q1–Q4 |
| 02 | [02_azure_data_factory_ingestion_orchestration.md](02_azure_data_factory_ingestion_orchestration.md) | Azure Data Factory — Ingestion & Orchestration | Q5–Q7 |
| 03 | [03_azure_databricks_and_pyspark.md](03_azure_databricks_and_pyspark.md) | Azure Databricks & PySpark | Q8–Q10 |
| 04 | [04_delta_lake_and_medallion_architecture.md](04_delta_lake_and_medallion_architecture.md) | Delta Lake & Medallion Architecture | Q11–Q12 |
| 05 | [05_silver_layer_data_quality_transformations.md](05_silver_layer_data_quality_transformations.md) | Silver Layer — Data Quality & Transformations | Q13–Q15 |
| 06 | [06_gold_layer_dimensional_modelling_star_schema.md](06_gold_layer_dimensional_modelling_star_schema.md) | Gold Layer — Dimensional Modelling & Star Schema | Q16–Q17 |
| 07 | [07_streaming_event_hubs_structured_streaming.md](07_streaming_event_hubs_structured_streaming.md) | Streaming — Event Hubs & Structured Streaming | Q18–Q19 |
| 08 | [08_access_control_security_compliance.md](08_access_control_security_compliance.md) | Access Control, Security & Compliance | Q20–Q22 |
| 09 | [09_unity_catalog_and_data_governance.md](09_unity_catalog_and_data_governance.md) | Unity Catalog & Data Governance | Q23–Q24 |
| 10 | [10_serving_layer_synapse_cosmosdb_powerbi.md](10_serving_layer_synapse_cosmosdb_powerbi.md) | Serving Layer — Synapse, Cosmos DB & Power BI | Q25–Q26 |
| 11 | [11_cicd_monitoring_observability.md](11_cicd_monitoring_observability.md) | CI/CD, Monitoring & Observability | Q27–Q28 |
| 12 | [12_scd_slowly_changing_dimensions_history.md](12_scd_slowly_changing_dimensions_history.md) | SCD — Slowly Changing Dimensions & History | Q29 |
| 13 | [13_performance_optimisation_and_cost.md](13_performance_optimisation_and_cost.md) | Performance, Optimisation & Cost | Q30–Q31 |
| 14 | [14_real_world_scenario_curveballs.md](14_real_world_scenario_curveballs.md) | Real-world Scenario Curveballs | Q32–Q35 |

---

## Quick Question Lookup

| Q# | Question summary | File |
|---|---|---|
| Q1 | Medallion architecture — why Bronze→Silver→Gold over single-hop | 01 |
| Q2 | 28 source systems — one ingestion architecture design | 01 |
| Q3 | ADLS Gen2 HNS vs regular Blob Storage | 01 |
| Q4 | Batch + streaming — avoiding workload starvation | 01 |
| Q5 | High-watermark incremental load + mid-pipeline failure | 02 |
| Q6 | Metadata-driven pipeline — 17 entities, 1 pipeline pair | 02 |
| Q7 | HTTP 429 rate-limit handling in ADF REST pipelines | 02 |
| Q8 | Silver notebook design — v1→v2→v3 progression | 03 |
| Q9 | IoT 82°C temperature event — full pipeline trace end-to-end | 03 |
| Q10 | Delta MERGE (upsert) vs overwrite — why and when | 03 |
| Q11 | Delta time travel — real project scenarios | 04 |
| Q12 | Schema evolution — new API field, Auto Loader handling | 04 |
| Q13 | Quarantine pattern — bad record routing + spike alerting | 05 |
| Q14 | PCI DSS card masking — what, where, and why | 05 |
| Q15 | SCD2 tariff rate change — correct historical join | 05 |
| Q16 | Star schema for FactChargingSession — why not flat table | 06 |
| Q17 | Billing reconciliation — MATCH/MISMATCH calculation | 06 |
| Q18 | Exactly-once semantics — Event Hubs + Structured Streaming | 07 |
| Q19 | IoT retry storm — 5 duplicate events, one fault in dashboard | 07 |
| Q20 | New analyst onboarding — end-to-end access control setup | 08 |
| Q21 | Service Principal vs Managed Identity vs Access Connector | 08 |
| Q22 | Australian Privacy Act 1988 compliance | 08 |
| Q23 | Unity Catalog 4-level namespace | 09 |
| Q24 | Storage Credential — what breaks if deleted | 09 |
| Q25 | Cosmos DB vs Synapse for <2 sec mobile app SLA | 10 |
| Q26 | Franchise owner seeing wrong state data in Power BI RLS | 10 |
| Q27 | Silver notebook change — Dev→QA→UAT→Prod CI/CD | 11 |
| Q28 | Pipeline health monitoring — 4 layers | 11 |
| Q29 | SCD Type 1, 2, 3 — concrete scenarios from project | 12 |
| Q30 | Power BI DirectQuery slow — diagnose and fix Synapse | 13 |
| Q31 | OPTIMIZE + ZORDER + VACUUM — when and why | 13 |
| Q32 | Incident: Gold table has 0 rows at 3 AM — response | 14 |
| Q33 | Charger Uptime KPI wrong for 2 weeks — investigation | 14 |
| Q34 | Expanding from Australia to New Zealand — model changes | 14 |
| Q35 | Accidental DELETE on Silver table — Delta recovery | 14 |

---

## Key Project Numbers (Quick Reference)

| Metric | Value |
|---|---|
| Total sources | 28 |
| Data scale | ~50,000 records |
| Streaming topics (Event Hub) | 10 |
| Bronze tables | 22+ |
| Silver tables | 25 |
| Gold dimensions | 13 |
| Gold fact tables | 9 |
| Aggregated marts | 10 |
| Power BI dashboards | 14 views |
| Streaming watermark | 10 minutes |
| IoT overheating threshold | 75°C |
| Cosmos DB read SLA | < 2 seconds |
| Bronze retention | 90 days |
| Australian GST rate | 10% |
| NZ GST rate | 15% |
| SP client secret rotation | Every 90 days |
| SAS token external permissions | `sp=rl` (read + list only) |
| Complaint SLA — P1/P2/P3 | 4 hr / 8 hr / 24 hr |
| Geo hierarchy levels | Country → State → City → Station → Charger → Session |
