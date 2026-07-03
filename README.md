# Azure EV Intelligence — 18-Day End-to-End Data Engineering Project

A hands-on, end-to-end Azure data engineering project from [Data Engineering Daily](https://www.dataengineeringdaily.com/azure-de-project/).  
Build a production-grade EV (Electric Vehicle) data platform on Azure over 18 guided sessions — from raw ingestion to Gold analytics, monitoring, CI/CD, and interview preparation.

---

## Project Overview

| Detail | Value |
|---|---|
| **Source** | [dataengineeringdaily.com](https://www.dataengineeringdaily.com/azure-de-project/) |
| **Duration** | 18 days (~2 hours/session) |
| **Domain** | Electric Vehicle (EV) Intelligence |
| **Architecture** | Medallion (Bronze → Silver → Gold) on Azure |
| **Primary Tools** | Azure Databricks, ADLS Gen2, ADF, Key Vault, Delta Lake, Power BI |
| **Region** | Central India (`centralindia`) |

---

## Architecture

```
[Source Systems]
    |
    |-- VoltGrid API (REST / CDC via updated_at)
    |-- Blob Storage (CSV / PDF / XML / JSON uploads)
    |-- Event Hub (IoT Streaming JSON)
                |
         [ADF] + [Databricks Auto Loader] + [Databricks Streaming]
                |
         [ADLS Gen2 — Medallion Layers]
         abfss://bronze@evdatalakedev.dfs.core.windows.net/   ← raw, append-only
         abfss://silver@evdatalakedev.dfs.core.windows.net/   ← cleaned, MERGE upsert (Delta)
         abfss://gold@evdatalakedev.dfs.core.windows.net/     ← aggregated, star schema (Delta)
                |
         [Azure Databricks — Delta Lake]
                |
         [Power BI / Synapse Analytics]
```

---

## 18-Day Plan

| Day | Title | Focus |
|-----|-------|-------|
| **Day 1** | [Kickoff, Architecture Scope, and Azure Setup](day_1_kickoff_architecture_azure_setup/DAY1_AZURE_SETUP.md) | Provision all Azure resources, wire up security, connect ADLS Gen2 in Databricks via SP OAuth direct access |
| **Day 2** | Storage Design, Security, and Secret Management | Establish secure data layer zones and credential management strategy |
| **Day 3** | Source Discovery and Data Contracts | Profile PostgreSQL, APIs, and file feeds with documented contracts |
| **Day 4** | Bronze Ingestion for Batch Sources | Raw data loading from database and files into Delta format |
| **Day 5** | Streaming Ingestion and Checkpointing | Process IoT telemetry stream into Bronze Delta layer |
| **Day 6** | CDC and Incremental Loading Pattern | Capture changed rows with watermark-based extraction |
| **Day 7** | Silver Cleansing and Validation | Apply data quality and transformation rules |
| **Day 8** | ADF Orchestration and Metadata-Driven Runs | Automate and parameterize pipeline execution |
| **Day 9** | SCD Type 2 and Late-Arriving Data | Handle historical changes and delayed events safely |
| **Day 10** | Gold Model Build (Facts and Dimensions) | Build analytics-ready serving tables |
| **Day 11** | Performance Optimization and Cost Controls | Tune jobs and data layout for scale and cost |
| **Day 12** | Monitoring, Audit, and Alerting | Build operational observability and incident alerts |
| **Day 13** | Failure Simulation and Recovery Drills | Practice real-world failure handling end-to-end |
| **Day 14** | Full End-to-End Production Dry Run | Execute complete source-to-gold run with checks |
| **Day 15** | Interview Packaging and Project Storytelling | Turn project work into strong interview material |
| **Day 16** | Spark Performance Tuning | Optimize Spark jobs for production-grade throughput and cost |
| **Day 17** | Metadata-Driven Pipeline Framework | Build a config-table-driven ingestion engine |
| **Day 18** | CI/CD and Git Workflow for Data Engineering | Ship pipeline changes safely using Azure DevOps |

---

## Repository Structure

```
azure-ev-end-to-end-project/
├── README.md
├── day_1_kickoff_architecture_azure_setup/
│   ├── DAY1_AZURE_SETUP.md
│   └── Day 1 — Kickoff.pdf
└── (day_2/ through day_18/ added as project progresses)
```

---

## Cost Estimate

| Resource | Estimated Cost |
|---|---|
| ADLS Gen2 (~10–15 GB) | ~₹20–30/month |
| Azure Key Vault | ~₹5 total (entire project) |
| Service Principal | ₹0 |
| Databricks (per 2-hr session) | ~₹40–45 |
| **Total across 18 sessions** | **~₹810–850** |

> New Azure accounts get ₹13,370 (~$200 USD) free for 30 days — this entire project costs ₹0 on a new account.

---

## Key Concepts Covered

- **Medallion Architecture** — Bronze / Silver / Gold data layers
- **Delta Lake** — ACID transactions, time travel, MERGE/upsert
- **Service Principal OAuth** — secure, least-privilege storage access (no hardcoded keys)
- **Azure Key Vault** — centralized secret management with Databricks secret scope
- **CDC & Watermarking** — incremental load patterns for large tables
- **SCD Type 2** — slowly changing dimensions with history tracking
- **ADF Metadata-Driven Pipelines** — config-table-based orchestration
- **Spark Performance Tuning** — partitioning, Z-ordering, caching
- **CI/CD for Data Engineering** — Azure DevOps pipelines for notebook deployments

---

## Getting Started

1. Complete [Day 1 setup](day_1_kickoff_architecture_azure_setup/DAY1_AZURE_SETUP.md) to provision all Azure infrastructure
2. Set a budget alert at ₹1,500/month before creating any resources
3. Always terminate your Databricks cluster after each session
4. Follow each day's checklist before moving to the next day

---

## Source

Full curriculum and day guides: [dataengineeringdaily.com/azure-de-project](https://www.dataengineeringdaily.com/azure-de-project/)
