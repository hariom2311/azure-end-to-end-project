# Day 12 — Blob Migration (Silver & Gold) + Autoloader & Delta Live Tables

## Part 1: Blob Migration Pipelines — Silver and Gold

### What We Built

Three ADF pipelines now cover the full medallion blob lifecycle:

| Pipeline | Source | Sink | Sink dataset |
|---|---|---|---|
| `pl_bronze_blob_ingestion` | `dataenggdailystorage/source` | Bronze ADLS | `ds_bronze_blob_sink` |
| `pl_silver_blob_migration` | Same blob source | Silver ADLS | `ds_silver_blob_sink` |
| `pl_gold_blob_migration`   | Same blob source | Gold ADLS   | `ds_gold_blob_sink`   |

All three share the same structure and parameter (`p_load_type`).

### Pipeline Structure (same across all three layers)

```
pl_*_blob_migration
  ├── act_copy_config_*           (always full, no IfCondition)
  ├── act_load_invoices_*         (IfCondition: full vs incremental)
  │     ├── True  → act_copy_invoices_full_*        (recursive)
  │     └── False → act_copy_invoices_incremental_* (wildcardFolderPath = yesterday)
  └── act_load_reports_*          (IfCondition: full vs incremental)
        ├── True  → act_copy_reports_full_*         (recursive)
        └── False → act_copy_reports_incremental_*  (wildcardFolderPath = prev month)
```

### Sink Folder Patterns

**Config** — always full, small XML:
```
silver-blob/config/load_type=full/
gold-blob/config/load_type=full/
```

**Invoices** — PDFs, daily cadence:
```
full:        silver-blob/invoices/load_type=full/
incremental: silver-blob/invoices/ingestion_date=2025-10-25/
```

**Reports** — JSONs, monthly cadence:
```
full:        silver-blob/reports/load_type=full/
incremental: silver-blob/reports/ingestion_date=2025-10/
```

### Key ADF Expressions

| Purpose | Expression |
|---|---|
| Route on load type | `@equals(toLower(pipeline().parameters.p_load_type), 'full')` |
| Yesterday's folder | `@concat('invoices/', formatDateTime(addDays(utcnow(), -1), 'yyyy/MM/dd'))` |
| Previous month | `@concat('reports/', formatDateTime(addDays(startOfMonth(utcnow()), -1), 'yyyy/MM'))` |
| Incremental sink date | `@concat('...ingestion_date=', formatDateTime(addDays(utcnow(), -1), 'yyyy-MM-dd'))` |

### Datasets Required

You need to create two additional ADF datasets (mirror of `ds_bronze_blob_sink`):

- **`ds_silver_blob_sink`** — Linked service: Azure Data Lake Gen2 (silver container), parameterised folder `@dataset().p_folder`
- **`ds_gold_blob_sink`** — Linked service: Azure Data Lake Gen2 (gold container), parameterised folder `@dataset().p_folder`

The source datasets (`ds_blob_config_src`, `ds_blob_invoices_src`, `ds_blob_reports_src`) are the same for all three pipelines.

---

## Part 2: Autoloader — Theoretical Understanding

### What Is Autoloader?

Autoloader (`cloudFiles`) is a Databricks-native structured streaming source that **automatically detects and ingests new files** as they land in cloud storage. It is built on top of Apache Spark Structured Streaming and Delta Lake.

Rather than listing the entire directory on every batch (which becomes slow at scale), Autoloader uses **file notification services** (Azure Event Grid + Azure Queue Storage, or AWS SNS + SQS) to receive events when new files arrive.

### How Autoloader Works Internally

```
ADLS Gen2
    │  new file lands
    ▼
Azure Event Grid ──► Azure Queue Storage ──► Databricks Autoloader
                                                  │
                                          Structured Streaming micro-batch
                                                  │
                                            Delta Table (append or merge)
```

1. A new file appears in ADLS.
2. Azure Event Grid fires a `BlobCreated` event to a Storage Queue.
3. Autoloader polls the queue — no full directory listing needed.
4. The file path is passed to the Spark reader; the file content is ingested.
5. Checkpoint state tracks which files have been processed — **exactly-once delivery**.

### Two Discovery Modes

| Mode | How it works | When to use |
|---|---|---|
| **Directory listing** (default) | Lists the source directory on every trigger | Small directories, simple setup, no Event Grid needed |
| **File notification** (scalable) | Uses Event Grid + Storage Queue | Millions of files, production workloads |

Switch modes with:
```python
.option("cloudFiles.useNotifications", "true")
```

### Basic Autoloader Code Pattern

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")            # or csv, parquet, avro, text, binaryFile
    .option("cloudFiles.schemaLocation", schema_path)  # stores inferred schema
    .option("cloudFiles.inferColumnTypes", "true")
    .load(source_path)                              # ADLS path
)

df.writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")                   # handles schema evolution
  .trigger(availableNow=True)                      # or processingTime="10 minutes"
  .toTable("ev_catalog.bronze.my_table")
```

### Trigger Modes

| Trigger | Behaviour |
|---|---|
| `availableNow=True` | Process all pending files then stop — like a scheduled batch. Best for ADF-triggered Databricks jobs |
| `processingTime="N minutes"` | Keep the stream running, check every N minutes |
| `once=True` (deprecated) | Process one micro-batch then stop — replaced by `availableNow` |
| Default (no trigger) | Continuous streaming as fast as possible |

### Key Options

| Option | Purpose |
|---|---|
| `cloudFiles.schemaLocation` | Required — stores inferred schema across runs |
| `cloudFiles.inferColumnTypes` | Infer proper types instead of all-string |
| `cloudFiles.maxFilesPerTrigger` | Throttle how many files per batch |
| `cloudFiles.backfillInterval` | Force periodic re-listing even in notification mode |
| `cloudFiles.includeExistingFiles` | Whether to process files already there when stream starts |

### Schema Evolution

Autoloader handles new columns automatically via `mergeSchema`:
- If a new field appears in a JSON file, the schema is updated
- Old records have `null` for the new field
- `cloudFiles.schemaEvolutionMode = "addNewColumns"` (default) — adds new columns silently
- `cloudFiles.schemaEvolutionMode = "rescue"` — puts unexpected columns into a `_rescued_data` column

### Autoloader vs ADF Copy Activity

| | ADF Copy Activity | Databricks Autoloader |
|---|---|---|
| Orchestration | ADF pipeline | Databricks Job / DLT |
| State tracking | `p_load_type` parameter, watermarks | Checkpoint directory (automatic) |
| Scale | Good | Excellent — millions of files |
| Format support | Binary, CSV, JSON, Parquet, ... | JSON, CSV, Parquet, Avro, text, binary, XML |
| Schema inference | No | Yes |
| Exactly-once | No (idempotent sink needed) | Yes (built-in checkpoint) |
| Schema evolution | Manual | Automatic |
| Best for | Blob migration, orchestrated ELT | Continuous/near-real-time ingestion into Delta |

**Rule of thumb:** ADF Copy for discrete blob migrations (what we did in Day 10–12). Autoloader for ongoing streaming ingestion of event data, logs, IoT feeds landing in ADLS.

---

## Part 3: Delta Live Tables (DLT) — Theoretical Understanding

### What Is DLT?

Delta Live Tables is a **declarative ETL framework** on Databricks. Instead of writing imperative notebooks that read → transform → write, you declare **what** the table should look like and DLT figures out **how** to execute and orchestrate it.

DLT manages:
- Execution order and dependencies between tables
- Data quality enforcement via **expectations**
- Incremental vs full refresh strategies
- Lineage tracking and observability
- Error handling and quarantine

### Core Concepts

#### Materialized Views vs Streaming Tables

| | Materialized View | Streaming Table |
|---|---|---|
| Use when | Source can be reprocessed anytime (dimensions, aggregations) | Source is append-only / event-driven (fact tables, logs) |
| How it works | Re-computes the full result set or uses incremental optimization | Processes only new records since the last checkpoint |
| Syntax | `@dlt.table` + regular DataFrame | `@dlt.table` + `dlt.read_stream()` |
| Analogy | A view that is materialised and cached | Structured Streaming table |

#### Expectations — Data Quality

Expectations are named constraints attached to a table:

```python
@dlt.expect("valid_energy_kwh", "energy_kwh > 0")
@dlt.expect_or_drop("non_null_session_id", "session_id IS NOT NULL")
@dlt.expect_or_fail("valid_station_id", "station_id IS NOT NULL")
```

| Decorator | What happens on violation |
|---|---|
| `@dlt.expect` | Warn — row passes through, violation counted in metrics |
| `@dlt.expect_or_drop` | Drop — violating rows removed, counted in metrics |
| `@dlt.expect_or_fail` | Fail — entire pipeline stops |

### DLT Code Pattern

```python
import dlt
from pyspark.sql import functions as F

# Bronze — raw ingestion with Autoloader
@dlt.table(
    name="raw_sessions",
    comment="Raw EV charging session events from ADLS",
    table_properties={"quality": "bronze"}
)
def raw_sessions():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/schema/raw_sessions")
        .load("abfss://bronze@evdatalakedev2.dfs.core.windows.net/api/sessions/")
    )

# Silver — cleaned with expectations
@dlt.table(
    name="sl_sessions",
    comment="Cleaned and typed EV sessions",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("non_null_session_id", "session_id IS NOT NULL")
@dlt.expect("valid_energy",               "energy_kwh > 0")
def sl_sessions():
    return (
        dlt.read_stream("raw_sessions")
        .select(
            F.col("session_id"),
            F.col("energy_kwh").cast("double"),
            F.to_timestamp(F.col("started_at")).alias("started_at_utc"),
            # ... other transforms
        )
    )

# Gold — aggregated mart
@dlt.table(
    name="mart_revenue_by_geo_month",
    comment="Monthly revenue aggregated by city and state",
    table_properties={"quality": "gold"}
)
def mart_revenue():
    return (
        dlt.read("sl_sessions")
        .join(dlt.read("dim_station"), "station_id")
        .groupBy("city", "state_region", F.month("started_at_utc").alias("month"))
        .agg(F.sum("cost_aud").alias("total_revenue_aud"))
    )
```

### DLT Pipeline Modes

| Mode | Description |
|---|---|
| **Development** | Full recompute every run, no checkpoints — good for testing |
| **Production** | Incremental, checkpointed, restarts where it left off |

### DLT vs Manual Notebooks

| | Manual Notebooks (our approach) | Delta Live Tables |
|---|---|---|
| Dependency management | Manual (`dependsOn` in ADF or notebook order) | Automatic — DLT builds a DAG from `dlt.read()` calls |
| Data quality | Custom `WHERE` filters or quarantine logic | Built-in `@dlt.expect` decorators |
| Lineage | Manual documentation | Automatic lineage graph in Databricks UI |
| Error handling | Try/except in code | Expectations + quarantine table per pipeline |
| Incremental logic | Custom watermarks, control tables | Built-in for Streaming Tables |
| Operational overhead | Higher — you manage state, retries, ordering | Lower — DLT manages all of this |
| Flexibility | Full control | Less flexible; declarative style required |
| Cost | Standard DBUs | Slightly higher DLT DBUs |

### When to Use What

**Use manual notebooks + ADF (our approach, Day 3–11) when:**
- You need full control over MERGE logic (SCD Type 2, complex dedup)
- You have complex multi-step transformations that don't fit the declarative model
- You need to integrate tightly with ADF triggers and parameters
- Team is more familiar with imperative Python/SQL

**Use Autoloader + DLT when:**
- You have continuous event streams landing in ADLS (IoT, clickstream, API events)
- You want automatic data quality monitoring with built-in dashboards
- You want zero-code lineage and dependency management
- You are building a new pipeline from scratch and want lower ops overhead

### DLT in the EV Project Context

If we were to re-implement this project with DLT:

```
Bronze DLT:   cloudFiles → raw_sessions, raw_vehicles, raw_stations ... (16 tables)
Silver DLT:   dlt.read_stream("raw_*") + @dlt.expect → sl_sessions, sl_vehicles ...
Gold DLT:     dlt.read("sl_*") → dim_station, dim_customer, fact_charging_session ...
```

The current architecture (ADF ForEach + Databricks notebooks) achieves the same result with more control over MERGE logic and SCD handling, which is why it was the right choice for this project.

---

## Summary

| Concept | Key Point |
|---|---|
| ADF Blob Migration | Same 3-activity pattern (config full, invoices IfCondition, reports IfCondition) repeated across Bronze → Silver → Gold using layer-specific sink datasets |
| Autoloader | Structured Streaming source (`cloudFiles`) that tracks new files via checkpoint — no full directory scan, handles schema evolution automatically |
| Trigger `availableNow` | Makes Autoloader behave like a scheduled batch — process all pending files then stop; ideal for ADF-triggered Databricks jobs |
| DLT Expectations | `@dlt.expect` = warn, `@dlt.expect_or_drop` = remove bad rows, `@dlt.expect_or_fail` = stop pipeline |
| DLT vs Notebooks | DLT = lower ops overhead, built-in lineage and quality; Notebooks = more control over complex MERGE/SCD logic |
