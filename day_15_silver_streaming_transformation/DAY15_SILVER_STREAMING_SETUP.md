# Day 15 — Silver Streaming Transformation: vehicle_battery_live
**Session:** ~1 hour | **Prerequisite:** Day 14 Bronze stream running and landing Parquet files in ADLS

---

## What Gets Built

| Layer | Path | Format | Write mode |
|---|---|---|---|
| Bronze (source) | `bronze/event-stream/vehicle_battery_live/` | Delta | Read-only (Day 14 wrote this) |
| Silver (clean) | `silver/sl_vehicle_battery_live/` | Delta | MERGE upsert on `event_id` |
| Quarantine (bad) | `silver/quarantine/vehicle_battery_invalid/` | Delta | Append |

---

## Architecture: Why Silver Matters

```
Bronze                          Silver                           Quarantine
──────                          ──────                           ──────────
Raw JSON from Event Hub         Validated, deduped, typed        Bad rows with rejection reason
- May have nulls                - 9 DQ rules applied             - NULL fields
- May have corrupt JSON         - battery_pct 0-100 enforced     - Out-of-range values
- May have duplicate events     - dedup by event_id              - Corrupt Bronze JSON
- No range validation           - MERGE = idempotent             - Stored for investigation
```

The Bronze → Silver step is the most important quality gate in the medallion architecture.
Gold and Power BI dashboards only read from Silver — they never touch Bronze.

---

## DQ Rules Applied Per Row

| Rule | Check | Reject reason |
|---|---|---|
| R01 | `event_id` not null | `NULL_event_id` |
| R02 | `vehicle_id` not null | `NULL_vehicle_id` |
| R03 | `session_id` not null | `NULL_session_id` |
| R04 | `battery_pct` between 0.0 and 100.0 | `INVALID_battery_pct` |
| R05 | `charging_rate_kw` between 0 and 350 | `INVALID_charging_rate_kw` |
| R06 | `battery_temp_c` between -10 and 80 | `INVALID_battery_temp_c` |
| R07 | `state_of_charge_target_pct` between 1 and 100 | `INVALID_soc_target` |
| R08 | `event_ts` not null | `NULL_event_ts` |
| R09 | `_is_corrupt` flag from Bronze is False | `CORRUPT_bronze_json` |

A row can fail multiple rules — reasons are pipe-separated: `NULL_event_id|INVALID_battery_pct`

---

## Silver Columns

| Column | Source | Description |
|---|---|---|
| `event_id` | Bronze | Dedup key |
| `vehicle_id` | Bronze | |
| `session_id` | Bronze | |
| `station_id` | Bronze | |
| `charger_id` | Bronze | |
| `battery_pct` | Bronze | Validated 0–100 |
| `charging_rate_kw` | Bronze | Validated 0–350 kW |
| `battery_temp_c` | Bronze | Validated -10 to 80°C |
| `state_of_charge_target_pct` | Bronze | Validated 1–100 |
| `estimated_minutes_to_full` | Bronze | |
| `event_ts` | Bronze | UTC ISO-8601 string |
| `event_date` | Bronze | Partition column |
| `_silver_ingested_at` | Added in Silver | Timestamp of Silver write |
| `_dq_passed` | Added in Silver | `"true"` for all rows in Silver (bad rows go to quarantine) |

---

## Steps

### Step 1 — Ensure Silver container exists in ADLS

1. Azure Portal → `evdatalakedev` → **Storage browser** → **Blob containers**
2. Check if `silver` container exists
3. If not: click **+ Add container** → name: `silver` → **Create**

### Step 2 — Import the Silver notebook into Databricks

1. Databricks workspace → **Workspace** → your user folder
2. Click **⋮** → **Import** → select `01_silver_vehicle_battery_transformation.ipynb`
3. Attach to `dev-cluster`

### Step 3 — Run cells 1 to 7 in order

| Cell | What it does |
|---|---|
| 1 | Load Key Vault secrets |
| 2 | Configure ADLS OAuth |
| 3 | Define Bronze / Silver / Quarantine / Checkpoint paths |
| 4 | Import PySpark functions and DeltaTable |
| 5 | Define `transform_to_silver()` — the core DQ + MERGE function |
| 6 | Start Silver streaming query (trigger every 60 seconds) |
| 7 | Monitor loop — prints batch progress |

### Step 4 — Verify Silver data (Cell 8 or separate notebook)

```python
silver_df = spark.read.format('delta').load(SILVER_PATH)
print(silver_df.count())
silver_df.groupBy('vehicle_id').agg(F.min('battery_pct'), F.max('battery_pct')).show()
```

---

## How the Streaming Pipeline Flows End-to-End

```
send_vehicle_battery_events.py   (local, 10 events/sec)
        │
        ▼  AMQP
Azure Event Hubs — vehicle-battery-live
        │
        ▼  every 30 seconds
Bronze Delta (Day 14 notebook)
  bronze/event-stream/vehicle_battery_live/
        │
        ▼  every 60 seconds
Silver Notebook (Day 15) — THIS NOTEBOOK
  → DQ 9 rules
  → dedup by event_id
  → MERGE into silver/sl_vehicle_battery_live/
  → bad rows → silver/quarantine/vehicle_battery_invalid/
        │
        ▼  every 120 seconds
Gold Notebook (Day 16)
  → 5-min window aggregation
  → MERGE into gold/mart_charging_progress_live/
```

---

## Key Concepts

**Why MERGE instead of append for Silver?**
If the Bronze stream replays (cluster restart, checkpoint issue), the same events would be appended again. MERGE on `event_id` means re-processing the same batch is completely safe — it updates the existing row instead of creating a duplicate.

**Why foreachBatch instead of native writeStream?**
Native `writeStream.format("delta")` only supports append or complete output modes. MERGE requires running a `DeltaTable.merge()` call — only possible inside `foreachBatch`.

**Why 60-second trigger instead of 30?**
Bronze runs every 30 seconds. Setting Silver to 60 seconds ensures each Silver batch sees at least one full Bronze micro-batch worth of data. Running both at 30 seconds risks Silver processing an empty slice while Bronze is still writing.
