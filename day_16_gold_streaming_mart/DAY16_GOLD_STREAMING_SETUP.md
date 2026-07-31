# Day 16 — Gold Streaming Mart: mart_charging_progress_live
**Session:** ~1 hour | **Prerequisite:** Day 15 Silver stream running and writing to `silver/sl_vehicle_battery_live/`

---

## What Gets Built

| Layer | Path | Grain | Trigger |
|---|---|---|---|
| Silver (source) | `silver/sl_vehicle_battery_live/` | 1 row per event | 60s (Day 15) |
| Gold mart | `gold/mart_charging_progress_live/` | 1 row per vehicle + station + 5-min window | 120s |

---

## Why This Mart Exists

The Silver table has one row per event — 10 events/second = 600 rows/minute = 36,000 rows/hour.
Power BI and mobile apps cannot query 36,000 rows every second. The Gold mart pre-aggregates
into 5-minute windows: instead of thousands of raw events, the dashboard reads ~10 rows
(one per vehicle), each representing the latest 5-minute summary.

```
Silver: 36,000 rows/hour (raw events)
           │
           ▼  5-min window aggregation
Gold mart: ~120 rows/hour (12 windows/hour × 10 vehicles)
           │
           ├──▶ Power BI Live Charging dashboard (refreshes every 5 min)
           └──▶ Cosmos DB session_live collection (mobile app, <2 sec reads)
```

---

## Gold Mart Schema — mart_charging_progress_live

| Column | Type | Description |
|---|---|---|
| `vehicle_id` | string | Partition key |
| `station_id` | string | Station where vehicle is plugged in |
| `window_start` | timestamp | Start of 5-minute window (merge key with vehicle_id + station_id) |
| `window_end` | timestamp | End of 5-minute window |
| `max_battery_pct` | float | Highest battery % seen in this window |
| `min_battery_pct` | float | Lowest battery % seen in this window |
| `avg_battery_pct` | float | Average battery % across all events in window |
| `avg_charging_rate_kw` | float | Average power draw in this window |
| `avg_battery_temp_c` | float | Average battery temperature |
| `max_battery_temp_c` | float | Peak temperature — used for overtemp alert |
| `overtemp_flag` | int | 1 if any event had battery_temp_c > 45°C, else 0 |
| `min_est_minutes_to_full` | int | Minimum (best case) minutes remaining to full charge |
| `session_id` | string | Active charging session ID |
| `charger_id` | string | Charger connector |
| `event_count` | long | How many Silver events fed this window |
| `_gold_updated_at` | timestamp | When this row was last updated |
| `_batch_id` | long | Which streaming batch last updated this row |

---

## The 5-Minute Window Explained

```
Timeline:  09:00  09:01  09:02  09:03  09:04  09:05  09:06  ...
                                                       │
Window 1:  [─────────────────────────────────────────]│
           09:00 ─────────────────────── 09:05        │
                                                       │
Window 2:                                             [─────... 09:10]
```

Each event is assigned to the window containing its `event_ts`.
A vehicle charging from 09:00 to 09:05 generates ~300 events (10/sec × 300 sec).
All 300 collapse into one Gold row for window `09:00–09:05`.

**Tumbling vs sliding:** We use tumbling (non-overlapping) windows because the Power BI
dashboard tiles are independent 5-minute summaries, not rolling averages.

---

## Merge Key: Why vehicle_id + station_id + window_start

A single 5-minute window (09:00–09:05) will receive data from multiple Silver micro-batches:
- Silver batch at 09:01 → partial window (1 min of data)
- Silver batch at 09:02 → partial window (2 min of data)
- ...
- Silver batch at 09:05 → complete window (5 min of data)

Without MERGE, each Silver batch would INSERT a new row → 5 duplicate Gold rows per window.
With MERGE on `vehicle_id + station_id + window_start`:
- First batch: INSERT
- Subsequent batches: UPDATE the same row with fresh aggregates

Final Gold row for window 09:00–09:05 = complete 5-minute picture.

---

## Steps

### Step 1 — Ensure Gold container exists in ADLS

1. Azure Portal → `evdatalakedev` → **Storage browser** → **Blob containers**
2. Check if `gold` container exists
3. If not: **+ Add container** → name: `gold` → **Create**

### Step 2 — Import the Gold notebook into Databricks

1. Databricks workspace → your user folder → **Import**
2. Select `01_gold_charging_progress_live_mart.ipynb`
3. Attach to `dev-cluster`

### Step 3 — Run cells 1 to 7 in order

| Cell | What it does |
|---|---|
| 1 | Load Key Vault secrets |
| 2 | Configure ADLS OAuth |
| 3 | Define Silver / Gold / Checkpoint paths |
| 4 | Import PySpark window functions and DeltaTable |
| 5 | Define `build_gold_mart()` — 5-min aggregation + MERGE |
| 6 | Start Gold streaming query (trigger every 120 seconds) |
| 7 | Monitor loop |

### Step 4 — Verify Gold mart (Cell 8 or separate notebook)

```python
gold_df = spark.read.format('delta').load(GOLD_PATH)
print(gold_df.count())  # expect: vehicles × windows processed
gold_df.orderBy(col('window_start').desc()).show(10)
```

---

## Running All Three Layers Together

You need **three separate notebooks** open simultaneously (each in its own browser tab):

| Notebook | Trigger | Status indicator |
|---|---|---|
| Day 14 — Bronze | 30s | Cell 8 printing `[Batch N] Rows this batch: 225` |
| Day 15 — Silver | 60s | Cell 7 printing `[Batch N] Clean merged: X` |
| Day 16 — Gold | 120s | Cell 7 printing `[Batch N] Silver rows in: X` |

All three attach to the same `dev-cluster`. Each runs its own streaming query independently.

---

## Production vs This Setup

| Aspect | This project (dev) | Production |
|---|---|---|
| Trigger cadence | Bronze 30s / Silver 60s / Gold 120s | Same or tuned per SLA |
| Python producer | Local script simulating 10 vehicles | Real charger telemetry via Azure IoT Hub |
| Gold consumer | Power BI reads Delta directly | Synapse Analytics + Cosmos DB sync job |
| Overtemp alert | Flag in Gold mart row | Azure Monitor alert → Teams notification |
| Cluster | Single `dev-cluster` for all notebooks | Separate job clusters per layer |
| Cost | ~₹22/hour (one cluster) | Separate autoscaling clusters per layer |

The architecture is identical — only the data source and compute scale differ.
