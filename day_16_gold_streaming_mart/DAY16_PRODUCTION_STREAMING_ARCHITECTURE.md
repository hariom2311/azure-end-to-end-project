# Day 16 — Real Production Streaming Architecture
### VoltGrid AU — How This Pipeline Works in the Real World

---

## The Real Source: Physical EV Chargers in Australia

In our dev setup we used a Python script to simulate events.  
In production, the source is **real EV charger hardware** deployed across 120 franchise stations in Australia.

**Hardware deployed by VoltGrid AU:**

| Charger Model | Type | Power | Locations | OCPP Version |
|---|---|---|---|---|
| ABB Terra 360 | DC Ultra-Fast | 150–360 kW | Sydney CBD, Melbourne Docklands, Brisbane CBD | OCPP 2.0.1 |
| Tritium VEEFIL-RT | DC Fast | 50 kW | Highway corridors, Westfield centres | OCPP 1.6 |
| Schneider EVlink Pro AC | AC | 7.4 kW / 22 kW | Long-stay car parks, shopping centres | OCPP 1.6 |

---

## Real IoT Event from a Charger

A Tritium VEEFIL-RT at **Westfield Bondi Junction, Sydney** sends this over OCPP 1.6 every second:

```json
{
  "messageTypeId": 2,
  "uniqueId": "uuid-7f3a8b2c",
  "action": "MeterValues",
  "payload": {
    "connectorId": 1,
    "transactionId": 98765,
    "meterValue": [{
      "timestamp": "2026-07-30T23:30:42Z",
      "sampledValue": [
        { "value": "67.4",  "measurand": "SoC",                           "unit": "Percent" },
        { "value": "50.2",  "measurand": "Power.Active.Import",           "unit": "kW"      },
        { "value": "31.2",  "measurand": "Temperature",                   "unit": "Celsius" },
        { "value": "415.3", "measurand": "Voltage",                       "unit": "V"       },
        { "value": "121.0", "measurand": "Current.Import",                "unit": "A"       },
        { "value": "12847", "measurand": "Energy.Active.Import.Register", "unit": "Wh"      }
      ]
    }]
  }
}
```

> **Note:** Timestamp is UTC. Charger is in Sydney (AEST = UTC+10) but all IoT events are normalised to UTC before storage. `2026-07-30T23:30:42Z` = `2026-07-31 09:30:42 AEST`.

---

## End-to-End Production Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│  FIELD LAYER — 120 stations across Australia                           │
│                                                                        │
│  STN-SYD-BJ (Westfield Bondi Junction, NSW)                           │
│  ├── CHG-SYD-BJ-01  ABB Terra 360    360 kW  DC                       │
│  ├── CHG-SYD-BJ-02  ABB Terra 360    360 kW  DC                       │
│  └── CHG-SYD-BJ-03  Tritium VEEFIL   50 kW   DC                       │
│                                                                        │
│  STN-MEL-DK (Melbourne Docklands, VIC)                                │
│  ├── CHG-MEL-DK-01  Schneider 22kW   22 kW   AC                       │
│  └── CHG-MEL-DK-02  Tritium VEEFIL   50 kW   DC                       │
│                                                                        │
│  + 118 more stations (NSW, VIC, QLD, WA, SA, ACT)                    │
│  Total: 300 chargers                                                   │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │
                               │  OCPP 1.6 / 2.0 over WSS (port 443)
                               │  X.509 certificate per device
                               │  1 MeterValues message/charger/second
                               │  ~180 events/sec at peak (7–9 PM AEST)
                               ▼
┌────────────────────────────────────────────────────────────────────────┐
│  AZURE IOT HUB — iothub-ev-intelligence-prod                          │
│  SKU: S2 Standard  |  6M messages/day  |  Region: Australia East      │
│                                                                        │
│  Device Registry                                                       │
│  ├── 300 devices registered (one per charger)                         │
│  ├── Per-device X.509 certificate — charger identity verified         │
│  └── Device twin per charger:                                         │
│      { "station_id": "STN-SYD-BJ",                                    │
│        "state": "NSW", "city": "Sydney",                              │
│        "max_kw": 360, "connector_type": "CCS2",                       │
│        "firmware": "3.4.1", "franchise_id": "FRN-042" }              │
│                                                                        │
│  Message Enrichment (adds twin data to every event)                   │
│  └── Injects: station_id, state, city, franchise_id into payload      │
│                                                                        │
│  Message Routing Rules                                                 │
│  ├── action = MeterValues    → endpoint: telemetry-eventhub           │
│  ├── action = FaultNotif.    → endpoint: faults-eventhub              │
│  └── action = StatusNotif.   → endpoint: status-eventhub             │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │
                               │  Built-in Event Hub-compatible endpoint
                               │  AMQP  |  Consumer group: databricks-prod
                               ▼
┌────────────────────────────────────────────────────────────────────────┐
│  AZURE EVENT HUBS — evh-ev-intelligence-prod                          │
│  SKU: Standard  |  4 Throughput Units (auto-inflate to 20)            │
│  Region: Australia East                                                │
│                                                                        │
│  Topics:                                                               │
│  ├── iot-telemetry       10 partitions  7-day retention  ← MeterValues│
│  ├── charger-faults       4 partitions  7-day retention               │
│  ├── connector-status     4 partitions  1-day retention               │
│  └── vehicle-battery-live 4 partitions  1-day retention  ← Day 14/15 │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │
                               │  azure-eventhubs-spark connector
                               │  DBR 13.3 LTS  |  Scala 2.12
                               ▼
┌────────────────────────────────────────────────────────────────────────┐
│  AZURE DATABRICKS — dbw-ev-intelligence-prod                          │
│  Region: Australia East                                                │
│                                                                        │
│  job-ev-streaming-pipeline  (Continuous, 3 parallel tasks)            │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  Task 1: bronze-stream                                           │  │
│  │  Cluster: stream-bronze  (autoscale 2–8 workers DS3_v2)         │  │
│  │  Trigger: every 30 seconds                                       │  │
│  │  Reads:   Event Hubs iot-telemetry                               │  │
│  │  Writes:  bronze/iot/charger_telemetry_raw/  (Delta, append)    │  │
│  │           partitioned by event_date / charger_id                 │  │
│  └─────────────────────────────────────────────────────────────────┘  │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  Task 2: silver-stream                                           │  │
│  │  Cluster: stream-silver  (autoscale 2–6 workers DS3_v2)         │  │
│  │  Trigger: every 60 seconds                                       │  │
│  │  Reads:   bronze/iot/charger_telemetry_raw/                      │  │
│  │  DQ:      9 rules — null checks, range validation, overtemp     │  │
│  │  Dedup:   charger_id + event_ts (OCPP sends retries)            │  │
│  │  Writes:  silver/sl_iot_charger_telemetry/  (Delta, MERGE)      │  │
│  │  Bad rows: silver/quarantine/charger_telemetry_invalid/          │  │
│  └─────────────────────────────────────────────────────────────────┘  │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  Task 3: gold-stream                                             │  │
│  │  Cluster: stream-gold  (autoscale 2–4 workers DS3_v2)           │  │
│  │  Trigger: every 120 seconds                                      │  │
│  │  Reads:   silver/sl_iot_charger_telemetry/ (or vehicle_battery) │  │
│  │  Agg:     5-min tumbling window per charger + station           │  │
│  │  Writes:  gold/mart_charging_progress_live/  (Delta, MERGE)     │  │
│  │  Key:     charger_id + station_id + window_start                │  │
│  └─────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │
               ┌───────────────┴──────────────┐
               ▼                              ▼
┌──────────────────────────┐    ┌──────────────────────────────────────┐
│  AZURE SYNAPSE ANALYTICS │    │  AZURE COSMOS DB (Mongo API)         │
│  Serverless SQL pool      │    │                                      │
│                           │    │  Collection: session_live            │
│  External table on Gold   │    │  Partition key: station_id           │
│  Delta via ABFSS          │    │  TTL: 24 hours (auto-expire)        │
│         │                 │    │  Throughput: 1000 RU/s               │
│         ▼                 │    │  Read SLA: < 2 seconds               │
│  Power BI DirectQuery     │    │         │                            │
│  Live Charging dashboard  │    │  VoltGrid Driver Mobile App         │
│  Refresh: every 5 min     │    │  ├── Battery % + time to full       │
│                           │    │  ├── Station map (live availability) │
│  Franchise Owner view:    │    │  └── Cost so far (inc. 10% GST)     │
│  ├── Charger uptime %     │    └──────────────────────────────────────┘
│  ├── Active sessions      │
│  └── Revenue today (AUD)  │
└──────────────────────────┘
```

---

## Real Example: Tesla Model Y at Westfield Bondi Junction

**Date:** 2026-07-31  
**Time:** 09:30 AEST (= 23:30 UTC previous day)  
**Vehicle:** Tesla Model Y (NSW rego: CXF-52K)  
**Station:** Westfield Bondi Junction, Oxford St, Bondi Junction NSW 2022  
**Charger:** ABB Terra 360 — CHG-SYD-BJ-02 (CCS2 connector, max 360 kW)  
**Driver target:** Charge from 22% → 80% (needs range to Byron Bay, 797 km)

### What IoT Hub receives every second (47-minute session):

```
09:30:00 AEST → battery: 22.0% | rate: 150.0 kW | temp: 28.1°C | est: 47 min
09:30:01 AEST → battery: 22.1% | rate: 149.8 kW | temp: 28.2°C | est: 46 min
09:30:02 AEST → battery: 22.2% | rate: 150.1 kW | temp: 28.2°C | est: 46 min
...            (1 event/second — 300 events per 5-min window)
09:34:59 AEST → battery: 29.6% | rate: 148.2 kW | temp: 31.4°C | est: 41 min
...
09:50:00 AEST → battery: 54.2% | rate: 120.0 kW | temp: 37.8°C | est: 23 min
                (rate drops here — lithium taper curve above 50%)
...
10:10:00 AEST → battery: 74.5% | rate:  28.0 kW | temp: 41.2°C | est:  6 min
10:17:00 AEST → battery: 80.0% | rate:  12.1 kW | temp: 39.2°C | est:  0 min ← session ends
```

**Total:** 2,820 events  |  34.2 kWh delivered  |  Cost: AUD $10.26 (inc. 10% GST)

---

### What the Gold Mart Contains (5-min window 09:30–09:35):

```
vehicle_id:               VH-SYD-CXF52K
station_id:               STN-SYD-BJ
window_start:             2026-07-30 23:30:00 UTC   (= 09:30 AEST)
window_end:               2026-07-30 23:35:00 UTC   (= 09:35 AEST)
min_battery_pct:          22.0
max_battery_pct:          29.6
avg_battery_pct:          25.8
avg_charging_rate_kw:     149.6
avg_battery_temp_c:       29.8
max_battery_temp_c:       31.4
overtemp_flag:            0         ← safe (< 45°C threshold)
min_est_minutes_to_full:  41
session_id:               SES-20260731-SYD-BJ-002
charger_id:               CHG-SYD-BJ-02
event_count:              300
_gold_updated_at:         2026-07-30 23:33:42 UTC
```

---

### What the Power BI Live Dashboard Shows (5-min refresh):

```
┌─────────────────────────────────────────────┐
│  Live Charging — Westfield Bondi Junction   │
│  CHG-SYD-BJ-02  |  ABB Terra 360           │
│                                             │
│  ████████░░░░░░░░  67.4%  → target 80%     │
│  150 kW  |  31°C  |  41 min to target      │
│  Energy: 34.2 kWh  |  Cost: AUD $10.26     │
└─────────────────────────────────────────────┘
```

---

### What the Driver Mobile App Shows (Cosmos DB, < 2 sec):

```json
{
  "session_id":       "SES-20260731-SYD-BJ-002",
  "station_name":     "Westfield Bondi Junction",
  "station_address":  "500 Oxford St, Bondi Junction NSW 2022",
  "charger_id":       "CHG-SYD-BJ-02",
  "battery_pct":      67.4,
  "target_pct":       80,
  "est_minutes":      41,
  "charging_kw":      150.0,
  "energy_kwh":       34.2,
  "cost_aud":         10.26,
  "gst_aud":          0.93,
  "updated_at":       "2026-07-31T09:30:42+10:00"
}
```

---

## Overtemp Alert: Real Production Flow

The Gold mart `overtemp_flag` column triggers an automated alert chain.

**Threshold:** `max_battery_temp_c > 45°C` in any 5-minute window

```
Gold mart row: overtemp_flag = 1, max_battery_temp_c = 52.3°C
     │
     ▼
Azure Monitor Metric Alert
  Rule: Gold mart overtemp_flag = 1 in last 5 minutes
  Severity: 2 (Warning)
     │
     ├──▶ Email → ops-team@voltgrid.com.au
     ├──▶ SMS → on-call station manager (+61 4xx xxx xxx)
     └──▶ Microsoft Teams webhook → #charger-alerts
             "⚠️ OVERTEMP: CHG-SYD-BJ-02 at Westfield Bondi Junction
              Temp: 52.3°C | Threshold: 45°C
              Vehicle: VH-SYD-CXF52K | Session: SES-20260731-SYD-BJ-002
              Auto-action: Charging rate reduced to 50 kW"
     │
     ▼
Azure Logic App (automated response)
  → OCPP ChangeConfiguration: MaxChargingPower = 50000 W (reduce from 150 kW)
  If temp > 60°C:
  → OCPP RemoteStopTransaction (safety cutoff per AS/NZS 62196 Australian standard)
```

---

## AEST / AEDT Time Zone Handling

All events stored in UTC. Reports shown in local time via DimTime.

| State | Timezone | UTC Offset (Standard) | UTC Offset (DST, Oct–Apr) |
|---|---|---|---|
| NSW, VIC, ACT, TAS | AEST / AEDT | +10:00 | +11:00 |
| QLD | AEST (no DST) | +10:00 | +10:00 |
| WA | AWST | +8:00 | +8:00 |
| SA | ACST / ACDT | +9:30 | +10:30 |

```python
# In Silver notebook — store UTC, derive AEST for reporting
from pyspark.sql.functions import to_utc_timestamp, from_utc_timestamp

# Charger sends local time → convert to UTC for Bronze/Silver storage
df = df.withColumn('event_ts_utc',
    to_utc_timestamp(col('event_ts'), 'Australia/Sydney'))

# In Gold — add AEST column for Power BI reporting
gold_df = gold_df.withColumn('window_start_aest',
    from_utc_timestamp(col('window_start'), 'Australia/Sydney'))
```

---

## Production Scale Numbers

| Metric | Value |
|---|---|
| Total chargers | 300 |
| Active chargers at peak (7–9 PM AEST) | ~180 (60% utilisation) |
| Events per second at peak | ~180 |
| Events per hour at peak | ~648,000 |
| Bronze rows per day | ~15.5 million |
| Silver rows per day | ~15.5 million (dedup removes OCPP retries) |
| Gold mart rows per day | ~51,840 (300 chargers × 12 windows/hr × 24 hr) |
| Bronze trigger | 30 seconds |
| Silver trigger | 60 seconds |
| Gold trigger | 120 seconds |
| End-to-end latency (charger → Gold) | < 3 minutes |
| Cosmos DB update cadence | Every 5 minutes (Gold → Cosmos sync job) |
| Mobile app read latency | < 2 seconds (Cosmos DB) |

---

## Dev vs Production: Key Differences

| Aspect | Dev (this project) | Production (VoltGrid AU) |
|---|---|---|
| Event source | `send_vehicle_battery_events.py` | ABB / Tritium / Schneider charger firmware |
| Protocol | Python SDK → Event Hubs direct | OCPP 1.6/2.0 → IoT Hub → Event Hubs |
| Device auth | SAS key in env var | X.509 certificate per device |
| Events/sec | 10 | ~180 peak |
| Cluster | 1 shared `dev-cluster` | 3 separate autoscaling clusters |
| AEST timezone | Not handled | `to_utc_timestamp('Australia/Sydney')` in Silver |
| Overtemp | Flag in Gold row | Flag + Azure Monitor + Logic App auto-cutoff |
| GST | Not in this mart | `cost_aud * 0.10 / 1.10` added in Gold |
| Gold consumer | Power BI reads Delta directly | Synapse Analytics + Cosmos DB sync |
| Monthly cost | ~AUD $20–30 (dev, when running) | ~AUD $800–1,200 (prod autoscaling) |

**The transformation logic is identical.** DQ rules, dedup, MERGE, 5-minute windows —  
the same code runs in prod at 18× the event volume.
