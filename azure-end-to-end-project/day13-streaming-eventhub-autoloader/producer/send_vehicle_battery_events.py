"""
send_vehicle_battery_events.py
──────────────────────────────────────────────────────────────────────────────
Local Python producer — sends simulated EV vehicle battery telemetry events
to Azure Event Hub topic: vehicle-battery-live

SETUP
  pip install azure-eventhub

  Set environment variable before running:
  Windows PowerShell:
    $env:EH_CONN_STR = "<your producer SAS connection string>"

  The connection string comes from:
    Event Hub Namespace → vehicle-battery-live → Shared Access Policies
    → producer-policy → Connection string–primary key

RUN
  python send_vehicle_battery_events.py

  Sends 10 events/second (1 per vehicle), runs indefinitely.
  Ctrl+C to stop.

EVENT SCHEMA (per event, JSON):
  event_id                  : UUID (unique per reading)
  vehicle_id                : VEH-001 to VEH-010
  session_id                : SES-<vehicle_num>-<YYYYMMDD>
  station_id                : STN-AU-<fixed per vehicle>
  charger_id                : CHG-<fixed per vehicle>
  battery_pct               : float 0-100 (increases each tick)
  charging_rate_kw          : float 7-150
  battery_temp_c            : float (base 25°C + noise)
  state_of_charge_target_pct: int 80-100
  estimated_minutes_to_full  : int (decreases as battery fills)
  event_ts                  : ISO8601 UTC timestamp
"""

import os
import json
import time
import random
import uuid
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData, EventDataBatch

# ── Config ────────────────────────────────────────────────────────────────────
EH_CONN_STR   = os.environ["EH_CONN_STR"]          # set via env var — never hardcode
EH_TOPIC_NAME = "vehicle-battery-live"
NUM_VEHICLES  = 10
SEND_INTERVAL = 1.0                                 # seconds between batches

# ── Vehicle state (persistent across ticks) ───────────────────────────────────
VEHICLES = {}
for i in range(1, NUM_VEHICLES + 1):
    vid = f"VEH-{i:03d}"
    VEHICLES[vid] = {
        "vehicle_id":    vid,
        "station_id":    f"STN-AU-{(10000 + i * 137) % 90000 + 10000:05d}",
        "charger_id":    f"CHG-{(20000 + i * 251) % 80000 + 10000:05d}",
        "battery_cap_kwh":   random.choice([40.0, 60.0, 75.0, 100.0]),
        "charging_rate_kw":  round(random.uniform(7.0, 150.0), 1),
        "soc_target_pct":    random.randint(80, 100),
        "battery_pct":       round(random.uniform(5.0, 30.0), 2),
        "base_temp_c":       round(random.uniform(22.0, 28.0), 1),
        "session_date":      datetime.now(timezone.utc).strftime("%Y%m%d"),
    }


def make_event(state: dict) -> dict:
    battery_pct = state["battery_pct"]
    rate_kw     = state["charging_rate_kw"]
    cap_kwh     = state["battery_cap_kwh"]
    soc_target  = state["soc_target_pct"]

    remaining_kwh  = max(0.0, (soc_target - battery_pct) / 100.0 * cap_kwh)
    est_min_full   = int((remaining_kwh / rate_kw) * 60) if rate_kw > 0 else 0
    temp_noise     = round(random.gauss(0, 0.8), 2)
    charging_boost = (battery_pct / 100.0) * 3.0

    return {
        "event_id":                   str(uuid.uuid4()),
        "vehicle_id":                 state["vehicle_id"],
        "session_id":                 f"SES-{state['vehicle_id']}-{state['session_date']}",
        "station_id":                 state["station_id"],
        "charger_id":                 state["charger_id"],
        "battery_pct":                round(battery_pct, 2),
        "charging_rate_kw":           rate_kw,
        "battery_temp_c":             round(state["base_temp_c"] + charging_boost + temp_noise, 2),
        "state_of_charge_target_pct": soc_target,
        "estimated_minutes_to_full":  est_min_full,
        "event_ts":                   datetime.now(timezone.utc).isoformat(),
    }


def tick_battery(state: dict) -> None:
    increment = (state["charging_rate_kw"] / state["battery_cap_kwh"]) / 3600.0 * SEND_INTERVAL * 100
    state["battery_pct"] = min(state["soc_target_pct"], state["battery_pct"] + increment)
    if state["battery_pct"] >= state["soc_target_pct"]:
        state["battery_pct"] = round(random.uniform(5.0, 20.0), 2)
        state["session_date"] = datetime.now(timezone.utc).strftime("%Y%m%d")


# ── Main send loop ─────────────────────────────────────────────────────────────
def main():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EH_CONN_STR,
        eventhub_name=EH_TOPIC_NAME,
    )
    print(f"Connected to Event Hub: {EH_TOPIC_NAME}")
    print(f"Sending {NUM_VEHICLES} events every {SEND_INTERVAL}s. Ctrl+C to stop.\n")

    tick = 0
    with producer:
        while True:
            batch: EventDataBatch = producer.create_batch()
            events_sent = []

            for vid, state in VEHICLES.items():
                event = make_event(state)
                batch.add(EventData(json.dumps(event)))
                events_sent.append(event)
                tick_battery(state)

            producer.send_batch(batch)
            tick += 1

            sample = events_sent[0]
            print(
                f"[Tick {tick:04d}] {len(events_sent)} events sent | "
                f"Sample: {sample['vehicle_id']} "
                f"bat={sample['battery_pct']:.1f}% "
                f"rate={sample['charging_rate_kw']}kW "
                f"temp={sample['battery_temp_c']}°C "
                f"eta={sample['estimated_minutes_to_full']}min"
            )
            time.sleep(SEND_INTERVAL)


if __name__ == "__main__":
    main()
