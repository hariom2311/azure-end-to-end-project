"""
Day 14 — Vehicle Battery Live Event Producer
Sends simulated vehicle charging events to Azure Event Hubs.

Usage:
    # Set connection string as environment variable (recommended)
    $env:EH_CONN_STR = "Endpoint=sb://evh-ev-intelligence-dev.servicebus.windows.net/;..."

    python send_vehicle_battery_events.py

Requirements:
    pip install azure-eventhub
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from azure.eventhub import EventHubProducerClient, EventData

# ---------------------------------------------------------------------------
# CONFIG — read from environment variable (never hardcode in production)
# ---------------------------------------------------------------------------
CONN_STR   = os.environ.get("EH_CONN_STR", "")   # Full Event Hub connection string
EVENTHUB_NAME = "vehicle-battery-live"             # Entity path / topic name

if not CONN_STR:
    raise EnvironmentError(
        "EH_CONN_STR environment variable is not set.\n"
        "Set it to the Event Hub connection string from Key Vault secret "
        "'eventhub-vehicle-battery-conn-str'.\n\n"
        "PowerShell example:\n"
        "  $env:EH_CONN_STR = 'Endpoint=sb://evh-ev-intelligence-dev...EntityPath=vehicle-battery-live'"
    )

# ---------------------------------------------------------------------------
# VEHICLE SIMULATION CONFIG
# ---------------------------------------------------------------------------
NUM_VEHICLES = 10          # Number of vehicles to simulate in parallel
SEND_INTERVAL_SEC = 1.0    # Seconds between each round of events
PRINT_INTERVAL = 10        # Print summary every N seconds


def build_initial_vehicle_states():
    """
    Creates the starting state for each simulated vehicle.
    Each vehicle has a random starting battery % and a target %.
    Charging rate determines how fast battery climbs per second.
    """
    stations = [f"STN-{str(i).zfill(3)}" for i in range(1, 6)]   # STN-001 to STN-005
    connector_types = {7.4: "AC-Type2", 22.0: "AC-Type2", 50.0: "DC-CCS2", 150.0: "DC-CCS2"}

    vehicles = []
    for i in range(1, NUM_VEHICLES + 1):
        station_id  = random.choice(stations)
        charger_num = random.randint(1, 4)
        rate_kw     = random.choice([7.4, 22.0, 50.0, 150.0])
        battery_cap = random.choice([40, 60, 75, 82, 100])  # kWh

        # battery_pct gain per second ≈ (rate_kw / battery_cap_kWh) * 100 / 3600
        pct_per_sec = (rate_kw / battery_cap) * 100 / 3600

        vehicles.append({
            "vehicle_id":                   f"VH-{str(i).zfill(4)}",
            "session_id":                   f"SES-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{str(i).zfill(4)}",
            "station_id":                   station_id,
            "charger_id":                   f"{station_id}-CHG-{str(charger_num).zfill(2)}",
            "battery_pct":                  round(random.uniform(10.0, 45.0), 2),  # start low
            "state_of_charge_target_pct":   random.choice([80, 85, 90, 95, 100]),
            "charging_rate_kw":             rate_kw,
            "battery_cap_kwh":              battery_cap,
            "pct_per_sec":                  pct_per_sec,
            "connector_type":               connector_types[rate_kw],
            "active":                       True,
        })
    return vehicles


def make_event(vehicle: dict) -> dict:
    """
    Builds one JSON event for the current state of a vehicle.
    Battery temperature increases slightly as battery % rises (realistic).
    """
    # Estimated minutes remaining
    pct_remaining = vehicle["state_of_charge_target_pct"] - vehicle["battery_pct"]
    if pct_remaining <= 0:
        eta_minutes = 0
    else:
        eta_minutes = int((pct_remaining / 100) * (vehicle["battery_cap_kwh"] / vehicle["charging_rate_kw"]) * 60)

    # Battery temperature: starts ~28°C, rises ~0.02°C per % charged above 60%
    base_temp = 28.0
    heat_factor = max(0, vehicle["battery_pct"] - 60) * 0.025
    battery_temp = round(base_temp + heat_factor + random.uniform(-0.3, 0.3), 1)

    now_utc = datetime.now(timezone.utc)

    return {
        "event_id":                    f"evt-{uuid.uuid4().hex[:8]}-{now_utc.strftime('%Y%m%d-%H%M%S')}",
        "vehicle_id":                  vehicle["vehicle_id"],
        "session_id":                  vehicle["session_id"],
        "station_id":                  vehicle["station_id"],
        "charger_id":                  vehicle["charger_id"],
        "battery_pct":                 round(vehicle["battery_pct"], 2),
        "charging_rate_kw":            vehicle["charging_rate_kw"],
        "battery_temp_c":              battery_temp,
        "state_of_charge_target_pct":  vehicle["state_of_charge_target_pct"],
        "estimated_minutes_to_full":   eta_minutes,
        "event_ts":                    now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def send_events(producer: EventHubProducerClient, events: list[dict]) -> int:
    """
    Sends a list of event dicts as a single EventDataBatch to Event Hubs.
    Returns the number of events actually sent.
    """
    batch = producer.create_batch()
    for event in events:
        batch.add(EventData(json.dumps(event, ensure_ascii=False)))
    producer.send_batch(batch)
    return len(events)


def main():
    print("=" * 65)
    print("  Day 14 — Vehicle Battery Live Producer")
    print(f"  Target:   {EVENTHUB_NAME}")
    print(f"  Vehicles: {NUM_VEHICLES}")
    print(f"  Interval: {SEND_INTERVAL_SEC}s per round")
    print("=" * 65)
    print("Starting producer... Press Ctrl+C to stop.\n")

    vehicles       = build_initial_vehicle_states()
    total_sent     = 0
    rounds         = 0
    last_print_ts  = time.time()

    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONN_STR,
        eventhub_name=EVENTHUB_NAME,
    )

    with producer:
        while True:
            active_vehicles = [v for v in vehicles if v["active"]]

            if not active_vehicles:
                print(f"\nAll {NUM_VEHICLES} vehicles reached their target charge level.")
                print(f"Total events sent: {total_sent:,}")
                break

            # Build events for all active vehicles this tick
            events_this_round = []
            for v in active_vehicles:
                events_this_round.append(make_event(v))

                # Advance battery % for next tick
                v["battery_pct"] = min(
                    100.0,
                    v["battery_pct"] + v["pct_per_sec"] * SEND_INTERVAL_SEC
                )

                # Mark inactive if target reached
                if v["battery_pct"] >= v["state_of_charge_target_pct"]:
                    v["active"] = False
                    print(
                        f"  [{datetime.now().strftime('%H:%M:%S')}] "
                        f"{v['vehicle_id']} reached target {v['state_of_charge_target_pct']}% — done."
                    )

            # Send batch to Event Hubs
            try:
                count = send_events(producer, events_this_round)
                total_sent += count
                rounds     += 1
            except Exception as exc:
                print(f"  [ERROR] Failed to send batch: {exc}")
                time.sleep(5)
                continue

            # Print summary every PRINT_INTERVAL seconds
            now = time.time()
            if now - last_print_ts >= PRINT_INTERVAL:
                sample = active_vehicles[:3]  # show first 3 vehicles as a sample
                sample_str = " | ".join(
                    f"{v['vehicle_id']}: {v['battery_pct']:.1f}%"
                    for v in sample
                )
                print(
                    f"  [{datetime.now().strftime('%H:%M:%S')}] "
                    f"Sent {total_sent:,} events total ({len(active_vehicles)} active) | {sample_str}"
                )
                last_print_ts = now

            time.sleep(SEND_INTERVAL_SEC)

    print("\nProducer finished.")


if __name__ == "__main__":
    main()
