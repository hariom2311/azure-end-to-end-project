# Day 14 — Practice Exercises: Delta Live Tables

> All exercises build on the DLT pipeline in `notebooks/dlt_ev_charging_pipeline.py`.
> Run each exercise in a DLT pipeline triggered run and verify the output in the pipeline UI.

---

## Exercise 1 — Add a New Expectation and Observe DQ Metrics

**Goal:** Add a rule that warns (but does not drop) when `estimated_minutes_to_full` is suspiciously high — over 480 minutes (8 hours) — and observe the violation count in the DLT event log.

Steps:
1. Open `dlt_ev_charging_pipeline.py`
2. Add a new `@dlt.expect` decorator above `silver_vehicle_battery_events` (after the existing ones):

```python
@dlt.expect("reasonable_eta", "estimated_minutes_to_full <= 480")
```

3. Use `expect` (not `expect_or_drop`) — these rows should pass through for analysis
4. Save the notebook and trigger a pipeline run
5. In the DLT UI, click the `silver_vehicle_battery_events` node
6. Check the right panel: how many rows violate `reasonable_eta`?
7. Confirm those rows still appear in the Silver table (not dropped)

**Verify:**
```sql
SELECT estimated_minutes_to_full, COUNT(*) as cnt
FROM ev_catalog.dlt_ev_charging.silver_vehicle_battery_events
WHERE estimated_minutes_to_full > 480
GROUP BY 1
ORDER BY 1 DESC;
```

---

## Exercise 2 — Add a Quarantine Table for Dropped Events

**Goal:** Capture the rows that fail DQ rules (currently just dropped and tracked as a count) into a separate `bronze_vehicle_battery_quarantine` table so engineers can investigate them.

By default DLT drops rows that fail `expect_or_drop` without storing them. To capture them, add a companion bronze table that reads from the same source **without** expectations:

Steps:
1. Add a new table to the notebook **before** `silver_vehicle_battery_events`:

```python
@dlt.table(
    name="bronze_vehicle_battery_quarantine",
    comment="Events that failed Silver DQ rules. For investigation only — not used in Gold.",
    table_properties={"quality": "bronze"},
)
def bronze_vehicle_battery_quarantine():
    return (
        dlt.read_stream("bronze_vehicle_battery_events")
        .filter(
            F.col("event_id").isNull()
            | ~F.col("battery_pct").between(0, 100)
            | ~F.col("charging_rate_kw").between(0, 350)
            | ~F.col("battery_temp_c").between(-10, 80)
            | F.col("event_ts").isNull()
        )
        .withColumn("quarantine_ts", F.current_timestamp())
    )
```

2. Run the pipeline and check:
```sql
SELECT COUNT(*) FROM ev_catalog.dlt_ev_charging.bronze_vehicle_battery_quarantine;
```

**Verify:** The count in `bronze_vehicle_battery_quarantine` matches the dropped row count shown in the DLT UI for `silver_vehicle_battery_events`.

---

## Exercise 3 — Add a 1-Hour Tumbling Window Gold Table

**Goal:** Add a second Gold streaming table that aggregates over a 1-hour window (instead of 5 minutes) for a higher-level view of charging trends per station.

Add to `dlt_ev_charging_pipeline.py`:

```python
@dlt.table(
    name="gold_hourly_station_summary",
    comment="Hourly charging summary per station. Coarser grain than the 5-min charging progress mart.",
    table_properties={"quality": "gold"},
)
def gold_hourly_station_summary():
    return (
        dlt.read_stream("silver_vehicle_battery_events")
        .groupBy(
            F.col("station_id"),
            F.window(F.col("event_ts_ts"), "1 hour"),
        )
        .agg(
            F.countDistinct("vehicle_id")                   .alias("active_vehicles"),
            F.countDistinct("session_id")                   .alias("active_sessions"),
            F.round(F.avg("battery_pct"), 2)               .alias("avg_battery_pct"),
            F.round(F.avg("charging_rate_kw"), 2)          .alias("avg_charging_rate_kw"),
            F.round(F.sum(F.col("charging_rate_kw") / 3600.0), 4).alias("kwh_delivered"),
            F.sum(F.when(F.col("battery_temp_c") > 45.0, 1).otherwise(0)).alias("overtemp_events"),
            F.count("*")                                   .alias("total_events"),
        )
        .select(
            "station_id",
            F.col("window.start").alias("hour_start"),
            F.col("window.end")  .alias("hour_end"),
            "active_vehicles", "active_sessions",
            "avg_battery_pct", "avg_charging_rate_kw",
            "kwh_delivered", "overtemp_events", "total_events",
        )
    )
```

**Verify:**
```sql
SELECT station_id, hour_start, active_vehicles, kwh_delivered
FROM ev_catalog.dlt_ev_charging.gold_hourly_station_summary
ORDER BY hour_start DESC, kwh_delivered DESC
LIMIT 20;
```

---

## Exercise 4 — Switch from Triggered to Continuous Mode

**Goal:** Change the pipeline mode from Triggered (batch) to Continuous (streaming) and observe the difference in latency and cluster behaviour.

Steps:
1. DLT UI → `dlt-ev-charging-pipeline` → Settings → Edit
2. Change **Pipeline mode** from `Triggered` to `Continuous`
3. Click Start
4. With the pipeline running continuously, start the Event Hub producer:
```powershell
$env:EH_CONN_STR = "Endpoint=sb://evh-ev-intelligence-dev.servicebus.windows.net/;SharedAccessKeyName=producer-policy-vehicle-live;SharedAccessKey=<YOUR_SAS_KEY>;EntityPath=vehicle-battery-live"
python azure-end-to-end-project/day13-streaming-eventhub-autoloader/producer/send_vehicle_battery_events.py
```
5. Watch the Silver table row count increase in near-real-time (refresh every 30s)
6. Stop the pipeline and switch back to Triggered mode

**Observe:** In Triggered mode, data appears in batches when the pipeline is manually started or scheduled. In Continuous mode, the cluster stays up and processes events within seconds of them arriving.

---

## Exercise 5 — Add the DLT Pipeline to the Databricks Asset Bundle (Day 16 prep)

**Goal:** Define the DLT pipeline as a DAB resource so it can be deployed via `databricks bundle deploy` — linking Day 14 (DLT) to Day 16 (CI/CD).

Steps:
1. In the Day 16 bundle (`day16-cicd-databricks/databricks/resources/`), create a new file `pipelines.yml`:

```yaml
resources:
  pipelines:
    dlt_ev_charging_pipeline:
      name: "[${bundle.target}] DLT EV Charging Pipeline"
      target: ev_catalog.dlt_ev_charging_${bundle.target}
      continuous: false
      photon: false
      libraries:
        - notebook:
            path: ../../day14-delta-live-tables/notebooks/dlt_ev_charging_pipeline
      clusters:
        - label: default
          autoscale:
            min_workers: 1
            max_workers: 4
          node_type_id: Standard_DS3_v2
      configuration:
        ev.adls_account: evdatalakedev2
        ev.eh_namespace: evh-ev-intelligence-dev
        ev.eh_topic: vehicle-battery-live
      development: ${bundle.target == 'staging'}
```

2. Add the pipeline `include` to `databricks.yml`:
```yaml
include:
  - resources/*.yml   # already picks up pipelines.yml
```

3. Validate: `databricks bundle validate --target staging`
4. Deploy: `databricks bundle deploy --target staging`
5. Verify: DLT UI shows `[dev hariom] DLT EV Charging Pipeline` created

**Verify:** `databricks pipelines list` shows the new pipeline. DLT UI shows the pipeline in the staging workspace.
