# Databricks notebook source
# =============================================================================
# Delta Live Tables — EV Charging Intelligence Pipeline
# =============================================================================
# Pipeline : dlt_ev_charging_pipeline
# Project  : VoltGrid AU — EV Charging Franchise Intelligence Platform
#
# Layers:
#   Bronze  → raw vehicle battery events from Event Hub (streaming)
#   Silver  → DQ-validated, deduplicated events (streaming)
#   Gold    → 5-minute tumbling window charging progress mart (streaming)
#             + daily revenue-by-station aggregation (batch)
#
# How to run:
#   Databricks UI → Delta Live Tables → Create Pipeline
#   Source code   → this file
#   Pipeline mode → Triggered (batch) or Continuous (streaming)
#   Target schema → ev_catalog.dlt_ev_charging
# =============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType, BooleanType
)

# =============================================================================
# CONFIGURATION
# =============================================================================
# In a real pipeline these come from pipeline parameters set in the UI / DAB
ADLS_ACCOUNT     = spark.conf.get("ev.adls_account",     "evdatalakedev2")
EH_NAMESPACE     = spark.conf.get("ev.eh_namespace",     "evh-ev-intelligence-dev")
EH_TOPIC         = spark.conf.get("ev.eh_topic",         "vehicle-battery-live")
EH_CONN_STR_KEY  = spark.conf.get("ev.eh_conn_str_key",  "ev-kv-scope:eh-conn-str")

BRONZE_PATH  = f"abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net/event-stream/vehicle_battery_live/"
SILVER_PATH  = f"abfss://silver@{ADLS_ACCOUNT}.dfs.core.windows.net/sl_vehicle_battery_live/"

# =============================================================================
# RAW EVENT SCHEMA
# Schema of JSON events produced by send_vehicle_battery_events.py
# =============================================================================
RAW_SCHEMA = StructType([
    StructField("event_id",                   StringType(),  True),
    StructField("vehicle_id",                 StringType(),  True),
    StructField("session_id",                 StringType(),  True),
    StructField("station_id",                 StringType(),  True),
    StructField("charger_id",                 StringType(),  True),
    StructField("battery_pct",                DoubleType(),  True),
    StructField("charging_rate_kw",           DoubleType(),  True),
    StructField("battery_temp_c",             DoubleType(),  True),
    StructField("state_of_charge_target_pct", IntegerType(), True),
    StructField("estimated_minutes_to_full",  IntegerType(), True),
    StructField("event_ts",                   StringType(),  True),
])

# =============================================================================
# LAYER 1 — BRONZE (Streaming)
# Reads raw JSON from the Bronze Delta path written by the Event Hub AutoLoader
# or Kafka consumer from Day 13.
#
# DLT handles:
#   - Checkpointing automatically
#   - Schema enforcement via the declared schema
#   - Quarantine of rows that cannot be parsed (rescued to _rescue_data)
# =============================================================================

@dlt.table(
    name="bronze_vehicle_battery_events",
    comment="Raw EV battery telemetry events landed from Event Hub. Append-only. No transforms.",
    table_properties={
        "quality":                   "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
def bronze_vehicle_battery_events():
    return (
        spark.readStream
        .format("delta")
        .load(BRONZE_PATH)
    )


# =============================================================================
# LAYER 2 — SILVER (Streaming)
# Applies 9 DQ rules, marks each row pass/fail, routes bad rows to quarantine.
# Clean rows are MERGED on event_id for idempotency.
#
# DLT Expectations enforce DQ rules declaratively — no manual if/else needed.
# Violations are tracked in the pipeline event log automatically.
# =============================================================================

# ── DQ expectations ──────────────────────────────────────────────────────────
# drop_on_violation → bad rows are dropped from silver and tracked in event log
# warn_on_violation  → bad rows pass through but are flagged (use for monitoring)
# fail_on_violation  → entire pipeline batch fails if any row violates the rule

@dlt.expect_or_drop("valid_event_id",        "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_vehicle_id",       "vehicle_id IS NOT NULL")
@dlt.expect_or_drop("valid_session_id",       "session_id IS NOT NULL")
@dlt.expect_or_drop("valid_battery_pct",      "battery_pct BETWEEN 0 AND 100")
@dlt.expect_or_drop("valid_charging_rate",    "charging_rate_kw BETWEEN 0 AND 350")
@dlt.expect_or_drop("valid_battery_temp",     "battery_temp_c BETWEEN -10 AND 80")
@dlt.expect_or_drop("valid_soc_target",       "state_of_charge_target_pct BETWEEN 1 AND 100")
@dlt.expect_or_drop("valid_event_ts",         "event_ts IS NOT NULL")
@dlt.expect("not_corrupt",                    "_rescue_data IS NULL")   # warn only — keep for analysis
@dlt.table(
    name="silver_vehicle_battery_events",
    comment="DQ-validated EV battery events. 9 rules enforced. Bad rows dropped and tracked in DLT event log.",
    table_properties={
        "quality":                        "silver",
        "delta.enableChangeDataFeed":     "true",   # enables downstream CDC consumers
        "pipelines.autoOptimize.managed": "true",
    },
)
def silver_vehicle_battery_events():
    return (
        dlt.read_stream("bronze_vehicle_battery_events")
        .withColumn("event_ts_ts",    F.to_timestamp(F.col("event_ts")))
        .withColumn("sl_ingested_at", F.current_timestamp())
        .withColumn("battery_pct",    F.round(F.col("battery_pct"), 2))
        .withColumn("battery_temp_c", F.round(F.col("battery_temp_c"), 2))
        # Derive session date for partitioning
        .withColumn("session_date",   F.to_date(F.col("event_ts_ts")))
        .select(
            "event_id", "vehicle_id", "session_id", "station_id", "charger_id",
            "battery_pct", "charging_rate_kw", "battery_temp_c",
            "state_of_charge_target_pct", "estimated_minutes_to_full",
            "event_ts", "event_ts_ts", "session_date", "sl_ingested_at",
        )
    )


# =============================================================================
# LAYER 3a — GOLD Streaming Mart
# 5-minute tumbling window aggregation per vehicle + station.
# Grain: one row per (vehicle_id, station_id, 5-minute window).
# MERGEs on (vehicle_id, station_id, window_start) so late-arriving data
# within the window updates rather than duplicates the row.
# =============================================================================

@dlt.table(
    name="gold_charging_progress_live",
    comment="5-min tumbling window charging progress per vehicle+station. Updates as Silver events arrive.",
    table_properties={
        "quality":                        "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.targetFileSize":           "134217728",   # 128 MB target file size
    },
)
def gold_charging_progress_live():
    return (
        dlt.read_stream("silver_vehicle_battery_events")
        .groupBy(
            F.col("vehicle_id"),
            F.col("station_id"),
            F.window(F.col("event_ts_ts"), "5 minutes"),
        )
        .agg(
            F.min("battery_pct")                     .alias("battery_pct_start"),
            F.max("battery_pct")                     .alias("battery_pct_end"),
            F.round(F.avg("battery_pct"), 2)         .alias("battery_pct_avg"),
            F.round(F.avg("charging_rate_kw"), 2)    .alias("avg_charging_rate_kw"),
            F.round(F.avg("battery_temp_c"), 2)      .alias("avg_temp_c"),
            F.round(F.max("battery_temp_c"), 2)      .alias("max_temp_c"),
            # Overtemperature: any reading above 45°C in this window
            F.max(F.when(F.col("battery_temp_c") > 45.0, 1).otherwise(0))
             .cast(BooleanType())                    .alias("overtemp_flag"),
            F.min("estimated_minutes_to_full")       .alias("eta_minutes_to_full"),
            F.first("session_id")                    .alias("session_id"),
            F.first("charger_id")                    .alias("charger_id"),
            F.count("*")                             .alias("event_count"),
        )
        .select(
            "vehicle_id", "station_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end")  .alias("window_end"),
            "session_id", "charger_id",
            "battery_pct_start", "battery_pct_end", "battery_pct_avg",
            "avg_charging_rate_kw", "avg_temp_c", "max_temp_c",
            "overtemp_flag", "eta_minutes_to_full", "event_count",
        )
    )


# =============================================================================
# LAYER 3b — GOLD Batch Mart (read from Silver, not streaming)
# Daily revenue proxy: total kWh delivered per station per day.
# Grain: one row per (station_id, session_date).
# This is a materialized view that DLT refreshes on each pipeline trigger.
#
# kWh proxy formula:
#   charging_rate_kw × (1 second / 3600) × event_count
#   = approximate energy delivered in the sampling window
# =============================================================================

@dlt.table(
    name="gold_daily_station_energy",
    comment="Daily kWh delivered per station — batch mart refreshed on each pipeline run.",
    table_properties={
        "quality":   "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_daily_station_energy():
    return (
        # dlt.read() (not read_stream) → batch snapshot of Silver at trigger time
        dlt.read("silver_vehicle_battery_events")
        .groupBy("station_id", "session_date")
        .agg(
            F.countDistinct("vehicle_id")                            .alias("unique_vehicles"),
            F.countDistinct("session_id")                           .alias("unique_sessions"),
            F.round(
                F.sum(F.col("charging_rate_kw") / 3600.0), 4
            )                                                       .alias("total_kwh_delivered"),
            F.round(F.avg("charging_rate_kw"), 2)                  .alias("avg_charging_rate_kw"),
            F.round(F.avg("battery_temp_c"), 2)                    .alias("avg_temp_c"),
            F.sum(
                F.when(F.col("battery_temp_c") > 45.0, 1).otherwise(0)
            )                                                       .alias("overtemp_event_count"),
            F.count("*")                                            .alias("total_event_count"),
            F.min("event_ts_ts")                                    .alias("first_event_ts"),
            F.max("event_ts_ts")                                    .alias("last_event_ts"),
        )
        .withColumn("pipeline_updated_at", F.current_timestamp())
    )


# =============================================================================
# LAYER 3c — GOLD Overtemperature Alert View
# Materialised view: stations with overtemp events in the last 24 hours.
# Downstream: Power BI alert dashboard, Azure Monitor webhook.
# =============================================================================

@dlt.view(
    name="gold_overtemp_alerts_24h",
    comment="Vehicles with any battery_temp_c > 45°C in the past 24 hours. Feeds alert dashboard.",
)
def gold_overtemp_alerts_24h():
    return (
        dlt.read("gold_charging_progress_live")
        .filter(F.col("overtemp_flag") == True)         # noqa: E712
        .filter(
            F.col("window_start") >= F.date_sub(F.current_timestamp().cast("date"), 1)
        )
        .select(
            "vehicle_id", "station_id", "session_id",
            "window_start", "window_end",
            "max_temp_c", "avg_temp_c",
            "battery_pct_start", "battery_pct_end",
            "event_count",
        )
        .orderBy(F.col("max_temp_c").desc())
    )
