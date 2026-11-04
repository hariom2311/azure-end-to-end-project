"""
Unit tests for the Gold charging-progress mart aggregation.

Mirrors the aggregation logic in:
  day13-streaming-eventhub-autoloader/notebooks/eventhub/nb_gold_stream_charging_progress

Tests run locally with PySpark (no Databricks cluster required).
"""

import uuid
from datetime import datetime, timezone, timedelta

import pytest
from pyspark.sql import Row, functions as F


# ── Inline Gold aggregation extracted from the notebook ───────────────────────

WINDOW_DURATION = "5 minutes"


def build_gold_mart_df(df):
    """
    Applies the Gold mart aggregation to a static DataFrame.
    Returns the mart DataFrame (no Delta MERGE — tested separately).
    """
    df = df.withColumn("event_ts_ts", F.to_timestamp(F.col("event_ts")))

    mart = (
        df.groupBy(
            F.col("vehicle_id"),
            F.col("station_id"),
            F.window(F.col("event_ts_ts"), WINDOW_DURATION),
        )
        .agg(
            F.min("battery_pct").alias("battery_pct_start"),
            F.max("battery_pct").alias("battery_pct_end"),
            F.round(F.avg("battery_pct"), 2).alias("battery_pct_avg"),
            F.round(F.avg("charging_rate_kw"), 2).alias("avg_charging_rate_kw"),
            F.round(F.avg("battery_temp_c"), 2).alias("avg_temp_c"),
            F.round(F.max("battery_temp_c"), 2).alias("max_temp_c"),
            F.max(
                F.when(F.col("battery_temp_c") > 45.0, 1).otherwise(0)
            ).alias("overtemp_flag"),
            F.min("estimated_minutes_to_full").alias("eta_minutes_to_full"),
            F.first("session_id").alias("session_id"),
            F.first("charger_id").alias("charger_id"),
            F.count("*").alias("event_count"),
        )
        .select(
            F.col("vehicle_id"),
            F.col("station_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("session_id"),
            F.col("charger_id"),
            F.col("battery_pct_start"),
            F.col("battery_pct_end"),
            F.col("battery_pct_avg"),
            F.col("avg_charging_rate_kw"),
            F.col("avg_temp_c"),
            F.col("max_temp_c"),
            F.col("overtemp_flag").cast("boolean"),
            F.col("eta_minutes_to_full"),
            F.col("event_count"),
        )
    )
    return mart


# ── Helpers ───────────────────────────────────────────────────────────────────

_BASE_TS = datetime(2026, 11, 3, 10, 0, 0, tzinfo=timezone.utc)


def _silver_row(vehicle_id="VEH-001", station_id="STN-AU-10137",
                battery_pct=50.0, charging_rate_kw=50.0,
                battery_temp_c=30.0, soc_target=90,
                eta_minutes=30, offset_seconds=0, **overrides):
    ts = (_BASE_TS + timedelta(seconds=offset_seconds)).isoformat()
    base = {
        "event_id": str(uuid.uuid4()),
        "vehicle_id": vehicle_id,
        "session_id": f"SES-{vehicle_id}-20261103",
        "station_id": station_id,
        "charger_id": "CHG-30271",
        "battery_pct": battery_pct,
        "charging_rate_kw": charging_rate_kw,
        "battery_temp_c": battery_temp_c,
        "state_of_charge_target_pct": soc_target,
        "estimated_minutes_to_full": eta_minutes,
        "event_ts": ts,
        "sl_ingested_at": _BASE_TS,
        "sl_batch_id": "0",
    }
    base.update(overrides)
    return Row(**base)


# ── Tests: aggregation correctness ───────────────────────────────────────────

class TestGoldAggregation:
    def test_single_event_produces_one_window_row(self, spark):
        df = spark.createDataFrame([_silver_row()])
        mart = build_gold_mart_df(df)
        assert mart.count() == 1

    def test_battery_pct_start_end_avg(self, spark):
        rows = [
            _silver_row(battery_pct=40.0, offset_seconds=0),
            _silver_row(battery_pct=50.0, offset_seconds=60),
            _silver_row(battery_pct=60.0, offset_seconds=120),
        ]
        df = spark.createDataFrame(rows)
        mart = build_gold_mart_df(df).collect()[0]
        assert mart["battery_pct_start"] == 40.0
        assert mart["battery_pct_end"] == 60.0
        assert mart["battery_pct_avg"] == 50.0

    def test_event_count_correct(self, spark):
        rows = [_silver_row(offset_seconds=i * 30) for i in range(6)]
        df = spark.createDataFrame(rows)
        mart = build_gold_mart_df(df).collect()[0]
        assert mart["event_count"] == 6

    def test_avg_charging_rate(self, spark):
        rows = [
            _silver_row(charging_rate_kw=100.0, offset_seconds=0),
            _silver_row(charging_rate_kw=200.0, offset_seconds=30),
        ]
        df = spark.createDataFrame(rows)
        mart = build_gold_mart_df(df).collect()[0]
        assert mart["avg_charging_rate_kw"] == 150.0

    def test_eta_is_minimum_across_events(self, spark):
        rows = [
            _silver_row(eta_minutes=60, offset_seconds=0),
            _silver_row(eta_minutes=45, offset_seconds=60),
            _silver_row(eta_minutes=30, offset_seconds=120),
        ]
        df = spark.createDataFrame(rows)
        mart = build_gold_mart_df(df).collect()[0]
        assert mart["eta_minutes_to_full"] == 30


# ── Tests: overtemperature flag ───────────────────────────────────────────────

class TestOvertempFlag:
    def test_no_overtemp_when_all_below_45(self, spark):
        rows = [_silver_row(battery_temp_c=t, offset_seconds=i * 30)
                for i, t in enumerate([30.0, 35.0, 44.9])]
        df = spark.createDataFrame(rows)
        mart = build_gold_mart_df(df).collect()[0]
        assert mart["overtemp_flag"] is False

    def test_overtemp_when_any_reading_above_45(self, spark):
        rows = [
            _silver_row(battery_temp_c=30.0, offset_seconds=0),
            _silver_row(battery_temp_c=45.1, offset_seconds=60),  # triggers flag
        ]
        df = spark.createDataFrame(rows)
        mart = build_gold_mart_df(df).collect()[0]
        assert mart["overtemp_flag"] is True

    def test_exactly_45_does_not_trigger_overtemp(self, spark):
        df = spark.createDataFrame([_silver_row(battery_temp_c=45.0)])
        mart = build_gold_mart_df(df).collect()[0]
        assert mart["overtemp_flag"] is False

    def test_max_temp_captured(self, spark):
        rows = [
            _silver_row(battery_temp_c=25.0, offset_seconds=0),
            _silver_row(battery_temp_c=50.0, offset_seconds=30),
            _silver_row(battery_temp_c=35.0, offset_seconds=60),
        ]
        df = spark.createDataFrame(rows)
        mart = build_gold_mart_df(df).collect()[0]
        assert mart["max_temp_c"] == 50.0
        assert mart["overtemp_flag"] is True


# ── Tests: window boundaries ──────────────────────────────────────────────────

class TestWindowBoundaries:
    def test_events_in_same_5min_window_grouped_together(self, spark):
        rows = [
            _silver_row(offset_seconds=0),    # 10:00:00
            _silver_row(offset_seconds=120),  # 10:02:00 — same window
            _silver_row(offset_seconds=240),  # 10:04:00 — same window
        ]
        df = spark.createDataFrame(rows)
        mart = build_gold_mart_df(df)
        assert mart.count() == 1

    def test_events_spanning_two_windows_produce_two_rows(self, spark):
        rows = [
            _silver_row(offset_seconds=0),    # 10:00:00 — window 1
            _silver_row(offset_seconds=360),  # 10:06:00 — window 2
        ]
        df = spark.createDataFrame(rows)
        mart = build_gold_mart_df(df)
        assert mart.count() == 2

    def test_different_vehicles_produce_separate_rows(self, spark):
        rows = [
            _silver_row(vehicle_id="VEH-001", offset_seconds=0),
            _silver_row(vehicle_id="VEH-002", offset_seconds=30),
        ]
        df = spark.createDataFrame(rows)
        mart = build_gold_mart_df(df)
        assert mart.count() == 2

    def test_different_stations_produce_separate_rows(self, spark):
        rows = [
            _silver_row(station_id="STN-A", offset_seconds=0),
            _silver_row(station_id="STN-B", offset_seconds=30),
        ]
        df = spark.createDataFrame(rows)
        mart = build_gold_mart_df(df)
        assert mart.count() == 2

    def test_same_vehicle_different_stations_separate(self, spark):
        rows = [
            _silver_row(vehicle_id="VEH-001", station_id="STN-A", offset_seconds=0),
            _silver_row(vehicle_id="VEH-001", station_id="STN-B", offset_seconds=30),
        ]
        df = spark.createDataFrame(rows)
        mart = build_gold_mart_df(df)
        assert mart.count() == 2


# ── Tests: output schema ──────────────────────────────────────────────────────

class TestGoldSchema:
    EXPECTED_COLS = [
        "vehicle_id", "station_id", "window_start", "window_end",
        "session_id", "charger_id",
        "battery_pct_start", "battery_pct_end", "battery_pct_avg",
        "avg_charging_rate_kw", "avg_temp_c", "max_temp_c",
        "overtemp_flag", "eta_minutes_to_full", "event_count",
    ]

    def test_all_expected_columns_present(self, spark):
        df = spark.createDataFrame([_silver_row()])
        mart = build_gold_mart_df(df)
        for col in self.EXPECTED_COLS:
            assert col in mart.columns, f"Missing Gold column: {col}"

    def test_overtemp_flag_is_boolean(self, spark):
        df = spark.createDataFrame([_silver_row()])
        mart = build_gold_mart_df(df)
        dtype = dict(mart.dtypes)["overtemp_flag"]
        assert dtype == "boolean"

    def test_window_start_before_window_end(self, spark):
        df = spark.createDataFrame([_silver_row()])
        mart = build_gold_mart_df(df).collect()[0]
        assert mart["window_start"] < mart["window_end"]
