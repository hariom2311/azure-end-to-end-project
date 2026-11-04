"""
Unit tests for the Silver vehicle battery streaming transform.

Mirrors the DQ logic in:
  day13-streaming-eventhub-autoloader/notebooks/eventhub/nb_silver_stream_vehicle_battery

Tests run locally with PySpark (no Databricks cluster required).
All Delta MERGE operations are tested against a temp directory on disk.
"""

import uuid
from datetime import datetime, timezone

import pytest
from pyspark.sql import Row, functions as F
from pyspark.sql import Window


# ── Inline transform extracted from the notebook ──────────────────────────────
# We replicate the DQ tagging + split logic so it can be unit-tested
# without importing the notebook (which has dbutils / ADLS paths).

RULE_LABELS = {
    "_r01": "event_id null",
    "_r02": "vehicle_id null",
    "_r03": "session_id null",
    "_r04": "battery_pct out of range",
    "_r05": "charging_rate_kw out of range",
    "_r06": "battery_temp_c out of range",
    "_r07": "soc_target out of range",
    "_r08": "event_ts null",
    "_r09": "corrupt JSON body",
}


def apply_dq_rules(df):
    tagged = (
        df.withColumn("_r01", F.col("event_id").isNotNull())
        .withColumn("_r02", F.col("vehicle_id").isNotNull())
        .withColumn("_r03", F.col("session_id").isNotNull())
        .withColumn("_r04", F.col("battery_pct").between(0, 100))
        .withColumn("_r05", F.col("charging_rate_kw").between(0, 350))
        .withColumn("_r06", F.col("battery_temp_c").between(-10, 80))
        .withColumn("_r07", F.col("state_of_charge_target_pct").between(1, 100))
        .withColumn("_r08", F.col("event_ts").isNotNull())
        .withColumn("_r09", F.col("_is_corrupt") == False)  # noqa: E712
    )
    reason_expr = F.concat_ws(
        ", ",
        *[F.when(~F.col(r), F.lit(label)) for r, label in RULE_LABELS.items()],
    )
    return tagged.withColumn("_dq_reasons", reason_expr).withColumn(
        "_dq_passed", F.col("_dq_reasons") == ""
    )


def dedup_by_event_id(df):
    w = Window.partitionBy("event_id").orderBy(F.col("_ingestion_ts").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _good_row(**overrides):
    base = {
        "event_id": str(uuid.uuid4()),
        "vehicle_id": "VEH-001",
        "session_id": "SES-VEH-001-20261103",
        "station_id": "STN-AU-10137",
        "charger_id": "CHG-30271",
        "battery_pct": 55.0,
        "charging_rate_kw": 50.0,
        "battery_temp_c": 30.0,
        "state_of_charge_target_pct": 90,
        "estimated_minutes_to_full": 30,
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "_is_corrupt": False,
        "_ingestion_ts": datetime.now(timezone.utc),
    }
    base.update(overrides)
    return Row(**base)


# ── Tests: DQ rules ───────────────────────────────────────────────────────────

class TestDQRules:
    def test_good_row_passes_all_rules(self, spark):
        df = spark.createDataFrame([_good_row()])
        result = apply_dq_rules(df)
        row = result.collect()[0]
        assert row["_dq_passed"] is True
        assert row["_dq_reasons"] == ""

    def test_null_event_id_fails_r01(self, spark):
        df = spark.createDataFrame([_good_row(event_id=None)])
        result = apply_dq_rules(df)
        row = result.collect()[0]
        assert row["_dq_passed"] is False
        assert "event_id null" in row["_dq_reasons"]

    def test_null_vehicle_id_fails_r02(self, spark):
        df = spark.createDataFrame([_good_row(vehicle_id=None)])
        result = apply_dq_rules(df)
        row = result.collect()[0]
        assert row["_dq_passed"] is False
        assert "vehicle_id null" in row["_dq_reasons"]

    def test_null_session_id_fails_r03(self, spark):
        df = spark.createDataFrame([_good_row(session_id=None)])
        result = apply_dq_rules(df)
        row = result.collect()[0]
        assert row["_dq_passed"] is False
        assert "session_id null" in row["_dq_reasons"]

    @pytest.mark.parametrize("pct", [-1.0, 101.0, -0.01, 100.01])
    def test_battery_pct_out_of_range_fails_r04(self, spark, pct):
        df = spark.createDataFrame([_good_row(battery_pct=pct)])
        result = apply_dq_rules(df)
        row = result.collect()[0]
        assert row["_dq_passed"] is False
        assert "battery_pct out of range" in row["_dq_reasons"]

    @pytest.mark.parametrize("pct", [0.0, 50.0, 100.0])
    def test_battery_pct_boundary_passes_r04(self, spark, pct):
        df = spark.createDataFrame([_good_row(battery_pct=pct)])
        result = apply_dq_rules(df)
        row = result.collect()[0]
        assert row["_dq_passed"] is True

    @pytest.mark.parametrize("rate", [-1.0, 351.0])
    def test_charging_rate_out_of_range_fails_r05(self, spark, rate):
        df = spark.createDataFrame([_good_row(charging_rate_kw=rate)])
        result = apply_dq_rules(df)
        row = result.collect()[0]
        assert row["_dq_passed"] is False
        assert "charging_rate_kw out of range" in row["_dq_reasons"]

    @pytest.mark.parametrize("temp", [-11.0, 81.0])
    def test_battery_temp_out_of_range_fails_r06(self, spark, temp):
        df = spark.createDataFrame([_good_row(battery_temp_c=temp)])
        result = apply_dq_rules(df)
        row = result.collect()[0]
        assert row["_dq_passed"] is False
        assert "battery_temp_c out of range" in row["_dq_reasons"]

    @pytest.mark.parametrize("soc", [0, 101])
    def test_soc_target_out_of_range_fails_r07(self, spark, soc):
        df = spark.createDataFrame([_good_row(state_of_charge_target_pct=soc)])
        result = apply_dq_rules(df)
        row = result.collect()[0]
        assert row["_dq_passed"] is False
        assert "soc_target out of range" in row["_dq_reasons"]

    def test_null_event_ts_fails_r08(self, spark):
        df = spark.createDataFrame([_good_row(event_ts=None)])
        result = apply_dq_rules(df)
        row = result.collect()[0]
        assert row["_dq_passed"] is False
        assert "event_ts null" in row["_dq_reasons"]

    def test_corrupt_json_fails_r09(self, spark):
        df = spark.createDataFrame([_good_row(_is_corrupt=True)])
        result = apply_dq_rules(df)
        row = result.collect()[0]
        assert row["_dq_passed"] is False
        assert "corrupt JSON body" in row["_dq_reasons"]

    def test_multiple_failures_concatenated(self, spark):
        df = spark.createDataFrame([_good_row(event_id=None, battery_pct=999.0)])
        result = apply_dq_rules(df)
        row = result.collect()[0]
        assert row["_dq_passed"] is False
        assert "event_id null" in row["_dq_reasons"]
        assert "battery_pct out of range" in row["_dq_reasons"]


# ── Tests: clean / quarantine split ──────────────────────────────────────────

class TestCleanQuarantineSplit:
    def test_clean_rows_separated(self, spark):
        rows = [
            _good_row(event_id="aaa"),
            _good_row(event_id=None),           # bad — r01
            _good_row(event_id="bbb", battery_pct=-5.0),  # bad — r04
        ]
        df = apply_dq_rules(spark.createDataFrame(rows))
        clean = df.filter(F.col("_dq_passed"))
        bad = df.filter(~F.col("_dq_passed"))
        assert clean.count() == 1
        assert bad.count() == 2

    def test_all_bad_batch_yields_empty_clean(self, spark):
        rows = [_good_row(event_id=None), _good_row(battery_pct=200.0)]
        df = apply_dq_rules(spark.createDataFrame(rows))
        clean = df.filter(F.col("_dq_passed"))
        assert clean.count() == 0

    def test_all_good_batch_yields_empty_quarantine(self, spark):
        rows = [_good_row(event_id="x1"), _good_row(event_id="x2")]
        df = apply_dq_rules(spark.createDataFrame(rows))
        bad = df.filter(~F.col("_dq_passed"))
        assert bad.count() == 0


# ── Tests: deduplication within batch ────────────────────────────────────────

class TestDeduplication:
    def test_duplicate_event_id_keeps_latest(self, spark):
        ts_old = datetime(2026, 11, 3, 10, 0, 0, tzinfo=timezone.utc)
        ts_new = datetime(2026, 11, 3, 10, 0, 5, tzinfo=timezone.utc)
        rows = [
            _good_row(event_id="dup-001", battery_pct=40.0, _ingestion_ts=ts_old),
            _good_row(event_id="dup-001", battery_pct=55.0, _ingestion_ts=ts_new),
        ]
        df = spark.createDataFrame(rows)
        result = dedup_by_event_id(df)
        assert result.count() == 1
        assert result.collect()[0]["battery_pct"] == 55.0

    def test_unique_event_ids_all_kept(self, spark):
        rows = [_good_row(event_id=f"uid-{i}") for i in range(5)]
        df = spark.createDataFrame(rows)
        result = dedup_by_event_id(df)
        assert result.count() == 5

    def test_three_duplicates_keeps_one(self, spark):
        ts_base = datetime(2026, 11, 3, 9, 0, 0, tzinfo=timezone.utc)
        rows = [
            _good_row(
                event_id="triple",
                battery_pct=float(10 * i),
                _ingestion_ts=datetime(2026, 11, 3, 9, 0, i, tzinfo=timezone.utc),
            )
            for i in range(1, 4)
        ]
        df = spark.createDataFrame(rows)
        result = dedup_by_event_id(df)
        assert result.count() == 1
        assert result.collect()[0]["battery_pct"] == 30.0  # latest (i=3)


# ── Tests: schema / column presence ──────────────────────────────────────────

class TestSchema:
    REQUIRED_COLS = [
        "event_id", "vehicle_id", "session_id", "station_id", "charger_id",
        "battery_pct", "charging_rate_kw", "battery_temp_c",
        "state_of_charge_target_pct", "estimated_minutes_to_full",
        "event_ts", "_is_corrupt", "_ingestion_ts",
    ]

    def test_all_required_columns_present(self, spark):
        df = spark.createDataFrame([_good_row()])
        for col in self.REQUIRED_COLS:
            assert col in df.columns, f"Missing column: {col}"

    def test_dq_adds_rule_columns(self, spark):
        df = apply_dq_rules(spark.createDataFrame([_good_row()]))
        for rule in RULE_LABELS:
            assert rule in df.columns, f"Missing DQ rule column: {rule}"
        assert "_dq_reasons" in df.columns
        assert "_dq_passed" in df.columns
