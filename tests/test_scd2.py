"""
Tests for the SCD Type 2 engine.

These are the tests that matter most in the repo: SCD2 is the component where
a subtle bug produces plausible-looking output and silently corrupts every
point-in-time question anyone asks afterwards.
"""
from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import functions as F

from src.silver.scd2 import HIGH_DATE, apply_scd2, as_of, current_view, initial_load

TRACKED = ["city", "phone"]
SCHEMA = "patient_id string, city string, phone string, updated_at timestamp"


def make(spark, rows):
    return spark.createDataFrame(rows, schema=SCHEMA)


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s)


def test_initial_load_single_version(spark):
    src = make(spark, [("P1", "Austin", "111", ts("2024-01-01T00:00:00"))])
    dim = initial_load(src, "patient_id", TRACKED, "updated_at")
    row = dim.collect()[0]
    assert row["version_number"] == 1
    assert row["is_current"] is True
    assert str(row["effective_to"]) == HIGH_DATE


def test_change_opens_new_version_and_closes_old(spark):
    src = make(spark, [
        ("P1", "Austin", "111", ts("2024-01-01T00:00:00")),
        ("P1", "Dallas", "111", ts("2024-06-01T00:00:00")),
    ])
    dim = initial_load(src, "patient_id", TRACKED, "updated_at").orderBy("version_number")
    v1, v2 = dim.collect()

    assert v1["city"] == "Austin" and v1["is_current"] is False
    assert v2["city"] == "Dallas" and v2["is_current"] is True
    # v1 must close exactly one second before v2 opens: no gap, no overlap.
    assert v1["effective_to"] == ts("2024-05-31T23:59:59")
    assert v2["effective_from"] == ts("2024-06-01T00:00:00")


def test_unchanged_attributes_do_not_create_a_version(spark):
    """A source that re-exports an identical row must not inflate history."""
    src = make(spark, [
        ("P1", "Austin", "111", ts("2024-01-01T00:00:00")),
        ("P1", "Austin", "111", ts("2024-06-01T00:00:00")),
        ("P1", "Austin", "111", ts("2024-09-01T00:00:00")),
    ])
    dim = initial_load(src, "patient_id", TRACKED, "updated_at")
    assert dim.count() == 1


def test_untracked_column_change_is_ignored(spark):
    """Only tracked columns trigger a version. Otherwise every batch churns."""
    src = make(spark, [
        ("P1", "Austin", "111", ts("2024-01-01T00:00:00")),
        ("P1", "Austin", "999", ts("2024-06-01T00:00:00")),
    ])
    dim = initial_load(src, "patient_id", ["city"], "updated_at")
    assert dim.count() == 1


def test_merge_into_existing_dimension(spark):
    target = initial_load(
        make(spark, [("P1", "Austin", "111", ts("2024-01-01T00:00:00"))]),
        "patient_id", TRACKED, "updated_at")
    incoming = make(spark, [("P1", "Dallas", "111", ts("2024-06-01T00:00:00"))])

    merged = apply_scd2(target, incoming, "patient_id", TRACKED, "updated_at")
    assert merged.count() == 2
    assert current_view(merged).collect()[0]["city"] == "Dallas"


def test_merge_is_idempotent(spark):
    """
    Re-running the same batch must not create a duplicate version.

    A pipeline gets retried. If a retry adds history, the dimension grows
    without bound and every point-in-time join starts fanning out.
    """
    src = make(spark, [("P1", "Austin", "111", ts("2024-01-01T00:00:00"))])
    dim = initial_load(src, "patient_id", TRACKED, "updated_at")
    again = apply_scd2(dim, src, "patient_id", TRACKED, "updated_at")
    assert again.count() == 1


def test_exactly_one_current_row_per_key(spark):
    src = make(spark, [
        ("P1", "Austin", "111", ts("2024-01-01T00:00:00")),
        ("P1", "Dallas", "111", ts("2024-06-01T00:00:00")),
        ("P2", "Reno", "222", ts("2024-02-01T00:00:00")),
    ])
    dim = initial_load(src, "patient_id", TRACKED, "updated_at")
    counts = (current_view(dim).groupBy("patient_id").count()
              .filter(F.col("count") != 1).count())
    assert counts == 0


def test_as_of_returns_the_version_in_force(spark):
    src = make(spark, [
        ("P1", "Austin", "111", ts("2024-01-01T00:00:00")),
        ("P1", "Dallas", "111", ts("2024-06-01T00:00:00")),
    ])
    dim = initial_load(src, "patient_id", TRACKED, "updated_at")

    assert as_of(dim, "2024-03-15 00:00:00").collect()[0]["city"] == "Austin"
    assert as_of(dim, "2024-08-15 00:00:00").collect()[0]["city"] == "Dallas"
    # Exactly one version is valid at any instant - no overlap at the boundary.
    assert as_of(dim, "2024-06-01 00:00:00").count() == 1


def test_late_arriving_record_is_ordered_correctly(spark):
    """Out-of-order input must still produce a chronologically sane history."""
    src = make(spark, [
        ("P1", "Dallas", "111", ts("2024-06-01T00:00:00")),
        ("P1", "Austin", "111", ts("2024-01-01T00:00:00")),
    ])
    dim = initial_load(src, "patient_id", TRACKED, "updated_at").orderBy("version_number")
    cities = [r["city"] for r in dim.collect()]
    assert cities == ["Austin", "Dallas"]


def test_null_attribute_is_a_real_change(spark):
    """NULL -> value must register; naive equality checks miss this."""
    src = make(spark, [
        ("P1", None, "111", ts("2024-01-01T00:00:00")),
        ("P1", "Austin", "111", ts("2024-06-01T00:00:00")),
    ])
    dim = initial_load(src, "patient_id", TRACKED, "updated_at")
    assert dim.count() == 2
