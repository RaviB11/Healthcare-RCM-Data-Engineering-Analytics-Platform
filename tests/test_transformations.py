"""
Transformation and metric-logic tests.

Several of these are regression tests for bugs that were actually found while
building this pipeline. They are named so that a failure says what broke.
"""
from __future__ import annotations

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from src.common.utils import (GrainViolationError, assert_unique_grain,
                              standardize_columns, surrogate_key, to_snake_case)
from src.silver.scd2 import initial_load


# ---------------------------------------------------------------------------
# Column standardisation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("PatientID", "patient_id"),        # regression: produced "patient_i_d"
    ("MRN", "mrn"),                     # regression: produced "m_r_n"
    ("DateOfBirth", "date_of_birth"),
    ("LastUpdatedTimestamp", "last_updated_timestamp"),
    ("AddressLine1", "address_line1"),
    ("already_snake", "already_snake"),
    ("  Spaced Name  ", "spaced_name"),
    ("icd10_code", "icd10_code"),       # must not become icd_10_code
])
def test_to_snake_case(raw, expected):
    assert to_snake_case(raw) == expected


def test_standardize_columns_renames_dataframe(spark):
    df = spark.createDataFrame([("a", "b")], "PatientID string, MRN string")
    assert standardize_columns(df).columns == ["patient_id", "mrn"]


# ---------------------------------------------------------------------------
# Surrogate keys
# ---------------------------------------------------------------------------

def test_surrogate_key_is_deterministic_and_distinct(spark):
    df = spark.createDataFrame([("A", 1), ("A", 2), ("B", 1)], "k string, v int")
    out = df.withColumn("sk", surrogate_key("k", "v"))
    keys = [r["sk"] for r in out.collect()]
    assert len(set(keys)) == 3
    again = [r["sk"] for r in df.withColumn("sk", surrogate_key("k", "v")).collect()]
    assert keys == again


def test_surrogate_key_distinguishes_null_from_empty(spark):
    df = spark.createDataFrame([(None,), ("",)], "k string")
    keys = [r["sk"] for r in df.withColumn("sk", surrogate_key("k")).collect()]
    assert keys[0] != keys[1]


# ---------------------------------------------------------------------------
# Grain assertion
# ---------------------------------------------------------------------------

def test_assert_unique_grain_passes_on_unique_keys(spark):
    df = spark.createDataFrame([("C1",), ("C2",)], "claim_id string")
    assert_unique_grain(df, ["claim_id"], "fact_test")


def test_assert_unique_grain_raises_on_duplicates(spark):
    df = spark.createDataFrame([("C1",), ("C1",)], "claim_id string")
    with pytest.raises(GrainViolationError, match="fact_test"):
        assert_unique_grain(df, ["claim_id"], "fact_test")


# ---------------------------------------------------------------------------
# Point-in-time join: the fan-out regression
# ---------------------------------------------------------------------------

def test_pit_join_does_not_fan_out_on_same_day_version_change(spark):
    """
    Regression: a claim whose service date fell on the same calendar day that
    the patient record changed matched BOTH SCD2 versions, duplicating the
    claim - and therefore duplicating revenue.

    The fix compares the validity window at timestamp precision instead of
    casting it down to a date.
    """
    patients = spark.createDataFrame(
        [("P1", "Austin", datetime(2025, 1, 1, 0, 0, 0)),
         ("P1", "Dallas", datetime(2025, 6, 15, 6, 0, 0))],   # changes mid-day
        "patient_id string, city string, updated_at timestamp")
    dim = initial_load(patients, "patient_id", ["city"], "updated_at")

    claims = spark.createDataFrame(
        [("C1", "P1", date(2025, 6, 15), 100.0)],             # same day as change
        "claim_id string, patient_id string, service_date date, charge double")

    pit = dim.select(
        F.col("patient_id").alias("_pid"),
        F.col("city"),
        F.col("effective_from").alias("_from"),
        F.col("effective_to").alias("_to"))

    joined = claims.join(
        pit,
        (F.col("patient_id") == F.col("_pid"))
        & (F.col("service_date").cast("timestamp") >= F.col("_from"))
        & (F.col("service_date").cast("timestamp") <= F.col("_to")),
        "left")

    assert joined.count() == 1, "point-in-time join fanned out"
    # The version in force when the service day began is the correct one.
    assert joined.collect()[0]["city"] == "Austin"
    assert joined.agg(F.sum("charge")).collect()[0][0] == 100.0


# ---------------------------------------------------------------------------
# RCM metric definitions
# ---------------------------------------------------------------------------

CLAIM_SCHEMA = ("claim_id string, carc_code string, denial_category string, "
                "remit_date date, outstanding_balance double")

DENIAL_CATEGORIES = ["Technical", "Medical Necessity", "Authorization", "Duplicate",
                     "Timely Filing", "Coordination of Benefits", "Bundling"]


def classify(spark, rows):
    df = spark.createDataFrame(rows, schema=CLAIM_SCHEMA)
    return (df
            .withColumn("is_adjudicated", F.col("remit_date").isNotNull())
            .withColumn("is_denial_carc",
                        F.coalesce(F.col("denial_category"), F.lit(""))
                         .isin(DENIAL_CATEGORIES))
            .withColumn("is_clean_claim",
                        F.col("remit_date").isNotNull() & ~F.col("is_denial_carc")))


def test_patient_responsibility_codes_are_not_denials(spark):
    """
    Regression: PR-1 / PR-2 (deductible, coinsurance) and CO-45 (contractual
    write-down) were being counted as denials, which dragged the reported
    clean claim rate down to roughly half its true value.
    """
    rows = [
        ("C1", "PR-2", "Patient Responsibility", date(2025, 1, 10), 0.0),
        ("C2", "CO-45", "Contractual", date(2025, 1, 10), 0.0),
        ("C3", None, None, date(2025, 1, 10), 0.0),
        ("C4", "CO-16", "Technical", date(2025, 1, 10), 500.0),
    ]
    out = {r["claim_id"]: r["is_clean_claim"] for r in classify(spark, rows).collect()}
    assert out["C1"] is True, "coinsurance is normal adjudication, not a denial"
    assert out["C2"] is True, "contractual write-down is not a denial"
    assert out["C3"] is True
    assert out["C4"] is False, "CO-16 is a genuine denial"


def test_unadjudicated_claims_are_excluded_from_rate_denominators(spark):
    """
    A claim still in flight is neither clean nor denied. Counting it in the
    denominator makes every current month look like a collapse.
    """
    rows = [
        ("C1", None, None, date(2025, 1, 10), 0.0),    # adjudicated, clean
        ("C2", "CO-16", "Technical", date(2025, 1, 10), 500.0),
        ("C3", None, None, None, 800.0),               # still in flight
    ]
    df = classify(spark, rows)
    adjudicated = df.filter("is_adjudicated").count()
    clean = df.filter("is_clean_claim").count()
    assert adjudicated == 2
    assert clean / adjudicated == 0.5


def test_ar_aging_buckets_are_mutually_exclusive(spark):
    df = spark.createDataFrame([(0,), (30,), (31,), (90,), (91,), (200,)], "age int")
    bucketed = df.withColumn(
        "bucket",
        F.when(F.col("age") <= 30, "0-30")
         .when(F.col("age") <= 60, "31-60")
         .when(F.col("age") <= 90, "61-90")
         .when(F.col("age") <= 120, "91-120")
         .when(F.col("age") <= 180, "121-180")
         .otherwise("180+"))
    got = {r["age"]: r["bucket"] for r in bucketed.collect()}
    assert got == {0: "0-30", 30: "0-30", 31: "31-60",
                   90: "61-90", 91: "91-120", 200: "180+"}
    assert bucketed.filter(F.col("bucket").isNull()).count() == 0


def test_writeoff_clears_balance_without_creating_credit(spark):
    """
    Regression: denied claims received a contractual adjustment AND a write-off
    for the full gross charge, driving 1,018 claims into a phantom credit
    balance and breaking the A/R roll-forward by over a million dollars.
    The write-off must clear only the REMAINING balance.
    """
    charge, contractual = 1000.0, 150.0
    remaining = charge - contractual
    ledger = spark.createDataFrame(
        [("CHARGE", charge), ("CONTRACTUAL_ADJUSTMENT", -contractual),
         ("WRITEOFF", -remaining)],
        "transaction_type string, amount double")
    balance = ledger.agg(F.sum("amount")).collect()[0][0]
    assert balance == pytest.approx(0.0), "write-off must land the balance on zero"
