"""Tests for the declarative data-quality engine."""
from __future__ import annotations

import textwrap

import pytest

from src.quality.dq_engine import DataQualityEngine

RULES = textwrap.dedent("""
datasets:
  widgets:
    rules:
      - name: id_not_null
        type: not_null
        columns: [id]
        severity: blocking
      - name: id_unique
        type: unique
        columns: [id]
        severity: blocking
      - name: status_domain
        type: accepted_values
        columns: [status]
        values: ["OPEN", "CLOSED"]
        severity: warning
      - name: amount_range
        type: range
        columns: [amount]
        min: 0
        max: 100
        severity: blocking
      - name: code_format
        type: regex
        columns: [code]
        pattern: "^[A-Z]{2}[0-9]{2}$"
        severity: warning
      - name: expr_check
        type: expression
        expression: "amount IS NULL OR amount <> 13"
        severity: warning
""")

SCHEMA = "id string, status string, amount double, code string"


@pytest.fixture()
def engine(spark, tmp_path):
    path = tmp_path / "rules.yaml"
    path.write_text(RULES, encoding="utf-8")
    return DataQualityEngine(spark, rules_path=path)


def df_of(spark, rows):
    return spark.createDataFrame(rows, schema=SCHEMA)


def test_clean_data_passes_entirely(spark, engine):
    df = df_of(spark, [("1", "OPEN", 10.0, "AB12"), ("2", "CLOSED", 20.0, "CD34")])
    clean, quarantine, report = engine.validate(df, "widgets")
    assert clean.count() == 2
    assert quarantine.count() == 0
    assert report.blocking_failures == []


def test_blocking_rule_quarantines_the_row(spark, engine):
    df = df_of(spark, [("1", "OPEN", 10.0, "AB12"), (None, "OPEN", 10.0, "AB12")])
    clean, quarantine, _ = engine.validate(df, "widgets")
    assert clean.count() == 1
    assert quarantine.count() == 1


def test_warning_rule_does_not_quarantine(spark, engine):
    """A warning must be counted but must not remove data from the warehouse."""
    df = df_of(spark, [("1", "PENDING", 10.0, "AB12")])
    clean, quarantine, report = engine.validate(df, "widgets")
    assert clean.count() == 1
    assert quarantine.count() == 0
    failed = {r.rule_name: r.failed_rows for r in report.results}
    assert failed["status_domain"] == 1


def test_quarantined_row_records_which_rules_it_broke(spark, engine):
    df = df_of(spark, [(None, "OPEN", 500.0, "AB12")])
    _, quarantine, _ = engine.validate(df, "widgets")
    violations = quarantine.collect()[0]["_dq_violations"]
    assert "id_not_null" in violations
    assert "amount_range" in violations


def test_uniqueness_catches_both_copies(spark, engine):
    df = df_of(spark, [("1", "OPEN", 1.0, "AB12"), ("1", "OPEN", 2.0, "AB12")])
    clean, quarantine, _ = engine.validate(df, "widgets")
    assert clean.count() == 0
    assert quarantine.count() == 2


def test_regex_and_expression_rules(spark, engine):
    df = df_of(spark, [("1", "OPEN", 13.0, "bad-code")])
    _, _, report = engine.validate(df, "widgets")
    failed = {r.rule_name: r.failed_rows for r in report.results}
    assert failed["code_format"] == 1
    assert failed["expr_check"] == 1


def test_referential_rule(spark, tmp_path):
    rules = textwrap.dedent("""
    datasets:
      claims:
        rules:
          - name: payer_exists
            type: referential
            columns: [payer_id]
            reference: payers
            reference_column: payer_id
            severity: blocking
    """)
    path = tmp_path / "ref.yaml"
    path.write_text(rules, encoding="utf-8")
    eng = DataQualityEngine(spark, rules_path=path)

    claims = spark.createDataFrame([("C1", "PAY1"), ("C2", "PAY999")],
                                   "claim_id string, payer_id string")
    payers = spark.createDataFrame([("PAY1",)], "payer_id string")

    clean, quarantine, _ = eng.validate(claims, "claims", refs={"payers": payers})
    assert clean.count() == 1
    assert quarantine.collect()[0]["payer_id"] == "PAY999"


def test_unknown_dataset_passes_through(spark, engine):
    df = df_of(spark, [("1", "OPEN", 1.0, "AB12")])
    clean, _, report = engine.validate(df, "not_configured")
    assert clean.count() == 1
    assert report.results == []


def test_pass_rate_and_metrics_frame(spark, engine):
    df = df_of(spark, [("1", "OPEN", 1.0, "AB12"), (None, "OPEN", 1.0, "AB12")])
    _, _, report = engine.validate(df, "widgets")
    metrics = engine.metrics_dataframe([report], "batch-1")
    assert metrics.count() == len(report.results)
    not_null = [r for r in report.results if r.rule_name == "id_not_null"][0]
    assert not_null.pass_rate == 0.5
