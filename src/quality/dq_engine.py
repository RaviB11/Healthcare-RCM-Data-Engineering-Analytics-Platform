"""
Declarative data-quality engine.

Rules live in `config/dq_rules.yaml`, not in job code, so an analyst can
add an expectation without touching PySpark. Each rule compiles to a
boolean Column; rows failing any BLOCKING rule are routed to a
quarantine table instead of silently poisoning the warehouse.

Supported rule types
--------------------
not_null          columns must be populated
unique            composite key must not repeat
accepted_values   value must be in a fixed domain
range             numeric bounds (min / max, inclusive)
regex             string must match a pattern
expression        any Spark SQL boolean expression
referential       value must exist in a reference dataset column

Severity
--------
blocking  -> row is quarantined and excluded from the curated output
warning   -> row passes through, violation is counted and reported
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.utils import PROJECT_ROOT, get_logger

LOG = get_logger(__name__)
DEFAULT_RULES = PROJECT_ROOT / "config" / "dq_rules.yaml"


@dataclass
class RuleResult:
    dataset: str
    rule_name: str
    rule_type: str
    severity: str
    columns: str
    failed_rows: int
    total_rows: int

    @property
    def pass_rate(self) -> float:
        return 1.0 if self.total_rows == 0 else 1 - (self.failed_rows / self.total_rows)

    def as_row(self, batch_id: str) -> tuple:
        return (batch_id, self.dataset, self.rule_name, self.rule_type, self.severity,
                self.columns, self.failed_rows, self.total_rows,
                round(self.pass_rate, 6), datetime.now(timezone.utc))


@dataclass
class DQReport:
    dataset: str
    results: list[RuleResult] = field(default_factory=list)
    quarantined: int = 0
    passed: int = 0

    @property
    def blocking_failures(self) -> list[RuleResult]:
        return [r for r in self.results if r.severity == "blocking" and r.failed_rows > 0]

    def summary(self) -> str:
        lines = [f"DQ report :: {self.dataset}",
                 f"  passed={self.passed:,}  quarantined={self.quarantined:,}"]
        for r in sorted(self.results, key=lambda x: -x.failed_rows):
            flag = "FAIL" if r.failed_rows else "ok  "
            lines.append(f"  [{flag}] {r.severity:<8} {r.rule_name:<38} "
                         f"failed={r.failed_rows:>6,}  pass_rate={r.pass_rate:.4%}")
        return "\n".join(lines)


class DataQualityEngine:
    def __init__(self, spark: SparkSession, rules_path: str | Path | None = None):
        self.spark = spark
        with Path(rules_path or DEFAULT_RULES).open(encoding="utf-8") as fh:
            self.rules: dict[str, Any] = yaml.safe_load(fh)["datasets"]

    # ----------------------------------------------------------------- rules

    def _compile(self, df: DataFrame, rule: dict, refs: dict[str, DataFrame]) -> Column:
        """Return a Column that is TRUE when the row SATISFIES the rule."""
        rtype = rule["type"]
        cols = rule.get("columns", [])

        if rtype == "not_null":
            cond = F.lit(True)
            for c in cols:
                cond = cond & F.col(c).isNotNull() & (F.trim(F.col(c).cast("string")) != "")
            return cond

        if rtype == "unique":
            w = Window.partitionBy(*[F.col(c) for c in cols])
            return F.count(F.lit(1)).over(w) == 1

        if rtype == "accepted_values":
            return F.col(cols[0]).isin(rule["values"]) | F.col(cols[0]).isNull() \
                if rule.get("allow_null") else F.col(cols[0]).isin(rule["values"])

        if rtype == "range":
            c = F.col(cols[0]).cast("double")
            cond = F.lit(True)
            if "min" in rule:
                cond = cond & (c >= F.lit(rule["min"]))
            if "max" in rule:
                cond = cond & (c <= F.lit(rule["max"]))
            return cond & c.isNotNull() if not rule.get("allow_null") else cond | c.isNull()

        if rtype == "regex":
            return F.col(cols[0]).rlike(rule["pattern"])

        if rtype == "expression":
            return F.expr(rule["expression"])

        if rtype == "referential":
            ref_name = rule["reference"]
            if ref_name not in refs:
                LOG.warning("Reference '%s' not supplied; skipping rule %s",
                            ref_name, rule["name"])
                return F.lit(True)
            ref_col = rule["reference_column"]
            ref = refs[ref_name].select(F.col(ref_col).alias("__ref_val")).distinct()
            keys = df.select(F.col(cols[0]).alias("__val")).distinct()
            valid = (keys.join(ref, keys["__val"] == ref["__ref_val"], "left_semi")
                        .select(F.col("__val")))
            valid_list = [r["__val"] for r in valid.collect()]
            return F.col(cols[0]).isin(valid_list) | F.col(cols[0]).isNull()

        raise ValueError(f"Unsupported rule type: {rtype}")

    # ---------------------------------------------------------------- public

    def validate(self, df: DataFrame, dataset: str,
                 refs: dict[str, DataFrame] | None = None
                 ) -> tuple[DataFrame, DataFrame, DQReport]:
        """
        Returns (clean_df, quarantine_df, report).

        quarantine_df carries a `_dq_violations` array naming every rule
        the row broke, so the stewardship team can work the exceptions.
        """
        refs = refs or {}
        cfg = self.rules.get(dataset)
        report = DQReport(dataset=dataset)

        if not cfg:
            LOG.warning("No DQ rules defined for dataset '%s' - passing through", dataset)
            empty = df.limit(0).withColumn("_dq_violations", F.array().cast("array<string>"))
            report.passed = df.count()
            return df, empty, report

        total = df.cache().count()
        flagged = df
        violation_cols: list[str] = []
        blocking_cols: list[str] = []

        for rule in cfg["rules"]:
            name = rule["name"]
            flag = f"__dq_{name}"
            satisfied = self._compile(df, rule, refs)
            flagged = flagged.withColumn(flag, satisfied)
            violation_cols.append(flag)
            if rule.get("severity", "warning") == "blocking":
                blocking_cols.append(flag)

        flagged = flagged.cache()

        for rule in cfg["rules"]:
            flag = f"__dq_{rule['name']}"
            failed = flagged.filter(~F.col(flag)).count()
            report.results.append(RuleResult(
                dataset=dataset, rule_name=rule["name"], rule_type=rule["type"],
                severity=rule.get("severity", "warning"),
                columns=",".join(rule.get("columns", [])) or "-",
                failed_rows=failed, total_rows=total,
            ))

        violations = F.array_compact(F.array(*[
            F.when(~F.col(f"__dq_{r['name']}"), F.lit(r["name"]))
            for r in cfg["rules"]
        ])) if hasattr(F, "array_compact") else F.array_except(F.array(*[
            F.when(~F.col(f"__dq_{r['name']}"), F.lit(r["name"]))
            for r in cfg["rules"]
        ]), F.array(F.lit(None).cast("string")))

        flagged = flagged.withColumn("_dq_violations", violations)

        if blocking_cols:
            is_clean = F.lit(True)
            for c in blocking_cols:
                is_clean = is_clean & F.col(c)
        else:
            is_clean = F.lit(True)

        drop = violation_cols
        clean = flagged.filter(is_clean).drop(*drop, "_dq_violations")
        quarantine = (flagged.filter(~is_clean)
                      .withColumn("_quarantined_at", F.current_timestamp())
                      .drop(*drop))

        report.passed = clean.count()
        report.quarantined = quarantine.count()
        LOG.info("\n%s", report.summary())
        return clean, quarantine, report

    def metrics_dataframe(self, reports: list[DQReport], batch_id: str) -> DataFrame:
        """Flatten reports into a DataFrame ready to persist for monitoring."""
        rows = [r.as_row(batch_id) for rep in reports for r in rep.results]
        schema = ("batch_id string, dataset string, rule_name string, rule_type string, "
                  "severity string, columns string, failed_rows bigint, total_rows bigint, "
                  "pass_rate double, evaluated_at timestamp")
        return self.spark.createDataFrame(rows, schema=schema)
