"""
Export the Gold star schema to CSV for Power BI Desktop.

In a deployed environment Power BI connects straight to Redshift (DirectQuery
for the A/R snapshot, Import for everything else). This exporter exists so the
report can be built and demonstrated from the repo alone, with no AWS account.

The column sets are deliberately narrower than the warehouse tables: a Power
BI import model should not carry columns nobody puts on a visual, and PHI-ish
attributes (address, phone, email) are dropped entirely because a BI extract is
the last place they belong.

Usage:
    python -m src.gold.export_for_powerbi --out powerbi/data
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F

from src.common.utils import (get_logger, get_spark, layer_path, load_config,
                              read_parquet)

LOG = get_logger("gold.powerbi_export")

# table -> columns to publish. None means "everything".
EXPORTS: dict[str, list[str] | None] = {
    "dim_date": None,
    "dim_payer": None,
    "dim_provider": ["provider_sk", "provider_id", "provider_name", "credential",
                     "specialty", "department_name", "service_line", "hospital",
                     "is_active"],
    "dim_procedure": None,
    "dim_diagnosis": None,
    "dim_denial_reason": None,
    # Current patients only, minus direct identifiers. The Type 2 history stays
    # in the warehouse; the report does not need 5,600 rows to show age bands.
    "dim_patient": ["patient_sk", "patient_id", "gender", "age_band",
                    "city", "state", "source_system", "is_current"],
    "fact_claim": None,
    "fact_ar_snapshot": None,
    "kpi_monthly_scorecard": None,
    "kpi_payer_performance": None,
    "kpi_denial_analysis": None,
    "kpi_ar_aging": None,
    "kpi_provider_performance": None,
    "kpi_service_line": None,
}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Export Gold tables as CSV for Power BI")
    ap.add_argument("--out", default="powerbi/data")
    args = ap.parse_args(argv)

    cfg = load_config()
    spark = get_spark("powerbi_export", cfg)
    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    manifest = []
    for table, columns in EXPORTS.items():
        try:
            df = read_parquet(spark, layer_path("gold", table, cfg))
        except Exception as exc:                                # noqa: BLE001
            LOG.warning("skipping %s: %s", table, exc)
            continue

        if table == "dim_patient":
            df = df.filter(F.col("is_current"))
        if columns:
            df = df.select(*[c for c in columns if c in df.columns])
        df = df.drop(*[c for c in df.columns if c.startswith("_")])

        # Single CSV file per table, written where Power BI can find it.
        tmp = out_root / f"_{table}_tmp"
        (df.coalesce(1).write.mode("overwrite")
           .option("header", True).option("escape", '"').csv(str(tmp)))
        part = next(tmp.glob("part-*.csv"))
        target = out_root / f"{table}.csv"
        part.replace(target)
        for leftover in tmp.iterdir():
            leftover.unlink()
        tmp.rmdir()

        rows = df.count()
        size_kb = target.stat().st_size / 1024
        manifest.append((table, rows, len(df.columns), size_kb))
        LOG.info("%-28s %8s rows  %3d cols  %8.1f KB", table, f"{rows:,}",
                 len(df.columns), size_kb)

    total_mb = sum(m[3] for m in manifest) / 1024
    LOG.info("-" * 70)
    LOG.info("exported %d tables, %.1f MB total -> %s", len(manifest), total_mb, out_root)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
