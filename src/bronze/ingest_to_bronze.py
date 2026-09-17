"""
Bronze layer: land raw source extracts as immutable, audited Parquet.

Principles
----------
1. No business logic. Bronze is a faithful, replayable copy of what the
   source system actually sent, including its defects.
2. Everything is read as STRING. Type casting is a Silver concern; a bad
   date must not fail ingestion at 3am.
3. Every row carries provenance: source system, source file, batch id,
   ingestion timestamp.
4. Driven entirely by the `sources:` block in pipeline_config.yaml.

Run:
    python -m src.bronze.ingest_to_bronze
    python -m src.bronze.ingest_to_bronze --only claims transactions
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import functions as F

from src.common.utils import (get_logger, get_spark, layer_path, load_config,
                              new_batch_id, read_csv, standardize_columns,
                              with_audit_columns, write_parquet)

LOG = get_logger("bronze.ingest")


def ingest_source(spark, source: dict, cfg: dict, batch_id: str) -> dict:
    name = source["name"]
    src_path = f"{layer_path('landing', '', cfg)}/{source['landing_path']}"
    tgt_path = layer_path("bronze", source["bronze_table"], cfg)

    LOG.info("ingesting %-28s %s", name, src_path)

    # All columns as string: Bronze never rejects a row for a type problem.
    df = read_csv(spark, src_path)
    df = standardize_columns(df)
    df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])
    df = with_audit_columns(df, source["system"], batch_id, "bronze")
    df = df.withColumn("_ingest_date", F.current_date())

    mode = "append" if source["load_type"] == "incremental" else "overwrite"
    rows = write_parquet(df, tgt_path, mode=mode, coalesce=1)

    return {"source": name, "table": source["bronze_table"], "rows": rows,
            "load_type": source["load_type"], "status": "SUCCESS"}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Bronze ingestion")
    ap.add_argument("--only", nargs="*", help="ingest only these source names")
    ap.add_argument("--batch-id", default=None)
    args = ap.parse_args(argv)

    cfg = load_config()
    batch_id = args.batch_id or new_batch_id()
    spark = get_spark("bronze_ingest", cfg)

    sources = cfg["sources"]
    if args.only:
        sources = [s for s in sources if s["name"] in args.only]
        if not sources:
            LOG.error("No sources matched %s", args.only)
            return 1

    LOG.info("=" * 78)
    LOG.info("BRONZE INGESTION  batch_id=%s  sources=%d", batch_id, len(sources))
    LOG.info("=" * 78)

    results, failures = [], []
    for source in sources:
        try:
            results.append(ingest_source(spark, source, cfg, batch_id))
        except Exception as exc:                       # noqa: BLE001
            LOG.error("FAILED %s: %s", source["name"], exc)
            failures.append({"source": source["name"], "status": "FAILED", "error": str(exc)})

    total = sum(r["rows"] for r in results)
    LOG.info("-" * 78)
    LOG.info("Bronze complete: %d/%d sources, %s rows ingested",
             len(results), len(sources), f"{total:,}")
    for f in failures:
        LOG.error("  failed: %s -> %s", f["source"], f["error"])

    # Persist a manifest so Airflow / Step Functions can assert on it.
    manifest = spark.createDataFrame(
        [(batch_id, r["source"], r["table"], r["rows"], r["load_type"], r["status"])
         for r in results],
        "batch_id string, source string, table_name string, row_count bigint, "
        "load_type string, status string",
    ).withColumn("completed_at", F.current_timestamp())
    write_parquet(manifest, layer_path("metrics", f"bronze_manifest/batch_id={batch_id}", cfg),
                  coalesce=1)

    spark.stop()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
