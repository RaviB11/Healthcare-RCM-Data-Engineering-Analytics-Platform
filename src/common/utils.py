"""
Shared plumbing for every job in the platform.

The same job code runs in three places without modification:

  local  -> local[*] Spark, data under ./data
  glue   -> AWS Glue 5.0 (Spark 3.5) with data on S3
  emr    -> EMR Serverless / EMR on EC2

The only thing that changes is `config/pipeline_config.yaml` plus the
RCM_ENV environment variable.
"""

from __future__ import annotations

import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "pipeline_config.yaml"


# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-7s | %(name)-28s | %(message)s",
            datefmt="%H:%M:%S",
        ))
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("RCM_LOG_LEVEL", "INFO"))
        logger.propagate = False
    return logger


LOG = get_logger(__name__)


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

@lru_cache(maxsize=1)
def load_config(path: str | None = None) -> dict[str, Any]:
    cfg_path = Path(path or os.environ.get("RCM_CONFIG", DEFAULT_CONFIG))
    with cfg_path.open(encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    env = os.environ.get("RCM_ENV", cfg.get("default_env", "local"))
    if env not in cfg["environments"]:
        raise ValueError(f"Unknown RCM_ENV '{env}'. Options: {list(cfg['environments'])}")

    resolved = {**cfg, "env": env, **cfg["environments"][env]}
    LOG.info("Config loaded for environment '%s' (root=%s)", env, resolved["storage_root"])
    return resolved


def layer_path(layer: str, dataset: str = "", cfg: dict | None = None) -> str:
    """Resolve a physical path for a medallion layer + dataset."""
    cfg = cfg or load_config()
    root = cfg["storage_root"].rstrip("/")
    bucket = cfg["layers"][layer]
    return f"{root}/{bucket}/{dataset}".rstrip("/")


# --------------------------------------------------------------------------
# Spark
# --------------------------------------------------------------------------

def get_spark(app_name: str, cfg: dict | None = None) -> SparkSession:
    cfg = cfg or load_config()
    builder = (
        SparkSession.builder.appName(f"rcm::{app_name}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    if cfg["env"] == "local":
        builder = (builder.master(cfg.get("spark_master", "local[*]"))
                   .config("spark.driver.memory", cfg.get("driver_memory", "4g"))
                   .config("spark.sql.shuffle.partitions", "8")
                   .config("spark.ui.showConsoleProgress", "false"))
    else:
        builder = (builder
                   .config("spark.sql.shuffle.partitions", cfg.get("shuffle_partitions", 200))
                   .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                           "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(cfg.get("spark_log_level", "WARN"))
    return spark


# --------------------------------------------------------------------------
# Read / write helpers
# --------------------------------------------------------------------------

def read_csv(spark: SparkSession, path: str, schema=None, header: bool = True) -> DataFrame:
    reader = spark.read.option("header", header).option("escape", '"').option("multiLine", "false")
    if schema is not None:
        return reader.schema(schema).csv(path)
    return reader.option("inferSchema", "false").csv(path)


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite",
                  partition_by: list[str] | None = None,
                  coalesce: int | None = None) -> int:
    """Write Parquet and return the row count that was persisted."""
    out = df.coalesce(coalesce) if coalesce else df
    writer = out.write.mode(mode).option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)
    count = df.count()
    LOG.info("wrote %s rows -> %s%s", f"{count:,}", path,
             f" (partitioned by {partition_by})" if partition_by else "")
    return count


def path_exists(spark: SparkSession, path: str) -> bool:
    """Works for both local paths and s3a:// URIs via the Hadoop FS API."""
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    juri = jvm.java.net.URI(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(juri, hconf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


# --------------------------------------------------------------------------
# Audit columns
# --------------------------------------------------------------------------

class GrainViolationError(Exception):
    """Raised when a fact table has more rows per key than its declared grain."""


def assert_unique_grain(df: DataFrame, keys: list[str], table: str,
                        sample: int = 5) -> None:
    """
    Fail loudly if a join fanned out a fact table.

    Silent fan-out is the single most expensive bug in a warehouse: nothing
    errors, the pipeline goes green, and every revenue number is quietly
    inflated. Asserting the grain turns that into a build failure.
    """
    dupes = (df.groupBy(*keys).count().filter(F.col("count") > 1)).cache()
    n = dupes.count()
    if n:
        examples = [r.asDict() for r in dupes.limit(sample).collect()]
        raise GrainViolationError(
            f"{table}: {n:,} duplicate keys on {keys}. Examples: {examples}")
    LOG.info("grain OK: %s is unique on %s", table, keys)


def new_batch_id() -> str:
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"


def with_audit_columns(df: DataFrame, source_system: str, batch_id: str,
                       layer: str) -> DataFrame:
    """Every persisted table carries provenance. Non-negotiable in healthcare."""
    return (df
            .withColumn("_source_system", F.lit(source_system))
            .withColumn("_source_file", F.input_file_name() if layer == "bronze"
                        else F.lit(None).cast("string"))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_layer", F.lit(layer)))


def surrogate_key(*cols: str):
    """Deterministic 64-bit surrogate key from the natural key columns."""
    concat = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in cols])
    return F.abs(F.xxhash64(concat))


def to_snake_case(name: str) -> str:
    """
    camelCase / PascalCase / ACRONYM-aware snake_case.

        PatientID        -> patient_id
        MRN              -> mrn
        AddressLine1     -> address_line1
        LastUpdatedTS    -> last_updated_ts
        already_snake    -> already_snake
    """
    import re
    s = name.strip()
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)   # HTTPServer -> HTTP_Server
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)      # patientId  -> patient_Id
    s = re.sub(r"[^0-9A-Za-z_]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_").lower()


def standardize_columns(df: DataFrame) -> DataFrame:
    """snake_case every column name, strip whitespace."""
    renamed = df
    for c in df.columns:
        clean = to_snake_case(c)
        if clean != c:
            renamed = renamed.withColumnRenamed(c, clean)
    return renamed
