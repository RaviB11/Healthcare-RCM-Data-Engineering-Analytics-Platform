"""
AWS Glue entrypoint.

Glue jobs cannot import from a repo layout directly, so this thin wrapper is
the job script uploaded to S3; the real logic ships as a zipped module in
`--extra-py-files` and is imported here. The business code in `src/` stays
identical between local, Glue and EMR - only this shim is Glue-specific.

Job parameters
--------------
--LAYER        bronze | silver | gold      which stage to run
--RCM_ENV      dev | prod                  selects the config block
--BATCH_ID     optional; generated if absent
--CONFIG_S3    s3:// path to pipeline_config.yaml

Deployed via Terraform (see aws/terraform/glue.tf) with Glue 5.0
(Spark 3.5, Python 3.11), which matches the PySpark API used in src/.
"""

from __future__ import annotations

import sys
import time

import boto3
from awsglue.utils import getResolvedOptions

REQUIRED = ["JOB_NAME", "LAYER", "RCM_ENV", "CONFIG_S3"]
OPTIONAL = ["BATCH_ID"]


def _resolve_args() -> dict:
    present = [a for a in OPTIONAL if f"--{a}" in sys.argv]
    return getResolvedOptions(sys.argv, REQUIRED + present)


def _download_config(config_s3: str) -> str:
    bucket, key = config_s3.replace("s3://", "").split("/", 1)
    local = "/tmp/pipeline_config.yaml"
    boto3.client("s3").download_file(bucket, key, local)
    return local


def _emit_metric(namespace: str, name: str, value: float, unit: str,
                 dimensions: list[dict]) -> None:
    """CloudWatch custom metrics drive the alarms in monitoring.tf."""
    boto3.client("cloudwatch").put_metric_data(
        Namespace=namespace,
        MetricData=[{"MetricName": name, "Value": value, "Unit": unit,
                     "Dimensions": dimensions}],
    )


def main() -> int:
    import os

    args = _resolve_args()
    layer = args["LAYER"].lower()

    os.environ["RCM_ENV"] = args["RCM_ENV"]
    os.environ["RCM_CONFIG"] = _download_config(args["CONFIG_S3"])

    # Imported after the env vars are set: config resolution happens at import.
    if layer == "bronze":
        from src.bronze.ingest_to_bronze import main as run
    elif layer == "silver":
        from src.silver.build_silver import main as run
    elif layer == "gold":
        from src.gold.build_gold import main as run
    else:
        raise ValueError(f"Unknown LAYER '{layer}' (expected bronze|silver|gold)")

    started = time.time()
    dimensions = [{"Name": "Layer", "Value": layer},
                  {"Name": "Environment", "Value": args["RCM_ENV"]}]

    try:
        rc = run()
    except Exception:
        _emit_metric("RCM/Pipeline", "JobFailure", 1, "Count", dimensions)
        raise

    duration = time.time() - started
    _emit_metric("RCM/Pipeline", "JobDurationSeconds", duration, "Seconds", dimensions)
    _emit_metric("RCM/Pipeline", "JobFailure", 0 if rc == 0 else 1, "Count", dimensions)

    print(f"[{args['JOB_NAME']}] layer={layer} rc={rc} duration={duration:.1f}s")
    return rc


if __name__ == "__main__":
    sys.exit(main())
