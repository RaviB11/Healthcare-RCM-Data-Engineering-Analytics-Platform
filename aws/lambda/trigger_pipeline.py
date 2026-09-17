"""
S3 event -> Step Functions trigger.

Fires when a source extract lands in the landing zone. Rather than starting a
run per file (fifteen files would mean fifteen concurrent pipelines fighting
over the same tables), it records arrivals in DynamoDB and only starts the
state machine once every expected source for that business date is present.

Environment
-----------
STATE_MACHINE_ARN   the RCM pipeline state machine
ARRIVALS_TABLE      DynamoDB table, PK business_date, SK source_name
EXPECTED_SOURCES    comma-separated list of source names
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone

import boto3

sfn = boto3.client("stepfunctions")
ddb = boto3.resource("dynamodb")

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
ARRIVALS_TABLE = os.environ["ARRIVALS_TABLE"]
EXPECTED = {s.strip() for s in os.environ["EXPECTED_SOURCES"].split(",") if s.strip()}


def parse_key(key: str) -> tuple[str | None, str | None]:
    """
    landing/emr/hospital_a/2025-12-31/patients.csv
        -> ("patients_hospital_a", "2025-12-31")
    """
    m = re.match(r"landing/(?P<system>[^/]+)/(?P<sub>[^/]+)/"
                 r"(?P<date>\d{4}-\d{2}-\d{2})/(?P<file>[^/]+)\.csv$", key)
    if not m:
        return None, None
    stem = m.group("file")
    sub = m.group("sub")
    name = stem if sub == m.group("system") else f"{stem}_{sub}"
    return name, m.group("date")


def handler(event, context):                               # noqa: ANN001
    table = ddb.Table(ARRIVALS_TABLE)
    started = []

    for record in event.get("Records", []):
        key = record["s3"]["object"]["key"]
        bucket = record["s3"]["bucket"]["name"]
        source, business_date = parse_key(key)

        if not source:
            print(f"ignoring unrecognised key: {key}")
            continue

        table.put_item(Item={
            "business_date": business_date,
            "source_name": source,
            "s3_key": key,
            "bucket": bucket,
            "arrived_at": datetime.now(timezone.utc).isoformat(),
        })

        arrived = {
            i["source_name"]
            for i in table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key(
                    "business_date").eq(business_date)
            )["Items"]
        }
        missing = EXPECTED - arrived
        print(f"{business_date}: {len(arrived)}/{len(EXPECTED)} sources, "
              f"missing={sorted(missing) if missing else 'none'}")

        if missing:
            continue

        # Deterministic execution name makes the start idempotent: a duplicate
        # S3 notification cannot launch a second run for the same date.
        execution_name = f"rcm-{business_date}"
        try:
            resp = sfn.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                name=execution_name,
                input=json.dumps({"business_date": business_date,
                                  "triggered_by": "s3_arrival",
                                  "bucket": bucket}),
            )
            started.append(resp["executionArn"])
            print(f"started {resp['executionArn']}")
        except sfn.exceptions.ExecutionAlreadyExists:
            print(f"execution {execution_name} already running - skipping")

    return {"statusCode": 200,
            "body": json.dumps({"executions_started": started})}
