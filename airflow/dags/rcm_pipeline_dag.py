"""
Airflow orchestration for the Healthcare RCM pipeline.

Step Functions (aws/stepfunctions/) is the serverless path. This DAG is the
alternative for shops already running MWAA, and it is the better option when
the pipeline has to coordinate with things outside AWS: an on-prem SFTP drop,
a Power BI refresh, a finance close calendar.

Design notes
------------
* Source arrival is detected with sensors in `reschedule` mode, not `poke`.
  Fifteen poking sensors would hold fifteen worker slots hostage all morning.
* The quality gate is a branch, not a bolt-on check. Bad data stops here
  rather than being published and retracted.
* The Redshift load is idempotent (delete-by-partition then insert), so a
  retried task cannot double-count revenue.
* `max_active_runs=1`: two concurrent runs would interleave writes to the
  Type 2 patient dimension and corrupt its history.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import (
    RedshiftDataOperator,
)
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

LAKE_BUCKET = "rcm-datalake-prod-us-east-1"
GLUE_ARGS = {
    "--RCM_ENV": "prod",
    "--CONFIG_S3": f"s3://{LAKE_BUCKET}/config/pipeline_config.yaml",
}

SOURCES = {
    "emr_hospital_a": "landing/emr/hospital_a/{{ ds }}/patients.csv",
    "emr_hospital_b": "landing/emr/hospital_b/{{ ds }}/patients.csv",
    "billing_claims": "landing/billing/{{ ds }}/claims.csv",
    "billing_transactions": "landing/billing/{{ ds }}/transactions.csv",
    "remittance_835": "landing/remittance/{{ ds }}/era_835.csv",
}

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=30),
    "email_on_failure": True,
    "email": ["rcm-data-alerts@example.org"],
}

with DAG(
    dag_id="rcm_medallion_pipeline",
    description="Healthcare RCM: landing -> bronze -> silver -> gold -> Redshift",
    default_args=default_args,
    # 05:00 UTC: after overnight source extracts, before the US business day.
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=pendulum.duration(hours=4),
    tags=["healthcare", "rcm", "medallion", "production"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    # -----------------------------------------------------------------------
    # Wait for source extracts
    # -----------------------------------------------------------------------
    with TaskGroup("await_sources") as await_sources:
        for name, key in SOURCES.items():
            S3KeySensor(
                task_id=f"wait_{name}",
                bucket_name=LAKE_BUCKET,
                bucket_key=key,
                aws_conn_id="aws_default",
                # reschedule frees the worker slot between checks
                mode="reschedule",
                poke_interval=300,
                timeout=60 * 60 * 3,
                soft_fail=False,
            )

    # -----------------------------------------------------------------------
    # Medallion layers
    # -----------------------------------------------------------------------
    ingest_bronze = GlueJobOperator(
        task_id="ingest_bronze",
        job_name="rcm-prod-bronze-ingest",
        script_args={**GLUE_ARGS, "--LAYER": "bronze",
                     "--BATCH_ID": "{{ run_id }}"},
        wait_for_completion=True,
        aws_conn_id="aws_default",
    )

    build_silver = GlueJobOperator(
        task_id="build_silver",
        job_name="rcm-prod-silver-build",
        script_args={**GLUE_ARGS, "--LAYER": "silver",
                     "--BATCH_ID": "{{ run_id }}"},
        wait_for_completion=True,
        aws_conn_id="aws_default",
    )

    # -----------------------------------------------------------------------
    # Quality gate
    # -----------------------------------------------------------------------
    @task(task_id="evaluate_quality")
    def evaluate_quality(**context) -> dict:
        """
        Read the DQ metrics the Silver job wrote and decide whether the batch
        is fit to publish.

        Two thresholds, because they fail differently: a single rule collapsing
        usually means a source changed shape, while a broad rise in quarantine
        volume usually means an upstream system had a bad night.
        """
        import awswrangler as wr

        batch_id = context["run_id"]
        metrics = wr.s3.read_parquet(
            path=f"s3://{LAKE_BUCKET}/metrics/dq_results/batch_id={batch_id}/")

        if metrics.empty:
            raise AirflowFailException(
                f"No DQ metrics found for batch {batch_id}; the Silver job "
                "did not complete its validation step.")

        blocking = metrics[metrics["severity"] == "blocking"]
        worst = blocking.nsmallest(1, "pass_rate").iloc[0] if not blocking.empty else None
        total_failed = int(blocking["failed_rows"].sum())
        total_rows = int(blocking["total_rows"].sum())
        quarantine_pct = 100.0 * total_failed / max(total_rows, 1)

        result = {
            "batch_id": batch_id,
            "quarantine_pct": round(quarantine_pct, 3),
            "worst_rule": None if worst is None else worst["rule_name"],
            "worst_pass_rate": None if worst is None else float(worst["pass_rate"]),
        }

        if quarantine_pct > 5.0 or (worst is not None and worst["pass_rate"] < 0.90):
            result["verdict"] = "FAIL"
        elif quarantine_pct > 1.0 or (worst is not None and worst["pass_rate"] < 0.98):
            result["verdict"] = "WARN"
        else:
            result["verdict"] = "PASS"

        context["ti"].xcom_push(key="quality", value=result)
        print(f"quality verdict: {result}")
        return result

    quality = evaluate_quality()

    def _route(**context) -> str:
        verdict = context["ti"].xcom_pull(task_ids="evaluate_quality",
                                          key="quality")["verdict"]
        return "quality_failed" if verdict == "FAIL" else "build_gold"

    quality_branch = BranchPythonOperator(task_id="quality_branch",
                                          python_callable=_route)

    quality_failed = EmptyOperator(task_id="quality_failed")

    build_gold = GlueJobOperator(
        task_id="build_gold",
        job_name="rcm-prod-gold-build",
        script_args={**GLUE_ARGS, "--LAYER": "gold",
                     "--BATCH_ID": "{{ run_id }}"},
        wait_for_completion=True,
        aws_conn_id="aws_default",
    )

    # -----------------------------------------------------------------------
    # Publish
    # -----------------------------------------------------------------------
    with TaskGroup("publish") as publish:
        load_warehouse = RedshiftDataOperator(
            task_id="load_warehouse",
            workgroup_name="rcm-wg-prod",
            database="rcm_dw",
            sql=[
                "CALL gold.sp_load_dimensions();",
                "CALL gold.sp_load_facts();",
                "CALL marts.sp_rebuild_kpis();",
            ],
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )

        @task(task_id="verify_warehouse")
        def verify_warehouse(**context) -> dict:
            """
            Post-load reconciliation. The A/R roll-forward must tie: opening
            plus charges less payments and adjustments equals closing. If it
            does not, the warehouse is internally inconsistent and the report
            must not refresh on top of it.
            """
            import awswrangler as wr

            variance = wr.redshift.read_sql_query(
                """
                WITH ar AS (
                    SELECT snapshot_month, SUM(ar_balance) AS closing
                    FROM gold.fact_ar_snapshot GROUP BY snapshot_month
                ),
                act AS (
                    SELECT post_year_month AS snapshot_month,
                           SUM(charge_amount) c, SUM(payment_amount) p,
                           SUM(adjustment_amount) a
                    FROM gold.fact_transaction GROUP BY post_year_month
                )
                SELECT MAX(ABS(ar.closing
                       - (LAG(ar.closing) OVER (ORDER BY ar.snapshot_month)
                          + COALESCE(act.c,0) - COALESCE(act.p,0)
                          - COALESCE(act.a,0)))) AS max_variance
                FROM ar LEFT JOIN act USING (snapshot_month)
                """,
                con=wr.redshift.connect("redshift-rcm"),
            )
            max_var = float(variance["max_variance"].iloc[0] or 0)
            if max_var > 1.00:
                raise AirflowFailException(
                    f"A/R roll-forward does not reconcile: max variance ${max_var:,.2f}")
            print(f"A/R reconciles, max variance ${max_var:.2f}")
            return {"max_variance": max_var}

        crawl_catalog = GlueJobOperator(
            task_id="refresh_catalog",
            job_name="rcm-prod-catalog-refresh",
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )

        load_warehouse >> verify_warehouse()
        crawl_catalog

    @task(task_id="refresh_powerbi", retries=1)
    def refresh_powerbi(**context) -> None:
        """
        Trigger the Power BI dataset refresh.

        Deliberately tolerant: the warehouse is already correct at this point,
        so a failed report refresh is a notification, not a pipeline failure.
        """
        import requests
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection("powerbi")
        url = (f"https://api.powerbi.com/v1.0/myorg/groups/{conn.login}"
               f"/datasets/{conn.schema}/refreshes")
        resp = requests.post(url, headers={"Authorization": f"Bearer {conn.password}"},
                             timeout=30)
        if resp.status_code not in (200, 202):
            raise AirflowSkipException(
                f"Power BI refresh not accepted ({resp.status_code}); "
                "warehouse load already succeeded.")
        print("Power BI refresh accepted")

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    (start >> await_sources >> ingest_bronze >> build_silver
     >> quality >> quality_branch)
    quality_branch >> quality_failed
    quality_branch >> build_gold >> publish >> refresh_powerbi() >> end
