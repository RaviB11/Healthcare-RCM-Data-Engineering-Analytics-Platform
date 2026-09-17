"""
Gold layer: a Kimball star schema plus the aggregate marts Power BI reads.

Grain decisions (the part that actually matters)
------------------------------------------------
dim_patient         one row per patient VERSION (Type 2)
dim_provider        one row per provider (Type 1)
dim_payer           one row per payer (Type 1)
dim_procedure       one row per CPT code
dim_diagnosis       one row per ICD-10 code
dim_denial_reason   one row per CARC code
dim_date            one row per calendar day

fact_claim          one row per claim
fact_claim_line     one row per claim line (CPT level)
fact_transaction    one row per financial transaction
fact_ar_snapshot    one row per open claim per month-end (accumulating)

Every fact carries surrogate keys to the dimensions AND the natural keys,
because someone in finance will always ask "which claim is that?".

Run:
    python -m src.gold.build_gold
"""

from __future__ import annotations

import sys
from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.utils import (assert_unique_grain, get_logger, get_spark,
                              layer_path, load_config, new_batch_id, read_parquet,
                              surrogate_key, write_parquet)

LOG = get_logger("gold.build")

UNKNOWN_KEY = -1


def silver(spark: SparkSession, table: str, cfg: dict) -> DataFrame:
    return read_parquet(spark, layer_path("silver", table, cfg))


def drop_audit(df: DataFrame) -> DataFrame:
    return df.drop(*[c for c in df.columns if c.startswith("_")])


# ==========================================================================
# Dimensions
# ==========================================================================

def build_dim_date(spark, cfg, start="2023-01-01", end="2027-12-31") -> DataFrame:
    LOG.info("building dim_date")
    df = (spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), "
                    f"interval 1 day)) AS full_date")
          .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
          .withColumn("day_of_month", F.dayofmonth("full_date"))
          .withColumn("day_of_week", F.dayofweek("full_date"))
          .withColumn("day_name", F.date_format("full_date", "EEEE"))
          .withColumn("week_of_year", F.weekofyear("full_date"))
          .withColumn("month_number", F.month("full_date"))
          .withColumn("month_name", F.date_format("full_date", "MMMM"))
          .withColumn("month_year", F.date_format("full_date", "yyyy-MM"))
          .withColumn("quarter", F.quarter("full_date"))
          .withColumn("quarter_label", F.concat(F.lit("Q"), F.quarter("full_date"),
                                                F.lit(" "), F.year("full_date")))
          .withColumn("year", F.year("full_date"))
          # US healthcare fiscal year commonly starts 1 October
          .withColumn("fiscal_year",
                      F.when(F.month("full_date") >= 10, F.year("full_date") + 1)
                       .otherwise(F.year("full_date")))
          .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
          .withColumn("is_month_end", F.col("full_date") == F.last_day("full_date"))
          .select("date_key", "full_date", "day_of_month", "day_of_week", "day_name",
                  "week_of_year", "month_number", "month_name", "month_year",
                  "quarter", "quarter_label", "year", "fiscal_year",
                  "is_weekend", "is_month_end"))
    write_parquet(df, layer_path("gold", "dim_date", cfg), coalesce=1)
    return df


def build_dim_patient(spark, cfg) -> DataFrame:
    LOG.info("building dim_patient (Type 2)")
    pat = drop_audit(silver(spark, "patients", cfg))
    dim = (pat
           .withColumn("patient_sk", surrogate_key("patient_id", "version_number"))
           .select("patient_sk", "patient_id", "mrn", "first_name", "last_name",
                   "full_name", "gender", "date_of_birth", "patient_age", "age_band",
                   "address_line_1", "city", "state", "zip_code", "phone_number",
                   "email_address", "source_system", "effective_from", "effective_to",
                   "is_current", "version_number", "record_hash"))
    # An "unknown member" row keeps facts from losing rows on inner joins.
    unknown = spark.createDataFrame(
        [(UNKNOWN_KEY, "UNKNOWN", "UNKNOWN", "Unknown", "Unknown", "Unknown Patient",
          "U", None, None, "Unknown", None, None, None, None, None, None, "SYSTEM",
          None, None, True, 1, "unknown")],
        schema=dim.schema)
    dim = dim.unionByName(unknown)
    write_parquet(dim, layer_path("gold", "dim_patient", cfg), coalesce=2)
    return dim


def build_dim_provider(spark, cfg) -> DataFrame:
    LOG.info("building dim_provider")
    prov = drop_audit(silver(spark, "providers", cfg))
    dept = drop_audit(silver(spark, "departments", cfg))
    dim = (prov.join(dept, "department_id", "left")
           .withColumn("provider_sk", surrogate_key("provider_id"))
           .select("provider_sk", "provider_id", "npi", "provider_name",
                   "provider_first_name", "provider_last_name", "credential",
                   "specialty", "department_id", "department_name", "service_line",
                   "hospital", "is_active"))
    write_parquet(dim, layer_path("gold", "dim_provider", cfg), coalesce=1)
    return dim


def build_dim_payer(spark, cfg) -> DataFrame:
    LOG.info("building dim_payer")
    dim = (drop_audit(silver(spark, "payers", cfg))
           .withColumn("payer_sk", surrogate_key("payer_id"))
           .withColumn("payer_group",
                       F.when(F.col("payer_type").isin("Government", "Medicare Advantage"),
                              "Government")
                        .when(F.col("payer_type") == "Self Pay", "Self Pay")
                        .when(F.col("payer_type") == "Workers Comp", "Workers Comp")
                        .otherwise("Commercial"))
           .select("payer_sk", "payer_id", "payer_name", "payer_type", "payer_group",
                   "contracted_rate", "avg_days_to_pay", "is_government"))
    write_parquet(dim, layer_path("gold", "dim_payer", cfg), coalesce=1)
    return dim


def build_dim_procedure(spark, cfg) -> DataFrame:
    LOG.info("building dim_procedure")
    dim = (drop_audit(silver(spark, "cpt_codes", cfg))
           .withColumn("procedure_sk", surrogate_key("cpt_code"))
           .withColumn("is_high_cost", F.col("standard_charge_amount") >= 1000)
           .select("procedure_sk", "cpt_code", "cpt_description", "service_category",
                   "standard_charge_amount", "is_high_cost"))
    write_parquet(dim, layer_path("gold", "dim_procedure", cfg), coalesce=1)
    return dim


def build_dim_diagnosis(spark, cfg) -> DataFrame:
    LOG.info("building dim_diagnosis")
    dim = (drop_audit(silver(spark, "icd10_codes", cfg))
           .withColumn("diagnosis_sk", surrogate_key("icd10_code"))
           .select("diagnosis_sk", "icd10_code", "icd10_description", "diagnosis_chapter"))
    write_parquet(dim, layer_path("gold", "dim_diagnosis", cfg), coalesce=1)
    return dim


def build_dim_denial_reason(spark, cfg) -> DataFrame:
    LOG.info("building dim_denial_reason")
    carc = drop_audit(silver(spark, "carc_codes", cfg))
    dim = (carc
           .withColumn("denial_sk", surrogate_key("carc_code"))
           .withColumn("is_patient_responsibility", F.col("carc_code").startswith("PR-"))
           .withColumn("is_preventable",
                       F.col("denial_category").isin("Technical", "Authorization",
                                                     "Duplicate", "Timely Filing"))
           .select("denial_sk", "carc_code", "carc_description", "denial_category",
                   "is_appealable", "is_patient_responsibility", "is_preventable"))
    write_parquet(dim, layer_path("gold", "dim_denial_reason", cfg), coalesce=1)
    return dim


# ==========================================================================
# Facts
# ==========================================================================

def build_fact_claim(spark, cfg, dims: dict) -> DataFrame:
    LOG.info("building fact_claim")
    claims = drop_audit(silver(spark, "claims", cfg))
    era = drop_audit(silver(spark, "remittance", cfg))
    enc = drop_audit(silver(spark, "encounters", cfg)).select(
        "encounter_id", "encounter_type", "department_id", "length_of_stay_days",
        "primary_diagnosis_code")
    txn = drop_audit(silver(spark, "transactions", cfg))

    # Money actually collected / written off, rolled up to the claim.
    money = (txn.groupBy("claim_id").agg(
        F.sum("charge_amount").cast("decimal(18,2)").alias("posted_charge_amount"),
        F.sum("payment_amount").cast("decimal(18,2)").alias("total_payment_amount"),
        F.sum("adjustment_amount").cast("decimal(18,2)").alias("total_adjustment_amount"),
        F.sum(F.when(F.col("transaction_type") == "INSURANCE_PAYMENT",
                     F.col("payment_amount")).otherwise(0))
         .cast("decimal(18,2)").alias("insurance_payment_amount"),
        F.sum(F.when(F.col("transaction_type") == "PATIENT_PAYMENT",
                     F.col("payment_amount")).otherwise(0))
         .cast("decimal(18,2)").alias("patient_payment_amount"),
        F.max("post_date").alias("last_activity_date"),
    ))

    remit = era.select(
        "claim_id",
        F.col("remit_date"),
        F.col("paid_amount").alias("remit_paid_amount"),
        F.col("patient_responsibility"),
        F.col("carc_code"),
        F.col("is_denied").alias("remit_is_denied"),
    )

    as_of = F.lit(cfg["business_rules"]["as_of_date"]).cast("date")

    # SCD2 point-in-time lookup: which version of the patient was true on
    # the date of service? Columns are pre-renamed so the join condition
    # cannot be ambiguous.
    #
    # Boundaries are compared at TIMESTAMP precision, not date precision.
    # Casting effective_from/to down to a date makes both versions match on
    # the day a record changed, which silently duplicates the claim - and a
    # duplicated claim is duplicated revenue. The service date is anchored to
    # the start of the day, so the version in force when the day began wins.
    pat_pit = dims["patient"].select(
        F.col("patient_sk"),
        F.col("patient_id").alias("_pit_patient_id"),
        F.col("effective_from").alias("_pit_from"),
        F.col("effective_to").alias("_pit_to"),
    )

    fact = (claims
            .join(money, "claim_id", "left")
            .join(remit, "claim_id", "left")
            .join(enc, "encounter_id", "left")
            .join(pat_pit,
                  (F.col("patient_id") == F.col("_pit_patient_id"))
                  & (F.col("service_date").cast("timestamp") >= F.col("_pit_from"))
                  & (F.col("service_date").cast("timestamp") <= F.col("_pit_to")),
                  "left")
            .drop("_pit_patient_id", "_pit_from", "_pit_to")
            .join(dims["payer"].select("payer_sk", "payer_id"), "payer_id", "left")
            .join(dims["provider"].select("provider_sk", "provider_id"), "provider_id", "left")
            .join(dims["denial"].select("denial_sk", "carc_code", "denial_category",
                                        "is_preventable"), "carc_code", "left")
            .join(dims["diagnosis"].select("diagnosis_sk",
                                           F.col("icd10_code").alias("primary_diagnosis_code")),
                  "primary_diagnosis_code", "left"))

    fact = (fact
            .withColumn("claim_sk", surrogate_key("claim_id"))
            .withColumn("patient_sk", F.coalesce("patient_sk", F.lit(UNKNOWN_KEY)))
            .withColumn("payer_sk", F.coalesce("payer_sk", F.lit(UNKNOWN_KEY)))
            .withColumn("provider_sk", F.coalesce("provider_sk", F.lit(UNKNOWN_KEY)))
            .withColumn("denial_sk", F.coalesce("denial_sk", F.lit(UNKNOWN_KEY)))
            .withColumn("diagnosis_sk", F.coalesce("diagnosis_sk", F.lit(UNKNOWN_KEY)))
            .withColumn("service_date_key",
                        F.date_format("service_date", "yyyyMMdd").cast("int"))
            .withColumn("submission_date_key",
                        F.date_format("submission_date", "yyyyMMdd").cast("int"))
            .withColumn("remit_date_key",
                        F.date_format("remit_date", "yyyyMMdd").cast("int"))
            # --- measures -------------------------------------------------
            .withColumn("total_payment_amount", F.coalesce("total_payment_amount", F.lit(0))
                        .cast("decimal(18,2)"))
            .withColumn("total_adjustment_amount",
                        F.coalesce("total_adjustment_amount", F.lit(0)).cast("decimal(18,2)"))
            .withColumn("outstanding_balance",
                        (F.col("total_charge_amount") - F.col("total_payment_amount")
                         - F.col("total_adjustment_amount")).cast("decimal(18,2)"))
            .withColumn("expected_reimbursement", F.col("allowed_amount"))
            .withColumn("underpayment_amount",
                        F.greatest(F.col("allowed_amount") - F.col("total_payment_amount"),
                                   F.lit(0)).cast("decimal(18,2)"))
            .withColumn("days_to_payment", F.datediff("remit_date", "submission_date"))
            .withColumn("days_outstanding",
                        F.when(F.col("remit_date").isNull(),
                               F.datediff(as_of, F.col("submission_date")))
                         .otherwise(F.datediff("remit_date", "submission_date")))
            .withColumn("ar_age_days",
                        F.when(F.col("outstanding_balance") > 0,
                               F.datediff(as_of, F.col("submission_date"))))
            .withColumn("ar_aging_bucket",
                        F.when(F.col("outstanding_balance") <= 0, "Closed")
                         .when(F.col("ar_age_days") <= 30, "0-30")
                         .when(F.col("ar_age_days") <= 60, "31-60")
                         .when(F.col("ar_age_days") <= 90, "61-90")
                         .when(F.col("ar_age_days") <= 120, "91-120")
                         .when(F.col("ar_age_days") <= 180, "121-180")
                         .otherwise("180+"))
            # Adjudicated = the payer has returned a remittance. Claims still
            # in flight are excluded from rate denominators, otherwise every
            # recent month looks like a catastrophe purely because it is young.
            .withColumn("is_adjudicated", F.col("remit_date").isNotNull())
            #
            # A clean claim passes payer edits and adjudicates on first
            # submission without a DENIAL reason code. PR-1 / PR-2 (deductible,
            # coinsurance) and CO-45 (contractual write-down) are normal
            # adjudication outcomes, NOT denials - counting them as denials is
            # what drags a reported clean claim rate down to a nonsense number.
            .withColumn("is_denial_carc",
                        F.coalesce(F.col("denial_category"), F.lit("")).isin(
                            "Technical", "Medical Necessity", "Authorization",
                            "Duplicate", "Timely Filing", "Coordination of Benefits",
                            "Bundling"))
            .withColumn("is_clean_claim",
                        F.col("remit_date").isNotNull() & ~F.col("is_denial_carc"))
            # First pass resolution: adjudicated AND closed to a zero balance
            # without anyone having to rework it.
            .withColumn("is_first_pass_resolved",
                        F.col("remit_date").isNotNull()
                        & ~F.col("is_denial_carc")
                        & (F.col("outstanding_balance") <= F.lit(0.01)))
            .withColumn("is_preventable_denial",
                        F.col("is_denied") & F.coalesce(F.col("is_preventable"), F.lit(False)))
            .withColumn("is_timely_filed",
                        F.col("charge_lag_days") <= cfg["business_rules"]["timely_filing_days"])
            .withColumn("is_bad_debt_candidate",
                        (F.col("outstanding_balance") > 0) & (F.col("ar_age_days") > 180)))

    fact = fact.select(
        "claim_sk", "claim_id", "encounter_id",
        "patient_sk", "payer_sk", "provider_sk", "denial_sk", "diagnosis_sk",
        "service_date_key", "submission_date_key", "remit_date_key",
        "service_date", "submission_date", "remit_date", "last_activity_date",
        "claim_status", "claim_type", "encounter_type", "source_hospital",
        "carc_code", "denial_category",
        "total_charge_amount", "allowed_amount", "contractual_adjustment_amount",
        "total_payment_amount", "insurance_payment_amount", "patient_payment_amount",
        "total_adjustment_amount", "patient_responsibility", "outstanding_balance",
        "expected_reimbursement", "underpayment_amount",
        "charge_lag_days", "days_to_payment", "days_outstanding", "ar_age_days",
        "ar_aging_bucket", "length_of_stay_days",
        "is_denied", "is_open", "is_adjudicated", "is_denial_carc",
        "is_clean_claim", "is_first_pass_resolved",
        "is_preventable_denial", "is_timely_filed", "is_bad_debt_candidate",
    ).withColumn("service_year_month", F.date_format("service_date", "yyyy-MM"))

    assert_unique_grain(fact, ["claim_id"], "fact_claim")
    write_parquet(fact, layer_path("gold", "fact_claim", cfg),
                  partition_by=["service_year_month"], coalesce=1)
    return fact


def build_fact_claim_line(spark, cfg, dims: dict) -> DataFrame:
    LOG.info("building fact_claim_line")
    lines = drop_audit(silver(spark, "claim_lines", cfg))
    claims = drop_audit(silver(spark, "claims", cfg)).select(
        "claim_id", "payer_id", "provider_id", "claim_status", "patient_id")

    fact = (lines.join(claims, "claim_id", "left")
            .join(dims["procedure"].select("procedure_sk", "cpt_code",
                                           "service_category", "standard_charge_amount"),
                  "cpt_code", "left")
            .join(dims["diagnosis"].select("diagnosis_sk",
                                           F.col("icd10_code").alias("icd10_code")),
                  "icd10_code", "left")
            .join(dims["payer"].select("payer_sk", "payer_id"), "payer_id", "left")
            .join(dims["provider"].select("provider_sk", "provider_id"), "provider_id", "left")
            .withColumn("claim_line_sk", surrogate_key("claim_line_id"))
            .withColumn("claim_sk", surrogate_key("claim_id"))
            .withColumn("service_date_key",
                        F.date_format("service_date", "yyyyMMdd").cast("int"))
            .withColumn("charge_variance_from_standard",
                        (F.col("unit_charge_amount") - F.col("standard_charge_amount"))
                        .cast("decimal(18,2)"))
            .select("claim_line_sk", "claim_sk", "claim_line_id", "claim_id",
                    F.coalesce("procedure_sk", F.lit(UNKNOWN_KEY)).alias("procedure_sk"),
                    F.coalesce("diagnosis_sk", F.lit(UNKNOWN_KEY)).alias("diagnosis_sk"),
                    F.coalesce("payer_sk", F.lit(UNKNOWN_KEY)).alias("payer_sk"),
                    F.coalesce("provider_sk", F.lit(UNKNOWN_KEY)).alias("provider_sk"),
                    "service_date_key", "service_date", "cpt_code", "icd10_code",
                    "service_category", "modifier", "units", "line_charge_amount",
                    "unit_charge_amount", "standard_charge_amount",
                    "charge_variance_from_standard", "claim_status"))

    assert_unique_grain(fact, ["claim_line_id"], "fact_claim_line")
    write_parquet(fact, layer_path("gold", "fact_claim_line", cfg), coalesce=2)
    return fact


def build_fact_transaction(spark, cfg, dims: dict) -> DataFrame:
    LOG.info("building fact_transaction")
    txn = drop_audit(silver(spark, "transactions", cfg))
    fact = (txn
            .join(dims["payer"].select("payer_sk", "payer_id"), "payer_id", "left")
            .withColumn("transaction_sk", surrogate_key("transaction_id"))
            .withColumn("claim_sk", surrogate_key("claim_id"))
            .withColumn("post_date_key", F.date_format("post_date", "yyyyMMdd").cast("int"))
            .select("transaction_sk", "claim_sk", "transaction_id", "claim_id",
                    "encounter_id", "patient_id",
                    F.coalesce("payer_sk", F.lit(UNKNOWN_KEY)).alias("payer_sk"),
                    "post_date_key", "post_date", "transaction_type",
                    "transaction_amount", "charge_amount", "payment_amount",
                    "adjustment_amount", "adjustment_reason_code", "post_year_month"))
    assert_unique_grain(fact, ["transaction_id"], "fact_transaction")
    write_parquet(fact, layer_path("gold", "fact_transaction", cfg),
                  partition_by=["post_year_month"], coalesce=1)
    return fact


def build_fact_ar_snapshot(spark, cfg, fact_claim: DataFrame) -> DataFrame:
    """
    Month-end accumulating snapshot of open A/R.

    Recomputed per month-end from the transaction ledger rather than
    carried forward, so a late-posted payment retroactively corrects the
    history instead of leaving a permanently wrong month.
    """
    LOG.info("building fact_ar_snapshot")
    txn = drop_audit(silver(spark, "transactions", cfg))
    claims = drop_audit(silver(spark, "claims", cfg)).select(
        "claim_id", "payer_id", "provider_id", "submission_date", "total_charge_amount",
        "claim_status", "source_hospital")

    period_end = cfg["business_rules"]["as_of_date"]

    # Aggregate the ledger to claim-month FIRST. Cross joining every claim to
    # every month-end and then joining raw transactions is correct but explodes
    # the intermediate; this keeps the shuffle proportional to claims x months.
    txn_monthly = (txn
                   .withColumn("activity_month", F.trunc("post_date", "month"))
                   .groupBy("claim_id", "activity_month")
                   .agg(F.sum("payment_amount").alias("month_payments"),
                        F.sum("adjustment_amount").alias("month_adjustments")))

    # One row per claim per month it was open, from submission month to period end.
    claim_months = (claims
                    .withColumn("_start", F.trunc("submission_date", "month"))
                    .withColumn("activity_month",
                                F.explode(F.expr(
                                    f"sequence(_start, trunc(date'{period_end}', 'month'), "
                                    f"interval 1 month)")))
                    .drop("_start"))

    # A running total converts per-month activity into a point-in-time balance,
    # so a late-posted payment retroactively corrects every later month.
    w_cum = (Window.partitionBy("claim_id").orderBy("activity_month")
             .rowsBetween(Window.unboundedPreceding, Window.currentRow))

    activity = (claim_months
                .join(txn_monthly, ["claim_id", "activity_month"], "left")
                .withColumn("payments_to_date",
                            F.sum(F.coalesce("month_payments", F.lit(0))).over(w_cum)
                            .cast("decimal(18,2)"))
                .withColumn("adjustments_to_date",
                            F.sum(F.coalesce("month_adjustments", F.lit(0))).over(w_cum)
                            .cast("decimal(18,2)"))
                .withColumn("snapshot_date", F.last_day("activity_month")))

    snap = (activity
            .withColumn("ar_balance",
                        (F.col("total_charge_amount") - F.col("payments_to_date")
                         - F.col("adjustments_to_date")).cast("decimal(18,2)"))
            # Keep CREDIT balances (overpayments) instead of filtering them out.
            # They are a reportable liability - hospitals must identify and
            # refund them - and dropping them silently breaks the A/R
            # roll-forward reconciliation.
            .filter(F.abs(F.col("ar_balance")) > 0.01)
            .withColumn("is_credit_balance", F.col("ar_balance") < 0)
            .withColumn("ar_age_days", F.datediff("snapshot_date", "submission_date"))
            .withColumn("ar_aging_bucket",
                        F.when(F.col("ar_balance") < 0, "Credit")
                         .when(F.col("ar_age_days") <= 30, "0-30")
                         .when(F.col("ar_age_days") <= 60, "31-60")
                         .when(F.col("ar_age_days") <= 90, "61-90")
                         .when(F.col("ar_age_days") <= 120, "91-120")
                         .when(F.col("ar_age_days") <= 180, "121-180")
                         .otherwise("180+"))
            .withColumn("is_over_90",
                        (F.col("ar_age_days") > 90) & (F.col("ar_balance") > 0))
            .withColumn("snapshot_date_key",
                        F.date_format("snapshot_date", "yyyyMMdd").cast("int"))
            .withColumn("claim_sk", surrogate_key("claim_id"))
            .withColumn("payer_sk", surrogate_key("payer_id"))
            .withColumn("provider_sk", surrogate_key("provider_id"))
            .withColumn("snapshot_month", F.date_format("snapshot_date", "yyyy-MM"))
            .select("snapshot_date_key", "snapshot_date", "snapshot_month", "claim_sk",
                    "claim_id", "payer_sk", "payer_id", "provider_sk", "provider_id",
                    "source_hospital", "submission_date", "total_charge_amount",
                    "payments_to_date", "adjustments_to_date", "ar_balance",
                    "ar_age_days", "ar_aging_bucket", "is_over_90",
                    "is_credit_balance"))

    assert_unique_grain(snap, ["claim_id", "snapshot_date"], "fact_ar_snapshot")
    write_parquet(snap, layer_path("gold", "fact_ar_snapshot", cfg),
                  partition_by=["snapshot_month"], coalesce=1)
    return snap


# ==========================================================================
# KPI marts - pre-aggregated so Power BI stays fast on import
# ==========================================================================

def build_kpi_marts(spark, cfg, fact_claim: DataFrame, ar: DataFrame,
                    dims: dict) -> dict[str, DataFrame]:
    LOG.info("building KPI marts")
    out = {}
    payer = dims["payer"].select("payer_sk", "payer_name", "payer_type", "payer_group")

    # --- monthly revenue cycle scorecard ---------------------------------
    monthly = (fact_claim
               .groupBy("service_year_month")
               .agg(F.count("*").alias("claim_count"),
                    F.sum(F.col("is_adjudicated").cast("int")).alias("adjudicated_claims"),
                    F.sum("total_charge_amount").alias("gross_charges"),
                    F.sum("allowed_amount").alias("allowed_amount"),
                    F.sum("contractual_adjustment_amount").alias("contractual_adjustments"),
                    F.sum("total_payment_amount").alias("total_payments"),
                    F.sum("insurance_payment_amount").alias("insurance_payments"),
                    F.sum("patient_payment_amount").alias("patient_payments"),
                    F.sum("outstanding_balance").alias("ending_ar"),
                    F.sum(F.col("is_denied").cast("int")).alias("denied_claims"),
                    F.sum(F.col("is_clean_claim").cast("int")).alias("clean_claims"),
                    F.sum(F.col("is_first_pass_resolved").cast("int")).alias("fpr_claims"),
                    F.sum(F.col("is_preventable_denial").cast("int"))
                     .alias("preventable_denials"),
                    F.sum(F.when(F.col("is_denied"), F.col("total_charge_amount"))
                          .otherwise(0)).alias("denied_charge_amount"),
                    F.avg("charge_lag_days").alias("avg_charge_lag_days"),
                    F.avg("days_to_payment").alias("avg_days_to_payment"))
               # Rates are taken over ADJUDICATED claims. Dividing by every
               # claim ever submitted makes the current month look broken when
               # it is simply not finished adjudicating yet.
               .withColumn("denial_rate",
                           F.round(F.col("denied_claims")
                                   / F.nullif(F.col("adjudicated_claims"), F.lit(0)), 4))
               .withColumn("clean_claim_rate",
                           F.round(F.col("clean_claims")
                                   / F.nullif(F.col("adjudicated_claims"), F.lit(0)), 4))
               .withColumn("first_pass_resolution_rate",
                           F.round(F.col("fpr_claims")
                                   / F.nullif(F.col("adjudicated_claims"), F.lit(0)), 4))
               .withColumn("gross_collection_rate",
                           F.round(F.col("total_payments") / F.col("gross_charges"), 4))
               .withColumn("net_collection_rate",
                           F.round(F.col("total_payments")
                                   / F.nullif(F.col("gross_charges")
                                              - F.col("contractual_adjustments"), F.lit(0)), 4))
               .orderBy("service_year_month"))

    #
    # Days in A/R = total A/R at period end / average daily charges.
    #
    # The A/R figure must be the POINT-IN-TIME balance of every open claim at
    # that month end - taken from the snapshot fact - not the residual balance
    # of the claims whose service date happened to fall in that month. Using
    # the service-month cohort gives a number that is small and meaningless for
    # recent months and does not reconcile to the aged trial balance.
    period_ar = (ar.groupBy(F.col("snapshot_month").alias("service_year_month"))
                 .agg(F.sum("ar_balance").alias("total_ar_at_month_end"),
                      F.sum(F.when(F.col("is_over_90"), F.col("ar_balance"))
                            .otherwise(0)).alias("ar_over_90"),
                      F.count("*").alias("open_claims_at_month_end")))

    w3 = Window.orderBy("service_year_month").rowsBetween(-2, 0)
    monthly = (monthly.join(period_ar, "service_year_month", "left")
               .withColumn("trailing_3m_charges", F.sum("gross_charges").over(w3))
               .withColumn("avg_daily_charges", F.col("trailing_3m_charges") / F.lit(91))
               .withColumn("days_in_ar",
                           F.round(F.col("total_ar_at_month_end")
                                   / F.nullif(F.col("avg_daily_charges"), F.lit(0)), 1))
               .withColumn("pct_ar_over_90",
                           F.round(F.col("ar_over_90")
                                   / F.nullif(F.col("total_ar_at_month_end"), F.lit(0)), 4))
               .drop("trailing_3m_charges")
               .orderBy("service_year_month"))
    out["kpi_monthly_scorecard"] = monthly

    # --- payer performance ------------------------------------------------
    payer_perf = (fact_claim.join(payer, "payer_sk", "left")
                  .groupBy("payer_sk", "payer_name", "payer_type", "payer_group")
                  .agg(F.count("*").alias("claim_count"),
                       F.sum("total_charge_amount").alias("gross_charges"),
                       F.sum("total_payment_amount").alias("total_payments"),
                       F.sum("contractual_adjustment_amount").alias("contractual_adjustments"),
                       F.sum("outstanding_balance").alias("open_ar"),
                       F.sum("underpayment_amount").alias("underpayment_exposure"),
                       F.sum(F.col("is_denied").cast("int")).alias("denied_claims"),
                       F.avg("days_to_payment").alias("avg_days_to_payment"),
                       F.expr("percentile_approx(days_to_payment, 0.5)")
                        .alias("median_days_to_payment"))
                  .withColumn("denial_rate",
                              F.round(F.col("denied_claims") / F.col("claim_count"), 4))
                  .withColumn("net_collection_rate",
                              F.round(F.col("total_payments")
                                      / F.nullif(F.col("gross_charges")
                                                 - F.col("contractual_adjustments"),
                                                 F.lit(0)), 4))
                  .withColumn("pct_of_total_charges",
                              F.round(F.col("gross_charges")
                                      / F.sum("gross_charges").over(Window.partitionBy()), 4))
                  .orderBy(F.desc("gross_charges")))
    out["kpi_payer_performance"] = payer_perf

    # --- denial analytics: what to work first ----------------------------
    denial = (fact_claim.filter(F.col("is_denied"))
              .join(payer, "payer_sk", "left")
              .join(dims["denial"].select("denial_sk", "carc_description",
                                          "is_appealable", "is_preventable"),
                    "denial_sk", "left")
              .groupBy("carc_code", "carc_description", "denial_category",
                       "is_appealable", "is_preventable", "payer_name", "payer_group")
              .agg(F.count("*").alias("denial_count"),
                   F.sum("total_charge_amount").alias("denied_charges"),
                   F.sum("outstanding_balance").alias("at_risk_balance"),
                   F.avg("ar_age_days").alias("avg_age_days"))
              .withColumn("recoverable_value",
                          F.when(F.col("is_appealable"),
                                 F.col("at_risk_balance")).otherwise(F.lit(0)))
              .orderBy(F.desc("recoverable_value")))
    out["kpi_denial_analysis"] = denial

    # --- A/R aging summary by month and bucket ---------------------------
    aging = (ar.join(payer, "payer_sk", "left")
             .groupBy("snapshot_month", "snapshot_date", "ar_aging_bucket",
                      "payer_name", "payer_group")
             .agg(F.count("*").alias("open_claims"),
                  F.sum("ar_balance").alias("ar_balance"))
             .withColumn("pct_of_month_ar",
                         F.round(F.col("ar_balance")
                                 / F.sum("ar_balance").over(
                                     Window.partitionBy("snapshot_month")), 4))
             .orderBy("snapshot_month", "ar_aging_bucket"))
    out["kpi_ar_aging"] = aging

    # --- provider productivity -------------------------------------------
    prov = dims["provider"].select("provider_sk", "provider_name", "specialty",
                                   "department_name", "service_line", "hospital")
    provider_perf = (fact_claim.join(prov, "provider_sk", "left")
                     .groupBy("provider_sk", "provider_name", "specialty",
                              "department_name", "service_line", "hospital")
                     .agg(F.count("*").alias("claim_count"),
                          F.sum("total_charge_amount").alias("gross_charges"),
                          F.sum("total_payment_amount").alias("total_payments"),
                          F.sum(F.col("is_denied").cast("int")).alias("denied_claims"),
                          F.avg("charge_lag_days").alias("avg_charge_lag_days"),
                          F.sum("outstanding_balance").alias("open_ar"))
                     .withColumn("denial_rate",
                                 F.round(F.col("denied_claims") / F.col("claim_count"), 4))
                     .withColumn("avg_charge_per_claim",
                                 F.round(F.col("gross_charges") / F.col("claim_count"), 2))
                     .orderBy(F.desc("gross_charges")))
    out["kpi_provider_performance"] = provider_perf

    # --- service line / CPT profitability --------------------------------
    lines = read_parquet(spark, layer_path("gold", "fact_claim_line", cfg))
    svc = (lines.groupBy("service_category", "cpt_code")
           .agg(F.count("*").alias("line_count"),
                F.sum("units").alias("total_units"),
                F.sum("line_charge_amount").alias("gross_charges"),
                F.avg("unit_charge_amount").alias("avg_unit_charge"),
                F.avg("charge_variance_from_standard").alias("avg_variance_from_standard"))
           .orderBy(F.desc("gross_charges")))
    out["kpi_service_line"] = svc

    for name, df in out.items():
        write_parquet(df, layer_path("gold", name, cfg), coalesce=1)
    return out


# ==========================================================================

def main() -> int:
    cfg = load_config()
    batch_id = new_batch_id()
    spark = get_spark("gold_build", cfg)

    LOG.info("=" * 78)
    LOG.info("GOLD BUILD  batch_id=%s  as_of=%s", batch_id,
             cfg["business_rules"]["as_of_date"])
    LOG.info("=" * 78)

    dims = {
        "date": build_dim_date(spark, cfg),
        "patient": build_dim_patient(spark, cfg),
        "provider": build_dim_provider(spark, cfg),
        "payer": build_dim_payer(spark, cfg),
        "procedure": build_dim_procedure(spark, cfg),
        "diagnosis": build_dim_diagnosis(spark, cfg),
        "denial": build_dim_denial_reason(spark, cfg),
    }

    fact_claim = build_fact_claim(spark, cfg, dims).cache()
    build_fact_claim_line(spark, cfg, dims)
    build_fact_transaction(spark, cfg, dims)
    ar = build_fact_ar_snapshot(spark, cfg, fact_claim)
    build_kpi_marts(spark, cfg, fact_claim, ar, dims)

    LOG.info("-" * 78)
    LOG.info("Gold complete.")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
