"""
Silver layer: conform, clean, validate, historise.

This is where the two hospital EMRs stop looking like two different
products. Every entity is mapped onto one canonical schema, typed
properly, de-duplicated, run through the declarative DQ engine, and -
for patients - turned into a Type 2 history.

Bad rows are not dropped. They are written to the quarantine zone with
the list of rules they broke, so someone can actually fix the source.

Run:
    python -m src.silver.build_silver
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.utils import (get_logger, get_spark, layer_path, load_config,
                              new_batch_id, path_exists, read_parquet,
                              with_audit_columns, write_parquet)
from src.quality.dq_engine import DataQualityEngine, DQReport
from src.silver.scd2 import apply_scd2

LOG = get_logger("silver.build")

# Columns whose change should open a new version of a patient record.
PATIENT_TRACKED_COLS = ["first_name", "last_name", "gender", "date_of_birth",
                        "address_line_1", "city", "state", "zip_code",
                        "phone_number", "email_address"]


def bronze(spark: SparkSession, table: str, cfg: dict) -> DataFrame:
    return read_parquet(spark, layer_path("bronze", table, cfg))


def parse_date(col: str, *formats: str):
    """Try each format in order; the first that parses wins."""
    expr = F.to_date(F.col(col), formats[0])
    for fmt in formats[1:]:
        expr = F.coalesce(expr, F.to_date(F.col(col), fmt))
    return expr


def parse_ts(col: str, *formats: str):
    expr = F.to_timestamp(F.col(col), formats[0])
    for fmt in formats[1:]:
        expr = F.coalesce(expr, F.to_timestamp(F.col(col), fmt))
    return expr


def clean_string(col: str):
    """Trim, collapse whitespace, and turn empty strings into real nulls."""
    c = F.regexp_replace(F.trim(F.col(col)), r"\s+", " ")
    return F.when((c == "") | (F.upper(c).isin("NULL", "N/A", "NONE", "UNKNOWN")), None).otherwise(c)


# --------------------------------------------------------------------------
# Patients: two schemas in, one canonical schema out
# --------------------------------------------------------------------------

def conform_patients_a(df: DataFrame) -> DataFrame:
    return df.select(
        clean_string("patient_id").alias("patient_id"),
        clean_string("mrn").alias("mrn"),
        F.initcap(clean_string("first_name")).alias("first_name"),
        F.initcap(clean_string("last_name")).alias("last_name"),
        parse_date("date_of_birth", "yyyy-MM-dd", "MM/dd/yyyy").alias("date_of_birth"),
        F.upper(F.substring(clean_string("gender"), 1, 1)).alias("gender"),
        clean_string("address_line1").alias("address_line_1"),
        F.initcap(clean_string("city")).alias("city"),
        F.upper(clean_string("state")).alias("state"),
        clean_string("zip_code").alias("zip_code"),
        F.regexp_replace(clean_string("phone_number"), r"[^0-9]", "").alias("phone_number"),
        F.lower(clean_string("email_address")).alias("email_address"),
        parse_ts("last_updated_timestamp", "yyyy-MM-dd HH:mm:ss").alias("source_updated_at"),
        F.lit("EMR_HOSP_A").alias("source_system"),
    )


def conform_patients_b(df: DataFrame) -> DataFrame:
    """
    Hospital B is the awkward one: 'Last, First' in a single column,
    US date format, MALE/FEMALE instead of M/F, different column names.
    """
    name = F.split(clean_string("full_name"), ",")
    return df.select(
        clean_string("id").alias("patient_id"),
        clean_string("medical_record_no").alias("mrn"),
        F.initcap(F.trim(name.getItem(1))).alias("first_name"),
        F.initcap(F.trim(name.getItem(0))).alias("last_name"),
        parse_date("birth_date", "MM/dd/yyyy", "yyyy-MM-dd").alias("date_of_birth"),
        F.when(F.upper(clean_string("sex")) == "MALE", "M")
         .when(F.upper(clean_string("sex")) == "FEMALE", "F")
         .otherwise("U").alias("gender"),
        clean_string("street").alias("address_line_1"),
        F.initcap(clean_string("city_name")).alias("city"),
        F.upper(clean_string("state_code")).alias("state"),
        clean_string("postal_code").alias("zip_code"),
        F.regexp_replace(clean_string("contact_number"), r"[^0-9]", "").alias("phone_number"),
        F.lower(clean_string("email_id")).alias("email_address"),
        parse_ts("modified_ts", "MM/dd/yyyy HH:mm", "yyyy-MM-dd HH:mm:ss").alias("source_updated_at"),
        F.lit("EMR_HOSP_B").alias("source_system"),
    )


def build_patients(spark, cfg, dq, batch_id, reports) -> DataFrame:
    LOG.info("building silver_patients (SCD2)")
    a = conform_patients_a(bronze(spark, "patients_hospital_a", cfg))
    b = conform_patients_b(bronze(spark, "patients_hospital_b", cfg))
    delta = conform_patients_a(bronze(spark, "patients_hospital_a_delta", cfg))

    union = a.unionByName(b).unionByName(delta)

    # De-duplicate exact source duplicates (same key AND same change stamp).
    # Multiple rows per patient_id with DIFFERENT stamps are kept on purpose:
    # they are the versions that SCD2 turns into history.
    before = union.count()
    w = Window.partitionBy("patient_id", "source_updated_at").orderBy(F.col("mrn"))
    union = (union.withColumn("_rn", F.row_number().over(w))
                  .filter(F.col("_rn") == 1).drop("_rn")).cache()
    removed = before - union.count()
    if removed:
        LOG.warning("  removed %s exact duplicate source rows", f"{removed:,}")

    # Derived attributes
    union = (union
             .withColumn("patient_age",
                         F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25))
             .withColumn("age_band",
                         F.when(F.col("patient_age") < 18, "0-17")
                          .when(F.col("patient_age") < 35, "18-34")
                          .when(F.col("patient_age") < 50, "35-49")
                          .when(F.col("patient_age") < 65, "50-64")
                          .when(F.col("patient_age") < 80, "65-79")
                          .otherwise("80+"))
             .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name")))

    clean, quarantine, report = dq.validate(union, "silver_patients")
    reports.append(report)
    _write_quarantine(quarantine, "silver_patients", batch_id, cfg)

    # Type 2 history. On a rerun we merge into the existing dimension.
    target_path = layer_path("silver", "patients", cfg)
    target = read_parquet(spark, target_path) if path_exists(spark, target_path) else None
    scd = apply_scd2(target, clean, "patient_id", PATIENT_TRACKED_COLS, "source_updated_at")

    scd = with_audit_columns(scd, "EMR", batch_id, "silver")
    write_parquet(scd, target_path, coalesce=2)

    versions = scd.groupBy("version_number").count().orderBy("version_number").collect()
    LOG.info("  SCD2 version distribution: %s",
             {r["version_number"]: r["count"] for r in versions})
    return scd


# --------------------------------------------------------------------------
# Encounters
# --------------------------------------------------------------------------

def build_encounters(spark, cfg, dq, batch_id, reports) -> DataFrame:
    LOG.info("building silver_encounters")
    a = bronze(spark, "encounters_hospital_a", cfg).select(
        clean_string("encounter_id").alias("encounter_id"),
        clean_string("patient_id").alias("patient_id"),
        clean_string("provider_id").alias("provider_id"),
        clean_string("department_id").alias("department_id"),
        F.initcap(clean_string("encounter_type")).alias("encounter_type"),
        parse_date("admit_date", "yyyy-MM-dd").alias("admit_date"),
        parse_date("discharge_date", "yyyy-MM-dd").alias("discharge_date"),
        clean_string("primary_diagnosis_code").alias("primary_diagnosis_code"),
        clean_string("payer_id").alias("payer_id"),
        F.lit("EMR_HOSP_A").alias("source_system"),
    )
    b = bronze(spark, "encounters_hospital_b", cfg).select(
        clean_string("visit_id").alias("encounter_id"),
        clean_string("id").alias("patient_id"),
        clean_string("rendering_provider").alias("provider_id"),
        clean_string("dept_id").alias("department_id"),
        F.initcap(F.lower(clean_string("visit_type"))).alias("encounter_type"),
        parse_date("admission_dt", "MM/dd/yyyy").alias("admit_date"),
        parse_date("discharge_dt", "MM/dd/yyyy").alias("discharge_date"),
        clean_string("primary_dx").alias("primary_diagnosis_code"),
        clean_string("insurance_id").alias("payer_id"),
        F.lit("EMR_HOSP_B").alias("source_system"),
    )

    enc = (a.unionByName(b)
           .withColumn("length_of_stay_days",
                       F.datediff("discharge_date", "admit_date"))
           .withColumn("is_inpatient", F.col("encounter_type").isin("Inpatient", "Observation"))
           .withColumn("admit_year_month", F.date_format("admit_date", "yyyy-MM")))

    clean, quarantine, report = dq.validate(enc, "silver_encounters")
    reports.append(report)
    _write_quarantine(quarantine, "silver_encounters", batch_id, cfg)

    clean = with_audit_columns(clean, "EMR", batch_id, "silver")
    write_parquet(clean, layer_path("silver", "encounters", cfg),
                  partition_by=["source_system"], coalesce=4)
    return clean


# --------------------------------------------------------------------------
# Claims, claim lines, transactions, remittance
# --------------------------------------------------------------------------

def build_claims(spark, cfg, dq, batch_id, reports, payers: DataFrame) -> DataFrame:
    LOG.info("building silver_claims")
    raw = bronze(spark, "claims", cfg)

    claims = raw.select(
        clean_string("claim_id").alias("claim_id"),
        clean_string("encounter_id").alias("encounter_id"),
        clean_string("patient_id").alias("patient_id"),
        clean_string("payer_id").alias("payer_id"),
        clean_string("provider_id").alias("provider_id"),
        parse_date("service_date", "yyyy-MM-dd").alias("service_date"),
        parse_date("submission_date", "yyyy-MM-dd").alias("submission_date"),
        F.col("total_charge_amount").cast("decimal(18,2)").alias("total_charge_amount"),
        F.col("allowed_amount").cast("decimal(18,2)").alias("allowed_amount"),
        F.upper(clean_string("claim_status")).alias("claim_status"),
        F.upper(clean_string("claim_type")).alias("claim_type"),
        clean_string("source_hospital").alias("source_hospital"),
    )

    # Exact duplicate claim rows are a known source defect: keep one.
    claims = claims.dropDuplicates(["claim_id"])

    claims = (claims
              .withColumn("charge_lag_days", F.datediff("submission_date", "service_date"))
              .withColumn("is_denied", F.col("claim_status") == "DENIED")
              .withColumn("is_open",
                          F.col("claim_status").isin("IN_PROCESS", "SUBMITTED", "APPEALED"))
              .withColumn("contractual_adjustment_amount",
                          (F.col("total_charge_amount") - F.col("allowed_amount"))
                          .cast("decimal(18,2)"))
              .withColumn("service_year_month", F.date_format("service_date", "yyyy-MM")))

    clean, quarantine, report = dq.validate(claims, "silver_claims", refs={"payers": payers})
    reports.append(report)
    _write_quarantine(quarantine, "silver_claims", batch_id, cfg)

    clean = with_audit_columns(clean, "BILLING", batch_id, "silver")
    write_parquet(clean, layer_path("silver", "claims", cfg),
                  partition_by=["service_year_month"], coalesce=1)
    return clean


def build_claim_lines(spark, cfg, dq, batch_id, reports) -> DataFrame:
    LOG.info("building silver_claim_lines")
    lines = bronze(spark, "claim_lines", cfg).select(
        clean_string("claim_line_id").alias("claim_line_id"),
        clean_string("claim_id").alias("claim_id"),
        F.upper(clean_string("cpt_code")).alias("cpt_code"),
        F.upper(clean_string("icd10_code")).alias("icd10_code"),
        F.col("units").cast("int").alias("units"),
        F.col("line_charge_amount").cast("decimal(18,2)").alias("line_charge_amount"),
        parse_date("service_date", "yyyy-MM-dd").alias("service_date"),
        clean_string("modifier").alias("modifier"),
    ).withColumn("unit_charge_amount",
                 (F.col("line_charge_amount") / F.col("units")).cast("decimal(18,2)"))

    clean, quarantine, report = dq.validate(lines, "silver_claim_lines")
    reports.append(report)
    _write_quarantine(quarantine, "silver_claim_lines", batch_id, cfg)

    clean = with_audit_columns(clean, "BILLING", batch_id, "silver")
    write_parquet(clean, layer_path("silver", "claim_lines", cfg), coalesce=2)
    return clean


def build_transactions(spark, cfg, dq, batch_id, reports) -> DataFrame:
    LOG.info("building silver_transactions")
    txn = bronze(spark, "transactions", cfg).select(
        clean_string("transaction_id").alias("transaction_id"),
        clean_string("claim_id").alias("claim_id"),
        clean_string("encounter_id").alias("encounter_id"),
        clean_string("patient_id").alias("patient_id"),
        F.upper(clean_string("transaction_type")).alias("transaction_type"),
        F.col("transaction_amount").cast("decimal(18,2)").alias("transaction_amount"),
        parse_date("post_date", "yyyy-MM-dd").alias("post_date"),
        clean_string("payer_id").alias("payer_id"),
        clean_string("adjustment_reason_code").alias("adjustment_reason_code"),
    ).dropDuplicates(["transaction_id"])

    # Signed convention: charges positive, everything that reduces A/R negative.
    txn = (txn
           .withColumn("charge_amount",
                       F.when(F.col("transaction_type") == "CHARGE",
                              F.col("transaction_amount")).otherwise(F.lit(0))
                       .cast("decimal(18,2)"))
           .withColumn("payment_amount",
                       F.when(F.col("transaction_type").isin("INSURANCE_PAYMENT",
                                                             "PATIENT_PAYMENT"),
                              -F.col("transaction_amount")).otherwise(F.lit(0))
                       .cast("decimal(18,2)"))
           .withColumn("adjustment_amount",
                       F.when(F.col("transaction_type").isin("CONTRACTUAL_ADJUSTMENT",
                                                             "WRITEOFF"),
                              -F.col("transaction_amount")).otherwise(F.lit(0))
                       .cast("decimal(18,2)"))
           .withColumn("post_year_month", F.date_format("post_date", "yyyy-MM")))

    clean, quarantine, report = dq.validate(txn, "silver_transactions")
    reports.append(report)
    _write_quarantine(quarantine, "silver_transactions", batch_id, cfg)

    clean = with_audit_columns(clean, "BILLING", batch_id, "silver")
    write_parquet(clean, layer_path("silver", "transactions", cfg),
                  partition_by=["post_year_month"], coalesce=1)
    return clean


def build_remittance(spark, cfg, dq, batch_id, reports) -> DataFrame:
    LOG.info("building silver_remittance")
    era = bronze(spark, "era_835", cfg).select(
        clean_string("remittance_id").alias("remittance_id"),
        clean_string("claim_id").alias("claim_id"),
        clean_string("payer_id").alias("payer_id"),
        parse_date("remit_date", "yyyy-MM-dd").alias("remit_date"),
        F.col("billed_amount").cast("decimal(18,2)").alias("billed_amount"),
        F.col("allowed_amount").cast("decimal(18,2)").alias("allowed_amount"),
        F.col("paid_amount").cast("decimal(18,2)").alias("paid_amount"),
        F.col("patient_responsibility").cast("decimal(18,2)").alias("patient_responsibility"),
        F.upper(clean_string("carc_code")).alias("carc_code"),
        clean_string("denied_flag").alias("denied_flag"),
    ).dropDuplicates(["remittance_id"])

    clean, quarantine, report = dq.validate(era, "silver_remittance")
    reports.append(report)
    _write_quarantine(quarantine, "silver_remittance", batch_id, cfg)

    clean = (clean
             .withColumn("is_denied", F.col("denied_flag") == "1")
             .withColumn("write_off_amount",
                         (F.col("billed_amount") - F.col("allowed_amount")).cast("decimal(18,2)")))
    clean = with_audit_columns(clean, "PAYER_REMITTANCE", batch_id, "silver")
    write_parquet(clean, layer_path("silver", "remittance", cfg), coalesce=1)
    return clean


# --------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------

def build_reference(spark, cfg, batch_id) -> dict[str, DataFrame]:
    LOG.info("building silver reference tables")
    out = {}

    payers = bronze(spark, "payers", cfg).select(
        clean_string("payer_id").alias("payer_id"),
        clean_string("payer_name").alias("payer_name"),
        clean_string("payer_type").alias("payer_type"),
        F.col("contracted_rate").cast("decimal(5,4)").alias("contracted_rate"),
        F.col("avg_days_to_pay").cast("int").alias("avg_days_to_pay"),
    ).withColumn("is_government", F.col("payer_type") == "Government")
    out["payers"] = payers

    cpt = bronze(spark, "cpt_codes", cfg).select(
        F.upper(clean_string("cpt_code")).alias("cpt_code"),
        clean_string("description").alias("cpt_description"),
        clean_string("service_category").alias("service_category"),
        F.col("standard_charge_amount").cast("decimal(18,2)").alias("standard_charge_amount"),
    )
    out["cpt_codes"] = cpt

    icd = bronze(spark, "icd10_codes", cfg).select(
        F.upper(clean_string("icd10_code")).alias("icd10_code"),
        clean_string("description").alias("icd10_description"),
        clean_string("chapter").alias("diagnosis_chapter"),
    )
    out["icd10_codes"] = icd

    carc = bronze(spark, "carc_codes", cfg).select(
        F.upper(clean_string("carc_code")).alias("carc_code"),
        clean_string("description").alias("carc_description"),
        clean_string("denial_category").alias("denial_category"),
        (F.lower(clean_string("is_appealable")) == "true").alias("is_appealable"),
    )
    out["carc_codes"] = carc

    providers = bronze(spark, "npi_providers", cfg).select(
        clean_string("provider_id").alias("provider_id"),
        clean_string("npi").alias("npi"),
        F.initcap(clean_string("first_name")).alias("provider_first_name"),
        F.initcap(clean_string("last_name")).alias("provider_last_name"),
        clean_string("credential").alias("credential"),
        clean_string("specialty").alias("specialty"),
        clean_string("department_id").alias("department_id"),
        clean_string("hospital").alias("hospital"),
        (F.upper(clean_string("active_flag")) == "Y").alias("is_active"),
    ).withColumn("provider_name",
                 F.concat_ws(" ", F.col("provider_first_name"), F.col("provider_last_name"),
                             F.concat(F.lit(", "), F.col("credential"))))
    out["providers"] = providers

    depts = bronze(spark, "departments", cfg).select(
        clean_string("department_id").alias("department_id"),
        clean_string("department_name").alias("department_name"),
        clean_string("service_line").alias("service_line"),
    )
    out["departments"] = depts

    for name, df in out.items():
        write_parquet(with_audit_columns(df, "REFERENCE", batch_id, "silver"),
                      layer_path("silver", name, cfg), coalesce=1)
    return out


# --------------------------------------------------------------------------

def _write_quarantine(df: DataFrame, dataset: str, batch_id: str, cfg: dict) -> None:
    if df.rdd.isEmpty():
        return
    path = layer_path("quarantine", f"{dataset}/batch_id={batch_id}", cfg)
    write_parquet(df, path, coalesce=1)
    LOG.warning("  quarantined rows written to %s", path)


def main() -> int:
    cfg = load_config()
    batch_id = new_batch_id()
    spark = get_spark("silver_build", cfg)
    dq = DataQualityEngine(spark)
    reports: list[DQReport] = []

    LOG.info("=" * 78)
    LOG.info("SILVER BUILD  batch_id=%s", batch_id)
    LOG.info("=" * 78)

    refs = build_reference(spark, cfg, batch_id)
    build_patients(spark, cfg, dq, batch_id, reports)
    build_encounters(spark, cfg, dq, batch_id, reports)
    build_claims(spark, cfg, dq, batch_id, reports, refs["payers"])
    build_claim_lines(spark, cfg, dq, batch_id, reports)
    build_transactions(spark, cfg, dq, batch_id, reports)
    build_remittance(spark, cfg, dq, batch_id, reports)

    metrics = dq.metrics_dataframe(reports, batch_id)
    write_parquet(metrics, layer_path("metrics", f"dq_results/batch_id={batch_id}", cfg),
                  coalesce=1)

    total_q = sum(r.quarantined for r in reports)
    blocking = [r for rep in reports for r in rep.blocking_failures]
    LOG.info("-" * 78)
    LOG.info("Silver complete. quarantined=%s  blocking_rule_failures=%d",
             f"{total_q:,}", len(blocking))
    for r in blocking:
        LOG.warning("  %s :: %s -> %s rows", r.dataset, r.rule_name, f"{r.failed_rows:,}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
