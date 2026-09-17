-- ===========================================================================
-- 04: load Gold Parquet from S3 into Redshift
--
-- Pattern: COPY into a staging table, validate, then swap. Never COPY
-- straight into a table Power BI is reading - a half-loaded fact table is
-- worse than a stale one.
--
-- :batch_id and :s3_root are bound by the Step Functions / Airflow caller.
-- ===========================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- Dimensions: full refresh (small, and history lives inside dim_patient)
-- ---------------------------------------------------------------------------
TRUNCATE staging.dim_patient_stg;
COPY staging.dim_patient_stg
FROM :'s3_root'/gold/dim_patient/
IAM_ROLE :'iam_role'
FORMAT AS PARQUET;

-- Guard: never publish an empty or suspiciously small dimension.
-- A source extract that silently failed shows up here, not in a board deck.
DO $$
DECLARE
    new_count BIGINT;
    old_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO new_count FROM staging.dim_patient_stg;
    SELECT COUNT(*) INTO old_count FROM gold.dim_patient;
    IF new_count = 0 THEN
        RAISE EXCEPTION 'dim_patient staging is empty - aborting load';
    END IF;
    IF old_count > 0 AND new_count < old_count * 0.9 THEN
        RAISE EXCEPTION 'dim_patient shrank by more than 10%% (% -> %) - aborting',
            old_count, new_count;
    END IF;
END $$;

DELETE FROM gold.dim_patient;
INSERT INTO gold.dim_patient SELECT * FROM staging.dim_patient_stg;

-- ---------------------------------------------------------------------------
-- fact_claim: delete-and-reload only the affected service months.
-- Cheaper and safer than a full reload once history grows, and idempotent
-- so a retried Airflow task cannot double-count revenue.
-- ---------------------------------------------------------------------------
TRUNCATE staging.fact_claim_stg;
COPY staging.fact_claim_stg
FROM :'s3_root'/gold/fact_claim/
IAM_ROLE :'iam_role'
FORMAT AS PARQUET;

DELETE FROM gold.fact_claim
WHERE service_year_month IN (SELECT DISTINCT service_year_month
                             FROM staging.fact_claim_stg);

INSERT INTO gold.fact_claim
SELECT * FROM staging.fact_claim_stg;

-- ---------------------------------------------------------------------------
-- A/R snapshot: replace whole months, never merge rows
-- ---------------------------------------------------------------------------
TRUNCATE staging.fact_ar_snapshot_stg;
COPY staging.fact_ar_snapshot_stg
FROM :'s3_root'/gold/fact_ar_snapshot/
IAM_ROLE :'iam_role'
FORMAT AS PARQUET;

DELETE FROM gold.fact_ar_snapshot
WHERE snapshot_month IN (SELECT DISTINCT snapshot_month
                         FROM staging.fact_ar_snapshot_stg);

INSERT INTO gold.fact_ar_snapshot
SELECT * FROM staging.fact_ar_snapshot_stg;

-- ---------------------------------------------------------------------------
-- Post-load integrity checks. These fail the transaction rather than
-- publishing a broken star schema.
-- ---------------------------------------------------------------------------

-- 1. No orphan foreign keys
DO $$
DECLARE orphans BIGINT;
BEGIN
    SELECT COUNT(*) INTO orphans
    FROM gold.fact_claim f
    LEFT JOIN gold.dim_payer p ON f.payer_sk = p.payer_sk
    WHERE p.payer_sk IS NULL;
    IF orphans > 0 THEN
        RAISE EXCEPTION 'fact_claim has % rows with no matching payer', orphans;
    END IF;
END $$;

-- 2. Claim grain is still one row per claim
DO $$
DECLARE dupes BIGINT;
BEGIN
    SELECT COUNT(*) INTO dupes FROM (
        SELECT claim_id FROM gold.fact_claim
        GROUP BY claim_id HAVING COUNT(*) > 1
    );
    IF dupes > 0 THEN
        RAISE EXCEPTION 'fact_claim grain violated: % duplicated claim_ids', dupes;
    END IF;
END $$;

-- 3. Exactly one current row per patient in the Type 2 dimension
DO $$
DECLARE bad BIGINT;
BEGIN
    SELECT COUNT(*) INTO bad FROM (
        SELECT patient_id FROM gold.dim_patient
        WHERE is_current GROUP BY patient_id HAVING COUNT(*) <> 1
    );
    IF bad > 0 THEN
        RAISE EXCEPTION 'dim_patient has % patients without exactly one current row', bad;
    END IF;
END $$;

COMMIT;

-- Statistics and storage housekeeping. Skipping ANALYZE after a large load
-- is the most common reason a Redshift dashboard "suddenly got slow".
ANALYZE gold.fact_claim;
ANALYZE gold.fact_ar_snapshot;
ANALYZE gold.dim_patient;
VACUUM DELETE ONLY gold.fact_claim;
