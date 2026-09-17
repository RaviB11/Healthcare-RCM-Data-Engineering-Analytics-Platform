-- ===========================================================================
-- Healthcare RCM Data Warehouse - Amazon Redshift
-- 01: schemas, roles and external (Spectrum) access to the S3 lake
-- ===========================================================================
-- Run order: 01 -> 02 -> 03 -> 04 -> 05
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS staging;   -- landing area for COPY
CREATE SCHEMA IF NOT EXISTS gold;      -- the star schema Power BI reads
CREATE SCHEMA IF NOT EXISTS marts;     -- pre-aggregated KPI tables
CREATE SCHEMA IF NOT EXISTS audit;     -- batch and data-quality telemetry

-- ---------------------------------------------------------------------------
-- Redshift Spectrum: query Silver Parquet on S3 without loading it.
-- Useful for ad hoc investigation of quarantined rows without growing
-- the cluster's local storage.
-- ---------------------------------------------------------------------------
CREATE EXTERNAL SCHEMA IF NOT EXISTS lake_silver
FROM DATA CATALOG
DATABASE 'rcm_prod'
IAM_ROLE 'arn:aws:iam::000000000000:role/RcmRedshiftSpectrumRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- ---------------------------------------------------------------------------
-- Read-only role for BI. Power BI never touches base tables directly;
-- it reads the marts and the reporting views.
-- ---------------------------------------------------------------------------
CREATE ROLE rcm_bi_reader;
GRANT USAGE ON SCHEMA gold, marts TO ROLE rcm_bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA gold, marts TO ROLE rcm_bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO ROLE rcm_bi_reader;

-- ---------------------------------------------------------------------------
-- Audit tables: every batch and every DQ rule evaluation is recorded.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.batch_log (
    batch_id          VARCHAR(64)   NOT NULL,
    layer             VARCHAR(20)   NOT NULL,
    job_name          VARCHAR(100)  NOT NULL,
    status            VARCHAR(20)   NOT NULL,
    rows_written      BIGINT,
    started_at        TIMESTAMP     NOT NULL,
    completed_at      TIMESTAMP,
    duration_seconds  INTEGER,
    error_message     VARCHAR(4000)
)
DISTSTYLE ALL
SORTKEY (started_at);

CREATE TABLE IF NOT EXISTS audit.dq_results (
    batch_id     VARCHAR(64)  NOT NULL,
    dataset      VARCHAR(100) NOT NULL,
    rule_name    VARCHAR(100) NOT NULL,
    rule_type    VARCHAR(40),
    severity     VARCHAR(20),
    columns      VARCHAR(500),
    failed_rows  BIGINT,
    total_rows   BIGINT,
    pass_rate    DECIMAL(9,6),
    evaluated_at TIMESTAMP
)
DISTSTYLE ALL
SORTKEY (evaluated_at, dataset);
