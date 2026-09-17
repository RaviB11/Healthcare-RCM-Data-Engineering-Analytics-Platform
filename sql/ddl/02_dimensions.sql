-- ===========================================================================
-- 02: dimension tables
--
-- Distribution strategy
-- ---------------------
-- Small dimensions  -> DISTSTYLE ALL  (replicated; joins never shuffle)
-- dim_patient       -> DISTKEY patient_sk (large enough that replication hurts)
-- Sort keys follow the columns the BI layer actually filters on.
-- ===========================================================================

DROP TABLE IF EXISTS gold.dim_date CASCADE;
CREATE TABLE gold.dim_date (
    date_key       INTEGER      NOT NULL PRIMARY KEY,
    full_date      DATE         NOT NULL,
    day_of_month   SMALLINT,
    day_of_week    SMALLINT,
    day_name       VARCHAR(10),
    week_of_year   SMALLINT,
    month_number   SMALLINT,
    month_name     VARCHAR(12),
    month_year     CHAR(7),
    quarter        SMALLINT,
    quarter_label  VARCHAR(10),
    year           SMALLINT,
    fiscal_year    SMALLINT,
    is_weekend     BOOLEAN,
    is_month_end   BOOLEAN
)
DISTSTYLE ALL
SORTKEY (date_key);

DROP TABLE IF EXISTS gold.dim_patient CASCADE;
CREATE TABLE gold.dim_patient (
    patient_sk      BIGINT        NOT NULL,
    patient_id      VARCHAR(20)   NOT NULL,
    mrn             VARCHAR(30),
    first_name      VARCHAR(60),
    last_name       VARCHAR(60),
    full_name       VARCHAR(120),
    gender          CHAR(1),
    date_of_birth   DATE,
    patient_age     SMALLINT,
    age_band        VARCHAR(10),
    address_line_1  VARCHAR(200),
    city            VARCHAR(80),
    state           CHAR(2),
    zip_code        VARCHAR(10),
    phone_number    VARCHAR(20),
    email_address   VARCHAR(120),
    source_system   VARCHAR(30),
    -- Type 2 history
    effective_from  TIMESTAMP     NOT NULL,
    effective_to    TIMESTAMP     NOT NULL,
    is_current      BOOLEAN       NOT NULL,
    version_number  SMALLINT      NOT NULL,
    record_hash     CHAR(64),
    PRIMARY KEY (patient_sk)
)
DISTKEY (patient_sk)
SORTKEY (patient_id, effective_from);

DROP TABLE IF EXISTS gold.dim_provider CASCADE;
CREATE TABLE gold.dim_provider (
    provider_sk         BIGINT      NOT NULL PRIMARY KEY,
    provider_id         VARCHAR(20) NOT NULL,
    npi                 CHAR(10),
    provider_name       VARCHAR(150),
    provider_first_name VARCHAR(60),
    provider_last_name  VARCHAR(60),
    credential          VARCHAR(10),
    specialty           VARCHAR(60),
    department_id       VARCHAR(20),
    department_name     VARCHAR(80),
    service_line        VARCHAR(40),
    hospital            VARCHAR(20),
    is_active           BOOLEAN
)
DISTSTYLE ALL
SORTKEY (provider_sk);

DROP TABLE IF EXISTS gold.dim_payer CASCADE;
CREATE TABLE gold.dim_payer (
    payer_sk         BIGINT      NOT NULL PRIMARY KEY,
    payer_id         VARCHAR(20) NOT NULL,
    payer_name       VARCHAR(100),
    payer_type       VARCHAR(40),
    payer_group      VARCHAR(30),
    contracted_rate  DECIMAL(5,4),
    avg_days_to_pay  SMALLINT,
    is_government    BOOLEAN
)
DISTSTYLE ALL
SORTKEY (payer_sk);

DROP TABLE IF EXISTS gold.dim_procedure CASCADE;
CREATE TABLE gold.dim_procedure (
    procedure_sk           BIGINT      NOT NULL PRIMARY KEY,
    cpt_code               VARCHAR(5)  NOT NULL,
    cpt_description        VARCHAR(255),
    service_category       VARCHAR(40),
    standard_charge_amount DECIMAL(18,2),
    is_high_cost           BOOLEAN
)
DISTSTYLE ALL
SORTKEY (procedure_sk);

DROP TABLE IF EXISTS gold.dim_diagnosis CASCADE;
CREATE TABLE gold.dim_diagnosis (
    diagnosis_sk      BIGINT      NOT NULL PRIMARY KEY,
    icd10_code        VARCHAR(10) NOT NULL,
    icd10_description VARCHAR(255),
    diagnosis_chapter VARCHAR(60)
)
DISTSTYLE ALL
SORTKEY (diagnosis_sk);

DROP TABLE IF EXISTS gold.dim_denial_reason CASCADE;
CREATE TABLE gold.dim_denial_reason (
    denial_sk                 BIGINT      NOT NULL PRIMARY KEY,
    carc_code                 VARCHAR(10) NOT NULL,
    carc_description          VARCHAR(255),
    denial_category           VARCHAR(40),
    is_appealable             BOOLEAN,
    is_patient_responsibility BOOLEAN,
    is_preventable            BOOLEAN
)
DISTSTYLE ALL
SORTKEY (denial_sk);
