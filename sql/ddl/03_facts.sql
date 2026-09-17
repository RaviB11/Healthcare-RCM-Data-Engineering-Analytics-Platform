-- ===========================================================================
-- 03: fact tables
--
-- fact_claim is the workhorse. It is distributed on payer_sk because almost
-- every analytical query in revenue cycle slices by payer, and the payer
-- dimension is replicated (DISTSTYLE ALL) so those joins stay local.
-- Sorting on service_date_key makes the near-universal date predicate a
-- zone-map scan rather than a full table read.
-- ===========================================================================

DROP TABLE IF EXISTS gold.fact_claim CASCADE;
CREATE TABLE gold.fact_claim (
    claim_sk                      BIGINT       NOT NULL,
    claim_id                      VARCHAR(20)  NOT NULL,
    encounter_id                  VARCHAR(20),

    -- foreign keys
    patient_sk                    BIGINT       NOT NULL,
    payer_sk                      BIGINT       NOT NULL,
    provider_sk                   BIGINT       NOT NULL,
    denial_sk                     BIGINT       NOT NULL,
    diagnosis_sk                  BIGINT       NOT NULL,
    service_date_key              INTEGER,
    submission_date_key           INTEGER,
    remit_date_key                INTEGER,

    -- degenerate dimensions
    service_date                  DATE,
    submission_date               DATE,
    remit_date                    DATE,
    last_activity_date            DATE,
    claim_status                  VARCHAR(20),
    claim_type                    VARCHAR(20),
    encounter_type                VARCHAR(20),
    source_hospital               VARCHAR(20),
    carc_code                     VARCHAR(10),
    denial_category               VARCHAR(40),

    -- additive measures
    total_charge_amount           DECIMAL(18,2),
    allowed_amount                DECIMAL(18,2),
    contractual_adjustment_amount DECIMAL(18,2),
    total_payment_amount          DECIMAL(18,2),
    insurance_payment_amount      DECIMAL(18,2),
    patient_payment_amount        DECIMAL(18,2),
    total_adjustment_amount       DECIMAL(18,2),
    patient_responsibility        DECIMAL(18,2),
    outstanding_balance           DECIMAL(18,2),
    expected_reimbursement        DECIMAL(18,2),
    underpayment_amount           DECIMAL(18,2),

    -- semi-additive / non-additive
    charge_lag_days               INTEGER,
    days_to_payment               INTEGER,
    days_outstanding              INTEGER,
    ar_age_days                   INTEGER,
    ar_aging_bucket               VARCHAR(10),
    length_of_stay_days           INTEGER,

    -- flags the marts and DAX aggregate over
    is_denied                     BOOLEAN,
    is_open                       BOOLEAN,
    is_adjudicated                BOOLEAN,
    is_denial_carc                BOOLEAN,
    is_clean_claim                BOOLEAN,
    is_first_pass_resolved        BOOLEAN,
    is_preventable_denial         BOOLEAN,
    is_timely_filed               BOOLEAN,
    is_bad_debt_candidate         BOOLEAN,

    service_year_month            CHAR(7),
    PRIMARY KEY (claim_sk)
)
DISTKEY (payer_sk)
COMPOUND SORTKEY (service_date_key, payer_sk);

DROP TABLE IF EXISTS gold.fact_claim_line CASCADE;
CREATE TABLE gold.fact_claim_line (
    claim_line_sk                 BIGINT      NOT NULL PRIMARY KEY,
    claim_sk                      BIGINT      NOT NULL,
    claim_line_id                 VARCHAR(20) NOT NULL,
    claim_id                      VARCHAR(20) NOT NULL,
    procedure_sk                  BIGINT      NOT NULL,
    diagnosis_sk                  BIGINT      NOT NULL,
    payer_sk                      BIGINT      NOT NULL,
    provider_sk                   BIGINT      NOT NULL,
    service_date_key              INTEGER,
    service_date                  DATE,
    cpt_code                      VARCHAR(5),
    icd10_code                    VARCHAR(10),
    service_category              VARCHAR(40),
    modifier                      VARCHAR(4),
    units                         INTEGER,
    line_charge_amount            DECIMAL(18,2),
    unit_charge_amount            DECIMAL(18,2),
    standard_charge_amount        DECIMAL(18,2),
    charge_variance_from_standard DECIMAL(18,2),
    claim_status                  VARCHAR(20)
)
DISTKEY (claim_sk)
COMPOUND SORTKEY (service_date_key, procedure_sk);

DROP TABLE IF EXISTS gold.fact_transaction CASCADE;
CREATE TABLE gold.fact_transaction (
    transaction_sk         BIGINT      NOT NULL PRIMARY KEY,
    claim_sk               BIGINT      NOT NULL,
    transaction_id         VARCHAR(20) NOT NULL,
    claim_id               VARCHAR(20),
    encounter_id           VARCHAR(20),
    patient_id             VARCHAR(20),
    payer_sk               BIGINT      NOT NULL,
    post_date_key          INTEGER,
    post_date              DATE,
    transaction_type       VARCHAR(30),
    transaction_amount     DECIMAL(18,2),
    charge_amount          DECIMAL(18,2),
    payment_amount         DECIMAL(18,2),
    adjustment_amount      DECIMAL(18,2),
    adjustment_reason_code VARCHAR(20),
    post_year_month        CHAR(7)
)
DISTKEY (claim_sk)
COMPOUND SORTKEY (post_date_key, transaction_type);

-- Accumulating month-end snapshot of open A/R.
-- This is the table the aged trial balance reconciles to.
DROP TABLE IF EXISTS gold.fact_ar_snapshot CASCADE;
CREATE TABLE gold.fact_ar_snapshot (
    snapshot_date_key   INTEGER     NOT NULL,
    snapshot_date       DATE        NOT NULL,
    snapshot_month      CHAR(7)     NOT NULL,
    claim_sk            BIGINT      NOT NULL,
    claim_id            VARCHAR(20) NOT NULL,
    payer_sk            BIGINT      NOT NULL,
    payer_id            VARCHAR(20),
    provider_sk         BIGINT,
    provider_id         VARCHAR(20),
    source_hospital     VARCHAR(20),
    submission_date     DATE,
    total_charge_amount DECIMAL(18,2),
    payments_to_date    DECIMAL(18,2),
    adjustments_to_date DECIMAL(18,2),
    ar_balance          DECIMAL(18,2),
    ar_age_days         INTEGER,
    ar_aging_bucket     VARCHAR(10),
    is_over_90          BOOLEAN,
    is_credit_balance   BOOLEAN,
    PRIMARY KEY (claim_sk, snapshot_date_key)
)
DISTKEY (payer_sk)
COMPOUND SORTKEY (snapshot_date_key, ar_aging_bucket);

-- ---------------------------------------------------------------------------
-- Referential integrity. Redshift does not enforce these, but the planner
-- uses them to eliminate redundant joins, so declaring them is not decorative.
-- ---------------------------------------------------------------------------
ALTER TABLE gold.fact_claim ADD FOREIGN KEY (patient_sk)  REFERENCES gold.dim_patient(patient_sk);
ALTER TABLE gold.fact_claim ADD FOREIGN KEY (payer_sk)    REFERENCES gold.dim_payer(payer_sk);
ALTER TABLE gold.fact_claim ADD FOREIGN KEY (provider_sk) REFERENCES gold.dim_provider(provider_sk);
ALTER TABLE gold.fact_claim ADD FOREIGN KEY (denial_sk)   REFERENCES gold.dim_denial_reason(denial_sk);
ALTER TABLE gold.fact_claim ADD FOREIGN KEY (diagnosis_sk) REFERENCES gold.dim_diagnosis(diagnosis_sk);
ALTER TABLE gold.fact_claim ADD FOREIGN KEY (service_date_key) REFERENCES gold.dim_date(date_key);
