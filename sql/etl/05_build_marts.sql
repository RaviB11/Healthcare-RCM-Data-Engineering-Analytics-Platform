-- ===========================================================================
-- 05: in-warehouse KPI marts and reporting views
--
-- The Spark job already writes these marts to S3. They are rebuilt here too
-- so the warehouse can serve them without a round trip to the lake, and so
-- an analyst can read the exact SQL behind every number on the dashboard.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Late-binding view: the definition of "current patient" in one place.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_dim_patient_current AS
SELECT * FROM gold.dim_patient WHERE is_current;

-- ---------------------------------------------------------------------------
-- Monthly revenue cycle scorecard
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS marts.kpi_monthly_scorecard;
CREATE TABLE marts.kpi_monthly_scorecard
DISTSTYLE ALL
SORTKEY (service_year_month)
AS
WITH claim_month AS (
    SELECT
        service_year_month,
        COUNT(*)                                             AS claim_count,
        SUM(CASE WHEN is_adjudicated THEN 1 ELSE 0 END)      AS adjudicated_claims,
        SUM(total_charge_amount)                             AS gross_charges,
        SUM(allowed_amount)                                  AS allowed_amount,
        SUM(contractual_adjustment_amount)                   AS contractual_adjustments,
        SUM(total_payment_amount)                            AS total_payments,
        SUM(insurance_payment_amount)                        AS insurance_payments,
        SUM(patient_payment_amount)                          AS patient_payments,
        SUM(CASE WHEN is_denied THEN 1 ELSE 0 END)           AS denied_claims,
        SUM(CASE WHEN is_clean_claim THEN 1 ELSE 0 END)      AS clean_claims,
        SUM(CASE WHEN is_first_pass_resolved THEN 1 ELSE 0 END) AS fpr_claims,
        SUM(CASE WHEN is_preventable_denial THEN 1 ELSE 0 END)  AS preventable_denials,
        SUM(CASE WHEN is_denied THEN total_charge_amount ELSE 0 END) AS denied_charge_amount,
        AVG(charge_lag_days::DECIMAL(10,2))                  AS avg_charge_lag_days,
        AVG(days_to_payment::DECIMAL(10,2))                  AS avg_days_to_payment
    FROM gold.fact_claim
    GROUP BY service_year_month
),
-- Point-in-time A/R, NOT the residual balance of that month's service cohort.
-- This is the figure that reconciles to the aged trial balance.
period_ar AS (
    SELECT
        snapshot_month AS service_year_month,
        SUM(ar_balance)                                          AS total_ar_at_month_end,
        SUM(CASE WHEN is_over_90 THEN ar_balance ELSE 0 END)     AS ar_over_90,
        COUNT(*)                                                 AS open_claims_at_month_end
    FROM gold.fact_ar_snapshot
    GROUP BY snapshot_month
),
joined AS (
    SELECT c.*, a.total_ar_at_month_end, a.ar_over_90, a.open_claims_at_month_end,
           -- 91-day trailing charges = the denominator of Days in A/R
           SUM(c.gross_charges) OVER (
               ORDER BY c.service_year_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
           ) AS trailing_3m_charges
    FROM claim_month c
    LEFT JOIN period_ar a USING (service_year_month)
)
SELECT
    service_year_month,
    claim_count,
    adjudicated_claims,
    gross_charges,
    allowed_amount,
    contractual_adjustments,
    total_payments,
    insurance_payments,
    patient_payments,
    denied_claims,
    clean_claims,
    preventable_denials,
    denied_charge_amount,
    total_ar_at_month_end,
    ar_over_90,
    open_claims_at_month_end,
    avg_charge_lag_days,
    avg_days_to_payment,
    ROUND(denied_claims::DECIMAL / NULLIF(adjudicated_claims, 0), 4) AS denial_rate,
    ROUND(clean_claims::DECIMAL / NULLIF(adjudicated_claims, 0), 4)  AS clean_claim_rate,
    ROUND(fpr_claims::DECIMAL / NULLIF(adjudicated_claims, 0), 4)    AS first_pass_resolution_rate,
    ROUND(total_payments / NULLIF(gross_charges, 0), 4)              AS gross_collection_rate,
    -- Net collection rate measures against what was COLLECTIBLE, i.e. after
    -- contractual allowances but before bad debt write-offs.
    ROUND(total_payments / NULLIF(gross_charges - contractual_adjustments, 0), 4)
                                                                     AS net_collection_rate,
    ROUND(total_ar_at_month_end / NULLIF(trailing_3m_charges / 91.0, 0), 1)
                                                                     AS days_in_ar,
    ROUND(ar_over_90 / NULLIF(total_ar_at_month_end, 0), 4)          AS pct_ar_over_90
FROM joined;

-- ---------------------------------------------------------------------------
-- Payer scorecard
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS marts.kpi_payer_performance;
CREATE TABLE marts.kpi_payer_performance
DISTSTYLE ALL
AS
SELECT
    p.payer_sk,
    p.payer_name,
    p.payer_type,
    p.payer_group,
    COUNT(*)                                                    AS claim_count,
    SUM(f.total_charge_amount)                                  AS gross_charges,
    SUM(f.total_payment_amount)                                 AS total_payments,
    SUM(f.contractual_adjustment_amount)                        AS contractual_adjustments,
    SUM(f.outstanding_balance)                                  AS open_ar,
    SUM(f.underpayment_amount)                                  AS underpayment_exposure,
    SUM(CASE WHEN f.is_denied THEN 1 ELSE 0 END)                AS denied_claims,
    AVG(f.days_to_payment::DECIMAL(10,2))                       AS avg_days_to_payment,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.days_to_payment)
                                                                AS median_days_to_payment,
    ROUND(SUM(CASE WHEN f.is_denied THEN 1 ELSE 0 END)::DECIMAL
          / NULLIF(SUM(CASE WHEN f.is_adjudicated THEN 1 ELSE 0 END), 0), 4)
                                                                AS denial_rate,
    ROUND(SUM(f.total_payment_amount)
          / NULLIF(SUM(f.total_charge_amount) - SUM(f.contractual_adjustment_amount), 0), 4)
                                                                AS net_collection_rate
FROM gold.fact_claim f
JOIN gold.dim_payer p ON f.payer_sk = p.payer_sk
GROUP BY 1, 2, 3, 4;

-- ---------------------------------------------------------------------------
-- Denial work queue: what the follow-up team should touch first
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS marts.kpi_denial_analysis;
CREATE TABLE marts.kpi_denial_analysis
DISTSTYLE ALL
AS
SELECT
    d.carc_code,
    d.carc_description,
    d.denial_category,
    d.is_appealable,
    d.is_preventable,
    p.payer_name,
    p.payer_group,
    COUNT(*)                        AS denial_count,
    SUM(f.total_charge_amount)      AS denied_charges,
    SUM(f.outstanding_balance)      AS at_risk_balance,
    AVG(f.ar_age_days::DECIMAL(10,2)) AS avg_age_days,
    -- Only appealable denials represent genuinely recoverable cash.
    SUM(CASE WHEN d.is_appealable THEN f.outstanding_balance ELSE 0 END)
                                    AS recoverable_value
FROM gold.fact_claim f
JOIN gold.dim_denial_reason d ON f.denial_sk = d.denial_sk
JOIN gold.dim_payer p         ON f.payer_sk  = p.payer_sk
WHERE f.is_denied
GROUP BY 1, 2, 3, 4, 5, 6, 7;

GRANT SELECT ON ALL TABLES IN SCHEMA marts TO ROLE rcm_bi_reader;
