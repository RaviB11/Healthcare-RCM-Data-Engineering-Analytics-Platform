-- ===========================================================================
-- Revenue Cycle analytics - the questions a CFO actually asks
--
-- Written in portable ANSI SQL so the same file runs against Redshift and
-- against the Parquet lake via DuckDB (see `make sql-check`).
--
-- Every query is tagged with the metric definition it implements, because
-- "denial rate" means four different things to four different people and
-- the argument should happen once, here, not in every meeting.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- Q1. Executive scorecard: the six numbers on the front page
--
--   Days in A/R           total A/R / average daily charges (91-day trailing)
--   Clean claim rate      adjudicated with no denial CARC / adjudicated
--   Denial rate           denied / adjudicated
--   Net collection rate   payments / (charges - contractual allowances)
--   A/R over 90 days      share of open A/R aged past 90 days
--   Charge lag            days from service to claim submission
-- ---------------------------------------------------------------------------
WITH claims AS (
    SELECT * FROM fact_claim
),
ar AS (
    SELECT * FROM fact_ar_snapshot
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM fact_ar_snapshot)
),
charges_90d AS (
    SELECT SUM(total_charge_amount) AS charges_last_90
    FROM claims
    WHERE service_date > (SELECT MAX(service_date) FROM claims) - INTERVAL 90 DAY
)
SELECT
    ROUND(SUM(ar.ar_balance) / NULLIF((SELECT charges_last_90 / 90.0
                                       FROM charges_90d), 0), 1)  AS days_in_ar,
    ROUND(SUM(CASE WHEN ar.is_over_90 THEN ar.ar_balance ELSE 0 END)
          / NULLIF(SUM(ar.ar_balance), 0), 4)                     AS pct_ar_over_90,
    ROUND(SUM(ar.ar_balance), 2)                                  AS total_open_ar
FROM ar;


-- ---------------------------------------------------------------------------
-- Q2. Aged trial balance - open A/R by payer and ageing bucket
--
-- This is the single most requested report in revenue cycle. Anything that
-- does not tie to this number is treated as wrong, regardless of how
-- sophisticated the model behind it is.
-- ---------------------------------------------------------------------------
SELECT
    p.payer_group,
    p.payer_name,
    SUM(CASE WHEN a.ar_aging_bucket = '0-30'    THEN a.ar_balance ELSE 0 END) AS bucket_0_30,
    SUM(CASE WHEN a.ar_aging_bucket = '31-60'   THEN a.ar_balance ELSE 0 END) AS bucket_31_60,
    SUM(CASE WHEN a.ar_aging_bucket = '61-90'   THEN a.ar_balance ELSE 0 END) AS bucket_61_90,
    SUM(CASE WHEN a.ar_aging_bucket = '91-120'  THEN a.ar_balance ELSE 0 END) AS bucket_91_120,
    SUM(CASE WHEN a.ar_aging_bucket = '121-180' THEN a.ar_balance ELSE 0 END) AS bucket_121_180,
    SUM(CASE WHEN a.ar_aging_bucket = '180+'    THEN a.ar_balance ELSE 0 END) AS bucket_180_plus,
    SUM(a.ar_balance)                                                         AS total_ar,
    ROUND(SUM(CASE WHEN a.is_over_90 THEN a.ar_balance ELSE 0 END)
          / NULLIF(SUM(a.ar_balance), 0), 4)                                  AS pct_over_90
FROM fact_ar_snapshot a
JOIN dim_payer p ON a.payer_sk = p.payer_sk
WHERE a.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_ar_snapshot)
GROUP BY p.payer_group, p.payer_name
ORDER BY total_ar DESC;


-- ---------------------------------------------------------------------------
-- Q3. Denial Pareto - where is the recoverable money?
--
-- Ranked by RECOVERABLE balance, not denial count. A high-volume denial
-- reason that is not appealable is a process fix, not a work queue; sorting
-- by count sends the follow-up team after the wrong claims.
-- ---------------------------------------------------------------------------
WITH denials AS (
    SELECT
        d.carc_code,
        d.carc_description,
        d.denial_category,
        d.is_appealable,
        d.is_preventable,
        COUNT(*)                   AS denial_count,
        SUM(f.total_charge_amount) AS denied_charges,
        SUM(CASE WHEN d.is_appealable THEN f.outstanding_balance ELSE 0 END)
                                   AS recoverable_value
    FROM fact_claim f
    JOIN dim_denial_reason d ON f.denial_sk = d.denial_sk
    WHERE f.is_denied
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    carc_code,
    denial_category,
    is_appealable,
    is_preventable,
    denial_count,
    ROUND(denied_charges, 2)    AS denied_charges,
    ROUND(recoverable_value, 2) AS recoverable_value,
    ROUND(100.0 * SUM(recoverable_value) OVER (ORDER BY recoverable_value DESC)
          / NULLIF(SUM(recoverable_value) OVER (), 0), 1) AS cumulative_pct_of_recoverable
FROM denials
ORDER BY recoverable_value DESC;


-- ---------------------------------------------------------------------------
-- Q4. Payer behaviour: who pays, how much, and how slowly?
--
-- The underpayment column is the contract-compliance question: the payer
-- agreed an allowed amount, so anything short of it on a settled claim is
-- money left on the table, not a contractual discount.
-- ---------------------------------------------------------------------------
SELECT
    p.payer_name,
    p.payer_group,
    COUNT(*)                                                       AS claims,
    ROUND(SUM(f.total_charge_amount), 2)                           AS gross_charges,
    ROUND(SUM(f.total_payment_amount), 2)                          AS payments,
    ROUND(SUM(f.total_payment_amount)
          / NULLIF(SUM(f.total_charge_amount)
                   - SUM(f.contractual_adjustment_amount), 0), 4)  AS net_collection_rate,
    ROUND(SUM(CASE WHEN f.is_denied THEN 1 ELSE 0 END) * 1.0
          / NULLIF(SUM(CASE WHEN f.is_adjudicated THEN 1 ELSE 0 END), 0), 4)
                                                                   AS denial_rate,
    ROUND(AVG(f.days_to_payment), 1)                               AS avg_days_to_pay,
    ROUND(SUM(CASE WHEN f.is_adjudicated AND f.total_payment_amount < f.allowed_amount
                   THEN f.allowed_amount - f.total_payment_amount ELSE 0 END), 2)
                                                                   AS underpayment_exposure
FROM fact_claim f
JOIN dim_payer p ON f.payer_sk = p.payer_sk
GROUP BY p.payer_name, p.payer_group
ORDER BY gross_charges DESC;


-- ---------------------------------------------------------------------------
-- Q5. Charge lag: does slow billing cause denials?
--
-- Tests the operational hypothesis directly rather than asserting it. If
-- denial rate climbs with charge lag, the fix is upstream in coding, not in
-- the denial work queue.
-- ---------------------------------------------------------------------------
SELECT
    CASE
        WHEN charge_lag_days <= 3  THEN '0-3 days'
        WHEN charge_lag_days <= 7  THEN '4-7 days'
        WHEN charge_lag_days <= 14 THEN '8-14 days'
        WHEN charge_lag_days <= 30 THEN '15-30 days'
        ELSE '30+ days'
    END                                                     AS charge_lag_band,
    COUNT(*)                                                AS claims,
    ROUND(AVG(CASE WHEN is_denied THEN 1.0 ELSE 0.0 END), 4) AS denial_rate,
    ROUND(AVG(days_to_payment), 1)                          AS avg_days_to_payment,
    ROUND(SUM(total_charge_amount), 2)                      AS gross_charges
FROM fact_claim
WHERE is_adjudicated
GROUP BY 1
ORDER BY MIN(charge_lag_days);


-- ---------------------------------------------------------------------------
-- Q6. Monthly trend with month-over-month movement
--
-- Window functions rather than a self-join, so the query stays readable and
-- the planner can do it in one pass.
-- ---------------------------------------------------------------------------
WITH monthly AS (
    SELECT
        service_year_month,
        SUM(total_charge_amount)                        AS gross_charges,
        SUM(total_payment_amount)                       AS payments,
        SUM(CASE WHEN is_denied THEN 1 ELSE 0 END)      AS denied,
        SUM(CASE WHEN is_adjudicated THEN 1 ELSE 0 END) AS adjudicated
    FROM fact_claim
    GROUP BY service_year_month
)
SELECT
    service_year_month,
    ROUND(gross_charges, 2)                                    AS gross_charges,
    ROUND(payments, 2)                                         AS payments,
    ROUND(denied * 1.0 / NULLIF(adjudicated, 0), 4)            AS denial_rate,
    ROUND(gross_charges - LAG(gross_charges) OVER (ORDER BY service_year_month), 2)
                                                               AS charges_mom_change,
    ROUND(100.0 * (gross_charges - LAG(gross_charges) OVER (ORDER BY service_year_month))
          / NULLIF(LAG(gross_charges) OVER (ORDER BY service_year_month), 0), 1)
                                                               AS charges_mom_pct,
    ROUND(AVG(gross_charges) OVER (ORDER BY service_year_month
                                   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2)
                                                               AS charges_3m_moving_avg
FROM monthly
ORDER BY service_year_month;


-- ---------------------------------------------------------------------------
-- Q7. Provider outliers: who bills late and gets denied?
--
-- Uses a statistical threshold (more than one standard deviation above the
-- specialty mean) rather than an arbitrary cut-off, so the list stays short
-- and defensible when a physician asks why they are on it.
-- ---------------------------------------------------------------------------
WITH provider_stats AS (
    SELECT
        pr.provider_sk,
        pr.provider_name,
        pr.specialty,
        COUNT(*)                                                  AS claims,
        AVG(CASE WHEN f.is_denied THEN 1.0 ELSE 0.0 END)          AS denial_rate,
        AVG(f.charge_lag_days)                                    AS avg_charge_lag
    FROM fact_claim f
    JOIN dim_provider pr ON f.provider_sk = pr.provider_sk
    WHERE f.is_adjudicated
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 30          -- ignore low-volume noise
),
specialty_norms AS (
    SELECT
        specialty,
        AVG(denial_rate)    AS specialty_avg_denial_rate,
        STDDEV(denial_rate) AS specialty_stddev
    FROM provider_stats
    GROUP BY specialty
)
SELECT
    s.provider_name,
    s.specialty,
    s.claims,
    ROUND(s.denial_rate, 4)                 AS denial_rate,
    ROUND(n.specialty_avg_denial_rate, 4)   AS specialty_avg,
    ROUND(s.avg_charge_lag, 1)              AS avg_charge_lag_days,
    ROUND((s.denial_rate - n.specialty_avg_denial_rate)
          / NULLIF(n.specialty_stddev, 0), 2) AS z_score
FROM provider_stats s
JOIN specialty_norms n ON s.specialty = n.specialty
WHERE s.denial_rate > n.specialty_avg_denial_rate + COALESCE(n.specialty_stddev, 0)
ORDER BY z_score DESC;


-- ---------------------------------------------------------------------------
-- Q8. A/R roll-forward: does the balance actually reconcile?
--
-- Opening A/R + charges - payments - adjustments should equal closing A/R.
-- If it does not, the ledger and the snapshot disagree and every downstream
-- number is suspect. This is the reconciliation control, not a KPI.
-- ---------------------------------------------------------------------------
WITH monthly_ar AS (
    SELECT
        snapshot_month,
        SUM(ar_balance) AS closing_ar
    FROM fact_ar_snapshot
    GROUP BY snapshot_month
),
monthly_activity AS (
    SELECT
        post_year_month AS snapshot_month,
        SUM(charge_amount)     AS charges,
        SUM(payment_amount)    AS payments,
        SUM(adjustment_amount) AS adjustments
    FROM fact_transaction
    GROUP BY post_year_month
)
SELECT
    a.snapshot_month,
    ROUND(LAG(a.closing_ar) OVER (ORDER BY a.snapshot_month), 2) AS opening_ar,
    ROUND(COALESCE(t.charges, 0), 2)                             AS charges,
    ROUND(COALESCE(t.payments, 0), 2)                            AS payments,
    ROUND(COALESCE(t.adjustments, 0), 2)                         AS adjustments,
    ROUND(a.closing_ar, 2)                                       AS closing_ar,
    ROUND(a.closing_ar
          - (COALESCE(LAG(a.closing_ar) OVER (ORDER BY a.snapshot_month), 0)
             + COALESCE(t.charges, 0)
             - COALESCE(t.payments, 0)
             - COALESCE(t.adjustments, 0)), 2)                   AS unexplained_variance
FROM monthly_ar a
LEFT JOIN monthly_activity t ON a.snapshot_month = t.snapshot_month
ORDER BY a.snapshot_month;
