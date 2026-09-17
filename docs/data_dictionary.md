# Data dictionary

Grain, keys and the columns that are easy to misread. Generated tables are
described as they exist after `make pipeline`.

## Conventions

- `*_sk` — surrogate key, deterministic 64-bit hash of the natural key
- `_*` — audit column (`_batch_id`, `_source_system`, `_ingested_at`, `_layer`)
- All monetary columns are `decimal(18,2)`
- `-1` in any `*_sk` column is the unknown member, so facts never lose rows to
  an inner join

## Dimensions

### `dim_patient` — Type 2, grain: one row per patient **version**

| Column | Notes |
|---|---|
| `patient_sk` | hash of (`patient_id`, `version_number`) |
| `patient_id` | natural key; **repeats across versions by design** |
| `effective_from` / `effective_to` | validity window; open rows end `9999-12-31 23:59:59` |
| `is_current` | exactly one true row per `patient_id` |
| `record_hash` | SHA-256 of tracked attributes; drives change detection |
| `age_band` | 0-17, 18-34, 35-49, 50-64, 65-79, 80+ |

Join to facts **point in time**, comparing at timestamp precision. Casting the
window to a date fans out any claim dated on a change day.

### `dim_payer` — grain: one payer
`payer_group` collapses `payer_type` into Government / Commercial / Workers Comp
/ Self Pay for reporting. `contracted_rate` is the expected allowed-to-charge
ratio.

### `dim_denial_reason` — grain: one CARC code

| Column | Notes |
|---|---|
| `is_appealable` | only these represent recoverable cash |
| `is_preventable` | technical, authorisation, duplicate, timely filing |
| `is_patient_responsibility` | `PR-*` codes — **not denials** |

### `dim_provider`, `dim_procedure`, `dim_diagnosis`, `dim_date`
Type 1, one row per provider / CPT / ICD-10 / calendar day. `dim_date` carries a
fiscal year starting 1 October, the common US healthcare convention.

## Facts

### `fact_claim` — grain: one row per claim

Asserted unique on `claim_id` at build time.

| Column | Notes |
|---|---|
| `total_charge_amount` | gross billed |
| `allowed_amount` | what the payer agreed to |
| `contractual_adjustment_amount` | `charge − allowed` |
| `total_payment_amount` | insurance + patient, positive |
| `outstanding_balance` | `charge − payments − adjustments` |
| `underpayment_amount` | `max(allowed − paid, 0)`; contract compliance exposure |
| `charge_lag_days` | service to submission |
| `ar_age_days` | days since submission, only when balance > 0 |
| `is_adjudicated` | a remittance has been received |
| `is_denial_carc` | the CARC is a genuine denial, excluding `PR-*` and `CO-45` |
| `is_clean_claim` | adjudicated with no denial CARC |
| `is_first_pass_resolved` | clean **and** closed to zero balance |

Partitioned by `service_year_month`.

### `fact_ar_snapshot` — grain: one row per claim per month-end

Asserted unique on (`claim_id`, `snapshot_date`). Recomputed from the ledger
each run rather than carried forward, so a late-posted payment retroactively
corrects history.

| Column | Notes |
|---|---|
| `ar_balance` | may be **negative** — a credit balance |
| `is_credit_balance` | overpayment; a reportable liability, kept not filtered |
| `ar_aging_bucket` | 0-30, 31-60, 61-90, 91-120, 121-180, 180+, Credit |
| `is_over_90` | false for credit balances |

This table is the reconciliation anchor. `sql/analytics/rcm_kpis.sql` Q8 asserts
it ties to `fact_transaction` every month.

### `fact_transaction` — grain: one financial transaction

Signed: `CHARGE` positive; `INSURANCE_PAYMENT`, `PATIENT_PAYMENT`,
`CONTRACTUAL_ADJUSTMENT`, `WRITEOFF` negative. The pre-split
`charge_amount` / `payment_amount` / `adjustment_amount` columns exist so
aggregates never need a `CASE` in the BI layer.

### `fact_claim_line` — grain: one claim line (CPT level)
`charge_variance_from_standard` compares the billed unit charge to the
chargemaster rate, which surfaces pricing inconsistency across departments.

## Marts

| Table | Grain |
|---|---|
| `kpi_monthly_scorecard` | service month |
| `kpi_payer_performance` | payer |
| `kpi_denial_analysis` | CARC × payer |
| `kpi_ar_aging` | snapshot month × bucket × payer |
| `kpi_provider_performance` | provider |
| `kpi_service_line` | service category × CPT |

## Quarantine

`quarantine/<dataset>/batch_id=<id>/` holds rows that failed a blocking rule,
with `_dq_violations` (array of rule names) and `_quarantined_at`. Retained 180
days.
