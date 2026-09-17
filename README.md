# Healthcare RCM Data Engineering & Analytics Platform

An end-to-end data platform for hospital **Revenue Cycle Management**: it ingests
claims, encounters, remittance advice and patient data from multiple source
systems, conforms and validates it through a medallion architecture, models it as
a Kimball star schema, and publishes the KPIs a revenue cycle director actually
manages against — Days in A/R, denial rate, clean claim rate, net collection rate.

**Stack:** Python · PySpark · SQL · AWS (S3, Glue, Step Functions, Redshift,
Lambda) · Terraform · Airflow · Power BI

The whole pipeline runs locally with no AWS account:

```bash
make setup && make pipeline
```

---

## Why revenue cycle management

A US hospital bills a payer, the payer pays part of it months later or denies it
outright, and the difference between a healthy and a failing hospital is largely
how well it manages that gap. The data problems are genuinely hard and genuinely
unglamorous:

- The same patient exists in two EMRs under different identifiers, different name
  formats and different date conventions, because the health system merged.
- A claim's financial state is not a column. It is the net of a transaction
  ledger — charges, contractual adjustments, insurance payments, patient
  payments, write-offs — that keeps moving for a year after the date of service.
- Every metric has four plausible definitions, and finance will only trust the
  dashboard if it ties to the aged trial balance to the dollar.

That last point drove most of the engineering decisions below.

---

## Architecture

```
  SOURCE SYSTEMS                 LAKE (S3)                   WAREHOUSE        BI
 ┌───────────────┐      ┌────────────────────────┐        ┌──────────┐   ┌────────┐
 │ EMR Hosp A    │      │ landing/               │        │ Redshift │   │ Power  │
 │  CamelCase    ├─────►│   raw extracts         │        │          │   │  BI    │
 │  ISO dates    │      │                        │        │  gold.*  │   │        │
 ├───────────────┤      │ bronze/  ◄─ immutable, │        │  marts.* │   │ 5 pages│
 │ EMR Hosp B    ├─────►│          audited,      │        │          │   │        │
 │  snake_case   │      │          all strings   │        └────▲─────┘   └───▲────┘
 │  US dates     │      │                        │             │             │
 ├───────────────┤      │ silver/  ◄─ conformed, │             │             │
 │ Billing       ├─────►│          typed, SCD2,  │─────────────┘             │
 │  claims/txns  │      │          DQ-validated  │   COPY + swap             │
 ├───────────────┤      │                        │                          │
 │ 835 Remittance├─────►│ gold/    ◄─ star schema│──────────────────────────┘
 │  CARC codes   │      │          + KPI marts   │
 ├───────────────┤      │                        │
 │ Reference     ├─────►│ quarantine/ ◄─ rejects │
 │  CPT/ICD/NPI  │      │ metrics/    ◄─ DQ telemetry
 └───────────────┘      └────────────────────────┘

  Orchestration: Step Functions (serverless) or Airflow/MWAA — both included
```

### Medallion layers

| Layer | Responsibility | Key rule |
|---|---|---|
| **Landing** | Raw source drops | 90-day expiry; it's a replay buffer, not an archive |
| **Bronze** | Immutable audited copy | Everything read as string. A bad date must never fail ingestion at 3am |
| **Silver** | Conform, type, validate, historise | Bad rows are quarantined with the rules they broke, never dropped |
| **Gold** | Star schema + KPI marts | Grain is asserted, not assumed |

---

## What actually runs

Verified end to end on the committed configuration:

| Stage | Output |
|---|---|
| Generator | 15 files, 12.3 MB across 5 simulated source systems |
| Bronze | 156,326 rows ingested, 15 sources, config-driven |
| Silver | 32 DQ rules evaluated, 227 rows quarantined, SCD2 history built |
| Gold | 7 dimensions, 4 facts, 6 KPI marts |
| `fact_claim` | 18,381 claims |
| `fact_ar_snapshot` | 60,927 claim-month rows, 24 months |
| Tests | 37 passing |
| Analytics SQL | 8 queries executed against the Gold layer |

Sample output is committed under `powerbi/sample_output/` so the results are
readable without running anything.

### The numbers it produces

The synthetic health system is deliberately an underperformer, so the dashboard
has something to say:

| Metric | This dataset | Better-performer benchmark |
|---|---|---|
| Days in A/R | 75–80 | under 45 |
| Clean claim rate | 88.1% | over 95% |
| Denial rate | 11.2% | under 5% |
| Net collection rate | 80.7% overall, ~88% on mature months | over 95% |
| % A/R over 90 days | 39.8% | under 20% |

---

## Engineering decisions worth explaining

### Grain is asserted, not assumed

Silent join fan-out is the most expensive bug in a warehouse: nothing errors, the
pipeline goes green, and every revenue figure is quietly inflated. Every fact
build calls `assert_unique_grain()` before writing, which turns fan-out into a
build failure.

This caught a real bug. A claim whose service date fell on the same calendar day
that the patient's SCD2 record changed matched **both** versions of that patient,
duplicating the claim. The cause was casting the Type 2 validity window down to a
date in the point-in-time join; the fix compares at timestamp precision.
`tests/test_transformations.py::test_pit_join_does_not_fan_out_on_same_day_version_change`
pins it.

### The A/R roll-forward is a control, not a KPI

`sql/analytics/rcm_kpis.sql` Q8 asserts that opening A/R + charges − payments −
adjustments = closing A/R, every month. It currently reconciles to the penny
across all 24 months.

It did not at first. It surfaced that denied claims were receiving a contractual
adjustment **and** a write-off for the full gross charge, pushing 1,018 claims
into a phantom credit balance and breaking the ledger by $1.12M. Write-offs now
clear only the remaining balance. This is exactly what a reconciliation control
is for: it found a bug that every individual KPI looked fine alongside.

### Credit balances are kept, not filtered

The obvious `WHERE ar_balance > 0` hides overpayments. Those are a reportable
liability that hospitals are required to identify and refund, and dropping them
silently breaks the roll-forward. They are retained with an `is_credit_balance`
flag and a `Credit` ageing bucket.

### Metric definitions live in one place

"Denial rate" means four different things to four different people. The argument
happens once, in `config/pipeline_config.yaml` and the comments in
`build_gold.py`, and every consumer inherits it.

Two definitional choices that materially change the reported numbers:

- **PR-1, PR-2 and CO-45 are not denials.** Deductible, coinsurance and
  contractual write-downs are normal adjudication. Counting them as denials was
  reporting a 51% clean claim rate against a true 88%.
- **Rate denominators are adjudicated claims**, not all submitted claims. The
  current month is always mid-adjudication; dividing by everything submitted
  makes every month look like a collapse.

### SCD Type 2 is hand-rolled

No Delta or Iceberg MERGE. The same code then runs on plain Parquet in Glue, EMR
and a laptop with no table-format jars, and the history semantics are explicit
and testable rather than hidden inside a MERGE statement. Change detection uses a
SHA-256 hash of the tracked columns, so adding a tracked attribute is a one-line
change. Ten unit tests cover idempotent re-runs, late-arriving records, null
transitions and boundary conditions.

### Quality rules are data, not code

`config/dq_rules.yaml` holds 32 expectations across 6 datasets in 7 rule types.
An analyst can add an expectation without touching PySpark. Rules are `blocking`
(row is quarantined) or `warning` (row passes, violation counted). Quarantined
rows keep a `_dq_violations` array naming every rule they broke, so the
stewardship team can work the exceptions instead of guessing.

### Configuration, not branching

The same job code runs local, dev and prod. `RCM_ENV` selects a config block;
nothing in `src/` knows which environment it is in. Adding a source file is a
YAML edit, not a code change.

---

## Repository layout

```
├── config/
│   ├── pipeline_config.yaml     environments, source registry, business rules
│   └── dq_rules.yaml            32 declarative quality expectations
├── src/
│   ├── generators/              synthetic multi-source RCM data with defects
│   ├── common/                  Spark session, config, grain assertion, SQL runner
│   ├── bronze/                  config-driven ingestion
│   ├── silver/                  conformance + scd2.py
│   ├── gold/                    star schema, KPI marts, Power BI export
│   └── quality/                 declarative DQ engine
├── sql/
│   ├── ddl/                     Redshift schemas, dimensions, facts
│   ├── etl/                     COPY-and-swap load, mart rebuild
│   └── analytics/               8 RCM business queries
├── aws/
│   ├── glue/                    job entrypoint
│   ├── lambda/                  S3 arrival trigger
│   ├── stepfunctions/           pipeline state machine
│   └── terraform/               S3, KMS, Glue, Step Functions, DynamoDB
├── airflow/dags/                MWAA alternative to Step Functions
├── powerbi/                     DAX measures, model + report spec
│   └── sample_output/           committed KPI marts - real output, no run needed
├── tests/                       37 tests including bug regressions
└── docs/                        architecture, data dictionary, RCM primer
```

---

## Running it

```bash
make setup            # install dependencies
make pipeline         # generate data, then bronze -> silver -> gold
make test             # 37 unit tests
make sql-check        # execute the analytics SQL against the gold layer
make verify           # both
make powerbi-export   # CSV extracts for Power BI Desktop
```

Requires Python 3.11+ and a JVM (Spark). `SCALE=0.2 make pipeline` runs a
smaller dataset on a constrained machine.

To deploy on AWS: `cd aws/terraform && terraform apply -var-file=envs/dev.tfvars`,
then set `storage_root` in the config to the `storage_root` Terraform output.

---

## Notes and limitations

- The data is synthetic. CPT, ICD-10 and CARC codes are real published code sets;
  the patients, providers and claims are generated. No PHI is involved, which is
  the only responsible way to put a healthcare project on GitHub.
- The `.pbix` is not committed. Binaries don't diff and would bundle a copy of
  the data; `powerbi/README.md` and `measures.dax` are enough to rebuild it.
- Terraform is written and validated but not applied against a live account, so
  the ARNs are placeholders.
- The generator's denial and payment behaviour is a plausible simulation, not a
  calibrated model of any real payer mix.
