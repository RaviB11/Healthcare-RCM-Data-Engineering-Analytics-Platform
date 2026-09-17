# Revenue cycle in fifteen minutes

Context for anyone reading the code without a healthcare background. Everything
here is why the pipeline is shaped the way it is.

## The lifecycle

```
Patient registers -> Service delivered -> Coded -> Claim submitted -> Payer
adjudicates -> Remittance (835) -> Payment posted -> Patient billed for the
remainder -> Collected, appealed, or written off
```

Money moves for months after the date of service. That is why a claim's
financial state is the net of a transaction ledger, not a status column.

## Key entities

**Encounter** — a visit. Inpatient, outpatient, emergency, observation,
telehealth. One encounter usually produces one claim.

**Claim** — the bill sent to the payer. Carries one or more **claim lines**,
each a CPT procedure code with units and a charge.

**835 / ERA** — the payer's electronic remittance advice. Says what was allowed,
what was paid, what the patient owes, and why anything was reduced.

**CARC** — Claim Adjustment Reason Code, the "why" on an 835. `CO-197` is
missing prior authorisation. `PR-2` is patient coinsurance.

This distinction matters more than it looks: **`PR-*` codes are not denials.**
They are the patient's share of a correctly adjudicated claim. Counting them as
denials is the single most common way a revenue cycle dashboard reports a clean
claim rate roughly half its true value.

## The money

For one claim:

```
Gross charge            $1,000   what the hospital billed (largely fictional)
- Contractual adjustment  -$150   the discount the payer contract requires
= Allowed amount          $850   what the payer agreed the service is worth
- Insurance payment       -$680   what the payer actually sent
= Patient responsibility  $170   deductible + coinsurance
- Patient payment         -$170   if they pay
= Balance                    $0
```

Anything still outstanding is **accounts receivable (A/R)**. If it ages past
roughly 180 days it usually becomes bad debt and gets written off.

Sign convention in this codebase: charges positive, everything that reduces A/R
negative. `silver_transactions` enforces it as a blocking DQ rule, because a
payment posted with the wrong sign silently doubles revenue.

## The metrics

| Metric | Definition | Good |
|---|---|---|
| **Days in A/R** | Total A/R ÷ average daily charges | under 45 |
| **Clean claim rate** | Adjudicated with no denial CARC ÷ adjudicated | over 95% |
| **Denial rate** | Denied ÷ adjudicated | under 5% |
| **Net collection rate** | Payments ÷ (charges − contractual adjustments) | over 95% |
| **First pass resolution** | Closed to zero on first submission ÷ adjudicated | over 90% |
| **% A/R over 90 days** | Aged A/R ÷ total A/R | under 20% |
| **Charge lag** | Days from service to claim submission | under 5 |

Three traps these definitions avoid:

**Days in A/R needs a point-in-time balance.** Summing the residual balance of
claims whose *service date* fell in a month is not the same thing and will not
tie to the aged trial balance. This pipeline computes it from
`fact_ar_snapshot`.

**Net collection rate is measured against what was collectible**, i.e. after
contractual allowances but *before* bad debt write-offs. Subtracting write-offs
too makes a hospital that writes off aggressively look excellent.

**Rate denominators must exclude claims still in flight.** The current month is
always mid-adjudication.

## Why denials get their own dimension

Denials are where the recoverable money is, and they are not homogeneous:

- **Preventable** (missing authorisation, duplicate submission, timely filing) —
  fix the upstream process; appealing them is treating the symptom.
- **Appealable** (medical necessity, coordination of benefits) — work the queue.
- **Neither** (contractual, bundling) — normal adjudication, not a problem.

So the denial Pareto in `sql/analytics/rcm_kpis.sql` ranks by **recoverable
value**, not by count. The highest-volume denial reason is frequently one nobody
should be working.
