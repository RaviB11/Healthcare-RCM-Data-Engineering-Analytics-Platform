# Power BI: model and report specification

The `.pbix` binary is not committed (it does not diff, and it would bundle a
copy of the data). Everything needed to rebuild the report is here: the model
structure, the relationships, the measure library in `measures.dax`, and the
page-by-page layout.

## Connecting

**Local / demo.** Point Power BI Desktop at `powerbi/data/*.csv`, produced by:

```bash
make powerbi-export
```

**Deployed.** Connect to Redshift instead. Storage mode per table:

| Table | Mode | Why |
|---|---|---|
| `gold.fact_claim` | Import | Fits comfortably in memory, needs fast slicing |
| `gold.fact_ar_snapshot` | DirectQuery | Grows monthly and finance wants it current |
| `gold.dim_*` | Dual | Lets the engine keep joins local in either mode |
| `marts.kpi_*` | Import | Already aggregated, refreshed with the pipeline |

Set refresh to run after the Step Functions pipeline completes, not on a fixed
clock, or the report will occasionally show a half-loaded warehouse.

## Model

Star schema, single direction filters flowing from dimensions to facts.

```
                      DimDate
                         |
   DimPayer ---------\   |   /--------- DimProvider
                      \  |  /
   DimPatient ---------> FactClaim <--------- DimDenialReason
                      /     \
   DimProcedure -----/       \------ FactARSnapshot
   DimDiagnosis                      (via DimPayer, DimDate)
```

### Relationships

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `DimDate[date_key]` | `FactClaim[service_date_key]` | 1:* | Single | Yes |
| `DimDate[date_key]` | `FactClaim[remit_date_key]` | 1:* | Single | No (inactive) |
| `DimDate[date_key]` | `FactARSnapshot[snapshot_date_key]` | 1:* | Single | Yes |
| `DimPayer[payer_sk]` | `FactClaim[payer_sk]` | 1:* | Single | Yes |
| `DimPayer[payer_sk]` | `FactARSnapshot[payer_sk]` | 1:* | Single | Yes |
| `DimProvider[provider_sk]` | `FactClaim[provider_sk]` | 1:* | Single | Yes |
| `DimPatient[patient_sk]` | `FactClaim[patient_sk]` | 1:* | Single | Yes |
| `DimDenialReason[denial_sk]` | `FactClaim[denial_sk]` | 1:* | Single | Yes |
| `DimProcedure[procedure_sk]` | `FactClaimLine[procedure_sk]` | 1:* | Single | Yes |

Two things worth knowing about this model:

**The remit-date relationship is inactive on purpose.** A fact with two dates
needs role-playing dates. Service date is the active one because that is how
volume is reported; cash questions activate the other with `USERELATIONSHIP`.
Two active date relationships is not an option, and duplicating DimDate makes
the field list twice as confusing.

**`DimDate` is marked as a date table** (`full_date` as the date column).
Without that, `TOTALYTD` and `DATESINPERIOD` silently return wrong results
rather than failing, which is the worst kind of bug to inherit.

## Report pages

### 1. Executive Scorecard
Six KPI cards across the top: Days in A/R, Net Collection Rate, Clean Claim
Rate, Denial Rate, Total A/R, % A/R over 90. Each card uses the matching
`* Indicator` measure for conditional formatting against benchmark, so a
number that is merely large does not look like a success.

Below: a combo chart of gross charges (columns) against net collection rate
(line) by month, and the A/R ageing waterfall.

Benchmarks to annotate on the visuals:

| Metric | Better performer | Needs work |
|---|---|---|
| Days in A/R | under 45 | over 60 |
| Clean Claim Rate | over 95% | under 90% |
| Denial Rate | under 5% | over 10% |
| Net Collection Rate | over 95% | under 93% |
| % A/R over 90 days | under 20% | over 25% |

### 2. A/R Ageing
Matrix of payer against ageing bucket with `ar_balance`, conditionally
formatted as a heat map. Month slicer bound to `snapshot_date`. This page has
to reconcile exactly to the aged trial balance; if it does not, nothing else
on the report will be believed.

### 3. Denial Management
Pareto chart of denial reason by **recoverable value**, not by count. A
high-volume denial that cannot be appealed is a process fix upstream, not a
work queue, and sorting by count sends the follow-up team after the wrong
claims. Drill through from any CARC code to the claim list.

### 4. Payer Performance
Scatter of net collection rate against average days to payment, bubble sized
by gross charges. The bottom-right quadrant is the contract renegotiation
list: slow and underpaying.

### 5. Provider Detail
Table of provider, claim volume, denial rate, charge lag, open A/R, with the
z-score outlier flag from `sql/analytics/rcm_kpis.sql` Q7 so the conversation
starts from a statistical threshold rather than someone's impression.

## Row-level security

One role, `PayerAnalyst`, filtering `DimPayer` by the user's mapped payer
group, so a payer-facing analyst sees only their own contract performance:

```dax
[payer_group] = LOOKUPVALUE (
    UserPayerMap[payer_group],
    UserPayerMap[user_email], USERPRINCIPALNAME ()
)
```

RLS on the dimension propagates to both fact tables through the existing
relationships. Applying it directly to the facts instead would need to be
repeated per table and would be silently bypassed by the pre-aggregated
`kpi_*` marts, which is the usual way RLS leaks.
