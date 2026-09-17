# Sample output

Committed so the repository shows real results without requiring a Spark run.
These are the small KPI marts produced by `make pipeline && make powerbi-export`.

| File | Grain | Rows |
|---|---|---|
| `kpi_monthly_scorecard.csv` | service month | 24 |
| `kpi_payer_performance.csv` | payer | 10 |
| `kpi_denial_analysis.csv` | CARC code x payer | 96 |
| `kpi_provider_performance.csv` | provider | 180 |
| `kpi_service_line.csv` | service category x CPT | 20 |

The two large fact extracts (`fact_claim`, `fact_ar_snapshot`, ~18 MB combined)
are deliberately not committed. Regenerate the full set with:

```bash
make pipeline
make powerbi-export
```
