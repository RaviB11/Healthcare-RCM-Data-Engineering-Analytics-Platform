# ===========================================================================
# Healthcare RCM Platform
#
#   make setup      install dependencies
#   make pipeline   generate data and run the full medallion pipeline
#   make test       unit tests
#   make verify     tests + SQL execution + reconciliation checks
# ===========================================================================

PYTHON  ?= python3
SCALE   ?= 1.0
export RCM_ENV ?= local

.DEFAULT_GOAL := help
.PHONY: help setup generate bronze silver gold pipeline powerbi-export \
        test sql-check verify lint clean clean-data tree

help:  ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup:  ## install python dependencies
	$(PYTHON) -m pip install -r requirements.txt

generate:  ## generate synthetic source data (SCALE=1.0)
	$(PYTHON) -m src.generators.generate_synthetic_data --out data/landing --scale $(SCALE)

bronze:  ## landing -> bronze
	$(PYTHON) -m src.bronze.ingest_to_bronze

silver:  ## bronze -> silver (conform, validate, SCD2)
	$(PYTHON) -m src.silver.build_silver

gold:  ## silver -> gold (star schema + KPI marts)
	$(PYTHON) -m src.gold.build_gold

pipeline: generate bronze silver gold  ## run everything end to end
	@echo ""
	@echo "Pipeline complete. Explore with:  make sql-check"

powerbi-export:  ## export gold tables as CSV for Power BI Desktop
	$(PYTHON) -m src.gold.export_for_powerbi --out powerbi/data

test:  ## run unit tests
	$(PYTHON) -m pytest tests/ -q

sql-check:  ## execute the analytics SQL against the gold layer
	$(PYTHON) -m src.common.run_sql --check

verify: test sql-check  ## tests plus SQL execution
	@echo ""
	@echo "All checks passed."

lint:  ## style and static checks
	$(PYTHON) -m ruff check src/ tests/ || true
	$(PYTHON) -m black --check src/ tests/ || true

clean-data:  ## delete every generated artefact (keeps code)
	rm -rf data/landing data/bronze data/silver data/gold \
	       data/quarantine data/metrics data/warehouse powerbi/data

clean: clean-data  ## clean data and python caches
	find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -prune -exec rm -rf {} + 2>/dev/null || true
	rm -rf .ruff_cache

tree:  ## show the repository layout
	@find . -type f -not -path './.git/*' -not -path './data/*' \
	   -not -path '*/__pycache__/*' -not -path './powerbi/data/*' | sort
