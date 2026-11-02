# Default demo dates that have captured fixtures (real API responses).
DEMO_DATES ?= 2026-10-15 2026-10-20

.PHONY: setup demo ingest refresh-fixtures test check fmt

## Create the local virtualenv (uv) with requests + duckdb.
setup:
	uv sync --quiet

## Run the whole pipeline offline: fixtures -> raw lake -> bronze/silver/gold.
demo:
	rm -rf data
	@for d in $(DEMO_DATES); do \
		uv run --quiet python scripts/ingest.py --date $$d --from-fixtures fixtures/raw; \
	done
	uv run --quiet python scripts/build_model.py

## Pull live APIs for a given date into data/raw (default: today, Asia/Kolkata).
##   make ingest DATE=2026-10-15
ingest:
	uv run --quiet python scripts/ingest.py --date $(or $(DATE),$$(date +%Y-%m-%d)) --out data/raw

## Re-capture the demo fixtures from the live APIs (needs network).
refresh-fixtures:
	rm -rf fixtures/raw
	@for d in $(DEMO_DATES); do \
		uv run --quiet python scripts/ingest.py --date $$d --out fixtures/raw; \
	done

## Cheap checks: no Databricks workspace needed.
test:
	uv run --quiet pytest

fmt:
	uv run --quiet ruff check --fix . && uv run --quiet ruff format .

## Full Databricks-side validation (needs `databricks auth login` first).
check:
	databricks bundle validate
	uv run --quiet pytest
