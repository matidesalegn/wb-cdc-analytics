# ---------------------------------------------------------------------------
# wb-cdc-analytics
#
# One command for a reviewer:
#
#     make demo
#
# That runs preflight checks, generates secrets, brings the stack up, applies
# all DDL, registers the CDC connector, ingests from the public API, waits for
# the change events to land in ClickHouse, builds and tests the dbt models, and
# prints a per-stage row count. It is idempotent: running it twice is safe.
#
# Progressive disclosure for everything else:
#   make up        core path only            (Postgres, Redpanda, Connect, ClickHouse)
#   make up-mon    core path + Prometheus + Grafana
#   make up-all    everything incl. Airflow and Redpanda Console
#   make verify    prove data moved through every stage
#   make help      list every target
# ---------------------------------------------------------------------------

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

COMPOSE     := docker compose
CORE        := postgres redpanda connect clickhouse
ALL_PROFILES := --profile observability --profile orchestration --profile console

# Load .env for targets that need to talk to a container directly. Guarded so
# `make help` and `make env` still work before .env exists.
-include .env
export

.PHONY: help preflight env config build up up-mon up-all bootstrap ingest \
        dbt-deps dbt-build dbt-test verify demo demo-offline demo-mutations test lint fmt \
        render report-pdf urls logs ps drop-slot down clean

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# --- setup -----------------------------------------------------------------

preflight: ## Check Docker, Compose v2, available RAM and free host ports
	@bash scripts/preflight.sh

env: ## Generate .env from .env.example with fresh random secrets
	@python3 scripts/gen_env.py

config: ## Validate the merged Compose model without starting anything
	@$(COMPOSE) $(ALL_PROFILES) config > /dev/null && echo "compose config OK"

build: ## Build the local images (pipeline runner, Airflow)
	@$(COMPOSE) $(ALL_PROFILES) build

# --- run -------------------------------------------------------------------

up: env ## Start the core path and wait for every service to report healthy
	@$(COMPOSE) up -d --wait $(CORE)
	@echo "core path healthy"

up-mon: env ## Start the core path plus Prometheus and Grafana
	@$(COMPOSE) --profile observability up -d --wait
	@$(MAKE) --no-print-directory urls

up-all: env ## Start everything, including Airflow and Redpanda Console
	@$(COMPOSE) $(ALL_PROFILES) up -d --wait
	@$(MAKE) --no-print-directory urls

bootstrap: ## Apply all DDL and register the CDC connector (idempotent)
	@bash scripts/bootstrap.sh

ingest: ## Run one ingestion cycle: public REST API -> PostgreSQL
	@$(COMPOSE) run --rm pipeline python -m ingest.run

# --- transform -------------------------------------------------------------

dbt-deps: ## Install dbt package dependencies
	@$(COMPOSE) run --rm pipeline dbt deps --project-dir /app/dbt --profiles-dir /app/dbt

dbt-build: ## Run dbt models and their tests together (staging then mart)
	@$(COMPOSE) run --rm pipeline dbt build --project-dir /app/dbt --profiles-dir /app/dbt

dbt-test: ## Run only the dbt tests
	@$(COMPOSE) run --rm pipeline dbt test --project-dir /app/dbt --profiles-dir /app/dbt

# --- prove it works --------------------------------------------------------

verify: ## Print row counts and measured CDC lag for every stage
	@bash scripts/verify_stages.sh

demo: ## THE ONE COMMAND: clean start to verified analytics-ready tables
	@bash scripts/demo.sh

# Setting the variable inline on this command is deliberate and load-bearing. This
# Makefile does `-include .env; export`, and a make variable overrides the inherited
# environment, so `SOURCE_API_MODE=fixture make demo` is silently reset to .env's
# `live` and goes to the network regardless. An inline prefix on the recipe wins, and
# scripts/demo.sh carries it across its own sourcing of .env.
demo-offline: ## `make demo` with no network at all: replays the committed API fixtures
	@SOURCE_API_MODE=fixture bash scripts/demo.sh

demo-mutations: ## Prove an UPDATE propagates and a DELETE disappears downstream
	@bash scripts/demo_mutations.sh

# --- quality ---------------------------------------------------------------

test: ## Run the Python unit tests (no containers needed)
	@python3 -m pytest tests/unit -q

ci-local: ## Run the CI fast lane locally, with the same commands ci-cd.yml uses
	@bash scripts/ci/run_locally.sh

lint: ## Lint Python and check formatting
	@python3 -m ruff check . && python3 -m ruff format --check .

fmt: ## Format Python
	@python3 -m ruff format . && python3 -m ruff check --fix .

# --- docs ------------------------------------------------------------------

render: ## Render diagrams/src/*.mmd to SVG and PNG
	@bash scripts/render_diagrams.sh

report-pdf: ## Render docs/design-report.md to docs/design-report.pdf
	@python3 scripts/render_report_pdf.py

# --- operate ---------------------------------------------------------------

urls: ## Print every service URL and its credentials
	@bash scripts/urls.sh

logs: ## Follow logs for all running services
	@$(COMPOSE) $(ALL_PROFILES) logs -f --tail=100

ps: ## Show service status
	@$(COMPOSE) $(ALL_PROFILES) ps

# --- teardown --------------------------------------------------------------
#
# Read this before running `down`. A Debezium connector leaves a logical
# replication slot behind on the source database. An inactive slot keeps
# retaining WAL forever, and that is how a forgotten slot fills a production
# disk. `clean` removes the volumes so the slot goes with them; `down` keeps
# the volumes, so it drops the slot explicitly first.

drop-slot: ## Delete the CDC connector and drop its replication slot
	@bash scripts/drop_slot.sh

down: drop-slot ## Stop the stack, keeping data volumes (drops the slot first)
	@$(COMPOSE) $(ALL_PROFILES) down --remove-orphans
	@echo "stopped. volumes kept. run 'make clean' to remove them."

clean: ## Stop the stack and delete this project's volumes only
	@$(COMPOSE) $(ALL_PROFILES) down -v --remove-orphans
	@echo "stopped and volumes removed. 'make demo' starts from scratch."
