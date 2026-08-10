#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# THE one command. Clean clone to verified analytics-ready tables.
#
#     make demo
#
# Idempotent: running it twice is safe and fast. It does not wipe data, so a second
# run exercises the incremental and change-detection paths rather than repeating the
# first run.
#
# Ordering here is the whole point, because a cold start has real dependencies:
# Postgres must be healthy AND have a publication before the connector can register;
# ClickHouse DDL must exist before the Kafka engine consumes; and the dbt models
# cannot build before the CDC events have landed. Each step below waits for the thing
# it depends on rather than sleeping and hoping.
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/.."

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$1"; }
START=$(date +%s)

step "1/7  Preflight"
bash scripts/preflight.sh

step "2/7  Generating .env (skipped if it exists)"
python3 scripts/gen_env.py

step "3/7  Starting the core stack and waiting for every service to be healthy"
docker compose up -d --wait postgres redpanda connect clickhouse

step "4/7  Applying schema, ClickHouse DDL, and registering the CDC connector"
bash scripts/bootstrap.sh

step "5/7  Ingesting from the public REST API into PostgreSQL"
# The live API takes several minutes for 2,970 rows. SOURCE_API_MODE=fixture replays
# committed responses instead and finishes in about a second, which is what CI uses
# and what makes this runnable with no network.
docker compose run --rm pipeline python -m ingest.run

step "6/7  Waiting for the change events to reach ClickHouse, then building the marts"
# shellcheck disable=SC1091
set -a; . ./.env; set +a
expected=$(docker compose exec -T postgres psql -tA -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" \
  -c "SELECT count(*) FROM wb.observation" | tr -d '[:space:]')
printf '    PostgreSQL has %s observations, waiting for ClickHouse to match ' "$expected"
for _ in $(seq 1 60); do
  got=$(docker compose exec -T clickhouse clickhouse-client \
      --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" \
      -q "SELECT count() FROM raw.observation FINAL WHERE _is_deleted=0" 2>/dev/null | tr -d '[:space:]')
  if [ "${got:-0}" -ge "${expected:-1}" ]; then printf ' matched at %s\n' "$got"; break; fi
  printf '.'
  sleep 2
done

docker compose run --rm pipeline dbt build --project-dir /app/dbt --profiles-dir /app/dbt

step "7/7  Verifying every stage"
bash scripts/verify_stages.sh

printf '\n\033[1;32mDone in %s seconds.\033[0m\n' "$(( $(date +%s) - START ))"
printf 'Next: `make up-all` adds Airflow, Prometheus and Grafana. `make urls` lists every endpoint.\n'
printf '      `make demo-mutations` proves an UPDATE propagates and a DELETE disappears.\n\n'
