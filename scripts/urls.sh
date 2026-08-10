#!/usr/bin/env bash
# Print every service endpoint and its credentials, read from .env so the
# output is always true for this machine rather than a guess in a README.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo "no .env yet. run 'make env' first." >&2
  exit 1
fi

# shellcheck disable=SC1091
set -a; . ./.env; set +a

printf '\n%-22s %-34s %s\n' "SERVICE" "URL" "CREDENTIALS"
printf '%-22s %-34s %s\n' "----------------------" "----------------------------------" "-----------"
printf '%-22s %-34s %s\n' "PostgreSQL (OLTP)"  "127.0.0.1:${POSTGRES_HOST_PORT}/${POSTGRES_DB}"   "${POSTGRES_USER} / ${POSTGRES_PASSWORD}"
printf '%-22s %-34s %s\n' "ClickHouse (HTTP)"  "http://127.0.0.1:${CLICKHOUSE_HTTP_HOST_PORT}"    "${CLICKHOUSE_USER} / ${CLICKHOUSE_PASSWORD}"
printf '%-22s %-34s %s\n' "ClickHouse (native)" "127.0.0.1:${CLICKHOUSE_NATIVE_HOST_PORT}"        "${CLICKHOUSE_USER} / ${CLICKHOUSE_PASSWORD}"
printf '%-22s %-34s %s\n' "Kafka Connect REST" "http://127.0.0.1:${CONNECT_HOST_PORT}/connectors" "none"
printf '%-22s %-34s %s\n' "Redpanda admin"     "http://127.0.0.1:${REDPANDA_ADMIN_HOST_PORT}/public_metrics" "none"
printf '%-22s %-34s %s\n' "Airflow UI"         "http://127.0.0.1:${AIRFLOW_HOST_PORT}"            "${AIRFLOW_ADMIN_USER} / ${AIRFLOW_ADMIN_PASSWORD}"
printf '%-22s %-34s %s\n' "Prometheus"         "http://127.0.0.1:${PROMETHEUS_HOST_PORT}"         "none"
printf '%-22s %-34s %s\n' "Grafana"            "http://127.0.0.1:${GRAFANA_HOST_PORT}"            "${GRAFANA_ADMIN_USER} / ${GRAFANA_ADMIN_PASSWORD}"
printf '%-22s %-34s %s\n' "Redpanda Console"   "http://127.0.0.1:${REDPANDA_CONSOLE_HOST_PORT}"   "none"
printf '\n'
echo "Airflow, Prometheus, Grafana and Console only run under their compose"
echo "profiles. 'make up-all' starts everything."
printf '\n'
