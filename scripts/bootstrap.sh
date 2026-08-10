#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Take the running stack from "containers are up" to "the pipeline is wired".
#
# This is the authoritative schema and connector applier, not
# docker-entrypoint-initdb.d. initdb runs exactly once, on an empty data
# directory, and it is not transactional across files: if a later script fails
# after an earlier one has committed, the volume is left half-applied and every
# subsequent start SKIPS initdb, so the tables exist while the publication
# silently does not. That failure is quiet and confusing, so initdb is treated
# here as a cold-start optimisation and this script as the source of truth.
#
# Every step is idempotent. Running it twice, or against an already-wired stack,
# is a no-op.
# ---------------------------------------------------------------------------
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo "no .env. run 'make env' first." >&2
  exit 1
fi
# shellcheck disable=SC1091
set -a; . ./.env; set +a

COMPOSE="docker compose"
CONNECT_URL="http://127.0.0.1:${CONNECT_HOST_PORT}"

step() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }
info() { printf '    %s\n' "$1"; }
die()  { printf '    \033[31mERROR\033[0m %s\n' "$1" >&2; exit 1; }

# Probe a service by doing the thing we actually need it to do, rather than by
# asking Docker what it thinks. `make up --wait` already gates on the
# healthchecks; this covers the case where bootstrap is called directly, and it
# fails with a message that names the service.
probe() {
  local name="$1" tries="$2"; shift 2
  printf '    %-11s' "$name"
  for _ in $(seq 1 "$tries"); do
    if "$@" > /dev/null 2>&1; then printf ' up\n'; return 0; fi
    printf '.'
    sleep 3
  done
  printf ' UNREACHABLE\n'
  die "$name did not become reachable. Try: docker compose logs $name"
}

step "Checking services"
probe postgres   20 $COMPOSE exec -T postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
probe clickhouse 20 $COMPOSE exec -T clickhouse wget -qO- http://localhost:8123/ping
probe redpanda   20 $COMPOSE exec -T redpanda rpk cluster health
# Connect boots a JVM and scans plugins, so it is materially slower than the rest.
probe connect    40 curl -sf "${CONNECT_URL}/connectors"

# ---------------------------------------------------------------------------
# PostgreSQL: schema, replication role, grants, publication.
# ---------------------------------------------------------------------------
step "Applying PostgreSQL schema"
$COMPOSE exec -T postgres psql -v ON_ERROR_STOP=1 -q \
  -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/oltp/001_schema.sql
info "tables and constraints applied"

step "Applying PostgreSQL CDC setup"
# The same script the initdb path runs, executed inside the container so psql and
# the server versions match, with credentials passed through the environment
# rather than written to disk or into a log.
$COMPOSE exec -T \
  -e PG_REPL_USER="$PG_REPL_USER" \
  -e PG_REPL_PASSWORD="$PG_REPL_PASSWORD" \
  -e PG_PUBLICATION="$PG_PUBLICATION" \
  -e POSTGRES_USER="$POSTGRES_USER" \
  -e POSTGRES_DB="$POSTGRES_DB" \
  postgres bash /docker-entrypoint-initdb.d/002_cdc_setup.sh

# Assert, do not trust. A publication that exists but captures the wrong tables
# yields an empty topic, which presents as a broken connector and sends you
# debugging the wrong component.
step "Verifying the publication captures the expected tables"
$COMPOSE exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -c \
  "SELECT '    captured: '||schemaname||'.'||tablename
   FROM pg_publication_tables WHERE pubname = '${PG_PUBLICATION}' ORDER BY 1"
captured=$($COMPOSE exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -c \
  "SELECT count(*) FROM pg_publication_tables WHERE pubname = '${PG_PUBLICATION}'" \
  | tr -d '[:space:]')
[ "$captured" = "4" ] || die "expected 4 captured tables in ${PG_PUBLICATION}, found ${captured}"

# ---------------------------------------------------------------------------
# ClickHouse: databases, landing tables, Kafka engine tables, materialized views.
# Applied in filename order; every file is written to be re-runnable.
# ---------------------------------------------------------------------------
step "Applying ClickHouse DDL"
shopt -s nullglob
ch_files=(sql/clickhouse/*.sql)
shopt -u nullglob
if [ ${#ch_files[@]} -eq 0 ]; then
  info "no ClickHouse DDL present yet, skipping"
else
  for f in "${ch_files[@]}"; do
    info "$(basename "$f")"
    $COMPOSE exec -T clickhouse clickhouse-client \
      --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
      --multiquery < "$f"
  done
fi

# ---------------------------------------------------------------------------
# Register the CDC connector.
#
# PUT /connectors/<name>/config, never POST /connectors. PUT is create-or-update
# and therefore idempotent; POST returns 409 Conflict on the second call, which
# turns a retry or a second bootstrap into a spurious failure.
# ---------------------------------------------------------------------------
step "Registering the Debezium connector"
rendered=$(mktemp)
trap 'rm -f "$rendered"' EXIT
python3 scripts/render_connector.py cdc/connectors/postgres-source.json > "$rendered"

registered=false
for attempt in $(seq 1 12); do
  code=$(curl -s -o /tmp/wb-connect-response -w '%{http_code}' \
    -X PUT -H 'Content-Type: application/json' --data @"$rendered" \
    "${CONNECT_URL}/connectors/${CONNECT_CONNECTOR_NAME}/config" || echo 000)
  if [ "$code" = "200" ] || [ "$code" = "201" ]; then
    info "registered ${CONNECT_CONNECTOR_NAME} (HTTP ${code})"
    registered=true
    break
  fi
  info "attempt ${attempt}: HTTP ${code}, retrying in 5s"
  [ -s /tmp/wb-connect-response ] && head -c 400 /tmp/wb-connect-response | sed 's/^/      /'
  sleep 5
done
[ "$registered" = true ] || die "connector registration failed after 12 attempts"

step "Waiting for the connector and its tasks to reach RUNNING"
python3 scripts/wait_for_connector.py \
  --url "$CONNECT_URL" --connector "$CONNECT_CONNECTOR_NAME" --timeout 150 \
  || die "connector did not start cleanly"

# ---------------------------------------------------------------------------
# Surface the replication slot. This is the object that retains WAL, so naming it
# at the end of bootstrap makes it visible rather than something a reviewer
# discovers when a disk fills.
# ---------------------------------------------------------------------------
step "Replication slot"
$COMPOSE exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -c \
  "SELECT format('    slot=%s active=%s wal_status=%s retained=%s',
                 slot_name, active, coalesce(wal_status,'n/a'),
                 pg_size_pretty(coalesce(pg_current_wal_lsn() - restart_lsn, 0)))
   FROM pg_replication_slots WHERE slot_name = '${PG_REPL_SLOT}'"

printf '\n\033[1mbootstrap complete.\033[0m Next: make ingest, then make verify\n\n'
