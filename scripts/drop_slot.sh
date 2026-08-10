#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Delete the CDC connector and drop its replication slot.
#
# This exists because of a failure mode worth taking seriously. A Debezium connector
# leaves a logical replication slot behind on the source database, and an INACTIVE
# slot keeps retaining WAL indefinitely so that a consumer which might come back can
# resume. If nothing comes back, the WAL is retained forever and the source's disk
# fills. The symptom is a database running out of space with no obvious cause, because
# nothing about the connector looks wrong: it is simply gone.
#
# `make down` therefore calls this first. `make clean` does not need it, because
# removing the volume takes the slot with it.
#
# Safe to run when nothing is up, and safe to run twice.
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || { echo "no .env, nothing to clean up"; exit 0; }
# shellcheck disable=SC1091
set -a; . ./.env; set +a

running() { docker compose ps --status running --services 2>/dev/null | grep -qx "$1"; }

if running connect; then
  code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE \
    "http://127.0.0.1:${CONNECT_HOST_PORT}/connectors/${CONNECT_CONNECTOR_NAME}" 2>/dev/null || echo 000)
  case "$code" in
    204|404) echo "  connector ${CONNECT_CONNECTOR_NAME} removed (HTTP ${code})" ;;
    *)       echo "  warning: could not delete the connector (HTTP ${code})" ;;
  esac
  # Give Debezium a moment to release the slot. Dropping an ACTIVE slot fails, and
  # the failure is the confusing kind: the slot is still there afterwards.
  sleep 3
fi

if running postgres; then
  docker compose exec -T postgres psql -q -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "
    DO \$\$
    DECLARE slot_active boolean;
    BEGIN
      SELECT active INTO slot_active FROM pg_replication_slots
       WHERE slot_name = '${PG_REPL_SLOT}';
      IF slot_active IS NULL THEN
        RAISE NOTICE 'slot ${PG_REPL_SLOT} does not exist, nothing to drop';
      ELSIF slot_active THEN
        RAISE WARNING 'slot ${PG_REPL_SLOT} is still active; a consumer is attached. Not dropping.';
      ELSE
        PERFORM pg_drop_replication_slot('${PG_REPL_SLOT}');
        RAISE NOTICE 'dropped replication slot ${PG_REPL_SLOT}';
      END IF;
    END
    \$\$;" 2>&1 | sed 's/^/  /'
fi
exit 0
