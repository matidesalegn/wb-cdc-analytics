#!/usr/bin/env bash
# Assert the committed Debezium connector config renders to something Kafka Connect accepts.
#
# The committed JSON is deliberately not a valid Connect payload: it carries "//" keys that
# document each decision, and ${VAR} placeholders so no credential is committed. This proves
# the renderer strips and substitutes correctly. The unresolved-placeholder check matters most:
# a connector registered with a literal "${PG_REPL_PASSWORD}" fails later, during the
# connection attempt, with an authentication error that never mentions the real cause.
set -euo pipefail
cd "$(dirname "$0")/../.."
# shellcheck disable=SC1091
set -a; . ./.env; set +a
python3 scripts/render_connector.py cdc/connectors/postgres-source.json \
  | python3 scripts/ci/assert_connector_payload.py
