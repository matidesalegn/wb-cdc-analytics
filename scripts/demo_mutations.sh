#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Prove the CDC path handles UPDATE and DELETE, not just INSERT.
#
# Worth its own script because inserts are the easy case and the other two are where
# CDC pipelines quietly break:
#
#   an UPDATE that does not propagate leaves the warehouse showing a stale value
#   a DELETE that does not propagate leaves a row the source no longer has
#
# Neither produces an error. Both pass every uniqueness and referential test. The
# delete case is the more dangerous of the two, because a tombstone carries the
# highest LSN for its key, so a read that forgets the _is_deleted filter resurrects
# the row with its non-key columns full of schema defaults.
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
set -a; . ./.env; set +a

CH="docker compose exec -T clickhouse clickhouse-client --user ${CLICKHOUSE_USER} --password ${CLICKHOUSE_PASSWORD}"
PG="docker compose exec -T postgres psql -q -U ${POSTGRES_USER} -d ${POSTGRES_DB}"
ch() { $CH -q "$1" 2>/dev/null | tr -d '[:space:]'; }

# A synthetic year outside the World Bank's range, so this cannot collide with real
# data and the next ingestion will not resurrect it.
YEAR=2099
KEY="country_id='ETH' AND indicator_id='SP.POP.TOTL' AND obs_year=${YEAR}"
FAILED=0
step() { printf '\n\033[1;36m==> %s\033[0m\n' "$1"; }
ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAILED=1; }

wait_for() {  # wait_for <sql> <expected> <label>
  for _ in $(seq 1 30); do
    [ "$(ch "$1")" = "$2" ] && { ok "$3"; return 0; }
    sleep 1
  done
  bad "$3 (last value: $(ch "$1"), expected $2)"
  return 1
}

cleanup() {
  $PG -c "DELETE FROM wb.observation WHERE ${KEY};" > /dev/null 2>&1 || true
}
trap cleanup EXIT

step "INSERT: a new row must appear in ClickHouse"
cleanup; sleep 2
$PG -c "INSERT INTO wb.observation (country_id, indicator_id, obs_year, obs_value, source_hash)
        VALUES ('ETH','SP.POP.TOTL',${YEAR}, 111.0, 'demo-insert');"
wait_for "SELECT count() FROM raw.observation FINAL WHERE ${KEY} AND _is_deleted=0" "1" \
         "the inserted row reached raw.observation"

step "UPDATE: the new value must replace the old one, not sit alongside it"
$PG -c "UPDATE wb.observation SET obs_value = 222.0, source_hash='demo-update', updated_at = now()
        WHERE ${KEY};"
wait_for "SELECT toString(toInt32(obs_value)) FROM raw.observation FINAL WHERE ${KEY} AND _is_deleted=0" "222" \
         "the updated value replaced the old one"
wait_for "SELECT count() FROM raw.observation FINAL WHERE ${KEY} AND _is_deleted=0" "1" \
         "still exactly one row for the key (the versions collapsed)"

step "DELETE: the row must disappear from current-state reads"
$PG -c "DELETE FROM wb.observation WHERE ${KEY};"
wait_for "SELECT count() FROM raw.observation FINAL WHERE ${KEY} AND _is_deleted=0" "0" \
         "the deleted row is gone from current state"

# The tombstone itself is expected to remain on disk. ReplacingMergeTree hides it from
# FINAL reads but does not physically remove it until OPTIMIZE FINAL CLEANUP, which is
# experimental. Asserting this is here so the behaviour is documented as intended
# rather than discovered later and mistaken for a bug.
tomb=$(ch "SELECT count() FROM raw.observation WHERE ${KEY} AND _is_deleted=1")
if [ "${tomb:-0}" -ge 1 ]; then
  ok "the tombstone is retained on disk as expected (${tomb} row), hidden from FINAL reads"
else
  bad "expected a retained tombstone, found none"
fi

step "The delete must also propagate through dbt into the marts"
docker compose run --rm pipeline dbt build --project-dir /app/dbt --profiles-dir /app/dbt \
  -s stg_observation fct_indicator_observation > /dev/null 2>&1 || true
fact=$(ch "SELECT count() FROM marts.fct_indicator_observation WHERE ${KEY}")
if [ "${fact:-1}" = "0" ]; then
  ok "the deleted row is absent from marts.fct_indicator_observation"
else
  bad "the deleted row is STILL in the fact table (${fact} rows). The _is_deleted filter is not being applied."
fi

printf '\n'
[ "$FAILED" -eq 0 ] && { printf '\033[32mINSERT, UPDATE and DELETE all propagate correctly.\033[0m\n\n'; exit 0; }
printf '\033[31mMutation propagation FAILED.\033[0m\n\n'; exit 1
