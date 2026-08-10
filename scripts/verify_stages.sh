#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Prove that data moved through every stage.
#
# This script is the answer to the README's "how do I validate that data moved
# through each stage" question, and it exists as an executable rather than as a list
# of commands in a document because a document drifts and a script does not. Every
# claim the README makes about this pipeline is checked here.
#
# Exit code is meaningful: 0 if every stage is populated and consistent, 1 otherwise.
# That makes it usable as a CI assertion and as an Airflow task, not just as something
# a human reads.
# ---------------------------------------------------------------------------
set -uo pipefail

cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
set -a; . ./.env; set +a

STRICT=0
[ "${1:-}" = "--strict" ] && STRICT=1

FAILED=0
CH="docker compose exec -T clickhouse clickhouse-client --user ${CLICKHOUSE_USER} --password ${CLICKHOUSE_PASSWORD}"
PG="docker compose exec -T postgres psql -tA -U ${POSTGRES_USER} -d ${POSTGRES_DB}"

hdr()  { printf '\n\033[1m%s\033[0m\n' "$1"; }
row()  { printf '  %-42s %s\n' "$1" "$2"; }
ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAILED=1; }

# Trim leading and trailing whitespace only. Deleting ALL whitespace (tr -d) also
# removes the spaces inside a multi-word result, which turns a readable status line
# into one run-together token.
trim() { sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | tr -d '\n'; }
# For values that must be compared numerically, strip everything.
num()  { tr -cd '0-9-'; }

ch()  { $CH -q "$1" 2>/dev/null | trim; }
pg()  { $PG -c "$1" 2>/dev/null | trim; }
chn() { $CH -q "$1" 2>/dev/null | num; }
pgn() { $PG -c "$1" 2>/dev/null | num; }

# ---------------------------------------------------------------------------
hdr "Stage 1: public REST API to PostgreSQL"
pg_country=$(pgn "SELECT count(*) FROM wb.country")
pg_ind=$(pgn     "SELECT count(*) FROM wb.indicator")
pg_obs=$(pgn     "SELECT count(*) FROM wb.observation")
pg_rej=$(pgn     "SELECT count(*) FROM ops.ingest_reject")
row "wb.country"                "${pg_country:-0}"
row "wb.indicator"              "${pg_ind:-0}"
row "wb.observation"            "${pg_obs:-0}"
row "ops.ingest_reject"         "${pg_rej:-0} (the pre-load gate's rejections)"
last_run=$(pg "SELECT stream_name||' '||status||' rows_seen='||rows_seen||' inserted='||rows_inserted||' updated='||rows_updated||' unchanged='||rows_unchanged FROM ops.ingest_run ORDER BY run_id DESC LIMIT 1")
row "latest ingest_run"         "${last_run:-none}"

[ "${pg_obs:-0}" -gt 0 ] && ok "PostgreSQL holds observations" \
                         || bad "PostgreSQL has no observations. Run: make ingest"

# ---------------------------------------------------------------------------
hdr "Stage 2: Debezium CDC to Redpanda"
# Reuse the module bootstrap already uses to poll the connector, rather than
# re-parsing the status JSON inline. One parser means one definition of "healthy", and
# it already knows that a connector reporting RUNNING while its only task has died is
# not healthy.
conn_out=$(python3 scripts/wait_for_connector.py \
  --url "http://127.0.0.1:${CONNECT_HOST_PORT}" \
  --connector "${CONNECT_CONNECTOR_NAME}" \
  --timeout 6 --interval 2 2>&1 | tail -1 | trim)
if printf '%s' "$conn_out" | grep -q 'RUNNING,'; then
  row "connector status" "$conn_out"
  ok "connector and all tasks are RUNNING"
else
  row "connector status" "${conn_out:-unreachable}"
  bad "connector is not fully RUNNING"
fi

# Topic offsets, straight from the broker. This is the evidence that events really
# traversed the log rather than being written to ClickHouse by some other path.
for t in country indicator observation; do
  n=$(docker compose exec -T redpanda rpk topic describe -p "wbcdc.wb.${t}" 2>/dev/null \
      | awk '/^ *0 /{print $NF; exit}')
  row "topic wbcdc.wb.${t} high watermark" "${n:-0}"
done

slot=$(pg "SELECT format('%s active=%s wal_status=%s retained=%s', slot_name, active, coalesce(wal_status,'n/a'), pg_size_pretty(coalesce(pg_current_wal_lsn()-restart_lsn,0))) FROM pg_replication_slots WHERE slot_name='${PG_REPL_SLOT}'" | sed 's/|/ /g')
row "replication slot" "${slot:-MISSING}"
retained_bytes=$(pgn "SELECT coalesce(pg_current_wal_lsn()-restart_lsn,0) FROM pg_replication_slots WHERE slot_name='${PG_REPL_SLOT}'")
if [ -n "${retained_bytes:-}" ] && [ "${retained_bytes:-0}" -lt "${CDC_SLOT_LAG_ERROR_BYTES}" ]; then
  ok "replication slot is not hoarding WAL (< $((CDC_SLOT_LAG_ERROR_BYTES / 1024 / 1024)) MiB)"
else
  bad "replication slot retains ${retained_bytes:-?} bytes of WAL"
fi

# ---------------------------------------------------------------------------
hdr "Stage 3: Redpanda to ClickHouse landing layer"
ch_log=$(chn "SELECT count() FROM raw.cdc_event_log FINAL")
ch_country=$(chn "SELECT count() FROM raw.country FINAL WHERE _is_deleted=0")
ch_ind=$(chn    "SELECT count() FROM raw.indicator FINAL WHERE _is_deleted=0")
ch_obs=$(chn    "SELECT count() FROM raw.observation FINAL WHERE _is_deleted=0")
ch_quar=$(chn   "SELECT count() FROM raw.cdc_quarantine")
row "raw.cdc_event_log (immutable log)" "${ch_log:-0}"
row "raw.country"                       "${ch_country:-0}"
row "raw.indicator"                     "${ch_ind:-0}"
row "raw.observation"                   "${ch_obs:-0}"
row "raw.cdc_quarantine"                "${ch_quar:-0} (should be 0)"

# The Kafka consumer's own view. This is where a silently stalled consumer shows up
# and nowhere else: a materialized view that throws fails the block, offsets are never
# committed, and the engine retries the same block forever with no error reaching any
# client.
consumers=$(ch "SELECT arrayStringConcat(groupArray(concat(\`table\`, '=', toString(num_messages_read))), ' ') FROM system.kafka_consumers")
row "kafka consumers (messages read)" "${consumers:-none}"

# Exceptions are reported separately and only when RECENT, for a specific reason:
# system.kafka_consumers keeps the exception HISTORY, so a Kafka engine table created
# before its topic existed permanently carries a "Broker: Unknown topic or partition"
# entry from that moment. Reporting the full history makes a healthy consumer look
# broken forever. Only an exception in the last five minutes indicates a live problem.
recent_exc=$(ch "SELECT arrayStringConcat(groupArray(concat(\`table\`, ': ', substring(msg, 1, 90))), ' | ')
  FROM (
    SELECT \`table\`, arrayJoin(arrayZip(exceptions.time, exceptions.text)) AS pair,
           pair.1 AS at, pair.2 AS msg
    FROM system.kafka_consumers
  ) WHERE at > now() - INTERVAL 5 MINUTE")
if [ -z "${recent_exc}" ]; then
  ok "no Kafka consumer exceptions in the last 5 minutes"
else
  bad "recent Kafka consumer exception: ${recent_exc}"
fi

# Parity is the actual test: the warehouse must agree with the source.
if [ "${ch_obs:-0}" = "${pg_obs:-0}" ] && [ "${pg_obs:-0}" -gt 0 ]; then
  ok "observation parity: PostgreSQL ${pg_obs} = ClickHouse ${ch_obs}"
else
  bad "observation parity broken: PostgreSQL ${pg_obs:-0} vs ClickHouse ${ch_obs:-0}"
fi
[ "${ch_quar:-0}" = "0" ] && ok "nothing quarantined" || bad "${ch_quar} events quarantined"

# Duplicate check. A ReplacingMergeTree deduplicates at merge time, so a FINAL read
# and a plain read disagreeing by a lot means merges are behind, not that data is
# wrong. Compared explicitly so the difference is visible rather than surprising.
dupes=$(chn "SELECT count() - uniqExact((country_id, indicator_id, obs_year)) FROM raw.observation WHERE _is_deleted=0")
row "raw.observation unmerged duplicates" "${dupes:-0} (harmless: collapse at merge time)"

# ---------------------------------------------------------------------------
hdr "Stage 4: CDC freshness and connector liveness"
hb=$(ch "SELECT concat('lag=', toString(lag_seconds), 's beats=', toString(beats_observed)) FROM ops.cdc_heartbeat_lag")
row "heartbeat (connector liveness)" "${hb:-no heartbeat yet}"
hb_lag=$(chn "SELECT lag_seconds FROM ops.cdc_heartbeat_lag")
if [ -n "${hb_lag:-}" ] && [ "${hb_lag}" -le "${CDC_LAG_ERROR_SECONDS}" ]; then
  ok "CDC lag ${hb_lag}s is within the ${CDC_LAG_ERROR_SECONDS}s threshold"
else
  bad "CDC lag ${hb_lag:-unknown}s exceeds the ${CDC_LAG_ERROR_SECONDS}s threshold"
fi
$CH -q "SELECT src_table, events_30d, inserts, updates, deletes, seconds_since_last_change
        FROM ops.cdc_freshness ORDER BY src_table FORMAT PrettyCompactMonoBlock" 2>/dev/null \
  | sed 's/^/  /'

# ---------------------------------------------------------------------------
hdr "Stage 5: dbt staging layer"
for t in stg_country stg_indicator stg_observation; do
  n=$(chn "SELECT count() FROM staging.${t}")
  row "staging.${t}" "${n:-0 (not built)}"
done
stg_obs=$(chn "SELECT count() FROM staging.stg_observation")
if [ "${stg_obs:-0}" = "${ch_obs:-0}" ] && [ "${stg_obs:-0}" -gt 0 ]; then
  ok "staging matches the landing layer (${stg_obs})"
else
  bad "staging ${stg_obs:-0} does not match landing ${ch_obs:-0}. Run: make dbt-build"
fi

# ---------------------------------------------------------------------------
hdr "Stage 6: dbt marts, analytics-ready and ML-ready"
for t in dim_country dim_indicator fct_indicator_observation agg_country_year_features; do
  n=$(chn "SELECT count() FROM marts.${t}")
  row "marts.${t}" "${n:-0 (not built)}"
done
fact=$(chn "SELECT count() FROM marts.fct_indicator_observation")
feat=$(chn "SELECT count() FROM marts.agg_country_year_features")

if [ "${fact:-0}" = "${stg_obs:-0}" ] && [ "${fact:-0}" -gt 0 ]; then
  ok "fact reconciles to staging (${fact})"
else
  bad "fact ${fact:-0} does not reconcile to staging ${stg_obs:-0}"
fi

# The ML table's grain, checked independently of the dbt test so the guarantee holds
# even if someone runs dbt without tests.
feat_dupes=$(chn "SELECT count() - uniqExact((country_id, obs_year)) FROM marts.agg_country_year_features")
if [ "${feat_dupes:-1}" = "0" ] && [ "${feat:-0}" -gt 0 ]; then
  ok "ML feature grain is unique: one row per country and year (${feat} rows)"
else
  bad "ML feature table has ${feat_dupes:-?} duplicate rows on its stated grain"
fi

completeness=$(ch "SELECT concat('min=', toString(min(feature_completeness_pct)), '% avg=', toString(round(avg(feature_completeness_pct),1)), '% max=', toString(max(feature_completeness_pct)), '%') FROM marts.agg_country_year_features")
row "feature completeness" "${completeness:-n/a}"

# ---------------------------------------------------------------------------
printf '\n'
if [ "$FAILED" -eq 0 ]; then
  printf '\033[32mAll stages verified.\033[0m Data moved from the public API to the marts.\n\n'
  exit 0
fi
printf '\033[31mVerification FAILED.\033[0m See the FAIL lines above.\n'
printf 'Most common cause: a stage has not been run yet. Try: make demo\n\n'
[ "$STRICT" -eq 1 ] && exit 1
exit 1
