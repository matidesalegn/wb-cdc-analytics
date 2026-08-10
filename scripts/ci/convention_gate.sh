#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Project-specific conventions that no off-the-shelf linter knows about.
#
# Every rule here exists because breaking it produces a SILENT failure. That is the
# selection criterion: ruff catches things that error, this catches things that quietly
# do the wrong thing. Each rule below corresponds to a bug that was actually hit while
# building this pipeline, or to a documented trap that would be.
#
# Collects every violation before exiting, so one run tells you everything rather than
# one thing at a time.
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/../.."

VIOLATIONS=0
fail() { printf '  \033[31mVIOLATION\033[0m %s\n' "$1"; VIOLATIONS=$((VIOLATIONS + 1)); }
pass() { printf '  \033[32mok\033[0m        %s\n' "$1"; }

printf '\nProject convention gate\n\n'

# ---------------------------------------------------------------------------
# 1. FINAL and the tombstone filter must appear only in the shared macro.
#
# A model that reads a CDC landing table without FINAL returns duplicates; one that omits
# _is_deleted = 0 resurrects deleted rows with their non-key columns full of schema
# defaults. Both pass every test. Centralising them in ch_current_state is the control,
# so a stray FINAL in a model is a sign someone bypassed it.
# Comments are stripped before matching. The models legitimately DISCUSS FINAL in their
# header comments, and a gate that cannot tell code from prose is a gate people disable.
stray_final=""
for model in $(find dbt/models -name '*.sql' 2>/dev/null); do
  if sed 's/--.*//' "$model" | grep -qiE '\bfinal\b'; then
    stray_final="${stray_final} ${model}"
  fi
done
if [ -n "$stray_final" ]; then
  fail "FINAL used directly in a model instead of via the ch_current_state macro:"
  printf '            %s\n' $stray_final
  printf '            Reading a CDC table without the tombstone filter resurrects deleted rows silently.\n'
else
  pass "no model reads a CDC table with a bare FINAL"
fi

# ---------------------------------------------------------------------------
# 2. Any model declaring a Replacing engine must declare order_by in the SAME config.
#
# dbt-clickhouse emits ORDER BY (tuple()) when order_by is unset, and an empty sort key
# means every row shares it, so a ReplacingMergeTree collapses the entire table to ONE
# row. No error, no warning.
while IFS= read -r model; do
  [ -z "$model" ] && continue
  if grep -qiE "engine\s*=\s*'Replacing" "$model" && ! grep -qiE "order_by\s*=" "$model"; then
    fail "$model declares a Replacing engine with no order_by (collapses the table to one row)"
  fi
done <<< "$(grep -rl --include='*.sql' -iE "engine\s*=\s*'Replacing" dbt/models/ 2>/dev/null || true)"
pass "every Replacing engine declares its own order_by"

# ---------------------------------------------------------------------------
# 3. Any incremental model must declare unique_key.
#
# Without unique_key the incremental materialisation silently degrades to a plain
# append, so every run re-appends the rows inside the lookback window and duplicates
# accumulate with nothing reporting an error.
while IFS= read -r model; do
  [ -z "$model" ] && continue
  if ! grep -qE "unique_key" "$model"; then
    fail "$model is incremental but declares no unique_key (degrades to a silent append)"
  fi
done <<< "$(grep -rl --include='*.sql' -E "materialized\s*=\s*'incremental'" dbt/models/ 2>/dev/null || true)"
pass "every incremental model declares a unique_key"

# ---------------------------------------------------------------------------
# 4. No committed secrets, and .env must never be tracked.
if git ls-files --error-unmatch .env > /dev/null 2>&1; then
  fail ".env is tracked by git. It holds every credential in the stack."
else
  pass ".env is not tracked"
fi

# Look for assignments to a password-like name whose value is a literal rather than an
# env_var reference or a documented placeholder.
secretish=$(grep -rInE "(password|secret|token|api_key)\s*[:=]\s*['\"][A-Za-z0-9+/_-]{12,}" \
  --include='*.py' --include='*.yml' --include='*.yaml' --include='*.sql' --include='*.json' \
  --exclude-dir=.git --exclude-dir=tests . 2>/dev/null \
  | grep -viE "env_var|environ|getenv|CHANGEME|\\$\{|example|test-only|placeholder" || true)
if [ -n "$secretish" ]; then
  fail "possible hardcoded credential:"
  printf '            %s\n' "$secretish"
else
  pass "no hardcoded credentials outside tests"
fi

# ---------------------------------------------------------------------------
# 5. Compose must pin every image and must not publish on 0.0.0.0.
unpinned=$(grep -nE '^\s+image:.*:latest' docker-compose.yml 2>/dev/null || true)
if [ -n "$unpinned" ]; then
  fail "unpinned :latest image in docker-compose.yml: $unpinned"
else
  pass "every compose image tag is pinned"
fi

# A published port without an explicit 127.0.0.1 binds to every interface, which on a
# reviewer's laptop exposes an unauthenticated broker to their network.
open_ports=$(grep -nE '^\s+- "[0-9]+:[0-9]+"' docker-compose.yml 2>/dev/null || true)
if [ -n "$open_ports" ]; then
  fail "port published on all interfaces (should be 127.0.0.1:...): $open_ports"
else
  pass "every published port binds to 127.0.0.1"
fi

# ---------------------------------------------------------------------------
# 6. .env.example and the compose model must agree.
#
# A variable referenced by compose but missing from .env.example means `make env`
# produces a .env that cannot start the stack, and the error appears at `up` time as an
# unset-variable message rather than here.
missing_vars=""
# Comment lines are stripped first: the compose header documents the ${VAR:?...} idiom
# by example, and matching that literal would demand a variable named VAR.
while IFS= read -r var; do
  [ -z "$var" ] && continue
  grep -qE "^${var}=" .env.example || missing_vars="${missing_vars} ${var}"
done <<< "$(sed 's/#.*//' docker-compose.yml | grep -oE '\$\{[A-Z][A-Z0-9_]*' | sed 's/\${//' | sort -u)"
if [ -n "$missing_vars" ]; then
  fail "referenced by docker-compose.yml but absent from .env.example:${missing_vars}"
else
  pass ".env.example covers every variable the compose model references"
fi

# ---------------------------------------------------------------------------
# 7. The Debezium connector config must use Debezium 3.x property names.
#
# Four properties commonly copied from older tutorials fail against 3.x, two of them
# silently.
for old in "database.server.name" "delete.handling.mode"; do
  if grep -q "\"${old}\"" cdc/connectors/*.json 2>/dev/null; then
    fail "connector config uses the pre-3.x property \"${old}\""
  fi
done
if ! grep -q '"topic.prefix"' cdc/connectors/postgres-source.json 2>/dev/null; then
  fail "connector config is missing topic.prefix (the 3.x replacement for database.server.name)"
fi
if ! grep -q '"plugin.name": *"pgoutput"' cdc/connectors/postgres-source.json 2>/dev/null; then
  fail "connector config does not pin plugin.name=pgoutput (the default, decoderbufs, needs an uninstalled extension)"
fi
pass "connector config uses Debezium 3.x property names"

# ---------------------------------------------------------------------------
# 8. Every alert rule must carry a `for:` and a runbook annotation.
#
# A rule with no `for:` pages on a single scrape blip. A rule with no runbook is a
# notification, and notifications get muted.
alert_count=$(grep -cE '^\s+- alert:' observability/prometheus/alerts.yml 2>/dev/null || echo 0)
for_count=$(grep -cE '^\s+for:' observability/prometheus/alerts.yml 2>/dev/null || echo 0)
runbook_count=$(grep -cE '^\s+runbook:' observability/prometheus/alerts.yml 2>/dev/null || echo 0)
if [ "$alert_count" -eq 0 ]; then
  fail "no alert rules found"
elif [ "$for_count" -ne "$alert_count" ] || [ "$runbook_count" -ne "$alert_count" ]; then
  fail "of ${alert_count} alerts, ${for_count} have 'for:' and ${runbook_count} have a runbook; all must have both"
else
  pass "all ${alert_count} alert rules have a for-duration and a runbook"
fi

# ---------------------------------------------------------------------------
# 9. No em dashes or en dashes anywhere.
#
# A house style rule, and the reason it is enforced here rather than trusted is that
# generated files (dashboard JSON, dbt artifacts) and commit messages are not covered by
# the editor hook.
dashes=$(grep -rlP '[\x{2014}\x{2013}]' --exclude-dir=.git --exclude-dir=dbt_packages . 2>/dev/null || true)
if [ -n "$dashes" ]; then
  fail "em dash or en dash found in:"
  printf '            %s\n' $dashes
else
  pass "no em dashes or en dashes"
fi

# ---------------------------------------------------------------------------
printf '\n'
if [ "$VIOLATIONS" -gt 0 ]; then
  printf '\033[31m%d convention violation(s).\033[0m\n\n' "$VIOLATIONS"
  exit 1
fi
printf '\033[32mAll project conventions satisfied.\033[0m\n\n'
