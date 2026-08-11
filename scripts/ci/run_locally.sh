#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Run the CI fast lane locally, with the same commands .github/workflows/ci.yml uses.
#
#   make ci-local
#
# WHY THIS EXISTS
#   A CI workflow is a set of claims. This script lets anyone verify those claims on their own
#   machine, without a GitHub account, without repository access, and without waiting for a
#   hosted runner. Each check below prints the CI job it corresponds to, so the mapping to
#   ci.yml is checkable rather than asserted.
#
#   It is not a replacement for CI: there is no clean checkout, no isolation, and it trusts the
#   local toolchain. It is evidence, and a fast inner loop.
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/../.."

PASS=0
FAIL=0
SKIP=0
RESULTS=()

hdr()  { printf '\n\033[1;36m==> %s\033[0m\n' "$1"; }
ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$1"; PASS=$((PASS+1)); RESULTS+=("PASS|$1"); }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAIL=$((FAIL+1)); RESULTS+=("FAIL|$1"); }
skip() { printf '  \033[33mSKIP\033[0m  %s\n' "$1"; SKIP=$((SKIP+1)); RESULTS+=("SKIP|$1"); }

# check <ci-job> <description> <command...>
check() {
  local job="$1" desc="$2"; shift 2
  if "$@" > /tmp/wb-ci-local.out 2>&1; then
    ok "[${job}] ${desc}"
  else
    bad "[${job}] ${desc}"
    tail -12 /tmp/wb-ci-local.out | sed 's/^/          /'
  fi
}

START=$(date +%s)
printf '\n\033[1mCI fast lane, run locally\033[0m\n'
printf 'Same commands as .github/workflows/ci.yml. Bracketed names are its job names.\n'

# --- lint ------------------------------------------------------------------
hdr "job: lint"
if python3 -m ruff --version > /dev/null 2>&1; then
  check lint "ruff check" python3 -m ruff check .
  check lint "ruff format --check" python3 -m ruff format --check .
else
  skip "[lint] ruff not installed: pip install ruff==0.12.5"
fi

# --- unit ------------------------------------------------------------------
hdr "job: unit"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-ci-local-not-a-real-secret}"
export CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-ci-local-not-a-real-secret}"
if python3 -c "import pytest_cov" > /dev/null 2>&1; then
  # Same scoping and floor as CI: the four modules a no-container lane can actually reach.
  check unit "59 unit tests, coverage floor 85 on the reachable modules" \
    python3 -m pytest tests/unit -q \
      --cov=ingest.api_client --cov=ingest.checks \
      --cov=ingest.contracts --cov=ingest.settings \
      --cov-report=term-missing --cov-fail-under=85
else
  check unit "59 unit tests (coverage skipped: pytest-cov not installed)" \
    python3 -m pytest tests/unit -q
fi

# --- static ----------------------------------------------------------------
hdr "job: static"
[ -f .env ] || python3 scripts/gen_env.py > /dev/null 2>&1

check static "compose model resolves across every profile" \
  docker compose --profile observability --profile orchestration --profile console config

check static "connector config renders to a valid Connect payload" \
  bash scripts/ci/check_connector_render.sh

check static "alert rules are syntactically valid (promtool check rules)" \
  docker run --rm -v "$PWD/observability/prometheus:/p:ro" \
    --entrypoint promtool prom/prometheus:v3.1.0 check rules /p/alerts.yml

check static "alert rules behave as intended (promtool test rules, 13 cases)" \
  docker run --rm -v "$PWD/observability/prometheus:/p:ro" \
    --entrypoint promtool prom/prometheus:v3.1.0 test rules /p/alerts_test.yml

check static "Grafana dashboard valid, every panel documented" \
  python3 scripts/ci/check_dashboard.py

check static "project convention gate (11 rules)" bash scripts/ci/convention_gate.sh

# The em-dash rule depends on a UTF-8 locale, and its failure mode is to pass silently, so it
# is asserted directly. A convention rule that cannot fail is not a rule.
if printf 'a\xe2\x80\x94b\n' | grep -qP '[\x{2014}]' 2>/dev/null; then
  ok "[static] grep -P PCRE is live, so the em-dash rule can actually fail"
else
  bad "[static] grep -P cannot match \\x{2014}: the em-dash rule is silently passing. Set LANG=C.UTF-8"
fi

# --- dbt -------------------------------------------------------------------
hdr "job: dbt"
if docker image inspect wb-cdc-analytics-pipeline:latest > /dev/null 2>&1; then
  check dbt "dbt parse: every model, macro, test and YAML compiles" \
    docker compose run --rm --entrypoint dbt pipeline \
      parse --project-dir /app/dbt --profiles-dir /app/dbt
else
  skip "[dbt] pipeline image not built: docker compose --profile tools build pipeline"
fi

# --- dags ------------------------------------------------------------------
hdr "job: dags"
if docker image inspect wb-cdc-analytics-airflow:latest > /dev/null 2>&1; then
  check dags "both DAGs import cleanly, each with a watcher and a doc_md" \
    docker compose run --rm --entrypoint python airflow /opt/airflow/tests/check_dags.py
else
  skip "[dags] airflow image not built: docker compose --profile orchestration build airflow"
fi

# --- the workflow file itself ----------------------------------------------
hdr "workflow file"
if command -v docker > /dev/null 2>&1; then
  check workflow "actionlint on .github/workflows/ci.yml" \
    docker run --rm -v "$PWD:/repo:ro" -w /repo rhysd/actionlint:latest
else
  skip "[workflow] docker unavailable"
fi

ELAPSED=$(( $(date +%s) - START ))
printf '\n\033[1m%d passed, %d failed, %d skipped, in %ds\033[0m\n' "$PASS" "$FAIL" "$SKIP" "$ELAPSED"

# Machine-readable summary, so docs/ci-evidence.md can be regenerated rather than hand-edited.
{
  printf '| Result | CI job | Check |\n|---|---|---|\n'
  for r in "${RESULTS[@]}"; do
    verdict="${r%%|*}"; rest="${r#*|}"
    job=$(printf '%s' "$rest" | sed -n 's/^\[\([a-z]*\)\].*/\1/p')
    desc=$(printf '%s' "$rest" | sed 's/^\[[a-z]*\] //')
    printf '| %s | `%s` | %s |\n' "$verdict" "${job:-n/a}" "$desc"
  done
  printf '\n%d passed, %d failed, %d skipped, %ds, on %s\n' \
    "$PASS" "$FAIL" "$SKIP" "$ELAPSED" "$(date -u '+%Y-%m-%d %H:%M UTC')"
} > /tmp/wb-ci-results.md

[ "$FAIL" -eq 0 ] || exit 1
