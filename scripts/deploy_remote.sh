#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Deploy one commit of this repository to the demo host, and roll back if the
# post-deploy health check fails.
#
# This runs ON the target host. It lives in the repository rather than inside the
# workflow YAML for three reasons: it can be read without knowing GitHub Actions, it can
# be run by hand during an incident when Actions is the thing that is broken, and a
# reviewer can audit exactly what deployment does.
#
# Usage:  deploy_remote.sh <git-sha>
#
# DESIGN NOTES, because the choices matter more than the commands.
#
# Deploys a SHA, never a branch. `git checkout main` deploys whatever main happens to be
# when the command runs, which means a re-run of an old pipeline silently ships new code.
# A SHA makes the deployed artifact identical to the one CI tested.
#
# Records the previous SHA before changing anything, so rollback is a fact rather than a
# hope. The rollback path is the same code path as the deploy, so it is exercised every
# time a deploy succeeds.
#
# Idempotent. Re-running with the same SHA converges rather than duplicating: compose is
# declarative, the DDL is written to be re-appliable, and the ingestion upsert is
# change-detecting.
# ---------------------------------------------------------------------------
set -uo pipefail

TARGET_SHA="${1:?usage: deploy_remote.sh <git-sha>}"
APP_DIR="${APP_DIR:-$HOME/wb-cdc-analytics}"
STATE_DIR="${STATE_DIR:-$HOME/.wbcdc-deploy}"
PROFILES=(--profile observability --profile orchestration --profile console)
OVERLAY=(-f docker-compose.yml -f docker-compose.deploy.yml)

mkdir -p "$STATE_DIR"
step() { printf '\n\033[1;36m==> %s\033[0m\n' "$1"; }
die()  { printf '\033[31mFAIL\033[0m %s\n' "$1" >&2; exit 1; }

cd "$APP_DIR" || die "no app dir at $APP_DIR"

# -------------------------------------------------------------------------
step "Recording the current state so rollback is possible"
PREVIOUS_SHA=$(git rev-parse HEAD 2>/dev/null || echo "")
echo "  previous: ${PREVIOUS_SHA:-<none>}"
echo "  target:   $TARGET_SHA"
if [ "$PREVIOUS_SHA" = "$TARGET_SHA" ]; then
  echo "  already at the target SHA; continuing anyway because converging is the point"
fi

# -------------------------------------------------------------------------
step "Fetching the exact commit"
git fetch --quiet origin || die "git fetch failed"
git rev-parse --quiet --verify "${TARGET_SHA}^{commit}" >/dev/null \
  || die "commit $TARGET_SHA does not exist on this host after fetch"

# .env is gitignored and holds the generated secrets for THIS deployment. A hard reset
# would leave it alone, but `git clean -ffdx` would delete it, and regenerating it while
# the database volumes persist leaves the databases rejecting the new passwords while
# still reporting healthy. So: reset tracked files, never clean untracked ones.
git reset --quiet --hard "$TARGET_SHA" || die "git reset to $TARGET_SHA failed"
[ -f .env ] || die ".env vanished; the databases would be reinitialised with new secrets"
echo "  now at $(git rev-parse --short HEAD): $(git log -1 --format=%s | cut -c1-60)"

# -------------------------------------------------------------------------
step "Applying the deployment overlay"
[ -f docker-compose.deploy.yml ] || die "docker-compose.deploy.yml missing on the host"
docker compose "${OVERLAY[@]}" "${PROFILES[@]}" config >/dev/null \
  || die "the merged compose model is invalid; nothing was changed"

step "Rolling the stack onto the new commit"
# --wait blocks until every healthcheck passes, so a container that comes up broken fails
# the deploy here rather than being discovered by the smoke test later.
if ! docker compose "${OVERLAY[@]}" "${PROFILES[@]}" up -d --wait --remove-orphans; then
  echo "  compose up failed"
  ROLL=1
fi

# -------------------------------------------------------------------------
step "Smoke test: does data actually still move end to end"
if [ "${ROLL:-0}" = "0" ]; then
  # verify_stages.sh has a meaningful exit code and checks all six stages including
  # source-to-warehouse parity, so it is the real assertion rather than a port check.
  if ! bash scripts/verify_stages.sh --strict; then
    echo "  verify_stages FAILED after deploy"
    ROLL=1
  fi
fi

# -------------------------------------------------------------------------
if [ "${ROLL:-0}" != "0" ]; then
  step "ROLLING BACK to $PREVIOUS_SHA"
  if [ -z "$PREVIOUS_SHA" ]; then
    die "no previous SHA recorded; cannot roll back automatically. Manual intervention needed."
  fi
  git reset --quiet --hard "$PREVIOUS_SHA" || die "rollback checkout failed; host is now inconsistent"
  docker compose "${OVERLAY[@]}" "${PROFILES[@]}" up -d --wait --remove-orphans \
    || die "rollback compose up failed; host is DOWN and needs manual attention"
  bash scripts/verify_stages.sh >/dev/null 2>&1 \
    && echo "  rolled back to $PREVIOUS_SHA and it verifies" \
    || echo "  WARNING: rolled back but verification still fails; the problem predates this deploy"
  die "deploy of $TARGET_SHA failed and was rolled back"
fi

# -------------------------------------------------------------------------
step "Recording the deployed SHA"
printf '%s\n' "$TARGET_SHA" > "$STATE_DIR/current-sha"
printf '%s\n' "${PREVIOUS_SHA}" > "$STATE_DIR/previous-sha"
echo "  deployed $TARGET_SHA"
printf '\n\033[1;32mDeploy succeeded.\033[0m\n'
