#!/usr/bin/env bash
# Check the host can actually run this stack, and say so clearly before
# anything starts. A reviewer whose Docker has 4 GB allocated will otherwise
# see containers OOM-killed in an order that looks like a code bug.
set -euo pipefail

FAIL=0
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$1"; }
ok()   { printf '  \033[32mOK\033[0m    %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAIL=1; }

echo "preflight"

# --- Docker present and running --------------------------------------------
if ! command -v docker > /dev/null 2>&1; then
  bad "docker not found on PATH"
elif ! docker info > /dev/null 2>&1; then
  bad "docker is installed but the daemon is not reachable"
else
  ok "docker daemon reachable"
fi

# --- Compose v2 -------------------------------------------------------------
# v1 (docker-compose) does not support the `--wait` flag or
# `depends_on: condition: service_healthy` the way this stack relies on.
if docker compose version > /dev/null 2>&1; then
  ok "docker compose v2 ($(docker compose version --short 2>/dev/null || echo present))"
else
  bad "docker compose v2 required (the 'docker-compose' v1 binary will not work)"
fi

# --- Memory available to Docker --------------------------------------------
# Measured budget for the full stack: Redpanda 1.0 GB, Connect 0.7 GB,
# ClickHouse 0.8 GB, Postgres 0.2 GB, Airflow 0.8 GB, Prometheus and Grafana
# 0.4 GB. Core path alone fits in about 3 GB.
MEM_BYTES=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)
MEM_GB=$(( MEM_BYTES / 1024 / 1024 / 1024 ))
if [ "$MEM_GB" -ge 8 ]; then
  ok "${MEM_GB} GB available to Docker (full stack fits)"
elif [ "$MEM_GB" -ge 4 ]; then
  warn "${MEM_GB} GB available to Docker. Use 'make up' for the core path only; 'make up-all' may be tight."
else
  bad "${MEM_GB} GB available to Docker. Raise Docker's memory limit to at least 4 GB."
fi

# --- Disk -------------------------------------------------------------------
DISK_AVAIL_GB=$(df -BG --output=avail / 2>/dev/null | tail -1 | tr -dc '0-9' || echo 0)
if [ "${DISK_AVAIL_GB:-0}" -ge 10 ]; then
  ok "${DISK_AVAIL_GB} GB free disk"
else
  warn "${DISK_AVAIL_GB:-?} GB free disk. Images total roughly 8 GB on first pull."
fi

# --- Host ports free --------------------------------------------------------
# Deliberately non-default ports, but check anyway: a collision surfaces as a
# container that will not start, which is confusing if you did not expect it.
PORTS_FILE=".env"
[ -f "$PORTS_FILE" ] || PORTS_FILE=".env.example"
# shellcheck disable=SC2013
for var in POSTGRES_HOST_PORT CLICKHOUSE_HTTP_HOST_PORT REDPANDA_KAFKA_HOST_PORT \
           CONNECT_HOST_PORT AIRFLOW_HOST_PORT PROMETHEUS_HOST_PORT GRAFANA_HOST_PORT; do
  port=$(grep -E "^${var}=" "$PORTS_FILE" 2>/dev/null | head -1 | cut -d= -f2 | tr -dc '0-9')
  [ -n "${port:-}" ] || continue
  if command -v ss > /dev/null 2>&1 && ss -ltn 2>/dev/null | grep -qE "[:.]${port}\b"; then
    bad "host port ${port} (${var}) is already in use"
  fi
done
[ "$FAIL" -eq 0 ] && ok "all host ports free"

# --- Tools used by verification --------------------------------------------
for tool in curl jq python3; do
  if command -v "$tool" > /dev/null 2>&1; then
    ok "$tool present"
  else
    warn "$tool not found. 'make verify' needs it."
  fi
done

echo
if [ "$FAIL" -ne 0 ]; then
  echo "preflight FAILED. Fix the items above, then re-run 'make preflight'." >&2
  exit 1
fi
echo "preflight passed. Run 'make demo'."
