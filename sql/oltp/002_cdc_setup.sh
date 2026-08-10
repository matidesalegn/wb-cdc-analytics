#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Replication role and publication for Debezium.
#
# A shell script rather than a .sql file because psql does not interpolate
# environment variables into SQL, and the replication password comes from the
# environment. Writing it as SQL would mean either committing a credential or an
# unexplained sed step.
#
# Two psql behaviours this file is deliberately written around:
#
#   1. psql does NOT substitute :'var' inside a dollar-quoted block. Inside
#      DO $$ ... $$ the text is passed to the server verbatim, so :'repl_user'
#      arrives as a literal colon and the server raises a syntax error. The
#      idiomatic fix is to build the statement with format() in a plain SELECT
#      and execute the result with \gexec, which is what happens below. There is
#      no DO block here, on purpose.
#
#   2. docker-entrypoint-initdb.d runs only once, on an empty data directory,
#      and it is not transactional across files. If this script fails after
#      001_schema.sql has committed, the volume is left half-applied and the
#      NEXT start skips initdb entirely, so the tables exist and the publication
#      silently does not. That failure mode is why scripts/bootstrap.sh re-runs
#      this same logic on every start and is the authoritative path. initdb is
#      only an optimisation.
# ---------------------------------------------------------------------------
set -euo pipefail

: "${PG_REPL_USER:=debezium}"
: "${PG_REPL_PASSWORD:?PG_REPL_PASSWORD must be set}"
: "${PG_PUBLICATION:=wb_cdc_pub}"
: "${POSTGRES_USER:?POSTGRES_USER must be set}"
: "${POSTGRES_DB:?POSTGRES_DB must be set}"

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname "$POSTGRES_DB" \
     --set=repl_user="$PG_REPL_USER" \
     --set=repl_password="$PG_REPL_PASSWORD" \
     --set=publication="$PG_PUBLICATION" <<'SQL'

-- ---------------------------------------------------------------------------
-- A dedicated role for the connector, holding REPLICATION plus read access and
-- nothing else. The application role never gets REPLICATION, and this role can
-- never write business data. If the connector's credentials leak, the blast
-- radius is "can read three tables and stream their changes".
--
-- REPLICATION is a cluster-level attribute and cannot be scoped to one
-- database. That is a Postgres constraint, not an oversight, and it is the
-- reason the table grants below are kept narrow.
--
-- Idempotent in two steps: create only when absent, then always ALTER so the
-- password tracks .env on a re-run.
-- ---------------------------------------------------------------------------
SELECT format('CREATE ROLE %I WITH LOGIN', :'repl_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'repl_user')
\gexec

SELECT format(
    'ALTER ROLE %I WITH LOGIN REPLICATION PASSWORD %L',
    :'repl_user', :'repl_password'
)
\gexec

GRANT USAGE ON SCHEMA wb TO :"repl_user";
GRANT SELECT ON ALL TABLES IN SCHEMA wb TO :"repl_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA wb GRANT SELECT ON TABLES TO :"repl_user";

-- The heartbeat write is executed by the connector as this role, so it needs
-- write access to exactly that one table and no other.
GRANT UPDATE, INSERT ON wb.cdc_heartbeat TO :"repl_user";

-- ---------------------------------------------------------------------------
-- The publication defines what logical decoding emits.
--
-- Created explicitly rather than leaving Debezium's
-- publication.autocreate.mode to do it, because the permissive default creates
-- the publication FOR ALL TABLES. That silently captures every table ever added
-- to the database, including the ops bookkeeping tables, and it means adding an
-- unrelated table changes what the pipeline streams. An explicit list makes the
-- capture surface a reviewable part of the repository, and the connector sets
-- publication.autocreate.mode=disabled to match.
--
-- Reconciled with ALTER ... SET TABLE rather than DROP and CREATE. SET TABLE is
-- equally declarative (whatever this file lists is what the publication
-- contains) but it does not invalidate an attached replication slot, so
-- re-running bootstrap against a live connector is safe.
-- ---------------------------------------------------------------------------
SELECT format('CREATE PUBLICATION %I FOR TABLE wb.country', :'publication')
WHERE NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = :'publication')
\gexec

SELECT format(
    'ALTER PUBLICATION %I SET TABLE wb.country, wb.indicator, wb.observation, wb.cdc_heartbeat',
    :'publication'
)
\gexec

SQL

echo "CDC setup complete: role ${PG_REPL_USER}, publication ${PG_PUBLICATION}"
