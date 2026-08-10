"""Load validated records into PostgreSQL.

Two properties this module exists to guarantee.

**Idempotency.** Running the same ingestion twice must leave the database in the
same state as running it once. This is not optional politeness: the orchestrator
retries failed tasks, so every task is potentially an at-least-once operation. The
mechanism is an upsert on the natural key, which is available because the natural
key IS the primary key (see the note in sql/oltp/001_schema.sql on why, and
docs/cdc-wire-format.md for what that buys downstream).

**Not emitting change events for changes that did not happen.** The upsert carries
`WHERE source_hash IS DISTINCT FROM EXCLUDED.source_hash`. Without that predicate,
a re-ingest rewrites every row with identical values, and Postgres emits a WAL
record for each of those no-op updates. Debezium then faithfully streams thousands
of events representing nothing: the topic fills, CDC lag and throughput graphs stop
meaning anything, the replication slot does real work to replicate nothing, and
ClickHouse merges versions that are all identical. A single WHERE clause is the
difference between a CDC stream that describes reality and one that describes the
scheduler.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence

import psycopg
from psycopg import sql

from ingest.checks import GateResult, Rejection

log = logging.getLogger(__name__)

# Rows per INSERT statement. Large enough that the round-trip count is not the
# bottleneck, small enough that one statement's parameter list stays well inside
# Postgres's 65535-parameter limit even on the widest table here (14 columns times
# 500 rows is 7000).
BATCH_SIZE = 500


@dataclass(frozen=True)
class TableSpec:
    """Everything the generic upsert needs to know about one target table."""

    name: str
    columns: tuple[str, ...]
    conflict_columns: tuple[str, ...]

    @property
    def updatable_columns(self) -> tuple[str, ...]:
        # The conflict key is never in the SET list: it is what identified the row.
        return tuple(c for c in self.columns if c not in self.conflict_columns)


COUNTRY_SPEC = TableSpec(
    name="wb.country",
    columns=(
        "country_id", "iso2_code", "name", "region_id", "region_name",
        "admin_region_id", "income_level_id", "income_level", "lending_type_id",
        "lending_type", "capital_city", "longitude", "latitude", "source_hash",
    ),
    conflict_columns=("country_id",),
)

INDICATOR_SPEC = TableSpec(
    name="wb.indicator",
    columns=(
        "indicator_id", "name", "source_id", "source_name", "source_note",
        "unit", "topics", "source_hash",
    ),
    conflict_columns=("indicator_id",),
)

OBSERVATION_SPEC = TableSpec(
    name="wb.observation",
    columns=(
        "country_id", "indicator_id", "obs_year", "obs_value", "obs_decimals",
        "api_last_updated", "source_hash",
    ),
    conflict_columns=("country_id", "indicator_id", "obs_year"),
)


@dataclass
class LoadStats:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    rejected: int = 0

    @property
    def written(self) -> int:
        return self.inserted + self.updated

    def __str__(self) -> str:
        return (
            f"inserted={self.inserted} updated={self.updated} "
            f"unchanged={self.unchanged} rejected={self.rejected}"
        )


def _build_upsert(spec: TableSpec, row_count: int) -> sql.Composed:
    """Compose the change-detecting upsert.

    `RETURNING (xmax = 0)` is how the three outcomes are told apart in one
    round trip. Postgres sets xmax to 0 on a freshly inserted tuple and to the
    updating transaction id on an updated one, so the returned boolean separates
    inserts from updates. Rows that hit the conflict but fail the
    IS DISTINCT FROM predicate return nothing at all, so "unchanged" is the
    difference between the batch size and the number of rows returned. That count
    is worth having: it is the direct evidence that re-running the pipeline does
    not churn the change stream.
    """
    table = sql.SQL(".").join(sql.Identifier(part) for part in spec.name.split("."))
    bare_table = sql.Identifier(spec.name.split(".")[-1])

    columns = sql.SQL(", ").join(sql.Identifier(c) for c in spec.columns)
    one_row = sql.SQL("({})").format(
        sql.SQL(", ").join(sql.Placeholder() for _ in spec.columns)
    )
    values = sql.SQL(", ").join([one_row] * row_count)
    conflict = sql.SQL(", ").join(sql.Identifier(c) for c in spec.conflict_columns)
    assignments = sql.SQL(", ").join(
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
        for c in spec.updatable_columns
    )

    return sql.SQL(
        "INSERT INTO {table} ({columns}) VALUES {values} "
        "ON CONFLICT ({conflict}) DO UPDATE SET {assignments}, updated_at = now() "
        "WHERE {bare}.source_hash IS DISTINCT FROM EXCLUDED.source_hash "
        "RETURNING (xmax = 0) AS inserted"
    ).format(
        table=table,
        columns=columns,
        values=values,
        conflict=conflict,
        assignments=assignments,
        bare=bare_table,
    )


def _upsert(
    cursor: psycopg.Cursor, spec: TableSpec, records: Sequence[Any]
) -> LoadStats:
    stats = LoadStats()
    for start in range(0, len(records), BATCH_SIZE):
        chunk = records[start : start + BATCH_SIZE]
        payloads = [record.model_dump() for record in chunk]
        params: list[Any] = []
        for payload in payloads:
            params.extend(payload[column] for column in spec.columns)

        cursor.execute(_build_upsert(spec, len(chunk)), params)
        returned = cursor.fetchall()

        inserted = sum(1 for row in returned if row[0])
        stats.inserted += inserted
        stats.updated += len(returned) - inserted
        stats.unchanged += len(chunk) - len(returned)
    return stats


def _record_rejections(
    cursor: psycopg.Cursor, stream: str, rejections: Sequence[Rejection]
) -> None:
    """Persist rejected payloads so the gate's decisions are inspectable.

    A rejection that is only logged is a rejection nobody will ever look at. In a
    table it can be counted, alerted on, and explained to whoever owns the source.
    """
    if not rejections:
        return
    for start in range(0, len(rejections), BATCH_SIZE):
        chunk = rejections[start : start + BATCH_SIZE]
        cursor.executemany(
            "INSERT INTO ops.ingest_reject (stream_name, reason, payload) "
            "VALUES (%s, %s, %s)",
            [(stream, r.reason, json.dumps(r.payload, default=str)) for r in chunk],
        )


class PostgresLoader:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: psycopg.Connection | None = None

    def __enter__(self) -> PostgresLoader:
        # autocommit off. Every load below is one explicit transaction, which is
        # the point: data, audit row and watermark commit together or not at all.
        self._conn = psycopg.connect(self._dsn, autocommit=False)
        return self

    def __exit__(self, exc_type: object, *_: object) -> None:
        if self._conn is not None:
            if exc_type is not None:
                self._conn.rollback()
            self._conn.close()
            self._conn = None

    @property
    def connection(self) -> psycopg.Connection:
        if self._conn is None:
            raise RuntimeError("PostgresLoader used outside its context manager")
        return self._conn

    # -----------------------------------------------------------------------
    def load(
        self,
        stream: str,
        spec: TableSpec,
        result: GateResult[Any],
        cursor_value: str | None = None,
    ) -> LoadStats:
        """Load one stream: data, rejections, audit row and watermark, atomically.

        The atomicity is the design, not an implementation detail. If the watermark
        advanced in its own transaction after the data, a crash in between would
        leave a watermark claiming work that was rolled back, and the next run would
        skip it. Committing them together means the recorded position and the data
        are never in disagreement: either both are there or neither is.

        The same argument applies to the audit row, which is why a failed load has
        no audit row claiming success rather than an orphaned one.
        """
        conn = self.connection
        started = datetime.now(timezone.utc)

        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO ops.ingest_run (stream_name, started_at, status) "
                "VALUES (%s, %s, 'running') RETURNING run_id",
                (stream, started),
            )
            row = cursor.fetchone()
            assert row is not None
            run_id = row[0]

            try:
                stats = _upsert(cursor, spec, result.accepted)
                stats.rejected = len(result.rejected)
                _record_rejections(cursor, stream, result.rejected)

                cursor.execute(
                    "UPDATE ops.ingest_run SET finished_at = now(), status = 'success', "
                    "rows_seen = %s, rows_inserted = %s, rows_updated = %s, "
                    "rows_unchanged = %s, rows_rejected = %s WHERE run_id = %s",
                    (
                        result.seen, stats.inserted, stats.updated,
                        stats.unchanged, stats.rejected, run_id,
                    ),
                )

                # A missing watermark row means "never run", which is the same code
                # path as a normal run rather than a separate bootstrap branch. One
                # code path means the bootstrap case is exercised by every test of
                # the steady-state case.
                cursor.execute(
                    "INSERT INTO ops.ingest_watermark "
                    "  (stream_name, last_success_at, last_cursor, rows_seen, rows_written) "
                    "VALUES (%s, now(), %s, %s, %s) "
                    "ON CONFLICT (stream_name) DO UPDATE SET "
                    "  last_success_at = now(), "
                    "  last_cursor = EXCLUDED.last_cursor, "
                    "  rows_seen = ops.ingest_watermark.rows_seen + EXCLUDED.rows_seen, "
                    "  rows_written = ops.ingest_watermark.rows_written + EXCLUDED.rows_written",
                    (stream, cursor_value, result.seen, stats.written),
                )
            except Exception as exc:
                # Roll back the data, then record the failure in its own
                # transaction. Without the second step a failed run leaves no trace
                # at all, and "the pipeline has not run since Tuesday" becomes
                # indistinguishable from "the pipeline ran and found nothing".
                conn.rollback()
                with conn.cursor() as failure_cursor:
                    failure_cursor.execute(
                        "INSERT INTO ops.ingest_run "
                        "  (stream_name, started_at, finished_at, status, error_message) "
                        "VALUES (%s, %s, now(), 'failed', %s)",
                        (stream, started, str(exc)[:2000]),
                    )
                conn.commit()
                raise

        conn.commit()
        log.info("%s: %s", stream, stats)
        return stats

    # -----------------------------------------------------------------------
    def existing_keys(self, table: str, column: str) -> set[str]:
        """Read a dimension's keys, for the referential check in the gate.

        Done in the gate rather than left to the foreign key because a foreign-key
        violation aborts the entire transaction, losing every good row alongside the
        one bad one. Checking first isolates the bad row.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT {col} FROM {tbl}").format(
                    col=sql.Identifier(column),
                    tbl=sql.SQL(".").join(
                        sql.Identifier(part) for part in table.split(".")
                    ),
                )
            )
            return {row[0] for row in cursor.fetchall()}
