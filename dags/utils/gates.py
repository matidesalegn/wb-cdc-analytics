"""The pipeline's quality gates, as plain functions.

Kept out of the DAG file on purpose. A gate that lives inside a DAG definition can only
be exercised by running Airflow, so in practice it is never tested and its thresholds
drift. Here they are importable, unit-testable, and the DAG is a thin wiring layer over
them.

Every threshold comes from the environment, and the same variables feed the Prometheus
alert rules. That is what stops the DAG gate and the alert from disagreeing about what
"too much lag" means, which is the usual reason a pipeline is green while the dashboard
is red.
"""

from __future__ import annotations

import logging
import os
import time

import clickhouse_connect
import psycopg

from dags.utils.observability import emit

log = logging.getLogger(__name__)


class GateFailed(RuntimeError):
    """A quality gate refused to let the pipeline continue."""


def _clickhouse():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        connect_timeout=10,
        send_receive_timeout=60,
    )


def _postgres():
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )


# ---------------------------------------------------------------------------
def gate_cdc_parity(timeout_seconds: int = 180, poll_seconds: int = 5) -> dict:
    """Wait until ClickHouse has caught up with PostgreSQL, or fail.

    This is the gate that makes the DAG's dependency on CDC honest. Debezium is a
    continuous service, not a task, so the DAG cannot "run" it. What the DAG CAN do is
    refuse to build models on top of a warehouse that has not received the rows the
    ingestion just wrote. Without this gate, dbt would build successfully against a
    partially-replicated landing table and every test would pass, because the tests
    check internal consistency and a partial copy is internally consistent.

    Polls rather than sleeping a fixed interval: replication normally completes in a
    few seconds, so a fixed sleep is either wasteful or flaky.
    """
    deadline = time.monotonic() + timeout_seconds
    with _postgres() as pg, pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM wb.observation")
        expected = cur.fetchone()[0]

    if expected == 0:
        raise GateFailed(
            "PostgreSQL holds no observations, so there is nothing to replicate. "
            "The ingestion task should have failed before this gate was reached."
        )

    client = _clickhouse()
    try:
        landed = 0
        while time.monotonic() < deadline:
            landed = client.query(
                "SELECT count() FROM raw.observation FINAL WHERE _is_deleted = 0"
            ).result_rows[0][0]
            if landed >= expected:
                emit(log, "cdc_parity_reached", expected=expected, landed=landed)
                return {"expected": expected, "landed": landed}
            time.sleep(poll_seconds)

        raise GateFailed(
            f"CDC parity not reached within {timeout_seconds}s: PostgreSQL has "
            f"{expected} observations, ClickHouse has {landed}. Check the connector "
            f"status and system.kafka_consumers for a stalled consumer."
        )
    finally:
        client.close()


# ---------------------------------------------------------------------------
def gate_cdc_lag() -> dict:
    """Assert the connector is alive, measured against the Debezium heartbeat.

    Deliberately NOT measured against a business table. An idle dimension has no recent
    events and would report unbounded lag while nothing is wrong: during testing an
    untouched dimension read 1805 seconds after half an hour of no changes. The
    heartbeat row is updated on a fixed 10 second timer, so lag against it means the
    connector, the broker or the ClickHouse consumer has actually stopped.

    The threshold is the same environment variable the CdcPipelineStalled alert uses.
    """
    threshold = int(os.environ.get("CDC_LAG_ERROR_SECONDS", "300"))
    client = _clickhouse()
    try:
        rows = client.query(
            "SELECT lag_seconds, beats_observed FROM ops.cdc_heartbeat_lag"
        ).result_rows
        if not rows or rows[0][0] is None:
            raise GateFailed(
                "No Debezium heartbeat has reached ClickHouse. Either the connector "
                "has never run, or wb.cdc_heartbeat is missing from "
                "table.include.list in the connector config."
            )
        lag, beats = int(rows[0][0]), rows[0][1]
        emit(log, "cdc_lag_checked", lag_seconds=lag, threshold=threshold, beats=beats)
        if lag > threshold:
            raise GateFailed(
                f"CDC lag is {lag}s, over the {threshold}s threshold. The heartbeat "
                f"ticks every 10s, so this is a stopped pipeline rather than a slow one."
            )
        return {"lag_seconds": lag, "beats_observed": beats}
    finally:
        client.close()


# ---------------------------------------------------------------------------
def gate_source_health() -> dict:
    """Assert the source database is not being harmed by the pipeline reading it.

    Included because a data pipeline that quietly degrades its own source is a worse
    outcome than one that stops. An inactive logical replication slot retains WAL
    indefinitely, and the disk it fills belongs to the OLTP database, not to the
    warehouse.
    """
    threshold = int(os.environ.get("CDC_SLOT_LAG_ERROR_BYTES", str(1024**3)))
    with _postgres() as pg, pg.cursor() as cur:
        cur.execute(
            "SELECT slot_name, active, "
            "       coalesce(pg_current_wal_lsn() - restart_lsn, 0)::bigint "
            "FROM pg_replication_slots"
        )
        slots = cur.fetchall()

    if not slots:
        raise GateFailed(
            "No replication slot exists. The connector is not capturing changes, so "
            "anything already in the warehouse is a snapshot rather than a live copy."
        )

    for slot_name, active, retained in slots:
        emit(log, "replication_slot_checked", slot=slot_name, active=active,
             retained_bytes=retained, threshold=threshold)
        if retained > threshold:
            raise GateFailed(
                f"Replication slot {slot_name} is retaining {retained} bytes of WAL "
                f"(threshold {threshold}). This threatens the SOURCE database's disk. "
                f"If the slot is inactive it will never drain on its own."
            )
    return {"slots": [{"name": s[0], "active": s[1], "retained_bytes": s[2]} for s in slots]}


# ---------------------------------------------------------------------------
def gate_marts_reconcile() -> dict:
    """Assert the marts contain exactly what staging does, and that the grain holds.

    Duplicates the two most important dbt tests on purpose. dbt tests run as part of
    `dbt build`, so if someone runs `dbt run` alone they do not run at all. This gate
    is in the DAG's critical path, so the guarantee holds regardless of how the models
    were built.
    """
    client = _clickhouse()
    try:
        staging = client.query("SELECT count() FROM staging.stg_observation").result_rows[0][0]
        fact = client.query(
            "SELECT count() FROM marts.fct_indicator_observation"
        ).result_rows[0][0]
        feature_rows, feature_keys = client.query(
            "SELECT count(), uniqExact((country_id, obs_year)) "
            "FROM marts.agg_country_year_features"
        ).result_rows[0]

        emit(log, "marts_reconciled", staging=staging, fact=fact,
             feature_rows=feature_rows, feature_keys=feature_keys)

        if fact != staging:
            raise GateFailed(
                f"The fact table does not reconcile to staging: staging has {staging} "
                f"rows, the fact has {fact}. An inner join to a dimension may be "
                f"dropping rows, or the incremental window may have missed a change."
            )
        if feature_rows != feature_keys:
            raise GateFailed(
                f"The ML feature table breaks its own stated grain: {feature_rows} rows "
                f"for {feature_keys} distinct (country, year) keys. A feature table with "
                f"duplicate rows leaks them straight into a training set."
            )
        return {"staging": staging, "fact": fact, "feature_rows": feature_rows}
    finally:
        client.close()
