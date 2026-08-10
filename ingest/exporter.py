"""Prometheus exporter for pipeline-level metrics.

Why this exists. Prometheus scrapes HTTP endpoints, and three of the four signals the
observability requirement names cannot be scraped from anything that already exists:

  pipeline health   lives in ops.ingest_run in PostgreSQL
  data freshness    lives in ops.cdc_freshness in ClickHouse
  CDC lag           lives in ops.cdc_heartbeat_lag in ClickHouse, and the replication
                    slot's retained WAL lives in pg_replication_slots
  resource usage    IS already exposed natively, by ClickHouse on :9363 and Redpanda
                    on :9644, which is why this exporter deliberately does not
                    duplicate any of it

So this process is the bridge for the data-plane signals only. Everything the
infrastructure already exposes is scraped directly, and no exporter container is added
for ClickHouse or the broker.

Design decisions worth stating:

  * Queries run at SCRAPE time, not on a timer. A cached value would let Prometheus
    record a fresh timestamp against stale data, which is worse than a gap: a stalled
    pipeline would look healthy at a one-minute resolution.

  * A failing query yields NO metric rather than a zero. Zero is a real, plausible row
    count, so exporting zero on error would show a healthy-looking empty warehouse.
    An absent series makes an alert fire on `absent()` and makes a graph show a gap,
    both of which are honest.

  * Served with a plain WSGI handler on its own port. The ASGI Mount("/metrics")
    pattern 307-redirects to /metrics/ and Prometheus does not follow redirects, so
    the target simply reads as down with no useful error.
"""

from __future__ import annotations

import logging
import os
import time
from wsgiref.simple_server import make_server

import clickhouse_connect
import psycopg
from prometheus_client import CollectorRegistry, make_wsgi_app
from prometheus_client.core import GaugeMetricFamily

from ingest.settings import get_settings

log = logging.getLogger("exporter")

# One namespace prefix for everything this process exports, so a Grafana query can
# select the pipeline's own metrics without matching ClickHouse's or Redpanda's.
NS = "wb_pipeline"


class PipelineCollector:
    """Collected on demand by the Prometheus client at each scrape."""

    def __init__(self) -> None:
        self._settings = get_settings()

    # -- connections ------------------------------------------------------
    def _clickhouse(self):
        s = self._settings
        return clickhouse_connect.get_client(
            host=s.clickhouse_host,
            port=s.clickhouse_http_port,
            username=s.clickhouse_user,
            password=s.clickhouse_password,
            connect_timeout=5,
            send_receive_timeout=15,
        )

    def _postgres(self):
        return psycopg.connect(self._settings.postgres_dsn, connect_timeout=5)

    # -- collection -------------------------------------------------------
    def collect(self):
        yield from self._collect_clickhouse()
        yield from self._collect_postgres()

    def _collect_clickhouse(self):
        try:
            client = self._clickhouse()
        except Exception as exc:  # noqa: BLE001 - a scrape must not crash the process
            log.warning("clickhouse unreachable: %s", exc)
            return

        try:
            # CDC lag, measured against the Debezium heartbeat rather than against a
            # business table. An idle dimension has no recent events and would report
            # unbounded lag while nothing is wrong; the heartbeat ticks on a timer, so
            # lag against it means the connector, broker or consumer has stopped.
            lag = GaugeMetricFamily(
                f"{NS}_cdc_heartbeat_lag_seconds",
                "Seconds since the last Debezium heartbeat reached ClickHouse. "
                "Connector liveness, not data freshness.",
            )
            rows = client.query("SELECT lag_seconds FROM ops.cdc_heartbeat_lag").result_rows
            if rows and rows[0][0] is not None:
                lag.add_metric([], float(rows[0][0]))
                yield lag

            # Data freshness per table. Informational on its own: a table with no
            # recent change is usually a table nothing changed in.
            freshness = GaugeMetricFamily(
                f"{NS}_seconds_since_last_change",
                "Seconds since the last change event for this source table.",
                labels=["src_table"],
            )
            events = GaugeMetricFamily(
                f"{NS}_cdc_events_total_30d",
                "Change events retained in the 30 day log, by source table and operation.",
                labels=["src_table", "op"],
            )
            for table, ev, ins, upd, dele, since in client.query(
                "SELECT src_table, events_30d, inserts, updates, deletes, "
                "seconds_since_last_change FROM ops.cdc_freshness"
            ).result_rows:
                freshness.add_metric([table], float(since))
                events.add_metric([table, "insert"], float(ins))
                events.add_metric([table, "update"], float(upd))
                events.add_metric([table, "delete"], float(dele))
            yield freshness
            yield events

            # Row counts per layer. This is what makes "did data move through every
            # stage" a graph rather than a manual query.
            layer_rows = GaugeMetricFamily(
                f"{NS}_layer_rows",
                "Rows per pipeline layer, deduplicated and excluding tombstones.",
                labels=["layer"],
            )
            for layer, count in client.query("SELECT layer, rows FROM ops.layer_counts").result_rows:
                layer_rows.add_metric([layer], float(count))

            for db, table in (
                ("staging", "stg_observation"),
                ("marts", "fct_indicator_observation"),
                ("marts", "agg_country_year_features"),
            ):
                try:
                    n = client.query(f"SELECT count() FROM {db}.{table}").result_rows[0][0]
                    layer_rows.add_metric([f"{db}.{table}"], float(n))
                except Exception:  # noqa: BLE001 - not built yet is a valid state
                    # Deliberately no zero here. A model that has never been built is
                    # not a model with zero rows, and conflating them would hide a
                    # failed dbt run behind a plausible-looking number.
                    pass
            yield layer_rows

            quarantine = GaugeMetricFamily(
                f"{NS}_cdc_quarantine_rows",
                "Change events the ingestion gate could not attribute to a business key. "
                "Expected to be zero.",
            )
            q = client.query("SELECT count() FROM raw.cdc_quarantine").result_rows
            quarantine.add_metric([], float(q[0][0] if q else 0))
            yield quarantine

            # Active parts per table. Direct evidence for the deliberate decision not
            # to partition, and the early warning for too_many_parts, which is the
            # failure mode an over-partitioned ClickHouse table reaches first.
            parts = GaugeMetricFamily(
                f"{NS}_active_parts",
                "Active parts per table. Rising sharply means merges are falling behind.",
                labels=["database", "table"],
            )
            for database, table, n in client.query(
                "SELECT database, table, count() FROM system.parts "
                "WHERE active AND database IN ('raw','staging','marts') "
                "GROUP BY database, table"
            ).result_rows:
                parts.add_metric([database, table], float(n))
            yield parts
        except Exception as exc:  # noqa: BLE001
            log.warning("clickhouse collection failed: %s", exc)
        finally:
            try:
                client.close()
            except Exception:  # noqa: BLE001
                pass

    def _collect_postgres(self):
        try:
            conn = self._postgres()
        except Exception as exc:  # noqa: BLE001
            log.warning("postgres unreachable: %s", exc)
            return

        try:
            with conn.cursor() as cur:
                # Retained WAL on the replication slot. This is the metric behind the
                # failure this pipeline is most careful about: an inactive slot retains
                # WAL indefinitely and fills the source's disk, and nothing about the
                # connector looks wrong while it happens.
                retained = GaugeMetricFamily(
                    f"{NS}_replication_slot_retained_wal_bytes",
                    "WAL bytes retained by the CDC replication slot.",
                    labels=["slot_name", "active"],
                )
                cur.execute(
                    "SELECT slot_name, active::text, "
                    "       coalesce(pg_current_wal_lsn() - restart_lsn, 0)::bigint "
                    "FROM pg_replication_slots"
                )
                for slot_name, active, wal_bytes in cur.fetchall():
                    retained.add_metric([slot_name, active], float(wal_bytes))
                yield retained

                # Ingestion outcomes. Answers "when did this last work", which a row
                # count cannot: a full table proves the pipeline worked once.
                last_success = GaugeMetricFamily(
                    f"{NS}_ingest_last_success_timestamp_seconds",
                    "Unix timestamp of the last successful ingestion, per stream.",
                    labels=["stream"],
                )
                cur.execute(
                    "SELECT stream_name, extract(epoch from last_success_at) "
                    "FROM ops.ingest_watermark WHERE last_success_at IS NOT NULL"
                )
                for stream, epoch in cur.fetchall():
                    last_success.add_metric([stream], float(epoch))
                yield last_success

                rejected = GaugeMetricFamily(
                    f"{NS}_ingest_rejected_rows",
                    "Rows the pre-load validation gate rejected, by stream.",
                    labels=["stream"],
                )
                cur.execute(
                    "SELECT stream_name, count(*) FROM ops.ingest_reject GROUP BY stream_name"
                )
                for stream, count in cur.fetchall():
                    rejected.add_metric([stream], float(count))
                yield rejected

                runs = GaugeMetricFamily(
                    f"{NS}_ingest_runs_total",
                    "Ingestion runs recorded, by stream and status.",
                    labels=["stream", "status"],
                )
                cur.execute(
                    "SELECT stream_name, status, count(*) FROM ops.ingest_run "
                    "GROUP BY stream_name, status"
                )
                for stream, status, count in cur.fetchall():
                    runs.add_metric([stream, status], float(count))
                yield runs

                source_rows = GaugeMetricFamily(
                    f"{NS}_source_rows",
                    "Rows in the OLTP source tables. The denominator for CDC parity.",
                    labels=["table"],
                )
                for table in ("country", "indicator", "observation"):
                    cur.execute(f"SELECT count(*) FROM wb.{table}")  # noqa: S608 - fixed list
                    source_rows.add_metric([f"wb.{table}"], float(cur.fetchone()[0]))
                yield source_rows
        except Exception as exc:  # noqa: BLE001
            log.warning("postgres collection failed: %s", exc)
        finally:
            conn.close()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )
    port = int(os.environ.get("EXPORTER_PORT", "9108"))

    registry = CollectorRegistry()
    registry.register(PipelineCollector())

    # A plain WSGI app on its own port. Deliberately not mounted under an ASGI app:
    # Mount("/metrics") issues a 307 redirect to /metrics/, Prometheus does not follow
    # redirects, and the target then reads as down with nothing useful in the logs.
    app = make_wsgi_app(registry)
    with make_server("0.0.0.0", port, app) as httpd:  # noqa: S104 - container-internal
        log.info("pipeline metrics exporter listening on :%d/metrics", port)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            log.info("shutting down")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
