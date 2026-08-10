"""
End-to-end pipeline: public REST API -> PostgreSQL -> CDC -> ClickHouse -> dbt marts.

    start
      |
      +-- [ingest_dimensions] ------------------+
      |     ingest_country                      |   dimensions first: the fact table has
      |     ingest_indicator                    |   foreign keys to both, and the
      +-----------------------------------------+   ingestion gate checks referential
      |                                             integrity against them
      v
    ingest_observations                             2,970 rows, change-detecting upsert
      |
      v
    gate_cdc_parity                                 WAIT for ClickHouse to match Postgres
      |                                             (Debezium is a service, not a task)
      +--> gate_cdc_lag                             connector liveness, via the heartbeat
      +--> gate_source_health                       replication slot is not hoarding WAL
      |
      v
    dbt_build_staging                               models + tests, tag:staging
      |
      v
    dbt_build_marts                                 models + tests, tag:marts
      |
      v
    gate_marts_reconcile                            fact == staging, feature grain unique
      |
      +--> end        (trigger_rule=all_done)       a JOIN, not a verdict
      +--> watcher    (trigger_rule=one_failed)     the verdict

WHY THERE IS NO "run_cdc" TASK
    Debezium runs continuously. A DAG cannot orchestrate it without pretending a stream
    is a batch, so this DAG does not try. What it does instead is refuse to build models
    on a warehouse that has not caught up: gate_cdc_parity blocks until ClickHouse
    matches the source, and gate_cdc_lag asserts the connector is actually alive. That
    is the honest representation of a streaming leg inside a scheduled graph.

WHY THE WATCHER EXISTS
    `end` uses trigger_rule=all_done so the graph always has a single leaf and the run
    terminates cleanly. But all_done succeeds even when upstream tasks failed, so a DAG
    whose only leaf is `end` reports SUCCESS for a run in which everything broke. The
    watcher is a second leaf with trigger_rule=one_failed downstream of every task: it
    can only run if something failed, and by failing itself it makes the DAG run's state
    tell the truth.

FAILURE SEMANTICS
    Ingestion tasks retry twice: they cross a network to a free public API that
    intermittently returns HTTP 400 with an HTML body for valid requests, and the client
    already treats that as transient. Gates do NOT retry, because a gate failing means a
    real condition is unmet and retrying only delays the alert.

RUNBOOK
    gate_cdc_parity failed      compare the topic high watermark against the landing
                                table count; `make verify` prints both sides
    gate_cdc_lag failed         check the connector status, then
                                system.kafka_consumers for a stalled consumer
    gate_source_health failed   `make drop-slot` if the slot is inactive
    dbt_build_* failed          the task log holds the failing model or test by name
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from dags.utils import gates
from dags.utils.observability import emit

log = logging.getLogger(__name__)

DBT_BIN = os.environ.get("DBT_BIN", "/opt/dbt-venv/bin/dbt")
DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    # Every task gets a ceiling. Without one, a hung network call holds a slot
    # indefinitely and the only symptom is a DAG that never finishes.
    "execution_timeout": timedelta(minutes=30),
}


def _run_ingest(stream: str, **_context) -> None:
    """Run one ingestion stream in-process.

    In-process rather than shelling out, so a failure surfaces as a Python traceback in
    the task log rather than as "exit code 2" from a subprocess.
    """
    from ingest.run import main

    emit(log, "ingest_start", stream=stream, mode=os.environ.get("SOURCE_API_MODE"))
    code = main(["--stream", stream])
    if code != 0:
        # Exit code 2 is the ingestion CLI's signal for a quality-gate failure, as
        # distinct from a crash. Preserved here so the distinction survives into the
        # task log: a gate failure means look at the source, a traceback means look at
        # the code.
        raise RuntimeError(
            f"ingestion of {stream} failed with exit code {code} "
            f"({'quality gate' if code == 2 else 'error'}); see the log above"
        )
    emit(log, "ingest_complete", stream=stream)


def _gate(name: str, fn, **_context) -> dict:
    emit(log, "gate_start", gate=name)
    result = fn()
    emit(log, "gate_passed", gate=name, **result if isinstance(result, dict) else {})
    return result


with DAG(
    dag_id="wb_cdc_pipeline",
    description="Public REST API to ClickHouse marts, via PostgreSQL and Debezium CDC",
    default_args=DEFAULT_ARGS,
    # Hourly is far more often than annual statistics change. That is deliberate: the
    # pipeline's job is to notice a revision promptly, and the change-detecting upsert
    # means a run that finds nothing new writes nothing and emits no CDC events, so a
    # frequent schedule is close to free. `make verify` shows unchanged=2970 after a
    # no-op run.
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    # No backfill. This pipeline reads current state from an API that does not accept a
    # date parameter, so every historical run would fetch identical data. catchup=True
    # here would queue hundreds of identical runs on first deploy.
    catchup=False,
    # One run at a time. Two concurrent runs would both upsert the same natural keys and
    # both build the same models: correct, because every step is idempotent, but a waste
    # of a source that is slow and free.
    max_active_runs=1,
    # Active on arrival. Airflow pauses new DAGs by default, so a reviewer would
    # otherwise find two DAGs that can never run and need to know to toggle them.
    is_paused_upon_creation=False,
    tags=["cdc", "clickhouse", "dbt", "elt"],
    # Publishes this module's docstring, including the ASCII graph and the runbook, onto
    # the DAG's page in the UI. The person debugging at 2am is already looking at that
    # screen.
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    # Dimensions before facts. Not cosmetic: wb.observation has foreign keys to both
    # dimensions, and the ingestion gate checks referential integrity against what is
    # actually in the database. Running observations first would reject every row.
    with TaskGroup(group_id="ingest_dimensions") as ingest_dimensions:
        for stream in ("country", "indicator"):
            PythonOperator(
                task_id=f"ingest_{stream}",
                python_callable=_run_ingest,
                op_kwargs={"stream": stream},
                doc_md=f"Fetch the {stream} dimension and upsert it. Unchanged rows are "
                       f"not rewritten, so no spurious CDC events are emitted.",
            )

    ingest_observations = PythonOperator(
        task_id="ingest_observations",
        python_callable=_run_ingest,
        op_kwargs={"stream": "observation"},
        # The slow one: 36 paginated requests against a free public API. Given a
        # generous ceiling because the upstream latency is not ours to control.
        execution_timeout=timedelta(minutes=45),
        doc_md="Fetch 2,970 observations. Asserts per-indicator completeness, which is "
               "the only check that catches an archived indicator still serving metadata.",
    )

    gate_parity = PythonOperator(
        task_id="gate_cdc_parity",
        python_callable=_gate,
        op_kwargs={"name": "cdc_parity", "fn": gates.gate_cdc_parity},
        retries=0,  # a gate failing is a real condition; retrying only delays the alert
        doc_md="Block until ClickHouse matches PostgreSQL. This is how a continuous CDC "
               "leg is represented honestly inside a scheduled graph.",
    )

    gate_lag = PythonOperator(
        task_id="gate_cdc_lag",
        python_callable=_gate,
        op_kwargs={"name": "cdc_lag", "fn": gates.gate_cdc_lag},
        retries=0,
        doc_md="Connector liveness, measured against the Debezium heartbeat rather than "
               "a business table, so an idle dimension cannot look like a stall.",
    )

    gate_source = PythonOperator(
        task_id="gate_source_health",
        python_callable=_gate,
        op_kwargs={"name": "source_health", "fn": gates.gate_source_health},
        retries=0,
        doc_md="Assert the replication slot is not hoarding WAL. A pipeline that "
               "degrades its own source is worse than one that stops.",
    )

    # dbt build, not dbt run: build interleaves each model with its tests, so a failing
    # staging test stops the marts from being built on top of bad data. `dbt run`
    # followed by `dbt test` would build everything first and report the problem after
    # the damage was already materialised.
    dbt_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=(
            f"{DBT_BIN} build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} "
            f"--select tag:staging --no-write-json"
        ),
        doc_md="Models and tests for the staging layer, together.",
    )

    dbt_marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command=(
            f"{DBT_BIN} build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} "
            f"--select tag:marts --no-write-json"
        ),
        doc_md="Dimensions, the incremental fact, and the ML feature table, with tests.",
    )

    gate_marts = PythonOperator(
        task_id="gate_marts_reconcile",
        python_callable=_gate,
        op_kwargs={"name": "marts_reconcile", "fn": gates.gate_marts_reconcile},
        retries=0,
        doc_md="Re-assert the two most important dbt tests in the DAG's critical path, "
               "so the guarantee holds even if someone ran `dbt run` without tests.",
    )

    # A join, not a verdict. all_done means this runs however the upstream went, which
    # is what gives the graph a single clean leaf.
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # The verdict. one_failed means this task can only run if something upstream failed,
    # and it fails on purpose so the DAG RUN is marked failed. Without it, `end`
    # succeeding on all_done would report a green run for a pipeline that broke.
    #
    # retries=0 because there is nothing to retry, and it must not be delayed.
    watcher = BashOperator(
        task_id="watcher",
        bash_command=(
            'echo "A task in this run failed; failing the DAG run deliberately. '
            'See the upstream task logs." >&2; exit 1'
        ),
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=0,
        doc_md="Makes the DAG run's state tell the truth. `end` uses all_done so the "
               "graph has one leaf, but all_done also succeeds when upstream tasks "
               "failed. This second leaf can only run when something failed.",
    )

    critical_path = [
        start,
        ingest_dimensions,
        ingest_observations,
        gate_parity,
        dbt_staging,
        dbt_marts,
        gate_marts,
    ]
    # Sequential critical path.
    for upstream, downstream in zip(critical_path, critical_path[1:]):
        upstream >> downstream

    # The two health gates run in parallel with the transformation, because neither
    # blocks it: they assert conditions about the platform rather than produce data. They
    # still gate the run's verdict, via the watcher.
    gate_parity >> [gate_lag, gate_source]

    gate_marts >> end
    [gate_lag, gate_source] >> end

    # Every task feeds the watcher, so a failure anywhere is caught. Without this, an
    # upstream_failed chain would skip the watcher and the run would read green.
    for task in dag.tasks:
        if task.task_id not in ("watcher", "end"):
            task >> watcher
