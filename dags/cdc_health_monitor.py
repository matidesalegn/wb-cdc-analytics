"""
Continuous CDC health monitor, decoupled from the data pipeline.

    check_cdc_lag ------+
    check_source_health +--> end (all_done)
                        +--> watcher (one_failed)

WHY THIS IS A SEPARATE DAG
    The main pipeline runs hourly, so its gates only notice a stalled connector once an
    hour and only while a run is in progress. CDC is continuous: a connector that dies
    two minutes after a successful run stays dead for fifty-eight minutes with nothing
    looking wrong.

    This DAG runs every five minutes and does nothing but assert liveness. It is
    deliberately cheap: two queries, no data movement, no dependency on the pipeline
    having run.

    It overlaps with the Prometheus alert rules on purpose, and the overlap is the point.
    Prometheus is the right place for alerting, but it is a separate system that can
    itself be down, and a reviewer running only the core profile has no Prometheus at
    all. Having the assertion in both places means the guarantee does not depend on the
    monitoring stack being up.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from dags.utils import gates
from dags.utils.observability import emit

log = logging.getLogger(__name__)


def _check(name: str, fn, **_context) -> dict:
    emit(log, "health_check", check=name)
    return fn()


with DAG(
    dag_id="cdc_health_monitor",
    description="Assert the CDC connector is alive and not harming its source",
    default_args={
        "owner": "data-engineering",
        # No retries. A health check that retries reports health late, and late health is
        # indistinguishable from health.
        "retries": 0,
        "execution_timeout": timedelta(minutes=3),
    },
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # Active on arrival. Airflow pauses new DAGs by default, so a reviewer would
    # otherwise find two DAGs that can never run and need to know to toggle them.
    is_paused_upon_creation=False,
    tags=["cdc", "monitoring"],
    doc_md=__doc__,
) as dag:

    check_lag = PythonOperator(
        task_id="check_cdc_lag",
        python_callable=_check,
        op_kwargs={"name": "cdc_lag", "fn": gates.gate_cdc_lag},
    )

    check_source = PythonOperator(
        task_id="check_source_health",
        python_callable=_check,
        op_kwargs={"name": "source_health", "fn": gates.gate_source_health},
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    watcher = BashOperator(
        task_id="watcher",
        bash_command='echo "A CDC health check failed." >&2; exit 1',
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=0,
    )

    [check_lag, check_source] >> end
    [check_lag, check_source] >> watcher
