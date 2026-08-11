#!/usr/bin/env python3
"""Assert both DAGs import cleanly and satisfy this project's DAG conventions.

Runs inside the Airflow image. Mirrors the `dags` job in .github/workflows/ci-cd.yml.

Checks more than importability, because a DAG with an import error does not appear in the UI at
all: the failure mode is a pipeline that silently is not there. Explicit exits rather than
asserts, since asserts vanish under `python -O`.
"""

from __future__ import annotations

import sys

from airflow.models import DagBag

EXPECTED = {"wb_cdc_pipeline", "cdc_health_monitor"}


def main() -> int:
    bag = DagBag("/opt/airflow/dags", include_examples=False)
    failures: list[str] = []

    for path, err in (bag.import_errors or {}).items():
        failures.append(f"import error in {path}: {err}")

    found = set(bag.dag_ids)
    missing = EXPECTED - found
    if missing:
        failures.append(f"missing DAGs: {sorted(missing)}")

    for dag_id in sorted(found):
        dag = bag.get_dag(dag_id)
        # Every DAG needs a watcher. `end` uses all_done so the graph has one clean leaf, but
        # all_done also succeeds when upstream tasks failed, so a DAG without a one_failed leaf
        # reports a green run for a pipeline that broke.
        if not [t for t in dag.tasks if t.trigger_rule == "one_failed"]:
            failures.append(f"{dag_id}: no one_failed watcher task")
        if not dag.doc_md:
            failures.append(f"{dag_id}: no doc_md, so its UI page carries no runbook")
        if not failures:
            print(f"  {dag_id}: {len(dag.tasks)} tasks, watcher present, documented")

    if failures:
        for failure in failures:
            print(f"  FAIL {failure}", file=sys.stderr)
        return 1

    print(f"OK: {len(found)} DAGs, no import errors")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
