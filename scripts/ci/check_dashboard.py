#!/usr/bin/env python3
"""Assert the Grafana dashboard is valid, provisionable, and self-documenting.

Explicit checks rather than `assert`, because assert statements are stripped under
`python -O`, which would leave this gate reporting success without checking anything.
"""

from __future__ import annotations

import json
import pathlib
import sys

DASHBOARD = pathlib.Path("observability/grafana/dashboards/pipeline-health.json")
MIN_PANELS = 6


def main() -> int:
    try:
        dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"cannot read {DASHBOARD}: {exc}", file=sys.stderr)
        return 1

    failures: list[str] = []
    panels = [p for p in dashboard.get("panels", []) if p.get("type") != "row"]

    if not dashboard.get("uid"):
        # Without a stable uid, provisioning creates a duplicate dashboard on every restart
        # instead of updating the existing one.
        failures.append("dashboard has no stable uid, so provisioning would duplicate it")

    if len(panels) < MIN_PANELS:
        failures.append(f"expected at least {MIN_PANELS} panels, found {len(panels)}")

    undocumented = [p.get("title", "<untitled>") for p in panels if not p.get("description")]
    if undocumented:
        # A panel without a description is a number nobody can act on.
        failures.append(f"panels missing a description: {undocumented}")

    if failures:
        for failure in failures:
            print(f"  FAIL {failure}", file=sys.stderr)
        return 1

    queries = sum(len(p.get("targets", [])) for p in panels)
    print(f"dashboard OK: {len(panels)} panels, {queries} queries, all documented")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
