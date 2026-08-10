#!/usr/bin/env python3
"""Poll a Kafka Connect connector until it and every task report RUNNING.

Why this is a separate step from registration: a successful PUT means Connect
accepted the configuration, not that the connector works. A task can fail
immediately afterwards on bad credentials, a missing publication, or an
unreachable database, and the connector-level state can still read RUNNING while
its only task has already died. Checking the connector without checking its tasks
is the most common way a broken CDC pipeline reports as healthy.

Exits 0 when everything is RUNNING, 1 on a terminal FAILED state (printing the
first line of the task trace, which is the part that names the actual cause), and
2 on timeout.

Standard library only.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request


def fetch_status(url: str, timeout: float = 5.0) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, TimeoutError):
        return None


def classify(status: dict) -> tuple[str, str]:
    """Return (verdict, detail) where verdict is ok, failed or pending."""
    connector_state = status.get("connector", {}).get("state", "UNKNOWN")
    tasks = status.get("tasks", [])
    task_states = [task.get("state", "UNKNOWN") for task in tasks]

    if connector_state == "FAILED":
        return "failed", status.get("connector", {}).get("trace", "connector FAILED").splitlines()[0]

    for task in tasks:
        if task.get("state") == "FAILED":
            trace = task.get("trace", "")
            first = trace.splitlines()[0] if trace else "task FAILED with no trace"
            return "failed", f"task {task.get('id')}: {first}"

    if connector_state == "RUNNING" and task_states and all(s == "RUNNING" for s in task_states):
        return "ok", f"connector RUNNING, {len(task_states)} task(s) RUNNING"

    return "pending", f"connector={connector_state} tasks={task_states or 'none yet'}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="Connect REST base URL")
    parser.add_argument("--connector", required=True, help="connector name")
    parser.add_argument("--timeout", type=int, default=120, help="seconds to wait")
    parser.add_argument("--interval", type=int, default=5, help="poll interval")
    args = parser.parse_args()

    status_url = f"{args.url.rstrip('/')}/connectors/{args.connector}/status"
    deadline = time.monotonic() + args.timeout

    while time.monotonic() < deadline:
        status = fetch_status(status_url)
        if status is None:
            print("    Connect REST not answering yet", flush=True)
        else:
            verdict, detail = classify(status)
            if verdict == "ok":
                print(f"    {detail}")
                return 0
            if verdict == "failed":
                print(f"    ERROR: {detail}", file=sys.stderr)
                print(f"    full status: curl -s {status_url}", file=sys.stderr)
                return 1
            print(f"    {detail}", flush=True)
        time.sleep(args.interval)

    print(f"    ERROR: connector did not reach RUNNING within {args.timeout}s", file=sys.stderr)
    print(f"    full status: curl -s {status_url}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
