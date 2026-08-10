#!/usr/bin/env python3
"""Assert a rendered Kafka Connect payload on stdin is valid and fully substituted.

Deliberately not written with `assert`. A CI gate built on assert statements is silently
disabled by `python -O`, which is the worst possible failure mode for a check: it reports
success without checking anything. Each condition below fails loudly with an exit code CI can
read, and a message that names the consequence rather than just the condition.
"""

from __future__ import annotations

import json
import sys

FAILURES: list[str] = []


def require(condition: bool, message: str) -> None:
    """Record a failure rather than raising, so one run reports every problem."""
    if not condition:
        FAILURES.append(message)


def main() -> int:
    try:
        cfg = json.load(sys.stdin)
    except json.JSONDecodeError as exc:
        print(f"rendered payload is not valid JSON: {exc}", file=sys.stderr)
        return 1

    require(
        all(isinstance(v, str) for v in cfg.values()),
        "Connect config values must all be strings: the config endpoint takes a flat string map",
    )
    require(
        not any(k.startswith("//") for k in cfg),
        "documentation keys were not stripped, and Connect will reject them",
    )
    require(
        "${" not in json.dumps(cfg),
        "an unresolved ${VAR} placeholder survived: the connector would register with a "
        "literal placeholder as its password and fail later with an authentication error "
        "that never mentions the real cause",
    )
    require(
        bool(cfg.get("topic.prefix")),
        "topic.prefix missing: it is the Debezium 3.x replacement for database.server.name",
    )
    require(
        cfg.get("plugin.name") == "pgoutput",
        "plugin.name must be pinned to pgoutput: the default, decoderbufs, needs a server "
        "extension that is not installed",
    )
    require(
        "wb.cdc_heartbeat" in cfg.get("table.include.list", ""),
        "the heartbeat table must be captured, or CDC lag cannot be measured against "
        "connector liveness and an idle dimension looks like a stall",
    )

    if FAILURES:
        for failure in FAILURES:
            print(f"  FAIL {failure}", file=sys.stderr)
        return 1

    print(f"connector config OK: {len(cfg)} properties, no placeholders left")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
