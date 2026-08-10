#!/usr/bin/env python3
"""Render a Debezium connector config for registration.

Does two things to the committed JSON:

1. Drops every key beginning with "//". Kafka Connect's config endpoint takes a
   flat map of string to string, so it cannot accept the explanatory arrays the
   committed file uses to document each decision. Keeping the documentation next
   to the configuration it explains is worth a five-line filter here; the
   alternative is an undocumented wall of properties or a second file that drifts
   out of step with the first.

2. Substitutes ${VAR} references from the environment. This is what keeps the
   replication password out of the repository while leaving the connector config
   itself committed and reviewable, which the deliverable asks for.

Fails loudly on an unresolved placeholder. A connector registered with a literal
"${PG_REPL_PASSWORD}" as its password fails later, during the connection attempt,
with an authentication error that does not mention the real cause.

Standard library only, so it runs anywhere the rest of the bootstrap does.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

PLACEHOLDER = re.compile(r"\$\{([A-Z0-9_]+)\}")


def substitute(value: str, env: dict[str, str], missing: set[str]) -> str:
    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in env or env[name] == "":
            missing.add(name)
            return match.group(0)
        return env[name]

    return PLACEHOLDER.sub(replace, value)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: render_connector.py <connector.json>", file=sys.stderr)
        return 2

    source = Path(sys.argv[1])
    raw = json.loads(source.read_text(encoding="utf-8"))

    env = dict(os.environ)
    missing: set[str] = set()
    config: dict[str, str] = {}

    for key, value in raw.items():
        if key.startswith("//"):
            continue
        if not isinstance(value, str):
            print(
                f"error: {source}: key {key!r} must be a string, got "
                f"{type(value).__name__}. Connect config values are strings.",
                file=sys.stderr,
            )
            return 1
        config[key] = substitute(value, env, missing)

    if missing:
        print(
            "error: unresolved placeholders in "
            f"{source}: {', '.join(sorted(missing))}. "
            "Run 'make env' and re-run bootstrap.",
            file=sys.stderr,
        )
        return 1

    json.dump(config, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
