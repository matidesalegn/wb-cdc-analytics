#!/usr/bin/env python3
"""Generate .env from .env.example, filling in fresh random secrets.

Why this exists: the assessment requires a reviewer to start the stack with one
command, and a repository must never ship a credential. Those two requirements
conflict unless the secrets are generated at setup time. So .env.example is
committed with CHANGEME_ placeholders, .env is gitignored from the first
commit, and this script bridges them.

Refuses to overwrite an existing .env unless --force is passed, so re-running
`make env` after the stack has data cannot silently orphan the volumes by
rotating the database passwords underneath them.

Standard library only. It has to run before any dependency is installed.
"""

from __future__ import annotations

import argparse
import base64
import os
import secrets
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLE = REPO_ROOT / ".env.example"
TARGET = REPO_ROOT / ".env"

# Placeholder -> generator. Keep this table small and explicit; a magic
# "replace anything that looks like a secret" rule would silently miss a new
# variable, and the stack would then start half-configured.
GENERATORS = {
    # URL-safe, no shell-hostile characters. 32 bytes of entropy.
    "CHANGEME_GENERATED": lambda: secrets.token_urlsafe(24),
    # Airflow needs a 32-byte urlsafe-base64 key. This is exactly what
    # cryptography.fernet.Fernet.generate_key() produces, without the import,
    # so `make env` works before the venv exists.
    "CHANGEME_FERNET": lambda: base64.urlsafe_b64encode(os.urandom(32)).decode(),
}


def render(example_text: str) -> tuple[str, int]:
    """Replace every known placeholder with a freshly generated value."""
    out_lines: list[str] = []
    filled = 0
    for line in example_text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            # Strip any trailing inline comment before matching, so a
            # placeholder followed by a comment is still recognised.
            bare = value.split("#", 1)[0].strip()
            if bare in GENERATORS:
                line = f"{key}={GENERATORS[bare]()}"
                filled += 1
        out_lines.append(line)
    return "\n".join(out_lines) + "\n", filled


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="overwrite an existing .env (rotates every generated secret)",
    )
    args = parser.parse_args()

    if not EXAMPLE.is_file():
        print(f"error: {EXAMPLE} not found", file=sys.stderr)
        return 1

    if TARGET.exists() and not args.force:
        print(".env already exists, leaving it alone. Use --force to rotate secrets.")
        return 0

    if TARGET.exists() and args.force:
        print(
            "warning: rotating secrets in an existing .env. If the stack has "
            "running volumes, run `make clean` first or the databases will "
            "reject the new passwords.",
            file=sys.stderr,
        )

    rendered, filled = render(EXAMPLE.read_text(encoding="utf-8"))
    TARGET.write_text(rendered, encoding="utf-8")
    # Owner-only. The file holds every credential in the stack.
    TARGET.chmod(0o600)

    print(f"wrote {TARGET.relative_to(REPO_ROOT)} with {filled} generated secrets")
    print("run `make up` next")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
