#!/usr/bin/env python3
"""Record live API responses as committed test fixtures.

Run this when the source shape changes, not on every test run:

    python scripts/record_fixtures.py

Each fixture file holds the ORDERED LIST of page payloads for one logical stream,
so replaying it exercises the pagination loop instead of bypassing it. That matters
because pagination is where the awkward behaviour lives, and a fixture that returns
everything in one page would test none of it.

Two fixtures are recorded deliberately as failure cases, because the failures are
as much a part of this source's contract as the successes:
  indicator__IC.REG.DURS.json  a retired id, which returns HTTP 200 with an error
                               envelope rather than an error status
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "api"

BASE = "https://api.worldbank.org/v2"
PER_PAGE = 100

# The catalogue is read from ingest/indicators.yml rather than duplicated here, so
# adding an indicator to the config cannot leave the fixtures behind. A fixture set
# that covers only part of the configured catalogue would make fixture mode fail the
# per-indicator completeness check, which is a confusing way to discover a stale
# recording.
sys.path.insert(0, str(REPO_ROOT))
from ingest.settings import get_catalogue  # noqa: E402

_CATALOGUE = get_catalogue()
COUNTRIES = _CATALOGUE.country_path
FIXTURE_INDICATORS = [indicator.id for indicator in _CATALOGUE.indicators]

# Recorded so the tests can assert on the error path using the real payload rather
# than a hand-written approximation of it. Deliberately NOT in indicators.yml: this
# id is archived, so its metadata endpoint returns a valid record while its data
# endpoint returns an error envelope. Both halves are recorded, because the gap
# between them is exactly what the completeness check exists to catch.
ARCHIVED_INDICATOR = "IC.REG.DURS"


def slug(path: str) -> str:
    return path.strip("/").replace("/", "__").replace(";", "-")


def fetch_pages(client: httpx.Client, path: str, max_pages: int = 50) -> list:
    """Fetch every page of a path, stopping the same way the client does."""
    pages: list = []
    page = 1
    total_pages: int | None = None
    while page <= max_pages:
        response = client.get(path, params={"format": "json", "per_page": PER_PAGE, "page": page})
        # utf-8-sig: some responses carry a BOM.
        try:
            payload = json.loads(response.content.decode("utf-8-sig"))
        except json.JSONDecodeError:
            print(f"  page {page}: HTTP {response.status_code}, non-JSON body, retrying once")
            response = client.get(
                path, params={"format": "json", "per_page": PER_PAGE, "page": page}
            )
            payload = json.loads(response.content.decode("utf-8-sig"))

        pages.append(payload)

        # An error envelope is a complete recording on its own.
        if isinstance(payload, list) and len(payload) == 1:
            break
        rows = payload[1] if len(payload) > 1 else None
        if not rows:
            break
        if total_pages is None:
            total_pages = payload[0].get("pages", 1)
        if page >= (total_pages or 1):
            break
        page += 1
    return pages


def write(path: str, pages: list) -> None:
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    target = FIXTURE_DIR / f"{slug(path)}.json"
    target.write_text(json.dumps(pages, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    rows = sum(len(p[1]) for p in pages if isinstance(p, list) and len(p) > 1 and p[1])
    size_kb = target.stat().st_size / 1024
    print(f"  wrote {target.name}: {len(pages)} page(s), {rows} row(s), {size_kb:.0f} KB")


def main() -> int:
    paths = [f"country/{COUNTRIES}"]
    paths += [f"indicator/{i}" for i in FIXTURE_INDICATORS]
    paths += [f"country/{COUNTRIES}/indicator/{i}" for i in FIXTURE_INDICATORS]
    # Both halves of the archived-indicator case.
    paths += [
        f"indicator/{ARCHIVED_INDICATOR}",
        f"country/{COUNTRIES}/indicator/{ARCHIVED_INDICATOR}",
    ]

    with httpx.Client(
        base_url=BASE,
        timeout=60.0,
        follow_redirects=True,
        headers={"User-Agent": "wb-cdc-analytics/1.0 (fixture recorder)"},
    ) as client:
        for path in paths:
            print(f"recording {path}")
            try:
                write(path, fetch_pages(client, path))
            except Exception as exc:
                print(f"  FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
                return 1
    print("\nfixtures recorded. Run: make demo-offline")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
