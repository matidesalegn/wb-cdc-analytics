"""Shared pytest configuration.

The unit tests deliberately need no containers, no network and no database. That
is what lets the CI fast lane give a verdict in under a minute, and it is why the
API fixtures are committed rather than fetched.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "api"

# Import the pipeline package from the repository root without requiring an
# editable install.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# The settings object requires passwords with no defaults, on purpose: a database
# password that silently falls back to a default is how a pipeline connects
# somewhere nobody intended. Unit tests therefore supply throwaway values rather
# than the module relaxing its own contract for testability.
import os  # noqa: E402

os.environ.setdefault("POSTGRES_PASSWORD", "test-only-not-a-real-secret")
os.environ.setdefault("CLICKHOUSE_PASSWORD", "test-only-not-a-real-secret")


def load_fixture(name: str) -> list:
    """Load a recorded fixture by its slugged filename stem."""
    return json.loads((FIXTURE_DIR / f"{name}.json").read_text(encoding="utf-8"))


@pytest.fixture
def countries_pages() -> list:
    return load_fixture("country__TCD-ETH-KEN-RWA-SSD")


@pytest.fixture
def observation_pages() -> list:
    """Four pages, 330 rows. Exercises the pagination loop rather than bypassing it."""
    return load_fixture("country__TCD-ETH-KEN-RWA-SSD__indicator__IC.BUS.NREG")


@pytest.fixture
def archived_indicator_data_page() -> list:
    """The real error-envelope payload for an ARCHIVED indicator's data series.

    IC.REG.DURS is the live example, and the distinction it demonstrates is subtle
    enough to be worth two fixtures:

      GET /indicator/IC.REG.DURS                    -> 200, valid metadata
      GET /country/{list}/indicator/IC.REG.DURS      -> 200, error envelope:
          "The indicator was not found. It may have been deleted or archived."

    So the catalogue still describes the series while the series itself is gone.
    Validating the metadata endpoint alone would never detect it. Only fetching the
    data does, which is why the completeness assertion lives on the observation
    stream.
    """
    return load_fixture("country__TCD-ETH-KEN-RWA-SSD__indicator__IC.REG.DURS")


@pytest.fixture
def archived_indicator_metadata_page() -> list:
    """The same archived indicator's metadata, which is valid and looks healthy."""
    return load_fixture("indicator__IC.REG.DURS")


@pytest.fixture
def settings():
    from ingest.settings import Settings

    return Settings(
        postgres_password="test-only-not-a-real-secret",
        clickhouse_password="test-only-not-a-real-secret",
        wb_per_page=100,
        wb_max_retries=3,
    )
