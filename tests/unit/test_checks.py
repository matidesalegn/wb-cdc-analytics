"""Tests for the pre-load validation gate.

This is the gate that stands in for a Great Expectations suite (see the module
docstring in ingest/checks.py and the design report for the reasoning). These tests
are what make that substitution defensible: the gate is asserted to actually
reject the things it claims to reject.
"""

from __future__ import annotations

import pytest

from ingest.checks import (
    GateFailure,
    assert_batch_acceptable,
    gate_countries,
    gate_indicators,
    gate_observations,
)

ALLOWED_COUNTRIES = {"TCD", "ETH", "KEN", "RWA", "SSD"}
ALLOWED_INDICATORS = {"IC.BUS.NREG", "SP.POP.TOTL"}


def obs(country="ETH", indicator="IC.BUS.NREG", year="2020", value=1.0):
    return {
        "countryiso3code": country,
        "indicator": {"id": indicator},
        "country": {"id": country[:2]},
        "date": year,
        "value": value,
        "decimal": 0,
    }


class TestCountryGate:
    def test_accepts_the_recorded_batch(self, countries_pages):
        result = gate_countries(countries_pages[0][1], ALLOWED_COUNTRIES)
        assert len(result.accepted) == 5
        assert result.rejected == []

    def test_rejects_a_country_outside_the_configured_set(self):
        """An aggregate or region row in a country dimension double-counts silently.

        The country endpoint will happily return one if it is requested by mistake,
        and nothing downstream would notice: the aggregate has a plausible name and a
        real income level.
        """
        rows = [{"id": "WLD", "name": "World", "iso2Code": "1W"}]
        result = gate_countries(rows, ALLOWED_COUNTRIES)
        assert result.accepted == []
        assert "not in the configured country set" in result.rejected[0].reason

    def test_rejects_an_out_of_range_latitude(self):
        rows = [{"id": "ETH", "name": "Ethiopia", "latitude": "999", "longitude": "38"}]
        result = gate_countries(rows, ALLOWED_COUNTRIES)
        assert "latitude" in result.rejected[0].reason

    def test_rejects_an_in_batch_duplicate(self):
        """The upsert would collapse duplicates silently.

        Without this check, a source that started returning a key twice would be
        indistinguishable from one that did not, so a real upstream regression would
        never surface.
        """
        row = {"id": "ETH", "name": "Ethiopia"}
        result = gate_countries([row, dict(row)], ALLOWED_COUNTRIES)
        assert len(result.accepted) == 1
        assert "duplicate" in result.rejected[0].reason

    def test_a_contract_failure_is_recorded_not_raised(self):
        # One malformed row must not lose the whole batch.
        result = gate_countries(
            [{"id": "ETH", "name": "Ethiopia"}, {"name": "no id here"}], ALLOWED_COUNTRIES
        )
        assert len(result.accepted) == 1
        assert len(result.rejected) == 1
        assert result.rejected[0].reason.startswith("contract:")


class TestIndicatorGate:
    def test_rejects_an_unconfigured_indicator(self):
        rows = [{"id": "SOMETHING.ELSE", "name": "Other"}]
        result = gate_indicators(rows, ALLOWED_INDICATORS)
        assert result.accepted == []
        assert "not in the configured set" in result.rejected[0].reason


class TestObservationGate:
    def test_accepts_valid_rows(self):
        result = gate_observations(
            [(None, obs())], ALLOWED_COUNTRIES, ALLOWED_INDICATORS
        )
        assert len(result.accepted) == 1

    def test_rejects_a_country_with_no_dimension_row(self):
        """Referential integrity, checked upstream of the foreign key.

        The database would reject it too, but a foreign-key violation aborts the
        whole transaction and loses every good row alongside the one bad one.
        Rejecting here isolates the bad row.
        """
        result = gate_observations(
            [(None, obs(country="ZWE"))], ALLOWED_COUNTRIES, ALLOWED_INDICATORS
        )
        assert result.accepted == []
        assert "no matching dimension row" in result.rejected[0].reason

    def test_rejects_an_impossible_year(self):
        result = gate_observations(
            [(None, obs(year="1850"))], ALLOWED_COUNTRIES, ALLOWED_INDICATORS
        )
        assert "outside" in result.rejected[0].reason

    def test_rejects_a_non_finite_value(self):
        """NaN compares false to everything, so it defeats equality AND range tests.

        It survives a JSON round trip through some encoders, and downstream it looks
        like a value rather than like an error.
        """
        result = gate_observations(
            [(None, obs(value=float("nan")))], ALLOWED_COUNTRIES, ALLOWED_INDICATORS
        )
        assert "not finite" in result.rejected[0].reason

    def test_accepts_a_null_value(self):
        # A null is "not measured that year", which is legitimate and must pass.
        result = gate_observations(
            [(None, obs(value=None))], ALLOWED_COUNTRIES, ALLOWED_INDICATORS
        )
        assert len(result.accepted) == 1
        assert result.accepted[0].obs_value is None

    def test_rejects_a_duplicate_natural_key(self):
        result = gate_observations(
            [(None, obs()), (None, obs())], ALLOWED_COUNTRIES, ALLOWED_INDICATORS
        )
        assert len(result.accepted) == 1
        assert "duplicate natural key" in result.rejected[0].reason

    def test_the_whole_recorded_stream_passes(self, observation_pages):
        rows = [
            (None, r) for page in observation_pages for r in (page[1] or [])
        ]
        result = gate_observations(rows, ALLOWED_COUNTRIES, ALLOWED_INDICATORS)
        assert len(result.accepted) == 330
        assert result.rejected == []


class TestBatchAssertions:
    def test_empty_batch_fails(self):
        """Zero rows must be an error, not a quiet success.

        Zero rows is indistinguishable from a run that had nothing to do, and the
        most likely cause is an error envelope returned with HTTP 200.
        """
        result = gate_countries([], ALLOWED_COUNTRIES)
        with pytest.raises(GateFailure, match="at least"):
            assert_batch_acceptable("country", result, min_rows=1)

    def test_a_mostly_rejected_batch_fails(self):
        """A source whose shape changed would otherwise load a small, unrepresentative
        slice and report success."""
        rows = [{"id": "WLD", "name": "World"} for _ in range(10)]
        rows.append({"id": "ETH", "name": "Ethiopia"})
        result = gate_countries(rows, ALLOWED_COUNTRIES)
        with pytest.raises(GateFailure, match="rejected"):
            assert_batch_acceptable("country", result, min_rows=1)

    def test_a_few_rejections_are_tolerated(self):
        rows = [{"id": c, "name": c} for c in sorted(ALLOWED_COUNTRIES)] * 10
        rows.append({"id": "WLD", "name": "World"})
        result = gate_countries(rows, ALLOWED_COUNTRIES)
        # Duplicates dominate the rejections here, so raise the tolerance to isolate
        # the behaviour under test: a small reject fraction must not fail the batch.
        assert_batch_acceptable("country", result, min_rows=1, max_reject_fraction=0.99)

    def test_failure_message_includes_a_sample_reason(self):
        rows = [{"id": "WLD", "name": "World"} for _ in range(10)]
        result = gate_countries(rows, ALLOWED_COUNTRIES)
        with pytest.raises(GateFailure) as exc:
            assert_batch_acceptable("country", result, min_rows=1)
        assert "configured country set" in str(exc.value)
