"""Tests for the source boundary contract.

Every test here corresponds to a behaviour measured against the live API and
recorded in docs/source-api-notes.md. They are regression tests for real traps, not
coverage for its own sake.
"""

from __future__ import annotations

import math

import pytest

from ingest.contracts import (
    CountryRecord,
    IndicatorRecord,
    ObservationRecord,
    SourceContractError,
    clean_text,
    parse_envelope,
    source_hash,
    to_float,
)


class TestEnvelope:
    def test_valid_response_splits_into_meta_and_rows(self, countries_pages):
        meta, rows = parse_envelope(countries_pages[0])
        assert meta.total == 5
        assert len(rows) == 5

    def test_error_envelope_with_http_200_is_rejected(self, archived_indicator_data_page):
        """The single most important check in this module.

        An archived indicator's data endpoint returns HTTP 200 with an error body. A
        client that trusts the status code ingests zero rows and reports success,
        which is indistinguishable from an indicator that genuinely has no data for
        these countries.
        """
        with pytest.raises(SourceContractError, match="error envelope"):
            parse_envelope(archived_indicator_data_page[0])

    def test_error_envelope_reason_names_the_cause(self, archived_indicator_data_page):
        with pytest.raises(SourceContractError) as exc:
            parse_envelope(archived_indicator_data_page[0])
        # The message has to be actionable, because this is what lands in a log at
        # 2am. "The API call failed" is not.
        assert "deleted or archived" in str(exc.value)

    def test_an_archived_indicators_metadata_still_looks_healthy(
        self, archived_indicator_metadata_page
    ):
        """The reason the completeness check cannot live on the metadata stream.

        The catalogue endpoint still serves a full, valid record for a series whose
        data has been withdrawn. Validating metadata alone would report the pipeline
        healthy while an entire indicator was silently missing from the facts.
        """
        meta, rows = parse_envelope(archived_indicator_metadata_page[0])
        assert meta.total == 1
        record = IndicatorRecord.from_api(rows[0])
        assert record.indicator_id == "IC.REG.DURS"
        assert record.name  # a real, human-readable name, not a placeholder

    def test_null_rows_are_treated_as_empty(self):
        """A page past the end returns rows as null on some endpoints, [] on others."""
        meta, rows = parse_envelope([{"page": 9, "pages": 1, "per_page": 100, "total": 0}, None])
        assert rows == []

    def test_non_list_payload_is_rejected(self):
        with pytest.raises(SourceContractError, match="top level"):
            parse_envelope({"unexpected": "object"})

    def test_short_array_is_rejected(self):
        with pytest.raises(SourceContractError, match="length 1"):
            parse_envelope([{"page": 1}])

    def test_per_page_accepts_both_string_and_int(self):
        """The API returns per_page as "50" on dimensions and 5 on data endpoints."""
        as_string, _ = parse_envelope([{"page": 1, "pages": 1, "per_page": "50", "total": 0}, []])
        as_int, _ = parse_envelope([{"page": 1, "pages": 1, "per_page": 5, "total": 0}, []])
        assert as_string.per_page == 50
        assert as_int.per_page == 5


class TestNormalisation:
    def test_trailing_whitespace_is_trimmed(self):
        """region.value really does come back as "Sub-Saharan Africa " with a space.

        Untrimmed, that is a second distinct region label that differs only in
        whitespace, and it silently splits any group-by on region.
        """
        assert clean_text("Sub-Saharan Africa ") == "Sub-Saharan Africa"

    def test_empty_string_becomes_none(self):
        """unit and obs_status are "" rather than null when not applicable.

        Stored as empty strings they would make every downstream null check
        meaningless.
        """
        assert clean_text("") is None
        assert clean_text("   ") is None

    def test_string_coordinates_are_coerced(self):
        """latitude and longitude arrive as strings on the country endpoint."""
        assert to_float("38.7468") == pytest.approx(38.7468)

    def test_unparseable_number_returns_none_rather_than_raising(self):
        # One bad coordinate should not fail an otherwise good batch. The gate
        # decides whether a missing value is acceptable for that field.
        assert to_float("not-a-number") is None
        assert to_float("") is None
        assert to_float(None) is None

    def test_booleans_are_not_silently_numbers(self):
        # True would otherwise coerce to 1.0 and look like a real measurement.
        assert to_float(True) is None


class TestSourceHash:
    def test_hash_is_independent_of_key_order(self):
        """Change detection must depend on content, not on dict ordering."""
        assert source_hash({"a": 1, "b": 2}) == source_hash({"b": 2, "a": 1})

    def test_hash_changes_when_a_value_changes(self):
        assert source_hash({"a": 1}) != source_hash({"a": 2})

    def test_hash_is_stable_across_calls(self):
        payload = {"country_id": "ETH", "name": "Ethiopia", "longitude": 38.7468}
        assert source_hash(payload) == source_hash(payload)

    def test_records_built_from_identical_payloads_hash_identically(self, countries_pages):
        """The property that makes the upsert change-detecting.

        If this ever fails, every re-ingest rewrites every row, each no-op update
        emits a CDC event, and the change stream stops describing reality.
        """
        rows = countries_pages[0][1]
        first = CountryRecord.from_api(rows[0])
        again = CountryRecord.from_api(rows[0])
        assert first.source_hash == again.source_hash


class TestCountryRecord:
    def test_parses_a_real_row(self, countries_pages):
        rows = {r["id"]: r for r in countries_pages[0][1]}
        record = CountryRecord.from_api(rows["ETH"])
        assert record.country_id == "ETH"
        assert record.iso2_code == "ET"
        assert record.name == "Ethiopia"
        assert record.capital_city == "Addis Ababa"
        assert record.income_level == "Low income"
        assert record.lending_type == "IDA"
        assert record.latitude == pytest.approx(9.02274)

    def test_nested_blocks_are_flattened(self, countries_pages):
        rows = {r["id"]: r for r in countries_pages[0][1]}
        record = CountryRecord.from_api(rows["KEN"])
        # Kenya is the one country in this set with a Blend lending type, which makes
        # it a useful check that the nested value is really being read rather than a
        # constant sneaking through.
        assert record.lending_type == "Blend"
        assert record.income_level == "Lower middle income"

    def test_region_name_has_no_trailing_space(self, countries_pages):
        record = CountryRecord.from_api(countries_pages[0][1][0])
        assert record.region_name == record.region_name.strip()

    def test_missing_id_is_rejected(self):
        with pytest.raises(SourceContractError, match="no id"):
            CountryRecord.from_api({"name": "Nowhere"})

    def test_missing_name_is_rejected(self):
        with pytest.raises(SourceContractError, match="no name"):
            CountryRecord.from_api({"id": "ZZZ"})


class TestObservationRecord:
    def test_uses_iso3_not_iso2(self, observation_pages):
        """The trap worth a test of its own.

        On this endpoint country.id is the ISO2 code ("ET") and countryiso3code is
        the ISO3 ("ETH"). The dimension is keyed on ISO3. They sit adjacent in the
        payload, and joining on the wrong one yields a fact table where every row
        fails referential integrity.
        """
        row = observation_pages[0][1][0]
        assert len(row["country"]["id"]) == 2, "fixture should still carry an ISO2 in country.id"

        record = ObservationRecord.from_api(row, None)
        assert record.country_id == row["countryiso3code"]
        assert len(record.country_id) == 3

    def test_year_is_parsed_from_a_string(self, observation_pages):
        record = ObservationRecord.from_api(observation_pages[0][1][0], None)
        assert isinstance(record.obs_year, int)
        assert 1960 <= record.obs_year <= 2100

    def test_null_value_is_preserved_not_coalesced(self, observation_pages):
        """A null means "not measured that year", which is not the same as zero."""
        rows = [r for page in observation_pages for r in (page[1] or [])]
        nulls = [r for r in rows if r["value"] is None]
        assert nulls, "fixture should contain at least one unmeasured year"
        record = ObservationRecord.from_api(nulls[0], None)
        assert record.obs_value is None

    def test_vintage_is_attached_from_response_metadata(self, observation_pages):
        from ingest.contracts import PageMeta

        meta = PageMeta.model_validate(observation_pages[0][0])
        record = ObservationRecord.from_api(observation_pages[0][1][0], meta.lastupdated)
        # lastupdated is reported once per response, not per row, so it has to be
        # carried down for point-in-time reproducibility to hold downstream.
        assert record.api_last_updated == meta.lastupdated
        assert record.api_last_updated is not None

    def test_missing_iso3_is_rejected(self):
        with pytest.raises(SourceContractError, match="countryiso3code"):
            ObservationRecord.from_api(
                {"country": {"id": "ET"}, "indicator": {"id": "X"}, "date": "2020"}, None
            )

    def test_unparseable_year_is_rejected(self):
        with pytest.raises(SourceContractError, match="unparseable date"):
            ObservationRecord.from_api(
                {
                    "countryiso3code": "ETH",
                    "indicator": {"id": "X"},
                    "date": "not-a-year",
                },
                None,
            )

    def test_all_fixture_rows_parse(self, observation_pages):
        """A broad sweep: the whole recorded stream must parse without exception."""
        rows = [r for page in observation_pages for r in (page[1] or [])]
        assert len(rows) == 330
        records = [ObservationRecord.from_api(r, None) for r in rows]
        assert len(records) == 330
        assert all(math.isfinite(r.obs_value) for r in records if r.obs_value is not None)


class TestIndicatorRecord:
    def test_topics_are_flattened_and_sorted(self):
        record = IndicatorRecord.from_api(
            {
                "id": "X.Y.Z",
                "name": "Something",
                "source": {"id": "2", "value": "World Development Indicators"},
                "sourceNote": "note",
                "unit": "",
                "topics": [{"id": "9", "value": "Zebra"}, {"id": "1", "value": "Apple"}],
            }
        )
        # Sorted so that the API reordering the array does not change the content
        # hash and therefore does not fabricate an update.
        assert record.topics == "Apple; Zebra"
        assert record.unit is None
        assert record.source_name == "World Development Indicators"

    def test_empty_topics_becomes_none(self):
        record = IndicatorRecord.from_api({"id": "X", "name": "N", "topics": []})
        assert record.topics is None
