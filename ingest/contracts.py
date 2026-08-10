"""The boundary contract for the World Bank API.

This module is the first data-quality gate in the pipeline. Nothing untyped gets
past it: a response either parses into these models or it is rejected with a named
reason, and the rejection is recorded rather than logged and forgotten.

It is also the module that would be replaced by a Great Expectations suite in a
larger deployment. The design report explains that choice; the short version is
that the checks needed at this boundary are structural rather than statistical, so
a schema plus explicit invariants is both cheaper and more precise than a
distributional expectation suite. The architectural slot is the same one.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


class SourceContractError(ValueError):
    """The API returned something this pipeline refuses to interpret."""


# ---------------------------------------------------------------------------
# Envelope handling
# ---------------------------------------------------------------------------
#
# The World Bank API signals errors with HTTP 200 and an error-shaped body:
#
#   GET /v2/country/ETH/indicator/NOT.A.REAL?format=json
#   200 OK
#   [{"message":[{"id":"120","key":"Invalid value",
#                 "value":"The provided parameter value is not valid"}]}]
#
# Verified live. A client that trusts the status code treats that as success and
# ingests zero rows, which looks exactly like "this indicator has no data for
# these countries" and is therefore invisible. Validating the SHAPE of the
# envelope, not the status code, is the only reliable check.


class PageMeta(BaseModel):
    """The first element of a successful response."""

    model_config = ConfigDict(extra="ignore")

    page: int
    pages: int
    per_page: int
    total: int
    # Present on data endpoints, absent on the dimension endpoints. This is the
    # vintage of the series, and it is reported once per response rather than per
    # row, so it has to be carried down from here onto every observation.
    lastupdated: date | None = None

    @field_validator("per_page", mode="before")
    @classmethod
    def _coerce_per_page(cls, value: Any) -> Any:
        # per_page comes back as a string on some endpoints ("50") and as an int on
        # others (5). Both are the same fact.
        return int(value) if isinstance(value, str) else value


def parse_envelope(payload: Any) -> tuple[PageMeta, list[dict[str, Any]]]:
    """Split a World Bank response into metadata and rows, or raise.

    Raises SourceContractError with a reason precise enough to act on, because
    "the API call failed" is not something anyone can debug at 2am.
    """
    if not isinstance(payload, list):
        raise SourceContractError(
            f"expected a JSON array at the top level, got {type(payload).__name__}"
        )

    # The error shape: a one-element array whose only element has a "message" key.
    if len(payload) == 1 and isinstance(payload[0], dict) and "message" in payload[0]:
        messages = payload[0].get("message") or []
        detail = "; ".join(
            f"{m.get('key', '?')}: {m.get('value', '?')}" for m in messages if isinstance(m, dict)
        )
        raise SourceContractError(f"API returned an error envelope with HTTP 200: {detail}")

    if len(payload) < 2:
        raise SourceContractError(
            f"expected [metadata, rows], got an array of length {len(payload)}"
        )

    meta_raw, rows = payload[0], payload[1]
    if not isinstance(meta_raw, dict):
        raise SourceContractError("first element of the response is not an object")

    # A page past the end of the result set returns rows as null rather than [].
    if rows is None:
        rows = []
    if not isinstance(rows, list):
        raise SourceContractError(
            f"second element should be an array of rows, got {type(rows).__name__}"
        )

    return PageMeta.model_validate(meta_raw), rows


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------


def clean_text(value: Any) -> str | None:
    """Trim, and treat an empty string as absent.

    Both halves matter on this source. `region.value` comes back as
    "Sub-Saharan Africa " with a trailing space, which would otherwise produce two
    distinct region labels differing only in whitespace and quietly split any
    group-by. And several fields (`unit`, `obs_status`) are "" rather than null,
    which is the API saying "not applicable"; storing that as an empty string
    rather than NULL makes every downstream null check meaningless.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def nested(record: dict[str, Any], key: str, field: str) -> str | None:
    """Pull a field out of one of the API's {id, iso2code, value} sub-objects."""
    block = record.get(key)
    if not isinstance(block, dict):
        return None
    return clean_text(block.get(field))


def to_float(value: Any) -> float | None:
    """Coerce a numeric-looking value, returning None rather than raising.

    Needed because latitude and longitude arrive as STRINGS ("38.7468") on the
    country endpoint while the observation value arrives as a real JSON number.
    Returning None on a malformed value rather than raising keeps one bad
    coordinate from failing an otherwise good batch; the pre-load gate decides
    whether a missing value is acceptable for that field.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def to_int(value: Any) -> int | None:
    parsed = to_float(value)
    return int(parsed) if parsed is not None else None


def source_hash(payload: dict[str, Any]) -> str:
    """A stable content hash of the business fields of one record.

    This is what makes the loader change-detecting, and change detection is what
    keeps the CDC stream honest. Without it, every re-ingest rewrites every row
    with identical values, and each of those no-op updates emits a change event.
    The topic fills with events that represent nothing, CDC lag and throughput
    graphs become meaningless, and the replication slot does real work to
    replicate nothing.

    Keys are sorted and separators are fixed so the hash depends on content and
    not on dictionary ordering. Timestamps are never included: ingested_at and
    updated_at change on every run by definition, so hashing them would defeat
    the entire purpose.
    """
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------


class CountryRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    country_id: str
    iso2_code: str | None
    name: str
    region_id: str | None
    region_name: str | None
    admin_region_id: str | None
    income_level_id: str | None
    income_level: str | None
    lending_type_id: str | None
    lending_type: str | None
    capital_city: str | None
    longitude: float | None
    latitude: float | None
    source_hash: str

    @classmethod
    def from_api(cls, record: dict[str, Any]) -> CountryRecord:
        country_id = clean_text(record.get("id"))
        name = clean_text(record.get("name"))
        if not country_id:
            raise SourceContractError("country record has no id")
        if not name:
            raise SourceContractError(f"country {country_id} has no name")

        fields: dict[str, Any] = {
            "country_id": country_id,
            "iso2_code": clean_text(record.get("iso2Code")),
            "name": name,
            "region_id": nested(record, "region", "id"),
            "region_name": nested(record, "region", "value"),
            "admin_region_id": nested(record, "adminregion", "id"),
            "income_level_id": nested(record, "incomeLevel", "id"),
            "income_level": nested(record, "incomeLevel", "value"),
            "lending_type_id": nested(record, "lendingType", "id"),
            "lending_type": nested(record, "lendingType", "value"),
            "capital_city": clean_text(record.get("capitalCity")),
            "longitude": to_float(record.get("longitude")),
            "latitude": to_float(record.get("latitude")),
        }
        return cls(**fields, source_hash=source_hash(fields))


class IndicatorRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    indicator_id: str
    name: str
    source_id: str | None
    source_name: str | None
    source_note: str | None
    unit: str | None
    topics: str | None
    source_hash: str

    @classmethod
    def from_api(cls, record: dict[str, Any]) -> IndicatorRecord:
        indicator_id = clean_text(record.get("id"))
        name = clean_text(record.get("name"))
        if not indicator_id:
            raise SourceContractError("indicator record has no id")
        if not name:
            raise SourceContractError(f"indicator {indicator_id} has no name")

        # topics is an array of {id, value}. Flattened to a sorted, delimited
        # string rather than kept nested: it is a low-cardinality label used for
        # filtering, and a scalar keeps the OLTP table relational and the
        # ClickHouse column trivially sortable. Sorted so the content hash does
        # not change when the API reorders the array.
        topic_values = sorted(
            filter(
                None,
                (
                    clean_text(topic.get("value"))
                    for topic in (record.get("topics") or [])
                    if isinstance(topic, dict)
                ),
            )
        )

        fields: dict[str, Any] = {
            "indicator_id": indicator_id,
            "name": name,
            "source_id": nested(record, "source", "id"),
            "source_name": nested(record, "source", "value"),
            "source_note": clean_text(record.get("sourceNote")),
            "unit": clean_text(record.get("unit")),
            "topics": "; ".join(topic_values) or None,
        }
        return cls(**fields, source_hash=source_hash(fields))


class ObservationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    country_id: str
    indicator_id: str
    obs_year: int
    obs_value: float | None
    obs_decimals: int | None
    api_last_updated: date | None
    source_hash: str

    @classmethod
    def from_api(cls, record: dict[str, Any], last_updated: date | None) -> ObservationRecord:
        # Use countryiso3code, NOT country.id. On this endpoint country.id is the
        # ISO2 code ("ET") while countryiso3code is the ISO3 ("ETH"), and the
        # dimension table is keyed on ISO3. Joining on the wrong one produces a
        # fact table where every row fails referential integrity, and the two
        # fields sit next to each other in the payload, so it is an easy mistake to
        # make and a slow one to find.
        country_id = clean_text(record.get("countryiso3code"))
        indicator_id = nested(record, "indicator", "id")
        obs_year = to_int(record.get("date"))

        if not country_id:
            raise SourceContractError(
                "observation has no countryiso3code (aggregate rows have none)"
            )
        if not indicator_id:
            raise SourceContractError(f"observation for {country_id} has no indicator id")
        if obs_year is None:
            raise SourceContractError(
                f"observation for {country_id}/{indicator_id} has an unparseable date: "
                f"{record.get('date')!r}"
            )

        fields: dict[str, Any] = {
            "country_id": country_id,
            "indicator_id": indicator_id,
            "obs_year": obs_year,
            # A null value is real information: the series was not measured that
            # year. It is preserved rather than coalesced, and the mart accounts
            # for it explicitly.
            "obs_value": to_float(record.get("value")),
            "obs_decimals": to_int(record.get("decimal")),
            "api_last_updated": last_updated,
        }
        return cls(**fields, source_hash=source_hash(fields))
