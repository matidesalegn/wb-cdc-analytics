"""The pre-load validation gate.

This is the quality gate that sits between the API and the database: the last
point at which a bad record can be stopped before it becomes a row, a change
event, and a fact.

**This is the gate Great Expectations would own.** The design report states the
reasoning in full; in short, dbt tests run inside the warehouse after
transformation and are the right tool for uniqueness, referential integrity and
business rules on modelled tables, but they structurally cannot reach a payload
that has not landed yet. Something has to occupy that earlier slot. In a team
deployment that something would be a GX suite, because Data Docs give a non-
engineer an auditable record of what was rejected and why. Here it is explicit
assertions, because every check this boundary actually needs is structural rather
than distributional, and forty lines of named invariants are more precise and
cheaper to run than an expectation suite that would express the same thing.

What matters either way is the property, not the tool: nothing reaches the OLTP
table unvalidated, and a rejection is recorded with a reason rather than dropped.
Rejections go to ops.ingest_reject, which is queryable, so "what did the gate
reject" is a question with an answer.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Generic, TypeVar

from ingest.contracts import (
    CountryRecord,
    IndicatorRecord,
    ObservationRecord,
    SourceContractError,
)

# The World Bank series start in 1960. An observation outside this window is not a
# late data point, it is a parsing mistake, so the bound is an assertion rather
# than a filter. The upper bound is generous on purpose: projections legitimately
# run a few years ahead, and a hard "not in the future" rule would reject them.
MIN_OBS_YEAR = 1960
MAX_OBS_YEAR = 2100


@dataclass
class Rejection:
    reason: str
    payload: dict[str, Any]


T = TypeVar("T", CountryRecord, IndicatorRecord, ObservationRecord)


@dataclass
class GateResult(Generic[T]):
    """What the gate let through, what it stopped, and why."""

    accepted: list[T] = field(default_factory=list)
    rejected: list[Rejection] = field(default_factory=list)

    @property
    def seen(self) -> int:
        return len(self.accepted) + len(self.rejected)

    def reject(self, reason: str, payload: dict[str, Any]) -> None:
        self.rejected.append(Rejection(reason=reason, payload=payload))


# ---------------------------------------------------------------------------
# Row-level invariants
# ---------------------------------------------------------------------------


def check_country(record: CountryRecord, allowed: set[str]) -> str | None:
    """Return a rejection reason, or None if the record is acceptable."""
    if record.country_id not in allowed:
        # Not pedantry: the country endpoint accepts a semicolon list and will
        # happily return an aggregate or a region if one is requested by mistake.
        # An aggregate row in a country dimension silently double-counts every
        # subsequent aggregation.
        return f"country_id {record.country_id!r} is not in the configured country set"
    if record.latitude is not None and not -90.0 <= record.latitude <= 90.0:
        return f"latitude {record.latitude} is outside [-90, 90]"
    if record.longitude is not None and not -180.0 <= record.longitude <= 180.0:
        return f"longitude {record.longitude} is outside [-180, 180]"
    return None


def check_indicator(record: IndicatorRecord, allowed: set[str]) -> str | None:
    if record.indicator_id not in allowed:
        return f"indicator_id {record.indicator_id!r} is not in the configured set"
    return None


def check_observation(
    record: ObservationRecord,
    allowed_countries: set[str],
    allowed_indicators: set[str],
) -> str | None:
    if record.country_id not in allowed_countries:
        # This is the referential-integrity check moved upstream of the foreign
        # key. The database would reject it too, but the FK aborts the whole
        # transaction, whereas rejecting here isolates the bad row and lets the
        # good ones land.
        return f"country_id {record.country_id!r} has no matching dimension row"
    if record.indicator_id not in allowed_indicators:
        return f"indicator_id {record.indicator_id!r} has no matching dimension row"
    if not MIN_OBS_YEAR <= record.obs_year <= MAX_OBS_YEAR:
        return f"obs_year {record.obs_year} is outside [{MIN_OBS_YEAR}, {MAX_OBS_YEAR}]"
    if record.obs_value is not None and not math.isfinite(record.obs_value):
        # NaN and infinity survive a JSON round trip through some encoders and are
        # poison downstream: NaN compares false to everything, so it defeats both
        # equality tests and range tests without ever looking like an error.
        return f"obs_value is not finite ({record.obs_value})"
    return None


# ---------------------------------------------------------------------------
# Stream-level gates
# ---------------------------------------------------------------------------


def gate_countries(rows: Iterable[dict[str, Any]], allowed: set[str]) -> GateResult[CountryRecord]:
    result: GateResult[CountryRecord] = GateResult()
    seen_keys: set[str] = set()
    for raw in rows:
        try:
            record = CountryRecord.from_api(raw)
        except (SourceContractError, ValueError) as exc:
            result.reject(f"contract: {exc}", raw)
            continue
        reason = check_country(record, allowed)
        if reason:
            result.reject(reason, raw)
            continue
        # In-batch duplicate detection. The upsert would collapse duplicates
        # silently, so without this a source that started returning a key twice
        # would look identical to one that did not.
        if record.country_id in seen_keys:
            result.reject(f"duplicate country_id {record.country_id!r} within the batch", raw)
            continue
        seen_keys.add(record.country_id)
        result.accepted.append(record)
    return result


def gate_indicators(
    rows: Iterable[dict[str, Any]], allowed: set[str]
) -> GateResult[IndicatorRecord]:
    result: GateResult[IndicatorRecord] = GateResult()
    seen_keys: set[str] = set()
    for raw in rows:
        try:
            record = IndicatorRecord.from_api(raw)
        except (SourceContractError, ValueError) as exc:
            result.reject(f"contract: {exc}", raw)
            continue
        reason = check_indicator(record, allowed)
        if reason:
            result.reject(reason, raw)
            continue
        if record.indicator_id in seen_keys:
            result.reject(f"duplicate indicator_id {record.indicator_id!r} within the batch", raw)
            continue
        seen_keys.add(record.indicator_id)
        result.accepted.append(record)
    return result


def gate_observations(
    rows: Iterable[tuple[date | None, dict[str, Any]]],
    allowed_countries: set[str],
    allowed_indicators: set[str],
) -> GateResult[ObservationRecord]:
    """Gate observations.

    Takes (vintage, row) pairs because the series vintage comes from the response
    metadata rather than from the row, and it has to be attached before the record
    can be validated as complete.
    """
    result: GateResult[ObservationRecord] = GateResult()
    seen_keys: set[tuple[str, str, int]] = set()
    for last_updated, raw in rows:
        try:
            record = ObservationRecord.from_api(raw, last_updated)
        except (SourceContractError, ValueError) as exc:
            result.reject(f"contract: {exc}", raw)
            continue
        reason = check_observation(record, allowed_countries, allowed_indicators)
        if reason:
            result.reject(reason, raw)
            continue
        key = (record.country_id, record.indicator_id, record.obs_year)
        if key in seen_keys:
            result.reject(f"duplicate natural key {key} within the batch", raw)
            continue
        seen_keys.add(key)
        result.accepted.append(record)
    return result


# ---------------------------------------------------------------------------
# Batch-level assertions
# ---------------------------------------------------------------------------


class GateFailure(RuntimeError):
    """The batch as a whole is not fit to load."""


def assert_batch_acceptable(
    stream: str,
    result: GateResult[Any],
    *,
    min_rows: int = 1,
    max_reject_fraction: float = 0.05,
) -> None:
    """Fail the run if the batch is empty or mostly rejected.

    Two checks, because per-row validation cannot see either problem:

    Empty batch. Zero rows is indistinguishable from a successful run that had
    nothing to do, and that is exactly why it has to be an error here. The single
    most likely cause is an error envelope returned with HTTP 200, which yields no
    rows and no exception.

    Reject ratio. A handful of rejected rows is normal for a public dataset. A
    batch that is mostly rejections means the source shape changed, and continuing
    would load a small, unrepresentative slice while reporting success. Failing
    loudly on a 5 percent threshold turns a silent partial load into an incident
    with a name.
    """
    if result.seen < min_rows:
        raise GateFailure(
            f"{stream}: expected at least {min_rows} row(s) from the source, got "
            f"{result.seen}. An error envelope returned with HTTP 200 is the usual "
            f"cause."
        )
    if result.seen:
        fraction = len(result.rejected) / result.seen
        if fraction > max_reject_fraction:
            sample = "; ".join(r.reason for r in result.rejected[:3])
            raise GateFailure(
                f"{stream}: {len(result.rejected)} of {result.seen} rows rejected "
                f"({fraction:.1%}, threshold {max_reject_fraction:.0%}). "
                f"First reasons: {sample}"
            )
