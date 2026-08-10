"""Ingestion entry point: public REST API to PostgreSQL.

    python -m ingest.run                  # all three streams
    python -m ingest.run --stream country # one stream

Stream order is load-bearing. Dimensions go first because the fact table has
foreign keys to both, and because the gate checks referential integrity against
the dimensions it can see. Running observations first would reject every row.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Iterator

from ingest.api_client import WorldBankClient
from ingest.checks import (
    GateFailure,
    assert_batch_acceptable,
    gate_countries,
    gate_indicators,
    gate_observations,
)
from ingest.contracts import SourceContractError
from ingest.load_postgres import (
    COUNTRY_SPEC,
    INDICATOR_SPEC,
    OBSERVATION_SPEC,
    LoadStats,
    PostgresLoader,
)
from ingest.settings import get_catalogue, get_settings

log = logging.getLogger("ingest")

STREAMS = ("country", "indicator", "observation")


def configure_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
        stream=sys.stdout,
    )
    # httpx logs every request at INFO, which drowns the pipeline's own output.
    logging.getLogger("httpx").setLevel(logging.WARNING)


def ingest_countries(client: WorldBankClient, loader: PostgresLoader) -> LoadStats:
    catalogue = get_catalogue()
    allowed = set(catalogue.countries)

    rows = list(client.fetch_countries(catalogue.country_path))
    result = gate_countries(rows, allowed)
    assert_batch_acceptable("country", result, min_rows=len(allowed))

    # An assertion the reject-ratio check cannot make: every configured country
    # must actually be present. A missing dimension row would silently orphan every
    # observation for that country, and the reject ratio would stay low because the
    # rows that arrived were all fine.
    got = {record.country_id for record in result.accepted}
    missing = allowed - got
    if missing:
        raise GateFailure(f"country: configured countries missing from the source: {sorted(missing)}")

    return loader.load("country", COUNTRY_SPEC, result)


def ingest_indicators(client: WorldBankClient, loader: PostgresLoader) -> LoadStats:
    catalogue = get_catalogue()
    allowed = {indicator.id for indicator in catalogue.indicators}

    # The metadata endpoint takes one indicator at a time, so this is a fan-out of
    # small requests rather than one paginated stream.
    rows = []
    for indicator in catalogue.indicators:
        rows.extend(client.fetch_indicator(indicator.id))

    result = gate_indicators(rows, allowed)
    assert_batch_acceptable("indicator", result, min_rows=len(allowed))

    got = {record.indicator_id for record in result.accepted}
    missing = allowed - got
    if missing:
        raise GateFailure(
            f"indicator: configured indicators returned no metadata: {sorted(missing)}. "
            f"A mistyped indicator id returns HTTP 200 with an error envelope rather "
            f"than an error status."
        )

    return loader.load("indicator", INDICATOR_SPEC, result)


def ingest_observations(client: WorldBankClient, loader: PostgresLoader) -> LoadStats:
    catalogue = get_catalogue()

    # Referential integrity is checked against what is actually in the database,
    # not against the config, so a dimension load that partially failed cannot be
    # papered over by a fact load that assumes it worked.
    known_countries = loader.existing_keys("wb.country", "country_id")
    known_indicators = loader.existing_keys("wb.indicator", "indicator_id")

    def rows() -> Iterator[tuple[object, dict]]:
        for indicator in catalogue.indicators:
            for meta, row in client.fetch_observations(
                catalogue.country_path, indicator.id
            ):
                # The series vintage is reported once per response, not per row, so
                # it is carried down here onto every observation.
                yield meta.lastupdated, row

    result = gate_observations(list(rows()), known_countries, known_indicators)
    # Five countries times nine indicators times roughly 66 years. The floor is set
    # well below that so a year rolling over or a series being trimmed does not fail
    # the run, but a collapse to a handful of rows does.
    assert_batch_acceptable("observation", result, min_rows=1000)

    # Per-indicator completeness, and this check has to live HERE rather than on the
    # metadata stream. An archived indicator keeps serving valid metadata while its
    # data series is withdrawn:
    #
    #   GET /indicator/IC.REG.DURS                  -> 200, full valid record
    #   GET /country/{list}/indicator/IC.REG.DURS   -> 200, error envelope,
    #       "The indicator was not found. It may have been deleted or archived."
    #
    # So the catalogue looks healthy while an entire series is missing from the
    # facts. A total row count cannot catch it either: losing one of nine indicators
    # still leaves 2,640 rows, comfortably above any sane floor. Only asserting that
    # every configured indicator contributed at least one observation catches it.
    per_indicator: dict[str, int] = {}
    for record in result.accepted:
        per_indicator[record.indicator_id] = per_indicator.get(record.indicator_id, 0) + 1
    silent = sorted({i.id for i in catalogue.indicators} - per_indicator.keys())
    if silent:
        raise GateFailure(
            f"observation: configured indicators produced no observations: {silent}. "
            f"An archived indicator still serves metadata but returns an error "
            f"envelope for its data, so this is the only check that detects it."
        )
    log.info(
        "observations per indicator: %s",
        ", ".join(f"{k}={v}" for k, v in sorted(per_indicator.items())),
    )

    return loader.load("observation", OBSERVATION_SPEC, result)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stream",
        choices=STREAMS,
        action="append",
        help="ingest only this stream (repeatable). Default: all, in dependency order.",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    configure_logging(args.verbose)
    settings = get_settings()
    selected = tuple(args.stream) if args.stream else STREAMS

    log.info(
        "starting ingestion: streams=%s source_api_mode=%s",
        ",".join(selected),
        settings.source_api_mode,
    )

    handlers = {
        "country": ingest_countries,
        "indicator": ingest_indicators,
        "observation": ingest_observations,
    }

    totals: dict[str, LoadStats] = {}
    try:
        with WorldBankClient(settings) as client, PostgresLoader(settings.postgres_dsn) as loader:
            # Iterate STREAMS rather than `selected` so dependency order holds
            # regardless of the order the flags were given in.
            for stream in STREAMS:
                if stream in selected:
                    totals[stream] = handlers[stream](client, loader)
    except (GateFailure, SourceContractError) as exc:
        # A data-quality failure, not a crash. Distinguished in the exit path
        # because the operator response is different: a gate failure means look at
        # the source, a traceback means look at the code.
        log.error("ingestion stopped by a quality gate: %s", exc)
        return 2

    print("\ningestion summary")
    for stream, stats in totals.items():
        print(f"  {stream:<12} {stats}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
