"""World Bank Indicators API v2 client.

Two things make this more than a wrapper around httpx.get, and both are responses
to behaviour verified against the live API rather than to hypotheticals:

1. Pagination is terminated on the FIRST response's page count, not on each
   response's. See the note on `paginate`.
2. An error is signalled with HTTP 200 and an error-shaped body, so the status
   code is not the check. That happens in contracts.parse_envelope.

The client also has a fixture mode. That is not a testing afterthought: it is what
lets CI run the whole ingestion path deterministically, and it is what lets a
reviewer run the pipeline on a plane. `SOURCE_API_MODE=fixture` is documented in
the README as a first-class way to run this.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Protocol

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from ingest.contracts import PageMeta, SourceContractError, parse_envelope
from ingest.settings import REPO_ROOT, Settings

log = logging.getLogger(__name__)

FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "api"


class RetryableHTTPError(Exception):
    """A server-side or transport failure that is worth trying again."""


def _decode_json(response: httpx.Response) -> Any | None:
    """Decode a response body as JSON, or return None if it is not JSON.

    Decodes with utf-8-sig rather than utf-8 because this API prefixes some
    responses with a UTF-8 byte order mark, and a plain utf-8 json.loads fails on
    it with "Unexpected UTF-8 BOM" - a parse error that looks like corruption and
    is really just an encoding preamble.

    Returns None instead of raising, because the caller needs to distinguish "this
    body is not JSON" from "this body is JSON describing an error", and those lead
    to opposite decisions about retrying.
    """
    try:
        return json.loads(response.content.decode("utf-8-sig"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


class Fetcher(Protocol):
    def get(self, path: str, params: dict[str, Any]) -> Any: ...
    def close(self) -> None: ...


# ---------------------------------------------------------------------------
# Live
# ---------------------------------------------------------------------------


class LiveFetcher:
    """Fetch from the public API, with bounded retries on transient failures."""

    def __init__(self, settings: Settings, transport: httpx.BaseTransport | None = None) -> None:
        self._settings = settings
        self._client = httpx.Client(
            base_url=settings.wb_api_base,
            timeout=settings.wb_request_timeout_seconds,
            follow_redirects=True,
            # Injectable so the retry policy can be tested against synthetic
            # responses with httpx's own MockTransport, rather than by adding a
            # mocking library or by hitting the live API from a unit test.
            transport=transport,
            headers={
                # Identifying the client is basic courtesy to a free public API and
                # makes this pipeline's traffic attributable in their logs.
                "User-Agent": "wb-cdc-analytics/1.0 (data engineering exercise)",
                "Accept": "application/json",
            },
        )

    def get(self, path: str, params: dict[str, Any]) -> Any:
        return self._get_with_retry(path, params)

    # Retry policy. The interesting decision here is that a 4xx CAN be retried,
    # which inverts the usual rule, and the inversion is justified by measured
    # behaviour rather than by hope.
    #
    # This API signals genuine client mistakes with HTTP 200 and a JSON error
    # envelope, not with a 4xx. Verified: an invalid indicator id returns
    #   200 OK  [{"message":[{"id":"120","key":"Invalid value",...}]}]
    #
    # Separately, it intermittently returns HTTP 400 with a non-JSON ASP.NET
    # "Request Error" HTML page for requests that are perfectly valid. Measured:
    #   per_page=480 -> 400, while 490, 495, 499, 500 and 501 all -> 200
    #   per_page=500 -> 400 once, then 200 on six consecutive retries
    #   per_page=100&page=4 -> 400 once mid-run, then 200 on six consecutive retries
    # No monotonic boundary exists, so it is not a documented limit being enforced;
    # it is a transient server-side fault wearing a client-error status code.
    #
    # Putting those two facts together gives a rule that is precise rather than
    # permissive: since real client errors arrive as 200-with-JSON, a 4xx carrying a
    # body that is not JSON cannot be a client error, so it is retried. A 4xx that
    # DOES carry JSON is a real client error and fails immediately. That keeps the
    # protection against retrying a genuinely malformed request while surviving an
    # upstream that lies about whose fault it is.
    #
    # wait_exponential_jitter rather than plain exponential: jitter is what stops
    # nine indicator streams that failed on the same blip from retrying in lockstep
    # and reproducing it.
    @retry(
        retry=retry_if_exception_type(RetryableHTTPError),
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=1, max=30),
        reraise=True,
    )
    def _get_with_retry(self, path: str, params: dict[str, Any]) -> Any:
        try:
            response = self._client.get(path, params=params)
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            log.warning("transport failure on %s: %s", path, exc)
            raise RetryableHTTPError(str(exc)) from exc

        if response.status_code >= 500:
            log.warning("server error %s on %s", response.status_code, path)
            raise RetryableHTTPError(f"HTTP {response.status_code}")

        if response.status_code >= 400:
            if _decode_json(response) is None:
                # Non-JSON body on a 4xx: the transient HTML error page.
                log.warning(
                    "HTTP %s with a non-JSON body on %s params=%s, treating as "
                    "transient and retrying",
                    response.status_code,
                    path,
                    params,
                )
                raise RetryableHTTPError(f"HTTP {response.status_code} with a non-JSON body")
            # JSON body on a 4xx: a real client error. Do not retry it.
            raise SourceContractError(
                f"HTTP {response.status_code} for {path} with {params}: {response.text[:200]}"
            )

        decoded = _decode_json(response)
        if decoded is None:
            # A truncated or mangled body on a 200 is a transport problem wearing a
            # parse error's clothes, so it is worth another attempt.
            raise RetryableHTTPError("undecodable JSON body on HTTP 200")
        return decoded

    def close(self) -> None:
        self._client.close()


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


class FixtureFetcher:
    """Replay recorded responses. No network.

    Fixtures are recorded by scripts/record_fixtures.py and committed. Each file
    holds the ordered list of page payloads for one logical stream, so pagination
    is exercised rather than bypassed.
    """

    def __init__(self, fixture_dir: Path = FIXTURE_DIR) -> None:
        self._dir = fixture_dir
        self._cursor: dict[str, int] = {}

    @staticmethod
    def slug(path: str) -> str:
        return path.strip("/").replace("/", "__").replace(";", "-")

    def get(self, path: str, params: dict[str, Any]) -> Any:
        name = self.slug(path)
        file = self._dir / f"{name}.json"
        if not file.is_file():
            raise SourceContractError(
                f"no fixture for {path} (expected {file}). "
                f"Record it with: python scripts/record_fixtures.py"
            )
        pages: list[Any] = json.loads(file.read_text(encoding="utf-8"))

        # Serve pages in order, mirroring what the live API would return for
        # successive page= values, including the empty tail.
        index = self._cursor.get(name, 0)
        self._cursor[name] = index + 1
        if index >= len(pages):
            return [{"page": index + 1, "pages": len(pages), "per_page": 100, "total": 0}, []]
        return pages[index]

    def close(self) -> None:  # pragma: no cover - nothing to release
        return None


# ---------------------------------------------------------------------------
# The client
# ---------------------------------------------------------------------------


class WorldBankClient:
    def __init__(self, settings: Settings, fetcher: Fetcher | None = None) -> None:
        self._settings = settings
        if fetcher is not None:
            self._fetcher = fetcher
        elif settings.source_api_mode == "fixture":
            log.info("source_api_mode=fixture, replaying committed responses")
            self._fetcher = FixtureFetcher()
        else:
            self._fetcher = LiveFetcher(settings)

    def __enter__(self) -> WorldBankClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._fetcher.close()

    # -----------------------------------------------------------------------
    def paginate(self, path: str) -> Iterator[tuple[PageMeta, list[dict[str, Any]]]]:
        """Yield every page of a result set.

        The termination condition is the interesting part, because the obvious one
        is wrong. Requesting a page past the end of a result set does not return an
        error and does not return the true page count: it returns a RECALCULATED
        and misleading count. Verified live:

            GET .../indicator/IC.BUS.NREG?per_page=100&page=99
            -> [{"page":99,"pages":1,"per_page":100,"total":66}, []]

        "pages":1 while asking for page 99. Any loop that re-reads `pages` from
        each response and compares it against the current page number will either
        exit early or, on a different result set, loop in a way that depends on
        arithmetic the server is doing wrong. So `pages` is captured once, from the
        first response, and an empty row array is also treated as the end.

        Note on per_page: this fetches 100 rows at a time even though the API
        honoured per_page=25000 in testing and returned 17,490 rows in a single
        response. That is a deliberate choice, not a limit. A bounded page size
        bounds peak memory, bounds how much work a retry repeats, and means the
        pipeline behaves the same way against a source that does enforce a cap.
        """
        page = 1
        total_pages: int | None = None

        while True:
            payload = self._fetcher.get(
                path,
                {"format": "json", "per_page": self._settings.wb_per_page, "page": page},
            )
            meta, rows = parse_envelope(payload)

            if total_pages is None:
                total_pages = meta.pages
                log.info(
                    "%s: %s rows across %s page(s) of %s",
                    path,
                    meta.total,
                    total_pages,
                    self._settings.wb_per_page,
                )

            if not rows:
                break

            yield meta, rows

            if page >= total_pages:
                break
            page += 1

            # A guard, not a expectation. If a source ever reported a page count
            # that does not converge, this stops the loop rather than letting it
            # run until something else breaks.
            if page > 10_000:
                raise SourceContractError(f"pagination did not terminate for {path}")

    # -----------------------------------------------------------------------
    def fetch_countries(self, country_path: str) -> Iterator[dict[str, Any]]:
        for _meta, rows in self.paginate(f"country/{country_path}"):
            yield from rows

    def fetch_indicator(self, indicator_id: str) -> Iterator[dict[str, Any]]:
        for _meta, rows in self.paginate(f"indicator/{indicator_id}"):
            yield from rows

    def fetch_observations(
        self, country_path: str, indicator_id: str
    ) -> Iterator[tuple[PageMeta, dict[str, Any]]]:
        """Yield (page metadata, row) pairs.

        The metadata travels with each row because the series vintage
        (`lastupdated`) is reported once per response, not per row, and it has to
        end up on every observation for the point-in-time story in the mart to
        hold.
        """
        path = f"country/{country_path}/indicator/{indicator_id}"
        for meta, rows in self.paginate(path):
            for row in rows:
                yield meta, row
