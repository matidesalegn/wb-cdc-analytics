"""Tests for pagination and the retry policy.

The retry tests use httpx's built-in MockTransport rather than a mocking library,
so the fast CI lane needs no extra dependency and the tests exercise the real
httpx request path.
"""

from __future__ import annotations

import json

import httpx
import pytest

from ingest.api_client import FixtureFetcher, LiveFetcher, WorldBankClient
from ingest.contracts import SourceContractError

HTML_400 = (
    "﻿<?xml version=\"1.0\" encoding=\"utf-8\"?>"
    "<html><body><h1>Request Error</h1></body></html>"
)


class TestPagination:
    def test_reads_every_page_of_a_multi_page_stream(self, settings):
        """The recorded fixture is 4 pages of 330 rows, so this is a real loop."""
        client = WorldBankClient(settings, fetcher=FixtureFetcher())
        rows = list(
            client.fetch_observations("TCD;ETH;KEN;RWA;SSD", "IC.BUS.NREG")
        )
        assert len(rows) == 330

    def test_single_page_stream_terminates(self, settings):
        client = WorldBankClient(settings, fetcher=FixtureFetcher())
        rows = list(client.fetch_countries("TCD;ETH;KEN;RWA;SSD"))
        assert len(rows) == 5

    def test_page_count_is_taken_from_the_first_response_only(self, settings):
        """The pagination trap, as its own test.

        Requesting a page past the end returns a RECALCULATED and wrong page count:
        asking for page 99 of a 66-row result returns {"page":99,"pages":1}. A loop
        that re-reads `pages` from each response is reasoning about arithmetic the
        server is doing wrong.

        Here the first response says 3 pages, and every later response lies and says
        1. A correct implementation still reads all three.
        """

        def page(n: int, pages_claim: int, rows: int) -> list:
            return [
                {"page": n, "pages": pages_claim, "per_page": 2, "total": 6},
                [{"countryiso3code": "ETH", "indicator": {"id": "X"}, "date": str(2000 + i),
                  "value": 1.0, "decimal": 0} for i in range(rows)],
            ]

        responses = [page(1, 3, 2), page(2, 1, 2), page(3, 1, 2)]

        class Lying:
            def __init__(self) -> None:
                self.calls = 0

            def get(self, path, params):
                index = self.calls
                self.calls += 1
                return responses[index] if index < len(responses) else [
                    {"page": index + 1, "pages": 1, "per_page": 2, "total": 6}, []
                ]

            def close(self) -> None:
                return None

        fetcher = Lying()
        client = WorldBankClient(settings, fetcher=fetcher)
        collected = list(client.paginate("whatever"))
        assert len(collected) == 3, "should follow the FIRST response's page count"
        assert fetcher.calls == 3

    def test_empty_row_array_ends_pagination(self, settings):
        class Empties:
            def __init__(self) -> None:
                self.calls = 0

            def get(self, path, params):
                self.calls += 1
                if self.calls == 1:
                    return [{"page": 1, "pages": 50, "per_page": 1, "total": 1}, [{"x": 1}]]
                return [{"page": 2, "pages": 50, "per_page": 1, "total": 1}, []]

            def close(self) -> None:
                return None

        fetcher = Empties()
        client = WorldBankClient(settings, fetcher=fetcher)
        pages = list(client.paginate("whatever"))
        # Stops on the empty array even though the metadata claims 50 pages.
        assert len(pages) == 1
        assert fetcher.calls == 2


class TestRetryPolicy:
    """The inverted 4xx rule, which is the least obvious decision in the client.

    This API reports genuine client errors as HTTP 200 with a JSON envelope, and
    intermittently reports transient faults as HTTP 400 with an HTML body. So a
    non-JSON 4xx is retried and a JSON 4xx is not.
    """

    def _fetcher(self, settings, handler) -> LiveFetcher:
        return LiveFetcher(settings, transport=httpx.MockTransport(handler))

    def test_flaky_400_with_html_body_is_retried_and_then_succeeds(self, settings):
        calls = {"n": 0}
        good = [{"page": 1, "pages": 1, "per_page": 100, "total": 0}, []]

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(400, text=HTML_400)
            return httpx.Response(200, json=good)

        result = self._fetcher(settings, handler).get("country/ETH", {})
        assert result == good
        assert calls["n"] == 2, "the first 400 should have been retried, not raised"

    def test_400_with_a_json_body_is_a_real_client_error_and_is_not_retried(self, settings):
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(400, json={"message": "genuinely bad request"})

        with pytest.raises(SourceContractError):
            self._fetcher(settings, handler).get("country/ETH", {})
        assert calls["n"] == 1, "a JSON 4xx must fail immediately, not be retried"

    def test_500_is_retried(self, settings):
        calls = {"n": 0}
        good = [{"page": 1, "pages": 1, "per_page": 100, "total": 0}, []]

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] < 3:
                return httpx.Response(503, text="service unavailable")
            return httpx.Response(200, json=good)

        assert self._fetcher(settings, handler).get("country/ETH", {}) == good
        assert calls["n"] == 3

    def test_timeout_is_retried(self, settings):
        calls = {"n": 0}
        good = [{"page": 1, "pages": 1, "per_page": 100, "total": 0}, []]

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] == 1:
                raise httpx.ReadTimeout("read timed out", request=request)
            return httpx.Response(200, json=good)

        assert self._fetcher(settings, handler).get("country/ETH", {}) == good
        assert calls["n"] == 2

    def test_utf8_bom_on_a_200_body_is_decoded(self, settings):
        """A BOM makes plain utf-8 json.loads fail with "Unexpected UTF-8 BOM".

        That reads like corruption and is really just an encoding preamble, so the
        client decodes with utf-8-sig.
        """
        good = [{"page": 1, "pages": 1, "per_page": 100, "total": 1}, [{"id": "ETH"}]]
        body = ("﻿" + json.dumps(good)).encode("utf-8")

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=body)

        assert self._fetcher(settings, handler).get("country/ETH", {}) == good

    def test_retries_are_bounded(self, settings):
        """A permanently broken upstream must fail, not loop forever."""
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(400, text=HTML_400)

        with pytest.raises(Exception):
            self._fetcher(settings, handler).get("country/ETH", {})
        assert calls["n"] <= 6, "retry attempts must be capped"


class TestFixtureMode:
    def test_missing_fixture_names_the_recorder(self, settings, tmp_path):
        fetcher = FixtureFetcher(fixture_dir=tmp_path)
        with pytest.raises(SourceContractError, match="record_fixtures"):
            fetcher.get("country/NOPE", {})
