# World Bank Indicators API v2: observed behaviour

Base URL: `https://api.worldbank.org/v2`
**Authentication: none. Public endpoint, no key, no token, no registration.**
Rate limit: none documented, and none observed as a distinct 429 response.

Everything below was measured against the live API on 2026-08-10, not read from
documentation. Each finding changed a line of code, and the code comments point
back here.

## Endpoints used

| Purpose | Path | Rows |
|---|---|---|
| Country dimension | `/country/TCD;ETH;KEN;RWA;SSD` | 5 |
| Indicator dimension | `/indicator/{id}` | 1 per call |
| Observation fact | `/country/{iso3;list}/indicator/{id}` | 330 per indicator (5 countries x 66 years) |

All calls add `format=json`, `per_page`, `page`. Nine indicators are configured in
`ingest/indicators.yml`, giving 2,970 observations.

## Four behaviours that a naive client gets wrong

### 1. Errors arrive with HTTP 200, and an archived indicator lies twice

```
GET /v2/country/ETH/indicator/NOT.A.REAL?format=json
200 OK
[{"message":[{"id":"120","key":"Invalid value","value":"The provided parameter value is not valid"}]}]
```

A client that checks the status code sees success and ingests zero rows. That is
indistinguishable from "this indicator has no data for these countries", so the
failure is invisible. `contracts.parse_envelope` validates the **shape** of the
envelope instead of trusting the status.

The sharper version of this trap involves an **archived** indicator, and it took a
failing test to find. `IC.REG.DURS` behaves differently on the two endpoints:

| Request | Result |
|---|---|
| `GET /indicator/IC.REG.DURS` | 200, a **complete and valid** metadata record: "Time required to start a business (days)" |
| `GET /country/{list}/indicator/IC.REG.DURS` | 200, error envelope: *"The indicator was not found. It may have been deleted or archived."* |

So the catalogue still describes a series whose data has been withdrawn. Three
consequences the pipeline is built around:

- **Validating the metadata stream cannot detect it.** The record is valid and
  looks healthy, so a completeness check on metadata passes.
- **A total row count cannot detect it either.** Losing one of nine indicators
  still leaves 2,640 rows, comfortably above any sensible floor.
- **Only a per-indicator assertion on the fact stream catches it.** `run.py`
  therefore asserts that every configured indicator contributed at least one
  observation, and names the ones that did not.

Both halves are committed as fixtures, because the gap between them is the thing
worth regression-testing.

### 2. A page past the end reports a recalculated, wrong page count

```
GET .../indicator/IC.BUS.NREG?format=json&per_page=100&page=99
[{"page":99,"pages":1,"per_page":100,"total":66,...}, []]
```

`"pages":1` while page 99 was requested. Any loop that re-reads `pages` from each
response and compares it to the current page is reasoning about arithmetic the
server is doing wrong. `api_client.paginate` therefore captures `pages` **once,
from the first response**, and also stops on an empty row array.

### 3. HTTP 400 with a non-JSON body is transient, not a client error

This is the finding that changed the retry policy, and it inverts the usual rule
that 4xx must never be retried.

Occasional requests return HTTP 400 with an ASP.NET "Request Error" HTML page
(prefixed with a UTF-8 BOM). Measured:

| Request | Result |
|---|---|
| `per_page=480` | **400** |
| `per_page=490`, `495`, `499`, `500`, `501` | 200 |
| `per_page=500`, repeated 6 times | 200 every time (after one earlier 400) |
| `per_page=100&page=4`, repeated 6 times | 200 every time (after one earlier 400) |
| `country/all` with `per_page=25000` | 200, 17,490 rows in one page |
| `country/all` with `per_page=500` | **400** |

There is no monotonic boundary, so this is not a documented limit being enforced.
It is a transient server-side fault reported with a client-error status.

Combining this with finding 1 gives a rule that is precise rather than permissive:
**since real client errors arrive as 200-with-JSON, a 4xx carrying a non-JSON body
cannot be a client error, so it is retried; a 4xx carrying JSON is a genuine client
error and fails immediately.** One full ingestion run absorbed three such 400s and
three read timeouts and completed with 2,970 of 2,970 rows.

Bodies are decoded with `utf-8-sig`, because the BOM makes a plain `utf-8`
`json.loads` fail with "Unexpected UTF-8 BOM", which reads like corruption and is
really just an encoding preamble.

### 4. `per_page` is effectively unbounded, so 100 is a choice

`per_page=25000` returned 17,490 rows in a single response. The pipeline
nonetheless requests 100 at a time, deliberately: a bounded page size bounds peak
memory, bounds how much work a retry repeats, and means the code behaves the same
way against a source that does enforce a cap. It also means pagination is genuinely
exercised on every run rather than being dead code.

The cost is honest: 36 page requests per full run against a slow upstream, and a
full run takes around nine minutes. That is upstream latency, not pipeline
latency, and it is the main reason fixture mode exists.

## Shape details worth knowing

- **`longitude` and `latitude` are strings** (`"38.7468"`) on the country endpoint,
  while the observation `value` is a real JSON number. Both go through the same
  coercion helper.
- **The observation endpoint returns two country codes.** `country.id` is the
  **ISO2** code (`"ET"`), and `countryiso3code` is the ISO3 (`"ETH"`). The dimension
  is keyed on ISO3. They sit adjacent in the payload, so joining on the wrong one is
  easy, and the symptom is a fact table where every row fails referential
  integrity.
- **`region.value` has a trailing space** (`"Sub-Saharan Africa "`). Untrimmed, that
  produces two region labels differing only in whitespace and silently splits any
  group-by.
- **`unit` and `obs_status` are `""` rather than null.** Stored as empty strings they
  would make every downstream null check meaningless, so empty is normalised to
  NULL.
- **The series vintage is per response, not per row.** `lastupdated` (for example
  `"2026-07-13"`) appears in the response metadata and has to be carried down onto
  every observation for point-in-time reproducibility.
- **Null values are real data.** Non-null density across the nine configured
  indicators ranges from 19/330 (`FX.OWN.TOTL.ZS`, survey-based) to 330/330
  (`SP.POP.TOTL`). A null means the series was not measured that year, which is a
  different claim from zero, so nulls are preserved and the mart accounts for them
  explicitly.

## Offline mode

`SOURCE_API_MODE=fixture` replays committed responses from
`tests/fixtures/api/` with no network at all. Fixtures are recorded by
`scripts/record_fixtures.py` and hold the ordered list of page payloads per stream,
so pagination is replayed rather than bypassed. CI always runs in this mode, which
is what makes the ingestion tests deterministic instead of dependent on a third
party's uptime.
