# wb-cdc-analytics

An end-to-end analytics engineering pipeline: a public REST API is ingested into
PostgreSQL, change events are streamed out of the write-ahead log by Debezium
into ClickHouse in near real time, and dbt shapes a staging layer and an
analytics mart on top. Orchestrated by Airflow, monitored by Prometheus and
Grafana, tested in CI, and started with one command.

```bash
make demo
```

That is the whole thing. It runs preflight checks, generates secrets, brings the
stack up, applies all DDL, registers the CDC connector, ingests from the API,
waits for the change events to land, builds and tests the dbt models, and prints
a row count for every stage.

> Status: build in progress. Sections below are filled as each layer lands.

---

## Running the pipeline end to end

_To be completed._

## Dependencies and setup

_To be completed._

## Architecture at a glance

_To be completed._

## Validating that data moved through each stage

_To be completed._

## Data source: link and authentication details

_To be completed._

## Accessing databases, orchestrator and platform observability

_To be completed._

## How CI/CD is triggered and what it validates

_To be completed._

## Repository layout

_To be completed._

## Design decisions and trade-offs

The full write-up is in [`docs/design-report.md`](docs/design-report.md).
Deliberate scope boundaries are recorded in [`docs/SCOPE.md`](docs/SCOPE.md).

## Troubleshooting

_To be completed._
