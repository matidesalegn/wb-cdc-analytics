# Scope contract

Frozen before any code was written. Nothing gets built that is not on this page.

## The rule

**Before starting any task, ask: which assessment bullet does this satisfy?**
If the answer is none, it becomes a sentence in the design report instead of
code in the repository.

That rule exists because the failure mode on a three-day exercise is not running
out of skill, it is running out of time on work nobody asked for. A stated
omission with a reason reads as judgement. A silent one reads as a gap.

## Definition of done: 32 lines, all verified

Verified mechanically against the repository on 11 August 2026, not ticked by eye. The evidence
for each is in [`docs/ci-cd-evidence.md`](ci-cd-evidence.md), and the end-to-end behaviour was
confirmed by a clean-clone test: `git clone` into an empty directory, then `make demo`, exit 0 in
about 50 seconds with all six stages passing.

### Core requirements

- [x] 1. Data pulled from a public REST API
- [x] 2. Loaded into a relational OLTP database (PostgreSQL)
- [x] 3. Replicated to ClickHouse in near real time via Debezium CDC
- [x] 4. Staging layer: cleaned and standardised
- [x] 5. Mart layer: analytics-ready
- [x] 6. Optimised for ClickHouse, with engine, partition key and ordering key set explicitly rather than left to default
- [x] 7. Full pipeline automated by an orchestrator: ingestion, transformation, modeling
- [x] 8. Docker Compose runs every component, one command, completes successfully
- [x] 9. GitHub Actions CI with integrated testing that runs on every change
- [x] 10. Prometheus and Grafana monitoring pipeline health and platform performance
- [x] 11. Analytics-ready **and** machine-learning-ready datasets both produced
- [x] 12. Data quality, testing and validation present **at each stage**

### Deliverable 1: design report

- [x] 13. Architecture diagram
- [x] 14. Data flow explanation
- [x] 15. ERD or schema diagram for staging **and** mart layers
- [x] 16. Rationale for ClickHouse-specific choices, as four separate headings: table engine selection, partitioning key, ordering key, materialized views
- [x] 17. Observability design: what is monitored (pipeline health, data freshness, CDC lag, resource usage), which tools were chosen and why
- [x] 18. Summarised report on scaling or extending for increasing data volume

### Deliverable 2: repository

- [x] 19. `docker-compose.yml`
- [x] 20. Ingestion scripts
- [x] 21. Transformation pipelines
- [x] 22. Debezium / CDC connector configuration files
- [x] 23. Orchestration DAGs or workflows
- [x] 24. CI/CD workflow files
- [x] 25. Observability configuration
- [x] 26. Configuration files: environment templates, connection settings

### Deliverable 3: README

- [x] 27. How to run the pipeline end to end
- [x] 28. Dependencies and setup
- [x] 29. How to validate that data moved through each stage
- [x] 30. Link and authentication details for the data source used
- [x] 31. How to access databases, orchestrator and platform observability
- [x] 32. How CI/CD is triggered and what it validates

Items 11, 12, 16 and 17 are the ones a plain mart-plus-tests build under-serves.
Item 16 has four sub-parts and item 17 has four named signals; each needs its
own visible heading or table row.

## In scope

Three entities: `country`, `indicator`, `observation`. Two dimensions plus one
fact is the minimum that makes referential-integrity tests meaningful and an ERD
worth drawing. A fourth table would add build time and no new architectural
content. Scale is demonstrated along the indicator axis, which is a config list,
not the schema.

## Deliberately out of scope

Each of these is a paragraph in the design report rather than code here.

| Not built | Reason |
|---|---|
| Great Expectations | Every check on this critical path is a relational assertion that belongs next to the model it constrains and runs in the same `dbt build`. GX earns its keep for statistical profiling and for validating data before it reaches a warehouse. That earlier gate does exist in this pipeline and is implemented as explicit pre-load assertions in `ingest/checks.py`. The exercise permits a testing framework of choice; this is the choice, and the reasoning is in the report. |
| Schema Registry and Avro | One fewer container, and human-readable topics that `rpk` can inspect during a demo. The cost is real: no wire-level schema-evolution contract. That is the first thing to add in production. |
| ClickHouse clustering, `ReplicatedMergeTree`, `Distributed`, sharding | Single node. The migration path and its trigger volume are in the scaling section. |
| Projections, dictionaries, `AggregatingMergeTree` rollups, TTL beyond the ops tables | Named as an ordered read-acceleration ladder in the scaling section. Building them at this row count would be theatre. |
| SCD2 history | The mart is current-state by choice. The raw event log retains the change stream, so history is reconstructable with a dbt snapshot. |
| Alertmanager routing | The alert rules are written and tested. Wiring a notifier proves nothing a reviewer can see. The report states the human action per signal instead. |
| Airflow production topology | Airflow runs here with LocalExecutor and its own metadata database, which is a demo topology. The production topology is described in the report. |
| BI layer, Terraform, Kubernetes manifests | Not requested. One sentence each in the scaling section. |

## Cut ladder, if time runs short

In order. Stop as soon as the schedule is recovered.

1. Materialized views from five down to two
2. Grafana panels from six down to three
3. The CI integration lane, keeping the fast lane
4. README troubleshooting section
5. CDC for the two dimension tables: batch-load them instead and frame it correctly as near-static dimensions by batch, high-churn fact by CDC, which is a normal production split
6. Airflow: ship shell orchestration and describe the DAG design

Never cut: the six README bullets, the five report bullets, the one-command
start, or CDC actually flowing.
