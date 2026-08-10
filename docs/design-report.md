# Design report

**Senior Data Engineer position assessment**
Matiwos Desalegn | August 2026
Repository: [`wb-cdc-analytics`](../README.md) | One command: `make demo`

> **Note for the author (delete before submitting).** Sections marked
> **`[YOUR VOICE]`** are the ones to write in your own words rather than edit. Everything
> else is structure, measurement and rationale drawn from what the code actually does. The
> two sections that will differentiate this submission are **CDC operational failure
> modes** and **Scaling**, because both draw on production experience rather than on this
> exercise. Target for the whole document is about 2,400 words; it currently runs close to
> that, so add by replacing rather than appending.
>
> Confidentiality reminders while writing: no employer or client name anywhere; frame
> production experience as "a production environment I have operated"; keep the WAL figure
> general ("hundreds of GB of retained WAL"); never write "banking" or pair cloud with a
> country name.

---

## Executive summary

This pipeline moves data from a public REST API to analytics-ready and ML-ready tables in
ClickHouse, using PostgreSQL as the OLTP system of record and Debezium logical-decoding
CDC as the transport. It starts with `make demo` and verifies itself with `make verify`.

Measured on the stack in this repository: 2,970 observations across 5 countries, 9
indicators and 66 years; end-to-end CDC latency of about 3 seconds from PostgreSQL commit
to a queryable ClickHouse row; 58 dbt tests and 59 unit tests green; 10 Prometheus alert
rules, each unit-tested with `promtool`.

Three decisions I would defend in detail, each of which prevents a failure that produces
**no error at all**:

1. **The natural key is the primary key.** Under `REPLICA IDENTITY DEFAULT` a Postgres
   delete event carries only the primary key. A surrogate key would make every delete
   arrive downstream as an integer with no way to identify the business entity it removed.
2. **The upsert is change-detecting.** Without `WHERE source_hash IS DISTINCT FROM ...`,
   a re-ingest rewrites every row, and each no-op update emits a CDC event. The change
   stream would describe the scheduler instead of reality.
3. **The Kafka engine reads raw JSON strings and types in materialized views.** Typed
   Kafka-engine columns look cleaner and are a trap: a parse failure fails the block,
   offsets are never committed, and the consumer retries forever silently.

**`[YOUR VOICE]`** *Two or three sentences on what you were optimising for. Suggested
line: the brief said the soundness of design decisions is weighted as heavily as the
working implementation, so I optimised for decisions I can defend and for failures that
are visible rather than silent.*

---

## Architecture diagram

![Architecture](../diagrams/exports/architecture.png)

Mermaid source: [`diagrams/src/architecture.mmd`](../diagrams/src/architecture.mmd),
rendered by `make render` and displayed inline in the [README](../README.md).

Nine containers at full extent, in four groups. The **core path** is four of them
(PostgreSQL, Redpanda, Debezium Connect, ClickHouse) and fits in roughly 2.5 GB, so the
data path can be reviewed on a small machine; **observability** and **orchestration** sit
behind Compose profiles. Every image tag is pinned, every stateful service has a
healthcheck, and dependents gate on `service_healthy` rather than on start order.

## Data flow explanation

The clearest way to describe this is to follow **one record** end to end, naming the
delivery guarantee at each hop. The record is Ethiopia's *New businesses registered* value
for 2023.

| Hop | What happens | Guarantee, and how |
|---|---|---|
| **1. API to memory** | `GET /v2/country/TCD;ETH;KEN;RWA;SSD/indicator/IC.BUS.NREG?page=3&per_page=100`. The row arrives as `{"countryiso3code":"ETH","date":"2023","value":45688,...}` with the series vintage in the response *metadata*, not the row | **At-least-once, safe.** Retries use exponential backoff with jitter. A repeated fetch is harmless because nothing has been written yet |
| **2. Validation** | Parsed into a typed record. `countryiso3code` is used, **not** `country.id`, which is the ISO2 code on this endpoint. The vintage is attached from the page metadata. A content hash is computed over the business fields only | **Fail-closed.** A malformed row is rejected with a reason into `ops.ingest_reject` rather than dropped, and a batch that is mostly rejections fails the run |
| **3. Load into PostgreSQL** | `INSERT ... ON CONFLICT (country_id, indicator_id, obs_year) DO UPDATE ... WHERE source_hash IS DISTINCT FROM EXCLUDED.source_hash` | **Idempotent, and quiet when nothing changed.** Verified: a second ingestion reports `unchanged=2970` and writes nothing. Data, audit row and watermark commit in **one transaction**, so a recorded position can never claim work that rolled back |
| **4. WAL to Debezium** | Logical decoding via `pgoutput` against an explicit publication over four tables | **At-least-once.** Position is the LSN. An explicit publication means adding an unrelated table cannot silently change what is streamed |
| **5. Debezium to Redpanda** | `ExtractNewRecordState` flattens the envelope and adds `__op`, `__lsn`, `__deleted`, `__source_ts_ms`. Topic `wbcdc.wb.observation` | **At-least-once.** `delete.tombstone.handling.mode=rewrite` is what makes deletes *visible* rather than absent |
| **6. Redpanda to ClickHouse** | A Kafka engine table reads the message as one `String`. Two materialized views fire: one appends to an immutable event log, one maintains typed current state | **At-least-once, made convergent.** Offsets commit after the block flushes, so a crash in between re-delivers. The log's sort key is the Kafka coordinate triple, so a duplicate collapses into the row it duplicates |
| **7. Landing to staging** | `staging.stg_observation` reads through `FINAL` with `_is_deleted = 0`, both emitted by a single shared macro | **Exactly-once in effect.** Many versions per key converge to the highest `_version`, which is the source LSN, so "newest" follows the source's commit order rather than arrival order |
| **8. Staging to marts** | `dbt build` with `delete+insert` on the natural key, plus a lookback window | **Idempotent.** Verified: three consecutive builds leave the fact at exactly 2,970 rows |
| **9. Marts to consumers** | `fct_indicator_observation` (long, analytics) and `agg_country_year_features` (flat, wide, ML) | Two shapes for two consumers, reconciled to staging by a test in the DAG's critical path |

The honest summary: **every hop is at-least-once, and the design makes duplication
harmless rather than pretending it cannot happen.** Two mechanisms do that work
throughout: a natural key that is stable and available at every hop, and version columns
(`_lsn`, Kafka offset) that make "latest" a property of the source rather than of timing.

## Data model and schema documentation

![ERD](../diagrams/exports/erd.png)

Mermaid source: [`diagrams/src/erd.mmd`](../diagrams/src/erd.mmd). The landing layer
mirrors the OLTP schema plus CDC metadata, so the ERD shows the staging and mart layers.

Four layers, each with exactly one writer, so "who put this row here" always has one
answer: `raw` (materialized views only), `staging` (dbt views), `marts` (dbt tables), `ops`
(pipeline metadata).

Three entities, not thirty. Two dimensions plus one fact is the minimum that makes
referential-integrity tests meaningful and an ERD worth drawing; a fourth table would add
build time and no new architectural content. **Scale is demonstrated along the indicator
axis instead**, which is a config list in `ingest/indicators.yml`, not a schema change.

### Rationale for ClickHouse-specific design choices

#### Table engine selection

| Table | Engine | Why |
|---|---|---|
| `raw.<entity>` | `ReplacingMergeTree(_version, _is_deleted)` | CDC produces many versions of one key: an insert, every update, then possibly a delete. `_version` is the **Postgres LSN**, which is monotonic per server, so "newest" follows the source's commit order. Using arrival time instead would reorder events under retry |
| `raw.cdc_event_log` | `ReplacingMergeTree` keyed on `(topic, partition, offset)` | The Kafka coordinate triple is globally unique per message, so an at-least-once redelivery collapses into the row it duplicates. This is what "at-least-once made convergent" means concretely |
| `staging.*` | View | Staging only deduplicates, drops tombstones and renames. None of that benefits from storage, and storing it would add a second copy that can go stale |
| `marts.dim_*`, `marts.agg_*` | `MergeTree` | Staging has already collapsed the versions, so these have one row per key by construction. A Replacing engine would add merge work with nothing to merge; choosing the simplest engine that is correct is the point |
| `marts.fct_*` | `MergeTree`, incremental `delete+insert` | Idempotent re-application on the natural key, which matters because the orchestrator retries tasks |

**Three `ReplacingMergeTree` traps, all of which produce wrong data rather than errors.**
Deduplication happens at **merge time** and merges are asynchronous, so between merges the
table legitimately holds several versions and a plain `SELECT` returns duplicates; correct
reads need `FINAL`. Second, `SELECT ... FINAL` *does* hide `_is_deleted` rows on 25.8.29
(measured both ways) but does **not** remove them from disk, so any read that omits `FINAL`
still sees them. Third, a Replacing engine with **no** `ORDER BY` collapses the entire
table to one row, because dbt-clickhouse emits `ORDER BY (tuple())` when `order_by` is
unset and an empty sort key means every row shares it.

The control for all three: `FINAL` and the tombstone filter appear in **exactly one macro**,
and engine plus sort key are always declared together in a model's own `config()` block,
never inherited. A CI convention gate enforces both.

#### Ordering key

The sort key is what defines row identity for deduplication, so it is **exactly the source
primary key**, no more and no less. Adding a column would stop an update collapsing onto
the row it updates; omitting one would collapse distinct entities together.
`raw.observation` and `fct_indicator_observation` both order by
`(country_id, indicator_id, obs_year)`, which is also the marts' access pattern: filter by
country, then indicator, then range-scan years. No sort key contains a `Nullable` column;
ClickHouse rejects that without `allow_nullable_key`, and enabling it would paper over a
design mistake, because a nullable business key cannot identify a row.

#### Partitioning key

**`PARTITION BY tuple()` on every table except the event log: deliberately not
partitioned.** Partitioning prunes scans and drops data cheaply *at scale*, and at 2,970
rows it does neither. It would create many small parts, push the table towards the
`too_many_parts` threshold (an approaching **write outage**, since ClickHouse refuses
inserts past 300 parts per partition), and slow merges, which for a Replacing engine
directly slows deduplication.

The adoption trigger is written down rather than left to taste: **partition by month once a
table passes roughly 100 million rows, or once retention needs whole-partition drops.** The
event log *is* partitioned by month, because it is the one table with a TTL and monthly
partitions let expiry drop parts instead of mutating rows. A Grafana panel tracks active
parts per table, so this decision is monitored rather than asserted.

#### Materialized views

Six, and a ClickHouse materialized view is an **insert trigger**, not a cached query: it
fires on each block arriving in its `FROM` table and writes to its `TO` table.

Two per topic, on purpose. One appends every event to the immutable log; one maintains
typed current state. They are separate because they have different keys, different engines
and different retention, and because a fault in the modelling view must not stop the audit
log from recording what arrived.

The most important decision in the file is that the Kafka engine table has **one `String`
column** and `kafka_format = 'JSONAsString'`. The obvious alternative, `JSONEachRow` with
typed columns, fails brutally: a message that does not fit the declared types throws, the
block fails, offsets are **not** committed, and the engine retries the same block forever.
Nothing is inserted, no client sees an error, and the only symptom is a landing table that
stopped growing. With `JSONAsString` the consumer cannot fail, all typing moves into views
that use only `JSONExtract*` (which returns a default or NULL rather than throwing), and
the pipeline degrades to nulls on an unexpected payload instead of stalling.

Every cast was verified against a live server before use, and the wire format was read off
a real topic rather than assumed. Three encodings would have been wrong if assumed:
`__deleted` is the **string** `"true"` (so `JSONExtractBool` returns false for a deleted
row, resurrecting every delete); a Postgres `DATE` arrives as an int32 day count while a
`TIMESTAMPTZ` in the same row arrives as an ISO-8601 string; and delete events fill
non-key `NOT NULL` columns with **schema defaults**, not nulls. Details in
[`docs/cdc-wire-format.md`](cdc-wire-format.md).

## Data quality and validation at each stage

Nine mechanisms across six stages, and every one is either a gate in the DAG's critical
path or a test in `dbt build`.

| Stage | Mechanism | Catches |
|---|---|---|
| API response | Envelope **shape** validation, not status code | An error returned with HTTP 200. Ingesting zero rows looks identical to an indicator with no data |
| API response | Per-indicator completeness on the **fact** stream | An **archived** indicator, which still serves valid metadata while its data endpoint returns an error envelope. Neither a metadata check nor a total row count detects this |
| Parse | Typed contracts, whitespace and empty-string normalisation, in-batch duplicate detection | A trailing space silently splitting a group-by; `""` defeating every null check; a source that started returning a key twice |
| Pre-load | Domain invariants: key in the configured set, coordinate ranges, year bounds, finite values, referential integrity | An aggregate row double-counting a dimension; NaN, which compares false to everything and so defeats equality *and* range tests |
| Batch | Empty-batch and reject-ratio assertions | A partial load reporting success |
| CDC transport | Connector and task state, replication slot lag, **source-to-warehouse parity** | Dropped events, which are invisible in every other signal |
| ClickHouse landing | Quarantine view over the event log; consumer exception check | An unattributable key; a silently stalled consumer |
| dbt staging and marts | 58 tests: `unique`, `not_null`, `relationships`, `accepted_values`, `accepted_range`, plus 5 singular tests | Dedup errors, join fan-out, a resurrected tombstone, an incremental model degraded to an append |
| DAG gates | Parity, CDC lag, source health, marts reconciliation | The above, re-asserted in the critical path so the guarantee survives someone running `dbt run` without tests |

### On Great Expectations

The brief permits *"a testing framework of your choice (e.g. pytest, dbt tests, Great
Expectations)"*, and I chose dbt tests plus pytest. That is a design position, not an
omission, so it is worth stating the reasoning.

dbt tests run **inside** the warehouse after transformation, and they are the right tool
for uniqueness, referential integrity, accepted values and business rules on modelled
tables, because they live next to the models they constrain and run in the same
`dbt build`. What they structurally cannot reach is a payload that has not landed yet.
Something has to occupy that earlier slot, and in a team deployment I would put Great
Expectations there specifically because Data Docs give a non-engineer an auditable record
of what was rejected and why.

Here that slot is occupied by explicit assertions in `ingest/checks.py`, labelled in the
code as the gate GX would own, because every check this boundary actually needs is
structural rather than distributional. Forty lines of named invariants are more precise and
cheaper to run than an expectation suite expressing the same thing, and they are unit
tested. The place I would reach for GX first in *this* pipeline is the ML feature boundary,
where the useful expectations are distributional (null-rate profiles, cardinality bounds,
label degeneracy) and genuinely are GX's strength rather than dbt's.

## Observability design

### What is monitored

| Signal | Source | Metric | Threshold | What a human does |
|---|---|---|---|---|
| **CDC lag** | Debezium heartbeat, via the exporter | `wb_pipeline_cdc_heartbeat_lag_seconds` | > 300s for 2m | Check connector and task state, then `system.kafka_consumers` for a stalled consumer |
| **Pipeline health** | `ops.ingest_run`, `ops.layer_counts` | `wb_pipeline_layer_rows`, `..._ingest_runs_total`, `..._ingest_last_success_timestamp_seconds` | parity < 99% for 10m | Compare the topic high watermark against the landing count; `make verify` prints both |
| **Data freshness** | `raw.cdc_event_log` | `wb_pipeline_seconds_since_last_change` | > 24h **and** heartbeat healthy | Check the ingestion DAG. Alone this is informational |
| **Resource usage** | ClickHouse `:9363`, Redpanda `:9644` natively | memory, merges, per-error-code counters, active parts | parts > 300 for 15m | Check insert batch size, then the partition key |
| **Source health** | `pg_replication_slots` | `wb_pipeline_replication_slot_retained_wal_bytes` | > 1 GiB for 5m | `make drop-slot` if inactive. This threatens the **source** disk |
| **Data quality** | quarantine and reject tables | `..._cdc_quarantine_rows`, `..._ingest_rejected_rows` | > 0 for 5m | Read the reasons; usually a source schema change |

**The distinction that matters most here is between CDC lag and data freshness**, and I got
it wrong first and fixed it by measurement. With lag computed per business table, an idle
dimension reported 1,805 seconds of "lag" after half an hour in which nothing had changed.
Nothing was wrong. Alerting on that pages someone nightly for a table that had no news. So
lag is now measured against a heartbeat row updated on a fixed 10-second timer, which means
connector liveness; per-table freshness is a separate, informational signal, and the
freshness alert is explicitly suppressed while the heartbeat is unhealthy, because two
alerts for one root cause is how a rota learns to ignore both.

### Tools, and why

**Prometheus and Grafana**, with **no exporter sidecars**. ClickHouse and Redpanda both
expose Prometheus metrics natively, so the usual `clickhouse-exporter` and JMX-exporter
containers are not needed: two fewer containers, two fewer version pins, two fewer reasons
a dashboard can be empty. The one exporter that exists covers only what nothing else can,
the data-plane signals that live in tables rather than in a process. It queries at **scrape
time** rather than caching, because a cached value lets Prometheus stamp a fresh timestamp
on stale data and a stalled pipeline would look healthy; and a failed query yields **no
metric** rather than a zero, because zero is a plausible row count while an absent series
makes a graph gap and fires `absent()`.

Grafana is provisioned from the repository with **no plugins**. The ClickHouse datasource
plugin would make the container's start depend on reaching grafana.com, so a reviewer
without network would get a Grafana that fails to boot for reasons unrelated to this
pipeline.

**Alert rules are code, and untested alert rules fail silently**, so all 10 have
`promtool` unit tests covering both directions: firing when they should and staying quiet
when they should. That caught two bugs review would not have. `CdcParityBroken` divided two
series carrying different label names, and PromQL matches on the full label set, so the
expression evaluated to an empty vector and the alert **could never have fired** while
looking exactly like one that was passing. And `ServiceDown` set a static
`component: platform` label, which overrides the label from the scraped series, so a
ClickHouse outage would have been relabelled "platform" and lost its only routing
information.

Alertmanager routing is deliberately not wired: it would add a container and a set of
credentials to prove something a reviewer cannot see in ten minutes. The rules are real and
each carries a runbook, which is the part that would matter on a rota.

## CDC operational failure modes

*Added section. These are the failures that separate a CDC pipeline that survives contact
with production from one that works on a laptop, and the design above is shaped by them.*

**1. The forgotten replication slot.** An inactive logical replication slot retains WAL
indefinitely so that a consumer which might return can resume. If none returns, the WAL
accumulates until the **source** database's disk fills, and nothing about the connector
looks wrong, because the connector is gone. Three controls here: `max_slot_wal_keep_size=2GB`
so the server invalidates the slot rather than filling its disk; a heartbeat so the slot
advances even when the captured tables are idle; and `make down` drops the slot explicitly.

**`[YOUR VOICE]`** *Two or three sentences on having recovered a production cluster from
abandoned slots holding hundreds of GB of retained WAL each, and what the symptom looked
like before the cause was found. This is your strongest paragraph in the document. Keep the
figure general and name no employer.*

**2. Dedup ordering under log rollover.** Any "latest version wins" rule needs a total
order over the source's log positions, and the obvious ordering is often subtly wrong: a
lexical sort over log-file names inverts when the counter rolls over, electing a stale
version as the winner. Here `_version` is a numeric LSN, so the ordering is arithmetic
rather than lexical.

**`[YOUR VOICE]`** *One or two sentences on having found and fixed exactly this class of
bug in a production CDC dedup path.*

**3. Logical decoding carries no DDL.** Postgres logical replication streams row changes,
not schema changes, so a column added upstream simply appears in the payload and a column
dropped simply stops appearing. Schema drift surfaces **in the data**, not in the event
stream. This design absorbs that rather than breaking on it: `JSONAsString` plus
`JSONExtract` means an unexpected payload produces nulls instead of a stalled consumer, and
the immutable event log retains the original payload so a model can be re-derived after the
schema is understood. Without a Schema Registry there is no wire-level evolution contract,
and that is the first thing I would add.

**4. Snapshot memory pressure.** An initial snapshot of a wide table streams the whole
table through the connector, and a buffered read will exhaust a modestly sized worker. At
this volume it is a non-issue; at production volume it is the first thing to size for.

**5. No-op updates poisoning the stream.** Covered above, and worth repeating as an
operational point: a pipeline whose CDC volume tracks its schedule rather than its data has
lost the ability to use lag and throughput as signals at all.

## Scaling and extending for increasing data volume

Three horizons, each with a **numeric trigger** rather than a vague "at scale".

**Horizon 1: the same shape, 100x the data (roughly 300 million fact rows).** Adopt monthly
`PARTITION BY toYYYYMM(...)` once a table passes ~100M rows, so expiry and backfill can drop
whole parts. Switch the staging views to incremental tables once `FINAL` at query time
becomes measurable. Increase the Kafka topic partition count and `kafka_num_consumers`
together, since ClickHouse consumer parallelism is bounded by partitions. Move the dbt fact
model from `delete+insert` to `insert_overwrite` on the partition, which avoids
mutations entirely.

**Horizon 2: more sources, and durability requirements (a real MERL deployment).** A
Schema Registry with Avro, so schema evolution has a wire-level contract instead of being
discovered in the data. `ReplicatedMergeTree` with ClickHouse Keeper and a `Distributed`
table for shard fan-out; note that the dbt adapter's distributed materializations are
experimental and read-after-write on a cluster needs care. Per-source connectors rather
than one, so a slow source cannot hold up another's slot. Airflow moves to
KubernetesExecutor with one pod per task, and the DAG becomes a factory over a source
manifest, so a new source is a config entry rather than a new file. Alertmanager routing on
top of the existing rules.

**Horizon 3: what changes qualitatively.** Below roughly 10 million rows per table this
design is over-engineered and a scheduled batch copy would do. Above roughly 1 billion, the
single-node assumption breaks and the interesting problems move to shard-key selection and
to whether the mart should be pre-aggregated (`AggregatingMergeTree`) rather than
recomputed. The read-acceleration ladder, in the order I would climb it: sort-key
refinement, then projections, then dictionaries for dimension lookups, then
`AggregatingMergeTree` rollups, then tiered storage with TTL moves. Building any of them at
2,970 rows would be theatre.

**The cheapest axis, and the one already built.** Adding indicators is a config change in
`ingest/indicators.yml`, not a schema change, because the fact is keyed on
`(country, indicator, year)`. A new series is more rows, never more columns. That is the
honest answer to "how would this handle ten times the data" for this particular source.

## Trade-offs, and what I would do with more time

| Chosen | Rejected | Why, and what it costs |
|---|---|---|
| Redpanda | Kafka + ZooKeeper | One container instead of three, `rpk` for demos, Kafka-API compatible. Cost: not the exact runtime named in the brief |
| ClickHouse Kafka engine + views | `clickhouse-kafka-connect` sink | No custom Connect image, no plugin install, and it produces real materialized views to reason about. Cost: weaker per-connector observability than Kafka Connect's REST API and DLQ |
| Plain JSON on the wire | Schema Registry + Avro | One fewer container and topics readable with `rpk` during a demo. **Cost: no wire-level schema-evolution contract.** First thing to add |
| dbt tests + pytest | Great Expectations | Structural checks, cheaper and more precise here, and unit tested. Cost: no Data Docs artifact for non-engineers |
| Current-state marts | SCD2 history | Simpler and matches the source, which is itself current-state. The 30-day event log means history is reconstructable with a dbt snapshot |
| `airflow standalone`, 2 containers | Split webserver/scheduler/worker | Fits alongside a broker, a JVM and two databases on one laptop. Cost: a demo topology, not production. The production one is described above rather than half-built |
| Alert rules without a notifier | Alertmanager | The rules are real and tested; wiring a notifier proves nothing visible in ten minutes |

**With more time, in priority order:** a Schema Registry, because it is the one missing
piece with a real correctness consequence; a Great Expectations suite at the ML feature
boundary, where distributional expectations genuinely beat relational tests; the
`ReplicatedMergeTree` migration, since the path is already written down; dbt snapshots over
the event log to reconstruct history; and per-source connector isolation.

**`[YOUR VOICE]`** *Close with two or three sentences: what you would want to talk through
in a follow-up conversation, and one thing about this exercise you found genuinely
interesting. Specific beats gracious.*

---

## Deliberate scope boundaries

Recorded in full in [`docs/SCOPE.md`](SCOPE.md), frozen before any code was written. Not
built, each for a stated reason rather than by omission: Great Expectations; Schema
Registry and Avro; ClickHouse clustering, `ReplicatedMergeTree`, `Distributed` and
sharding; projections, dictionaries, `AggregatingMergeTree` rollups and TTL beyond the ops
tables; SCD2 history; Alertmanager routing; a BI layer, Terraform or Kubernetes manifests.

A stated omission with a reason is a decision. A silent one is a gap.
