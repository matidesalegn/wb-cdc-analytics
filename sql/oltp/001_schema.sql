-- ---------------------------------------------------------------------------
-- OLTP schema. This is the system of record and the CDC source.
--
-- Two schemas, and the split matters for CDC:
--   wb   the replicated business tables. Everything here is in the publication.
--   ops  pipeline bookkeeping. Deliberately NOT replicated, because the
--        ingestion watermark advancing is not a business event, and streaming
--        it to the warehouse would put pipeline state into analytics data.
--
-- Applied by docker-entrypoint-initdb.d on first start, and re-applied
-- idempotently by scripts/bootstrap.sh on every start, so editing this file
-- does not require wiping the volume.
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS wb;
CREATE SCHEMA IF NOT EXISTS ops;

-- ---------------------------------------------------------------------------
-- Dimension: country
--
-- The primary key is the natural key (ISO 3166-1 alpha-3). That is a deliberate
-- CDC decision, not a modelling shortcut. With REPLICA IDENTITY DEFAULT, a
-- Postgres DELETE emits only the primary key columns in the change event. If the
-- primary key were a surrogate sequence, a delete would arrive downstream
-- carrying an integer and nothing else, and the warehouse would have no way to
-- know which business entity had been deleted without keeping its own
-- surrogate-to-natural mapping. Making the natural key the primary key means a
-- delete event is self-describing. The alternative is REPLICA IDENTITY FULL,
-- which writes every column of every UPDATE and DELETE into the WAL: correct,
-- but it multiplies WAL volume for a problem the key choice solves for free.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wb.country (
    country_id        text        NOT NULL,
    iso2_code         text,
    name              text        NOT NULL,
    region_id         text,
    region_name       text,
    admin_region_id   text,
    income_level_id   text,
    income_level      text,
    -- The World Bank's own lending classification (IDA, IBRD, Blend).
    lending_type_id   text,
    lending_type      text,
    capital_city      text,
    -- double precision rather than numeric on purpose. Debezium encodes a
    -- Postgres NUMERIC as base64-encoded bytes by default, which forces either a
    -- decimal.handling.mode override or a base64 decode in every downstream
    -- cast. These are geographic measurements, not money, so float is the honest
    -- type and it keeps the wire format a plain JSON number.
    longitude         double precision,
    latitude          double precision,
    -- Content hash of the source payload. The loader compares it before writing,
    -- so an unchanged row is not rewritten. Without this, a re-ingest would
    -- UPDATE every row with identical values, and every one of those no-op
    -- updates would emit a CDC event, flood the topic, and make CDC lag and
    -- throughput graphs meaningless.
    source_hash       text        NOT NULL,
    ingested_at       timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT country_pk PRIMARY KEY (country_id)
);

-- ---------------------------------------------------------------------------
-- Dimension: indicator
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wb.indicator (
    indicator_id      text        NOT NULL,
    name              text        NOT NULL,
    source_id         text,
    source_name       text,
    source_note       text,
    unit              text,
    topics            text,
    source_hash       text        NOT NULL,
    ingested_at       timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT indicator_pk PRIMARY KEY (indicator_id)
);

-- ---------------------------------------------------------------------------
-- Fact: observation. One row per country, indicator and year.
--
-- Composite natural primary key, for the delete-event reason above.
--
-- obs_value is nullable and that is real data, not a defect: the World Bank
-- returns a row with a null value for years where an indicator was not measured.
-- Dropping those rows would silently turn "not measured" into "does not exist",
-- which is a different claim. They are kept, and the mart accounts for them
-- explicitly.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wb.observation (
    country_id        text        NOT NULL,
    indicator_id      text        NOT NULL,
    obs_year          smallint    NOT NULL,
    obs_value         double precision,
    obs_decimals      smallint,
    -- When the World Bank last revised this series. Economic indicators are
    -- restated, so this is the vintage marker that makes point-in-time
    -- reproducibility possible downstream.
    api_last_updated  date,
    source_hash       text        NOT NULL,
    ingested_at       timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT observation_pk PRIMARY KEY (country_id, indicator_id, obs_year),
    CONSTRAINT observation_year_sane CHECK (obs_year BETWEEN 1960 AND 2100),
    CONSTRAINT observation_country_fk FOREIGN KEY (country_id)
        REFERENCES wb.country (country_id) ON DELETE RESTRICT,
    CONSTRAINT observation_indicator_fk FOREIGN KEY (indicator_id)
        REFERENCES wb.indicator (indicator_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS observation_updated_at_idx
    ON wb.observation (updated_at);

-- ---------------------------------------------------------------------------
-- CDC heartbeat.
--
-- This exists to solve a specific, well-known production failure. A logical
-- replication slot only advances its confirmed_flush_lsn when the consumer
-- acknowledges an event it received. If the captured tables are idle while
-- OTHER activity on the same cluster keeps generating WAL, the slot's restart
-- point stays pinned and WAL accumulates behind it indefinitely, even though
-- nothing the connector cares about has changed. The disk fills, and the cause
-- looks like Postgres misbehaving rather than an idle connector.
--
-- Debezium's heartbeat.action.query writes into this table on a timer. Because
-- the table is in the publication, that write produces an event the connector
-- consumes and acknowledges, which drags the slot forward. It is a keepalive for
-- the replication slot.
--
-- Single row, updated in place, so it cannot grow.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wb.cdc_heartbeat (
    id          smallint    NOT NULL DEFAULT 1,
    beat_at     timestamptz NOT NULL DEFAULT now(),
    beat_count  bigint      NOT NULL DEFAULT 0,
    CONSTRAINT cdc_heartbeat_pk PRIMARY KEY (id),
    CONSTRAINT cdc_heartbeat_single_row CHECK (id = 1)
);

INSERT INTO wb.cdc_heartbeat (id, beat_at, beat_count)
VALUES (1, now(), 0)
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- ops: pipeline bookkeeping, not replicated.
-- ---------------------------------------------------------------------------

-- High-water mark per logical source stream. A missing row means "never run",
-- which is the same code path as a normal incremental run rather than a special
-- bootstrap branch.
CREATE TABLE IF NOT EXISTS ops.ingest_watermark (
    stream_name       text        NOT NULL,
    last_success_at   timestamptz,
    last_cursor       text,
    rows_seen         bigint      NOT NULL DEFAULT 0,
    rows_written      bigint      NOT NULL DEFAULT 0,
    CONSTRAINT ingest_watermark_pk PRIMARY KEY (stream_name)
);

-- One row per ingestion attempt, successful or not. This is the audit trail the
-- ingestion-failure alert and the pipeline-health dashboard panel both read.
CREATE TABLE IF NOT EXISTS ops.ingest_run (
    run_id            bigint      GENERATED ALWAYS AS IDENTITY,
    stream_name       text        NOT NULL,
    started_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz,
    status            text        NOT NULL DEFAULT 'running',
    pages_fetched     integer     NOT NULL DEFAULT 0,
    rows_seen         integer     NOT NULL DEFAULT 0,
    rows_inserted     integer     NOT NULL DEFAULT 0,
    rows_updated      integer     NOT NULL DEFAULT 0,
    rows_unchanged    integer     NOT NULL DEFAULT 0,
    rows_rejected     integer     NOT NULL DEFAULT 0,
    error_message     text,
    CONSTRAINT ingest_run_pk PRIMARY KEY (run_id),
    CONSTRAINT ingest_run_status_known
        CHECK (status IN ('running', 'success', 'failed'))
);

CREATE INDEX IF NOT EXISTS ingest_run_started_at_idx
    ON ops.ingest_run (started_at DESC);

-- Rows the pre-load validation gate rejected, kept rather than dropped so a
-- reviewer can see what was rejected and why. This is the quarantine that makes
-- "validation at the ingestion stage" inspectable instead of just asserted.
CREATE TABLE IF NOT EXISTS ops.ingest_reject (
    reject_id         bigint      GENERATED ALWAYS AS IDENTITY,
    stream_name       text        NOT NULL,
    rejected_at       timestamptz NOT NULL DEFAULT now(),
    reason            text        NOT NULL,
    payload           jsonb       NOT NULL,
    CONSTRAINT ingest_reject_pk PRIMARY KEY (reject_id)
);
