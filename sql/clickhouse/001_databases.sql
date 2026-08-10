-- ---------------------------------------------------------------------------
-- Databases. In ClickHouse a database is what dbt calls a schema, so the medallion
-- layers are databases here.
--
--   raw        CDC landing. Written only by materialized views reading the Kafka
--              engine tables. Never written by dbt, never edited by hand.
--   staging    dbt views: cleaned, deduplicated, current-state. No storage.
--   marts      dbt tables: the analytics-ready and ML-ready outputs.
--   ops        pipeline observability: metrics the exporter and Grafana read.
--
-- The separation is not decoration. It makes the write boundary explicit: exactly
-- one process writes each layer, so "who put this row here" always has one
-- answer.
-- ---------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS marts;
CREATE DATABASE IF NOT EXISTS ops;
