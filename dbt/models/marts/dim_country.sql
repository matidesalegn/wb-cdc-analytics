{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(country_id)',
        partition_by = 'tuple()',
        tags         = ['marts', 'dimension']
    )
}}

-- Country dimension.
--
-- PHYSICAL DESIGN
--   engine MergeTree, not a Replacing variant. Staging has already collapsed the
--   CDC versions, so this table has exactly one row per key by construction and a
--   Replacing engine would add merge work with nothing to merge. Choosing the
--   simplest engine that is correct is the point.
--
--   order_by (country_id) matches the grain and the only lookup pattern, and it
--   gives the sparse primary index the right prefix for the joins the fact and
--   feature marts perform.
--
--   partition_by tuple(), so no partitioning. Five rows. Partitioning here would
--   create one part per partition key value and buy nothing. The engine and the sort
--   key are declared together in this block, never inherited, because
--   dbt-clickhouse emits ORDER BY (tuple()) when order_by is unset and an empty sort
--   key on a Replacing engine collapses a table to one row.

select
    country_id,
    iso2_code,
    country_name,
    region_id,
    region_name,
    income_level_id,
    income_level,
    lending_type_id,
    lending_type,
    is_concessional_borrower,
    capital_city,
    longitude,
    latitude,
    source_updated_at,
    cdc_synced_at
from {{ ref('stg_country') }}
