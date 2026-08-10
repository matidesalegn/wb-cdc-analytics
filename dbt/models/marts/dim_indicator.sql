{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(indicator_id)',
        partition_by = 'tuple()',
        tags         = ['marts', 'dimension']
    )
}}

-- Indicator dimension.
--
-- LowCardinality is applied to the two columns that are genuinely low-cardinality
-- and are used for filtering. It is a dictionary encoding, so it shrinks storage and
-- speeds equality filters, and it is the right call for a handful of distinct values
-- repeated across every row. It would be the wrong call on indicator_id itself,
-- where every value is distinct and the dictionary would be pure overhead.

select
    indicator_id,
    indicator_name,
    toLowCardinality(coalesce(source_name, 'unknown'))  as source_name,
    toLowCardinality(coalesce(topics, 'unclassified'))  as topics,
    source_note,
    unit,
    source_updated_at,
    cdc_synced_at
from {{ ref('stg_indicator') }}
