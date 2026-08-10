{{
    config(
        materialized = 'view',
        tags = ['staging', 'dimension']
    )
}}

-- Cleaned, deduplicated current state of the indicator dimension.

with current_state as (
    {{ ch_current_state('raw', 'indicator') }}
)

select
    indicator_id,
    name                                        as indicator_name,
    source_id,
    source_name,
    source_note,
    unit,
    topics,
    source_hash,
    src_updated_at                              as source_updated_at,
    _version                                    as cdc_version,
    _synced_at                                  as cdc_synced_at
from current_state
