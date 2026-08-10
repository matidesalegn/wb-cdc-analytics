{{
    config(
        materialized = 'view',
        tags = ['staging', 'dimension']
    )
}}

-- Cleaned, deduplicated current state of the country dimension.
--
-- The FINAL read and the tombstone filter both live in ch_current_state, so this
-- model cannot forget either. See that macro for why each one matters.

with current_state as (
    {{ ch_current_state('raw', 'country') }}
)

select
    country_id,
    iso2_code,
    name                                        as country_name,
    region_id,
    region_name,
    admin_region_id,
    income_level_id,
    income_level,
    lending_type_id,
    lending_type,
    capital_city,
    longitude,
    latitude,

    -- Derived flag, kept here rather than in the mart so every consumer of the
    -- dimension agrees on what it means. IDA and Blend are the World Bank's
    -- concessional lending categories.
    lending_type_id in ('IDX', 'IDB')           as is_concessional_borrower,

    source_hash,
    src_updated_at                              as source_updated_at,
    _version                                    as cdc_version,
    _synced_at                                  as cdc_synced_at
from current_state
