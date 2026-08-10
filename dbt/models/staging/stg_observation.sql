{{
    config(
        materialized = 'view',
        tags = ['staging', 'fact']
    )
}}

-- Cleaned, deduplicated current state of the observation fact.
--
-- Note what is NOT done here: nulls are not filled, and rows with a null value are
-- not dropped. A null obs_value means the World Bank did not measure that series in
-- that year. Dropping the row would turn "not measured" into "does not exist", and
-- coalescing to zero would turn it into "measured as zero". Both are false claims,
-- and both would be invisible downstream. The mart carries the null through and
-- reports the density explicitly.

with current_state as (
    {{ ch_current_state('raw', 'observation') }}
)

select
    country_id,
    indicator_id,
    obs_year,
    obs_value,
    obs_decimals,

    -- The series vintage: when the World Bank last revised this indicator. Economic
    -- series are restated, so this is what makes a point-in-time reproducible read
    -- possible rather than merely plausible.
    api_last_updated                            as series_vintage,

    -- Explicit, so a consumer never has to decide what a null means, and so the
    -- null density is countable rather than inferred.
    obs_value is null                           as is_unmeasured,

    source_hash,
    src_updated_at                              as source_updated_at,
    _version                                    as cdc_version,
    _synced_at                                  as cdc_synced_at
from current_state
