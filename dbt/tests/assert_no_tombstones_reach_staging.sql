-- No deleted row may reach the staging layer.
--
-- This is the regression test for the highest-consequence silent failure in the
-- whole pipeline. A CDC delete event carries the highest LSN for its key, so any
-- "latest version wins" read picks the tombstone. If the read forgets either FINAL
-- or the _is_deleted filter, the deleted record comes back, with its non-key columns
-- filled with schema defaults: empty strings and 1970-01-01 timestamps. Nothing
-- errors. Every uniqueness and referential test still passes. The row is simply
-- wrong.
--
-- Detected here by looking for the fingerprint of a tombstone rather than for the
-- flag itself, because staging does not expose the flag: a resurrected row is one
-- whose source_updated_at sits at the epoch, which no real record ever does.

select
    country_id,
    indicator_id,
    obs_year,
    source_updated_at,
    'a tombstone reached staging: source_updated_at is at the epoch' as failure_reason
from {{ ref('stg_observation') }}
where source_updated_at is null
   or source_updated_at <= toDateTime64('1970-01-02 00:00:00', 6, 'UTC')
