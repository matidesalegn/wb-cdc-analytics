-- The fact table must contain exactly the rows staging has, no more and no fewer.
--
-- Two failures this catches that per-column tests cannot:
--   * the inner joins to the dimensions dropping rows, which would otherwise look
--     like the source simply having less data
--   * the incremental lookback window failing to pick up a change, which would
--     otherwise look like nothing at all
--
-- Compared as a full outer difference on the grain rather than as two counts, so a
-- row lost and a row gained cannot cancel each other out and report success.

with staging as (
    select country_id, indicator_id, obs_year from {{ ref('stg_observation') }}
),

fact as (
    select country_id, indicator_id, obs_year from {{ ref('fct_indicator_observation') }}
),

missing_from_fact as (
    select country_id, indicator_id, obs_year, 'in staging, absent from fact' as failure_reason
    from staging
    where (country_id, indicator_id, obs_year) not in (select country_id, indicator_id, obs_year from fact)
),

missing_from_staging as (
    select country_id, indicator_id, obs_year, 'in fact, absent from staging' as failure_reason
    from fact
    where (country_id, indicator_id, obs_year) not in (select country_id, indicator_id, obs_year from staging)
)

select * from missing_from_fact
union all
select * from missing_from_staging
