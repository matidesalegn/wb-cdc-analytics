-- Row-count bounds on the fact table.
--
-- A generic test cannot express this: not_null and unique both pass on a table that
-- has silently doubled, and both pass on a table that has silently collapsed to a
-- handful of rows. The count is the only thing that catches either.
--
-- The upper bound is the important half. A dbt-clickhouse incremental model whose
-- unique_key is missing degrades to a plain append with no warning, so the second
-- run quietly doubles the rows inside the lookback window. This test is the tripwire
-- for that.
--
-- Five countries times nine indicators times roughly 66 years is 2,970. The bounds
-- are loose enough that a year rolling over or one series being trimmed does not
-- fail the build.

with actual as (
    select count(*) as row_count from {{ ref('fct_indicator_observation') }}
)

select
    row_count,
    'fact row count outside the expected range 2000 to 4000' as failure_reason
from actual
where row_count < 2000 or row_count > 4000
