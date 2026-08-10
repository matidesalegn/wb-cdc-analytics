{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = ['country_id', 'indicator_id', 'obs_year'],
        engine               = 'MergeTree()',
        order_by             = '(country_id, indicator_id, obs_year)',
        partition_by         = 'tuple()',
        tags                 = ['marts', 'fact']
    )
}}

-- The analytics-ready fact: one row per country, indicator and year.
--
-- INCREMENTAL DESIGN, and why each piece is not optional.
--
--   unique_key is present and it is the full natural key. This is the single most
--   dangerous omission available in a dbt-clickhouse incremental model: with
--   unique_key unset, the incremental materialisation degrades to a plain append,
--   with no warning and no error. Every run then re-appends the rows inside the
--   lookback window and the table grows duplicates that no test on the source would
--   catch. The unique test on the grain below is the tripwire.
--
--   delete+insert deletes the affected keys and re-inserts them, so the operation is
--   idempotent: running it twice leaves the same rows. That matters because the
--   orchestrator retries tasks, so this model is an at-least-once operation whether
--   or not it was designed as one. It relies on lightweight deletes, verified
--   available on this server (enable_lightweight_delete=1, with
--   allow_experimental_lightweight_delete kept as an alias in 25.8), and on
--   mutations_sync=1 in the profile so the delete completes before the insert lands.
--   Without mutations_sync the two race and the run ends with duplicates that
--   disappear on the next merge, which is a bug that heals itself before you can
--   look at it.
--
--   The incremental filter uses a lookback window rather than a strict watermark.
--   A boundary equal to the run interval loses any row whose change landed during
--   the run itself, invisibly. Re-reading a wider window is free precisely because
--   delete+insert on the natural key is idempotent: overlap costs a little work and
--   never correctness. That is the same trade the CDC loader makes, and it is the
--   reason both can be safely retried.

with observations as (
    select * from {{ ref('stg_observation') }}

    {% if is_incremental() %}
        -- Only the rows that arrived recently. cdc_synced_at is ClickHouse-side
        -- arrival time, so this window is about when the warehouse learned of a
        -- change, not when the source made it. That is the correct choice here: a
        -- backdated correction to a 1985 observation arrives now and must be picked
        -- up now.
        where {{ cdc_incremental_cutoff('cdc_synced_at') }}
    {% endif %}
),

enriched as (
    -- EVERY column is aliased explicitly, including the ones where the alias looks
    -- redundant. This is not style, it is required on ClickHouse.
    --
    -- ClickHouse preserves the table qualifier in a subquery's output column name, so
    -- `select observations.country_id` inside a CTE produces a column literally named
    -- `observations.country_id`. The outer `select *` then carries that name into the
    -- created table, and the model's ORDER BY (country_id, ...) fails with "Missing
    -- columns: country_id" while the column list shows `observations.country_id`
    -- sitting right there. Postgres and Snowflake strip the qualifier, so this is a
    -- portability trap rather than an obvious mistake.
    select
        observations.country_id                 as country_id,
        observations.indicator_id               as indicator_id,
        observations.obs_year                   as obs_year,
        observations.obs_value                  as obs_value,
        observations.is_unmeasured              as is_unmeasured,
        observations.series_vintage             as series_vintage,
        observations.obs_decimals               as obs_decimals,

        -- Denormalised dimension attributes. A star schema would make a consumer
        -- join for these; carrying the few that every query needs makes the common
        -- case a single-table scan, which is what ClickHouse is good at. The full
        -- attributes stay in the dimensions.
        country.country_name                    as country_name,
        country.region_name                     as region_name,
        country.income_level                    as income_level,
        country.is_concessional_borrower        as is_concessional_borrower,
        indicator.indicator_name                as indicator_name,
        indicator.topics                        as indicator_topic,

        observations.cdc_synced_at              as cdc_synced_at
    from observations
    -- inner joins, deliberately. The ingestion gate already enforced referential
    -- integrity before these rows reached PostgreSQL, so a miss here means something
    -- upstream is broken rather than that the data is legitimately sparse. An inner
    -- join makes that show up as a row-count test failure instead of a column of
    -- nulls that looks like ordinary missing data.
    inner join {{ ref('dim_country') }}   as country
        on observations.country_id = country.country_id
    inner join {{ ref('dim_indicator') }} as indicator
        on observations.indicator_id = indicator.indicator_id
)

select * from enriched
