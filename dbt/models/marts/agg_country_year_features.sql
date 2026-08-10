{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(country_id, obs_year)',
        partition_by = 'tuple()',
        tags         = ['marts', 'ml']
    )
}}

-- The machine-learning-ready dataset.
--
-- GRAIN: exactly one row per (country_id, obs_year). Stated because a feature table
-- whose grain is ambiguous is a feature table that will silently leak duplicates
-- into a training set, and the row count is the only thing a model author checks.
--
-- WHY THIS IS SHAPED DIFFERENTLY FROM THE FACT TABLE
--   fct_indicator_observation is long: one row per measurement, which is right for
--   analytics and BI, where the indicator is a dimension you slice by. A model
--   trainer wants the opposite: one row per observation unit with one column per
--   feature, flat, no nesting, and no join required. Those are genuinely different
--   shapes for genuinely different consumers, which is why both exist rather than one
--   being a view over the other.
--
-- THREE PROPERTIES A FEATURE TABLE NEEDS AND USUALLY LACKS
--
--   1. Nulls are preserved, never filled. Across the nine configured series,
--      non-null density ranges from 19/330 (survey-based account ownership) to
--      330/330 (population). Imputing a zero would encode "this country had no
--      poverty that year" where the truth is "no survey ran". Imputation is a
--      modelling decision that belongs to the model author, with the null density in
--      front of them, not to the pipeline that hands them the data.
--
--   2. Completeness is measured and carried on the row. feature_count and
--      feature_completeness_pct let a trainer filter to sufficiently populated rows
--      without first computing the null profile themselves, and they make a silent
--      upstream loss visible as a distribution shift rather than as nothing.
--
--   3. Point-in-time honesty. max_series_vintage records the latest World Bank
--      revision date behind the row. Economic series are restated, so a model trained
--      today on 1985 data is using numbers that did not exist in 1985. Carrying the
--      vintage is what makes that auditable instead of an unstated assumption.
--
-- The column layout comes from the ml_features var in dbt_project.yml, so adding a
-- series is a config change in two files and no SQL edit. A test asserts that every
-- ingested indicator has a feature column, so the two cannot drift apart quietly.

{% set features = var('ml_features') %}

with observations as (
    select
        country_id,
        obs_year,
        indicator_id,
        obs_value,
        series_vintage
    from {{ ref('fct_indicator_observation') }}
),

pivoted as (
    select
        country_id,
        obs_year,

        {#- One column per configured feature. anyIf rather than maxIf: the grain of
            the source is one row per (country, indicator, year), so there is at most
            one value to pick and an aggregate that implies a choice between several
            would be misleading about the data. -#}
        {%- for feature in features %}
        anyIf(obs_value, indicator_id = '{{ feature.indicator }}') as {{ feature.column }},
        {%- endfor %}

        -- Completeness accounting, computed over the configured feature set rather
        -- than over whatever happened to arrive, so a missing indicator lowers the
        -- score instead of going unnoticed.
        {{ features | length }}                                       as feature_slots,
        countIf(obs_value is not null)                                as feature_count,
        round(100.0 * countIf(obs_value is not null) / {{ features | length }}, 1)
                                                                      as feature_completeness_pct,
        max(series_vintage)                                           as max_series_vintage
    from observations
    group by country_id, obs_year
)

select
    pivoted.country_id,
    country.country_name,
    country.region_name,
    country.income_level,
    country.is_concessional_borrower,
    pivoted.obs_year,

    {%- for feature in features %}
    pivoted.{{ feature.column }},
    {%- endfor %}

    pivoted.feature_slots,
    pivoted.feature_count,
    pivoted.feature_completeness_pct,
    pivoted.max_series_vintage
from pivoted
inner join {{ ref('dim_country') }} as country
    on pivoted.country_id = country.country_id
