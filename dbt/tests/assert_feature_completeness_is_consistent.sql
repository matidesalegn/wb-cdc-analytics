-- feature_count on each feature row must equal the number of feature columns that
-- are actually non-null on that row.
--
-- The completeness columns are the only signal a model author has about how much of
-- a row is real, so a completeness figure that does not match the row it describes is
-- worse than no figure at all: it is a number that will be trusted. Recomputing it
-- independently here is the check that it means what it claims.

{% set features = var('ml_features') %}

with recomputed as (
    select
        country_id,
        obs_year,
        feature_count,
        (
            {%- for feature in features %}
            if({{ feature.column }} is not null, 1, 0){{ " +" if not loop.last }}
            {%- endfor %}
        ) as actual_non_null
    from {{ ref('agg_country_year_features') }}
)

select
    country_id,
    obs_year,
    feature_count,
    actual_non_null,
    'feature_count disagrees with the number of populated feature columns' as failure_reason
from recomputed
where feature_count != actual_non_null
