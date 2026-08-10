-- Every ingested indicator must have a column in the ML feature mart.
--
-- The pivot in agg_country_year_features is driven by the ml_features var in
-- dbt_project.yml, while ingestion is driven by ingest/indicators.yml. Two config
-- files describing one set is a drift risk, and the drift is silent: an indicator
-- ingested but not pivoted just never appears as a feature, and the feature table
-- still builds, still passes its grain test, and is simply missing a column nobody
-- notices.
--
-- This test closes that gap by comparing what actually landed in the warehouse
-- against what the pivot was told about.

{% set configured = var('ml_features') | map(attribute='indicator') | list %}

select
    indicator_id,
    indicator_name,
    'ingested but has no column in agg_country_year_features (add it to the ml_features var)'
        as failure_reason
from {{ ref('dim_indicator') }}
where indicator_id not in (
    {%- for indicator in configured %}
    '{{ indicator }}'{{ "," if not loop.last }}
    {%- endfor %}
)
