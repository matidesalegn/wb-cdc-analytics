{#
  Current-state reads from a CDC landing table.

  This macro exists so that FINAL and the tombstone filter appear in exactly ONE
  place in the project. Both are easy to forget, neither produces an error when
  forgotten, and each failure mode is silent in a different way:

    * Omitting FINAL returns duplicate rows. A ReplacingMergeTree deduplicates at
      merge time, and merges are asynchronous, so between merges the table
      legitimately holds several versions of the same key. Counts come out high and
      joins fan out.

    * Omitting `_is_deleted = 0` resurrects deleted rows. A delete event carries the
      highest LSN for its key, so "latest version wins" picks the tombstone, and the
      row comes back with its non-key columns filled with schema defaults: empty
      strings and 1970-01-01 timestamps. Every test still passes.

  Measured on ClickHouse 25.8.29: SELECT ... FINAL on
  ReplacingMergeTree(ver, is_deleted) DOES hide tombstones, so the filter is
  belt-and-braces rather than strictly required today. It is kept because any read
  that omits FINAL still sees them, and because relying on an engine-internal
  behaviour that has changed across ClickHouse versions for a business-correctness
  guarantee is fragile. See docs/cdc-wire-format.md for the measurement.
#}

{% macro ch_current_state(source_name, table_name) %}
    select *
    from {{ source(source_name, table_name) }} final
    where _is_deleted = 0
{% endmacro %}


{#
  Guard that the connection resolved to the database this project expects.

  Runs from on-run-start. A wrong CLICKHOUSE_DB or a stale profile otherwise builds
  every model successfully in the wrong place, which is far more expensive to unpick
  than a failed run. One query per invocation.
#}
{% macro assert_target_database() %}
    {% if execute %}
        {% set expected = ['marts', 'staging'] %}
        {% if target.schema not in expected %}
            {{ exceptions.raise_compiler_error(
                "Refusing to run: target.schema resolved to '" ~ target.schema ~
                "' but this project writes only to " ~ expected | join(' or ') ~
                ". Check CLICKHOUSE_DB and dbt/profiles.yml."
            ) }}
        {% endif %}
        {% do log("target check ok: " ~ target.type ~ " " ~ target.host ~
                  " schema=" ~ target.schema, info=false) %}
    {% endif %}
{% endmacro %}


{#
  The incremental boundary for CDC-fed fact models.

  A single source of truth for the lookback window, so a model cannot quietly use a
  different one. The window is intentionally wider than the pipeline's run cadence:
  an incremental boundary equal to the interval loses any row whose change landed
  during the run itself, and the loss is invisible because the row simply never
  appears.

  Re-reading the boundary is free because the apply is an idempotent upsert on the
  natural key, so overlap costs a little work and never correctness.
#}
{% macro cdc_incremental_cutoff(timestamp_column) %}
    {%- set hours = var('cdc_lookback_hours', 48) -%}
    {{ timestamp_column }} >= (
        select coalesce(max({{ timestamp_column }}), toDateTime64('1970-01-01 00:00:00', 3, 'UTC'))
             - interval {{ hours }} hour
        from {{ this }}
    )
{% endmacro %}
