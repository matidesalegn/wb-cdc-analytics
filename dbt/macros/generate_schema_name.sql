{#
  Return the configured schema verbatim instead of concatenating it onto the
  target schema.

  dbt's default behaviour is `<target_schema>_<custom_schema>`, so a model
  configured with +schema: staging against a target schema of marts lands in
  `marts_staging`. That turns the medallion layers into names nobody chose and makes
  the layer a suffix of the environment rather than a thing in its own right.

  With this override, +schema: staging means the `staging` database and nothing else.
  In ClickHouse a database is what dbt calls a schema, so the layers ARE databases
  here, and they are created up front by sql/clickhouse/001_databases.sql.

  The trade-off is stated rather than hidden: this project loses dbt's automatic
  per-developer schema isolation. That isolation is worth having on a shared
  warehouse with several people building at once; here the whole stack is
  ephemeral and per-developer, so the cost is zero and the clarity is worth it.
  On a shared deployment the environment would be carried in the target database
  instead, which is the substitution the design report describes.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
