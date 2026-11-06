{# Models land in exactly the schema they configure (staging, intermediate, marts). #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}


{#
  Latest version of every row of a raw CDC table.
  - Highest lsn wins (tie-break: source_ts_ms, then kafka_offset), so a late re-delivery of an
    older change never overwrites a newer one.
  - Rows whose latest change is a delete are dropped.
  - Returns `row_json` (the after image, or the before image for deletes) plus CDC metadata.
#}
{% macro latest_cdc_version(source_table, pk) -%}
    with ranked as (
        select
            coalesce(after, before) as row_json,
            op,
            lsn,
            source_ts_ms,
            row_number() over (
                partition by coalesce(after, before) ->> '{{ pk }}'
                order by lsn desc, source_ts_ms desc, kafka_offset desc
            ) as version_rank
        from {{ source('raw', source_table) }}
    )
    select row_json, op, lsn, source_ts_ms
    from ranked
    where version_rank = 1 and op <> 'd'
{%- endmacro %}


{# Salted SHA-256 for personal data (NIK, phone). #}
{% macro hash_pii(expression) -%}
    encode(sha256(convert_to('{{ var("pii_salt") }}' || {{ expression }}, 'UTF8')), 'hex')
{%- endmacro %}


{# Debezium sends `date` columns as days since 1970-01-01. #}
{% macro debezium_date(expression) -%}
    (date '1970-01-01' + ({{ expression }})::int)
{%- endmacro %}


{# The business date the marts are computed for (WIB). #}
{% macro as_of_date() -%}
    {%- if var('as_of_date') -%}
        date '{{ var("as_of_date") }}'
    {%- else -%}
        (now() at time zone 'Asia/Jakarta')::date
    {%- endif -%}
{%- endmacro %}
