{#- Single definition of "today" for every model that needs a reporting date.
    Default: current_date(). To pin a run (e.g. for validation):
    dbt run --vars '{as_of_date: "2026-11-15"}' -#}
{% macro as_of_date() -%}
    {%- if var('as_of_date', none) is none -%}
        current_date()
    {%- else -%}
        date('{{ var("as_of_date") }}')
    {%- endif -%}
{%- endmacro %}
