-- Macro: Fill unknown dimension key
-- Returns default SK (-1, -2, -3) for NULL dimension lookups

{% macro fill_unknown_dimension(column, default_type='unknown') %}
    {%- set defaults = {
        'unknown': var('unknown_sk'),
        'not_applicable': var('not_applicable_sk'),
        'error': var('error_sk')
    } -%}
    COALESCE({{ column }}, '{{ defaults[default_type] }}')
{% endmacro %}
