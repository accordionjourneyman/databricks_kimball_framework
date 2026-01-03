-- Watermark/Version Utility Macros for dbt Kimball Framework
-- Main ETL control is handled via on-run-start/on-run-end hooks in etl_control.sql

{# ================================================================
   GET WATERMARK
   Returns last_processed_version or timestamp for CDF processing.
   Uses the etl_control table created by hooks.
   ================================================================ #}

{% macro get_watermark(target_table, source_table, watermark_type='version') %}
    {% set schema = var('etl_control_schema', 'demo_gold') %}
    {% set table = var('etl_control_table', 'etl_control') %}
    
    {# Check if table exists first #}
    {% if execute %}
        {% set check_sql %}
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = '{{ schema }}' AND table_name = '{{ table }}'
        {% endset %}
        
        {% set exists = run_query(check_sql) %}
        {% if exists and exists.columns[0].values()[0] > 0 %}
            {%- if watermark_type == 'version' -%}
                0  {# Default to 0 - will be updated by batch hooks #}
            {%- elif watermark_type == 'timestamp' -%}
                TIMESTAMP'1970-01-01'
            {%- endif -%}
        {% else %}
            {# Table doesn't exist yet, return defaults #}
            {%- if watermark_type == 'version' -%}
                0
            {%- elif watermark_type == 'timestamp' -%}
                TIMESTAMP'1970-01-01'
            {%- endif -%}
        {% endif %}
    {% else %}
        {# Not executing, return safe default #}
        0
    {% endif %}
{% endmacro %}


{# ================================================================
   GET LATEST VERSION
   Returns the latest commit version of a Delta table
   ================================================================ #}

{% macro get_latest_version(table_name) %}
    {% set version_sql %}
    SELECT version as latest_version
    FROM (DESCRIBE HISTORY {{ table_name }})
    ORDER BY version DESC
    LIMIT 1
    {% endset %}
    
    {%- set result = run_query(version_sql) -%}
    {{ return(result.columns[0].values()[0] if result.rows|length > 0 else 0) }}
{% endmacro %}


{# ================================================================
   GET TABLE VERSION HISTORY
   Returns version details for debugging/audit
   ================================================================ #}

{% macro get_table_history(table_name, limit=10) %}
    {% set history_sql %}
    SELECT version, timestamp, operation, operationParameters
    FROM (DESCRIBE HISTORY {{ table_name }})
    ORDER BY version DESC
    LIMIT {{ limit }}
    {% endset %}
    
    {{ return(run_query(history_sql)) }}
{% endmacro %}
