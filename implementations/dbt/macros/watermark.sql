-- Watermark/Version Utility Macros for dbt Kimball Framework
-- Main ETL control is handled via on-run-start/on-run-end hooks in etl_control.sql

{# ================================================================
   GET LATEST VERSION
   Returns latest Delta version for a source table (for CDF tracking)
   ================================================================ #}

{% macro get_latest_version(source_table) %}
    {% set version_sql %}
    SELECT MAX(version) FROM (DESCRIBE HISTORY {{ source_table }})
    {% endset %}
    
    {% set result = run_query(version_sql) %}
    {% if execute %}
        {{ return(result.columns[0].values()[0]) }}
    {% else %}
        {{ return(0) }}
    {% endif %}
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
