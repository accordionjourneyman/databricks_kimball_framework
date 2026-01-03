-- Watermark/Version Utility Macros for dbt Kimball Framework
-- Main ETL control is handled via on-run-start/on-run-end hooks in etl_control.sql
-- CDF utilities are in load_cdf.sql

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
