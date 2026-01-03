-- Macro: Generate Kimball audit columns
-- Adds standard ETL audit columns to any model

{% macro kimball_audit_columns() %}
    current_timestamp() as __etl_processed_at,
    '{{ invocation_id }}' as __etl_batch_id
{% endmacro %}
