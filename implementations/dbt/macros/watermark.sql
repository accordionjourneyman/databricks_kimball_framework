-- Watermark Management Macros for dbt Kimball Framework
-- Provides CDF support with timestamp fallback

{# ================================================================
   GET WATERMARK
   Returns last_processed_version or timestamp for a (target, source) pair
   ================================================================ #}

{% macro get_watermark(target_table, source_table, watermark_type='version') %}
    {%- set control_table = ref('etl_control') -%}
    
    {%- if watermark_type == 'version' -%}
        (SELECT COALESCE(MAX(last_processed_version), 0) 
         FROM {{ control_table }}
         WHERE target_table = '{{ target_table }}' 
           AND source_table = '{{ source_table }}')
    {%- elif watermark_type == 'timestamp' -%}
        (SELECT COALESCE(MAX(last_processed_timestamp), TIMESTAMP'1970-01-01')
         FROM {{ control_table }}
         WHERE target_table = '{{ target_table }}'
           AND source_table = '{{ source_table }}')
    {%- else -%}
        {{ exceptions.raise_compiler_error("watermark_type must be 'version' or 'timestamp'") }}
    {%- endif -%}
{% endmacro %}


{# ================================================================
   UPDATE WATERMARK
   Upserts watermark row with MERGE (via run_query)
   ================================================================ #}

{% macro update_watermark(target_table, source_table, new_version=none, new_timestamp=none, rows_read=none, rows_written=none) %}
    {%- set control_table = ref('etl_control') -%}
    
    {% set merge_sql %}
    MERGE INTO {{ control_table }} AS t
    USING (
        SELECT 
            '{{ target_table }}' as target_table,
            '{{ source_table }}' as source_table,
            {% if new_version is not none %}{{ new_version }}{% else %}NULL{% endif %} as last_processed_version,
            {% if new_timestamp is not none %}TIMESTAMP'{{ new_timestamp }}'{% else %}NULL{% endif %} as last_processed_timestamp,
            {% if rows_read is not none %}{{ rows_read }}{% else %}NULL{% endif %} as rows_read,
            {% if rows_written is not none %}{{ rows_written }}{% else %}NULL{% endif %} as rows_written,
            'SUCCESS' as batch_status,
            current_timestamp() as batch_completed_at,
            current_timestamp() as updated_at
    ) AS s
    ON t.target_table = s.target_table AND t.source_table = s.source_table
    WHEN MATCHED THEN UPDATE SET
        last_processed_version = COALESCE(s.last_processed_version, t.last_processed_version),
        last_processed_timestamp = COALESCE(s.last_processed_timestamp, t.last_processed_timestamp),
        rows_read = s.rows_read,
        rows_written = s.rows_written,
        batch_status = s.batch_status,
        batch_completed_at = s.batch_completed_at,
        updated_at = s.updated_at
    WHEN NOT MATCHED THEN INSERT *
    {% endset %}
    
    {% do run_query(merge_sql) %}
    {{ log("Updated watermark for " ~ target_table ~ " <- " ~ source_table, info=True) }}
{% endmacro %}


{# ================================================================
   BATCH LIFECYCLE MACROS
   ================================================================ #}

{% macro batch_start(target_table, source_table) %}
    {%- set control_table = ref('etl_control') -%}
    {%- set batch_id = modules.uuid.uuid4() | string -%}
    
    {% set merge_sql %}
    MERGE INTO {{ control_table }} AS t
    USING (
        SELECT 
            '{{ target_table }}' as target_table,
            '{{ source_table }}' as source_table,
            '{{ batch_id }}' as batch_id,
            current_timestamp() as batch_started_at,
            NULL as batch_completed_at,
            'RUNNING' as batch_status,
            NULL as error_message,
            current_timestamp() as updated_at
    ) AS s
    ON t.target_table = s.target_table AND t.source_table = s.source_table
    WHEN MATCHED THEN UPDATE SET
        batch_id = s.batch_id,
        batch_started_at = s.batch_started_at,
        batch_completed_at = s.batch_completed_at,
        batch_status = s.batch_status,
        error_message = s.error_message,
        updated_at = s.updated_at
    WHEN NOT MATCHED THEN INSERT (
        target_table, source_table, batch_id, batch_started_at, 
        batch_completed_at, batch_status, error_message, updated_at
    ) VALUES (
        s.target_table, s.source_table, s.batch_id, s.batch_started_at,
        s.batch_completed_at, s.batch_status, s.error_message, s.updated_at
    )
    {% endset %}
    
    {% do run_query(merge_sql) %}
    {{ log("Batch started: " ~ batch_id ~ " for " ~ target_table, info=True) }}
{% endmacro %}


{% macro batch_complete(target_table, source_table, new_version=none, new_timestamp=none, rows_read=none, rows_written=none) %}
    {# Wrapper that marks batch SUCCESS and updates watermark #}
    {{ update_watermark(target_table, source_table, new_version, new_timestamp, rows_read, rows_written) }}
{% endmacro %}


{% macro batch_fail(target_table, source_table, error_message) %}
    {%- set control_table = ref('etl_control') -%}
    {%- set truncated_error = error_message[:4000] if error_message|length > 4000 else error_message -%}
    
    {% set update_sql %}
    UPDATE {{ control_table }}
    SET batch_status = 'FAILED',
        batch_completed_at = current_timestamp(),
        error_message = '{{ truncated_error | replace("'", "''") }}',
        updated_at = current_timestamp()
    WHERE target_table = '{{ target_table }}'
      AND source_table = '{{ source_table }}'
    {% endset %}
    
    {% do run_query(update_sql) %}
    {{ log("Batch FAILED for " ~ target_table ~ ": " ~ truncated_error[:100], info=True) }}
{% endmacro %}
