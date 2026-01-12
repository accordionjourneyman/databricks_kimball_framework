-- Watermark Utility Macros for dbt Kimball Framework
-- Queries etl_watermarks table for actual last_processed_version

{# ================================================================
   GET WATERMARK
   Returns last_processed_version or timestamp for CDF processing.
   Queries the etl_watermarks table for actual stored watermark.
   ================================================================ #}

{% macro get_watermark(target_table, source_table, watermark_type='version') %}
    {% set schema = var('etl_control_schema', 'demo_gold') %}
    {% set watermark_table = 'etl_watermarks' %}
    
    {% if execute %}
        {# Check if watermark table exists #}
        {% set check_sql %}
        SELECT COUNT(*) as cnt FROM information_schema.tables 
        WHERE table_schema = '{{ schema }}' AND table_name = '{{ watermark_table }}'
        {% endset %}
        
        {% set exists_result = run_query(check_sql) %}
        {% set table_exists = exists_result.columns[0].values()[0] > 0 if exists_result.rows|length > 0 else false %}
        
        {% if table_exists %}
            {# Query actual watermark from table #}
            {% set watermark_sql %}
            SELECT 
                COALESCE(last_processed_version, 0) as version,
                COALESCE(last_processed_timestamp, TIMESTAMP'1970-01-01') as ts
            FROM {{ schema }}.{{ watermark_table }}
            WHERE target_table = '{{ target_table }}'
              AND source_table = '{{ source_table }}'
            {% endset %}
            
            {% set result = run_query(watermark_sql) %}
            {% if result.rows|length > 0 %}
                {%- if watermark_type == 'version' -%}
                    {{ result.columns[0].values()[0] }}
                {%- elif watermark_type == 'timestamp' -%}
                    TIMESTAMP'{{ result.columns[1].values()[0] }}'
                {%- endif -%}
            {% else %}
                {# No record for this source/target pair yet #}
                {%- if watermark_type == 'version' -%}
                    0
                {%- elif watermark_type == 'timestamp' -%}
                    TIMESTAMP'1970-01-01'
                {%- endif -%}
            {% endif %}
        {% else %}
            {# Watermark table doesn't exist yet, return defaults #}
            {%- if watermark_type == 'version' -%}
                0
            {%- elif watermark_type == 'timestamp' -%}
                TIMESTAMP'1970-01-01'
            {%- endif -%}
        {% endif %}
    {% else %}
        {# Not executing (compile phase), return safe default #}
        0
    {% endif %}
{% endmacro %}


{# ================================================================
   UPDATE WATERMARK
   Upserts the watermark for a (target, source) pair.
   Called after successful CDF processing to advance the watermark.
   ================================================================ #}

{% macro update_watermark(target_table, source_table, new_version, new_timestamp=none) %}
    {% set schema = var('etl_control_schema', 'demo_gold') %}
    {% set watermark_table = 'etl_watermarks' %}
    
    {% if execute %}
        {# Use MERGE to upsert the watermark #}
        {% set merge_sql %}
        MERGE INTO {{ schema }}.{{ watermark_table }} AS target
        USING (
            SELECT 
                '{{ target_table }}' AS target_table,
                '{{ source_table }}' AS source_table,
                {{ new_version }} AS last_processed_version,
                {% if new_timestamp %}
                TIMESTAMP'{{ new_timestamp }}' AS last_processed_timestamp,
                {% else %}
                current_timestamp() AS last_processed_timestamp,
                {% endif %}
                current_timestamp() AS updated_at
        ) AS source
        ON target.target_table = source.target_table 
           AND target.source_table = source.source_table
        WHEN MATCHED THEN UPDATE SET
            last_processed_version = source.last_processed_version,
            last_processed_timestamp = source.last_processed_timestamp,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            target_table, source_table, last_processed_version, last_processed_timestamp, updated_at
        ) VALUES (
            source.target_table, source.source_table, source.last_processed_version, 
            source.last_processed_timestamp, source.updated_at
        )
        {% endset %}
        
        {% do run_query(merge_sql) %}
        {{ log("Updated watermark for " ~ target_table ~ " <- " ~ source_table ~ " to version " ~ new_version, info=True) }}
    {% endif %}
{% endmacro %}


{# ================================================================
   GET LATEST VERSION
   Returns the latest commit version of a Delta table.
   Used to determine the ending_version for CDF processing.
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
   Returns version details for debugging/audit.
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
