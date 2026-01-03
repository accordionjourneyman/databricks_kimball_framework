-- ETL Control Table Management Macros
-- Uses on-run-start/on-run-end hooks for atomic batch tracking

{# ================================================================
   CREATE ETL CONTROL TABLE (if not exists)
   Called in on-run-start hook
   ================================================================ #}

{% macro create_etl_control() %}
    {% set schema = var('etl_control_schema', 'demo_gold') %}
    {% set table = var('etl_control_table', 'etl_control') %}
    
    {% set create_sql %}
    CREATE TABLE IF NOT EXISTS {{ schema }}.{{ table }} (
        batch_id STRING NOT NULL,
        run_started_at TIMESTAMP NOT NULL,
        run_completed_at TIMESTAMP,
        run_status STRING DEFAULT 'RUNNING',
        models_run INT,
        models_success INT,
        models_error INT,
        error_message STRING,
        dbt_version STRING,
        updated_at TIMESTAMP DEFAULT current_timestamp()
    )
    USING DELTA
    {% endset %}
    
    {% do run_query(create_sql) %}
    {{ log("ETL Control table ready: " ~ schema ~ "." ~ table, info=True) }}
{% endmacro %}


{# ================================================================
   BATCH START (on-run-start)
   Records batch start in etl_control table
   ================================================================ #}

{% macro batch_start_run() %}
    {% set schema = var('etl_control_schema', 'demo_gold') %}
    {% set table = var('etl_control_table', 'etl_control') %}
    
    {% set insert_sql %}
    INSERT INTO {{ schema }}.{{ table }} (
        batch_id,
        run_started_at,
        run_status,
        dbt_version,
        updated_at
    ) VALUES (
        '{{ invocation_id }}',
        current_timestamp(),
        'RUNNING',
        '{{ dbt_version }}',
        current_timestamp()
    )
    {% endset %}
    
    {% do run_query(insert_sql) %}
    {{ log("Batch started: " ~ invocation_id, info=True) }}
{% endmacro %}


{# ================================================================
   BATCH END (on-run-end)
   Updates batch status based on run results
   ================================================================ #}

{% macro batch_end_run() %}
    {% set schema = var('etl_control_schema', 'demo_gold') %}
    {% set table = var('etl_control_table', 'etl_control') %}
    
    {# Count results from this run #}
    {% set results = results | default([]) %}
    {% set success_count = results | selectattr('status', 'equalto', 'success') | list | length %}
    {% set error_count = results | selectattr('status', 'equalto', 'error') | list | length %}
    {% set total_count = results | length %}
    
    {% set status = 'SUCCESS' if error_count == 0 else 'PARTIAL_FAILURE' if success_count > 0 else 'FAILED' %}
    
    {% set update_sql %}
    UPDATE {{ schema }}.{{ table }}
    SET 
        run_completed_at = current_timestamp(),
        run_status = '{{ status }}',
        models_run = {{ total_count }},
        models_success = {{ success_count }},
        models_error = {{ error_count }},
        updated_at = current_timestamp()
    WHERE batch_id = '{{ invocation_id }}'
    {% endset %}
    
    {% do run_query(update_sql) %}
    {{ log("Batch completed: " ~ invocation_id ~ " - " ~ status ~ " (" ~ success_count ~ "/" ~ total_count ~ " models)", info=True) }}
{% endmacro %}


{# ================================================================
   GET LAST SUCCESSFUL RUN (utility macro)
   Returns the last successful batch_id for dependency tracking
   ================================================================ #}

{% macro get_last_successful_run() %}
    {% set schema = var('etl_control_schema', 'demo_gold') %}
    {% set table = var('etl_control_table', 'etl_control') %}
    
    (SELECT MAX(batch_id) 
     FROM {{ schema }}.{{ table }}
     WHERE run_status = 'SUCCESS')
{% endmacro %}
