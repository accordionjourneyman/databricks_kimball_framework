-- ETL Control Table for Watermark & Batch Tracking
-- Mirrors PySpark's etl_control table schema

{{
    config(
        materialized='incremental',
        unique_key=['target_table', 'source_table'],
        on_schema_change='append_new_columns'
    )
}}

{# This model is populated via macros, not direct SELECT #}
{# Initial seed row to create table structure #}

{% if not is_incremental() %}
SELECT 
    CAST(NULL AS STRING) as target_table,
    CAST(NULL AS STRING) as source_table,
    CAST(NULL AS BIGINT) as last_processed_version,
    CAST(NULL AS TIMESTAMP) as last_processed_timestamp,
    CAST(NULL AS STRING) as batch_id,
    CAST(NULL AS TIMESTAMP) as batch_started_at,
    CAST(NULL AS TIMESTAMP) as batch_completed_at,
    CAST(NULL AS STRING) as batch_status,
    CAST(NULL AS BIGINT) as rows_read,
    CAST(NULL AS BIGINT) as rows_written,
    CAST(NULL AS STRING) as error_message,
    current_timestamp() as updated_at
WHERE 1=0  -- Empty seed, populated by watermark macros
{% else %}
-- On incremental runs, keep existing data
SELECT * FROM {{ this }}
{% endif %}
