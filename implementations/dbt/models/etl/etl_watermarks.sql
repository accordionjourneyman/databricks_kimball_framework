-- ETL Watermarks Table for dbt Kimball Framework
-- Stores per-source watermarks for incremental CDF processing
-- This table is shared between dbt and PySpark implementations

{{ config(
    materialized='incremental',
    unique_key=['target_table', 'source_table'],
    incremental_strategy='merge'
) }}

{# 
   This model ensures the etl_watermarks table exists.
   The actual watermark updates are done by:
   - PySpark: ETLControlManager.batch_complete()
   - dbt: update_watermark() macro in on-run-end hooks
   
   Schema:
   - target_table: The target dimension/fact table
   - source_table: The source table being processed
   - last_processed_version: Delta table version last processed
   - last_processed_timestamp: Timestamp of last processed commit
   - updated_at: When this record was last updated
#}

{% if execute %}
    {% set schema = var('etl_control_schema', 'demo_gold') %}
    
    {# Check if table exists #}
    {% set check_sql %}
    SELECT COUNT(*) as cnt FROM information_schema.tables 
    WHERE table_schema = '{{ schema }}' AND table_name = 'etl_watermarks'
    {% endset %}
    
    {% set exists_result = run_query(check_sql) %}
    {% set table_exists = exists_result.columns[0].values()[0] > 0 if exists_result.rows|length > 0 else false %}
    
    {% if not table_exists %}
        {# Create the table if it doesn't exist #}
        {% set create_sql %}
        CREATE TABLE IF NOT EXISTS {{ schema }}.etl_watermarks (
            target_table STRING NOT NULL,
            source_table STRING NOT NULL,
            last_processed_version LONG,
            last_processed_timestamp TIMESTAMP,
            updated_at TIMESTAMP NOT NULL,
            
            -- Composite primary key for deduplication
            CONSTRAINT etl_watermarks_pk PRIMARY KEY (target_table, source_table)
        )
        USING DELTA
        PARTITIONED BY (target_table, source_table)
        COMMENT 'Kimball ETL Watermarks. Tracks last processed version per (target, source) pair for incremental CDF processing.'
        {% endset %}
        
        {% do run_query(create_sql) %}
        {{ log("Created etl_watermarks table in " ~ schema, info=True) }}
    {% endif %}
{% endif %}

-- Return empty result for incremental model (table is managed via DDL above)
SELECT 
    CAST(NULL AS STRING) as target_table,
    CAST(NULL AS STRING) as source_table,
    CAST(NULL AS LONG) as last_processed_version,
    CAST(NULL AS TIMESTAMP) as last_processed_timestamp,
    CAST(NULL AS TIMESTAMP) as updated_at
WHERE 1=0
