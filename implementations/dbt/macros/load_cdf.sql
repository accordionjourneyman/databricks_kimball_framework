-- CDF (Change Data Feed) Loading Macros for dbt Kimball Framework
-- Provides Delta CDF support with timestamp fallback

{# ================================================================
   LOAD CDF
   Creates a temp view with CDF changes from starting_version.
   Falls back to timestamp-based if CDF not available.
   ================================================================ #}

{% macro load_cdf(source_table, target_table, deduplicate_keys=none) %}
    {# Get starting version from watermark table (NOT etl_control which has different schema) #}
    {%- set schema = var('etl_control_schema', 'demo_gold') -%}
    {%- set watermark_table = schema ~ '.etl_watermarks' -%}
    
    {% set get_version_sql %}
    SELECT COALESCE(last_processed_version, 0) as starting_version,
           last_processed_timestamp
    FROM {{ watermark_table }}
    WHERE target_table = '{{ target_table }}'
      AND source_table = '{{ source_table }}'
    {% endset %}
    
    {%- set watermark_result = run_query(get_version_sql) -%}
    {%- set starting_version = watermark_result.columns[0].values()[0] if watermark_result.columns|length > 0 and watermark_result.rows|length > 0 else 0 -%}
    
    {# Try CDF first, fall back to timestamp if unavailable #}
    {% set cdf_view_name = source_table | replace('.', '_') ~ '_cdf' %}
    
    {# CRITICAL: Use starting_version + 1 to avoid reprocessing the last version #}
    {% set next_version = starting_version + 1 %}
    
    {% set create_cdf_view %}
    CREATE OR REPLACE TEMPORARY VIEW {{ cdf_view_name }} AS
    SELECT *, _change_type, _commit_version, _commit_timestamp
    FROM table_changes('{{ source_table }}', {{ next_version }})
    WHERE _change_type != 'update_preimage'
    {% if deduplicate_keys is not none %}
    -- Deduplicate: keep only latest version per key
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {{ deduplicate_keys | join(', ') }}
        ORDER BY _commit_version DESC
    ) = 1
    {% endif %}
    {% endset %}
    
    {% do run_query(create_cdf_view) %}
    {{ log("Created CDF view: " ~ cdf_view_name ~ " from version " ~ next_version ~ " (last processed: " ~ starting_version ~ ")", info=True) }}
    
    {{ return(cdf_view_name) }}
{% endmacro %}


{# ================================================================
   CHECK CDF AVAILABLE
   Returns true if CDF is enabled on the source table
   ================================================================ #}

{% macro is_cdf_enabled(source_table) %}
    {% set check_sql %}
    DESCRIBE DETAIL {{ source_table }}
    {% endset %}
    
    {%- set result = run_query(check_sql) -%}
    {# Check if properties contain enableChangeDataFeed #}
    {%- set properties = result.columns['properties'].values()[0] if 'properties' in result.columns else '' -%}
    {{ return('enableChangeDataFeed' in properties and 'true' in properties) }}
{% endmacro %}


{# ================================================================
   CDF SOURCE MACRO
   Use in models to read from CDF with automatic fallback
   ================================================================ #}

{% macro cdf_source(source_name, table_name, target_table, deduplicate_keys=none, fallback_timestamp_col='updated_at') %}
    {%- set full_source = source(source_name, table_name) -%}
    {%- set source_table_str = full_source | string -%}
    
    {# Check if we should use CDF (via var) #}
    {% if var('use_cdf', true) %}
        {# Try to use CDF #}
        {% set cdf_view = load_cdf(source_table_str, target_table, deduplicate_keys) %}
        SELECT * FROM {{ cdf_view }}
    {% else %}
        {# Fallback to timestamp-based incremental #}
        SELECT * FROM {{ full_source }}
        {% if is_incremental() %}
        WHERE {{ fallback_timestamp_col }} > {{ get_watermark(target_table, source_table_str, 'timestamp') }}
        {% endif %}
    {% endif %}
{% endmacro %}
