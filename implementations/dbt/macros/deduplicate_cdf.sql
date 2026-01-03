-- CDF Deduplication Macro for dbt Kimball Framework
-- Handles multiple updates to same row between versions

{# ================================================================
   DEDUPLICATE CDF
   Keeps only the latest version per primary key.
   Required when same row updated multiple times between watermarks.
   
   Usage in a model:
   WITH source_cdf AS (
       SELECT * FROM {{ source('silver', 'customers') }}_cdf
   )
   {{ deduplicate_cdf('source_cdf', ['customer_id']) }}
   ================================================================ #}

{% macro deduplicate_cdf(source_cte, primary_keys) %}
SELECT * EXCEPT(_rn)
FROM (
    SELECT *, 
        ROW_NUMBER() OVER (
            PARTITION BY {{ primary_keys | join(', ') }} 
            ORDER BY _commit_version DESC
        ) as _rn
    FROM {{ source_cte }}
)
WHERE _rn = 1
{% endmacro %}


{# ================================================================
   DEDUPLICATE BY TIMESTAMP
   Fallback deduplication using timestamp column instead of version
   ================================================================ #}

{% macro deduplicate_by_timestamp(source_cte, primary_keys, timestamp_col='updated_at') %}
SELECT * EXCEPT(_rn)
FROM (
    SELECT *, 
        ROW_NUMBER() OVER (
            PARTITION BY {{ primary_keys | join(', ') }} 
            ORDER BY {{ timestamp_col }} DESC
        ) as _rn
    FROM {{ source_cte }}
)
WHERE _rn = 1
{% endmacro %}
