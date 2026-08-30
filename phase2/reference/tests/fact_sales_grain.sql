select source_system, line_id, count(*) as row_count
from {{ ref('fact_sales') }}
group by source_system, line_id
having count(*) > 1
