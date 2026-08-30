select source_system, customer_id, count(*) as current_count
from dbt_reference.dim_customer
where is_current
group by source_system, customer_id
having count(*) <> 1