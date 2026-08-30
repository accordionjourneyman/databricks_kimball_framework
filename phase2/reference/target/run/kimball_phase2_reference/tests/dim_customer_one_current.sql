
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select source_system, customer_id, count(*) as current_count
from dbt_reference.dim_customer
where is_current
group by source_system, customer_id
having count(*) <> 1
  
  
      
    ) dbt_internal_test