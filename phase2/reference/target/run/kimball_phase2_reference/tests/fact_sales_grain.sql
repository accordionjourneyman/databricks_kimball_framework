
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select source_system, line_id, count(*) as row_count
from dbt_reference.fact_sales
group by source_system, line_id
having count(*) > 1
  
  
      
    ) dbt_internal_test