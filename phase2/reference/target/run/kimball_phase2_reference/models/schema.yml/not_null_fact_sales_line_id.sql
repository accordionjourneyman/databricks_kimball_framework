
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select line_id
from dbt_reference.fact_sales
where line_id is null



  
  
      
    ) dbt_internal_test