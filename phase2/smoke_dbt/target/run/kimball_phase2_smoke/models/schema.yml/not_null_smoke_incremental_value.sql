
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select value
from dbt_baseline.smoke_incremental
where value is null



  
  
      
    ) dbt_internal_test