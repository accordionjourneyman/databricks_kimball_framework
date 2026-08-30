
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_seq
from dbt_reference_source.retail_events
where event_seq is null



  
  
      
    ) dbt_internal_test