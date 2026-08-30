
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    event_seq as unique_field,
    count(*) as n_records

from dbt_reference_source.retail_events
where event_seq is not null
group by event_seq
having count(*) > 1



  
  
      
    ) dbt_internal_test