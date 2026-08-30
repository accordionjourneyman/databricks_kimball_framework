
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
  earlier.source_system,
  earlier.customer_id,
  earlier.valid_from,
  earlier.valid_to,
  later.valid_from as overlapping_valid_from
from dbt_reference.dim_customer earlier
join dbt_reference.dim_customer later
  on earlier.source_system = later.source_system
 and earlier.customer_id = later.customer_id
 and earlier.valid_from < later.valid_from
 and earlier.valid_to > later.valid_from
  
  
      
    ) dbt_internal_test