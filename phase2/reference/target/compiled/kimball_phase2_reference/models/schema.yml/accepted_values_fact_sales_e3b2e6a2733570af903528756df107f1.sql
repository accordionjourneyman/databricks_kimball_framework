
    
    

with all_values as (

    select
        customer_resolution as value_field,
        count(*) as n_records

    from dbt_reference.fact_sales
    group by customer_resolution

)

select *
from all_values
where value_field not in (
    'MATCHED','UNKNOWN','SKELETON'
)


