
    
    

select
    event_seq as unique_field,
    count(*) as n_records

from dbt_reference_source.retail_events
where event_seq is not null
group by event_seq
having count(*) > 1


