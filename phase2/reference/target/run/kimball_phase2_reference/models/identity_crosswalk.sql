create or replace view dbt_reference.identity_crosswalk
  
  
  as
    select 'uci' as source_system, '17850' as source_customer_id,
       'retail-party-17850' as enterprise_customer_id
union all
select 'erp', '17850', 'retail-party-17850'
