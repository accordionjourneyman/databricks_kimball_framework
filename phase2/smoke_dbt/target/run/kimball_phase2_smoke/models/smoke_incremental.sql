
    insert into table dbt_baseline.smoke_incremental
    select `id`, `value` from smoke_incremental__dbt_tmp

