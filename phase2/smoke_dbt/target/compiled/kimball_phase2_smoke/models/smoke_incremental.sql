

select id, value
from dbt_baseline.smoke_input
where id <= 3

  and id > (select coalesce(max(id), 0) from dbt_baseline.smoke_incremental)
