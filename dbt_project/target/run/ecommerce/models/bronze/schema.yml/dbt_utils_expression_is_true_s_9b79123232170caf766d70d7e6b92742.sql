select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from ECOMMERCE.dbt.status_bronze

where not(event_timestamp event_timestamp <= current_timestamp())


      
    ) dbt_internal_test