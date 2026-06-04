select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from ECOMMERCE.dbt.payment_bronze

where not(event_timestamp <= current_timestamp())


      
    ) dbt_internal_test