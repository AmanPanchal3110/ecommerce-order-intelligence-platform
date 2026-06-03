select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from ECOMMERCE.dbt.items_bronze

where not(quantity > 0)


      
    ) dbt_internal_test