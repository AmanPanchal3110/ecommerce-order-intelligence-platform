select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from ECOMMERCE.dbt.return_bronze

where not(refund_amount refund_amount > 0)


      
    ) dbt_internal_test