select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select refund_amount
from ECOMMERCE.dbt.return_bronze
where refund_amount is null



      
    ) dbt_internal_test