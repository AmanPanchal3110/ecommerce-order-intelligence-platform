select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select amount
from ECOMMERCE.dbt.payment_bronze
where amount is null



      
    ) dbt_internal_test