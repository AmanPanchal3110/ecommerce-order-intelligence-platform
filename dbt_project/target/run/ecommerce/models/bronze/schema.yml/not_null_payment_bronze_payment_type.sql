select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select payment_type
from ECOMMERCE.dbt.payment_bronze
where payment_type is null



      
    ) dbt_internal_test