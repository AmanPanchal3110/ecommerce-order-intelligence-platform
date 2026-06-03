select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select payment_id
from ECOMMERCE.dbt.payment_bronze
where payment_id is null



      
    ) dbt_internal_test