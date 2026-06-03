select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select return_reason
from ECOMMERCE.dbt.return_bronze
where return_reason is null



      
    ) dbt_internal_test