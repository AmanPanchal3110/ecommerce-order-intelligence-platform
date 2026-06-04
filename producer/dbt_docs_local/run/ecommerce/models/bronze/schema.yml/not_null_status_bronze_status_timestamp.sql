select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select status_timestamp
from ECOMMERCE.dbt.status_bronze
where status_timestamp is null



      
    ) dbt_internal_test