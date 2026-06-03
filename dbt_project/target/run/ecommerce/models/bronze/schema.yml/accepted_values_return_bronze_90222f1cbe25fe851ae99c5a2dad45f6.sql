select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        return_reason as value_field,
        count(*) as n_records

    from ECOMMERCE.dbt.return_bronze
    group by return_reason

)

select *
from all_values
where value_field not in (
    'DAMAGED','WRONG_ITEM','NO_LONGER_NEEDED'
)



      
    ) dbt_internal_test