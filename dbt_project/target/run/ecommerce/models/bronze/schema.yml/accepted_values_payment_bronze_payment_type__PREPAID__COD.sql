select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        payment_type as value_field,
        count(*) as n_records

    from ECOMMERCE.dbt.payment_bronze
    group by payment_type

)

select *
from all_values
where value_field not in (
    'PREPAID','COD'
)



      
    ) dbt_internal_test