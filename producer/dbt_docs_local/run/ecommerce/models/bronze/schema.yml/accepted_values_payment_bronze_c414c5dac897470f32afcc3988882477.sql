select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        payment_method as value_field,
        count(*) as n_records

    from ECOMMERCE.dbt.payment_bronze
    group by payment_method

)

select *
from all_values
where value_field not in (
    'UPI','CARD','NETBANKING','COD'
)



      
    ) dbt_internal_test