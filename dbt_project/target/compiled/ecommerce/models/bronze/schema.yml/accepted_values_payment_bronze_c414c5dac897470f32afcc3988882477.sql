
    
    

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


