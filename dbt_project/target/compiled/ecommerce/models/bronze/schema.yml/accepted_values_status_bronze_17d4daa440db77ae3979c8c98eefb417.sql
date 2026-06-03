
    
    

with all_values as (

    select
        payment_status as value_field,
        count(*) as n_records

    from ECOMMERCE.dbt.status_bronze
    group by payment_status

)

select *
from all_values
where value_field not in (
    'PENDING','SUCCESS','FAILED'
)


