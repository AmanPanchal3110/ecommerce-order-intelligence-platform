
    
    

with all_values as (

    select
        order_status as value_field,
        count(*) as n_records

    from ECOMMERCE.bronze.status_bronze
    group by order_status

)

select *
from all_values
where value_field not in (
    'CREATED','CONFIRMED','SHIPPED','DELIVERED','CANCELLED'
)


