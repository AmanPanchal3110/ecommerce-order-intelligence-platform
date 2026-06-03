
    
    

with all_values as (

    select
        return_status as value_field,
        count(*) as n_records

    from ECOMMERCE.dbt.return_bronze
    group by return_status

)

select *
from all_values
where value_field not in (
    'INITIATED','APPROVED','REJECTED'
)


