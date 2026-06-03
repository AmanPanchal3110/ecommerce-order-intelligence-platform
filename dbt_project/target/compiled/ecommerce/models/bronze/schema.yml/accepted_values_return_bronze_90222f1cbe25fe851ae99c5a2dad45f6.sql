
    
    

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


