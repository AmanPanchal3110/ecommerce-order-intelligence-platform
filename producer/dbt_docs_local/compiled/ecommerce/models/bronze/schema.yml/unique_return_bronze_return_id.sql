
    
    

select
    return_id as unique_field,
    count(*) as n_records

from ECOMMERCE.bronze.return_bronze
where return_id is not null
group by return_id
having count(*) > 1


