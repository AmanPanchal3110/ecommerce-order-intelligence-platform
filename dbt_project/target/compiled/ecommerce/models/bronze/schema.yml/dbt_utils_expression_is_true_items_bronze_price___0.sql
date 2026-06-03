



select
    1
from ECOMMERCE.dbt.items_bronze

where not(price > 0)

