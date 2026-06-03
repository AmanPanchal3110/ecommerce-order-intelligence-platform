



select
    1
from ECOMMERCE.dbt.items_bronze

where not(quantity > 0)

